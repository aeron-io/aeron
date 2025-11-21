/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.driver;

import io.aeron.driver.media.DataTransportPoller;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.ReceiveDestinationTransport;
import io.aeron.driver.media.UdpChannel;
import io.aeron.driver.status.DutyCycleStallTracker;
import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;

import static io.aeron.driver.Configuration.PENDING_SETUPS_TIMEOUT_NS;
import static io.aeron.driver.status.SystemCounterDescriptor.BYTES_RECEIVED;
import static io.aeron.driver.status.SystemCounterDescriptor.RESOLUTION_CHANGES;

/**
 * Agent that receives messages streams and rebuilds {@link PublicationImage}s, plus iterates over them sending status
 * and control messages back to the {@link Sender}.
 */
public final class Receiver implements Agent
{
    private static final PublicationImage[] EMPTY_IMAGES = new PublicationImage[0];

    private final long reResolutionCheckIntervalNs;
    private long reResolutionDeadlineNs;
    private final DataTransportPoller dataTransportPoller;
    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue;
    private final AtomicCounter totalBytesReceived;
    private final AtomicCounter resolutionChanges;
    private final NanoClock nanoClock;
    private final CachedNanoClock cachedNanoClock;
    private PublicationImage[] publicationImages = EMPTY_IMAGES;
    private final ArrayList<PendingSetupMessageFromSource> pendingSetupMessages = new ArrayList<>();
    private final DriverConductorProxy conductorProxy;
    private final DutyCycleTracker dutyCycleTracker;

    Receiver(final MediaDriver.Context ctx)
    {
        dataTransportPoller = ctx.dataTransportPoller();
        commandQueue = ctx.receiverCommandQueue();
        totalBytesReceived = ctx.systemCounters().get(BYTES_RECEIVED);
        resolutionChanges = ctx.systemCounters().get(RESOLUTION_CHANGES);
        nanoClock = ctx.nanoClock();
        cachedNanoClock = ctx.receiverCachedNanoClock();
        conductorProxy = ctx.driverConductorProxy();
        reResolutionCheckIntervalNs = ctx.reResolutionCheckIntervalNs();
        dutyCycleTracker = ctx.receiverDutyCycleTracker();
    }

    /**
     * {@inheritDoc}
     */
    public void onStart()
    {
        final long nowNs = nanoClock.nanoTime();
        cachedNanoClock.update(nowNs);
        dutyCycleTracker.update(nowNs);
        reResolutionDeadlineNs = nowNs + reResolutionCheckIntervalNs;

        if (dutyCycleTracker instanceof DutyCycleStallTracker)
        {
            final DutyCycleStallTracker dutyCycleStallTracker = (DutyCycleStallTracker)dutyCycleTracker;

            dutyCycleStallTracker.maxCycleTime().appendToLabel(": " + conductorProxy.threadingMode().name());
            dutyCycleStallTracker.cycleTimeThresholdExceededCount().appendToLabel(
                ": threshold=" + dutyCycleStallTracker.cycleTimeThresholdNs() + "ns " +
                conductorProxy.threadingMode().name());
        }
    }

    /**
     * {@inheritDoc}
     */
    public void onClose()
    {
        dataTransportPoller.close();
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        return "receiver";
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        final long nowNs = nanoClock.nanoTime();
        cachedNanoClock.update(nowNs);
        dutyCycleTracker.measureAndUpdate(nowNs);

        int workCount = commandQueue.drain(CommandProxy.RUN_TASK, Configuration.COMMAND_DRAIN_LIMIT);

        final int bytesReceived = dataTransportPoller.pollTransports();
        totalBytesReceived.getAndAddOrdered(bytesReceived);

        final PublicationImage[] publicationImages = this.publicationImages;
        for (int lastIndex = publicationImages.length - 1, i = lastIndex; i >= 0; i--)
        {
            final PublicationImage image = publicationImages[i];
            if (image.isConnected(nowNs))
            {
                image.checkEosForDrainTransition(nowNs);

                workCount += image.sendPendingStatusMessage(nowNs);
                workCount += image.processPendingLoss();
                workCount += image.initiateAnyRttMeasurements(nowNs);
            }
            else
            {
                this.publicationImages = 1 == this.publicationImages.length ?
                    EMPTY_IMAGES : ArrayUtil.remove(this.publicationImages, i);
                image.removeFromDispatcher();
                image.receiverRelease();
            }
        }

        checkPendingSetupMessages(nowNs);

        if (reResolutionCheckIntervalNs > 0 && (reResolutionDeadlineNs - nowNs) < 0)
        {
            reResolutionDeadlineNs = nowNs + reResolutionCheckIntervalNs;
            dataTransportPoller.checkForReResolutions(nowNs, conductorProxy);
        }

        return workCount + bytesReceived;
    }

    void addPendingSetupMessage(
        final int sessionId,
        final int streamId,
        final int transportIndex,
        final ReceiveChannelEndpoint channelEndpoint,
        final boolean periodic,
        final InetSocketAddress controlAddress)
    {
        final PendingSetupMessageFromSource cmd = new PendingSetupMessageFromSource(
            sessionId, streamId, transportIndex, channelEndpoint, periodic, controlAddress);

        cmd.timeOfStatusMessageNs(cachedNanoClock.nanoTime());
        pendingSetupMessages.add(cmd);
    }

    void onAddSubscription(final ReceiveChannelEndpoint channelEndpoint, final int streamId)
    {
        channelEndpoint.dispatcher().addSubscription(streamId);
    }

    void onAddSubscription(final ReceiveChannelEndpoint channelEndpoint, final int streamId, final int sessionId)
    {
        channelEndpoint.dispatcher().addSubscription(streamId, sessionId);
        if (channelEndpoint.hasExplicitControl())
        {
            channelEndpoint.sendSetupElicitingStatusMessage(
                0, channelEndpoint.explicitControlAddress(), sessionId, streamId);
        }
    }

    void onRequestSetup(final ReceiveChannelEndpoint channelEndpoint, final int streamId, final int sessionId)
    {
        if (channelEndpoint.hasExplicitControl())
        {
            channelEndpoint.sendSetupElicitingStatusMessage(
                0, channelEndpoint.explicitControlAddress(), sessionId, streamId);
        }
    }

    void onRemoveSubscription(final ReceiveChannelEndpoint channelEndpoint, final int streamId)
    {
        channelEndpoint.dispatcher().removeSubscription(streamId);
    }

    void onRemoveSubscription(final ReceiveChannelEndpoint channelEndpoint, final int streamId, final int sessionId)
    {
        channelEndpoint.dispatcher().removeSubscription(streamId, sessionId);

        final ArrayList<PendingSetupMessageFromSource> pendingSetupMessages = this.pendingSetupMessages;
        for (int lastIndex = pendingSetupMessages.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final PendingSetupMessageFromSource pending = pendingSetupMessages.get(i);

            if (pending.channelEndpoint() == channelEndpoint &&
                pending.streamId() == streamId &&
                pending.sessionId() == sessionId)
            {
                ArrayListUtil.fastUnorderedRemove(pendingSetupMessages, i, lastIndex--);
                pending.removeFromDataPacketDispatcher();
            }
        }
    }

    void onNewPublicationImage(final ReceiveChannelEndpoint channelEndpoint, final PublicationImage image)
    {
        disconnectInactiveImage(channelEndpoint, image.streamId(), image.sessionId());
        publicationImages = ArrayUtil.add(publicationImages, image);
        channelEndpoint.dispatcher().addPublicationImage(image);
    }

    void onRegisterReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        if (!channelEndpoint.hasDestinationControl())
        {
            channelEndpoint.registerForRead(dataTransportPoller);

            if (channelEndpoint.hasExplicitControl())
            {
                addPendingSetupMessage(0, 0, 0, channelEndpoint, true, channelEndpoint.explicitControlAddress());
                channelEndpoint.sendSetupElicitingStatusMessage(0, channelEndpoint.explicitControlAddress(), 0, 0);
            }
        }
    }

    void onCloseReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        final ArrayList<PendingSetupMessageFromSource> pendingSetupMessages = this.pendingSetupMessages;
        for (int lastIndex = pendingSetupMessages.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final PendingSetupMessageFromSource pending = pendingSetupMessages.get(i);

            if (pending.channelEndpoint() == channelEndpoint)
            {
                ArrayListUtil.fastUnorderedRemove(pendingSetupMessages, i, lastIndex--);
                pending.removeFromDataPacketDispatcher();
            }
        }

        channelEndpoint.closeMultiRcvDestinationTransports(dataTransportPoller);
        channelEndpoint.close();
        channelEndpoint.closeMultiRcvDestinationIndicators(conductorProxy);
    }

    void onRemoveCoolDown(final ReceiveChannelEndpoint channelEndpoint, final int sessionId, final int streamId)
    {
        channelEndpoint.dispatcher().removeCoolDown(sessionId, streamId);
    }

    void onAddDestination(final ReceiveChannelEndpoint channelEndpoint, final ReceiveDestinationTransport transport)
    {
        final int transportIndex = channelEndpoint.addDestination(transport);
        final SelectionKey key = dataTransportPoller.registerForRead(channelEndpoint, transport, transportIndex);
        transport.selectionKey(key);

        if (transport.hasExplicitControl())
        {
            addPendingSetupMessage(0, 0, transportIndex, channelEndpoint, true, transport.explicitControlAddress());
            channelEndpoint.sendSetupElicitingStatusMessage(transportIndex, transport.explicitControlAddress(), 0, 0);
        }

        for (final PublicationImage image : publicationImages)
        {
            if (channelEndpoint == image.channelEndpoint())
            {
                image.addDestination(transportIndex, transport);
            }
        }
    }

    void onRemoveDestination(final ReceiveChannelEndpoint channelEndpoint, final UdpChannel udpChannel)
    {
        final int transportIndex = channelEndpoint.destination(udpChannel);
        if (ArrayUtil.UNKNOWN_INDEX != transportIndex)
        {
            final ReceiveDestinationTransport transport = channelEndpoint.destination(transportIndex);

            dataTransportPoller.cancelRead(channelEndpoint, transport);
            channelEndpoint.removeDestination(transportIndex);
            transport.closeTransport();
            dataTransportPoller.selectNowWithoutProcessing();

            for (final PublicationImage image : publicationImages)
            {
                if (channelEndpoint == image.channelEndpoint())
                {
                    image.removeDestination(transportIndex);
                }
            }

            conductorProxy.closeReceiveDestinationIndicators(transport);
        }
    }

    void onResolutionChange(
        final ReceiveChannelEndpoint channelEndpoint, final UdpChannel channel, final InetSocketAddress newAddress)
    {
        final int transportIndex = channelEndpoint.hasDestinationControl() ? channelEndpoint.destination(channel) : 0;

        for (int i = 0, size = pendingSetupMessages.size(); i < size; i++)
        {
            final PendingSetupMessageFromSource pending = pendingSetupMessages.get(i);

            if (pending.channelEndpoint() == channelEndpoint &&
                pending.isPeriodic() &&
                pending.transportIndex() == transportIndex)
            {
                pending.controlAddress(newAddress);
                resolutionChanges.getAndAddRelease(1);
            }
        }

        channelEndpoint.updateControlAddress(transportIndex, newAddress);
    }

    void onRejectImage(final long imageCorrelationId, final long position, final String reason)
    {
        for (final PublicationImage image : publicationImages)
        {
            if (imageCorrelationId == image.correlationId())
            {
                image.reject(reason);
                break;
            }
        }
    }

    private void checkPendingSetupMessages(final long nowNs)
    {
        for (int lastIndex = pendingSetupMessages.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final PendingSetupMessageFromSource pending = pendingSetupMessages.get(i);

            if ((pending.timeOfStatusMessageNs() + PENDING_SETUPS_TIMEOUT_NS) - nowNs < 0)
            {
                if (!pending.isPeriodic())
                {
                    ArrayListUtil.fastUnorderedRemove(pendingSetupMessages, i, lastIndex--);
                    pending.removeFromDataPacketDispatcher();
                }
                else if (pending.shouldElicitSetupMessage())
                {
                    pending.timeOfStatusMessageNs(nowNs);
                    pending.channelEndpoint().sendSetupElicitingStatusMessage(
                        pending.transportIndex(), pending.controlAddress(), pending.sessionId(), pending.streamId());
                }
            }
        }
    }

    void disconnectInactiveImage(
        final ReceiveChannelEndpoint channelEndpoint,
        final int streamId,
        final int sessionId)
    {
        for (final PublicationImage publicationImage : publicationImages)
        {
            if (publicationImage.channelEndpoint() == channelEndpoint &&
                publicationImage.streamId() == streamId &&
                publicationImage.sessionId() == sessionId)
            {
                publicationImage.stopStatusMessagesIfNotActive();
            }
        }
    }
}
