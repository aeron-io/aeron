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

import io.aeron.driver.exceptions.UnknownSubscriptionException;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.exceptions.AeronEvent;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.SetupFlyweight;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;

import static io.aeron.driver.DataPacketDispatcher.SessionState.*;

/**
 * Handling of dispatching data packets to {@link PublicationImage}s streams.
 * <p>
 * All methods should be called from the {@link Receiver} thread.
 */
public final class DataPacketDispatcher
{
    @SuppressWarnings("JavadocVariable")
    enum SessionState
    {
        ACTIVE,
        PENDING_SETUP_FRAME,
        INIT_IN_PROGRESS,
        ON_COOL_DOWN,
        NO_INTEREST
    }

    static class StreamInterest
    {
        boolean isAllSessions;
        final Int2ObjectHashMap<SessionState> sessionInterestByIdMap = new Int2ObjectHashMap<>();
        final Int2ObjectHashMap<PublicationImage> activeImageByIdMap = new Int2ObjectHashMap<>();
        final IntHashSet subscribedSessionIds = new IntHashSet();

        StreamInterest(final boolean isAllSessions)
        {
            this.isAllSessions = isAllSessions;
        }

        PublicationImage findActive(final int sessionId)
        {
            return activeImageByIdMap.get(sessionId);
        }

        public boolean isSessionLimitExceeded(final int sessionLimit)
        {
            return sessionLimit <= activeImageByIdMap.size();
        }

        void removeNonSessionSpecificInterest()
        {
            final Int2ObjectHashMap<PublicationImage>.EntryIterator activeIterator =
                activeImageByIdMap.entrySet().iterator();

            while (activeIterator.hasNext())
            {
                activeIterator.next();

                final int sessionId = activeIterator.getIntKey();
                if (!subscribedSessionIds.contains(sessionId))
                {
                    activeIterator.getValue().deactivate();
                    activeIterator.remove();
                }
            }

            final Int2ObjectHashMap<SessionState>.EntryIterator iterator =
                sessionInterestByIdMap.entrySet().iterator();

            while (iterator.hasNext())
            {
                iterator.next();

                final int sessionId = iterator.getIntKey();
                if (!subscribedSessionIds.contains(sessionId))
                {
                    iterator.remove();
                }
            }
        }
    }

    private final Int2ObjectHashMap<StreamInterest> streamInterestByIdMap = new Int2ObjectHashMap<>();
    private final DriverConductorProxy conductorProxy;
    private final Receiver receiver;
    private final int streamSessionLimit;

    DataPacketDispatcher(
        final DriverConductorProxy conductorProxy,
        final Receiver receiver,
        final int streamSessionLimit)
    {
        this.conductorProxy = conductorProxy;
        this.receiver = receiver;
        this.streamSessionLimit = streamSessionLimit;
    }

    /**
     * Add a subscription to a channel for a given stream id and wildcard session id.
     *
     * @param streamId to capture within a channel.
     */
    public void addSubscription(final int streamId)
    {
        final StreamInterest streamInterest = streamInterestByIdMap.get(streamId);

        if (null == streamInterest)
        {
            streamInterestByIdMap.put(streamId, new StreamInterest(true));
        }
        else if (!streamInterest.isAllSessions)
        {
            streamInterest.isAllSessions = true;

            final Int2ObjectHashMap<SessionState>.ValueIterator iterator =
                streamInterest.sessionInterestByIdMap.values().iterator();

            while (iterator.hasNext())
            {
                if (NO_INTEREST == iterator.next())
                {
                    iterator.remove();
                }
            }
        }
    }

    /**
     * Add a subscription to a channel for given stream and session ids.
     *
     * @param streamId  to capture within a channel.
     * @param sessionId to capture within a stream id.
     */
    public void addSubscription(final int streamId, final int sessionId)
    {
        StreamInterest streamInterest = streamInterestByIdMap.get(streamId);
        if (null == streamInterest)
        {
            streamInterest = new StreamInterest(false);
            streamInterestByIdMap.put(streamId, streamInterest);
        }

        streamInterest.subscribedSessionIds.add(sessionId);

        final SessionState sessionState = streamInterest.sessionInterestByIdMap.get(sessionId);
        if (NO_INTEREST == sessionState)
        {
            streamInterest.sessionInterestByIdMap.remove(sessionId);
        }
    }

    /**
     * Remove a subscription for a previously registered given stream id and wildcard session id.
     *
     * @param streamId to remove the capture for.
     */
    public void removeSubscription(final int streamId)
    {
        final StreamInterest streamInterest = streamInterestByIdMap.get(streamId);
        if (null == streamInterest)
        {
            throw new UnknownSubscriptionException("no subscription for stream " + streamId);
        }

        streamInterest.removeNonSessionSpecificInterest();

        streamInterest.isAllSessions = false;

        if (streamInterest.subscribedSessionIds.isEmpty())
        {
            streamInterestByIdMap.remove(streamId);
        }
    }

    /**
     * Remove a subscription for a previously registered given stream id and session id.
     *
     * @param streamId  to remove the capture for.
     * @param sessionId within the given stream id.
     */
    public void removeSubscription(final int streamId, final int sessionId)
    {
        final StreamInterest streamInterest = streamInterestByIdMap.get(streamId);
        if (null == streamInterest)
        {
            throw new UnknownSubscriptionException("no subscription for stream " + streamId);
        }

        if (!streamInterest.isAllSessions)
        {
            final PublicationImage publicationImage = streamInterest.activeImageByIdMap.remove(sessionId);
            if (null != publicationImage)
            {
                publicationImage.deactivate();
            }
            streamInterest.sessionInterestByIdMap.remove(sessionId);
        }

        streamInterest.subscribedSessionIds.remove(sessionId);

        if (!streamInterest.isAllSessions && streamInterest.subscribedSessionIds.isEmpty())
        {
            streamInterestByIdMap.remove(streamId);
        }
    }

    /**
     * Add a {@link PublicationImage} to dispatch packets to.
     *
     * @param image to which the packets are dispatched.
     */
    public void addPublicationImage(final PublicationImage image)
    {
        final StreamInterest streamInterest = streamInterestByIdMap.get(image.streamId());
        if (null != streamInterest)
        {
            streamInterest.sessionInterestByIdMap.remove(image.sessionId());
            streamInterest.activeImageByIdMap.put(image.sessionId(), image);
            image.activate();
        }
    }

    /**
     * Remove a previously added {@link PublicationImage} so packets are no longer dispatched to it.
     *
     * @param image to which the packets are dispatched.
     */
    public void removePublicationImage(final PublicationImage image)
    {
        final StreamInterest streamInterest = streamInterestByIdMap.get(image.streamId());
        if (null != streamInterest)
        {
            final PublicationImage activeImage = streamInterest.activeImageByIdMap.get(image.sessionId());
            if (null != activeImage && activeImage.correlationId() == image.correlationId())
            {
                streamInterest.activeImageByIdMap.remove(image.sessionId());

                if (!image.isEndOfStream())
                {
                    streamInterest.sessionInterestByIdMap.put(image.sessionId(), ON_COOL_DOWN);
                }
            }
        }

        image.deactivate();
    }

    /**
     * Remove a pending setup message action once it has been handled.
     *
     * @param sessionId of the registered interest.
     * @param streamId  of the registered interest.
     */
    public void removePendingSetup(final int sessionId, final int streamId)
    {
        removeByState(sessionId, streamId, PENDING_SETUP_FRAME);
    }

    /**
     * Remove a cool down action once it has expired.
     *
     * @param sessionId of the registered interest.
     * @param streamId  of the registered interest.
     */
    public void removeCoolDown(final int sessionId, final int streamId)
    {
        removeByState(sessionId, streamId, ON_COOL_DOWN);
    }

    /**
     * Dispatch a data packet to the registered interest.
     *
     * @param channelEndpoint on which the packet was received.
     * @param header          of the data first frame.
     * @param buffer          containing the data packet.
     * @param length          of the data packet.
     * @param srcAddress      from which the data packet was received.
     * @param transportIndex  on which the packet was received.
     * @return number of bytes applied as a result of this action.
     */
    public int onDataPacket(
        final ReceiveChannelEndpoint channelEndpoint,
        final DataHeaderFlyweight header,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress,
        final int transportIndex)
    {
        final int streamId = header.streamId();
        final StreamInterest streamInterest = streamInterestByIdMap.get(streamId);

        if (null != streamInterest)
        {
            final int sessionId = header.sessionId();

            final PublicationImage image = streamInterest.findActive(sessionId);
            if (null != image)
            {
                return image.insertPacket(
                    header.termId(), header.termOffset(), buffer, length, transportIndex, srcAddress);
            }
            else if (!DataHeaderFlyweight.isEndOfStream(buffer) &&
                !streamInterest.sessionInterestByIdMap.containsKey(sessionId))
            {
                if (streamInterest.isAllSessions || streamInterest.subscribedSessionIds.contains(sessionId))
                {
                    streamInterest.sessionInterestByIdMap.put(sessionId, PENDING_SETUP_FRAME);
                    elicitSetupMessageFromSource(channelEndpoint, transportIndex, srcAddress, streamId, sessionId);
                }
                else
                {
                    streamInterest.sessionInterestByIdMap.put(sessionId, NO_INTEREST);
                }
            }
        }

        return 0;
    }

    /**
     * Dispatch a setup message to registered interest.
     *
     * @param channelEndpoint of reception.
     * @param msg             flyweight over the network packet.
     * @param srcAddress      the message came from.
     * @param transportIndex  on which the message was received.
     */
    public void onSetupMessage(
        final ReceiveChannelEndpoint channelEndpoint,
        final SetupFlyweight msg,
        final InetSocketAddress srcAddress,
        final int transportIndex)
    {
        final int streamId = msg.streamId();
        final StreamInterest streamInterest = streamInterestByIdMap.get(streamId);

        if (null != streamInterest)
        {
            final int sessionId = msg.sessionId();

            if (streamInterest.isSessionLimitExceeded(streamSessionLimit))
            {
                throw new AeronEvent("exceeded session limit, streamId=" + streamId + " sourceAddress=" + srcAddress);
            }

            final PublicationImage image = streamInterest.findActive(sessionId);
            final SessionState sessionInterest = streamInterest.sessionInterestByIdMap.get(sessionId);

            if (null != image)
            {
                image.addDestinationConnectionIfUnknown(transportIndex, srcAddress);
            }
            else if (null != sessionInterest)
            {
                if (PENDING_SETUP_FRAME == sessionInterest)
                {
                    streamInterest.sessionInterestByIdMap.put(sessionId, INIT_IN_PROGRESS);

                    createPublicationImage(
                        channelEndpoint,
                        transportIndex,
                        srcAddress,
                        streamId,
                        sessionId,
                        msg.initialTermId(),
                        msg.activeTermId(),
                        msg.termOffset(),
                        msg.termLength(),
                        msg.mtuLength(),
                        msg.ttl(),
                        msg.flags());
                }
            }
            else
            {
                if (streamInterest.isAllSessions || streamInterest.subscribedSessionIds.contains(sessionId))
                {
                    streamInterest.sessionInterestByIdMap.put(sessionId, INIT_IN_PROGRESS);
                    createPublicationImage(
                        channelEndpoint,
                        transportIndex,
                        srcAddress,
                        streamId,
                        sessionId,
                        msg.initialTermId(),
                        msg.activeTermId(),
                        msg.termOffset(),
                        msg.termLength(),
                        msg.mtuLength(),
                        msg.ttl(),
                        msg.flags());
                }
                else
                {
                    streamInterest.sessionInterestByIdMap.put(sessionId, NO_INTEREST);
                }
            }
        }
    }

    /**
     * Dispatch an RTT measurement message to registered interest.
     *
     * @param channelEndpoint of reception.
     * @param msg             flyweight over the network packet.
     * @param srcAddress      the message came from.
     * @param transportIndex  on which the message was received.
     */
    public void onRttMeasurement(
        final ReceiveChannelEndpoint channelEndpoint,
        final RttMeasurementFlyweight msg,
        final InetSocketAddress srcAddress,
        final int transportIndex)
    {
        final int streamId = msg.streamId();
        final StreamInterest streamInterest = streamInterestByIdMap.get(streamId);

        if (null != streamInterest)
        {
            final int sessionId = msg.sessionId();

            final PublicationImage image = streamInterest.findActive(sessionId);
            if (null != image)
            {
                if (RttMeasurementFlyweight.REPLY_FLAG == (msg.flags() & RttMeasurementFlyweight.REPLY_FLAG))
                {
                    final InetSocketAddress controlAddress = channelEndpoint.isMulticast(transportIndex) ?
                        channelEndpoint.udpChannel(transportIndex).remoteControl() : srcAddress;

                    channelEndpoint.sendRttMeasurement(
                        transportIndex, controlAddress, sessionId, streamId, msg.echoTimestampNs(), 0, false);
                }
                else
                {
                    image.onRttMeasurement(msg, transportIndex, srcAddress);
                }
            }
        }
    }

    /**
     * Should a setup message be elicited for a channel given interest.
     *
     * @return true if there is interest otherwise false.
     */
    public boolean shouldElicitSetupMessage()
    {
        return !streamInterestByIdMap.isEmpty();
    }

    private void removeByState(final int sessionId, final int streamId, final SessionState state)
    {
        final StreamInterest streamInterest = streamInterestByIdMap.get(streamId);
        if (null != streamInterest)
        {
            final SessionState sessionState = streamInterest.sessionInterestByIdMap.get(sessionId);
            if (null != sessionState && state == sessionState)
            {
                streamInterest.sessionInterestByIdMap.remove(sessionId);
            }
        }
    }

    private void elicitSetupMessageFromSource(
        final ReceiveChannelEndpoint channelEndpoint,
        final int transportIndex,
        final InetSocketAddress srcAddress,
        final int streamId,
        final int sessionId)
    {
        final InetSocketAddress controlAddress = channelEndpoint.isMulticast(transportIndex) ?
            channelEndpoint.udpChannel(transportIndex).remoteControl() : srcAddress;

        channelEndpoint.sendSetupElicitingStatusMessage(transportIndex, controlAddress, sessionId, streamId);
        receiver.addPendingSetupMessage(sessionId, streamId, transportIndex, channelEndpoint, false, controlAddress);
    }

    private void createPublicationImage(
        final ReceiveChannelEndpoint channelEndpoint,
        final int transportIndex,
        final InetSocketAddress srcAddress,
        final int streamId,
        final int sessionId,
        final int initialTermId,
        final int activeTermId,
        final int termOffset,
        final int termLength,
        final int mtuLength,
        final int setupTtl,
        final short flags)
    {
        final InetSocketAddress controlAddress = channelEndpoint.isMulticast(transportIndex) ?
            channelEndpoint.udpChannel(transportIndex).remoteControl() : srcAddress;

        if (channelEndpoint.isMulticast(transportIndex) && channelEndpoint.multicastTtl(transportIndex) < setupTtl)
        {
            channelEndpoint.possibleTtlAsymmetryEncountered();
        }

        conductorProxy.createPublicationImage(
            sessionId,
            streamId,
            initialTermId,
            activeTermId,
            termOffset,
            termLength,
            mtuLength,
            transportIndex,
            flags,
            controlAddress,
            srcAddress,
            channelEndpoint);
    }
}
