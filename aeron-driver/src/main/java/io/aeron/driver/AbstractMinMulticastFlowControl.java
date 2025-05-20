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

import io.aeron.CommonContext;
import io.aeron.driver.media.UdpChannel;
import io.aeron.driver.status.FlowControlReceivers;
import io.aeron.protocol.ErrorFlyweight;
import io.aeron.protocol.SetupFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

import java.net.InetSocketAddress;
import java.util.Arrays;

import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;
import static io.aeron.protocol.StatusMessageFlyweight.END_OF_STREAM_FLAG;
import static org.agrona.AsciiEncoding.parseIntAscii;
import static org.agrona.AsciiEncoding.parseLongAscii;
import static org.agrona.SystemUtil.parseDuration;
import static org.agrona.collections.ArrayUtil.add;

abstract class AbstractMinMulticastFlowControlLhsPadding
{
    byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;
}

abstract class AbstractMinMulticastFlowControlFields extends AbstractMinMulticastFlowControlLhsPadding
{
    long lastSetupSenderLimit;
    long timeOfLastSetupNs;
    boolean hasTaggedStatusMessageTriggeredSetup;
}

abstract class AbstractMinMulticastFlowControlRhsPadding extends AbstractMinMulticastFlowControlFields
{
    byte p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;
}

/**
 * Abstract minimum multicast sender flow control strategy. It supports the concept of only tracking the minimum of a
 * group of receivers, not all possible receivers. However, it is agnostic of how that group is determined.
 * <p>
 * Tracking of receivers is done as long as they continue to send Status Messages. Once SMs stop, the receiver tracking
 * for that receiver will time out after a given number of nanoseconds.
 */
public abstract class AbstractMinMulticastFlowControl
    extends AbstractMinMulticastFlowControlRhsPadding
    implements FlowControl
{
    private static final Receiver[] EMPTY_RECEIVERS = new Receiver[0];

    private final boolean isGroupTagAware;
    private volatile boolean hasRequiredReceivers;
    private int groupMinSize;
    private long groupTag;
    private long receiverTimeoutNs;
    private Receiver[] receivers = EMPTY_RECEIVERS;
    private String channel;
    private AtomicCounter receiverCount;
    private ErrorHandler errorHandler;
    private int retransmitReceiverWindowMultiple;

    /**
     * Base constructor for use by specialised implementations.
     *
     * @param isGroupTagAware true if the group tag is used.
     */
    protected AbstractMinMulticastFlowControl(final boolean isGroupTagAware)
    {
        this.isGroupTagAware = isGroupTagAware;
    }

    /**
     * {@inheritDoc}
     */
    public void initialize(
        final MediaDriver.Context context,
        final CountersManager countersManager,
        final UdpChannel udpChannel,
        final int streamId,
        final int sessionId,
        final long registrationId,
        final int initialTermId,
        final int termBufferLength)
    {
        receiverTimeoutNs = context.flowControlReceiverTimeoutNs();
        groupTag = isGroupTagAware ? context.flowControlGroupTag() : 0;
        groupMinSize = context.flowControlGroupMinSize();
        channel = udpChannel.originalUriString();

        parseUriParam(udpChannel.channelUri().get(CommonContext.FLOW_CONTROL_PARAM_NAME));
        hasRequiredReceivers = receivers.length >= groupMinSize;
        errorHandler = context.errorHandler();
        receiverCount = FlowControlReceivers.allocate(
            context.tempBuffer(), countersManager, registrationId, sessionId, streamId, channel);
        timeOfLastSetupNs = 0;
        lastSetupSenderLimit = -1;
        hasTaggedStatusMessageTriggeredSetup = false;
        retransmitReceiverWindowMultiple = FlowControl.retransmitReceiverWindowMultiple(
            udpChannel,
            context.multicastFlowControlRetransmitReceiverWindowMultiple()
        );
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.close(errorHandler, receiverCount);
    }

    /**
     * {@inheritDoc}
     */
    public long onSetup(
        final SetupFlyweight flyweight,
        final long senderLimit,
        final long senderPosition,
        final int positionBitsToShift,
        final long timeNs)
    {
        if (hasTaggedStatusMessageTriggeredSetup && receivers.length > 0)
        {
            timeOfLastSetupNs = timeNs;
            lastSetupSenderLimit = senderLimit;
        }

        hasTaggedStatusMessageTriggeredSetup = false;

        return senderLimit;
    }

    /**
     * {@inheritDoc}
     */
    public long onIdle(final long timeNs, final long senderLimit, final long senderPosition, final boolean isEos)
    {
        long minLimitPosition = lastSetupSenderLimit(timeNs);
        int removed = 0;
        Receiver[] receivers = this.receivers;

        for (int lastIndex = receivers.length - 1, i = lastIndex; i >= 0; i--)
        {
            final Receiver receiver = receivers[i];
            if ((receiver.timeOfLastStatusMessageNs + receiverTimeoutNs) - timeNs < 0 || receiver.eosFlagged)
            {
                if (i != lastIndex)
                {
                    receivers[i] = receivers[lastIndex--];
                }
                removed++;
                receiverRemoved(
                    receiver.receiverId, receiver.sessionId, receiver.streamId, channel, receivers.length - removed);
            }
            else
            {
                minLimitPosition = Math.min(minLimitPosition, receiver.lastPositionPlusWindow);
            }
        }

        if (removed > 0)
        {
            receivers = truncateReceivers(receivers, removed);
            hasRequiredReceivers = receivers.length >= groupMinSize;
            this.receivers = receivers;
            receiverCount.setRelease(receivers.length);
        }

        return receivers.length < groupMinSize || receivers.length == 0 ? senderLimit : minLimitPosition;
    }

    /**
     * Has the observed receiver count reached the {@link #groupMinSize()} threshold?
     *
     * @return true if the observed receiver count reached the {@link #groupMinSize()} threshold?
     */
    public boolean hasRequiredReceivers()
    {
        return hasRequiredReceivers;
    }

    /**
     * {@inheritDoc}
     */
    public int maxRetransmissionLength(
        final int termOffset,
        final int resendLength,
        final int termBufferLength,
        final int mtuLength)
    {
        return FlowControl.calculateRetransmissionLength(
            resendLength, termBufferLength, termOffset, retransmitReceiverWindowMultiple);
    }

    /**
     * Process a received status message.
     *
     * @param flyweight           mapped over the status message.
     * @param senderLimit         the sender is currently limited to for sending.
     * @param initialTermId       for the publication.
     * @param positionBitsToShift when calculating term length with requiring a divide.
     * @param timeNs              current time.
     * @param matchesTag          if the status messages comes from a receiver with a tag matching the group.
     * @return the new position limit to be employed by the sender.
     */
    protected final long processStatusMessage(
        final StatusMessageFlyweight flyweight,
        final long senderLimit,
        final int initialTermId,
        final int positionBitsToShift,
        final long timeNs,
        final boolean matchesTag)
    {
        final long position = computePosition(
            flyweight.consumptionTermId(),
            flyweight.consumptionTermOffset(),
            positionBitsToShift,
            initialTermId);

        final long windowLength = flyweight.receiverWindowLength();
        final long receiverId = flyweight.receiverId();
        final long lastPositionPlusWindow = position + windowLength;
        final boolean eosFlagged = END_OF_STREAM_FLAG == (flyweight.flags() & END_OF_STREAM_FLAG);
        boolean isExisting = false;
        long minPosition = lastSetupSenderLimit(timeNs);

        Receiver[] receivers = this.receivers;

        for (final Receiver receiver : receivers)
        {
            if (matchesTag && receiverId == receiver.receiverId)
            {
                receiver.eosFlagged = eosFlagged;
                receiver.lastPosition = Math.max(position, receiver.lastPosition);
                receiver.lastPositionPlusWindow = lastPositionPlusWindow;
                receiver.timeOfLastStatusMessageNs = timeNs;
                isExisting = true;
            }

            minPosition = Math.min(minPosition, receiver.lastPositionPlusWindow);
        }

        if (!isExisting &&
            !eosFlagged &&
            matchesTag &&
            (0 == receivers.length || lastPositionPlusWindow >= minPosition - windowLength))
        {
            final Receiver receiver = new Receiver(
                receiverId, flyweight.sessionId(), flyweight.streamId(), position, lastPositionPlusWindow, timeNs);
            receivers = add(receivers, receiver);
            hasRequiredReceivers = receivers.length >= groupMinSize;
            this.receivers = receivers;
            minPosition = Math.min(minPosition, lastPositionPlusWindow);
            receiverAdded(receiver.receiverId, receiver.sessionId, receiver.streamId, channel, receivers.length);
            receiverCount.setRelease(receivers.length);
            lastSetupSenderLimit = -1;
        }

        if (receivers.length < groupMinSize)
        {
            return senderLimit;
        }
        else if (0 == receivers.length)
        {
            return Math.max(senderLimit, lastPositionPlusWindow);
        }
        else
        {
            return Math.max(senderLimit, minPosition);
        }
    }

    /**
     *  Process a status message triggering a setup to be sent.
     *
     * @param flyweight       over the status message receiver.
     * @param receiverAddress of the receiver.
     * @param timeNs          current time (in nanoseconds).
     * @param hasMatchingTag  if the status messages comes from a receiver with a tag matching the group.
     */
    protected void processSendSetupTrigger(
        final StatusMessageFlyweight flyweight,
        final InetSocketAddress receiverAddress,
        final long timeNs,
        final boolean hasMatchingTag)
    {
        if (!hasTaggedStatusMessageTriggeredSetup)
        {
            hasTaggedStatusMessageTriggeredSetup = hasMatchingTag;
        }
    }

    /**
     * Process an error frame from a downstream receiver.
     *
     * @param error             flyweight over the error frame.
     * @param receiverAddress   of the receiver.
     * @param timeNs            current time in nanoseconds.
     * @param hasMatchingTag    if the error message comes from a receiver with a tag matching the group.
     */
    protected void processError(
        final ErrorFlyweight error,
        final InetSocketAddress receiverAddress,
        final long timeNs,
        final boolean hasMatchingTag)
    {
        final long receiverId = error.receiverId();

        for (final Receiver receiver : receivers)
        {
            if (hasMatchingTag && receiverId == receiver.receiverId)
            {
                receiver.eosFlagged = true;
            }
        }
    }

    /**
     * Timeout after which an inactive receiver will be dropped.
     *
     * @return timeout after which an inactive receiver will be dropped.
     */
    protected final long receiverTimeoutNs()
    {
        return receiverTimeoutNs;
    }

    /**
     * Indicates if the flow control strategy has a group tag it is aware of for tracking membership.
     *
     * @return true if the flow control strategy has a group tag it is aware of for tracking membership.
     */
    protected final boolean hasGroupTag()
    {
        return isGroupTagAware;
    }

    /**
     * The tag used to identify members of the group.
     *
     * @return tag used to identify members of the group.
     */
    protected final long groupTag()
    {
        return groupTag;
    }

    /**
     * The minimum group size required for progress.
     *
     * @return minimum group size required for progress.
     */
    protected final int groupMinSize()
    {
        return groupMinSize;
    }

    static Receiver[] truncateReceivers(final Receiver[] receivers, final int removed)
    {
        final int length = receivers.length;
        final int newLength = length - removed;

        if (0 == newLength)
        {
            return EMPTY_RECEIVERS;
        }
        else
        {
            return Arrays.copyOf(receivers, newLength);
        }
    }

    private void parseUriParam(final String fcValue)
    {
        if (null != fcValue)
        {
            for (final String arg : fcValue.split(","))
            {
                if (arg.startsWith("t:"))
                {
                    receiverTimeoutNs = parseDuration("fc receiver timeout", arg.substring(2));
                }
                else if (arg.startsWith("g:"))
                {
                    final int groupMinSizeIndex = arg.indexOf('/');

                    if (2 != groupMinSizeIndex && isGroupTagAware)
                    {
                        final int lengthToParse = -1 == groupMinSizeIndex ? arg.length() - 2 : groupMinSizeIndex - 2;
                        groupTag = parseLongAscii(arg, 2, lengthToParse);
                    }

                    if (-1 != groupMinSizeIndex)
                    {
                        groupMinSize = parseIntAscii(
                            arg, groupMinSizeIndex + 1, arg.length() - (groupMinSizeIndex + 1));
                    }
                }
            }
        }
    }

    private void receiverAdded(
        final long receiverId, final int sessionId, final int streamId, final String channel, final int receiverCount)
    {
//        System.out.println("Receiver added: receiverCount=" + receiverCount +
//            ", receiverId=" + receiverId + ", sessionId=" + sessionId + ", streamId=" + streamId +
//            ", channel=" + channel);
    }

    private void receiverRemoved(
        final long receiverId, final int sessionId, final int streamId, final String channel, final int receiverCount)
    {
//        System.out.println("Receiver removed: receiverCount=" + receiverCount +
//            ", receiverId=" + receiverId + ", sessionId=" + sessionId + ", streamId=" + streamId +
//            ", channel=" + channel);
    }

    private long lastSetupSenderLimit(final long nowNs)
    {
        if (-1 != lastSetupSenderLimit)
        {
            if ((timeOfLastSetupNs + receiverTimeoutNs) - nowNs < 0)
            {
                lastSetupSenderLimit = -1;
            }
            else
            {
                return lastSetupSenderLimit;
            }
        }

        return Long.MAX_VALUE;
    }

    static final class Receiver
    {
        final int sessionId;
        final int streamId;
        final long receiverId;
        long lastPosition;
        long lastPositionPlusWindow;
        long timeOfLastStatusMessageNs;
        boolean eosFlagged;

        Receiver(
            final long receiverId,
            final int sessionId,
            final int streamId,
            final long lastPosition,
            final long lastPositionPlusWindow,
            final long timeNs)
        {
            this.receiverId = receiverId;
            this.sessionId = sessionId;
            this.streamId = streamId;
            this.lastPosition = lastPosition;
            this.lastPositionPlusWindow = lastPositionPlusWindow;
            this.timeOfLastStatusMessageNs = timeNs;
            this.eosFlagged = false;
        }
    }
}
