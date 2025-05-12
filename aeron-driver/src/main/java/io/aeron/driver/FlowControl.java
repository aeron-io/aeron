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
import io.aeron.protocol.ErrorFlyweight;
import io.aeron.protocol.SetupFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.concurrent.status.CountersManager;

import java.net.InetSocketAddress;

/**
 * Strategy for applying flow control to the {@link Sender} on each stream.
 */
public interface FlowControl extends AutoCloseable
{
    /**
     * Calculates a retransmission length by clamping to a min of <code>resendLength</code>,
     * <code>termBufferLength - termOffset</code> and <code>retransmitReceiverWindowMultiple *
     * ${configured initial window length}</code>.
     *
     * @param resendLength                     requested length of a retransmit
     * @param termBufferLength                 length of the current term.
     * @param termOffset                       offset within the term.
     * @param retransmitReceiverWindowMultiple multiplier to the receiver window length.
     * @return the clamped retransmit length
     */
    static int calculateRetransmissionLength(
        final int resendLength,
        final int termBufferLength,
        final int termOffset,
        final int retransmitReceiverWindowMultiple)
    {
        final int lengthToEndOfTerm = termBufferLength - termOffset;
        final int estimatedRetransmitLength = Configuration.receiverWindowLength(
            termBufferLength, Configuration.INITIAL_WINDOW_LENGTH_DEFAULT) * retransmitReceiverWindowMultiple;

        return (lengthToEndOfTerm < estimatedRetransmitLength) ?
            Math.min(lengthToEndOfTerm, resendLength) : Math.min(estimatedRetransmitLength, resendLength);
    }

    /**
     * Determines the retransmit receiver window multiple to use. If a value is specified in the
     * channel URI, this will be used. Otherwise, the supplied default will be used.
     *
     * @param udpChannel for the stream.
     * @param defaultRetransmitReceiverWindowMultiple window multiple to use when one is not set in the URI.
     * @return receiver window multiple.
     */
    static int retransmitReceiverWindowMultiple(
        UdpChannel udpChannel, int defaultRetransmitReceiverWindowMultiple)
    {
        final String fcValue = udpChannel.channelUri().get(CommonContext.FLOW_CONTROL_PARAM_NAME);
        if (fcValue != null)
        {
            for (final String arg : fcValue.split(","))
            {
                if (arg.startsWith("rrwm:"))
                {
                    final int rrwm = Integer.parseInt(arg.substring("rrwm:".length()));
                    if (rrwm <= 0)
                    {
                        throw new IllegalArgumentException("Invalid retransmit receiver window multiple: " + rrwm);
                    }
                    return rrwm;
                }
            }
        }
        return defaultRetransmitReceiverWindowMultiple;
    }

    /**
     * Update the sender flow control strategy based on a status message from the receiver.
     *
     * @param flyweight           over the status message received.
     * @param receiverAddress     of the receiver.
     * @param senderLimit         the current sender position limit.
     * @param initialTermId       for the term buffers.
     * @param positionBitsToShift in use for the length of each term buffer.
     * @param timeNs              current time (in nanoseconds).
     * @return the new position limit to be employed by the sender.
     */
    long onStatusMessage(
        StatusMessageFlyweight flyweight,
        InetSocketAddress receiverAddress,
        long senderLimit,
        int initialTermId,
        int positionBitsToShift,
        long timeNs);

    /**
     * Update the sender flow control strategy based on a Status Message received triggering a setup to be sent.
     *
     * @param flyweight       over the Status Message received
     * @param receiverAddress of the receiver.
     * @param timeNs          current time (in nanoseconds).
     */
    void onTriggerSendSetup(
        StatusMessageFlyweight flyweight,
        InetSocketAddress receiverAddress,
        long timeNs);

    /**
     * Update the sender flow control strategy based on an elicited setup message being sent out.
     *
     * @param flyweight           over the setup to be sent.
     * @param senderLimit         for the current sender position.
     * @param senderPosition      which has been sent.
     * @param positionBitsToShift in use for the length of each term buffer.
     * @param timeNs              current time in nanoseconds.
     * @return the new position limit to be employed by the sender.
     */
    long onSetup(
        SetupFlyweight flyweight,
        long senderLimit,
        long senderPosition,
        int positionBitsToShift,
        long timeNs);

    /**
     * Update the sender flow control strategy if an error comes from one of the receivers.
     *
     * @param errorFlyweight    over the error received.
     * @param receiverAddress   the address of the receiver.
     * @param timeNs            current time in nanoseconds
     */
    void onError(ErrorFlyweight errorFlyweight, InetSocketAddress receiverAddress, long timeNs);

    /**
     * Initialize the flow control strategy for a stream.
     *
     * @param context          to allow access to media driver configuration
     * @param countersManager  to use for any counters in use by the strategy
     * @param streamId         for the stream.
     * @param sessionId        for the stream.
     * @param registrationId   for the stream.
     * @param udpChannel       for the stream.
     * @param initialTermId    at which the stream started.
     * @param termBufferLength to use as the length of each term buffer.
     */
    void initialize(
        MediaDriver.Context context,
        CountersManager countersManager,
        UdpChannel udpChannel,
        int streamId,
        int sessionId,
        long registrationId,
        int initialTermId,
        int termBufferLength);

    /**
     * Perform any maintenance needed by the flow control strategy and return current sender limit position.
     *
     * @param timeNs         current time in nanoseconds.
     * @param senderLimit    for the current sender position.
     * @param senderPosition which has been sent.
     * @param isEos          is this end-of-stream for the sender.
     * @return the position limit to be employed by the sender.
     */
    long onIdle(long timeNs, long senderLimit, long senderPosition, boolean isEos);

    /**
     * Has the flow control strategy its required group of receivers to be considered connected? The
     * result of this feeds into the determination of if a publication is connected.
     *
     * @return true if the required group of receivers is connected, otherwise false.
     */
    boolean hasRequiredReceivers();

    /**
     * The maximum window length allowed to retransmit per NAK. Will limit it by an estimate of the window limit and to
     * the end of the current term.
     *
     * @param termOffset       of the NAK.
     * @param resendLength     of the NAK.
     * @param termBufferLength of the publication.
     * @param mtuLength        of the publication.
     * @return the maximum window length allowed to retransmit per NAK.
     */
    int maxRetransmissionLength(int termOffset, int resendLength, int termBufferLength, int mtuLength);

    /**
     * {@inheritDoc}
     */
    void close();
}
