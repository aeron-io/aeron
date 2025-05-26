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
package io.aeron.driver.status;

import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.UnsafeBufferPosition;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.status.CountersReader.MAX_LABEL_LENGTH;

/**
 * Allocates counters on a stream of messages.
 * <p>
 * Positions tracked in bytes include:
 * <ul>
 * <li>{@link PublisherPos}: Highest position on a {@link io.aeron.Publication} reached for offers and claims as an
 *     approximation which is sampled once per second.</li>
 * <li>{@link PublisherLimit}: Limit for flow controlling a {@link io.aeron.Publication} offers and claims.</li>
 * <li>{@link SenderPos}: Highest position on a {@link io.aeron.Publication} stream sent to the media.</li>
 * <li>{@link SenderLimit}: Limit for flow controlling a {@link io.aeron.driver.Sender} of a stream.</li>
 * <li>{@link ReceiverHwm}: Highest position observed by the {@link io.aeron.driver.Receiver} when rebuilding an
 *     {@link io.aeron.Image} of a stream.</li>
 * <li>{@link ReceiverPos}: Highest contiguous position rebuilt by the {@link io.aeron.driver.Receiver} on an
 *     {@link io.aeron.Image} of a stream.</li>
 * <li>{@link SubscriberPos}: Consumption position on an {@link io.aeron.Image} of a stream by an individual
 *     Subscriber.</li>
 * </ul>
 */
public class StreamCounter
{
    /**
     * Offset in the key metadata for the registration id of the counter.
     */
    public static final int REGISTRATION_ID_OFFSET = 0;

    /**
     * Offset in the key metadata for the session id of the counter.
     */
    public static final int SESSION_ID_OFFSET = REGISTRATION_ID_OFFSET + SIZE_OF_LONG;

    /**
     * Offset in the key metadata for the stream id of the counter.
     */
    public static final int STREAM_ID_OFFSET = SESSION_ID_OFFSET + SIZE_OF_INT;

    /**
     * Offset in the key metadata for the channel of the counter.
     */
    public static final int CHANNEL_OFFSET = STREAM_ID_OFFSET + SIZE_OF_INT;

    /**
     * The maximum length in bytes of the encoded channel identity.
     */
    public static final int MAX_CHANNEL_LENGTH = CountersReader.MAX_KEY_LENGTH - (CHANNEL_OFFSET + SIZE_OF_INT);

    /**
     * Allocate a counter for tracking a position on a stream of messages.
     *
     * @param tempBuffer      to be used for labels and key.
     * @param name            of the counter for the label.
     * @param typeId          of the counter for classification.
     * @param countersManager from which the underlying storage is allocated.
     * @param registrationId  to be associated with the counter.
     * @param sessionId       for the stream of messages.
     * @param streamId        for the stream of messages.
     * @param channel         for the stream of messages.
     * @return a new {@link UnsafeBufferPosition} for tracking the stream.
     */
    public static UnsafeBufferPosition allocate(
        final MutableDirectBuffer tempBuffer,
        final String name,
        final int typeId,
        final CountersManager countersManager,
        final long registrationId,
        final int sessionId,
        final int streamId,
        final String channel)
    {
        final int counterId = allocateCounterId(
            tempBuffer, name, typeId, countersManager, registrationId, sessionId, streamId, channel);

        return new UnsafeBufferPosition((UnsafeBuffer)countersManager.valuesBuffer(), counterId, countersManager);
    }

    /**
     * Allocate a counter id for tracking a position on a stream of messages.
     *
     * @param tempBuffer      to be used for labels and key.
     * @param name            of the counter for the label.
     * @param typeId          of the counter for classification.
     * @param countersManager from which the underlying storage is allocated.
     * @param registrationId  to be associated with the counter.
     * @param sessionId       for the stream of messages.
     * @param streamId        for the stream of messages.
     * @param channel         for the stream of messages.
     * @return the id of the allocated counter.
     */
    public static int allocateCounterId(
        final MutableDirectBuffer tempBuffer,
        final String name,
        final int typeId,
        final CountersManager countersManager,
        final long registrationId,
        final int sessionId,
        final int streamId,
        final String channel)
    {
        tempBuffer.putLong(REGISTRATION_ID_OFFSET, registrationId);
        tempBuffer.putInt(SESSION_ID_OFFSET, sessionId);
        tempBuffer.putInt(STREAM_ID_OFFSET, streamId);

        final int channelLength = tempBuffer.putStringWithoutLengthAscii(
            CHANNEL_OFFSET + SIZE_OF_INT, channel, 0, MAX_CHANNEL_LENGTH);
        tempBuffer.putInt(CHANNEL_OFFSET, channelLength);
        final int keyLength = CHANNEL_OFFSET + SIZE_OF_INT + channelLength;

        final int labelOffset = BitUtil.align(keyLength, SIZE_OF_INT);
        int labelLength = 0;
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, name);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, ": ");
        labelLength += tempBuffer.putLongAscii(labelOffset + labelLength, registrationId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, " ");
        labelLength += tempBuffer.putIntAscii(labelOffset + labelLength, sessionId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, " ");
        labelLength += tempBuffer.putIntAscii(labelOffset + labelLength, streamId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, " ");
        labelLength += tempBuffer.putStringWithoutLengthAscii(
            labelOffset + labelLength, channel, 0, MAX_LABEL_LENGTH - labelLength);

        final int counterId = countersManager.allocate(
            typeId, tempBuffer, 0, keyLength, tempBuffer, labelOffset, labelLength);

        countersManager.setCounterRegistrationId(counterId, registrationId);

        return counterId;
    }

    /**
     * Allocate a counter for tracking a position on a stream of messages.
     *
     * @param tempBuffer      to be used for labels and key.
     * @param name            of the counter for the label.
     * @param typeId          of the counter for classification.
     * @param countersManager from which the underlying storage is allocated.
     * @param registrationId  to be associated with the counter.
     * @param sessionId       for the stream of messages.
     * @param streamId        for the stream of messages.
     * @param channel         for the stream of messages.
     * @param joinPosition    for the label.
     * @return a new {@link UnsafeBufferPosition} for tracking the stream.
     */
    public static UnsafeBufferPosition allocate(
        final MutableDirectBuffer tempBuffer,
        final String name,
        final int typeId,
        final CountersManager countersManager,
        final long registrationId,
        final int sessionId,
        final int streamId,
        final String channel,
        final long joinPosition)
    {
        tempBuffer.putLong(REGISTRATION_ID_OFFSET, registrationId);
        tempBuffer.putInt(SESSION_ID_OFFSET, sessionId);
        tempBuffer.putInt(STREAM_ID_OFFSET, streamId);

        final int channelLength = tempBuffer.putStringWithoutLengthAscii(
            CHANNEL_OFFSET + SIZE_OF_INT, channel, 0, MAX_CHANNEL_LENGTH);
        tempBuffer.putInt(CHANNEL_OFFSET, channelLength);
        final int keyLength = CHANNEL_OFFSET + SIZE_OF_INT + channelLength;

        final int labelOffset = BitUtil.align(keyLength, SIZE_OF_INT);
        int labelLength = 0;
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, name);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, ": ");
        labelLength += tempBuffer.putLongAscii(labelOffset + labelLength, registrationId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, " ");
        labelLength += tempBuffer.putIntAscii(labelOffset + labelLength, sessionId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, " ");
        labelLength += tempBuffer.putIntAscii(labelOffset + labelLength, streamId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, " ");
        labelLength += tempBuffer.putStringWithoutLengthAscii(
            labelOffset + labelLength, channel, 0, MAX_LABEL_LENGTH - labelLength);

        if (labelLength < (MAX_LABEL_LENGTH - 20))
        {
            labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, " @");
            labelLength += tempBuffer.putLongAscii(labelOffset + labelLength, joinPosition);
        }

        final int counterId = countersManager.allocate(
            typeId, tempBuffer, 0, keyLength, tempBuffer, labelOffset, labelLength);

        countersManager.setCounterRegistrationId(counterId, registrationId);

        return new UnsafeBufferPosition((UnsafeBuffer)countersManager.valuesBuffer(), counterId, countersManager);
    }

    /**
     * Return the label name for a counter type identifier.
     *
     * @param typeId of the counter.
     * @return the label name as a String.
     */
    public static String labelName(final int typeId)
    {
        return switch (typeId)
        {
            case PublisherLimit.PUBLISHER_LIMIT_TYPE_ID -> PublisherLimit.NAME;
            case SenderPos.SENDER_POSITION_TYPE_ID -> SenderPos.NAME;
            case ReceiverHwm.RECEIVER_HWM_TYPE_ID -> ReceiverHwm.NAME;
            case SubscriberPos.SUBSCRIBER_POSITION_TYPE_ID -> SubscriberPos.NAME;
            case ReceiverPos.RECEIVER_POS_TYPE_ID -> ReceiverPos.NAME;
            case SenderLimit.SENDER_LIMIT_TYPE_ID -> SenderLimit.NAME;
            case PublisherPos.PUBLISHER_POS_TYPE_ID -> PublisherPos.NAME;
            case SenderBpe.SENDER_BPE_TYPE_ID -> SenderBpe.NAME;
            case SenderNaksReceived.TYPE_ID -> SenderNaksReceived.NAME;
            case ReceiverNaksSent.TYPE_ID -> ReceiverNaksSent.NAME;
            default -> "<unknown>";
        };
    }
}
