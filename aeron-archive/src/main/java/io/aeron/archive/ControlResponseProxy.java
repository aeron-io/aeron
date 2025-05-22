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
package io.aeron.archive;

import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveEvent;
import io.aeron.archive.codecs.*;
import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.archive.codecs.RecordingDescriptorEncoder.recordingIdEncodingOffset;
import static org.agrona.BitUtil.SIZE_OF_LONG;

class ControlResponseProxy
{
    private static final int SEND_ATTEMPTS = 3;
    private static final int MESSAGE_HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH;
    private static final int DESCRIPTOR_PREFIX_LENGTH = MESSAGE_HEADER_LENGTH + 2 * SIZE_OF_LONG;
    private static final int DESCRIPTOR_CONTENT_OFFSET =
        RecordingDescriptorHeaderDecoder.BLOCK_LENGTH + recordingIdEncodingOffset();

    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(1024);
    private final BufferClaim bufferClaim = new BufferClaim();

    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final ControlResponseEncoder responseEncoder = new ControlResponseEncoder();
    private final RecordingDescriptorEncoder recordingDescriptorEncoder = new RecordingDescriptorEncoder();
    private final RecordingSubscriptionDescriptorEncoder recordingSubscriptionDescriptorEncoder =
        new RecordingSubscriptionDescriptorEncoder();
    private final RecordingSignalEventEncoder recordingSignalEventEncoder = new RecordingSignalEventEncoder();
    private final ChallengeEncoder challengeEncoder = new ChallengeEncoder();
    private final PingEncoder pingEncoder = new PingEncoder();

    boolean sendDescriptor(
        final long controlSessionId,
        final long correlationId,
        final UnsafeBuffer descriptorBuffer,
        final ControlSession session)
    {
        final int messageLength = Catalog.descriptorLength(descriptorBuffer) + MESSAGE_HEADER_LENGTH;
        final int contentLength = messageLength - recordingIdEncodingOffset() - MESSAGE_HEADER_LENGTH;

        recordingDescriptorEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId);

        int attempts = SEND_ATTEMPTS;
        final Publication publication = session.controlPublication();
        do
        {
            final long position = publication.offer(
                buffer,
                0,
                DESCRIPTOR_PREFIX_LENGTH,
                descriptorBuffer,
                DESCRIPTOR_CONTENT_OFFSET,
                contentLength);
            if (position > 0)
            {
                return true;
            }

            checkResult(session, position);
        }
        while (--attempts > 0);

        return false;
    }

    boolean sendSubscriptionDescriptor(
        final long controlSessionId,
        final long correlationId,
        final Subscription subscription,
        final ControlSession session)
    {
        recordingSubscriptionDescriptorEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .subscriptionId(subscription.registrationId())
            .streamId(subscription.streamId())
            .strippedChannel(subscription.channel());

        final int length = MESSAGE_HEADER_LENGTH + recordingSubscriptionDescriptorEncoder.encodedLength();

        return send(session, buffer, length);
    }

    boolean sendResponse(
        final long controlSessionId,
        final long correlationId,
        final long relevantId,
        final ControlResponseCode code,
        final String errorMessage,
        final ControlSession session)
    {
        responseEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .relevantId(relevantId)
            .code(code)
            .version(AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION)
            .errorMessage(errorMessage);

        final int length = MESSAGE_HEADER_LENGTH + responseEncoder.encodedLength();
        final int offset = 0;
        int attempts = SEND_ATTEMPTS;
        do
        {
            final long position = session.controlPublication().offer(buffer, offset, length);
            if (position > 0)
            {
                logSendResponse(buffer, offset, length);
                return true;
            }

            checkResult(session, position);
        }
        while (--attempts > 0);

        return false;
    }

    boolean sendChallenge(
        final long controlSessionId,
        final long correlationId,
        final byte[] encodedChallenge,
        final ControlSession session)
    {
        challengeEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .putEncodedChallenge(encodedChallenge, 0, encodedChallenge.length);

        return send(session, buffer, MESSAGE_HEADER_LENGTH + challengeEncoder.encodedLength());
    }

    boolean sendSignal(
        final long controlSessionId,
        final long correlationId,
        final long recordingId,
        final long subscriptionId,
        final long position,
        final RecordingSignal recordingSignal,
        final Publication controlPublication)
    {
        final int length = MESSAGE_HEADER_LENGTH + RecordingSignalEventEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = controlPublication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                final MutableDirectBuffer buffer = bufferClaim.buffer();
                final int offset = bufferClaim.offset();

                recordingSignalEventEncoder
                    .wrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
                    .controlSessionId(controlSessionId)
                    .correlationId(correlationId)
                    .recordingId(recordingId)
                    .subscriptionId(subscriptionId)
                    .position(position)
                    .signal(recordingSignal);

                bufferClaim.commit();

                logSendSignal(buffer, offset, length);
                return true;
            }
        }
        while (--attempts > 0);

        return false;
    }

    public boolean sendPing(final long controlSessionId, final Publication controlPublication)
    {
        final int length = MESSAGE_HEADER_LENGTH + PingEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = controlPublication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                final MutableDirectBuffer buffer = bufferClaim.buffer();
                final int offset = bufferClaim.offset();
                pingEncoder.wrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
                    .controlSessionId(controlSessionId);
                bufferClaim.commit();
                return true;
            }
        }
        while (--attempts > 0);

        return false;
    }

    private boolean send(final ControlSession session, final DirectBuffer buffer, final int length)
    {
        int attempts = SEND_ATTEMPTS;
        do
        {
            final long position = session.controlPublication().offer(buffer, 0, length);
            if (position > 0)
            {
                return true;
            }

            checkResult(session, position);
        }
        while (--attempts > 0);

        return false;
    }

    private static void checkResult(final ControlSession session, final long result)
    {
        if (result == Publication.NOT_CONNECTED)
        {
            session.abort("response publication is not connected");
            throw new ArchiveEvent("response publication is not connected: " + session);
        }

        if (result == Publication.CLOSED)
        {
            session.abort("response publication is closed");
            throw new ArchiveEvent("response publication is closed: " + session, AeronException.Category.ERROR);
        }

        if (result == Publication.MAX_POSITION_EXCEEDED)
        {
            session.abort("response publication at max position");
            throw new ArchiveEvent("response publication at max position: " + session, AeronException.Category.ERROR);
        }
    }

    private void logSendResponse(final DirectBuffer buffer, final int offset, final int length)
    {
    }

    private void logSendSignal(final DirectBuffer buffer, final int offset, final int length)
    {
    }
}
