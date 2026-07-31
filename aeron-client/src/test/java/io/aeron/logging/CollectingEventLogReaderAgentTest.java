/*
 * Copyright 2014-2026 Real Logic Limited.
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
package io.aeron.logging;

import io.aeron.test.Tests;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import static io.aeron.logging.CborEncode.encode;
import static io.aeron.logging.CborEncode.encodeFooter;
import static io.aeron.logging.CborEncode.encodeHeader;
import static io.aeron.logging.CborEncode.length;
import static io.aeron.logging.CborEncode.lengthFooter;
import static io.aeron.logging.CborEncode.lengthHeader;
import static io.aeron.logging.CborUtils.IPV4_TAG;
import static io.aeron.logging.CborUtils.IPV6_TAG;
import static io.aeron.logging.CborUtils.NO_TAG;
import static io.aeron.logging.CborUtils.UINT8_TYPED_ARRAY_TAG;
import static io.aeron.logging.EventConfiguration.BUFFER_LENGTH_DEFAULT;
import static io.aeron.logging.EventConfiguration.BUFFER_LENGTH_PROP_NAME;
import static java.lang.Long.MAX_VALUE;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.agrona.SystemUtil.getSizeAsInt;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.junit.jupiter.api.Assertions.*;

class CollectingEventLogReaderAgentTest
{
    private final long fixedNanoTime = 123_456_789_987_654_321L;
    private final long fixedEpochMs = 1_700_000_000_000L;
    private final NanoClock fixedNanoClock = () -> fixedNanoTime;
    private final EpochClock fixedEpochClock = () -> fixedEpochMs;

    private String expectedLogStartMessage()
    {
        final StringBuilder sb = new StringBuilder();
        LogUtil.appendTimestamp(sb, fixedNanoTime);
        sb.append("log started ")
            .append(RollingFileEventWriter.DATE_TIME_FORMATTER.format(
                ZonedDateTime.ofInstant(Instant.ofEpochMilli(fixedEpochMs), ZoneId.systemDefault())))
            .append(PrintLoggerEventCallback.NEW_LINE);
        return sb.toString();
    }

    @Test
    void shouldWriteLogsToFileWhenTriggered(@TempDir final Path loggingDir) throws IOException
    {
        final ManyToOneRingBuffer ringBuffer = new ManyToOneRingBuffer(new UnsafeBuffer(allocateDirectAligned(
            getSizeAsInt(BUFFER_LENGTH_PROP_NAME, BUFFER_LENGTH_DEFAULT) + TRAILER_LENGTH, CACHE_LINE_LENGTH)));

        final String collectingFilename = loggingDir.resolve("collecting.log").toString();
        final String printingFilename = loggingDir.resolve("printing.log").toString();

        final CollectingEventLogReaderAgent collectingAgent = new CollectingEventLogReaderAgent(ringBuffer);
        final CborDecode decode = new CborDecode(List.of(
            new PrintLoggerEventCallback(printingFilename, MAX_VALUE, fixedNanoClock, fixedEpochClock)));

//        final int base = 1000;
        final EncodingState encodingState = new EncodingState();
        collectingAgent.setCollecting(true);

        for (int base = 1000; base < 1010; base++)
        {
            int length = 0;
            final TestEventCode eventCode = new TestEventCode(1, base + 2);
            length += lengthHeader(eventCode, base + 3);
            length += length("key1", NO_TAG, base + 4);
            length += length("key2", false);
            length += length("key3", UINT8_TYPED_ARRAY_TAG, new UnsafeBuffer(new byte[base + 5]));
            length += length("key4", IPV4_TAG, new UnsafeBuffer(new byte[4]));
            length += length("key5", IPV6_TAG, new UnsafeBuffer(new byte[16]));
            length += length("key6", NO_TAG, "Hello_" + (base + 6));
            length += lengthFooter();

            encodingState.reset(new ExpandableArrayBuffer(length), 0, length);

            encodeHeader(encodingState, eventCode, base + 3);
            encode(encodingState, "key1", NO_TAG, base + 4);
            encode(encodingState, "key2", false);
            encode(encodingState, "key3", UINT8_TYPED_ARRAY_TAG, new UnsafeBuffer(new byte[base + 5]), false);
            encode(encodingState, "key4", IPV4_TAG, new UnsafeBuffer(new byte[4]), false);
            encode(encodingState, "key5", IPV6_TAG, new UnsafeBuffer(new byte[16]), false);
            encode(encodingState, "key6", NO_TAG, "Hello_" + (base + 6), false);
            encodeFooter(encodingState);

            decode.onMessage(eventCode.toEventCodeId(), encodingState.buffer(), 0, encodingState.length());
            ringBuffer.write(eventCode.toEventCodeId(), encodingState.buffer(), 0, encodingState.length());
            while (0 == collectingAgent.doWork())
            {
                Tests.yield();
            }
        }

        collectingAgent.writeToFile(collectingFilename);
        collectingAgent.reset();

        final List<String> collected = Files.readAllLines(Path.of(collectingFilename));
        final List<String> printed = Files.readAllLines(Path.of(printingFilename));

        // TODO: Discuss whether the file header was originally printed for CollectingEventLogReaderAgent
        final String startMessage = expectedLogStartMessage();
        assertEquals(
            startMessage.substring(
                0,
                startMessage.length() - PrintLoggerEventCallback.NEW_LINE.length()),
            printed.get(0));
        assertEquals(collected, printed.subList(1, printed.size()));
    }
}
