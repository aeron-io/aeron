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
package io.aeron.archive.logging;

import io.aeron.test.logging.GenericLoggerEventVerifier;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.junit.jupiter.api.Test;

import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

/**
 * Reflectively drives every {@link ArchiveEventLogger} method with a range of nominal, boundary, and {@code null}
 * values and verifies (via {@link io.aeron.logging.CborDecode}) that they all survive the CBOR round trip.
 */
class GenericLoggingTest
{
    @Test
    void shouldRoundTripEveryLoggerMethod()
    {
        final ManyToOneRingBuffer ringBuffer = new ManyToOneRingBuffer(
            new UnsafeBuffer(BufferUtil.allocateDirectAligned(64 * 1024 + TRAILER_LENGTH, CACHE_LINE_LENGTH)));
        final ArchiveEventLogger logger = new CborArchiveEventLogger(ringBuffer);

        GenericLoggerEventVerifier.verifyAllLogMethods(ArchiveEventLogger.class, logger, ringBuffer);
    }
}
