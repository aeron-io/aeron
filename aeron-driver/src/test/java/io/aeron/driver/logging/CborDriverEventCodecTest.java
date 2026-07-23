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
package io.aeron.driver.logging;

import io.aeron.logging.CborDecode;
import io.aeron.logging.EventCodeType;
import io.aeron.logging.LoggerEventCallback;
import io.aeron.test.Tests;
import io.aeron.test.logging.ProxyLoggerEventCallback;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static io.aeron.logging.CborUtils.IPV4_TAG;
import static io.aeron.logging.CborUtils.UINT8_TYPED_ARRAY_TAG;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class CborDriverEventCodecTest
{
    private final ManyToOneRingBuffer ringBuffer = new ManyToOneRingBuffer(
        new UnsafeBuffer(BufferUtil.allocateDirectAligned(64 * 1024 + TRAILER_LENGTH, CACHE_LINE_LENGTH)));


    @Test
    void encodeDecodeLogFrameOut()
    {
        final InetSocketAddress inetSocketAddress = new InetSocketAddress("192.168.1.1", 1234);
        final byte[] testBytes = new byte[1024];
        Arrays.fill(testBytes, (byte)0xF0);
        final ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(64 * 1024 + TRAILER_LENGTH, CACHE_LINE_LENGTH);
        byteBuffer.put(testBytes);
        byteBuffer.flip();
        final LoggerEventCallback mockLoggingCallback = mock(LoggerEventCallback.class);
        final CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(mockLoggingCallback)));
        final CborDriverEventLogger cborDriverEventLogger = new CborDriverEventLogger(ringBuffer);

        cborDriverEventLogger.logFrameOut(byteBuffer, inetSocketAddress);

        while (0 == ringBuffer.read(cborDecode))
        {
            Tests.yield();
        }

        verify(mockLoggingCallback).onHeader(
            eq(EventCodeType.DRIVER.getTypeCode()),
            eq(DriverEventCode.FRAME_OUT.id()),
            eq(DriverEventCode.FRAME_OUT.name()),
            anyLong());
        verify(mockLoggingCallback).onValue("buffer", UINT8_TYPED_ARRAY_TAG, new UnsafeBuffer(testBytes));
        verify(mockLoggingCallback).onValue("dstAddress", IPV4_TAG,
            new UnsafeBuffer(inetSocketAddress.getAddress().getAddress()));
        verify(mockLoggingCallback).onFooter(false);
    }
}