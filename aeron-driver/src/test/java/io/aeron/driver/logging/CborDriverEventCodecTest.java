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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;

import static io.aeron.logging.CborUtils.IPV4_TAG;
import static io.aeron.logging.CborUtils.IPV6_TAG;
import static io.aeron.logging.CborUtils.UINT8_TYPED_ARRAY_TAG;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class CborDriverEventCodecTest
{
    private final ManyToOneRingBuffer ringBuffer = new ManyToOneRingBuffer(
        new UnsafeBuffer(BufferUtil.allocateDirectAligned(64 * 1024 + TRAILER_LENGTH, CACHE_LINE_LENGTH)));


    static Stream<Arguments> ipAddresses()
    {
        try
        {
            return Stream.of(
                Arguments.arguments(IPV4_TAG, Inet4Address.getByName("192.168.0.10")),
                Arguments.arguments(IPV6_TAG, Inet6Address.getByName("fe80::54d3:4122:e738:a862"))
            );
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    @ParameterizedTest
    @MethodSource("ipAddresses")
    void encodeDecodeLogFrameOut(final long tag, final InetAddress address)
    {
        final InetSocketAddress inetSocketAddress = new InetSocketAddress(address, 1234);
        final byte[] testBytes = new byte[1024];

        final LoggerEventCallback mockLoggingCallback = mock(LoggerEventCallback.class);
        final CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(mockLoggingCallback)));
        final CborDriverEventLogger cborDriverEventLogger = new CborDriverEventLogger(ringBuffer);

        cborDriverEventLogger.logFrameOut(ByteBuffer.wrap(testBytes), inetSocketAddress);

        while (0 == ringBuffer.read(cborDecode))
        {
            Tests.yield();
        }

        final InOrder inOrder = inOrder(mockLoggingCallback);

        inOrder.verify(mockLoggingCallback).onHeader(
            eq(EventCodeType.DRIVER.getTypeCode()),
            eq(DriverEventCode.FRAME_OUT.id()),
            eq(DriverEventCode.FRAME_OUT.name()),
            anyLong());
        inOrder.verify(mockLoggingCallback).onValue("buffer", UINT8_TYPED_ARRAY_TAG, new UnsafeBuffer(testBytes));
        inOrder.verify(mockLoggingCallback).onValue(
            "dstAddress", tag, new UnsafeBuffer(inetSocketAddress.getAddress().getAddress()));
        inOrder.verify(mockLoggingCallback).onFooter(false);
    }
}