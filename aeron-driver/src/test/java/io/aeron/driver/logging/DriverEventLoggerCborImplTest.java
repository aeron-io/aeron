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
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static io.aeron.logging.CborUtils.AERON_DRIVER_ADMIN_TAG;
import static io.aeron.logging.CborUtils.AERON_PROTOCOL_TAG;
import static io.aeron.logging.CborUtils.IPV4_TAG;
import static io.aeron.logging.CborUtils.IPV6_TAG;
import static io.aeron.logging.CborUtils.NO_TAG;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class DriverEventLoggerCborImplTest
{
    private final ManyToOneRingBuffer ringBuffer = new ManyToOneRingBuffer(
        new UnsafeBuffer(BufferUtil.allocateDirectAligned(64 * 1024 + TRAILER_LENGTH, CACHE_LINE_LENGTH)));

    private final LoggerEventCallback mockLoggingCallback = mock(LoggerEventCallback.class);
    private final CborDecode cborDecode = new CborDecode(List.of(new ProxyLoggerEventCallback(mockLoggingCallback)));
    private final DriverEventLogger logger = new CborDriverEventLogger(ringBuffer);

    private void drain()
    {
        while (0 == ringBuffer.read(cborDecode))
        {
            Tests.yield();
        }
    }

    @Test
    void logUsesTheOverriddenTagAndTheBufferViewSubRange()
    {
        final byte[] backing = new byte[1024];
        Arrays.fill(backing, (byte)0xAA);
        final byte[] subRange = new byte[16];
        Arrays.fill(subRange, (byte)0xAA);
        final UnsafeBuffer buffer = new UnsafeBuffer(backing);

        logger.log(DriverEventCode.CMD_IN_ADD_PUBLICATION, buffer, 100, 16);

        drain();

        final InOrder inOrder = Mockito.inOrder(mockLoggingCallback);
        inOrder.verify(mockLoggingCallback).onHeader(
            eq(EventCodeType.DRIVER.getTypeCode()),
            eq(DriverEventCode.CMD_IN_ADD_PUBLICATION.id()),
            eq(DriverEventCode.CMD_IN_ADD_PUBLICATION.name()),
            anyLong());
        inOrder.verify(mockLoggingCallback).onValue("buffer", AERON_DRIVER_ADMIN_TAG, new UnsafeBuffer(subRange));
        inOrder.verify(mockLoggingCallback).onFooter(false);
    }

    @Test
    void logDoesNotTruncateEvenWhenOversizedSinceAllowTruncateIsNotSet()
    {
        final byte[] backing = new byte[10_000];
        Arrays.fill(backing, (byte)0x11);
        final UnsafeBuffer buffer = new UnsafeBuffer(backing);

        logger.log(DriverEventCode.CMD_IN_ADD_PUBLICATION, buffer, 0, backing.length);

        drain();

        verify(mockLoggingCallback, never()).onValue(
            eq("buffer"), eq(AERON_DRIVER_ADMIN_TAG), org.mockito.ArgumentMatchers.any(org.agrona.DirectBuffer.class));
        verify(mockLoggingCallback).onFooter(true);
    }

    @Test
    void logFrameInUsesProtocolTagAllowsTruncationAndInfersIpv4AddressTag() throws UnknownHostException
    {
        final byte[] backing = new byte[64];
        Arrays.fill(backing, (byte)0xF0);
        final UnsafeBuffer buffer = new UnsafeBuffer(backing);
        final InetSocketAddress address = new InetSocketAddress(InetAddress.getByName("192.168.1.1"), 1234);

        logger.logFrameIn(address.getAddress(), address.getPort(), buffer, 0, backing.length);

        drain();

        final InOrder inOrder = Mockito.inOrder(mockLoggingCallback);
        inOrder.verify(mockLoggingCallback).onHeader(
            eq(EventCodeType.DRIVER.getTypeCode()),
            eq(DriverEventCode.FRAME_IN.id()),
            eq(DriverEventCode.FRAME_IN.name()),
            anyLong());
        inOrder.verify(mockLoggingCallback).onValue(
            "dstAddress", IPV4_TAG, new UnsafeBuffer(address.getAddress().getAddress()));
        inOrder.verify(mockLoggingCallback).onValue("buffer", AERON_PROTOCOL_TAG, new UnsafeBuffer(backing));
        inOrder.verify(mockLoggingCallback).onFooter(false);
    }

    @Test
    void logFrameInInfersIpv6AddressTag() throws UnknownHostException
    {
        final byte[] backing = new byte[16];
        final UnsafeBuffer buffer = new UnsafeBuffer(backing);
        final InetSocketAddress address = new InetSocketAddress(InetAddress.getByName("2001:db8::1"), 1234);

        logger.logFrameIn(address.getAddress(), address.getPort(), buffer, 0, backing.length);

        drain();

        verify(mockLoggingCallback).onValue(
            eq("dstAddress"), eq(IPV6_TAG), eq(new UnsafeBuffer(address.getAddress().getAddress())));
    }

    @Test
    void logFrameOutWrapsTheBareByteBufferByItsOwnPositionAndRemaining()
    {
        final byte[] testBytes = new byte[1024];
        Arrays.fill(testBytes, (byte)0xF0);
        final ByteBuffer byteBuffer = ByteBuffer.allocate(2048);
        byteBuffer.put(testBytes);
        byteBuffer.flip();
        final InetSocketAddress address = new InetSocketAddress("192.168.1.1", 1234);

        logger.logFrameOut(address.getAddress(), address.getPort(), byteBuffer);

        drain();

        final InOrder inOrder = Mockito.inOrder(mockLoggingCallback);
        inOrder.verify(mockLoggingCallback).onHeader(
            eq(EventCodeType.DRIVER.getTypeCode()),
            eq(DriverEventCode.FRAME_OUT.id()),
            eq(DriverEventCode.FRAME_OUT.name()),
            anyLong());
        inOrder.verify(mockLoggingCallback).onValue(
            "dstAddress", IPV4_TAG, new UnsafeBuffer(address.getAddress().getAddress()));
        inOrder.verify(mockLoggingCallback).onValue("buffer", AERON_PROTOCOL_TAG, new UnsafeBuffer(testBytes));
        inOrder.verify(mockLoggingCallback).onFooter(false);
    }

    @Test
    void logResolveHandlesANullAddressAndEncodesTheBooleanWithNoTag()
    {
        logger.logResolve("DefaultNameResolver", 42L, "host.example.com", true, null);

        drain();

        final InOrder inOrder = Mockito.inOrder(mockLoggingCallback);
        inOrder.verify(mockLoggingCallback).onHeader(
            eq(EventCodeType.DRIVER.getTypeCode()),
            eq(DriverEventCode.NAME_RESOLUTION_RESOLVE.id()),
            eq(DriverEventCode.NAME_RESOLUTION_RESOLVE.name()),
            anyLong());
        inOrder.verify(mockLoggingCallback).onValue("resolverName", NO_TAG, "DefaultNameResolver");
        inOrder.verify(mockLoggingCallback).onValue("durationNs", NO_TAG, 42L);
        inOrder.verify(mockLoggingCallback).onValue("name", NO_TAG, "host.example.com");
        inOrder.verify(mockLoggingCallback).onValue("isReResolution", NO_TAG, true);
        // CBOR represents a null value as a generic "null" simple value regardless of the field's Java type,
        // so the decode side always routes it through the CharSequence overload, never the DirectBuffer one.
        inOrder.verify(mockLoggingCallback).onValue(eq("address"), eq(NO_TAG), eq((CharSequence)null));
        inOrder.verify(mockLoggingCallback).onFooter(false);
    }

    @Test
    void logResolveEncodesANonNullAddress() throws UnknownHostException
    {
        final InetAddress address = InetAddress.getByName("10.0.0.1");

        logger.logResolve("DefaultNameResolver", 42L, "host.example.com", false, address);

        drain();

        verify(mockLoggingCallback).onValue("address", IPV4_TAG, new UnsafeBuffer(address.getAddress()));
    }
}
