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
package io.aeron.logging;

import io.aeron.test.CapturingPrintStream;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static io.aeron.logging.CborUtils.IPV4_TAG;
import static io.aeron.logging.CborUtils.IPV6_TAG;
import static io.aeron.logging.CborUtils.NO_TAG;
import static org.agrona.PrintBufferUtil.appendPrettyHexDump;
import static org.junit.jupiter.api.Assertions.*;

class PrintLoggerEventCallbackTest
{
    @Test
    void shouldWriteBasicValuesToStream()
    {
        final CapturingPrintStream capturingPrintStream = new CapturingPrintStream();
        final PrintLoggerEventCallback printLoggerEventCallback = new PrintLoggerEventCallback(
            capturingPrintStream.resetAndGetPrintStream());

        final int typeCode = EventCodeType.DRIVER.getTypeCode();
        final int eventCode = 1234;
        final String eventName = "SOME_LOG";
        final long timestamp = 2827937432L;

        final String key1 = "key1";
        final long value1 = 987234;

        final String key2 = "key2";
        final String value2 = "value2";

        final String key3 = "key3";
        final boolean value3 = true;

        final String key4 = "key4";
        final byte[] value4 = new byte[256];
        Arrays.fill(value4, (byte)'x');

        printLoggerEventCallback.onHeader(typeCode, eventCode, eventName, timestamp);
        printLoggerEventCallback.onValue(key1, NO_TAG, value1);
        printLoggerEventCallback.onValue(key2, NO_TAG, value2);
        printLoggerEventCallback.onValue(key3, NO_TAG, value3);
        printLoggerEventCallback.onValue(key4, NO_TAG, new UnsafeBuffer(value4));
        printLoggerEventCallback.onFooter(false);

        final StringBuilder sb = new StringBuilder();

        LogUtil.appendTimestamp(sb, timestamp);
        sb.append(EventCodeType.DRIVER.name()).append(": ");
        sb.append(eventName).append(" ");
        sb.append(key1).append("=").append(value1).append(" ");
        sb.append(key2).append("=\"").append(value2).append("\" ");
        sb.append(key3).append("=").append(value3).append(" ");
        sb.append(key4).append("=\n");
        appendPrettyHexDump(sb, new UnsafeBuffer(value4));
        sb.append("\n");

        final String expected = sb.toString();

        final String content = capturingPrintStream.flushAndGetContent();
        assertEquals(content, expected);
    }

    @Test
    void shouldWriteIpAddressesToStream()
    {
        final CapturingPrintStream capturingPrintStream = new CapturingPrintStream();
        final PrintLoggerEventCallback printLoggerEventCallback = new PrintLoggerEventCallback(
            capturingPrintStream.resetAndGetPrintStream());

        final int typeCode = EventCodeType.DRIVER.getTypeCode();
        final int eventCode = 1234;
        final String eventName = "SOME_LOG";
        final long timestamp = 2827937432L;

        final String key1 = "key1";
        final byte[] value1 = {(byte)192, (byte)168, 0, 10};

        final String key2 = "key2";
        // fe80::54d3:4122:e738:a862
        final byte[] value2 = {
            (byte)0xfe, (byte)0x80, (byte)0x00, (byte)0x00,
            (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00,
            (byte)0x54, (byte)0xd3, (byte)0x41, (byte)0x22,
            (byte)0xe7, (byte)0x38, (byte)0xa8, (byte)0x62
        };

        printLoggerEventCallback.onHeader(typeCode, eventCode, eventName, timestamp);
        printLoggerEventCallback.onValue(key1, IPV4_TAG, new UnsafeBuffer(value1));
        printLoggerEventCallback.onValue(key2, IPV6_TAG, new UnsafeBuffer(value2));
        printLoggerEventCallback.onFooter(false);

        final StringBuilder sb = new StringBuilder();

        LogUtil.appendTimestamp(sb, timestamp);
        sb.append(EventCodeType.DRIVER.name()).append(": ");
        sb.append(eventName).append(" ");
        sb.append(key1).append("=").append("192.168.0.10").append(" ");
        sb.append(key2).append("=").append("[fe80::54d3:4122:e738:a862]");
        sb.append("\n");

        assertEquals(capturingPrintStream.flushAndGetContent(), sb.toString());
    }
}