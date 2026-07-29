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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;

import static io.aeron.logging.CborUtils.IPV4_TAG;
import static io.aeron.logging.CborUtils.IPV6_TAG;
import static io.aeron.logging.CborUtils.NO_TAG;
import static org.agrona.PrintBufferUtil.appendPrettyHexDump;
import static org.junit.jupiter.api.Assertions.*;

class PrintLoggerEventCallbackTest
{

    @Test
    void shouldWriteBasicValuesToFile(@TempDir final Path testPath) throws IOException
    {
        final Path testFilePath = testPath.resolve("test.log");
        final PrintLoggerEventCallback printLoggerEventCallback = new PrintLoggerEventCallback(
            testFilePath.toString(),
            Long.MAX_VALUE);
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
        assertEquals(expected, Files.readString(testFilePath));
    }

    @Test
    void shouldWriteBasicValuesToRollingFilesWhenExceededFileSizeLimit(@TempDir final Path testPath) throws IOException
    {
        final int expectedRollingFileCount = 3;
        final Path testFilePath = testPath.resolve("test.log");
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
        final int fileSizeLimit = expected.length() - 1;

        final PrintLoggerEventCallback printLoggerEventCallback = new PrintLoggerEventCallback(
            testFilePath.toString(),
            fileSizeLimit);

        for (int i = 1; i <= expectedRollingFileCount; i++)
        {
            printLoggerEventCallback.onHeader(typeCode, eventCode, eventName, timestamp);
            printLoggerEventCallback.onValue(key1, NO_TAG, value1);
            printLoggerEventCallback.onValue(key2, NO_TAG, value2);
            printLoggerEventCallback.onValue(key3, NO_TAG, value3);
            printLoggerEventCallback.onValue(key4, NO_TAG, new UnsafeBuffer(value4));
            printLoggerEventCallback.onFooter(false);
        }
        for (int i = 1; i <= expectedRollingFileCount; i++)
        {
            final Path rollingFilePath = testFilePath.resolveSibling(testFilePath.getFileName() + "." + i);
            assertEquals(expected, Files.readString(rollingFilePath));
        }

        assertEquals(0, Files.size(testFilePath));

        try (Stream<Path> paths = Files.list(testPath))
        {
            assertEquals(expectedRollingFileCount + 1, paths.filter(Files::isRegularFile).count());
        }
    }

    @Test
    void shouldRollExactlyAtSizeLimitBoundary(@TempDir final Path testPath) throws IOException
    {
        final Path testFilePath = testPath.resolve("test.log");
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
        final int fileSizeLimit = expected.length();

        final PrintLoggerEventCallback printLoggerEventCallback = new PrintLoggerEventCallback(
            testFilePath.toString(),
            fileSizeLimit);

        printLoggerEventCallback.onHeader(typeCode, eventCode, eventName, timestamp);
        printLoggerEventCallback.onValue(key1, NO_TAG, value1);
        printLoggerEventCallback.onValue(key2, NO_TAG, value2);
        printLoggerEventCallback.onValue(key3, NO_TAG, value3);
        printLoggerEventCallback.onValue(key4, NO_TAG, new UnsafeBuffer(value4));
        printLoggerEventCallback.onFooter(false);

        final Path rolledFilePath = testFilePath.resolveSibling(testFilePath.getFileName() + ".1");
        assertEquals(expected, Files.readString(rolledFilePath));
        assertEquals(0, Files.size(testFilePath));
    }

    @Test
    void shouldWriteMultipleEntriesInSingleFileIfFitBeforeRolling(@TempDir final Path testPath) throws IOException
    {
        final Path testFilePath = testPath.resolve("test.log");
        final int typeCode = EventCodeType.DRIVER.getTypeCode();
        final int eventCode = 1234;
        final String eventName = "SOME_LOG";
        final long timestamp = 2827937432L;

        final String key1 = "key1";
        final long value1Base = 987234;

        final String key2 = "key2";
        final String value2 = "value2";

        final String key3 = "key3";
        final boolean value3 = true;

        final String key4 = "key4";
        final byte[] value4 = new byte[256];
        Arrays.fill(value4, (byte)'x');

        final String[] entries = new String[4];
        for (int i = 0; i < entries.length; i++)
        {
            final StringBuilder sb = new StringBuilder();

            LogUtil.appendTimestamp(sb, timestamp);
            sb.append(EventCodeType.DRIVER.name()).append(": ");
            sb.append(eventName).append(" ");
            sb.append(key1).append("=").append(value1Base + i).append(" ");
            sb.append(key2).append("=\"").append(value2).append("\" ");
            sb.append(key3).append("=").append(value3).append(" ");
            sb.append(key4).append("=\n");
            appendPrettyHexDump(sb, new UnsafeBuffer(value4));
            sb.append("\n");

            entries[i] = sb.toString();
        }

        final int fileSizeLimit = (2 * entries[0].length()) - 1;

        final PrintLoggerEventCallback printLoggerEventCallback = new PrintLoggerEventCallback(
            testFilePath.toString(),
            fileSizeLimit);

        for (int i = 0; i < entries.length; i++)
        {
            printLoggerEventCallback.onHeader(typeCode, eventCode, eventName, timestamp);
            printLoggerEventCallback.onValue(key1, NO_TAG, value1Base + i);
            printLoggerEventCallback.onValue(key2, NO_TAG, value2);
            printLoggerEventCallback.onValue(key3, NO_TAG, value3);
            printLoggerEventCallback.onValue(key4, NO_TAG, new UnsafeBuffer(value4));
            printLoggerEventCallback.onFooter(false);
        }

        final Path rolledFilePath0 = testFilePath.resolveSibling(testFilePath.getFileName() + ".1");
        assertEquals(entries[0] + entries[1], Files.readString(rolledFilePath0));

        final Path rolledFilePath1 = testFilePath.resolveSibling(testFilePath.getFileName() + ".2");
        assertEquals(entries[2] + entries[3], Files.readString(rolledFilePath1));

        assertEquals(0, Files.size(testFilePath));
    }

    @Test
    void shouldNotRollWhenUnderSizeLimit(@TempDir final Path testPath) throws IOException
    {
        final Path testFilePath = testPath.resolve("test.log");
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

        final String singleEntry = sb.toString();
        final int fileSizeLimit = 100 * singleEntry.length();

        final PrintLoggerEventCallback printLoggerEventCallback = new PrintLoggerEventCallback(
            testFilePath.toString(),
            fileSizeLimit);

        for (int i = 1; i <= 3; i++)
        {
            printLoggerEventCallback.onHeader(typeCode, eventCode, eventName, timestamp);
            printLoggerEventCallback.onValue(key1, NO_TAG, value1);
            printLoggerEventCallback.onValue(key2, NO_TAG, value2);
            printLoggerEventCallback.onValue(key3, NO_TAG, value3);
            printLoggerEventCallback.onValue(key4, NO_TAG, new UnsafeBuffer(value4));
            printLoggerEventCallback.onFooter(false);
        }

        final String expected = singleEntry + singleEntry + singleEntry;
        assertEquals(expected, Files.readString(testFilePath));

        final Path rolledFilePath = testFilePath.resolveSibling(testFilePath.getFileName() + ".1");
        assertFalse(Files.exists(rolledFilePath));
    }

    @Test
    void shouldSkipExistingRolledFileIndicesWhenRolling(@TempDir final Path testPath) throws IOException
    {
        final Path testFilePath = testPath.resolve("test.log");
        final Path preExistingRolledFilePath = testFilePath.resolveSibling(testFilePath.getFileName() + ".1");
        final String preExistingContent = "existing";
        Files.writeString(preExistingRolledFilePath, preExistingContent);

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
        final int fileSizeLimit = expected.length() - 1;

        final PrintLoggerEventCallback printLoggerEventCallback = new PrintLoggerEventCallback(
            testFilePath.toString(),
            fileSizeLimit);

        printLoggerEventCallback.onHeader(typeCode, eventCode, eventName, timestamp);
        printLoggerEventCallback.onValue(key1, NO_TAG, value1);
        printLoggerEventCallback.onValue(key2, NO_TAG, value2);
        printLoggerEventCallback.onValue(key3, NO_TAG, value3);
        printLoggerEventCallback.onValue(key4, NO_TAG, new UnsafeBuffer(value4));
        printLoggerEventCallback.onFooter(false);

        assertEquals(preExistingContent, Files.readString(preExistingRolledFilePath));

        final Path newRolledFilePath = testFilePath.resolveSibling(testFilePath.getFileName() + ".2");
        assertEquals(expected, Files.readString(newRolledFilePath));

        assertEquals(0, Files.size(testFilePath));
    }

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

        assertEquals(sb.toString(), capturingPrintStream.flushAndGetContent());
    }
}