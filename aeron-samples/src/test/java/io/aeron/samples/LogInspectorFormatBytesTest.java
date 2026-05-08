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
package io.aeron.samples;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class LogInspectorFormatBytesTest
{
    private static final byte[] INPUT = { 0x0A, (byte)0xBC };
    private static final UnsafeBuffer BUFFER = new UnsafeBuffer(INPUT);

    @Test
    void shouldFormatBytesToHex()
    {
        final char[] result = LogInspector.formatBytes(BUFFER, 0, INPUT.length, "hex");

        assertEquals("0ABC", new String(result));
    }

    @ParameterizedTest
    @ValueSource(strings = { "ascii", "us-ascii", "us_ascii" })
    void shouldFormatBytesToAsciiForAllAliases(final String format)
    {
        // 0x0A is a valid positive byte; (byte)0xBC is negative and should clamp to 0
        final char[] result = LogInspector.formatBytes(BUFFER, 0, INPUT.length, format);

        assertEquals(2, result.length);
        assertEquals((char)0x0A, result[0]);
        assertEquals((char)0, result[1]);
    }

    @Test
    void shouldDefaultToHexForUnknownFormat()
    {
        final char[] result = LogInspector.formatBytes(BUFFER, 0, INPUT.length, "binary");

        assertEquals("0ABC", new String(result));
    }

    @Test
    void shouldRespectOffsetAndLength()
    {
        final char[] result = LogInspector.formatBytes(BUFFER, 1, 1, "hex");

        assertEquals("BC", new String(result));
    }

    @Test
    void shouldReturnEmptyArrayForZeroLength()
    {
        final char[] result = LogInspector.formatBytes(BUFFER, 0, 0, "hex");

        assertEquals(0, result.length);
    }

    @Test
    void shouldThreeArgOverloadMatchFourArgWithDefaultFormat()
    {
        final char[] threeArg = LogInspector.formatBytes(BUFFER, 0, INPUT.length);
        final char[] fourArg = LogInspector.formatBytes(BUFFER, 0, INPUT.length,
            LogInspector.AERON_LOG_DATA_FORMAT_DEFAULT);

        assertArrayEquals(fourArg, threeArg);
    }
}
