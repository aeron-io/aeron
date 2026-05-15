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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

public class LogInspectorCliTest
{
    // --- happy paths ---

    @Test
    void shouldUseDefaultsWhenOnlyFilenameIsGiven()
    {
        final LogInspectorCli.ParsedArgs parsed = LogInspectorCli.parseArgs(new String[]{ "my.log" });

        assertNotNull(parsed);
        assertEquals("my.log", parsed.logFileName());
        assertEquals("hex", parsed.format());
        assertFalse(parsed.skipDefaultHeader());
        assertFalse(parsed.scanOverZeroes());
        assertEquals(Integer.MAX_VALUE, parsed.messageDumpLimit());
    }

    @Test
    void shouldParseFilenameAndByteLimit()
    {
        final LogInspectorCli.ParsedArgs parsed = LogInspectorCli.parseArgs(new String[]{ "my.log", "128" });

        assertNotNull(parsed);
        assertEquals("my.log", parsed.logFileName());
        assertEquals(128, parsed.messageDumpLimit());
    }

    @Test
    void shouldParseFormatHex()
    {
        final LogInspectorCli.ParsedArgs parsed = LogInspectorCli.parseArgs(new String[]{ "-f", "hex", "my.log" });

        assertNotNull(parsed);
        assertEquals("hex", parsed.format());
    }

    @Test
    void shouldParseFormatAscii()
    {
        final LogInspectorCli.ParsedArgs parsed = LogInspectorCli.parseArgs(new String[]{ "-f", "ascii", "my.log" });

        assertNotNull(parsed);
        assertEquals("ascii", parsed.format());
    }

    @Test
    void shouldParseSkipDefaultHeaderTrue()
    {
        final LogInspectorCli.ParsedArgs parsed = LogInspectorCli.parseArgs(new String[]{ "-d", "true", "my.log" });

        assertNotNull(parsed);
        assertTrue(parsed.skipDefaultHeader());
    }

    @Test
    void shouldParseSkipDefaultHeaderFalse()
    {
        final LogInspectorCli.ParsedArgs parsed = LogInspectorCli.parseArgs(new String[]{ "-d", "false", "my.log" });

        assertNotNull(parsed);
        assertFalse(parsed.skipDefaultHeader());
    }

    @Test
    void shouldParseScanOverZeroesTrue()
    {
        final LogInspectorCli.ParsedArgs parsed = LogInspectorCli.parseArgs(new String[]{ "-z", "true", "my.log" });

        assertNotNull(parsed);
        assertTrue(parsed.scanOverZeroes());
    }

    @Test
    void shouldParseScanOverZeroesFalse()
    {
        final LogInspectorCli.ParsedArgs parsed = LogInspectorCli.parseArgs(new String[]{ "-z", "false", "my.log" });

        assertNotNull(parsed);
        assertFalse(parsed.scanOverZeroes());
    }

    @Test
    void shouldReturnNullForHelpFlag()
    {
        assertNull(LogInspectorCli.parseArgs(new String[]{ "-h" }));
    }

    @Test
    void shouldParseAllOptionsTogether()
    {
        final LogInspectorCli.ParsedArgs parsed = LogInspectorCli.parseArgs(
            new String[]{ "-f", "ascii", "-d", "true", "-z", "true", "my.log", "64" });

        assertNotNull(parsed);
        assertEquals("ascii", parsed.format());
        assertTrue(parsed.skipDefaultHeader());
        assertTrue(parsed.scanOverZeroes());
        assertEquals("my.log", parsed.logFileName());
        assertEquals(64, parsed.messageDumpLimit());
    }

    // --- error paths ---

    @Test
    void shouldThrowForMissingFormatValue()
    {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> LogInspectorCli.parseArgs(new String[]{ "-f" }));

        assertTrue(ex.getMessage().contains("-f"));
    }

    @ParameterizedTest
    @ValueSource(strings = { "binary", "hex16", "", "HEX" })
    void shouldThrowForInvalidFormatValue(final String badFormat)
    {
        assertThrows(IllegalArgumentException.class,
            () -> LogInspectorCli.parseArgs(new String[]{ "-f", badFormat, "my.log" }));
    }

    @Test
    void shouldThrowForMissingSkipHeaderValue()
    {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> LogInspectorCli.parseArgs(new String[]{ "-d" }));

        assertTrue(ex.getMessage().contains("-d"));
    }

    @ParameterizedTest
    @ValueSource(strings = { "yes", "no", "1", "TRUE", "FALSE" })
    void shouldThrowForInvalidSkipHeaderValue(final String badValue)
    {
        assertThrows(IllegalArgumentException.class,
            () -> LogInspectorCli.parseArgs(new String[]{ "-d", badValue, "my.log" }));
    }

    @Test
    void shouldThrowForMissingScanOverZeroesValue()
    {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> LogInspectorCli.parseArgs(new String[]{ "-z" }));

        assertTrue(ex.getMessage().contains("-z"));
    }

    @ParameterizedTest
    @ValueSource(strings = { "yes", "no", "1", "TRUE", "FALSE" })
    void shouldThrowForInvalidScanOverZeroesValue(final String badValue)
    {
        assertThrows(IllegalArgumentException.class,
            () -> LogInspectorCli.parseArgs(new String[]{ "-z", badValue, "my.log" }));
    }

    @Test
    void shouldThrowForUnknownFlag()
    {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> LogInspectorCli.parseArgs(new String[]{ "-x", "my.log" }));

        assertTrue(ex.getMessage().contains("-x"));
    }

    @Test
    void shouldThrowForNoArguments()
    {
        assertThrows(IllegalArgumentException.class,
            () -> LogInspectorCli.parseArgs(new String[0]));
    }

    @Test
    void shouldThrowForTooManyPositionalArguments()
    {
        assertThrows(IllegalArgumentException.class,
            () -> LogInspectorCli.parseArgs(new String[]{ "my.log", "128", "extra" }));
    }
}
