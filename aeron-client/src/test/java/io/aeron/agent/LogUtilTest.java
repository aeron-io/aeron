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
package io.aeron.agent;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

class LogUtilTest
{
    private final StringBuilder buff = new StringBuilder(32);

    @ParameterizedTest
    @CsvSource({
        "0, [0.000000000]",
        "1, [0.000000001]",
        "1234, [0.000001234]",
        "12345678, [0.012345678]",
        "123456789, [0.123456789]",
        "1000000000, [1.000000000]",
        "1987654321, [1.987654321]",
        "9223372036854775807, [9223372036.854775807]",
    })
    void shouldAppendTimestamp(final long timestamp, final String expected)
    {
        LogUtil.appendTimestamp(buff, timestamp);

        final int lastCharIndex = buff.length() - 1;
        assertEquals(expected, buff.substring(0, lastCharIndex));
        assertEquals(' ', buff.charAt(lastCharIndex));
    }
}
