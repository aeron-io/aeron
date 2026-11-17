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
package io.aeron;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AeronTest
{
    private final Aeron aeron = mock(Aeron.class);

    @ParameterizedTest
    @CsvSource({
        "0, 0",
        "1, 1",
        "2147483646, 2147483646",       // Integer.MAX_VALUE - 1
        "2147483647, 2147483647",       // Integer.MAX_VALUE
        "2147483648, -2147483648",      // Integer.MAX_VALUE + 1 wraps to Integer.MIN_VALUE
        "2147483649, -2147483647",      // Integer.MIN_VALUE + 1
        "4294967294, -2",               // last value before NULL_VALUE
        "4294967296, 0",                // 2^32 wraps to 0
        "4294967297, 1",
        "9223372036854775806, -2",      // Long.MAX_VALUE - 1
    })
    void shouldReturnLowerBitsOfCorrelationIdAsSessionId(final long correlationId, final int expectedSessionId)
    {
        when(aeron.nextSessionId()).thenCallRealMethod();
        when(aeron.nextCorrelationId()).thenReturn(correlationId);

        assertEquals(expectedSessionId, aeron.nextSessionId());
        verify(aeron, times(1)).nextCorrelationId();
    }

    @ParameterizedTest
    @ValueSource(longs = {
        4294967295L,                    // 2^32 - 1
        8589934591L,                    // 2^33 - 1
        Long.MAX_VALUE,
        -1L,
    })
    void shouldSkipCorrelationIdWhoseLowerBitsAreNullValue(final long correlationId)
    {
        when(aeron.nextSessionId()).thenCallRealMethod();
        when(aeron.nextCorrelationId()).thenReturn(correlationId, correlationId + 1);

        assertEquals((int)(correlationId + 1), aeron.nextSessionId());
        verify(aeron, times(2)).nextCorrelationId();
    }
}
