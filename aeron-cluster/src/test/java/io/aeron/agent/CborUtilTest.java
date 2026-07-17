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

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.cbor.CBORFactory;

import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class CborUtilTest
{
    @ParameterizedTest
    @ValueSource(longs = {
        2, 25, 0x7F, 0x100,
        0x1234, 0x7FFF, 0x10000,
        0x12345678, 0x7FFFFFFF, 0x80000000,
        0x123456789ABCDEF0L, 0xFFFFFFFFFFFFFFFFL,
        -2, -25, -0xFF,
        -0x1234, -0xFFFF,
        -0x12345678, -0x7FFFFFFE })
    void shouldEncodeNumberMessage(final long memberId)
    {
        final int offset = 0;
        final int length = 1024;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);

        final long timestamp = 12643263L;

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);

        CborUtil.encodeHeader(encodingState, ClusterEventCode.ELECTION_STATE_CHANGE, timestamp);
        CborUtil.encode(encodingState, "memberId", memberId);
        CborUtil.encodeFooter(encodingState);

        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());

        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Map<String, Object> stringObjectMap = cborObjectMapper.readValue(
            data,
            new TypeReference<>()
            {
            });

        assertEquals(memberId, ((Number)stringObjectMap.get("memberId")).longValue());

        System.out.println(stringObjectMap);
    }


    static Stream<Arguments> generateBigStrings()
    {
        return Stream.of(
            Arguments.of("A".repeat(10)),
            Arguments.of("A".repeat(1000)),
            Arguments.of("A".repeat(100_000))
        );
    }

    @ParameterizedTest
    @MethodSource("generateBigStrings")
    void shouldEncodeCharSequenceMessage(final String reason)
    {
        final int offset = 0;
        final int length = 200_000;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);

        final long timestamp = 12643263L;

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);

        CborUtil.encodeHeader(encodingState, ClusterEventCode.ELECTION_STATE_CHANGE, timestamp);
        CborUtil.encode(encodingState, "reason", reason);
        CborUtil.encodeFooter(encodingState);

        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());

        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Map<String, Object> stringObjectMap = cborObjectMapper.readValue(
            data,
            new TypeReference<>()
            {
            });

        assertEquals(reason, stringObjectMap.get("reason").toString());

        System.out.println(stringObjectMap);
    }

    @ParameterizedTest
    @CsvSource({
        "10,1",
        "0x1F,2",
        "0x1234,3",
        "0x12345678,5",
        "0x123456781234567, 9"
    })
    void shouldGetCorrectLengthForNumber(final long number, final int expectedLength)
    {
        assertEquals(expectedLength, CborUtil.lengthNumber(number));
    }

    static Stream<Arguments> generateBigStringsForLengthCalculation()
    {
        return Stream.of(
            Arguments.of("A".repeat(10), 11),
            Arguments.of("A".repeat(100), 102),
            Arguments.of("A".repeat(1000), 1003),
            Arguments.of("A".repeat(100_000), 100_005)
        );
    }

    @ParameterizedTest
    @MethodSource("generateBigStringsForLengthCalculation")
    void shouldGetCorrectLengthForCharSequence(final CharSequence text, final int expectedLength)
    {
        assertEquals(expectedLength, CborUtil.lengthString(text));
    }

}