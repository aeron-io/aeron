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

package io.aeron.topology;

import org.agrona.collections.IntArrayList;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class AffinityParserTest
{

    public static Stream<Arguments> affinityList()
    {
        return Stream.of(
            Arguments.of("0", new int[] { 0 }),
            Arguments.of("0-3,7", new int[] { 0, 1, 2, 3, 7 }),
            Arguments.of("0-3,7,10-12", new int[] { 0, 1, 2, 3, 7, 10, 11, 12 })
        );
    }

    @ParameterizedTest
    @MethodSource("affinityList")
    void parseAffinity(final String cpuAffinity, final int[] expected)
    {
        final IntArrayList result = AffinityParser.parse(cpuAffinity);
        assertArrayEquals(expected, result.toIntArray());
    }

}
