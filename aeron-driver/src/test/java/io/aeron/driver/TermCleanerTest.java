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
package io.aeron.driver;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static io.aeron.driver.TermCleaner.TERM_CLEANUP_BLOCK_LENGTH;
import static io.aeron.driver.TermCleaner.alignCleanPositionToTheStartOfTheBlock;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TermCleanerTest
{
    @Test
    void blockLength()
    {
        assertEquals(4096, TERM_CLEANUP_BLOCK_LENGTH);
    }

    @ParameterizedTest
    @CsvSource({
        "0,0",
        "128,0",
        "4096,4096",
        "65536,65536",
        "65504,61440",
        "828458926080,828458926080",
        "828458924111,828458921984"
    })
    void shouldComputeCleanStartPosition(final long startPosition, final long expectedResult)
    {
        assertEquals(expectedResult, alignCleanPositionToTheStartOfTheBlock(startPosition));
    }
}
