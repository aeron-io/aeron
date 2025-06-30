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

#include <array>

#include <gtest/gtest.h>

extern "C"
{
#include "aeron_term_cleaner.h"
}

class TermCleanerTest : public testing::Test
{
};

TEST_F(TermCleanerTest, blockLength)
{
    ASSERT_EQ(4096u, AERON_TERM_CLEANER_TERM_CLEANUP_BLOCK_LENGTH);
}

TEST_F(TermCleanerTest, shouldReturnOriginalPositionIfProperlyAligned)
{
    EXPECT_EQ(0, aeron_term_cleaner_block_start_position(0));
    EXPECT_EQ(
        AERON_TERM_CLEANER_TERM_CLEANUP_BLOCK_LENGTH,
        aeron_term_cleaner_block_start_position(AERON_TERM_CLEANER_TERM_CLEANUP_BLOCK_LENGTH));
    EXPECT_EQ(
        AERON_TERM_CLEANER_TERM_CLEANUP_BLOCK_LENGTH * 13,
        aeron_term_cleaner_block_start_position(AERON_TERM_CLEANER_TERM_CLEANUP_BLOCK_LENGTH * 13));
    EXPECT_EQ(
        17179869184,
        aeron_term_cleaner_block_start_position(17179869184));
}

TEST_F(TermCleanerTest, shouldReturnBlockStartPosition)
{
    EXPECT_EQ(0, aeron_term_cleaner_block_start_position(128));
    EXPECT_EQ(0, aeron_term_cleaner_block_start_position(4095));
    EXPECT_EQ(AERON_TERM_CLEANER_TERM_CLEANUP_BLOCK_LENGTH, aeron_term_cleaner_block_start_position(6000));
    EXPECT_EQ(
        AERON_TERM_CLEANER_TERM_CLEANUP_BLOCK_LENGTH * 13,
        aeron_term_cleaner_block_start_position(AERON_TERM_CLEANER_TERM_CLEANUP_BLOCK_LENGTH * 13 + 100));
    EXPECT_EQ(17179869184, aeron_term_cleaner_block_start_position(17179869399));
}
