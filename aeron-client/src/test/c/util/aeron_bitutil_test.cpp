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

#include <gtest/gtest.h>

extern "C"
{
#include "util/aeron_bitutil.h"
}

class BitutilTest : public testing::Test
{
public:
    BitutilTest() = default;
};

TEST_F(BitutilTest, shouldCountTrailingZeros64Bit)
{
    for (uint64_t i = 0; i < 64; i++)
    {
        uint64_t value = UINT64_C(1) << i;
        EXPECT_EQ(aeron_number_of_trailing_zeroes_u64(value), static_cast<int>(i));
    }
}

TEST_F(BitutilTest, shouldCheckIfValueIsAlignedCorrectly)
{
    EXPECT_TRUE(AERON_IS_ALIGNED(64, 2));
    EXPECT_TRUE(AERON_IS_ALIGNED(0, 64));
    EXPECT_TRUE(AERON_IS_ALIGNED(0, 4096));
    EXPECT_TRUE(AERON_IS_ALIGNED(4096, 4096));
    EXPECT_TRUE(AERON_IS_ALIGNED(17179869184, 1073741824));
    EXPECT_TRUE(AERON_IS_ALIGNED(17179869184, 4096));
    EXPECT_FALSE(AERON_IS_ALIGNED(1, 4));
    EXPECT_FALSE(AERON_IS_ALIGNED(128, 1024));
    EXPECT_FALSE(AERON_IS_ALIGNED(1023, 1024));
    EXPECT_FALSE(AERON_IS_ALIGNED(1025, 1024));
    EXPECT_FALSE(AERON_IS_ALIGNED(2049, 1024));
    EXPECT_FALSE(AERON_IS_ALIGNED(2047, 1024));
}

TEST_F(BitutilTest, shouldAlignValue)
{
    EXPECT_EQ(64, AERON_ALIGN(64, 2));
    EXPECT_EQ(0, AERON_ALIGN(0, 2));
    EXPECT_EQ(0, AERON_ALIGN(0, 4096));
    EXPECT_EQ(4, AERON_ALIGN(4, 4));
    EXPECT_EQ(8, AERON_ALIGN(5, 4));
    EXPECT_EQ(1024, AERON_ALIGN(128, 1024));
    EXPECT_EQ(1024, AERON_ALIGN(3, 1024));
    EXPECT_EQ(17179869184, AERON_ALIGN(17179869184, 1073741824));
    EXPECT_EQ(17179869184, AERON_ALIGN(17179869184, 4096));
    EXPECT_EQ(17179869184, AERON_ALIGN(17179869184, 2097152));
    EXPECT_EQ(17181966336, AERON_ALIGN(17179869185, 2097152));
    EXPECT_EQ(4096, AERON_ALIGN(4095, 4096));
    EXPECT_EQ(8192, AERON_ALIGN(4097, 4096));
}

TEST_F(BitutilTest, shouldCheckIfValueIsPowerOfTwo)
{
    EXPECT_FALSE(AERON_IS_POWER_OF_TWO(-1000));
    EXPECT_FALSE(AERON_IS_POWER_OF_TWO(-1024));
    EXPECT_FALSE(AERON_IS_POWER_OF_TWO(0));
    EXPECT_FALSE(AERON_IS_POWER_OF_TWO(19));
    EXPECT_FALSE(AERON_IS_POWER_OF_TWO(1973245985794359374));
    EXPECT_FALSE(AERON_IS_POWER_OF_TWO(INT64_MAX));
    EXPECT_FALSE(AERON_IS_POWER_OF_TWO(INT64_MIN));
    EXPECT_FALSE(AERON_IS_POWER_OF_TWO(INT32_MAX));
    EXPECT_FALSE(AERON_IS_POWER_OF_TWO(INT32_MIN));
    EXPECT_FALSE(AERON_IS_POWER_OF_TWO(UINT64_MAX));
    EXPECT_FALSE(AERON_IS_POWER_OF_TWO(UINT32_MAX));
    EXPECT_FALSE(AERON_IS_POWER_OF_TWO(UINT16_MAX));
    EXPECT_TRUE(AERON_IS_POWER_OF_TWO(1));
    EXPECT_TRUE(AERON_IS_POWER_OF_TWO(2));
    EXPECT_TRUE(AERON_IS_POWER_OF_TWO(1024 * 1024));
    EXPECT_TRUE(AERON_IS_POWER_OF_TWO(1024u * 1024));
    EXPECT_TRUE(AERON_IS_POWER_OF_TWO(1024 * 1024 * 1024));
    EXPECT_TRUE(AERON_IS_POWER_OF_TWO(1024u * 1024 * 1024));
    EXPECT_TRUE(AERON_IS_POWER_OF_TWO(1024l * 1024 * 1024 * 1024));
    EXPECT_TRUE(AERON_IS_POWER_OF_TWO(1024ul * 1024 * 1024 * 1024));
    for(int i = 0; i <= 30; i++)
    {
        EXPECT_TRUE(AERON_IS_POWER_OF_TWO(1 << i));
    }
    for(int i = 0; i <= 30; i++)
    {
        EXPECT_TRUE(AERON_IS_POWER_OF_TWO(1u << i));
    }
    for(int i = 31; i <= 62; i++)
    {
        EXPECT_TRUE(AERON_IS_POWER_OF_TWO(1l << i));
    }
}
