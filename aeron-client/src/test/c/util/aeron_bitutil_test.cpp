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
    EXPECT_EQ(64, aeron_number_of_trailing_zeroes_u64(0));
    for (uint64_t i = 0; i < 64; i++)
    {
        uint64_t value = UINT64_C(1) << i;
        EXPECT_EQ(aeron_number_of_trailing_zeroes_u64(value), static_cast<int>(i));
    }
}

TEST_F(BitutilTest, shouldCountTrailingZeros32Bit)
{
    EXPECT_EQ(32, aeron_number_of_trailing_zeroes(0));
    for (uint32_t i = 0; i < 32; i++)
    {
        uint32_t value = UINT32_C(1) << i;
        EXPECT_EQ(aeron_number_of_trailing_zeroes(value), static_cast<int>(i));
    }
}

TEST_F(BitutilTest, shouldCountLeadingZeros32Bit)
{
    EXPECT_EQ(32, aeron_number_of_leading_zeroes(0));
    for (uint64_t i = 0; i < 32; i++)
    {
        uint32_t value = UINT32_C(1) << i;
        EXPECT_EQ(aeron_number_of_leading_zeroes(value), 31 - i);
    }
}

TEST_F(BitutilTest, shouldCountLeadingZeros64Bit)
{
    EXPECT_EQ(64, aeron_number_of_leading_zeroes_u64(0));
    for (uint64_t i = 0; i < 64; i++)
    {
        uint64_t value = UINT64_C(1) << i;
        EXPECT_EQ(aeron_number_of_leading_zeroes_u64(value), 63 - i);
    }
}
