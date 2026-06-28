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
#include "aeron_alloc.h"
#include "util/aeron_error.h"
}

class AllocTest : public testing::Test
{
};

TEST_F(AllocTest, shouldAllocateAlignedMemory)
{
    char *ptr = nullptr;
    size_t offset;
    size_t size = 100;
    size_t alignment = 64;

    EXPECT_EQ(0, aeron_alloc_aligned(reinterpret_cast<void**>(&ptr), &offset, size, alignment));
    EXPECT_NE(nullptr, ptr);
    size_t actual_allocated_size;
#if defined(AERON_COMPILER_MSVC)
    actual_allocated_size = size + alignment;
    EXPECT_NE(0, offset);
#else
    actual_allocated_size = size;
    EXPECT_EQ(0, offset);
#endif

    EXPECT_EQ(0, ((uintptr_t)ptr + offset) % alignment);
    for (size_t i = 0; i < actual_allocated_size; i++)
    {
        EXPECT_EQ(0, ptr[i]);
    }

    aeron_free(ptr);
}

TEST_F(AllocTest, shouldAllocateAndZeroOutMemory)
{
    char *ptr = nullptr;
    size_t size = 100;

    EXPECT_EQ(0, aeron_alloc(reinterpret_cast<void**>(&ptr), size));
    EXPECT_NE(nullptr, ptr);
    for (size_t i = 0; i < size; i++)
    {
        EXPECT_EQ(0, ptr[i]);
    }

    aeron_free(ptr);
}

TEST_F(AllocTest, shouldFailIfAlignmentIsNotAPowerOfTwo)
{
    char *ptr = nullptr;
    size_t offset;
    size_t size = 100;
    size_t alignment = 13;

    EXPECT_EQ(-1, aeron_alloc_aligned(reinterpret_cast<void**>(&ptr), &offset, size, alignment));
    EXPECT_EQ(nullptr, ptr);
    EXPECT_EQ(EINVAL, aeron_errcode());
    aeron_err_clear();
}

TEST_F(AllocTest, shouldAllocateMemoryIfPointerIsNullWithoutZeroingItOut)
{
    char *ptr = nullptr;
    size_t size = 32;

    EXPECT_EQ(0, aeron_reallocf(reinterpret_cast<void**>(&ptr), size));
    EXPECT_NE(nullptr, ptr);

    aeron_free(ptr);
}

TEST_F(AllocTest, shouldReallocMemory)
{
    char *ptr = nullptr;
    size_t original_size = 50;
    EXPECT_EQ(0, aeron_alloc(reinterpret_cast<void**>(&ptr), original_size));
    EXPECT_NE(nullptr, ptr);
    memset(ptr, 'a', original_size);

    size_t new_size = 120;
    EXPECT_EQ(0, aeron_reallocf(reinterpret_cast<void**>(&ptr), new_size));
    EXPECT_NE(nullptr, ptr);
    memset(ptr + original_size, 'x', new_size - original_size);

    char tmp[120];
    memset(tmp, 'a', original_size);
    memset(tmp + original_size, 'x', new_size - original_size);
    EXPECT_EQ(0, memcmp(ptr, tmp, new_size));

    new_size = 30;
    EXPECT_EQ(0, aeron_reallocf(reinterpret_cast<void**>(&ptr), new_size));
    EXPECT_NE(nullptr, ptr);
    EXPECT_EQ(0, memcmp(ptr, tmp, new_size));

    aeron_free(ptr);
}

TEST_F(AllocTest, shouldFreeMemoryIfSizeIsZero)
{
    char *ptr = nullptr;
    size_t original_size = 50;
    EXPECT_EQ(0, aeron_alloc(reinterpret_cast<void**>(&ptr), original_size));
    EXPECT_NE(nullptr, ptr);

    EXPECT_EQ(0, aeron_reallocf(reinterpret_cast<void**>(&ptr), 0));
    EXPECT_EQ(nullptr, ptr);
}
