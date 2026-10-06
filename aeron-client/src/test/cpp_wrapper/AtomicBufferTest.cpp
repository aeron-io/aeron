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

#include <cstdint>
#include <limits>

#include <gtest/gtest.h>

#include "concurrent/AtomicBuffer.h"
#include "util/Exceptions.h"

using namespace aeron::concurrent;
using namespace aeron::util;

static const std::size_t CAPACITY = 64;

class AtomicBufferTest : public testing::Test
{
protected:
    // the backing memory is larger than the buffer so that a missed bounds check does not crash the test
    alignas(64) std::uint8_t m_memory[4096] = {};
    AtomicBuffer m_buffer = AtomicBuffer(m_memory, CAPACITY);
};

TEST_F(AtomicBufferTest, shouldAllowAccessWithinCapacity)
{
    EXPECT_NO_THROW(m_buffer.getInt32(0));
    EXPECT_NO_THROW(m_buffer.getInt32(CAPACITY - sizeof(std::int32_t)));
    EXPECT_NO_THROW(m_buffer.putInt64(CAPACITY - sizeof(std::int64_t), 7));
    EXPECT_NO_THROW(m_buffer.setMemory(CAPACITY, 0, 0));
}

TEST_F(AtomicBufferTest, shouldThrowWhenAccessOverlapsEndOfBuffer)
{
    EXPECT_THROW(m_buffer.getInt32(CAPACITY - 2), OutOfBoundsException);
    EXPECT_THROW(m_buffer.getInt32(CAPACITY), OutOfBoundsException);
}

TEST_F(AtomicBufferTest, shouldThrowWhenIndexIsNegative)
{
    EXPECT_THROW(m_buffer.getInt32(-4), OutOfBoundsException);
    EXPECT_THROW(m_buffer.getInt32(std::numeric_limits<index_t>::min()), OutOfBoundsException);
}

TEST_F(AtomicBufferTest, shouldThrowWhenIndexIsBeyondCapacity)
{
    EXPECT_THROW(m_buffer.getInt32(CAPACITY + 1), OutOfBoundsException);
    EXPECT_THROW(m_buffer.getInt32(1000), OutOfBoundsException);
    EXPECT_THROW(m_buffer.putInt64(1000, 7), OutOfBoundsException);
    EXPECT_THROW(m_buffer.setMemory(CAPACITY + 1, 0, 0), OutOfBoundsException);
}
