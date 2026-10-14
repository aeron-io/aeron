/*
 * Copyright 2026 Real Logic Limited.
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
#include <limits>

#include "gtest/gtest.h"

#include "BufferBuilder.h"

using namespace aeron;
using namespace aeron::concurrent;

TEST(BufferBuilderTest, shouldRejectNegativeOffsetOrLength)
{
    std::array<std::uint8_t, 8> bytes = {};
    AtomicBuffer buffer(bytes);
    BufferBuilder builder;

    EXPECT_THROW(builder.append(buffer, -1, 1), util::IllegalArgumentException);
    EXPECT_THROW(builder.append(buffer, 0, -1), util::IllegalArgumentException);
}

TEST(BufferBuilderTest, shouldRejectAppendOutsideSourceBuffer)
{
    std::array<std::uint8_t, 8> bytes = {};
    AtomicBuffer buffer(bytes);
    BufferBuilder builder;

    EXPECT_THROW(builder.append(buffer, 1, 8), util::OutOfBoundsException);
}

TEST(BufferBuilderTest, shouldRejectAppendThatExceedsMaxCapacity)
{
    std::array<std::uint8_t, 1> bytes = {};
    AtomicBuffer buffer(bytes.data(), std::numeric_limits<util::index_t>::max());
    BufferBuilder builder(2);
    builder.limit(1);

    EXPECT_THROW(
        builder.append(buffer, 0, std::numeric_limits<util::index_t>::max()),
        util::IllegalStateException);
}
