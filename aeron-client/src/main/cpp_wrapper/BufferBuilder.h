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
#ifndef AERON_BUFFERBUILDER_H
#define AERON_BUFFERBUILDER_H

#include <limits>

#include "Aeron.h"

namespace aeron
{

static constexpr std::uint32_t BUFFER_BUILDER_MAX_CAPACITY = std::numeric_limits<std::int32_t>::max() - 8;
static constexpr std::uint32_t BUFFER_BUILDER_INIT_MIN_CAPACITY = 4096;

class BufferBuilder
{
public:
    using this_t = BufferBuilder;

    explicit BufferBuilder(std::uint32_t initialCapacity = 0) :
        m_capacity(initialCapacity),
        m_buffer(new std::uint8_t[m_capacity])
    {
        if (BUFFER_BUILDER_MAX_CAPACITY < m_capacity)
        {
            throw IllegalArgumentException(
                "initialCapacity outside range 0 - " + std::to_string(BUFFER_BUILDER_MAX_CAPACITY) +
                ": capacity=" + std::to_string(m_capacity), SOURCEINFO, EINVAL);
        }
    }

    BufferBuilder(BufferBuilder &&builder) noexcept:
        m_capacity(builder.m_capacity),
        m_limit(builder.m_limit),
        m_nextTermOffset(builder.m_nextTermOffset),
        m_buffer(std::move(builder.m_buffer))
    {
    }

    std::uint8_t *buffer() const
    {
        return &m_buffer[0];
    }

    std::uint32_t limit() const
    {
        return m_limit;
    }

    std::uint32_t capacity() const
    {
        return m_capacity;
    }

    void limit(std::uint32_t limit)
    {
        if (limit >= m_capacity)
        {
            throw IllegalArgumentException(
                "limit outside range: capacity=" + std::to_string(m_capacity) + " limit=" + std::to_string(limit),
                SOURCEINFO, EINVAL);
        }

        m_limit = limit;
    }

    util::index_t nextTermOffset() const
    {
        return m_nextTermOffset;
    }

    void nextTermOffset(util::index_t offset)
    {
        m_nextTermOffset = offset;
    }

    this_t &reset()
    {
        m_limit = 0;
        m_nextTermOffset = NULL_VALUE;
        return *this;
    }

    this_t &compact()
    {
        const std::uint32_t newCapacity =
            BUFFER_BUILDER_INIT_MIN_CAPACITY < m_limit ? m_limit : BUFFER_BUILDER_INIT_MIN_CAPACITY;
        if (newCapacity < m_capacity)
        {
            resize(newCapacity);
        }

        return *this;
    }

    this_t &append(AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &)
    {
        ensureCapacity(static_cast<std::uint32_t>(length));

        ::memcpy(&m_buffer[0] + m_limit, buffer.buffer() + offset, static_cast<std::uint32_t>(length));
        m_limit += length;
        return *this;
    }

    this_t &append(AtomicBuffer &buffer, util::index_t offset, util::index_t length)
    {
        ensureCapacity(static_cast<std::uint32_t>(length));

        ::memcpy(&m_buffer[0] + m_limit, buffer.buffer() + offset, static_cast<std::uint32_t>(length));
        m_limit += length;
        return *this;
    }

    this_t &captureHeader(Header &header)
    {
        return *this;
    }

private:
    std::uint32_t m_capacity = 0;
    std::uint32_t m_limit = 0;
    util::index_t m_nextTermOffset = NULL_VALUE;
    std::unique_ptr<std::uint8_t[]> m_buffer;

    inline static std::uint32_t findSuitableCapacity(
        std::uint32_t currentCapacity, std::uint32_t requiredCapacity) noexcept
    {
        std::uint32_t newCapacity = currentCapacity < BUFFER_BUILDER_INIT_MIN_CAPACITY ?
            BUFFER_BUILDER_INIT_MIN_CAPACITY : currentCapacity;

        while (newCapacity < requiredCapacity)
        {
            newCapacity = newCapacity + (newCapacity / 2);
            if (newCapacity > BUFFER_BUILDER_MAX_CAPACITY)
            {
                newCapacity = BUFFER_BUILDER_MAX_CAPACITY;
            }
        }

        return newCapacity;
    }

    void ensureCapacity(std::uint32_t additionalCapacity)
    {
        const std::uint32_t requiredCapacity = m_limit + additionalCapacity;

        if (requiredCapacity > m_capacity)
        {
            if (requiredCapacity > BUFFER_BUILDER_MAX_CAPACITY)
            {
                throw util::IllegalStateException(
                    "max capacity reached: " + std::to_string(BUFFER_BUILDER_MAX_CAPACITY), SOURCEINFO, EINVAL);
            }

            const std::uint32_t newCapacity = findSuitableCapacity(m_capacity, requiredCapacity);
            resize(newCapacity);
        }
    }

    void resize(std::uint32_t newCapacity)
    {
        std::unique_ptr<std::uint8_t[]> newBuffer(new std::uint8_t[newCapacity]);

        ::memcpy(&newBuffer[0], &m_buffer[0], m_limit);
        m_buffer = std::move(newBuffer);
        m_capacity = newCapacity;
    }
};

}

#endif
