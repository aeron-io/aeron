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

#ifndef AERON_CONCURRENT_ATOMIC_BUFFER_H
#define AERON_CONCURRENT_ATOMIC_BUFFER_H

#include <cstring>
#include <string>
#include <array>

#include "util/Index.h"
#include "util/Exceptions.h"
#include "util/StringUtil.h"
#include "util/MacroUtil.h"
#include "concurrent/Atomic64.h"

namespace aeron { namespace concurrent {

/**
 * Wraps, but does not own, a buffer of memory for providing atomic operations. This is for providing a view.
 */
class AtomicBuffer
{
public:
    constexpr AtomicBuffer() noexcept = default;

    /**
     * Wrap a buffer of memory for a given length.
     *
     * @param buffer to be wrapped.
     * @param length of the buffer for bounds checking.
     */
    AtomicBuffer(std::uint8_t *buffer, std::size_t length) :
        m_buffer(buffer),
        m_length(static_cast<length_t>(length))
    {
#if !defined(DISABLE_BOUNDS_CHECKS)
        if (AERON_COND_EXPECT(length > static_cast<std::size_t>(std::numeric_limits<util::index_t>::max()), true))
        {
            throw aeron::util::OutOfBoundsException(
                aeron::util::strPrintf("length out of bounds[%p]: length=%lld", this, static_cast<long long>(length)),
                SOURCEINFO, EINVAL);
        }
#endif
    }

    /**
     * Wrap a buffer of memory for a given length and initialise the contents.
     *
     * @param buffer       to be wrapped.
     * @param length       of the buffer for bounds checking.
     * @param initialValue to set the memory too.
     */
    AtomicBuffer(std::uint8_t *buffer, std::size_t length, std::uint8_t initialValue) :
        m_buffer(buffer),
        m_length(static_cast<length_t>(length))
    {
#if !defined(DISABLE_BOUNDS_CHECKS)
        if (AERON_COND_EXPECT(length > static_cast<std::size_t>(std::numeric_limits<util::index_t>::max()), true))
        {
            throw aeron::util::OutOfBoundsException(
                aeron::util::strPrintf("length out of bounds[%p]. length=%lld", this, static_cast<long long>(length)),
                SOURCEINFO, EINVAL);
        }
#endif

        setMemory(0, static_cast<std::size_t>(length), initialValue);
    }

    template<std::size_t N>
    explicit AtomicBuffer(std::array<std::uint8_t, N> &buffer)
    {
        wrap(buffer);
    }

    template<std::size_t N>
    AtomicBuffer(std::array<std::uint8_t, N> &buffer, std::uint8_t initialValue)
    {
        wrap(buffer);
        buffer.fill(initialValue);
    }

#if COND_MOCK
    AtomicBuffer(const AtomicBuffer &) = default;
    AtomicBuffer& operator=(const AtomicBuffer &) = default;
    virtual ~AtomicBuffer() = default;
#endif

    /**
     * Wrap a buffer of memory for a given length.
     *
     * @param buffer to be wrapped.
     * @param length of the buffer for bounds checking.
     */
    inline void wrap(std::uint8_t *buffer, std::size_t length)
    {
#if !defined(DISABLE_BOUNDS_CHECKS)
        if (AERON_COND_EXPECT(length > static_cast<std::size_t>(std::numeric_limits<util::index_t>::max()), true))
        {
            throw aeron::util::OutOfBoundsException(
                aeron::util::strPrintf("length out of bounds[%p]: length=%lld", this, static_cast<long long>(length)),
                SOURCEINFO, EINVAL);
        }
#endif

        m_buffer = buffer;
        m_length = static_cast<length_t>(length);
    }

    /**
     * Wrap an existing AtomicBuffer.
     *
     * @param buffer from which the address and length are used.
     */
    inline void wrap(const AtomicBuffer &buffer)
    {
        m_buffer = buffer.m_buffer;
        m_length = buffer.m_length;
    }

    template<std::size_t N>
    inline void wrap(std::array<std::uint8_t, N> &buffer)
    {
        static_assert(
            N <= std::numeric_limits<util::index_t>::max(),
            "requires the array to have a size that fits in an index_t");

        m_buffer = buffer.data();
        m_length = static_cast<length_t>(N);
    }

    /**
     * The capacity of the underlying buffer.
     *
     * @return the capacity of the underlying buffer.
     */
    inline util::index_t capacity() const
    {
        return static_cast<util::index_t>(m_length);
    }

    /**
     * Update the capacity of the underlying buffer.
     *
     * @param length to be used for the new capacity.
     */
    inline void capacity(std::size_t length)
    {
#if !defined(DISABLE_BOUNDS_CHECKS)
        if (AERON_COND_EXPECT(length > static_cast<std::size_t>(std::numeric_limits<util::index_t>::max()), true))
        {
            throw aeron::util::OutOfBoundsException(
                aeron::util::strPrintf("length out of bounds[%p]: length=%lld", this, static_cast<long long>(length)),
                SOURCEINFO, EINVAL);
        }
#endif

        m_length = static_cast<length_t>(length);
    }

    /**
     * Get a pointer to the underlying buffer.
     *
     * @return a pointer to the underlying buffer
     */
    inline std::uint8_t *buffer() const
    {
        return m_buffer;
    }

    inline char *sbeData() const
    {
        return reinterpret_cast<char *>(m_buffer);
    }

    template <typename struct_t>
    struct_t &overlayStruct()
    {
        boundsCheck(0, sizeof(struct_t));
        return *reinterpret_cast<struct_t *>(m_buffer);
    }

    template <typename struct_t>
    struct_t &overlayStruct(util::index_t offset)
    {
        boundsCheck(offset, sizeof(struct_t));
        return *reinterpret_cast<struct_t *>(m_buffer + offset);
    }

    template <typename struct_t>
    const struct_t &overlayStruct(util::index_t offset) const
    {
        boundsCheck(offset, sizeof(struct_t));
        return *reinterpret_cast<struct_t *>(m_buffer + offset);
    }

    inline COND_MOCK_VIRTUAL void putInt64(util::index_t offset, std::int64_t v)
    {
        boundsCheck(offset, sizeof(std::int64_t));
        *reinterpret_cast<std::int64_t *>(m_buffer + offset) = v;
    }

    inline COND_MOCK_VIRTUAL std::int64_t getInt64(util::index_t offset) const
    {
        boundsCheck(offset, sizeof(std::int64_t));
        return *reinterpret_cast<std::int64_t *>(m_buffer + offset);
    }

    inline COND_MOCK_VIRTUAL void putInt32(util::index_t offset, std::int32_t v)
    {
        boundsCheck(offset, sizeof(std::int32_t));
        *reinterpret_cast<std::int32_t *>(m_buffer + offset) = v;
    }

    inline COND_MOCK_VIRTUAL std::int32_t getInt32(util::index_t offset) const
    {
        boundsCheck(offset, sizeof(std::int32_t));
        return *reinterpret_cast<std::int32_t *>(m_buffer + offset);
    }

    inline COND_MOCK_VIRTUAL std::int16_t getInt16(util::index_t offset) const
    {
        boundsCheck(offset, sizeof(std::int16_t));
        return *reinterpret_cast<std::int16_t *>(m_buffer + offset);
    }

    inline COND_MOCK_VIRTUAL void putInt16(util::index_t offset, std::int16_t v)
    {
        boundsCheck(offset, sizeof(std::int16_t));
        *reinterpret_cast<std::int16_t *>(m_buffer + offset) = v;
    }

    inline COND_MOCK_VIRTUAL std::uint16_t getUInt16(util::index_t offset) const
    {
        boundsCheck(offset, sizeof(std::uint16_t));
        return *reinterpret_cast<std::uint16_t *>(m_buffer + offset);
    }

    inline COND_MOCK_VIRTUAL void putUInt16(util::index_t offset, std::uint16_t v)
    {
        boundsCheck(offset, sizeof(std::uint16_t));
        *reinterpret_cast<std::uint16_t *>(m_buffer + offset) = v;
    }

    inline std::uint8_t getUInt8(util::index_t offset) const
    {
        boundsCheck(offset, sizeof(std::uint8_t));
        return *reinterpret_cast<std::uint8_t *>(m_buffer + offset);
    }

    inline COND_MOCK_VIRTUAL void putUInt8(util::index_t offset, std::uint8_t v)
    {
        boundsCheck(offset, sizeof(std::uint8_t));
        *reinterpret_cast<std::uint8_t *>(m_buffer + offset) = v;
    }

    inline COND_MOCK_VIRTUAL void putInt64Ordered(util::index_t offset, std::int64_t v)
    {
        boundsCheck(offset, sizeof(std::int64_t));
        atomic::putInt64Ordered((volatile std::int64_t *)(m_buffer + offset), v);
    }

    inline COND_MOCK_VIRTUAL std::int64_t getInt64Volatile(util::index_t offset) const
    {
        boundsCheck(offset, sizeof(std::int64_t));
        return atomic::getInt64Volatile((volatile std::int64_t *)(m_buffer + offset));
    }

    inline COND_MOCK_VIRTUAL void putInt32Ordered(util::index_t offset, std::int32_t v)
    {
        boundsCheck(offset, sizeof(std::int32_t));
        atomic::putInt32Ordered((volatile std::int32_t *)(m_buffer + offset), v);
    }

    inline COND_MOCK_VIRTUAL std::int32_t getInt32Volatile(util::index_t offset) const
    {
        boundsCheck(offset, sizeof(std::int32_t));
        return atomic::getInt32Volatile((volatile std::int32_t *)(m_buffer + offset));
    }

    inline void putInt64Atomic(util::index_t offset, std::int64_t v)
    {
        boundsCheck(offset, sizeof(std::int64_t));
        atomic::putInt64Atomic((volatile std::int64_t *)(m_buffer + offset), v);
    }

    inline void putInt32Atomic(util::index_t offset, std::int32_t v)
    {
        boundsCheck(offset, sizeof(std::int32_t));
        atomic::putInt32Atomic((volatile std::int32_t *)(m_buffer + offset), v);
    }

    /**
     * Single threaded increment with release semantics.
     *
     * @param offset in the buffer of the word.
     * @param delta  for to be applied to the value.
     */
    inline void addInt64Ordered(util::index_t offset, std::int64_t delta)
    {
        boundsCheck(offset, sizeof(std::int64_t));

        const std::int64_t value = getInt64(offset);
        atomic::putInt64Ordered((volatile std::int64_t *)(m_buffer + offset), value + delta);
    }

    inline bool compareAndSetInt64(util::index_t offset, std::int64_t expectedValue, std::int64_t updateValue)
    {
        boundsCheck(offset, sizeof(std::int64_t));
        std::int64_t original = atomic::cmpxchg((volatile std::int64_t *)(m_buffer + offset), expectedValue, updateValue);
        return original == expectedValue;
    }

    inline std::int64_t getAndSetInt64(util::index_t offset, std::int64_t value)
    {
        boundsCheck(offset, sizeof(std::int64_t));
        return atomic::xchg((volatile std::int64_t *)(m_buffer + offset), value);
    }

    inline std::int32_t getAndSetInt32(util::index_t offset, std::int32_t value)
    {
        boundsCheck(offset, sizeof(std::int32_t));
        return atomic::xchg((volatile std::int32_t *)(m_buffer + offset), value);
    }

    /**
     * Multi threaded increment.
     *
     * @param offset in the buffer of the word.
     * @param delta  for to be applied to the value.
     * @return the value before applying the delta.
     */
    inline COND_MOCK_VIRTUAL std::int64_t getAndAddInt64(util::index_t offset, std::int64_t delta)
    {
        boundsCheck(offset, sizeof(std::int64_t));
        return atomic::getAndAddInt64((volatile std::int64_t *)(m_buffer + offset), delta);
    }

    /**
     * Single threaded add with release semantics.
     *
     * @param offset in the buffer of the word.
     * @param delta  for to be applied to the value.
     */
    inline void addInt32Ordered(util::index_t offset, std::int32_t delta)
    {
        boundsCheck(offset, sizeof(std::int32_t));

        const std::int32_t value = getInt32(offset);
        atomic::putInt32Ordered((volatile std::int32_t *)(m_buffer + offset), value + delta);
    }

    inline bool compareAndSetInt32(util::index_t offset, std::int32_t expectedValue, std::int32_t updateValue)
    {
        boundsCheck(offset, sizeof(std::int32_t));
        std::int32_t original = atomic::cmpxchg((volatile std::int32_t *)(m_buffer + offset), expectedValue, updateValue);
        return original == expectedValue;
    }

    /**
     * Multi threaded increment.
     *
     * @param offset in the buffer of the word.
     * @param delta  for to be applied to the value.
     * @return the value before applying the delta.
     */
    inline COND_MOCK_VIRTUAL std::int32_t getAndAddInt32(util::index_t offset, std::int32_t delta)
    {
        boundsCheck(offset, sizeof(std::int32_t));
        return atomic::getAndAddInt32((volatile std::int32_t *)(m_buffer + offset), delta);
    }

    inline COND_MOCK_VIRTUAL void putBytes(
        util::index_t index, const concurrent::AtomicBuffer &srcBuffer, util::index_t srcIndex, util::index_t length)
    {
        boundsCheck(index, length);
        srcBuffer.boundsCheck(srcIndex, length);
        ::memcpy(m_buffer + index, srcBuffer.m_buffer + srcIndex, static_cast<std::size_t>(length));
    }

    inline COND_MOCK_VIRTUAL void putBytes(util::index_t index, const std::uint8_t *srcBuffer, util::index_t length)
    {
        boundsCheck(index, length);
        ::memcpy(m_buffer + index, srcBuffer, static_cast<std::size_t>(length));
    }

    inline void getBytes(util::index_t index, std::uint8_t *dst, util::index_t length) const
    {
        boundsCheck(index, length);
        ::memcpy(dst, m_buffer + index, static_cast<std::size_t>(length));
    }

    inline void setMemory(util::index_t offset, std::size_t length, std::uint8_t value)
    {
        boundsCheck(offset, length);
        ::memset(m_buffer + offset, value, length);
    }

    inline std::string getString(util::index_t offset) const
    {
        std::int32_t length;

        boundsCheck(offset, sizeof(length));
        ::memcpy(reinterpret_cast<char *>(&length), m_buffer + offset, sizeof(length));

        return getStringWithoutLength(offset + sizeof(std::int32_t), static_cast<std::size_t>(length));
    }

    inline std::string getStringWithoutLength(util::index_t offset, std::size_t length) const
    {
        boundsCheck(offset, length);
        return { m_buffer + static_cast<std::size_t>(offset), m_buffer + static_cast<std::size_t>(offset) + length };
    }

    inline std::int32_t getStringLength(util::index_t offset) const
    {
        std::int32_t length;

        boundsCheck(offset, sizeof(length));
        ::memcpy(reinterpret_cast<char *>(&length), m_buffer + offset, sizeof(length));

        return length;
    }

    std::int32_t putString(util::index_t offset, const std::string &value)
    {
        auto length = static_cast<std::int32_t>(value.length());

        boundsCheck(offset, value.length() + sizeof(length));

        ::memcpy(m_buffer + offset, reinterpret_cast<const char *>(&length), sizeof(length));
        ::memcpy(m_buffer + offset + sizeof(length), value.c_str(), value.length());

        return static_cast<std::int32_t>(sizeof(std::int32_t)) + length;
    }

    std::int32_t COND_MOCK_VIRTUAL putStringWithoutLength(util::index_t offset, const std::string &value)
    {
        boundsCheck(offset, value.length());
        ::memcpy(m_buffer + offset, value.c_str(), value.length());
        return static_cast<std::int32_t>(value.length());
    }

#if !defined(DISABLE_BOUNDS_CHECKS)
    inline void boundsCheck(util::index_t index, std::uint64_t length) const
    {
        if (AERON_COND_EXPECT(index < 0 || (static_cast<std::uint64_t>(m_length) - index) < length, false))
        {
            throw aeron::util::OutOfBoundsException(
                aeron::util::strPrintf(
                    "index out of bounds[%p]: index=%lld + %lld, capacity=%lld",
                    this, static_cast<long long>(index), length, static_cast<long long>(m_length)),
                SOURCEINFO, EINVAL);
        }
    }
#else
    inline void boundsCheck(util::index_t, std::uint64_t) const {}
#endif

private:
    // The internal length type used by the atomic buffer
    typedef std::uint32_t length_t;

    std::uint8_t *m_buffer = nullptr;
    length_t m_length = 0;
};

}}

#endif
