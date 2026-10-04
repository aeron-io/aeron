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
#ifndef AERON_CONCURRENT_ATOMIC_COUNTER_H
#define AERON_CONCURRENT_ATOMIC_COUNTER_H

#include <cstdint>
#include <memory>

#include "aeronc.h"

#include "util/Index.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/CountersReader.h"

namespace aeron { namespace concurrent {

class AtomicCounter
{
public:
    explicit AtomicCounter(aeron_counter_t *counter) : m_counter(counter), m_ptr(aeron_counter_addr(counter))
    {
        aeron_counter_constants(m_counter, &m_constants);
    }

    AtomicCounter(std::int64_t *ptr, std::int64_t registrationId, std::int32_t counterId) :
        m_ptr(ptr)
    {
        m_constants.registration_id = registrationId;
        m_constants.counter_id = counterId;
    }

    ~AtomicCounter()
    {
        close();
    }

    inline std::int32_t id() const
    {
        return m_constants.counter_id;
    }

    inline void increment()
    {
        atomic::getAndAddInt64(m_ptr, 1);
    }

    inline void incrementOrdered()
    {
        std::int64_t currentValue = *m_ptr;
        atomic::putInt64Ordered(m_ptr, currentValue + 1);
    }

    inline void set(std::int64_t value)
    {
        atomic::putInt64Atomic(m_ptr, value);
    }

    inline void setOrdered(std::int64_t value)
    {
        atomic::putInt64Ordered(m_ptr, value);
    }

    inline void setWeak(std::int64_t value)
    {
        *m_ptr = value;
    }

    inline std::int64_t getAndAdd(std::int64_t value)
    {
        return atomic::getAndAddInt64(m_ptr, value);
    }

    inline std::int64_t getAndAddOrdered(std::int64_t increment)
    {
        std::int64_t currentValue = *m_ptr;
        atomic::putInt64Ordered(m_ptr, currentValue + increment);
        return currentValue;
    }

    inline std::int64_t getAndSet(std::int64_t value)
    {
        return atomic::xchg(m_ptr, value);
    }

    inline bool compareAndSet(std::int64_t expectedValue, std::int64_t updateValue)
    {
        std::int64_t original = atomic::cmpxchg(m_ptr, expectedValue, updateValue);
        return original == expectedValue;
    }

    inline std::int64_t get() const
    {
        return atomic::getInt64Volatile(m_ptr);
    }

    inline std::int64_t getWeak() const
    {
        return *m_ptr;
    }

protected:
    aeron_counter_t *counter() const
    {
        return m_counter;
    }

    void close()
    {
        if (nullptr != m_counter)
        {
            aeron_counter_close(m_counter, nullptr, nullptr);
            m_counter = nullptr;
        }
    }

private:
    aeron_counter_t *m_counter = nullptr;
    std::int64_t *m_ptr = nullptr;
    aeron_counter_constants_t m_constants = {};
};

}}

#endif
