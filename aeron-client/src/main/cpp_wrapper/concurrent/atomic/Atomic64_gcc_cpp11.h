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
#ifndef AERON_CONCURRENT_ATOMIC64_GCC_CPP11_H
#define AERON_CONCURRENT_ATOMIC64_GCC_CPP11_H

#include <atomic>
#include <thread>
#include <cstdint>

// Implement all operations using C++11 standard library atomics and GCC intrinsics.
// Not as fast as the x64 specializations, but allows Aeron to work on other platforms (e.g. ARM).
// See: https://gcc.gnu.org/onlinedocs/gcc/_005f_005fatomic-Builtins.html

namespace aeron { namespace concurrent { namespace atomic {

inline void thread_fence()
{
    std::atomic_thread_fence(std::memory_order_acq_rel);
}

inline void fence()
{
    std::atomic_thread_fence(std::memory_order_seq_cst);
}

inline void acquire()
{
    std::atomic_thread_fence(std::memory_order_acquire);
}

inline void release()
{
    std::atomic_thread_fence(std::memory_order_release);
}

inline void cpu_pause()
{
}

inline std::int32_t getInt32Volatile(volatile std::int32_t *source)
{
    std::int32_t sequence = *reinterpret_cast<volatile std::int32_t *>(source);
    acquire();

    return sequence;
}

inline void putInt32Volatile(volatile std::int32_t *source, std::int32_t value)
{
    __atomic_store(source, &value, __ATOMIC_SEQ_CST);
}

inline void putInt32Ordered(volatile std::int32_t *source, std::int32_t value)
{
    release();
    *reinterpret_cast<volatile std::int32_t *>(source) = value;
}

inline void putInt32Atomic(volatile std::int32_t *address, std::int32_t value)
{
    __atomic_store(address, &value, __ATOMIC_SEQ_CST);
}

inline std::int64_t getInt64Volatile(volatile std::int64_t *source)
{
    std::int64_t sequence = *reinterpret_cast<volatile std::int64_t *>(source);
    acquire();

    return sequence;
}

inline void putInt64Volatile(volatile std::int64_t *address, std::int64_t value)
{
    __atomic_store(address, &value, __ATOMIC_SEQ_CST);
}

inline void putInt64Ordered(volatile std::int64_t *address, std::int64_t value)
{
    release();
    *reinterpret_cast<volatile std::int64_t *>(address) = value;
}

inline void putInt64Atomic(volatile std::int64_t *address, std::int64_t value)
{
    __atomic_store(address, &value, __ATOMIC_SEQ_CST);
}

inline std::int64_t getAndAddInt64(volatile std::int64_t *address, std::int64_t value)
{
    return __atomic_fetch_add(address, value, __ATOMIC_SEQ_CST);
}

inline std::int32_t getAndAddInt32(volatile std::int32_t *address, std::int32_t value)
{
    return __atomic_fetch_add(address, value, __ATOMIC_SEQ_CST);
}

inline std::int32_t xchg(volatile std::int32_t *address, std::int32_t value)
{
    std::int32_t original;
    __atomic_exchange(address, &value, &original, __ATOMIC_SEQ_CST);
    return original;
}

inline std::int64_t xchg(volatile std::int64_t *address, std::int64_t value)
{
    std::int64_t original;
    __atomic_exchange(address, &value, &original, __ATOMIC_SEQ_CST);
    return original;
}

inline std::int32_t cmpxchg(volatile std::int32_t *address, std::int32_t expected, std::int32_t desired)
{
    // on failure, expected is updated with the value observed by the compare and exchange
    __atomic_compare_exchange(address, &expected, &desired, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
    return expected;
}

inline std::int64_t cmpxchg(volatile std::int64_t *address, std::int64_t expected, std::int64_t desired)
{
    // on failure, expected is updated with the value observed by the compare and exchange
    __atomic_compare_exchange(address, &expected, &desired, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
    return expected;
}

//-------------------------------------
//  Alignment
//-------------------------------------
// Note: May not work on local variables.
// http://gcc.gnu.org/bugzilla/show_bug.cgi?id=24691
#define AERON_DECL_ALIGNED(declaration, amt) declaration __attribute__((aligned(amt)))

}}}

#endif