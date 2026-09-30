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

#ifndef AERON_ATOMIC64_C11_H
#define AERON_ATOMIC64_C11_H

#include <stdbool.h>
#include <stdint.h>

#define AERON_GET_ACQUIRE(dst, src)                                           \
do                                                                            \
{                                                                             \
    AERON_ATOMIC_ASSERT_VOLATILE_LVALUE(                                      \
        src,                                                                  \
        "AERON_GET_ACQUIRE: src must be a volatile lvalue"); \
    dst = (src);                                                              \
    __atomic_thread_fence(__ATOMIC_ACQUIRE);                                  \
}                                                                             \
while (false)

#define AERON_SET_RELEASE(dst, src)                                           \
do                                                                            \
{                                                                             \
    AERON_ATOMIC_ASSERT_VOLATILE_LVALUE(                                      \
        dst,                                                                  \
        "AERON_SET_RELEASE: dst must be a volatile lvalue"); \
    __atomic_thread_fence(__ATOMIC_RELEASE);                                  \
    (dst) = (src);                                                            \
}                                                                             \
while (false)

#define AERON_GET_AND_ADD_INT64(original, dst, value)                         \
do                                                                            \
{                                                                             \
    original = __atomic_fetch_add(&(dst), value, __ATOMIC_SEQ_CST);           \
}                                                                             \
while (false)                                                                 \

#define AERON_GET_AND_ADD_INT32(original, dst, value)                         \
do                                                                            \
{                                                                             \
    original = __atomic_fetch_add(&(dst), value, __ATOMIC_SEQ_CST);           \
}                                                                             \
while (false)                                                                 \

inline bool aeron_cas_int64(volatile int64_t *dst, int64_t expected, int64_t desired)
{
    return __atomic_compare_exchange_n(
        dst, &expected, desired, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
}

inline bool aeron_cas_uint64(volatile uint64_t *dst, uint64_t expected, uint64_t desired)
{
    return __atomic_compare_exchange_n(
        dst, &expected, desired, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
}

inline bool aeron_cas_int32(volatile int32_t *dst, int32_t expected, int32_t desired)
{
    return __atomic_compare_exchange_n(
        dst, &expected, desired, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
}

inline void aeron_acquire(void)
{
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
}

inline void aeron_release(void)
{
    __atomic_thread_fence(__ATOMIC_RELEASE);
}

/*-------------------------------------
 *  Alignment
 *-------------------------------------
 * Note: May not work on local variables.
 * http://gcc.gnu.org/bugzilla/show_bug.cgi?id=24691
 */
#define AERON_DECL_ALIGNED(declaration, amt) declaration __attribute__((aligned(amt)))

#endif //AERON_ATOMIC64_C11_H
