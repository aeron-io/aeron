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

#ifndef AERON_ATOMIC64_GCC_X86_64_H
#define AERON_ATOMIC64_GCC_X86_64_H

#include <stdbool.h>
#include <stdint.h>

#define AERON_GET_ACQUIRE(dst, src) \
do \
{ \
    dst = src; \
    __asm__ volatile("" ::: "memory"); \
} \
while (false) \

#define AERON_SET_RELEASE(dst, src) \
do \
{ \
    __asm__ volatile("" ::: "memory"); \
    dst = src; \
} \
while (false) \

#define AERON_GET_AND_ADD_INT64(original, dst, value) \
do \
{ \
    __asm__ volatile( \
        "lock; xaddq %0, %1" \
        : "=r"(original), "+m"(dst) \
        : "0"((int64_t)value)); \
} \
while (false) \

#define AERON_GET_AND_ADD_INT32(original, dst, value) \
do \
{ \
    __asm__ volatile( \
        "lock; xaddl %0, %1" \
        : "=r"(original), "+m"(dst) \
        : "0"(value)); \
} \
while (false) \

inline bool aeron_cas_int64(volatile int64_t *dst, int64_t expected, int64_t desired)
{
    int64_t original;
    __asm__ volatile(
        "lock; cmpxchgq %2, %1"
        : "=a"(original), "+m"(*dst)
        : "r"(desired), "0"(expected));

    return original == expected;
}

inline bool aeron_cas_uint64(volatile uint64_t *dst, uint64_t expected, uint64_t desired)
{
    uint64_t original;
    __asm__ volatile(
        "lock; cmpxchgq %2, %1"
        : "=a"(original), "+m"(*dst)
        : "r"(desired), "0"(expected));

    return original == expected;
}

inline bool aeron_cas_int32(volatile int32_t *dst, int32_t expected, int32_t desired)
{
    int32_t original;
    __asm__ volatile(
        "lock; cmpxchgl %2, %1"
        : "=a"(original), "+m"(*dst)
        : "r"(desired), "0"(expected));

    return original == expected;
}

inline void aeron_acquire(void)
{
    volatile int64_t *dummy;
    __asm__ volatile("movq 0(%%rsp), %0" : "=r"(dummy) : : "memory");
}

inline void aeron_release(void)
{
    volatile int64_t dummy = 0;
    (void)dummy;
    __asm__ volatile("" ::: "memory");
}


/*-------------------------------------
 *  Alignment
 *-------------------------------------
 * Note: May not work on local variables.
 * http://gcc.gnu.org/bugzilla/show_bug.cgi?id=24691
 */
#define AERON_DECL_ALIGNED(declaration, amt) declaration __attribute__((aligned(amt)))

#endif //AERON_ATOMIC64_GCC_X86_64_H
