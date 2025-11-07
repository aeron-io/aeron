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

#ifndef AERON_BITUTIL_H
#define AERON_BITUTIL_H

#include <assert.h>
#include <stdint.h>
#include <stddef.h>
#include "util/aeron_platform.h"

#if defined(_MSC_VER)
#include <intrin.h>
#pragma intrinsic(_BitScanForward)
#pragma intrinsic(_BitScanReverse)
#pragma intrinsic(_BitScanForward64)
#pragma intrinsic(_BitScanReverse64)
#endif

#define AERON_CACHE_LINE_LENGTH (64u)

#define AERON_ALIGN(value, alignment) (((value) + ((alignment) - 1u)) & ~((alignment) - 1u))

#define AERON_PADDED_SIZEOF(_struct) AERON_ALIGN(sizeof(_struct), sizeof(int32_t))

#define AERON_IS_POWER_OF_TWO(value) ((value) > 0 && (((value) & (~(value) + 1u)) == (value)))

#define AERON_MIN(a, b) ((a) < (b) ? (a) : (b))

#if defined(__GNUC__)
#define AERON_C_COND_EXPECT(exp, c) (__builtin_expect((exp), c))
#else
#define AERON_C_COND_EXPECT(exp, c) (exp)
#endif

inline uint8_t *aeron_cache_line_align_buffer(uint8_t *buffer)
{
    size_t remainder = ((size_t)buffer) % AERON_CACHE_LINE_LENGTH;
    return 0 == remainder ? buffer : (buffer + (AERON_CACHE_LINE_LENGTH - remainder));
}

/* Taken from Hacker's Delight as ntz10 at http://www.hackersdelight.org/hdcodetxt/ntz.c.txt */
inline int aeron_number_of_trailing_zeroes(int32_t value)
{
    if (0 == value)
    {
        return 32;
    }
#if defined(__GNUC__)
    return __builtin_ctz(value);
#elif defined(_MSC_VER)
    unsigned long r;
    _BitScanForward(&r, (unsigned long)value);
    return (int)r;
#else
    // Hacker's Delight. Figure 5-26.
    char table[32] =
    {
        0, 1, 2, 24, 3, 19, 6, 25,
        22, 4, 20, 10, 16, 7, 12, 26,
        31, 23, 18, 5, 21, 9, 15, 11,
        30, 17, 8, 14, 29, 13, 28, 27
    };

    uint32_t index = (uint32_t)((value & -value) * 0x04D7651F);

    return table[index >> 27u];
#endif
}

inline int aeron_number_of_trailing_zeroes_u64(uint64_t value)
{
    if (0 == value)
    {
        return 64;
    }
#if defined(__GNUC__)
    return __builtin_ctzll(value);
#elif defined(_MSC_VER)
    unsigned long r;
    _BitScanForward64(&r, (__int64)value);
    return (int)r;
#else
    int lower_tzc = aeron_number_of_trailing_zeroes((int32_t) (value & UINT64_C(0xFFFFFFFF)));
    if (32 != lower_tzc)
    {
        return lower_tzc;
    }
    int upper_tzc = aeron_number_of_trailing_zeroes((int32_t) ((value >> 32u) & UINT64_C(0xFFFFFFFF)));
    return lower_tzc + upper_tzc;
#endif
}

inline int aeron_number_of_leading_zeroes(uint32_t value)
{
    if (0 == value)
    {
        return 32;
    }
#if defined(__GNUC__)
    return __builtin_clz(value);
#elif defined(_MSC_VER)
    unsigned long r;
    _BitScanReverse(&r, (unsigned long)value);
    return 31 - (int)r;
#else
    // Hacker's Delight. Figure 5-18.
    char table[64] =
    {
        32, 20, 19, -1, -1, 18, -1, 7, 10, 17, -1, -1, 14, -1, 6, -1,
        -1, 9, -1, 16, -1, -1, 1, 26, -1, 13, -1, -1, 24, 5, -1, -1,
        -1, 21, -1, 8, 11, -1, 15, -1,  -1,  -1,  -1, 2, 27, 0, 25, -1,
        22, -1, 12, -1, -1, 3, 28, -1, 23, -1, 4, 29, -1, -1, 30, 31
    };

    value = value | (value >> 1);
    value = value | (value >> 2);
    value = value | (value >> 4);
    value = value | (value >> 8);
    value = value & ~(value >> 16);
    value = value * 0xFD7049FF;

    return table[value >> 26];
#endif
}

inline int aeron_number_of_leading_zeroes_u64(uint64_t value)
{
    if (0 == value)
    {
        return 64;
    }
#if defined(__GNUC__)
    return __builtin_clzll(value);
#elif defined(_MSC_VER)
    unsigned long r;
    _BitScanReverse64(&r, (__int64)value);
    return 63 - (int)r;
#else
    int upper_lzc = aeron_number_of_leading_zeroes((int32_t) ((value >> 32u) & UINT64_C(0xFFFFFFFF)));
    if (32 != upper_lzc)
    {
        return upper_lzc;
    }
    int lower_lzc = aeron_number_of_leading_zeroes((int32_t) (value & UINT64_C(0xFFFFFFFF)));
    return upper_lzc + lower_lzc;
#endif
}

inline int32_t aeron_find_next_power_of_two(int32_t value)
{
    value--;

    /*
     * Set all bits below the leading one using binary expansion
     * http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
     */
    for (size_t i = 1; i < sizeof(value) * 8; i = i * 2)
    {
        value |= (value >> i);
    }

    return value + 1;
}

int32_t aeron_randomised_int32(void);

#endif //AERON_BITUTIL_H
