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
#include "util/aeron_platform.h"

#if defined(AERON_COMPILER_MSVC)
#include "aeron_windows.h"
#include "util/aeron_error.h"

#include <WinSock2.h>
#include <Windows.h>
#include <time.h>
#include <intrin.h>

#include "concurrent/aeron_thread.h"
#include "aeron_alloc.h"

#define __builtin_bswap32 _byteswap_ulong
#define __builtin_bswap64 _byteswap_uint64
#define __builtin_popcount __popcnt
#define __builtin_popcountll __popcnt64

BOOL WINAPI DllMain(HINSTANCE hinstDLL, DWORD fdwReason, LPVOID lpvReserved)
{
    switch (fdwReason)
    {
        case DLL_PROCESS_ATTACH:
            if (!aeron_error_dll_process_attach())
            {
                return FALSE;
            }
            break;

        case DLL_THREAD_DETACH:
            aeron_error_dll_thread_detach();
            break;

        case DLL_PROCESS_DETACH:
            aeron_error_dll_process_detach();
            break;

        default:
            break;
    }

    return TRUE;
}

typedef struct { UINT64 q[2]; } aeron_uint128_t;

aeron_uint128_t make_aeron_uint128_t(UINT64 x)
{
    aeron_uint128_t result;
    result.q[0] = x;
    result.q[1] = 0;
    return result;
}

aeron_uint128_t aeron_uint128_bitwise_negate(aeron_uint128_t x)
{
    aeron_uint128_t r;
    r.q[0] = ~x.q[0];
    r.q[1] = ~x.q[1];
    return r;
}

BOOL aeron_uint128_equals(const aeron_uint128_t lhs, const aeron_uint128_t rhs)
{
    return lhs.q[0] == rhs.q[0] && lhs.q[1] == rhs.q[1];
}

aeron_uint128_t aeron_uint128_bitwise_shift_left(const aeron_uint128_t lhs, size_t n)
{
    aeron_uint128_t result = lhs;

    if (n >= 128)
    {
        result.q[1] = 0;
        result.q[0] = 0;
    }
    else
    {
        const unsigned int halfsize = 128 / 2;

        if (n >= halfsize)
        {
            n -= halfsize;
            result.q[1] = result.q[0];
            result.q[0] = 0;
        }

        if (n != 0)
        {
            // shift high half
            result.q[1] <<= n;

            const UINT64 mask = (~((UINT64)-1) >> n);

            // and add them to high half
            result.q[1] |= (result.q[0] & mask) >> (halfsize - n);

            // and finally shift also low half
            result.q[0] <<= n;
        }
    }

    return result;
}

aeron_uint128_t aeron_uint128_sub(const aeron_uint128_t lhs, const aeron_uint128_t rhs)
{
    aeron_uint128_t result = lhs;

    result.q[1] -= rhs.q[1];
    result.q[0] -= rhs.q[0];

    if (rhs.q[0] >= lhs.q[0])
    {
        result.q[1] -= 1;
    }

    return result;
}

aeron_uint128_t aeron_uint128_bitwise_and(const aeron_uint128_t lhs, const aeron_uint128_t rhs)
{
    aeron_uint128_t result;
    result.q[0] = lhs.q[0] & rhs.q[0];
    result.q[1] = lhs.q[1] & rhs.q[1];
    return result;
}

aeron_uint128_t aeron_ipv6_netmask_from_prefixlen(size_t prefixlen)
{
    aeron_uint128_t netmask;

    if (0 == prefixlen)
    {
        netmask.q[1] = 0;
        netmask.q[0] = 0;
    }
    else
    {
        netmask = aeron_uint128_bitwise_negate(aeron_uint128_sub(aeron_uint128_bitwise_shift_left(
            make_aeron_uint128_t(1), (128 - prefixlen)), make_aeron_uint128_t(1)));
    }

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    UINT64 lo = netmask.q[0];
    netmask.q[0] = __builtin_bswap64(netmask.q[1]);
    netmask.q[1] = __builtin_bswap64(lo);
#endif

    return netmask;
}

BOOL aeron_ipv6_does_prefix_match(struct in6_addr *in6_addr1, struct in6_addr *in6_addr2, size_t prefixlen)
{
    aeron_uint128_t addr1;
    aeron_uint128_t addr2;
    const aeron_uint128_t netmask = aeron_ipv6_netmask_from_prefixlen(prefixlen);

    memcpy(&addr1, in6_addr1, sizeof(addr1));
    memcpy(&addr2, in6_addr2, sizeof(addr2));

    return aeron_uint128_equals(aeron_uint128_bitwise_and(addr1, netmask), aeron_uint128_bitwise_and(addr2, netmask));
}

double aeron_erand48(unsigned short xsubi[3])
{
    // POSIX-compatible 48-bit linear congruential generator: X_{n+1} = (a * X_n + c) mod 2^48.
    // Standard drand48/erand48 constants. Per POSIX, xsubi[0] holds the low 16 bits of the
    // state and xsubi[2] the high 16 bits.
    static const uint64_t a = 0x5DEECE66DULL;
    static const uint64_t c = 0xBULL;
    static const uint64_t mask48 = (1ULL << 48) - 1ULL;

    uint64_t x = (uint64_t)xsubi[0] | ((uint64_t)xsubi[1] << 16) | ((uint64_t)xsubi[2] << 32);
    x = (a * x + c) & mask48;
    xsubi[0] = (unsigned short)(x & 0xFFFF);
    xsubi[1] = (unsigned short)((x >> 16) & 0xFFFF);
    xsubi[2] = (unsigned short)((x >> 32) & 0xFFFF);

    return (double)x / (double)(1ULL << 48);
}

// Global state for drand48/srand48. POSIX-compatible default matches "as if srand48(1) was called".
static unsigned short aeron_drand48_state[3] = { 0x330E, 0x0001, 0x0000 };

void aeron_srand48(uint64_t aeron_nano_clock)
{
    // POSIX srand48 uses only the low 32 bits of the seed and sets the low 16 of X to 0x330E.
    aeron_drand48_state[0] = 0x330E;
    aeron_drand48_state[1] = (unsigned short)(aeron_nano_clock & 0xFFFF);
    aeron_drand48_state[2] = (unsigned short)((aeron_nano_clock >> 16) & 0xFFFF);
}

double aeron_drand48()
{
    return aeron_erand48(aeron_drand48_state);
}

void localtime_r(const time_t *timep, struct tm *result)
{
    localtime_s(result, timep);
}

#else

typedef int aeron_make_into_non_empty_translation_unit_t;

#endif
