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

#ifndef AERON_ATOMIC_H
#define AERON_ATOMIC_H

#include "util/aeron_platform.h"

#define AERON_ATOMIC_STR2(x) #x
#define AERON_ATOMIC_STR(x)  AERON_ATOMIC_STR2(x)
#define AERON_ATOMIC_LOC __FILE__ ":" AERON_ATOMIC_STR(__LINE__)

#if defined(__cplusplus)

#include <type_traits>

/*
 * C++ version:
 *  - decltype((expr)) yields T& / volatile T& for lvalues
 *  - require lvalue reference
 *  - require volatile on the referred-to type
 */
#define AERON_ATOMIC_ASSERT_VOLATILE_LVALUE(expr, msg)                               \
_Static_assert(                                                                      \
    std::is_lvalue_reference<decltype((expr))>::value &&                             \
    std::is_volatile<typename std::remove_reference<decltype((expr))>::type>::value, \
    msg " violation at " AERON_ATOMIC_LOC " (expr: " #expr ")");

#elif defined(AERON_COMPILER_GCC)

#define AERON_ATOMIC_ASSERT_VOLATILE_LVALUE(expr, msg)                               \
_Static_assert(                                                                      \
    __builtin_types_compatible_p(__typeof__(&(expr)),                                \
    volatile __typeof__(expr) *),                                                    \
    msg " violation at " AERON_ATOMIC_LOC " (expr: " #expr ")");

#else

#define AERON_ATOMIC_ASSERT_VOLATILE_LVALUE(expr, msg)                               \
do                                                                                   \
{                                                                                    \
    (void)(expr);                                                                    \
}                                                                                    \
while (false)

#endif

#if defined(AERON_COMPILER_GCC) && defined(AERON_CPU_X64)
    #include "concurrent/aeron_atomic64_gcc_x86_64.h"
#elif defined(AERON_COMPILER_GCC) && defined(AERON_CPU_ARM)
    #include "concurrent/aeron_atomic64_c11.h"
#elif defined(AERON_COMPILER_MSVC) && defined(AERON_CPU_X64)
    #include "concurrent/aeron_atomic64_msvc.h"
#else
    #error Unsupported platform!
#endif

#endif //AERON_ATOMIC_H
