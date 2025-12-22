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
#ifndef AERON_DRIVER_PLATFORM_H
#define AERON_DRIVER_PLATFORM_H

/*
 * Determine platform, compiler, and CPU and set defines to be used later.
 * Also, error out here if on a platform that is not supported.
 */

#if defined(_MSC_VER)
    #define AERON_COMPILER_MSVC 1

    #if defined(_M_X64)
        #define AERON_CPU_X64 1
    #else
        #error Unsupported CPU!
    #endif

    #define _Static_assert static_assert
#elif defined(__GNUC__)
    #define AERON_COMPILER_GCC 1

    #if defined(__llvm__)
        #define AERON_COMPILER_LLVM 1
    #endif

    #if defined(__x86_64__)
        #define AERON_CPU_X64 1
    #endif

    #if defined(__aarch64__)
        #define AERON_CPU_ARM 1
        #if defined(__STDC_NO_ATOMICS__)
            #error C11 atomics are required to compile for aarch64!
        #endif
    #endif

#else
    #error Unsupported compiler!
#endif

#endif
