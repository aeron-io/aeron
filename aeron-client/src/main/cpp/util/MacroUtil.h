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
#ifndef AERON_UTIL_MACRO_UTIL_FILE_H
#define AERON_UTIL_MACRO_UTIL_FILE_H

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)

#define CONCAT_SYMBOLS(x, y) x##y

#include <cstdint>
#include <string>

namespace aeron { namespace util {

inline static constexpr std::int32_t semanticVersionCompose(
    std::uint8_t major, std::uint8_t minor, std::uint8_t patch) noexcept
{
    return (major << 16) | (minor << 8) | patch;
}

inline static constexpr std::uint8_t semanticVersionMajor(std::int32_t version) noexcept
{
    return static_cast<std::uint8_t>((version >> 16) & 0xFF);
}

inline static constexpr std::uint8_t semanticVersionMinor(std::int32_t version) noexcept
{
    return static_cast<std::uint8_t>((version >> 8) & 0xFF);
}

inline static constexpr std::uint8_t semanticVersionPatch(std::int32_t version) noexcept
{
    return static_cast<std::uint8_t>(version & 0xFF);
}

inline static std::string semanticVersionToString(std::int32_t version) noexcept
{
    return
        std::to_string(semanticVersionMajor(version)) + "." +
        std::to_string(semanticVersionMinor(version)) + "." +
        std::to_string(semanticVersionPatch(version));
}

}}

#if COND_MOCK == 1
    #define COND_MOCK_VIRTUAL virtual
#else
    #define COND_MOCK_VIRTUAL
#endif

#if defined(__GNUC__)
    #define AERON_COND_EXPECT(exp, c) (__builtin_expect((exp), c))
#else
    #define AERON_COND_EXPECT(exp, c) (exp)
#endif

#endif
