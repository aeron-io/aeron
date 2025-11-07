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

#ifndef AERON_STRUTIL_H
#define AERON_STRUTIL_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "aeron_bitutil.h"
#include "util/aeron_platform.h"

#define AERON_FORMAT_DATE_MAX_LENGTH (100)
#define AERON_FORMAT_NUMBER_TO_LOCALE_STR_LEN (32)
#define AERON_FORMAT_HEX_LENGTH(b) ((2 * (b)) + 1)

#if defined(AERON_DLL_EXPORTS)
#define AERON_EXPORT __declspec(dllexport)
#else
#define AERON_EXPORT __declspec(dllimport)
#endif

#if defined(AERON_COMPILER_GCC)

#define aeron_strndup strndup

#elif defined(AERON_COMPILER_MSVC)

char *aeron_strndup(const char *value, size_t length);

#ifndef AERON_NO_GETOPT
AERON_EXPORT extern char *optarg;
AERON_EXPORT extern int optind;

int getopt(int argc, char *const argv[], const char *opt_string);
#endif

#endif

void aeron_format_date(char *str, size_t count, int64_t timestamp);

char *aeron_format_number_to_locale(long long value, char *buffer, size_t buffer_len);

void aeron_format_to_hex(char *str, size_t str_length, const uint8_t *data, size_t data_len);

/*
 * FNV-1a hash function
 *
 * http://www.isthe.com/chongo/tech/comp/fnv/index.html
 */
inline uint64_t aeron_fnv_64a_buf(uint8_t *buf, size_t len)
{
    uint8_t *bp = buf;
    uint8_t *be = bp + len;
    uint64_t hval = 0xcbf29ce484222325ULL;

    while (bp < be)
    {
        hval ^= (uint64_t)*bp++;
#if defined(__GNUC__)
        hval += (hval << 1) + (hval << 4) + (hval << 5) + (hval << 7) + (hval << 8) + (hval << 40);
#else
        hval *= ((uint64_t)0x100000001b3ULL);
#endif
    }

    return hval;
}

/*
 * Splits a null terminated string using the delimiter specified, which is replaced with \0 characters.
 * Each of the tokens is stored in reverse order in the tokens array.
 *
 * Returns the number of tokens found. Or a value < 0 for an error:
 * ERANGE: number of tokens is greater than max_tokens.
 */
int aeron_tokenise(char *input, char delimiter, int max_tokens, char **tokens);

/**
 * Checks that the string length is strictly less than the specified bound.
 *
 * @param str           String to be compared.
 * @param length_bound  Limit to the length of the string being checked.
 * @param length        Out parameter for the actual length of the string (if non-null and less than length_bound).
 *                      NULL values for this parameter are permitted.  If str is NULL this value is unmodified.  If
 *                      the function returns false, the value is also unmodified.
 * @return              true if less than the specified bound, NULL is always true.
 */
inline bool aeron_str_length(const char *str, size_t length_bound, size_t *length)
{
    if (NULL == str)
    {
        return true;
    }

    size_t i = 0;
    bool result = false;
    for (; i < length_bound; i++)
    {
        if ('\0' == str[i])
        {
            result = true;
            break;
        }
    }

    if (result && NULL != length)
    {
        *length = i;
    }

    return result;
}

inline void aeron_str_null_terminate(uint8_t *text, int index)
{
    text[index] = '\0';
}

int aeron_digit_count(uint32_t value);

#endif //AERON_STRUTIL_H
