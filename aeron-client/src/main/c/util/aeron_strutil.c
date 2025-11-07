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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <time.h>
#include <stdio.h>
#include <inttypes.h>
#include <errno.h>
#include <string.h>
#include <locale.h>
#include <stdlib.h>

#define AERON_DLL_EXPORTS

#include "util/aeron_strutil.h"

void aeron_format_date(char *str, size_t count, int64_t timestamp)
{
    char time_buffer[80];
    char msec_buffer[8];
    char tz_buffer[8];
    struct tm time;
    time_t just_seconds = timestamp / 1000;
    int64_t msec_after_sec = timestamp % 1000;

    localtime_r(&just_seconds, &time);

    strftime(time_buffer, sizeof(time_buffer) - 1, "%Y-%m-%d %H:%M:%S.", &time);
    snprintf(msec_buffer, sizeof(msec_buffer) - 1, "%03" PRId64, msec_after_sec);
    strftime(tz_buffer, sizeof(tz_buffer) - 1, "%z", &time);

    snprintf(str, count, "%s%s%s", time_buffer, msec_buffer, tz_buffer);
}

static size_t aeron_format_min(size_t a, size_t b)
{
    return a < b ? a : b;
}

static size_t aeron_format_number_next(long long value, const char *sep, char *buffer, size_t buffer_len)
{
    if (0 <= value && value < 1000)
    {
        return snprintf(buffer, aeron_format_min(4, buffer_len), "%lld", value);
    }
    else if (-1000 < value && value < 0)
    {
        return snprintf(buffer, aeron_format_min(5, buffer_len), "%lld", value);
    }
    else
    {
        size_t printed_len = aeron_format_number_next(value / 1000, sep, buffer, buffer_len);
        return printed_len + snprintf(
            buffer + printed_len, aeron_format_min(5, buffer_len - printed_len), "%s%03lld", sep, llabs(value % 1000));
    }
}

/**
 * Uses locale specific thousands separator to format the number, or "," if none found. Will truncate to buffer_len - 1
 * and null terminate. Use AERON_FORMAT_NUMBER_TO_LOCALE_STR_LEN for the buffer size to prevent truncations. Works for
 * string lengths up to INT64_MIN.
 *
 * @param value Value for format
 * @param buffer buffer to use
 * @param buffer_len length of buffer
 * @return NULL terminated buffer containing formatted string.
 */
char *aeron_format_number_to_locale(long long value, char *buffer, size_t buffer_len)
{
    setlocale(LC_NUMERIC, "");
    const char *sep = 1 == strlen(localeconv()->thousands_sep) ? localeconv()->thousands_sep : ",";
    aeron_format_number_next(value, sep, buffer, buffer_len);
    buffer[buffer_len - 1] = '\0';
    return buffer;
}

void aeron_format_to_hex(char *str, size_t str_length, const uint8_t *data, size_t data_len)
{
    static char table[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    size_t j = 0;

    for (size_t i = 0; i < data_len && j < str_length; i++)
    {
        char c_high = table[(data[i] >> 4) & 0x0F];
        char c_low = table[data[i] & 0x0F];

        str[j++] = c_high;
        str[j++] = c_low;
    }

    str[j] = '\0';
}

int aeron_tokenise(char *input, char delimiter, int max_tokens, char **tokens)
{
    if (NULL == input)
    {
        return -EINVAL;
    }

    const size_t len = strlen(input);

    if (INT32_MAX < len)
    {
        return -EINVAL;
    }

    if (0 == len)
    {
        return 0;
    }

    int num_tokens = 0;

    for (int i = (int)len; --i != -1;)
    {
        if (delimiter == input[i])
        {
            input[i] = '\0';
        }

        if (0 == i && '\0' != input[i])
        {
            if (max_tokens <= num_tokens)
            {
                num_tokens = -ERANGE;
                break;
            }

            tokens[num_tokens] = &input[i];
            num_tokens++;
        }
        else if ('\0' == input[i] && '\0' != input[i + 1])
        {
            if (max_tokens <= num_tokens)
            {
                num_tokens = -ERANGE;
                break;
            }

            tokens[num_tokens] = &input[i + 1];
            num_tokens++;
        }
    }

    return num_tokens;
}

int aeron_digit_count(uint32_t value)
{
    static uint64_t table[] = {
        4294967296,  8589934582,  8589934582,  8589934582,  12884901788,
        12884901788, 12884901788, 17179868184, 17179868184, 17179868184,
        21474826480, 21474826480, 21474826480, 21474826480, 25769703776,
        25769703776, 25769703776, 30063771072, 30063771072, 30063771072,
        34349738368, 34349738368, 34349738368, 34349738368, 38554705664,
        38554705664, 38554705664, 41949672960, 41949672960, 41949672960,
        42949672960, 42949672960 };
    int log2 = 63 - aeron_number_of_leading_zeroes_u64((uint64_t)value | 1);
    return (int)((value + table[log2]) >> 32);
}

#if defined(AERON_COMPILER_MSVC)

char *aeron_strndup(const char *value, size_t length)
{
    size_t str_length = strlen(value);
    char *dup = NULL;

    str_length = (str_length > length) ? length : str_length;
    if (aeron_alloc((void **)&dup, str_length + 1) < 0)
    {
        errno = ENOMEM;
        return NULL;
    }

    strncpy(dup, value, str_length);
    dup[str_length] = '\0';
    return dup;
}

#ifndef AERON_NO_GETOPT

// Taken and modified from https://www.codeproject.com/KB/cpp/xgetopt/XGetopt_demo.zip
// *****************************************************************
//
// Author:  Hans Dietrich
//          hdietrich2@hotmail.com
//
// This software is released into the public domain.
// You are free to use it in any way you like.
//
// This software is provided "as is" with no expressed
// or implied warranty.  I accept no liability for any
// damage or loss of business that this software may cause.
//
// ******************************************************************

AERON_EXPORT char *optarg = NULL;
AERON_EXPORT int optind = 1;

int getopt(int argc, char *const argv[], const char *opt_string)
{
    static char *next = NULL;
    if (optind == 0)
    {
        next = NULL;
    }

    optarg = NULL;

    if (next == NULL || *next == '\0')
    {
        if (optind == 0)
        {
            optind++;
        }

        if (optind >= argc || argv[optind][0] != '-' || argv[optind][1] == '\0')
        {
            optarg = NULL;
            if (optind < argc)
            {
                optarg = argv[optind];
            }

            return EOF;
        }

        if (strcmp(argv[optind], "--") == 0)
        {
            optind++;
            optarg = NULL;
            if (optind < argc)
            {
                optarg = argv[optind];
            }

            return EOF;
        }

        next = argv[optind];
        next++;
        optind++;
    }

    char c = *next++;
    char *cp = strchr(opt_string, c);

    if (cp == NULL || c == ':')
    {
        return '?';
    }

    cp++;
    if (*cp == ':')
    {
        if (*next != '\0')
        {
            optarg = next;
            next = NULL;
        }
        else if (optind < argc)
        {
            optarg = argv[optind];
            optind++;
        }
        else
        {
            return '?';
        }
    }

    return c;
}
#endif

#endif

extern uint64_t aeron_fnv_64a_buf(uint8_t *buf, size_t len);
extern bool aeron_str_length(const char *str, size_t length_bound, size_t *length);
extern void aeron_str_null_terminate(uint8_t *text, int index);
