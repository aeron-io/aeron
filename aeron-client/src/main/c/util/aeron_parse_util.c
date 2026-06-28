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

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <limits.h>
#include <ctype.h>
#include <inttypes.h>
#include <string.h>
#include "util/aeron_parse_util.h"
#include "util/aeron_error.h"
#include "aeronc.h"

const uint64_t AERON_ONE_GIGABYTE = 1073741824ULL;
const uint64_t AERON_ONE_MEGABYTE = 1048576ULL;
const uint64_t AERON_ONE_KILOBYTE = 1024ULL;
const uint64_t AERON_MAX_G_VALUE = 8589934591ULL;
const uint64_t AERON_MAX_M_VALUE = 8796093022207ULL;
const uint64_t AERON_MAX_K_VALUE = 9007199254740991ULL;

const uint64_t AERON_ONE_MICROSECOND_NS = 1000ULL;
const uint64_t AERON_ONE_MILLISECOND_NS = 1000000ULL;
const uint64_t AERON_ONE_SECOND_NS = 1000000000ULL;
const uint64_t AERON_MAX_MICROSECONDS = 9223372036854775ULL;
const uint64_t AERON_MAX_MILLISECONDS = 9223372036854ULL;
const uint64_t AERON_MAX_SECONDS = 9223372036ULL;

int aeron_parse_size64(const char *str, uint64_t *result)
{
    if (NULL == str)
    {
        return -1;
    }

    errno = 0;
    char *end = "";
    const int64_t v = strtoll(str, &end, 10);

    if (0 == v && 0 != errno)
    {
        return -1;
    }

    if (v < 0 || end == str)
    {
        return -1;
    }

    const uint64_t value = (uint64_t)v;

    if ('\0' != *end)
    {
        switch (*end)
        {
            case 'k':
            case 'K':
                if (value > AERON_MAX_K_VALUE)
                {
                    return -1;
                }
                *result = value * AERON_ONE_KILOBYTE;
                break;

            case 'm':
            case 'M':
                if (value > AERON_MAX_M_VALUE)
                {
                    return -1;
                }
                *result = value * AERON_ONE_MEGABYTE;
                break;

            case 'g':
            case 'G':
                if (value > AERON_MAX_G_VALUE)
                {
                    return -1;
                }
                *result = value * AERON_ONE_GIGABYTE;
                break;

            default:
                return -1;
        }
    }
    else
    {
        *result = value;
    }

    return 0;
}

int aeron_format_size64(uint64_t value, char *buffer, size_t buffer_size)
{
    if (value > LLONG_MAX)
    {
        AERON_SET_ERR(EINVAL, "value is out of range: %" PRIu64, value);
        return -1;
    }

    if (value >= AERON_ONE_GIGABYTE)
    {
        uint64_t result = value / AERON_ONE_GIGABYTE;
        if (value == result * AERON_ONE_GIGABYTE)
        {
            int rc = snprintf(buffer, buffer_size, "%" PRIu64 "g", result);
            if (rc < 0)
            {
                AERON_SET_ERR(rc, "failed to format value: %" PRIu64, value);
                return -1;
            }
            return rc;
        }
    }

    if (value >= AERON_ONE_MEGABYTE)
    {
        uint64_t result = value / AERON_ONE_MEGABYTE;
        if (value == result * AERON_ONE_MEGABYTE)
        {
            int rc = snprintf(buffer, buffer_size, "%" PRIu64 "m", result);
            if (rc < 0)
            {
                AERON_SET_ERR(rc, "failed to format value: %" PRIu64, value);
                return -1;
            }
            return rc;
        }
    }

    if (value >= AERON_ONE_KILOBYTE)
    {
        uint64_t result = value / AERON_ONE_KILOBYTE;
        if (value == result * AERON_ONE_KILOBYTE)
        {
            int rc = snprintf(buffer, buffer_size, "%" PRIu64 "k", result);
            if (rc < 0)
            {
                AERON_SET_ERR(rc, "failed to format value: %" PRIu64, value);
                return -1;
            }
            return rc;
        }
    }

    int rc = snprintf(buffer, buffer_size, "%" PRIu64, value);
    if (rc < 0)
    {
        AERON_SET_ERR(rc, "failed to format value: %" PRIu64, value);
        return -1;
    }
    return rc;
}

int aeron_parse_duration_ns(const char *str, uint64_t *result)
{
    if (NULL == str)
    {
        return -1;
    }

    errno = 0;
    char *end = "";
    const int64_t v = strtoll(str, &end, 10);

    if (0 == v && 0 != errno)
    {
        return -1;
    }

    if (v < 0 || end == str)
    {
        return -1;
    }

    const uint64_t value = (uint64_t)v;

    if ('\0' != *end)
    {
        switch (*end)
        {
            case 's':
            case 'S':
                if ('\0' != *(end + 1))
                {
                    return -1;
                }

                if (value > AERON_MAX_SECONDS)
                {
                    *result = LLONG_MAX;
                }
                else
                {
                    *result = value * AERON_ONE_SECOND_NS;
                }
                break;

            case 'm':
            case 'M':
                if ('s' != *(end + 1) && 'S' != *(end + 1) && '\0' != *(end + 2))
                {
                    return -1;
                }

                if (value > AERON_MAX_MILLISECONDS)
                {
                    *result = LLONG_MAX;
                }
                else
                {
                    *result = value * AERON_ONE_MILLISECOND_NS;
                }
                break;

            case 'u':
            case 'U':
                if ('s' != *(end + 1) && 'S' != *(end + 1) && '\0' != *(end + 2))
                {
                    return -1;
                }

                if (value > AERON_MAX_MICROSECONDS)
                {
                    *result = LLONG_MAX;
                }
                else
                {
                    *result = value * AERON_ONE_MICROSECOND_NS;
                }
                break;

            case 'n':
            case 'N':
                if ('s' != *(end + 1) && 'S' != *(end + 1) && '\0' != *(end + 2))
                {
                    return -1;
                }

                *result = value;
                break;

            default:
                return -1;
        }
    }
    else
    {
        *result = value;
    }

    return 0;
}

int aeron_format_duration_ns(uint64_t duration_ns, char *buffer, size_t buffer_size)
{
    if (duration_ns > LLONG_MAX)
    {
        AERON_SET_ERR(EINVAL, "duration is out of range: %" PRIu64, duration_ns);
        return -1;
    }

    if (duration_ns >= AERON_ONE_SECOND_NS)
    {
        uint64_t result = duration_ns / AERON_ONE_SECOND_NS;
        if (duration_ns == result * AERON_ONE_SECOND_NS)
        {
            int rc = snprintf(buffer, buffer_size, "%" PRIu64 "s", result);
            if (rc < 0)
            {
                AERON_SET_ERR(rc, "failed to format value: %" PRIu64, duration_ns);
                return -1;
            }
            return rc;
        }
    }

    if (duration_ns >= AERON_ONE_MILLISECOND_NS)
    {
        uint64_t result = duration_ns / AERON_ONE_MILLISECOND_NS;
        if (duration_ns == result * AERON_ONE_MILLISECOND_NS)
        {
            int rc = snprintf(buffer, buffer_size, "%" PRIu64 "ms", result);
            if (rc < 0)
            {
                AERON_SET_ERR(rc, "failed to format value: %" PRIu64, duration_ns);
                return -1;
            }
            return rc;
        }
    }

    if (duration_ns >= AERON_ONE_MICROSECOND_NS)
    {
        uint64_t result = duration_ns / AERON_ONE_MICROSECOND_NS;
        if (duration_ns == result * AERON_ONE_MICROSECOND_NS)
        {
            int rc = snprintf(buffer, buffer_size, "%" PRIu64 "us", result);
            if (rc < 0)
            {
                AERON_SET_ERR(rc, "failed to format value: %" PRIu64, duration_ns);
                return -1;
            }
            return rc;
        }
    }

    int rc = snprintf(buffer, buffer_size, "%" PRIu64 "ns", duration_ns);
    if (rc < 0)
    {
        AERON_SET_ERR(rc, "failed to format value: %" PRIu64, duration_ns);
        return -1;
    }
    return rc;
}

bool aeron_parse_bool(const char *str, bool def)
{
    if (NULL != str)
    {
        if (strncmp(str, "1", 1) == 0 || strncmp(str, "on", 2) == 0 || strncmp(str, "true", 4) == 0)
        {
            return true;
        }

        if (strncmp(str, "0", 1) == 0 || strncmp(str, "off", 3) == 0 || strncmp(str, "false", 5) == 0)
        {
            return false;
        }
    }

    return def;
}

int aeron_address_split(const char *address_str, aeron_parsed_address_t *parsed_address)
{
    if (NULL == address_str || '\0' == *address_str)
    {
        AERON_SET_ERR(EINVAL, "%s", "no address value");
        return -1;
    }

    int percent_index = -1;
    int colon_index = -1;
    int l_brace_index = -1;
    int r_brace_index = -1;

    int i = 0;
    char c = *address_str;
    while ('\0' != c)
    {
        if (':' == c)
        {
            colon_index = i;
        }
        else if ('[' == c)
        {
            l_brace_index = i;
        }
        else if (']' == c)
        {
            r_brace_index = i;
        }
        else if ('%'  == c)
        {
            percent_index = i;
        }

        ++i;
        c = *(address_str + i);
    }

    bool is_ipv6 = false;
    if (l_brace_index > 0 || r_brace_index > 0)
    {
        if (l_brace_index < 0 || r_brace_index < 0 || r_brace_index < l_brace_index)
        {
            AERON_SET_ERR(EINVAL, "host address invalid: %s", address_str);
            return -1;
        }

        is_ipv6 = true;
        parsed_address->ip_version_hint = 6;
    }
    else
    {
        parsed_address->ip_version_hint = 4;
    }

    *parsed_address->port = '\0';
    if (colon_index >= 0 && r_brace_index < colon_index)
    {
        if (i - 1 == colon_index)
        {
            AERON_SET_ERR(EINVAL, "port invalid: %s", address_str);
            return -1;
        }

        int port_begin = colon_index + 1;
        int length = i - port_begin;

        if (length >= AERON_MAX_PORT_LENGTH)
        {
            AERON_SET_ERR(EINVAL, "port invalid: %s", address_str);
            return -1;
        }

        strncpy(parsed_address->port, address_str + port_begin, (size_t)length);
        *(parsed_address->port + length) = '\0';
    }

    int length = i;

    if (colon_index >= 0 && colon_index > r_brace_index)
    {
        length = colon_index;
    }

    bool is_scoped = false;
    if (percent_index >= 0 && percent_index < r_brace_index)
    {
        is_scoped = true;
        length = percent_index;
    }

    const char *host = is_ipv6 ? address_str + 1 : address_str;
    if (is_ipv6)
    {
        length =  is_scoped ? length -1 : length - 2;
    }

    if (length >= AERON_MAX_HOST_LENGTH)
    {
        AERON_SET_ERR(EINVAL, "host address invalid: %s", address_str);
        return -1;
    }

    strncpy(parsed_address->host, host, (size_t)length);
    *(parsed_address->host + length) = '\0';

    return 0;
}

int aeron_interface_split(const char *interface_str, aeron_parsed_interface_t *parsed_interface)
{
    if (NULL == interface_str || '\0' == *interface_str)
    {
        AERON_SET_ERR(EINVAL, "%s", "no interface value");
        return -1;
    }

    int percent_index = -1;
    int colon_index = -1;
    int l_brace_index = -1;
    int r_brace_index = -1;
    int slash_index = -1;

    int i = 0;
    char c = *interface_str;
    while ('\0' != c)
    {
        if (':' == c)
        {
            colon_index = i;
        }
        else if ('[' == c)
        {
            l_brace_index = i;
        }
        else if (']' == c)
        {
            r_brace_index = i;
        }
        else if ('/'  == c)
        {
            slash_index = i;
        }
        else if ('%'  == c)
        {
            percent_index = i;
        }

        ++i;
        c = *(interface_str + i);
    }

    bool is_ipv6 = false;
    if (l_brace_index > 0 || r_brace_index > 0)
    {
        if (l_brace_index < 0 || r_brace_index < 0 || r_brace_index < l_brace_index)
        {
            AERON_SET_ERR(EINVAL, "host address invalid: %s", interface_str);
            return -1;
        }

        is_ipv6 = true;
        parsed_interface->ip_version_hint = 6;
    }
    else
    {
        parsed_interface->ip_version_hint = 4;
    }

    *parsed_interface->prefix = '\0';
    if (slash_index >= 0)
    {
        int length = i - slash_index;
        if (length <= 0)
        {
            AERON_SET_ERR(EINVAL, "subnet prefix invalid: %s", interface_str);
            return -1;
        }

        if (length >= AERON_MAX_PREFIX_LENGTH)
        {
            AERON_SET_ERR(EINVAL, "subnet prefix invalid: %s", interface_str);
            return -1;
        }

        strncpy(parsed_interface->prefix, interface_str + slash_index + 1, (size_t)length);
        *(parsed_interface->prefix + length) = '\0';
    }

    *parsed_interface->port = '\0';
    if (colon_index >= 0 && r_brace_index < colon_index)
    {
        if (i - 1 == colon_index)
        {
            AERON_SET_ERR(EINVAL, "port invalid: %s", interface_str);
            return -1;
        }

        int port_begin = colon_index + 1;
        int length = slash_index > 0 ? slash_index - port_begin : i - port_begin;

        if (length >= AERON_MAX_PORT_LENGTH)
        {
            AERON_SET_ERR(EINVAL, "port invalid: %s", interface_str);
            return -1;
        }

        strncpy(parsed_interface->port, interface_str + port_begin, (size_t)length);
        *(parsed_interface->port + length) = '\0';
    }

    int length = i;

    if (slash_index >= 0)
    {
        length = slash_index;
    }

    if (colon_index >= 0 && colon_index > r_brace_index)
    {
        length = colon_index;
    }

    bool is_scoped = false;
    if (percent_index >= 0 && percent_index < r_brace_index)
    {
        length = percent_index;
        is_scoped = true;
    }

    const char *host = is_ipv6 ? interface_str + 1 : interface_str;
    if (is_ipv6)
    {
        length =  is_scoped ? length -1 : length - 2;
    }

    if (length >= AERON_MAX_HOST_LENGTH)
    {
        AERON_SET_ERR(EINVAL, "host address invalid: %s", interface_str);
        return -1;
    }

    strncpy(parsed_interface->host, host, (size_t)length);
    *(parsed_interface->host + length) = '\0';

    return 0;
}

int aeron_parse_get_line(char *str, size_t max_length, const char *buffer)
{
    size_t i;

    for (i = 0; i < max_length - 1; i++)
    {
        str[i] = buffer[i];
        if ('\0' == buffer[i])
        {
            return 0;
        }

        if ('\n' == buffer[i])
        {
            str[i + 1] = '\0';
            return (int)(i + 1);
        }
    }

    AERON_SET_ERR(EINVAL, "line too long: %" PRIu64 "/%" PRIu64, (uint64_t)i, (uint64_t)max_length);
    return -1;
}

void aeron_config_prop_warning(const char *name, const char *str)
{
    AERON_FPRINTF(stderr, "WARNING: %s=%s is invalid, using default\n", name, str);
}

uint64_t aeron_config_parse_uint64(const char *name, const char *str, uint64_t def, uint64_t min, uint64_t max)
{
    uint64_t result = def;

    if (NULL != str)
    {
        errno = 0;
        char *end_ptr = NULL;
        uint64_t value = strtoull(str, &end_ptr, 0);

        if ((0 == value && 0 != errno) || end_ptr == str)
        {
            aeron_config_prop_warning(name, str);
            value = def;
        }

        result = value;
        result = result > max ? max : result;
        result = result < min ? min : result;
    }

    return result;
}

int32_t aeron_config_parse_int32(const char *name, const char *str, int32_t def, int32_t min, int32_t max)
{
    int32_t result = def;

    if (NULL != str)
    {
        errno = 0;
        char *end_ptr = NULL;
        long long value = strtoll(str, &end_ptr, 0);

        if ((0 == value && 0 != errno) || '\0' != *end_ptr || value < INT32_MIN || INT32_MAX < value)
        {
            aeron_config_prop_warning(name, str);
            value = def;
        }

        result = value > max ? max : (int32_t)value;
        result = value < min ? min : result;
    }

    return result;
}

int64_t aeron_config_parse_int64(const char *name, const char *str, int64_t def, int64_t min, int64_t max)
{
    int64_t result = def;

    if (NULL != str)
    {
        errno = 0;
        char *end_ptr = NULL;
        int64_t value = strtoll(str, &end_ptr, 0);

        if ((0 == value && 0 != errno) || '\0' != *end_ptr)
        {
            aeron_config_prop_warning(name, str);
            value = def;
        }

        result = value > max ? max : value;
        result = value < min ? min : result;
    }

    return result;
}

uint32_t aeron_config_parse_uint32(const char *name, const char *str, uint32_t def, uint32_t min, uint32_t max)
{
    return (uint32_t)aeron_config_parse_int64(name, str, def, min, max);
}

uint64_t aeron_config_parse_size64(const char *name, const char *str, uint64_t def, uint64_t min, uint64_t max)
{
    uint64_t result = def;

    if (NULL != str)
    {
        uint64_t value = 0;

        if (-1 == aeron_parse_size64(str, &value))
        {
            aeron_config_prop_warning(name, str);
        }
        else
        {
            result = value;
            result = result > max ? max : result;
            result = result < min ? min : result;
        }
    }

    return result;
}

uint64_t aeron_config_parse_duration_ns(const char *name, const char *str, uint64_t def, uint64_t min, uint64_t max)
{
    uint64_t result = def;

    if (NULL != str)
    {
        uint64_t value = 0;

        if (-1 == aeron_parse_duration_ns(str, &value))
        {
            aeron_config_prop_warning(name, str);
        }
        else
        {
            result = value;
            result = result > max ? max : result;
            result = result < min ? min : result;
        }
    }

    return result;
}
