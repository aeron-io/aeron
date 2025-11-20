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

#include <string.h>

#include "util/aeron_bitutil.h"
#include "aeron_udp_protocol.h"

#ifdef _MSC_VER
#define _Static_assert static_assert
#endif

_Static_assert(
    sizeof(aeron_data_header_as_longs_t) == sizeof(aeron_data_header_t),
    "sizeof(aeron_data_header_as_longs_t) must match sizeof(aeron_data_header_t)");

int aeron_udp_protocol_group_tag(aeron_status_message_header_t *sm, int64_t *group_tag)
{
    const size_t group_tag_offset = sizeof(aeron_status_message_header_t) +
        offsetof(aeron_status_message_optional_header_t, group_tag);
    const size_t group_tag_size = sizeof(*group_tag);
    const size_t frame_length_with_group_tag = group_tag_offset + group_tag_size;

    if (sm->frame_header.frame_length == (int32_t)frame_length_with_group_tag)
    {
        const uint8_t *sm_ptr = (const uint8_t *)sm + group_tag_offset;
        memcpy(group_tag, sm_ptr, group_tag_size);

        return (int)group_tag_size;
    }

    *group_tag = 0;

    return (int)((size_t)sm->frame_header.frame_length - group_tag_offset);
}

extern bool aeron_is_frame_valid(const aeron_frame_header_t *header, size_t frame_length);

extern size_t aeron_res_header_address_length(int8_t res_type);

extern size_t aeron_compute_max_message_length(size_t term_length);

size_t aeron_res_header_entry_length_ipv4(aeron_resolution_header_ipv4_t *header)
{
    return AERON_ALIGN(sizeof(aeron_resolution_header_ipv4_t) + header->name_length, sizeof(int64_t));
}

size_t aeron_res_header_entry_length_ipv6(aeron_resolution_header_ipv6_t *header)
{
    return AERON_ALIGN(sizeof(aeron_resolution_header_ipv6_t) + header->name_length, sizeof(int64_t));
}

int aeron_res_header_entry_length(void *res, size_t remaining)
{
    if (remaining < sizeof(aeron_resolution_header_t))
    {
        return -1;
    }

    aeron_resolution_header_t *res_header = (aeron_resolution_header_t *)res;

    int result = -1;
    switch (res_header->res_type)
    {
        case AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD:
        {
            if (sizeof(aeron_resolution_header_ipv6_t) <= remaining)
            {
                aeron_resolution_header_ipv6_t *res_ipv6 = (aeron_resolution_header_ipv6_t *)res_header;
                size_t entry_length = aeron_res_header_entry_length_ipv6(res_ipv6);
                result = entry_length <= remaining ? (int)entry_length : -1;
            }
            break;
        }

        case AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD:
        {
            if (sizeof(aeron_resolution_header_ipv4_t) <= remaining)
            {
                aeron_resolution_header_ipv4_t *res_ipv4 = (aeron_resolution_header_ipv4_t *)res_header;
                size_t entry_length = aeron_res_header_entry_length_ipv4(res_ipv4);
                result = entry_length <= remaining ? (int)entry_length : -1;
            }
            break;
        }

        default:
            break;
    }

    return result;
}
