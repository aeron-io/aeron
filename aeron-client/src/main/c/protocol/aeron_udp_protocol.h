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

#ifndef AERON_UDP_PROTOCOL_H
#define AERON_UDP_PROTOCOL_H

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

#pragma pack(push)
#pragma pack(4)
typedef struct aeron_frame_header_stct
{
    volatile int32_t frame_length;
    int8_t version;
    uint8_t flags;
    int16_t type;
}
aeron_frame_header_t;

typedef struct aeron_data_header_as_longs_stct
{
    volatile uint64_t hdr[4];
}
aeron_data_header_as_longs_t;

typedef struct aeron_setup_header_stct
{
    aeron_frame_header_t frame_header;
    int32_t term_offset;
    int32_t session_id;
    int32_t stream_id;
    int32_t initial_term_id;
    int32_t active_term_id;
    int32_t term_length;
    int32_t mtu;
    int32_t ttl;
}
aeron_setup_header_t;

typedef struct aeron_data_header_stct
{
    aeron_frame_header_t frame_header;
    int32_t term_offset;
    int32_t session_id;
    int32_t stream_id;
    int32_t term_id;
    int64_t reserved_value;
}
aeron_data_header_t;

typedef struct aeron_nak_header_stct
{
    aeron_frame_header_t frame_header;
    int32_t session_id;
    int32_t stream_id;
    int32_t term_id;
    int32_t term_offset;
    int32_t length;
}
aeron_nak_header_t;

typedef struct aeron_status_message_header_stct
{
    aeron_frame_header_t frame_header;
    int32_t session_id;
    int32_t stream_id;
    int32_t consumption_term_id;
    int32_t consumption_term_offset;
    int32_t receiver_window;
    int64_t receiver_id;
}
aeron_status_message_header_t;

typedef struct aeron_status_message_optional_header_stct
{
    int64_t group_tag;
}
aeron_status_message_optional_header_t;

struct aeron_error_stct
{
    aeron_frame_header_t frame_header;
    int32_t session_id;
    int32_t stream_id;
    int64_t receiver_id;
    int64_t group_tag;
    int32_t error_code;
    int32_t error_length;
};
typedef struct aeron_error_stct aeron_error_t;

typedef struct aeron_rttm_header_stct
{
    aeron_frame_header_t frame_header;
    int32_t session_id;
    int32_t stream_id;
    int64_t echo_timestamp;
    int64_t reception_delta;
    int64_t receiver_id;
}
aeron_rttm_header_t;

#pragma pack(pop)

#define AERON_RES_HEADER_ADDRESS_LENGTH_IP4 (4u)
#define AERON_RES_HEADER_ADDRESS_LENGTH_IP6 (16u)

#pragma pack(push)
#pragma pack(1)
typedef struct aeron_resolution_header_stct
{
    int8_t res_type;
    uint8_t res_flags;
    uint16_t udp_port;
    int32_t age_in_ms;
}
aeron_resolution_header_t;

typedef struct aeron_resolution_header_ipv4_stct
{
    aeron_resolution_header_t resolution_header;
    uint8_t addr[AERON_RES_HEADER_ADDRESS_LENGTH_IP4];
    int16_t name_length;
}
aeron_resolution_header_ipv4_t;

typedef struct aeron_resolution_header_ipv6_stct
{
    aeron_resolution_header_t resolution_header;
    uint8_t addr[AERON_RES_HEADER_ADDRESS_LENGTH_IP6];
    int16_t name_length;
}
aeron_resolution_header_ipv6_t;

typedef struct aeron_option_header_stct
{
    uint16_t option_length;
    uint16_t type;
}
aeron_option_header_t;

typedef struct aeron_response_setup_header_stct
{
    aeron_frame_header_t frame_header;
    int32_t session_id;
    int32_t stream_id;
    int32_t response_session_id;
}
aeron_response_setup_header_t;
#pragma pack(pop)

int aeron_udp_protocol_group_tag(aeron_status_message_header_t *sm, int64_t *group_tag);

#define AERON_FRAME_HEADER_VERSION (0)

#define AERON_HDR_TYPE_PAD (INT16_C(0x00))
#define AERON_HDR_TYPE_DATA (INT16_C(0x01))
#define AERON_HDR_TYPE_NAK (INT16_C(0x02))
#define AERON_HDR_TYPE_SM (INT16_C(0x03))
#define AERON_HDR_TYPE_ERR (INT16_C(0x04))
#define AERON_HDR_TYPE_SETUP (INT16_C(0x05))
#define AERON_HDR_TYPE_RTTM (INT16_C(0x06))
#define AERON_HDR_TYPE_RES (INT16_C(0x07))
#define AERON_HDR_TYPE_ATS_DATA (INT16_C(0x08))
#define AERON_HDR_TYPE_ATS_SETUP (INT16_C(0x09))
#define AERON_HDR_TYPE_ATS_SM (INT16_C(0x0A))
#define AERON_HDR_TYPE_RSP_SETUP (INT16_C(0x0B))
#define AERON_HDR_TYPE_EXT (INT16_C(-1))

#define AERON_FRAME_HEADER_LENGTH (sizeof(aeron_frame_header_t))
#define AERON_DATA_HEADER_LENGTH (sizeof(aeron_data_header_t))

#define AERON_DATA_HEADER_BEGIN_FLAG (UINT8_C(0x80))
#define AERON_DATA_HEADER_END_FLAG (UINT8_C(0x40))
#define AERON_DATA_HEADER_EOS_FLAG (UINT8_C(0x20))
#define AERON_DATA_HEADER_REVOKED_FLAG (UINT8_C(0x10))

#define AERON_DATA_HEADER_UNFRAGMENTED (AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_END_FLAG)

#define AERON_DATA_HEADER_DEFAULT_RESERVED_VALUE (INT64_C(0))

#define AERON_STATUS_MESSAGE_HEADER_SEND_SETUP_FLAG (UINT8_C(0x80))
#define AERON_STATUS_MESSAGE_HEADER_EOS_FLAG (UINT8_C(0x40))

#define AERON_SETUP_HEADER_SEND_RESPONSE_FLAG (UINT8_C(0x80))
#define AERON_SETUP_HEADER_GROUP_FLAG (UINT8_C(0x40))

#define AERON_RTTM_HEADER_REPLY_FLAG (UINT8_C(0x80))

#define AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD (0x01)
#define AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD (0x02)
#define AERON_RES_HEADER_SELF_FLAG (UINT8_C(0x80))

#define AERON_FRAME_MAX_MESSAGE_LENGTH (16u * 1024u * 1024u)

#define AERON_OPTION_HEADER_IGNORE_FLAG (UINT16_C(0x8000))

#define AERON_OPT_HDR_TYPE_ATS_SUITE (UINT16_C(0x0001))
#define AERON_OPT_HDR_TYPE_ATS_RSA_KEY (UINT16_C(0x0002))
#define AERON_OPT_HDR_TYPE_ATS_RSA_KEY_ID (UINT16_C(0x0003))
#define AERON_OPT_HDR_TYPE_ATS_EC_KEY (UINT16_C(0x0004))
#define AERON_OPT_HDR_TYPE_ATS_EC_SIG (UINT16_C(0x0005))
#define AERON_OPT_HDR_TYPE_ATS_SECRET (UINT16_C(0x0006))
#define AERON_OPT_HDR_TYPE_ATS_GROUP_TAG (UINT16_C(0x0007))

#define AERON_OPT_HDR_ALIGNMENT (4u)

#define AERON_ERROR_MAX_TEXT_LENGTH (1023)
#define AERON_ERROR_MAX_FRAME_LENGTH (sizeof(aeron_error_t) + AERON_ERROR_MAX_TEXT_LENGTH)
#define AERON_ERROR_HAS_GROUP_TAG_FLAG (0x08)

inline bool aeron_is_frame_valid(const aeron_frame_header_t *header, const size_t frame_length)
{
    return frame_length >= AERON_FRAME_HEADER_LENGTH && AERON_FRAME_HEADER_VERSION == header->version;
}

inline size_t aeron_res_header_address_length(int8_t res_type)
{
    return AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD == res_type ?
        AERON_RES_HEADER_ADDRESS_LENGTH_IP6 : AERON_RES_HEADER_ADDRESS_LENGTH_IP4;
}

inline size_t aeron_compute_max_message_length(size_t term_length)
{
    size_t max_length_for_term = term_length >> 3;
    return max_length_for_term < AERON_FRAME_MAX_MESSAGE_LENGTH ? max_length_for_term : AERON_FRAME_MAX_MESSAGE_LENGTH;
}

size_t aeron_res_header_entry_length_ipv4(aeron_resolution_header_ipv4_t *header);

size_t aeron_res_header_entry_length_ipv6(aeron_resolution_header_ipv6_t *header);

int aeron_res_header_entry_length(void *res, size_t remaining);

#endif //AERON_UDP_PROTOCOL_H
