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

#ifndef AERON_URI_H
#define AERON_URI_H

#include "aeron_common.h"

typedef struct aeron_uri_param_stct
{
    const char *key;
    const char *value;
}
aeron_uri_param_t;

typedef struct aeron_uri_params_stct
{
    size_t length;
    aeron_uri_param_t *array;
}
aeron_uri_params_t;

#define AERON_SPY_PREFIX "aeron-spy:"
#define AERON_SPY_PREFIX_LEN strlen(AERON_SPY_PREFIX)

#define AERON_IPC_CHANNEL "aeron:ipc"
#define AERON_IPC_CHANNEL_LEN strlen(AERON_IPC_CHANNEL)

#define AERON_UDP_CHANNEL_RELIABLE_KEY "reliable"
#define AERON_UDP_CHANNEL_TTL_KEY "ttl"

#define AERON_UDP_CHANNEL_ENDPOINT_KEY "endpoint"
#define AERON_UDP_CHANNEL_INTERFACE_KEY "interface"
#define AERON_UDP_CHANNEL_CONTROL_KEY "control"
#define AERON_UDP_CHANNEL_CONTROL_MODE_KEY "control-mode"
#define AERON_UDP_CHANNEL_CONTROL_MODE_MANUAL_VALUE "manual"
#define AERON_UDP_CHANNEL_CONTROL_MODE_DYNAMIC_VALUE "dynamic"
#define AERON_UDP_CHANNEL_CONTROL_MODE_RESPONSE_VALUE "response"

#define AERON_URI_INITIAL_TERM_ID_KEY "init-term-id"
#define AERON_URI_TERM_ID_KEY "term-id"
#define AERON_URI_TERM_OFFSET_KEY "term-offset"

#define AERON_URI_ALIAS_KEY "alias"
#define AERON_URI_TERM_LENGTH_KEY "term-length"
#define AERON_URI_LINGER_TIMEOUT_KEY "linger"
#define AERON_URI_MTU_LENGTH_KEY "mtu"
#define AERON_URI_SPARSE_TERM_KEY "sparse"
#define AERON_URI_EOS_KEY "eos"
#define AERON_URI_TETHER_KEY "tether"
#define AERON_URI_TAGS_KEY "tags"
#define AERON_URI_SESSION_ID_KEY "session-id"
#define AERON_URI_GROUP_KEY "group"
#define AERON_URI_REJOIN_KEY "rejoin"
#define AERON_URI_FC_KEY "fc"
#define AERON_URI_GTAG_KEY "gtag"
#define AERON_URI_CC_KEY "cc"
#define AERON_URI_SPIES_SIMULATE_CONNECTION_KEY "ssc"
#define AERON_URI_ATS_KEY "ats"
#define AERON_URI_SOCKET_SNDBUF_KEY "so-sndbuf"
#define AERON_URI_SOCKET_RCVBUF_KEY "so-rcvbuf"
#define AERON_URI_RECEIVER_WINDOW_KEY "rcv-wnd"
#define AERON_URI_MEDIA_RCV_TIMESTAMP_OFFSET_KEY "media-rcv-ts-offset"
#define AERON_URI_CHANNEL_RCV_TIMESTAMP_OFFSET_KEY "channel-rcv-ts-offset"
#define AERON_URI_CHANNEL_SND_TIMESTAMP_OFFSET_KEY "channel-snd-ts-offset"
#define AERON_URI_TIMESTAMP_OFFSET_RESERVED "reserved"
#define AERON_URI_RESPONSE_CORRELATION_ID_KEY "response-correlation-id"
#define AERON_URI_NAK_DELAY_KEY "nak-delay"
#define AERON_URI_UNTETHERED_WINDOW_LIMIT_TIMEOUT_KEY "untethered-window-limit-timeout"
#define AERON_URI_UNTETHERED_LINGER_TIMEOUT_KEY "untethered-linger-timeout"
#define AERON_URI_UNTETHERED_RESTING_TIMEOUT_KEY "untethered-resting-timeout"
#define AERON_URI_MAX_RESEND_KEY "max-resend"
#define AERON_URI_STREAM_ID_KEY "stream-id"
#define AERON_URI_PUBLICATION_WINDOW_KEY "pub-wnd"
#define AERON_URI_INVALID_TAG (-1)

#define AERON_URI_MAX_LENGTH (4096) // max channel uri length including NUL character

typedef struct aeron_udp_channel_params_stct
{
    const char *endpoint;
    const char *bind_interface;
    const char *control;
    const char *control_mode;
    const char *channel_tag;
    const char *entity_tag;
    const char *ttl;
    aeron_uri_params_t additional_params;
}
aeron_udp_channel_params_t;

typedef struct aeron_ipc_channel_params_stct
{
    const char *channel_tag;
    const char *entity_tag;
    aeron_uri_params_t additional_params;
}
aeron_ipc_channel_params_t;

typedef enum aeron_uri_type_enum
{
    AERON_URI_UDP, AERON_URI_IPC, AERON_URI_UNKNOWN
}
aeron_uri_type_t;

typedef struct aeron_uri_stct
{
    char mutable_uri[AERON_URI_MAX_LENGTH];
    aeron_uri_type_t type;

    union
    {
        aeron_udp_channel_params_t udp;
        aeron_ipc_channel_params_t ipc;
    }
    params;
}
aeron_uri_t;

typedef enum aeron_uri_ats_status_en
{
    AERON_URI_ATS_STATUS_DEFAULT,
    AERON_URI_ATS_STATUS_ENABLED,
    AERON_URI_ATS_STATUS_DISABLED
}
aeron_uri_ats_status_t;

typedef int (*aeron_uri_parse_callback_t)(void *clientd, const char *key, const char *value);

int aeron_uri_parse_params(char *uri, aeron_uri_parse_callback_t param_func, void *clientd);

int aeron_uri_parse(size_t uri_length, const char *uri, aeron_uri_t *params);

void aeron_uri_close(aeron_uri_t *params);

uint8_t aeron_uri_multicast_ttl(aeron_uri_t *uri);

const char *aeron_uri_find_param_value(const aeron_uri_params_t *uri_params, const char *key);
int aeron_uri_get_int32(aeron_uri_params_t *uri_params, const char *key, int32_t *retval);
int aeron_uri_get_int64(aeron_uri_params_t *uri_params, const char *key, int64_t default_val, int64_t *retval);
int aeron_uri_get_bool(aeron_uri_params_t *uri_params, const char *key, bool *retval);
int aeron_uri_get_ats(aeron_uri_params_t *uri_params, aeron_uri_ats_status_t *uri_ats_status);
int aeron_uri_get_timeout(aeron_uri_params_t *uri_params, const char *param_name, uint64_t *timeout_ns);
int aeron_uri_sprint(aeron_uri_t *uri, char *buffer, size_t buffer_len);
int aeron_uri_get_socket_buf_lengths(
    aeron_uri_params_t *uri_params, size_t *socket_sndbuf_length, size_t *socket_rcvbuf_length);
int aeron_uri_get_receiver_window_length(aeron_uri_params_t *uri_params, size_t *receiver_window_length);

int64_t aeron_uri_parse_tag(const char *tag_str);

#endif //AERON_URI_H
