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

#ifndef AERON_UDP_CHANNEL_H
#define AERON_UDP_CHANNEL_H

#include "aeron_socket.h"
#include "uri/aeron_uri.h"
#include "util/aeron_netutil.h"
#include "aeron_name_resolver.h"
#include "util/aeron_error.h"

#define AERON_UDP_CHANNEL_RESERVED_VALUE_OFFSET (-8)

enum aeron_udp_channel_control_mode_en
{
    AERON_UDP_CHANNEL_CONTROL_MODE_NONE,
    AERON_UDP_CHANNEL_CONTROL_MODE_DYNAMIC,
    AERON_UDP_CHANNEL_CONTROL_MODE_MANUAL,
    AERON_UDP_CHANNEL_CONTROL_MODE_RESPONSE
};

typedef enum aeron_udp_channel_control_mode_en aeron_udp_channel_control_mode;

typedef struct aeron_udp_channel_stct
{
    char original_uri[AERON_URI_MAX_LENGTH];
    char canonical_form[AERON_URI_MAX_LENGTH];
    aeron_uri_t uri;
    struct sockaddr_storage remote_data;
    struct sockaddr_storage local_data;
    struct sockaddr_storage remote_control;
    struct sockaddr_storage local_control;
    int64_t tag_id;
    unsigned int interface_index;
    size_t uri_length;
    size_t canonical_length;
    uint8_t multicast_ttl;
    bool has_explicit_endpoint;
    bool has_explicit_control;
    aeron_udp_channel_control_mode control_mode;
    bool is_multicast;
    aeron_uri_ats_status_t ats_status;
    size_t socket_sndbuf_length;
    size_t socket_rcvbuf_length;
    int32_t socket_tos;
    size_t receiver_window_length;
    int32_t media_rcv_timestamp_offset;
    int32_t channel_rcv_timestamp_offset;
    int32_t channel_snd_timestamp_offset;
}
aeron_udp_channel_t;

typedef struct aeron_udp_channel_async_parse_stct
{
    aeron_udp_channel_t *channel;
    bool is_destination;
}
aeron_udp_channel_async_parse_t;

int aeron_udp_channel_do_initial_parse(
    size_t uri_length,
    const char *uri,
    aeron_udp_channel_async_parse_t *async_parse);

int aeron_udp_channel_finish_parse(
    aeron_name_resolver_t *resolver,
    aeron_udp_channel_async_parse_t *async_parse);

int aeron_udp_channel_parse(
    size_t uri_length,
    const char *uri,
    aeron_name_resolver_t *resolver,
    aeron_udp_channel_t **channel,
    bool is_destination);

int aeron_udp_channel_matches_tag(
    aeron_udp_channel_t *channel,
    aeron_udp_channel_t *endpoint_channel,
    struct sockaddr_storage *local_address,
    struct sockaddr_storage *remote_address,
    bool *has_match);

void aeron_udp_channel_delete(aeron_udp_channel_t *channel);

inline bool aeron_udp_channel_is_wildcard(aeron_udp_channel_t *channel)
{
    return aeron_is_wildcard_addr(&channel->remote_data) && aeron_is_wildcard_port(&channel->remote_data) &&
        aeron_is_wildcard_addr(&channel->local_data) && aeron_is_wildcard_port(&channel->local_data);
}

inline bool aeron_udp_channel_control_modes_match(aeron_udp_channel_t *channel, aeron_udp_channel_t *other)
{
    return AERON_UDP_CHANNEL_CONTROL_MODE_NONE == channel->control_mode || channel->control_mode == other->control_mode;
}

inline bool aeron_udp_channel_equals(aeron_udp_channel_t *a, aeron_udp_channel_t *b)
{
    return a == b || (a != NULL && 0 == strncmp(a->canonical_form, b->canonical_form, AERON_URI_MAX_LENGTH));
}

inline size_t aeron_udp_channel_receiver_window(aeron_udp_channel_t *channel, size_t default_receiver_window)
{
    return 0 != channel->receiver_window_length ? channel->receiver_window_length : default_receiver_window;
}

inline size_t aeron_udp_channel_socket_so_sndbuf(aeron_udp_channel_t *channel, size_t default_so_sndbuf)
{
    return 0 != channel->socket_sndbuf_length ? channel->socket_sndbuf_length : default_so_sndbuf;
}

inline size_t aeron_udp_channel_socket_so_rcvbuf(aeron_udp_channel_t *channel, size_t default_so_rcvbuf)
{
    return 0 != channel->socket_rcvbuf_length ? channel->socket_rcvbuf_length : default_so_rcvbuf;
}

inline int32_t aeron_udp_channel_socket_tos(aeron_udp_channel_t *channel, int32_t default_socket_tos)
{
    return AERON_NULL_VALUE != channel->socket_tos ? channel->socket_tos : default_socket_tos;
}

inline bool aeron_udp_channel_is_media_rcv_timestamps_enabled(aeron_udp_channel_t *channel)
{
    return AERON_UDP_CHANNEL_RESERVED_VALUE_OFFSET == channel->media_rcv_timestamp_offset ||
        0 <= channel->media_rcv_timestamp_offset;
}

inline bool aeron_udp_channel_is_channel_rcv_timestamps_enabled(aeron_udp_channel_t *channel)
{
    return AERON_UDP_CHANNEL_RESERVED_VALUE_OFFSET == channel->channel_rcv_timestamp_offset ||
        0 <= channel->channel_rcv_timestamp_offset;
}

inline bool aeron_udp_channel_is_channel_snd_timestamps_enabled(aeron_udp_channel_t *channel)
{
    return AERON_UDP_CHANNEL_RESERVED_VALUE_OFFSET == channel->channel_snd_timestamp_offset ||
        0 <= channel->channel_snd_timestamp_offset;
}

inline bool aeron_udp_channel_is_multi_destination(const aeron_udp_channel_t *channel)
{
    return AERON_UDP_CHANNEL_CONTROL_MODE_DYNAMIC == channel->control_mode ||
        AERON_UDP_CHANNEL_CONTROL_MODE_MANUAL == channel->control_mode;
}

inline bool aeron_udp_channel_has_group_semantics(const aeron_udp_channel_t *channel)
{
    return channel->is_multicast || aeron_udp_channel_is_multi_destination(channel);
}

#endif //AERON_UDP_CHANNEL_H
