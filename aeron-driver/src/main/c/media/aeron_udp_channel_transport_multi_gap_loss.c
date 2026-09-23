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

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "concurrent/aeron_thread.h"
#include "protocol/aeron_udp_protocol.h"
#include "util/aeron_error.h"
#include "aeron_alloc.h"
#include "aeron_windows.h"
#include "aeron_udp_channel_transport_multi_gap_loss.h"
#include "collections/aeron_int64_counter_map.h"

#define AERON_CONFIG_GETENV_OR_DEFAULT(e, d) ((NULL == getenv(e)) ? (d) : getenv(e))
#define AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MULTI_GAP_LOSS_ARGS_ENV_VAR "AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MULTI_GAP_LOSS_ARGS"

static AERON_INIT_ONCE env_is_initialized = AERON_INIT_ONCE_VALUE;

static aeron_udp_channel_interceptor_multi_gap_loss_params_t aeron_udp_channel_interceptor_multi_gap_loss_params;

#define STREAM_AND_SESSION_ID_NULL_OFFSET (-1)

aeron_udp_channel_interceptor_bindings_t *aeron_udp_channel_interceptor_multi_gap_loss_load(
    aeron_udp_channel_interceptor_bindings_t *delegate_bindings)
{
    aeron_udp_channel_interceptor_bindings_t *interceptor_bindings;
    if (aeron_alloc((void **)&interceptor_bindings, sizeof(aeron_udp_channel_interceptor_bindings_t)) < 0)
    {
        return NULL;
    }

    interceptor_bindings->incoming_init_func = aeron_udp_channel_interceptor_multi_gap_loss_init_incoming;
    interceptor_bindings->outgoing_init_func = NULL;
    interceptor_bindings->outgoing_send_func = NULL;
    interceptor_bindings->incoming_func = aeron_udp_channel_interceptor_multi_gap_loss_incoming;
    interceptor_bindings->outgoing_close_func = NULL;
    interceptor_bindings->incoming_close_func = aeron_udp_channel_interceptor_multi_gap_loss_close_incoming;
    interceptor_bindings->outgoing_transport_notification_func = NULL;
    interceptor_bindings->outgoing_publication_notification_func = NULL;
    interceptor_bindings->outgoing_image_notification_func = NULL;
    interceptor_bindings->incoming_transport_notification_func = NULL;
    interceptor_bindings->incoming_publication_notification_func = NULL;
    interceptor_bindings->incoming_image_notification_func = NULL;

    interceptor_bindings->meta_info.name = "multi-gap-loss";
    interceptor_bindings->meta_info.type = "interceptor";
    interceptor_bindings->meta_info.next_interceptor_bindings = delegate_bindings;

    return interceptor_bindings;
}

int aeron_udp_channel_interceptor_multi_gap_loss_configure(const aeron_udp_channel_interceptor_multi_gap_loss_params_t *multi_gap_loss_params)
{
    memcpy(&aeron_udp_channel_interceptor_multi_gap_loss_params, multi_gap_loss_params, sizeof(aeron_udp_channel_interceptor_multi_gap_loss_params_t));

    return 0;
}

void aeron_udp_channel_transport_multi_gap_loss_load_env(void)
{
    const char *args = AERON_CONFIG_GETENV_OR_DEFAULT(AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MULTI_GAP_LOSS_ARGS_ENV_VAR, "");
    char *args_dup = strdup(args);
    if (NULL == args_dup)
    {
        AERON_SET_ERR(errno, "%s", "Duplicating args string");
        return;
    }

    aeron_udp_channel_interceptor_multi_gap_loss_params_t *params;
    if (aeron_alloc((void **)&params, sizeof(aeron_udp_channel_interceptor_multi_gap_loss_params_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        aeron_free(args_dup);
        return;
    }

    if (aeron_udp_channel_interceptor_multi_gap_loss_parse_params(args_dup, params) >= 0)
    {
        aeron_udp_channel_interceptor_multi_gap_loss_configure(params);
    }

    aeron_free(params);
    aeron_free(args_dup);
}

int aeron_udp_channel_interceptor_multi_gap_loss_init_incoming(
    void **interceptor_state, aeron_driver_context_t *context, aeron_udp_channel_transport_affinity_t affinity)
{
    (void)aeron_thread_once(&env_is_initialized, aeron_udp_channel_transport_multi_gap_loss_load_env);

    aeron_int64_counter_map_t *stream_and_session_id_to_offset_map;

    if (aeron_alloc((void **)&stream_and_session_id_to_offset_map, sizeof(aeron_int64_counter_map_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (aeron_int64_counter_map_init(stream_and_session_id_to_offset_map, STREAM_AND_SESSION_ID_NULL_OFFSET, 16, AERON_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        AERON_SET_ERR(errno, "%s", "Unable to init stream_and_session_id_to_offset_map");
        aeron_free(stream_and_session_id_to_offset_map);
        return -1;
    }

    *interceptor_state = stream_and_session_id_to_offset_map;

    return 0;
}

int aeron_udp_channel_interceptor_multi_gap_loss_close_incoming(void *interceptor_state)
{
    aeron_int64_counter_map_delete((aeron_int64_counter_map_t *)interceptor_state);

    aeron_free(interceptor_state);

    return 0;
}

static bool aeron_udp_channel_interceptor_multi_gap_loss_should_drop_frame(
    aeron_int64_counter_map_t *stream_and_session_id_to_offset_map,
    const uint8_t *buffer, size_t buffer_length, const int32_t term_id, const int gap_radix_bits,
    const uint32_t gap_radix_mask, const size_t gap_length, const int32_t last_gap_limit)
{
    const aeron_frame_header_t *frame_header = (aeron_frame_header_t *)buffer;
    const bool is_data_msg = (unsigned int)frame_header->type == AERON_HDR_TYPE_DATA;

    if (!is_data_msg)
    {
        return false;
    }

    const aeron_data_header_t *data_header = (aeron_data_header_t *)buffer;

    if (term_id != data_header->term_id)
    {
        return false;
    }

    if (data_header->term_offset > last_gap_limit)
    {
        return false;
    }

    const int64_t key = aeron_map_compound_key(data_header->stream_id, data_header->session_id);

    int64_t tracking_offset = aeron_int64_counter_map_get(stream_and_session_id_to_offset_map, key);

    if (tracking_offset == STREAM_AND_SESSION_ID_NULL_OFFSET)
    {
        tracking_offset = data_header->term_offset;
        if (aeron_int64_counter_map_put(stream_and_session_id_to_offset_map, key, tracking_offset, NULL) != 0)
        {
            return false;
        }
    }

    if (tracking_offset > data_header->term_offset)
    {
        return false;
    }

    const int64_t frame_limit = data_header->term_offset + (int64_t)buffer_length;

    if (data_header->term_offset != 0 && aeron_number_of_trailing_zeroes(data_header->term_offset) >= gap_radix_bits)
    {
        goto drop_frame;
    }

    const int64_t previous_gap_offset = data_header->term_offset & gap_radix_mask;
    const int64_t previous_gap_limit = previous_gap_offset + (int64_t)gap_length;

    if (previous_gap_offset > 0 && data_header->term_offset < previous_gap_limit)
    {
        goto drop_frame;
    }

    const int64_t next_gap_offset = ((data_header->term_offset >> gap_radix_bits) + 1) << gap_radix_bits;
    const int64_t next_gap_limit = next_gap_offset + (int64_t)gap_length;

    if (frame_limit > next_gap_offset && data_header->term_offset < next_gap_limit)
    {
        goto drop_frame;
    }

    return false;

drop_frame:
    if (aeron_int64_counter_map_put(stream_and_session_id_to_offset_map, key, frame_limit, NULL) != 0)
    {
        return false;
    }

    return true;
}

void aeron_udp_channel_interceptor_multi_gap_loss_incoming(
    void *interceptor_state,
    aeron_udp_channel_incoming_interceptor_t *delegate,
    aeron_udp_channel_transport_t *transport,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr,
    struct timespec *media_timestamp)
{
    if (!aeron_udp_channel_interceptor_multi_gap_loss_should_drop_frame(
        (aeron_int64_counter_map_t *)interceptor_state,
        buffer,
        length,
        aeron_udp_channel_interceptor_multi_gap_loss_params.term_id,
        aeron_udp_channel_interceptor_multi_gap_loss_params.gap_radix_bits,
        aeron_udp_channel_interceptor_multi_gap_loss_params.gap_radix_mask,
        aeron_udp_channel_interceptor_multi_gap_loss_params.gap_length,
        aeron_udp_channel_interceptor_multi_gap_loss_params.last_gap_limit))
    {
        delegate->incoming_func(
            delegate->interceptor_state,
            delegate->next_interceptor,
            transport,
            receiver_clientd,
            endpoint_clientd,
            destination_clientd,
            buffer,
            length,
            addr,
            media_timestamp);
    }
}

int aeron_udp_channel_interceptor_multi_gap_loss_parse_params(char *uri, aeron_udp_channel_interceptor_multi_gap_loss_params_t *multi_gap_loss_params)
{
    int rc = aeron_uri_parse_params(uri, aeron_udp_channel_interceptor_multi_gap_loss_parse_callback, (void *)multi_gap_loss_params);

    if (rc < 0)
    {
        return rc;
    }

    const int32_t gap_radix = aeron_find_next_power_of_two(multi_gap_loss_params->gap_radix);
    multi_gap_loss_params->gap_radix_bits = aeron_number_of_trailing_zeroes(gap_radix);
    multi_gap_loss_params->gap_radix_mask = ~(gap_radix - 1);
    multi_gap_loss_params->last_gap_limit = (multi_gap_loss_params->total_gaps * multi_gap_loss_params->gap_radix) + (int32_t)multi_gap_loss_params->gap_length;

    return rc;
}

int aeron_udp_channel_interceptor_multi_gap_loss_parse_callback(void *clientd, const char *key, const char *value)
{
    aeron_udp_channel_interceptor_multi_gap_loss_params_t *multi_gap_loss_params = clientd;
    int result = 0;

    if (strncmp(key, "term-id", sizeof("term-id")) == 0)
    {
        errno = 0;
        char *endptr;
        multi_gap_loss_params->term_id = strtol(value, &endptr, 10);

        if (errno != 0 || value == endptr)
        {
            AERON_SET_ERR(EINVAL, "Could not parse multi-gap-loss %s from: %s:", key, value);
            result = -1;
        }
    }
    else if (strncmp(key, "gap-radix", sizeof("gap-radix")) == 0)
    {
        errno = 0;
        char *endptr;
        multi_gap_loss_params->gap_radix = strtol(value, &endptr, 10);

        if (errno != 0 || value == endptr)
        {
            AERON_SET_ERR(EINVAL, "Could not parse multi-gap-loss %s from: %s:", key, value);
            result = -1;
        }
    }
    else if (strncmp(key, "gap-length", sizeof("gap-length")) == 0)
    {
        errno = 0;
        char *endptr;
        multi_gap_loss_params->gap_length = strtoul(value, &endptr, 10);

        if (errno != 0 || value == endptr)
        {
            AERON_SET_ERR(EINVAL, "Could not parse multi-gap-loss %s from: %s:", key, value);
            result = -1;
        }
    }
    else if (strncmp(key, "total-gaps", sizeof("total-gaps")) == 0)
    {
        errno = 0;
        char *endptr;
        multi_gap_loss_params->total_gaps = strtol(value, &endptr, 10);

        if (errno != 0 || value == endptr)
        {
            AERON_SET_ERR(EINVAL, "Could not parse multi-gap-loss %s from: %s:", key, value);
            result = -1;
        }
    }

    return result;
}
