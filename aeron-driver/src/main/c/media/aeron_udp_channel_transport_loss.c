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
#include "aeron_udp_channel_transport_loss.h"

#define AERON_CONFIG_GETENV_OR_DEFAULT(e, d) ((NULL == getenv(e)) ? (d) : getenv(e))
#define AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_LOSS_ARGS_ENV_VAR "AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_LOSS_ARGS"

static AERON_INIT_ONCE env_is_initialized = AERON_INIT_ONCE_VALUE;

static aeron_udp_channel_interceptor_loss_params_t aeron_udp_channel_interceptor_loss_params;
static unsigned short data_loss_xsubi[3];

int aeron_udp_channel_interceptor_loss_init_incoming(
    void **interceptor_state, aeron_driver_context_t *context, aeron_udp_channel_transport_affinity_t affinity);

aeron_udp_channel_interceptor_bindings_t *aeron_udp_channel_interceptor_loss_load(
    aeron_udp_channel_interceptor_bindings_t *delegate_bindings)
{
    aeron_udp_channel_interceptor_bindings_t *interceptor_bindings;
    if (aeron_alloc((void **)&interceptor_bindings, sizeof(aeron_udp_channel_interceptor_bindings_t)) < 0)
    {
        return NULL;
    }

    interceptor_bindings->incoming_init_func = aeron_udp_channel_interceptor_loss_init_incoming;
    interceptor_bindings->outgoing_init_func = NULL;
    interceptor_bindings->outgoing_send_func = NULL;
    interceptor_bindings->incoming_func = aeron_udp_channel_interceptor_loss_incoming;
    interceptor_bindings->outgoing_close_func = NULL;
    interceptor_bindings->incoming_close_func = NULL;
    interceptor_bindings->outgoing_transport_notification_func = NULL;
    interceptor_bindings->outgoing_publication_notification_func = NULL;
    interceptor_bindings->outgoing_image_notification_func = NULL;
    interceptor_bindings->incoming_transport_notification_func = NULL;
    interceptor_bindings->incoming_publication_notification_func = NULL;
    interceptor_bindings->incoming_image_notification_func = NULL;

    interceptor_bindings->meta_info.name = "loss";
    interceptor_bindings->meta_info.type = "interceptor";
    interceptor_bindings->meta_info.next_interceptor_bindings = delegate_bindings;

    return interceptor_bindings;
}

int aeron_udp_channel_interceptor_loss_configure(const aeron_udp_channel_interceptor_loss_params_t *loss_params)
{
    memcpy(&aeron_udp_channel_interceptor_loss_params, loss_params, sizeof(aeron_udp_channel_interceptor_loss_params_t));

    data_loss_xsubi[2] = (unsigned short)(aeron_udp_channel_interceptor_loss_params.seed & 0xFFFF);
    data_loss_xsubi[1] = (unsigned short)((aeron_udp_channel_interceptor_loss_params.seed >> 16) & 0xFFFF);
    data_loss_xsubi[0] = (unsigned short)((aeron_udp_channel_interceptor_loss_params.seed >> 32) & 0xFFFF);

    return 0;
}

void aeron_udp_channel_transport_loss_load_env(void)
{
    const char *args = AERON_CONFIG_GETENV_OR_DEFAULT(AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_LOSS_ARGS_ENV_VAR, "");
    char *args_dup = strdup(args);
    if (NULL == args_dup)
    {
        AERON_SET_ERR(errno, "%s", "Duplicating args string");
        return;
    }

    aeron_udp_channel_interceptor_loss_params_t *params;
    if (aeron_alloc((void **)&params, sizeof(aeron_udp_channel_interceptor_loss_params_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        aeron_free(args_dup);
        return;
    }

    if (aeron_udp_channel_interceptor_loss_parse_params(args_dup, params) >= 0)
    {
        aeron_udp_channel_interceptor_loss_configure(params);
    }

    aeron_free(params);
    aeron_free(args_dup);
}

int aeron_udp_channel_interceptor_loss_init_incoming(
    void **interceptor_state, aeron_driver_context_t *context, aeron_udp_channel_transport_affinity_t affinity)
{
    (void)aeron_thread_once(&env_is_initialized, aeron_udp_channel_transport_loss_load_env);

    *interceptor_state = NULL;

    return 0;
}

static bool aeron_udp_channel_interceptor_loss_should_drop_frame(
    const uint8_t *buffer, const double rate, const unsigned long msg_type_mask)
{
    const aeron_frame_header_t *frame_header = (aeron_frame_header_t *)buffer;
    const unsigned int msg_type_bit = 1U << (unsigned int)frame_header->type;
    const bool msg_type_matches_mask = (msg_type_bit & msg_type_mask) != 0;

    return 0.0 < rate && msg_type_matches_mask && (aeron_erand48(data_loss_xsubi) <= rate);
}

void aeron_udp_channel_interceptor_loss_incoming(
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
    if (!aeron_udp_channel_interceptor_loss_should_drop_frame(
        buffer, aeron_udp_channel_interceptor_loss_params.rate,
        aeron_udp_channel_interceptor_loss_params.recv_msg_type_mask))
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

int aeron_udp_channel_interceptor_loss_parse_params(char *uri, aeron_udp_channel_interceptor_loss_params_t *params)
{
    return aeron_uri_parse_params(uri, aeron_udp_channel_interceptor_loss_parse_callback, (void *)params);
}

int aeron_udp_channel_interceptor_loss_parse_callback(void *clientd, const char *key, const char *value)
{
    aeron_udp_channel_interceptor_loss_params_t *loss_params = clientd;
    int result = 0;

    if (strncmp(key, "rate", sizeof("rate")) == 0)
    {
        errno = 0;
        char *endptr;
        loss_params->rate = strtod(value, &endptr);

        if (errno != 0 || value == endptr)
        {
            AERON_SET_ERR(EINVAL, "Could not parse loss %s from: %s:", key, value);
            result = -1;
        }
    }
    else if (strncmp(key, "seed", sizeof("seed")) == 0)
    {
        errno = 0;
        char *endptr;
        loss_params->seed = strtoull(value, &endptr, 10);

        if (errno != 0 || value == endptr)
        {
            AERON_SET_ERR(EINVAL, "Could not parse seed %s from: %s:", key, value);
            result = -1;
        }
    }
    else if (strncmp(key, "recv-msg-mask", sizeof("recv-msg-mask")) == 0)
    {
        errno = 0;
        char *endptr;
        loss_params->recv_msg_type_mask = strtoul(value, &endptr, 16);

        if (errno != 0 || value == endptr)
        {
            AERON_SET_ERR(EINVAL, "Could not parse recv-msg-mask %s from: %s:", key, value);
            result = -1;
        }
    }

    return result;
}
