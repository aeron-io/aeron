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

#include "util/aeron_platform.h"

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <inttypes.h>
#include <errno.h>
#if !defined(AERON_COMPILER_MSVC)
#include <unistd.h>
#endif


#include "util/aeron_math.h"
#include "util/aeron_arrayutil.h"
#include "aeron_driver_conductor.h"
#include "aeron_position.h"
#include "aeron_driver_sender.h"
#include "aeron_driver_receiver.h"
#include "collections/aeron_bit_set.h"
#include "uri/aeron_uri.h"
#include "util/aeron_parse_util.h"


#define STATIC_BIT_SET_U64_LEN (512u)

#define COPY_ENDPOINT_NAME(_d, _s)                    \
do                                                    \
{                                                     \
    size_t _len = strnlen(_s, AERON_MAX_HOST_LENGTH); \
    memcpy(_d, _s, _len);                             \
    _d[_len] = '\0';                                  \
} while (0)                                           \

typedef struct aeron_time_tracking_name_resolver_stct
{
    aeron_name_resolver_t delegate_resolver;
    aeron_driver_context_t *context;
}
aeron_time_tracking_name_resolver_t;

const char * const AERON_DRIVER_CONDUCTOR_INVALID_DESTINATION_KEYS[] =
{
    AERON_URI_MTU_LENGTH_KEY,
    AERON_URI_RECEIVER_WINDOW_KEY,
    AERON_URI_SOCKET_RCVBUF_KEY,
    AERON_URI_SOCKET_SNDBUF_KEY,
    AERON_URI_RESPONSE_CORRELATION_ID_KEY,
    NULL
};

static bool aeron_driver_conductor_network_subscription_link_matches(
    const aeron_subscription_link_t *link,
    const aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id,
    bool has_session_id,
    int32_t session_id)
{
    return link->endpoint == endpoint &&
        link->stream_id == stream_id &&
        link->has_session_id == has_session_id &&
        (!has_session_id || link->session_id == session_id);
}

static bool aeron_driver_conductor_is_wildcard_or_session_id_match(
    const aeron_subscription_link_t *link, int32_t session_id)
{
    return (!link->has_session_id && !link->is_response) || (link->session_id == session_id);
}

static bool aeron_driver_conductor_network_subscription_link_matches_allowing_wildcard(
    const aeron_subscription_link_t *link,
    const aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id,
    int32_t session_id)
{
    return link->endpoint == endpoint &&
        link->stream_id == stream_id &&
        aeron_driver_conductor_is_wildcard_or_session_id_match(link, session_id);
}

static bool aeron_driver_conductor_spy_subscription_link_matches(
    const aeron_subscription_link_t *link,
    const aeron_network_publication_t *publication)
{
    aeron_udp_channel_t *const spy_channel = link->spy_channel;
    const aeron_udp_channel_t *publication_channel = publication->endpoint->conductor_fields.udp_channel;
    bool is_same_channel_tag =
        AERON_URI_INVALID_TAG != spy_channel->tag_id && spy_channel->tag_id == publication_channel->tag_id;
    return link->stream_id == publication->stream_id &&
        (is_same_channel_tag ||
            (aeron_driver_conductor_is_wildcard_or_session_id_match(link, publication->session_id) &&
                publication_channel->canonical_length == spy_channel->canonical_length &&
                0 == strncmp(
                    publication_channel->canonical_form,
                    spy_channel->canonical_form,
                    publication_channel->canonical_length)));
}

static bool aeron_driver_conductor_subscription_link_matches_ipc_publication(
    const aeron_subscription_link_t *link, const aeron_ipc_publication_t *publication)
{
    return link->stream_id == publication->stream_id &&
        aeron_driver_conductor_is_wildcard_or_session_id_match(link, publication->session_id);
}

static bool aeron_driver_conductor_has_network_subscription_interest(
    aeron_driver_conductor_t *conductor,
    const aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id,
    int32_t session_id)
{
    for (size_t i = 0, length = conductor->network_subscriptions.length; i < length; i++)
    {
        aeron_subscription_link_t *link = &conductor->network_subscriptions.array[i];

        if (aeron_driver_conductor_network_subscription_link_matches_allowing_wildcard(
            link,
            endpoint,
            stream_id,
            session_id))
        {
            return true;
        }
    }

    return false;
}

static bool aeron_driver_conductor_is_oldest_subscription_sparse(
    aeron_driver_conductor_t *conductor,
    const aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id,
    int32_t session_id,
    int64_t highest_id)
{
    int64_t registration_id = highest_id;
    bool is_sparse = conductor->context->term_buffer_sparse_file;

    for (size_t i = 0, length = conductor->network_subscriptions.length; i < length; i++)
    {
        aeron_subscription_link_t *link = &conductor->network_subscriptions.array[i];

        if (aeron_driver_conductor_network_subscription_link_matches_allowing_wildcard(
            link,
            endpoint,
            stream_id,
            session_id) &&
            link->registration_id < registration_id)
        {
            registration_id = link->registration_id;
            is_sparse = link->is_sparse;
        }
    }

    return is_sparse;
}

static bool aeron_driver_conductor_receive_endpoint_has_clashing_timestamp_offsets(
    aeron_driver_conductor_t *conductor,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_udp_channel_t *channel)
{
    if (endpoint->conductor_fields.udp_channel->media_rcv_timestamp_offset != channel->media_rcv_timestamp_offset)
    {
        AERON_SET_ERR(
            EINVAL,
            "option conflicts with existing subscription: %s=%" PRId32 " %s existingChannel=%.*s channel=%.*s",
            AERON_URI_MEDIA_RCV_TIMESTAMP_OFFSET_KEY,
            channel->media_rcv_timestamp_offset,
            aeron_driver_uri_get_offset_info(channel->media_rcv_timestamp_offset),
            (int)endpoint->conductor_fields.udp_channel->uri_length,
            endpoint->conductor_fields.udp_channel->original_uri,
            (int)channel->uri_length,
            channel->original_uri);
        return true;
    }

    if (endpoint->conductor_fields.udp_channel->channel_rcv_timestamp_offset != channel->channel_rcv_timestamp_offset)
    {
        AERON_SET_ERR(
            EINVAL,
            "option conflicts with existing subscription: %s=%" PRId32 " %s existingChannel=%.*s channel=%.*s",
            AERON_URI_CHANNEL_RCV_TIMESTAMP_OFFSET_KEY,
            channel->channel_rcv_timestamp_offset,
            aeron_driver_uri_get_offset_info(channel->channel_rcv_timestamp_offset),
            (int)endpoint->conductor_fields.udp_channel->uri_length,
            endpoint->conductor_fields.udp_channel->original_uri,
            (int)channel->uri_length,
            channel->original_uri);
        return true;
    }

    return false;
}

static int aeron_driver_conductor_find_existing_receive_channel_endpoint(
    aeron_driver_conductor_t *conductor,
    aeron_udp_channel_t *channel,
    aeron_receive_channel_endpoint_t **result_endpoint)
{
    aeron_receive_channel_endpoint_t *endpoint;
    if (AERON_URI_INVALID_TAG != channel->tag_id)
    {
        for (size_t i = 0, size = conductor->receive_channel_endpoints.length; i < size; i++)
        {
            endpoint = conductor->receive_channel_endpoints.array[i].endpoint;

            bool has_match = false;
            if (aeron_receive_channel_endpoint_matches_tag(endpoint, channel, &has_match) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                return -1;
            }

            if (has_match)
            {
                *result_endpoint = endpoint;
                return 0;
            }
        }
    }

    endpoint = aeron_str_to_ptr_hash_map_get(
        &conductor->receive_channel_endpoint_by_channel_map, channel->canonical_form, channel->canonical_length);
    if (NULL != endpoint &&
        AERON_URI_INVALID_TAG != endpoint->conductor_fields.udp_channel->tag_id &&
        AERON_URI_INVALID_TAG != channel->tag_id &&
        channel->tag_id != endpoint->conductor_fields.udp_channel->tag_id)
    {
        endpoint = NULL;
    }

    *result_endpoint = endpoint;
    return 0;
}
static int aeron_driver_conductor_find_existing_send_channel_endpoint(
    aeron_driver_conductor_t *conductor,
    aeron_udp_channel_t *channel,
    aeron_send_channel_endpoint_t **result_endpoint)
{
    if (AERON_URI_INVALID_TAG != channel->tag_id)
    {
        for (size_t i = 0, size = conductor->send_channel_endpoints.length; i < size; i++)
        {
            aeron_send_channel_endpoint_t *endpoint = conductor->send_channel_endpoints.array[i].endpoint;

            bool has_match = false;
            if (aeron_send_channel_endpoint_matches_tag(endpoint, channel, &has_match) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                return -1;
            }

            if (has_match)
            {
                *result_endpoint = endpoint;
                return 0;
            }
        }

        if (!channel->has_explicit_control &&
            AERON_UDP_CHANNEL_CONTROL_MODE_MANUAL != channel->control_mode &&
            NULL == channel->uri.params.udp.endpoint)
        {
            AERON_SET_ERR(
                EINVAL,
                "URI must have explicit control, endpoint, or be manual control-mode when original: channel=%.*s",
                (int)channel->uri_length,
                channel->original_uri);
            return -1;
        }
    }

    aeron_send_channel_endpoint_t *endpoint = aeron_str_to_ptr_hash_map_get(
        &conductor->send_channel_endpoint_by_channel_map, channel->canonical_form, channel->canonical_length);
    if (NULL != endpoint &&
        AERON_URI_INVALID_TAG != endpoint->conductor_fields.udp_channel->tag_id &&
        AERON_URI_INVALID_TAG != channel->tag_id &&
        channel->tag_id != endpoint->conductor_fields.udp_channel->tag_id)
    {
        endpoint = NULL;
    }

    *result_endpoint = endpoint;
    return 0;
}

static bool aeron_driver_conductor_has_clashing_subscription(
    aeron_driver_conductor_t *conductor,
    aeron_udp_channel_t *channel,
    int32_t stream_id,
    aeron_driver_uri_subscription_params_t *params)
{
    aeron_receive_channel_endpoint_t *endpoint;
    if (aeron_driver_conductor_find_existing_receive_channel_endpoint(conductor, channel, &endpoint) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return true;
    }

    if (NULL != endpoint)
    {
        if (aeron_driver_conductor_receive_endpoint_has_clashing_timestamp_offsets(conductor, endpoint, channel))
        {
            AERON_APPEND_ERR("%s", "");
            return true;
        }

        for (size_t i = 0, length = conductor->network_subscriptions.length; i < length; i++)
        {
            aeron_subscription_link_t *link = &conductor->network_subscriptions.array[i];
            bool matches_tag = AERON_URI_INVALID_TAG != channel->tag_id ||
                link->endpoint->conductor_fields.udp_channel->tag_id == channel->tag_id;

            if (matches_tag && aeron_driver_conductor_network_subscription_link_matches(
                link,
                endpoint,
                stream_id,
                params->has_session_id,
                params->session_id))
            {
                if (params->is_reliable != link->is_reliable)
                {
                    const char *value = params->is_reliable ? "true" : "false";
                    AERON_SET_ERR(
                        EINVAL,
                        "option conflicts with existing subscription: reliable=%s existingChannel=%.*s channel=%.*s",
                        value,
                        link->channel_length,
                        link->channel,
                        (int)channel->uri_length,
                        channel->original_uri);
                    return true;
                }

                if (params->is_rejoin != link->is_rejoin)
                {
                    const char *value = params->is_rejoin ? "true" : "false";
                    AERON_SET_ERR(
                        EINVAL,
                        "option conflicts with existing subscription: rejoin=%s existingChannel=%.*s channel=%.*s",
                        value,
                        link->channel_length,
                        link->channel,
                        (int)channel->uri_length,
                        channel->original_uri);
                    return true;
                }

                if (params->is_response != link->is_response)
                {
                    const char *value = params->is_response ? "true" : "false";
                    AERON_SET_ERR(
                        EINVAL,
                        "option conflicts with existing subscription: isResponse=%s existingChannel=%.*s channel=%.*s",
                        value,
                        link->channel_length,
                        link->channel,
                        (int)channel->uri_length,
                        channel->original_uri);
                    return true;
                }
            }
        }
    }

    return false;
}

static bool aeron_driver_conductor_send_endpoint_has_clashing_timestamp_offsets(
    aeron_driver_conductor_t *conductor,
    aeron_send_channel_endpoint_t *endpoint,
    aeron_udp_channel_t *channel)
{
    if (endpoint->conductor_fields.udp_channel->channel_snd_timestamp_offset != channel->channel_snd_timestamp_offset)
    {
        AERON_SET_ERR(
            EINVAL,
            "option conflicts with existing subscription: %s=%" PRId32 " %s existingChannel=%.*s channel=%.*s",
            AERON_URI_CHANNEL_SND_TIMESTAMP_OFFSET_KEY,
            channel->channel_snd_timestamp_offset,
            aeron_driver_uri_get_offset_info(channel->channel_snd_timestamp_offset),
            (int)endpoint->conductor_fields.udp_channel->uri_length,
            endpoint->conductor_fields.udp_channel->original_uri,
            (int)channel->uri_length,
            channel->original_uri);
        return true;
    }

    return false;
}


static int aeron_driver_conductor_validate_destination_uri_prefix(
    const char *channel_uri, int32_t channel_length, const char *transport_direction)
{
    if ((int32_t)AERON_SPY_PREFIX_LEN <= channel_length &&
        0 == strncmp(channel_uri, AERON_SPY_PREFIX, AERON_SPY_PREFIX_LEN))
    {
        AERON_SET_ERR(
            -AERON_ERROR_CODE_INVALID_CHANNEL,
            "Aeron spies are invalid as %s destinations: %.*s",
            transport_direction,
            (int)channel_length,
            channel_uri);

        return -1;
    }

    return 0;
}

static int aeron_driver_conductor_validate_send_destination_uri(aeron_uri_t *uri, size_t uri_length)
{
    if (AERON_URI_UDP == uri->type && NULL != uri->params.udp.endpoint)
    {
        aeron_parsed_address_t parsed_address = { .host = { 0 }, .port = { 0 }, .ip_version_hint= 0 };

        if (0 <= aeron_address_split(uri->params.udp.endpoint, &parsed_address))
        {
            if (0 == strcmp("0", parsed_address.port))
            {
                AERON_SET_ERR(
                    EINVAL,
                    "%s has port=0 for send destination: channel=%.*s",
                    AERON_UDP_CHANNEL_ENDPOINT_KEY,
                    (int)uri_length,
                    uri->mutable_uri);
                return -1;
            }
        }
    }

    return 0;
}

static int aeron_driver_conductor_validate_destination_uri_params(aeron_uri_t *uri, size_t uri_length)
{
    aeron_uri_params_t *params = NULL;
    switch (uri->type)
    {
        case AERON_URI_UDP:
            params = &uri->params.udp.additional_params;
            break;

        case AERON_URI_IPC:
            params = &uri->params.ipc.additional_params;
            break;

        case AERON_URI_UNKNOWN:
            AERON_SET_ERR(EINVAL, "unknown uri type: channel=%.*s", (int)uri_length, uri->mutable_uri);
            break;
    }

    if (NULL == params)
    {
        return -1;
    }

    for (int i = 0; NULL != AERON_DRIVER_CONDUCTOR_INVALID_DESTINATION_KEYS[i]; i++)
    {
        const char *param = AERON_DRIVER_CONDUCTOR_INVALID_DESTINATION_KEYS[i];
        if (NULL != aeron_uri_find_param_value(params, param))
        {
            AERON_SET_ERR(
                -AERON_ERROR_CODE_INVALID_CHANNEL,
                "destinations must not contain the key: %s channel=%.*s",
                param,
                (int)uri_length,
                uri->mutable_uri);
            return -1;
        }
    }

    if (AERON_URI_UDP == uri->type &&
        NULL != uri->params.udp.control_mode &&
        0 == strcmp(uri->params.udp.control_mode, AERON_UDP_CHANNEL_CONTROL_MODE_RESPONSE_VALUE))
    {
        AERON_SET_ERR(
            -AERON_ERROR_CODE_INVALID_CHANNEL,
            "destinations may not specify %s=%s",
            AERON_UDP_CHANNEL_CONTROL_MODE_KEY,
            AERON_UDP_CHANNEL_CONTROL_MODE_RESPONSE_VALUE);
        return -1;
    }

    return 0;
}

static int aeron_driver_conductor_validate_initial_window_for_rcvbuf(
    aeron_driver_uri_subscription_params_t *params,
    size_t endpoint_socket_rcvbuf,
    size_t os_default_socket_rcvbuf,
    aeron_udp_channel_t *channel,
    aeron_udp_channel_t *existing_channel)
{
    if (0 != endpoint_socket_rcvbuf && endpoint_socket_rcvbuf < params->initial_window_length)
    {
        if (NULL == existing_channel)
        {
            AERON_SET_ERR(
                EINVAL,
                "Initial window greater than SO_SNDBUF for channel: rcv-wnd=%" PRIu64 " so-rcvbuf=%" PRIu64 " channel=%.*s",
                params->initial_window_length,
                endpoint_socket_rcvbuf,
                (int)channel->uri_length,
                channel->original_uri);
        }
        else
        {
            AERON_SET_ERR(
                EINVAL,
                "Initial window greater than SO_SNDBUF for channel: rcv-wnd=%" PRIu64 " so-rcvbuf=%" PRIu64 " existingChannel=%.*s channel=%.*s",
                params->initial_window_length,
                endpoint_socket_rcvbuf,
                (int)existing_channel->uri_length,
                existing_channel->original_uri,
                (int)channel->uri_length,
                channel->original_uri);
        }
        return -1;
    }

    if (0 == endpoint_socket_rcvbuf && os_default_socket_rcvbuf < params->initial_window_length)
    {
        if (NULL == existing_channel)
        {
            AERON_SET_ERR(
                EINVAL,
                "Initial window greater than SO_SNDBUF for channel: rcv-wnd=%" PRIu64 " so-rcvbuf=%" PRIu64 " (OS default) channel=%.*s",
                params->initial_window_length,
                endpoint_socket_rcvbuf,
                (int)channel->uri_length,
                channel->original_uri);
        }
        else
        {
            AERON_SET_ERR(
                EINVAL,
                "Initial window greater than SO_SNDBUF for channel: rcv-wnd=%" PRIu64 " so-rcvbuf=%" PRIu64 " (OS default) existingChannel=%.*s channel=%.*s",
                params->initial_window_length,
                endpoint_socket_rcvbuf,
                (int)existing_channel->uri_length,
                existing_channel->original_uri,
                (int)channel->uri_length,
                channel->original_uri);
        }
        return -1;
    }

    return 0;
}

static int aeron_driver_conductor_validate_channel_buffer_length(
    const char *param_name,
    size_t new_length,
    size_t existing_length,
    aeron_udp_channel_t *channel,
    aeron_udp_channel_t *existing_channel)
{
    if (0 != new_length && new_length != existing_length)
    {
        if (0 == existing_length)
        {
            AERON_SET_ERR(
                EINVAL,
                "%s=%" PRIu64 " does not match existing value of OS default: existingChannel=%.*s channel=%.*s",
                param_name,
                (uint64_t)new_length,
                (int)existing_channel->uri_length,
                existing_channel->original_uri,
                (int)channel->uri_length,
                channel->original_uri);
        }
        else
        {
            AERON_SET_ERR(
                EINVAL,
                "%s=%" PRIu64 " does not match existing value of %" PRIu64 ": existingChannel=%.*s channel=%.*s",
                param_name,
                (uint64_t)new_length,
                (uint64_t)existing_length,
                (int)existing_channel->uri_length,
                existing_channel->original_uri,
                (int)channel->uri_length,
                channel->original_uri);
        }

        return -1;
    }

    return 0;
}

static int aeron_driver_conductor_validate_endpoint_for_publication(aeron_udp_channel_t *udp_channel)
{
    if (!aeron_udp_channel_is_multi_destination(udp_channel) &&
        udp_channel->has_explicit_endpoint &&
        aeron_is_wildcard_port(&udp_channel->remote_data))
    {
        AERON_SET_ERR(
            EINVAL,
            "%s has port=0 for publication: channel=%.*s",
            AERON_UDP_CHANNEL_ENDPOINT_KEY,
            (int)udp_channel->uri_length,
            udp_channel->original_uri);
        return -1;
    }

    return 0;
}

static int aeron_driver_conductor_validate_control_for_publication(aeron_udp_channel_t *udp_channel)
{
    if (AERON_UDP_CHANNEL_CONTROL_MODE_DYNAMIC == udp_channel->control_mode && !udp_channel->has_explicit_control)
    {
        AERON_SET_ERR(
            EINVAL,
            "'control-mode=dynamic' requires that 'control' parameter is set, channel=%.*s",
            (int)udp_channel->uri_length,
            udp_channel->original_uri);
        return -1;
    }

    if (udp_channel->has_explicit_control &&
        !udp_channel->has_explicit_endpoint &&
        NULL == udp_channel->uri.params.udp.control_mode)
    {
        AERON_SET_ERR(
            EINVAL,
            "'control' parameter requires that either 'endpoint' or 'control-mode' is specified, channel=%.*s",
            (int)udp_channel->uri_length,
            udp_channel->original_uri);
        return -1;
    }

    return 0;
}

static int aeron_driver_conductor_validate_control_for_subscription(aeron_udp_channel_t *udp_channel)
{
    if (udp_channel->has_explicit_control &&
        aeron_is_wildcard_port(&udp_channel->local_control))
    {
        AERON_SET_ERR(
            EINVAL,
            "%s has port=0 for subscription: channel=%.*s",
            AERON_UDP_CHANNEL_CONTROL_KEY,
            (int)udp_channel->uri_length,
            udp_channel->original_uri);
        return -1;
    }

    return 0;
}

static int aeron_driver_conductor_validate_response_subscription(
    aeron_driver_conductor_t *conductor,
    aeron_udp_channel_t *udp_channel,
    aeron_driver_uri_publication_params_t *param)
{
    if (AERON_UDP_CHANNEL_CONTROL_MODE_RESPONSE != udp_channel->control_mode &&
        AERON_NULL_VALUE != param->response_correlation_id)
    {
        for (int last_index = (int)conductor->network_subscriptions.length - 1, i = last_index; i >= 0; i--)
        {
            aeron_subscription_link_t *link = &conductor->network_subscriptions.array[i];
            if (param->response_correlation_id == link->registration_id)
            {
                return 0;
            }
        }

        AERON_SET_ERR(
            EINVAL,
            "unable to find response subscription for response-correlation-id=%" PRId64,
            param->response_correlation_id);
        return -1;
    }

    return 0;
}

static int aeron_driver_conductor_validate_experimental_features(
    bool enable_experimental_features,
    aeron_udp_channel_t *udp_channel)
{
    if (enable_experimental_features)
    {
        return 0;
    }

    /*
     * Put experimental feature validation here.
     */

    return 0;
}

static int aeron_time_tracking_name_resolver_resolve(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    struct sockaddr_storage *address)
{
    aeron_time_tracking_name_resolver_t *time_tracking_resolver = (aeron_time_tracking_name_resolver_t *)resolver->state;
    aeron_driver_context_t *context = time_tracking_resolver->context;
    int64_t begin_ns = context->nano_clock();
    aeron_duty_cycle_tracker_t *tracker = context->name_resolver_time_tracker;
    tracker->update(tracker->state, begin_ns);

    int result = time_tracking_resolver->delegate_resolver.resolve_func(
        &time_tracking_resolver->delegate_resolver,
        name,
        uri_param_name,
        is_re_resolution,
        address);

    int64_t end_ns = context->nano_clock();
    tracker->measure_and_update(tracker->state, end_ns);

    if (NULL != context->log.on_name_resolve)
    {
        struct sockaddr_storage *resolved_address = 0 <= result ? address : NULL;
        context->log.on_name_resolve(
            &time_tracking_resolver->delegate_resolver, end_ns - begin_ns, name, is_re_resolution, resolved_address);
    }

    return result;
}

static int aeron_time_tracking_name_resolver_lookup(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_lookup,
    const char **resolved_name)
{
    aeron_time_tracking_name_resolver_t *time_tracking_resolver = (aeron_time_tracking_name_resolver_t *)resolver->state;
    aeron_driver_context_t *context = time_tracking_resolver->context;
    int64_t begin_ns = context->nano_clock();
    aeron_duty_cycle_tracker_t *tracker = context->name_resolver_time_tracker;
    tracker->update(tracker->state, begin_ns);

    int result = time_tracking_resolver->delegate_resolver.lookup_func(
        &time_tracking_resolver->delegate_resolver,
        name,
        uri_param_name,
        is_re_lookup,
        resolved_name);

    int64_t end_ns = context->nano_clock();
    tracker->measure_and_update(tracker->state, end_ns);

    if (NULL != context->log.on_name_lookup)
    {
        const char *result_name = 0 <= result ? *resolved_name : NULL;
        context->log.on_name_lookup(
            &time_tracking_resolver->delegate_resolver, end_ns - begin_ns, name, is_re_lookup, result_name);
    }

    return result;
}

static int aeron_time_tracking_name_resolver_do_work(aeron_name_resolver_t *resolver, int64_t now_ms)
{
    aeron_time_tracking_name_resolver_t *time_tracking_resolver = (aeron_time_tracking_name_resolver_t *)resolver->state;
    return time_tracking_resolver->delegate_resolver.do_work_func(&time_tracking_resolver->delegate_resolver, now_ms);
}

static int aeron_time_tracking_name_resolver_close(aeron_name_resolver_t *resolver)
{
    aeron_time_tracking_name_resolver_t *time_tracking_resolver = (aeron_time_tracking_name_resolver_t *)resolver->state;
    time_tracking_resolver->delegate_resolver.close_func(&time_tracking_resolver->delegate_resolver);
    aeron_free(time_tracking_resolver);
    return 0;
}

static bool aeron_driver_conductor_treat_image_as_multicast(
    aeron_udp_channel_t *channel, uint8_t flags, aeron_inferable_boolean_t is_group)
{
    bool is_group_from_flag = ((flags & AERON_SETUP_HEADER_GROUP_FLAG) == AERON_SETUP_HEADER_GROUP_FLAG);
    return AERON_INFER == is_group ?
        channel->is_multicast || is_group_from_flag : AERON_FORCE_TRUE == is_group;
}

int aeron_driver_conductor_init(aeron_driver_conductor_t *conductor, aeron_driver_context_t *context)
{
    if (aeron_mpsc_rb_init(
        &conductor->to_driver_commands, context->to_driver_buffer, context->to_driver_buffer_length) < 0)
    {
        return -1;
    }

    if (aeron_broadcast_transmitter_init(
        &conductor->to_clients, context->to_clients_buffer, context->to_clients_buffer_length) < 0)
    {
        return -1;
    }

    int64_t free_to_reuse_timeout_ms = 0;
    if (context->counter_free_to_reuse_ns > 0)
    {
        free_to_reuse_timeout_ms = (int64_t)context->counter_free_to_reuse_ns / (1000 * 1000LL);
        free_to_reuse_timeout_ms = free_to_reuse_timeout_ms <= 0 ? 1 : free_to_reuse_timeout_ms;
    }

    if (aeron_counters_manager_init(
        &conductor->counters_manager,
        context->counters_metadata_buffer,
        AERON_COUNTERS_METADATA_BUFFER_LENGTH(context->counters_values_buffer_length),
        context->counters_values_buffer,
        context->counters_values_buffer_length,
        context->cached_clock,
        free_to_reuse_timeout_ms) < 0)
    {
        return -1;
    }

    if (aeron_system_counters_init(&conductor->system_counters, &conductor->counters_manager) < 0)
    {
        return -1;
    }

    if (aeron_distinct_error_log_init(
        &conductor->error_log, context->error_buffer, context->error_buffer_length, context->epoch_clock) < 0)
    {
        return -1;
    }

    if (aeron_str_to_ptr_hash_map_init(
        &conductor->send_channel_endpoint_by_channel_map, 64, AERON_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        return -1;
    }

    if (aeron_str_to_ptr_hash_map_init(
        &conductor->receive_channel_endpoint_by_channel_map, 64, AERON_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        return -1;
    }

    if (aeron_loss_reporter_init(&conductor->loss_reporter, context->loss_report.addr, context->loss_report.length) < 0)
    {
        return -1;
    }

    conductor->conductor_proxy.command_queue = &context->conductor_command_queue;
    conductor->conductor_proxy.fail_counter = aeron_counters_manager_addr(
        &conductor->counters_manager, AERON_SYSTEM_COUNTER_CONDUCTOR_PROXY_FAILS);
    conductor->conductor_proxy.threading_mode = context->threading_mode;
    conductor->conductor_proxy.conductor = conductor;

    if (aeron_executor_init(&conductor->executor, context->async_executor_threads >= 1, NULL, conductor) < 0)
    {
        return -1;
    }
    conductor->async_client_command_in_flight = false;

    conductor->clients.array = NULL;
    conductor->clients.capacity = 0;
    conductor->clients.length = 0;
    conductor->clients.on_time_event = aeron_client_on_time_event;
    conductor->clients.has_reached_end_of_life = aeron_client_has_reached_end_of_life;
    conductor->clients.delete_func = aeron_client_delete;
    conductor->clients.free_func = NULL;

    conductor->ipc_publications.array = NULL;
    conductor->ipc_publications.length = 0;
    conductor->ipc_publications.capacity = 0;
    conductor->ipc_publications.on_time_event = aeron_ipc_publication_entry_on_time_event;
    conductor->ipc_publications.has_reached_end_of_life = aeron_ipc_publication_entry_has_reached_end_of_life;
    conductor->ipc_publications.delete_func = aeron_ipc_publication_entry_delete;
    conductor->ipc_publications.free_func = aeron_ipc_publication_entry_free;

    conductor->network_publications.array = NULL;
    conductor->network_publications.length = 0;
    conductor->network_publications.capacity = 0;
    conductor->network_publications.on_time_event = aeron_network_publication_entry_on_time_event;
    conductor->network_publications.has_reached_end_of_life = aeron_network_publication_entry_has_reached_end_of_life;
    conductor->network_publications.delete_func = aeron_network_publication_entry_delete;
    conductor->network_publications.free_func = aeron_network_publication_entry_free;

    conductor->send_channel_endpoints.array = NULL;
    conductor->send_channel_endpoints.length = 0;
    conductor->send_channel_endpoints.capacity = 0;
    conductor->send_channel_endpoints.on_time_event = aeron_send_channel_endpoint_entry_on_time_event;
    conductor->send_channel_endpoints.has_reached_end_of_life =
        aeron_send_channel_endpoint_entry_has_reached_end_of_life;
    conductor->send_channel_endpoints.delete_func = aeron_send_channel_endpoint_entry_delete;
    conductor->send_channel_endpoints.free_func = NULL;

    conductor->receive_channel_endpoints.array = NULL;
    conductor->receive_channel_endpoints.length = 0;
    conductor->receive_channel_endpoints.capacity = 0;
    conductor->receive_channel_endpoints.on_time_event = aeron_receive_channel_endpoint_entry_on_time_event;
    conductor->receive_channel_endpoints.has_reached_end_of_life =
        aeron_receive_channel_endpoint_entry_has_reached_end_of_life;
    conductor->receive_channel_endpoints.delete_func = aeron_receive_channel_endpoint_entry_delete;
    conductor->receive_channel_endpoints.free_func = NULL;

    conductor->publication_images.array = NULL;
    conductor->publication_images.length = 0;
    conductor->publication_images.capacity = 0;
    conductor->publication_images.on_time_event = aeron_publication_image_entry_on_time_event;
    conductor->publication_images.has_reached_end_of_life = aeron_publication_image_entry_has_reached_end_of_life;
    conductor->publication_images.delete_func = aeron_publication_image_entry_delete;
    conductor->publication_images.free_func = aeron_publication_image_entry_free;

    conductor->lingering_resources.array = NULL;
    conductor->lingering_resources.length = 0;
    conductor->lingering_resources.capacity = 0;
    conductor->lingering_resources.on_time_event = aeron_linger_resource_entry_on_time_event;
    conductor->lingering_resources.has_reached_end_of_life = aeron_linger_resource_entry_has_reached_end_of_life;
    conductor->lingering_resources.delete_func = aeron_linger_resource_entry_delete;
    conductor->lingering_resources.free_func = NULL;

    conductor->ipc_subscriptions.array = NULL;
    conductor->ipc_subscriptions.length = 0;
    conductor->ipc_subscriptions.capacity = 0;

    conductor->network_subscriptions.array = NULL;
    conductor->network_subscriptions.length = 0;
    conductor->network_subscriptions.capacity = 0;

    conductor->spy_subscriptions.array = NULL;
    conductor->spy_subscriptions.length = 0;
    conductor->spy_subscriptions.capacity = 0;

    if (aeron_deque_init(&conductor->end_of_life_queue, 1024, sizeof(aeron_end_of_life_resource_t)))
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    conductor->errors_counter = aeron_counters_manager_addr(&conductor->counters_manager, AERON_SYSTEM_COUNTER_ERRORS);
    conductor->images_rejected_counter = aeron_counters_manager_addr(
        &conductor->counters_manager, AERON_SYSTEM_COUNTER_IMAGES_REJECTED);
    conductor->unblocked_commands_counter = aeron_counters_manager_addr(
        &conductor->counters_manager, AERON_SYSTEM_COUNTER_UNBLOCKED_COMMANDS);
    conductor->client_timeouts_counter = aeron_counters_manager_addr(
        &conductor->counters_manager, AERON_SYSTEM_COUNTER_CLIENT_TIMEOUTS);

    int64_t now_ns = context->nano_clock();
    aeron_clock_update_cached_time(context->cached_clock, context->epoch_clock(), now_ns);

    conductor->clock_update_deadline_ns = now_ns + AERON_DRIVER_CONDUCTOR_CLOCK_UPDATE_INTERNAL_NS;
    conductor->timeout_check_deadline_ns = now_ns;
    conductor->time_of_last_to_driver_position_change_ns = now_ns;
    conductor->next_session_id = aeron_randomised_int32();
    conductor->publication_reserved_session_id_low = context->publication_reserved_session_id_low;
    conductor->publication_reserved_session_id_high = context->publication_reserved_session_id_high;
    conductor->last_command_consumer_position = aeron_mpsc_rb_consumer_position(&conductor->to_driver_commands);

    context->conductor_duty_cycle_stall_tracker.max_cycle_time_counter = aeron_counters_manager_addr(
        &conductor->counters_manager, AERON_SYSTEM_COUNTER_CONDUCTOR_MAX_CYCLE_TIME);
    context->conductor_duty_cycle_stall_tracker.cycle_time_threshold_exceeded_counter = aeron_counters_manager_addr(
        &conductor->counters_manager, AERON_SYSTEM_COUNTER_CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED);
    context->sender_duty_cycle_stall_tracker.max_cycle_time_counter = aeron_counters_manager_addr(
        &conductor->counters_manager, AERON_SYSTEM_COUNTER_SENDER_MAX_CYCLE_TIME);
    context->sender_duty_cycle_stall_tracker.cycle_time_threshold_exceeded_counter = aeron_counters_manager_addr(
        &conductor->counters_manager, AERON_SYSTEM_COUNTER_SENDER_CYCLE_TIME_THRESHOLD_EXCEEDED);
    context->receiver_duty_cycle_stall_tracker.max_cycle_time_counter = aeron_counters_manager_addr(
        &conductor->counters_manager, AERON_SYSTEM_COUNTER_RECEIVER_MAX_CYCLE_TIME);
    context->receiver_duty_cycle_stall_tracker.cycle_time_threshold_exceeded_counter = aeron_counters_manager_addr(
        &conductor->counters_manager, AERON_SYSTEM_COUNTER_RECEIVER_CYCLE_TIME_THRESHOLD_EXCEEDED);
    context->name_resolver_time_stall_tracker.max_cycle_time_counter = aeron_counters_manager_addr(
        &conductor->counters_manager, AERON_SYSTEM_COUNTER_NAME_RESOLVER_MAX_TIME);
    context->name_resolver_time_stall_tracker.cycle_time_threshold_exceeded_counter = aeron_counters_manager_addr(
        &conductor->counters_manager, AERON_SYSTEM_COUNTER_NAME_RESOLVER_TIME_THRESHOLD_EXCEEDED);

    aeron_time_tracking_name_resolver_t *time_tracking_name_resolver = NULL;
    if (aeron_alloc((void **)&time_tracking_name_resolver, sizeof(aeron_time_tracking_name_resolver_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to allocate aeron_time_tracking_name_resolver_t");
        return -1;
    }
    time_tracking_name_resolver->context = context;

    if (aeron_name_resolver_init(
        &time_tracking_name_resolver->delegate_resolver,
        context->name_resolver_init_args,
        context) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to init name resolver");
        aeron_free(time_tracking_name_resolver);
        return -1;
    }

    conductor->name_resolver.name = "time_tracking_name_resolver";
    conductor->name_resolver.resolve_func = aeron_time_tracking_name_resolver_resolve;
    conductor->name_resolver.lookup_func = aeron_time_tracking_name_resolver_lookup;
    conductor->name_resolver.do_work_func = aeron_time_tracking_name_resolver_do_work;
    conductor->name_resolver.close_func = aeron_time_tracking_name_resolver_close;
    conductor->name_resolver.state = time_tracking_name_resolver;

    char label[AERON_COUNTER_MAX_LABEL_LENGTH];
    const char *driver_name = NULL == context->resolver_name ? "" : context->resolver_name;

    int label_length = snprintf(label, sizeof(label), ": driverName=%s", driver_name);
    if (label_length > 0)
    {
        aeron_counters_manager_append_to_label(
            &conductor->counters_manager, AERON_SYSTEM_COUNTER_RESOLUTION_CHANGES, (size_t)label_length, label);
    }

    label_length = snprintf(label, sizeof(label), ": %s",
        aeron_driver_threading_mode_to_string(context->threading_mode));
    if (label_length > 0)
    {
        aeron_counters_manager_append_to_label(
            &conductor->counters_manager,
            AERON_SYSTEM_COUNTER_CONDUCTOR_MAX_CYCLE_TIME,
            (size_t)label_length,
            label);
    }

    label_length = snprintf(label, sizeof(label), ": %s",
        aeron_driver_threading_mode_to_string(context->threading_mode));
    if (label_length > 0)
    {
        aeron_counters_manager_append_to_label(
            &conductor->counters_manager,
            AERON_SYSTEM_COUNTER_SENDER_MAX_CYCLE_TIME,
            (size_t)label_length,
            label);
    }

    label_length = snprintf(label, sizeof(label), ": %s",
        aeron_driver_threading_mode_to_string(context->threading_mode));
    if (label_length > 0)
    {
        aeron_counters_manager_append_to_label(
            &conductor->counters_manager,
            AERON_SYSTEM_COUNTER_RECEIVER_MAX_CYCLE_TIME,
            (size_t)label_length,
            label);
    }

    label_length = snprintf(label, sizeof(label), ": threshold=%" PRIu64 "ns %s",
        context->conductor_duty_cycle_stall_tracker.cycle_threshold_ns,
        aeron_driver_threading_mode_to_string(context->threading_mode));
    if (label_length > 0)
    {
        aeron_counters_manager_append_to_label(
            &conductor->counters_manager,
            AERON_SYSTEM_COUNTER_CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED,
            (size_t)label_length,
            label);
    }

    label_length = snprintf(label, sizeof(label), ": threshold=%" PRIu64 "ns %s",
        context->sender_duty_cycle_stall_tracker.cycle_threshold_ns,
        aeron_driver_threading_mode_to_string(context->threading_mode));
    if (label_length > 0)
    {
        aeron_counters_manager_append_to_label(
            &conductor->counters_manager,
            AERON_SYSTEM_COUNTER_SENDER_CYCLE_TIME_THRESHOLD_EXCEEDED,
            (size_t)label_length,
            label);
    }

    label_length = snprintf(label, sizeof(label), ": threshold=%" PRIu64 "ns %s",
        context->receiver_duty_cycle_stall_tracker.cycle_threshold_ns,
        aeron_driver_threading_mode_to_string(context->threading_mode));
    if (label_length > 0)
    {
        aeron_counters_manager_append_to_label(
            &conductor->counters_manager,
            AERON_SYSTEM_COUNTER_RECEIVER_CYCLE_TIME_THRESHOLD_EXCEEDED,
            (size_t)label_length,
            label);
    }

    context->conductor_duty_cycle_tracker->update(context->conductor_duty_cycle_tracker->state, now_ns);

    conductor->context = context;

    return 0;
}

int aeron_driver_conductor_find_client(aeron_driver_conductor_t *conductor, int64_t client_id)
{
    int index = -1;

    for (int i = (int)conductor->clients.length - 1; i >= 0; i--)
    {
        if (client_id == conductor->clients.array[i].client_id)
        {
            index = i;
            break;
        }
    }

    return index;
}

aeron_client_t *aeron_driver_conductor_get_or_add_client(aeron_driver_conductor_t *conductor, int64_t client_id)
{
    aeron_client_t *client = NULL;
    int index = aeron_driver_conductor_find_client(conductor, client_id);

    if (-1 == index)
    {
        int ensure_capacity_result = 0;
        AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, conductor->clients, aeron_client_t)

        if (ensure_capacity_result >= 0)
        {
            aeron_atomic_counter_t client_heartbeat;

            client_heartbeat.counter_id = aeron_counter_client_heartbeat_timestamp_allocate(
                &conductor->counters_manager, client_id);

            client_heartbeat.value_addr = aeron_counters_manager_addr(
                &conductor->counters_manager, client_heartbeat.counter_id);

            if (client_heartbeat.counter_id >= 0)
            {
                aeron_counters_manager_counter_registration_id(
                    &conductor->counters_manager, client_heartbeat.counter_id, client_id);

                aeron_counters_manager_counter_owner_id(
                    &conductor->counters_manager, client_heartbeat.counter_id, client_id);

                index = (int)conductor->clients.length;
                client = &conductor->clients.array[index];

                client->client_id = client_id;
                client->reached_end_of_life = false;
                client->closed_by_command = false;

                client->heartbeat_timestamp.counter_id = client_heartbeat.counter_id;
                client->heartbeat_timestamp.value_addr = client_heartbeat.value_addr;
                const int64_t now_ms = aeron_clock_cached_epoch_time(conductor->context->cached_clock);
                aeron_counter_set_release(client->heartbeat_timestamp.value_addr, now_ms);

                client->client_liveness_timeout_ms = conductor->context->client_liveness_timeout_ns < 1000000 ?
                    1 : (int64_t)(conductor->context->client_liveness_timeout_ns / 1000000);
                client->publication_links.array = NULL;
                client->publication_links.length = 0;
                client->publication_links.capacity = 0;
                client->counter_links.array = NULL;
                client->counter_links.length = 0;
                client->counter_links.capacity = 0;
                conductor->clients.length++;

                aeron_driver_conductor_on_counter_ready(conductor, client_id, client_heartbeat.counter_id);
            }
        }
    }
    else
    {
        client = &conductor->clients.array[index];
    }

    return client;
}

void aeron_client_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_client_t *client, int64_t now_ns, int64_t now_ms)
{
    int64_t timestamp_ms = aeron_counter_get_acquire(client->heartbeat_timestamp.value_addr);
    if (now_ms > (timestamp_ms + client->client_liveness_timeout_ms))
    {
        client->reached_end_of_life = true;

        if (!client->closed_by_command)
        {
            aeron_counter_increment_release(conductor->client_timeouts_counter);
            aeron_driver_conductor_on_client_timeout(conductor, client->client_id);
        }

        aeron_driver_conductor_on_unavailable_counter(
            conductor, client->client_id, client->heartbeat_timestamp.counter_id);
    }
}

bool aeron_client_has_reached_end_of_life(aeron_driver_conductor_t *conductor, aeron_client_t *client)
{
    return client->reached_end_of_life;
}

int32_t aeron_driver_conductor_next_session_id(aeron_driver_conductor_t *conductor)
{
    const int32_t low = conductor->publication_reserved_session_id_low;
    const int32_t high = conductor->publication_reserved_session_id_high;

    return low <= conductor->next_session_id && conductor->next_session_id <= high ?
        aeron_add_wrap_i32(high, 1) : conductor->next_session_id;
}

int32_t aeron_driver_conductor_update_next_session_id(aeron_driver_conductor_t *conductor, int32_t session_id)
{
    conductor->next_session_id = aeron_add_wrap_i32(session_id, 1);
    return session_id;
}

static void aeron_driver_conductor_track_session_id_offsets(
    aeron_driver_conductor_t *conductor, aeron_bit_set_t *session_id_offsets, int32_t publication_session_id)
{
    const int32_t next_session_id = aeron_driver_conductor_next_session_id(conductor);
    const int32_t session_id_offset = aeron_sub_wrap_i32(publication_session_id, next_session_id);

    if (0 <= session_id_offset && (size_t)session_id_offset < session_id_offsets->bit_set_length)
    {
        aeron_bit_set_set(session_id_offsets, (size_t)session_id_offset, true);
    }
}

static int aeron_driver_conductor_speculate_next_session_id(
    aeron_driver_conductor_t *conductor, aeron_bit_set_t *session_id_offsets, int32_t *session_id)
{
    size_t index = 0;

    if (aeron_bit_set_find_first(session_id_offsets, false, &index) < 0)
    {
        return -1;
    }

    *session_id = aeron_add_wrap_i32(aeron_driver_conductor_next_session_id(conductor), (int32_t)index);

    return 0;
}

int aeron_confirm_publication_match(
    const aeron_driver_uri_publication_params_t *params,
    const int32_t existing_session_id,
    const aeron_logbuffer_metadata_t *logbuffer_metadata,
    const int32_t existing_initial_term_id,
    const int32_t existing_term_id,
    const size_t existing_term_offset)
{
    if (params->has_session_id && params->session_id != existing_session_id)
    {
        AERON_SET_ERR(
            EINVAL,
            "existing publication has different '%s': existing=%" PRId32 " requested=%" PRId32,
            AERON_URI_SESSION_ID_KEY, existing_session_id, params->session_id);

        return -1;
    }

    if (params->has_mtu_length && params->mtu_length != (size_t)logbuffer_metadata->mtu_length)
    {
        AERON_SET_ERR(
            EINVAL,
            "existing publication has different '%s': existing=%" PRId32 " requested=%" PRIu64,
            AERON_URI_MTU_LENGTH_KEY, logbuffer_metadata->mtu_length, (uint64_t)params->mtu_length);

        return -1;
    }

    if (params->has_term_length && params->term_length != (size_t)logbuffer_metadata->term_length)
    {
        AERON_SET_ERR(
            EINVAL,
            "existing publication has different '%s': existing=%" PRId32 " requested=%" PRIu64,
            AERON_URI_TERM_LENGTH_KEY, logbuffer_metadata->term_length, (uint64_t)params->term_length);

        return -1;
    }

    if (params->has_position)
    {
        if (params->initial_term_id != existing_initial_term_id)
        {
            AERON_SET_ERR(
                EINVAL,
                "existing publication has different '%s': existing=%" PRId32 " requested=%" PRId32,
                AERON_URI_INITIAL_TERM_ID_KEY, existing_initial_term_id, params->initial_term_id);

            return -1;
        }

        if (params->term_id != existing_term_id)
        {
            AERON_SET_ERR(
                EINVAL,
                "existing publication has different '%s': existing=%" PRId32 " requested=%" PRId32,
                AERON_URI_TERM_ID_KEY, existing_term_id, params->term_id);

            return -1;
        }

        if (params->term_offset != existing_term_offset)
        {
            AERON_SET_ERR(
                EINVAL,
                "existing publication has different '%s': existing=%" PRId64 " requested=%" PRIu64,
                AERON_URI_TERM_OFFSET_KEY, (uint64_t)existing_term_offset, (uint64_t)params->term_offset);

            return -1;
        }
    }

    return 0;
}

void aeron_driver_conductor_unlink_from_endpoint(aeron_driver_conductor_t *conductor, aeron_subscription_link_t *link)
{
    conductor->context->log.remove_subscription_cleanup(
        link->registration_id, link->stream_id, link->channel_length, link->channel);

    aeron_receive_channel_endpoint_t *endpoint = link->endpoint;

    link->endpoint = NULL;
    if (link->has_session_id)
    {
        aeron_receive_channel_endpoint_decref_to_stream_and_session(endpoint, link->stream_id, link->session_id);
    }
    else if (link->is_response)
    {
        aeron_receive_channel_endpoint_decref_to_response_stream(endpoint, link->stream_id);
    }
    else
    {
        aeron_receive_channel_endpoint_decref_to_stream(endpoint, link->stream_id);
    }

    aeron_driver_conductor_unlink_all_subscribable(conductor, link);
}

void aeron_driver_conductor_log_explicit_error(
    aeron_driver_conductor_t *conductor,
    int error_code,
    const char *description)
{
    aeron_distinct_error_log_record(&conductor->error_log, error_code, description);
    aeron_counter_increment(conductor->errors_counter);
    aeron_err_clear();
}

void aeron_driver_conductor_log_error(aeron_driver_conductor_t *conductor)
{
    aeron_driver_conductor_log_explicit_error(conductor, aeron_errcode(), aeron_errmsg());
}

void aeron_client_delete(aeron_driver_conductor_t *conductor, aeron_client_t *client)
{
    for (size_t i = 0; i < client->publication_links.length; i++)
    {
        aeron_driver_managed_resource_t *resource = client->publication_links.array[i].resource;
        AERON_DRIVER_MANAGED_RESOURCE_DECREF(resource);
    }

    for (size_t i = 0; i < client->counter_links.length; i++)
    {
        aeron_counter_link_t *link = &client->counter_links.array[i];
        aeron_driver_conductor_on_unavailable_counter(conductor, link->registration_id, link->counter_id);
        aeron_counters_manager_free(&conductor->counters_manager, link->counter_id);
    }

    for (int last_index = (int)conductor->ipc_subscriptions.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_subscription_link_t *link = &conductor->ipc_subscriptions.array[i];

        if (client->client_id == link->client_id)
        {
            aeron_driver_conductor_unlink_all_subscribable(conductor, link);

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->ipc_subscriptions.array, sizeof(aeron_subscription_link_t), i, last_index);
            conductor->ipc_subscriptions.length--;
            last_index--;
        }
    }

    for (int last_index = (int)conductor->network_subscriptions.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_subscription_link_t *link = &conductor->network_subscriptions.array[i];

        if (client->client_id == link->client_id)
        {
            aeron_driver_conductor_unlink_from_endpoint(conductor, link);

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->network_subscriptions.array, sizeof(aeron_subscription_link_t), i, last_index);
            conductor->network_subscriptions.length--;
            last_index--;
        }
    }

    for (int last_index = (int)conductor->spy_subscriptions.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_subscription_link_t *link = &conductor->spy_subscriptions.array[i];

        if (client->client_id == link->client_id)
        {
            aeron_driver_conductor_unlink_all_subscribable(conductor, link);
            aeron_udp_channel_delete(link->spy_channel);
            link->spy_channel = NULL;

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->spy_subscriptions.array, sizeof(aeron_subscription_link_t), i, last_index);
            conductor->spy_subscriptions.length--;
            last_index--;
        }
    }

    aeron_counters_manager_free(&conductor->counters_manager, client->heartbeat_timestamp.counter_id);

    aeron_free(client->publication_links.array);
    client->publication_links.array = NULL;
    client->publication_links.length = 0;
    client->publication_links.capacity = 0;

    aeron_free(client->counter_links.array);
    client->counter_links.array = NULL;
    client->counter_links.length = 0;
    client->counter_links.capacity = 0;

    client->client_id = -1;
    client->heartbeat_timestamp.counter_id = -1;
    client->heartbeat_timestamp.value_addr = NULL;
}

bool aeron_client_free(void *resource)
{
    return true;
}

void on_available_image(
    aeron_driver_conductor_t *conductor,
    const int64_t correlation_id,
    const int32_t stream_id,
    const int32_t session_id,
    const char *log_file_name,
    const size_t log_file_name_length,
    const int32_t subscriber_position_id,
    const int64_t subscriber_registration_id,
    const char *source_identity,
    const size_t source_identity_length,
    const size_t response_length,
    char *response_buffer)
{
    aeron_image_buffers_ready_t *response = (aeron_image_buffers_ready_t *)response_buffer;

    response->correlation_id = correlation_id;
    response->stream_id = stream_id;
    response->session_id = session_id;
    response->subscriber_position_id = subscriber_position_id;
    response->subscriber_registration_id = subscriber_registration_id;
    response_buffer += sizeof(aeron_image_buffers_ready_t);

    int32_t length_field;

    length_field = (int32_t)log_file_name_length;
    memcpy(response_buffer, &length_field, sizeof(length_field));
    response_buffer += sizeof(int32_t);
    memcpy(response_buffer, log_file_name, log_file_name_length);
    response_buffer += AERON_ALIGN(log_file_name_length, sizeof(int32_t));

    length_field = (int32_t)source_identity_length;
    memcpy(response_buffer, &length_field, sizeof(length_field));
    response_buffer += sizeof(int32_t);
    memcpy(response_buffer, source_identity, source_identity_length);

    aeron_driver_conductor_client_transmit(conductor, AERON_RESPONSE_ON_AVAILABLE_IMAGE, response, response_length);
}

void aeron_driver_conductor_on_available_image(
    aeron_driver_conductor_t *conductor,
    int64_t correlation_id,
    int32_t stream_id,
    int32_t session_id,
    const char *log_file_name,
    size_t log_file_name_length,
    int32_t subscriber_position_id,
    int64_t subscriber_registration_id,
    const char *source_identity,
    size_t source_identity_length)
{
    const size_t response_length =
        sizeof(aeron_image_buffers_ready_t) +
        AERON_ALIGN(log_file_name_length, sizeof(int32_t)) +
        source_identity_length +
        (2 * sizeof(int32_t));

    if (response_length > sizeof(aeron_image_buffers_ready_t) + (2 * AERON_MAX_PATH))
    {
        char *buffer = NULL;
        if (aeron_alloc((void **)&buffer, response_length) < 0)
        {
            AERON_APPEND_ERR("%s", "failed to allocate response buffer");
            aeron_driver_conductor_log_error(conductor);
            return;
        }

        on_available_image(
            conductor,
            correlation_id,
            stream_id,
            session_id,
            log_file_name,
            log_file_name_length,
            subscriber_position_id,
            subscriber_registration_id,
            source_identity,
            source_identity_length,
            response_length,
            buffer);
        aeron_free(buffer);
    }
    else
    {
        char buffer[sizeof(aeron_image_buffers_ready_t) + (2 * AERON_MAX_PATH)];
        on_available_image(
            conductor,
            correlation_id,
            stream_id,
            session_id,
            log_file_name,
            log_file_name_length,
            subscriber_position_id,
            subscriber_registration_id,
            source_identity,
            source_identity_length,
            response_length,
            buffer);
    }
}

void aeron_ipc_publication_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_ipc_publication_entry_t *entry, int64_t now_ns, int64_t now_ms)
{
    aeron_ipc_publication_on_time_event(conductor, entry->publication, now_ns, now_ms);
}

bool aeron_ipc_publication_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_ipc_publication_entry_t *entry)
{
    return aeron_ipc_publication_has_reached_end_of_life(entry->publication);
}

void aeron_ipc_publication_entry_delete(aeron_driver_conductor_t *conductor, aeron_ipc_publication_entry_t *entry)
{
    aeron_ipc_publication_t *publication = entry->publication;
    conductor->context->log.remove_publication_cleanup(
        publication->session_id, publication->stream_id, publication->channel_length, publication->channel);

    for (size_t i = 0, size = conductor->ipc_subscriptions.length; i < size; i++)
    {
        aeron_subscription_link_t *link = &conductor->ipc_subscriptions.array[i];
        aeron_driver_conductor_unlink_subscribable(link, &publication->conductor_fields.subscribable);
    }

    aeron_ipc_publication_close(&conductor->counters_manager, publication);
    entry->publication = NULL;
}

static bool aeron_ipc_publication_free_voidp(void *publication)
{
    return aeron_ipc_publication_free((aeron_ipc_publication_t *)publication);
}

void aeron_ipc_publication_entry_free(aeron_driver_conductor_t *conductor, aeron_ipc_publication_entry_t *entry)
{
    aeron_driver_conductor_add_end_of_life_resource(conductor, entry->publication, aeron_ipc_publication_free_voidp);
}

void aeron_network_publication_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_network_publication_entry_t *entry, int64_t now_ns, int64_t now_ms)
{
    aeron_network_publication_on_time_event(conductor, entry->publication, now_ns, now_ms);
}

bool aeron_network_publication_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_network_publication_entry_t *entry)
{
    return aeron_network_publication_has_sender_released(entry->publication);
}

void aeron_network_publication_entry_delete(
    aeron_driver_conductor_t *conductor, aeron_network_publication_entry_t *entry)
{
    aeron_send_channel_endpoint_t *endpoint = entry->publication->endpoint;

    for (size_t i = 0, size = conductor->spy_subscriptions.length; i < size; i++)
    {
        aeron_subscription_link_t *link = &conductor->spy_subscriptions.array[i];
        aeron_driver_conductor_unlink_subscribable(link, &entry->publication->conductor_fields.subscribable);
    }

    aeron_network_publication_close(&conductor->counters_manager, entry->publication);
    entry->publication = NULL;

    AERON_DRIVER_MANAGED_RESOURCE_DECREF(&(endpoint->conductor_fields.managed_resource));
}

static bool aeron_network_publication_free_voidp(void *publication)
{
    return aeron_network_publication_free((aeron_network_publication_t *)publication);
}

void aeron_network_publication_entry_free(aeron_driver_conductor_t *conductor, aeron_network_publication_entry_t *entry)
{
    aeron_driver_conductor_add_end_of_life_resource(
        conductor, entry->publication, aeron_network_publication_free_voidp);
}

void aeron_driver_conductor_cleanup_spies(aeron_driver_conductor_t *conductor, aeron_network_publication_t *publication)
{
    for (size_t i = 0, size = conductor->spy_subscriptions.length; i < size; i++)
    {
        aeron_subscription_link_t *link = &conductor->spy_subscriptions.array[i];

        if (aeron_driver_conductor_is_subscribable_linked(link, &publication->conductor_fields.subscribable))
        {
            aeron_driver_conductor_on_unavailable_image(
                conductor,
                publication->conductor_fields.managed_resource.registration_id,
                link->registration_id,
                link->stream_id,
                link->channel,
                link->channel_length);
        }

        aeron_driver_conductor_unlink_subscribable(link, &publication->conductor_fields.subscribable);
    }
}

void aeron_driver_conductor_cleanup_network_publication(
    aeron_driver_conductor_t *conductor, aeron_network_publication_t *publication)
{
    const aeron_udp_channel_t *udp_channel = publication->endpoint->conductor_fields.udp_channel;
    conductor->context->log.remove_publication_cleanup(
        publication->session_id, publication->stream_id, udp_channel->uri_length, udp_channel->original_uri);

    aeron_driver_sender_proxy_on_remove_publication(conductor->context->sender_proxy, publication);
}

void aeron_send_channel_endpoint_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_send_channel_endpoint_entry_t *entry, int64_t now_ns, int64_t now_ms)
{
    /* Nothing done here. Could linger if needed. */
}

bool aeron_send_channel_endpoint_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_send_channel_endpoint_entry_t *entry)
{
    return aeron_send_channel_endpoint_has_sender_released(entry->endpoint);
}

void aeron_send_channel_endpoint_entry_delete(
    aeron_driver_conductor_t *conductor, aeron_send_channel_endpoint_entry_t *entry)
{
    aeron_str_to_ptr_hash_map_remove(
        &conductor->send_channel_endpoint_by_channel_map,
        entry->endpoint->conductor_fields.udp_channel->canonical_form,
        entry->endpoint->conductor_fields.udp_channel->canonical_length);

    aeron_send_channel_endpoint_delete(&conductor->counters_manager, entry->endpoint);
}

void aeron_receive_channel_endpoint_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_receive_channel_endpoint_entry_t *entry, int64_t now_ns, int64_t now_ms)
{
    /* Nothing done here. could linger if needed. */
}

bool aeron_receive_channel_endpoint_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_receive_channel_endpoint_entry_t *entry)
{
    return aeron_receive_channel_endpoint_has_receiver_released(entry->endpoint);
}

// Called from conductor.
void aeron_receive_channel_endpoint_entry_delete(
    aeron_driver_conductor_t *conductor, aeron_receive_channel_endpoint_entry_t *entry)
{
    for (size_t i = 0, size = conductor->publication_images.length; i < size; i++)
    {
        aeron_publication_image_t *image = conductor->publication_images.array[i].image;

        if (entry->endpoint == image->conductor_fields.endpoint)
        {
            const aeron_udp_channel_t *udp_channel = entry->endpoint->conductor_fields.udp_channel;
            conductor->context->log.remove_image_cleanup(
                image->conductor_fields.managed_resource.registration_id,
                image->session_id,
                image->stream_id,
                udp_channel->uri_length,
                udp_channel->original_uri);
            aeron_publication_image_conductor_disconnect_endpoint(image);
        }
    }

    aeron_receive_channel_endpoint_delete(&conductor->counters_manager, entry->endpoint);
}

void aeron_publication_image_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_publication_image_entry_t *entry, int64_t now_ns, int64_t now_ms)
{
    aeron_publication_image_on_time_event(conductor, entry->image, now_ns, now_ms);
}

bool aeron_publication_image_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_publication_image_entry_t *entry)
{
    bool has_receiver_released = false;
    AERON_GET_ACQUIRE(has_receiver_released, entry->image->has_receiver_released);

    return AERON_PUBLICATION_IMAGE_STATE_DONE == entry->image->conductor_fields.state && has_receiver_released;
}

void aeron_publication_image_entry_delete(
    aeron_driver_conductor_t *conductor, aeron_publication_image_entry_t *entry)
{
    aeron_publication_image_t *image = entry->image;

    for (size_t i = 0, size = conductor->network_subscriptions.length; i < size; i++)
    {
        aeron_subscription_link_t *link = &conductor->network_subscriptions.array[i];
        aeron_driver_conductor_unlink_subscribable(link, &image->conductor_fields.subscribable);
    }

    aeron_publication_image_close(&conductor->counters_manager, image);
    entry->image = NULL;
}

static bool aeron_publication_image_free_voidp(void *image)
{
    return aeron_publication_image_free((aeron_publication_image_t *)image);
}

void aeron_publication_image_entry_free(aeron_driver_conductor_t *conductor, aeron_publication_image_entry_t *entry)
{
    aeron_driver_conductor_add_end_of_life_resource(conductor, entry->image, aeron_publication_image_free_voidp);
}

void aeron_linger_resource_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_linger_resource_entry_t *entry, int64_t now_ns, int64_t now_ms)
{
    if (now_ns > entry->timeout_ns)
    {
        entry->has_reached_end_of_life = true;
    }
}

bool aeron_linger_resource_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_linger_resource_entry_t *entry)
{
    return entry->has_reached_end_of_life;
}

void aeron_linger_resource_entry_delete(aeron_driver_conductor_t *conductor, aeron_linger_resource_entry_t *entry)
{
    aeron_free(entry->buffer);
}

// Called from conductor
void aeron_driver_conductor_image_transition_to_linger(
    aeron_driver_conductor_t *conductor, aeron_publication_image_t *image)
{
    if (NULL != image->conductor_fields.endpoint)
    {
        bool rejoin = true;

        for (size_t i = 0, size = conductor->network_subscriptions.length; i < size; i++)
        {
            aeron_subscription_link_t *link = &conductor->network_subscriptions.array[i];

            if (aeron_driver_conductor_is_subscribable_linked(link, &image->conductor_fields.subscribable))
            {
                rejoin = link->is_rejoin;

                aeron_driver_conductor_on_unavailable_image(
                    conductor,
                    image->conductor_fields.managed_resource.registration_id,
                    link->registration_id,
                    image->stream_id,
                    link->channel,
                    link->channel_length);
            }
        }

        if (rejoin)
        {
            aeron_driver_receiver_proxy_on_remove_cool_down(
                conductor->context->receiver_proxy,
                image->conductor_fields.endpoint,
                image->session_id,
                image->stream_id);
        }
    }
}

void aeron_driver_conductor_add_end_of_life_resource(
    aeron_driver_conductor_t *conductor,
    void *resource,
    aeron_end_of_life_resource_free_t free_func)
{
    aeron_end_of_life_resource_t end_of_life_resource = { 0 };
    int64_t *counter = aeron_system_counter_addr(
        &conductor->system_counters, AERON_SYSTEM_COUNTER_FREE_FAILS);

    end_of_life_resource.resource = resource;
    end_of_life_resource.free_func = free_func;
    if (aeron_deque_add_last(&conductor->end_of_life_queue, (void *)&end_of_life_resource) < 0)
    {
        aeron_counter_increment_release(counter);
    }
}

int aeron_driver_conductor_free_end_of_life_resources(aeron_driver_conductor_t *conductor)
{
    const int limit = (int)conductor->context->resource_free_limit;
    aeron_end_of_life_resource_t end_of_life_resource = { 0 };
    int count = 0;

    for (; count < limit; count++)
    {
        if (0 == aeron_deque_remove_first(&conductor->end_of_life_queue, (void *)&end_of_life_resource))
        {
            break;
        }

        if (!end_of_life_resource.free_func(end_of_life_resource.resource))
        {
            int64_t *counter = aeron_system_counter_addr(&conductor->system_counters, AERON_SYSTEM_COUNTER_FREE_FAILS);
            aeron_counter_increment_release(counter);
            aeron_deque_add_last(&conductor->end_of_life_queue, (void *)&end_of_life_resource);
        }
    }

    return count;
}

#define AERON_DRIVER_CONDUCTOR_CHECK_MANAGED_RESOURCE(c, l, t, now_ns, now_ms)             \
for (int last_index = (int)l.length - 1, i = last_index; i >= 0; i--)                      \
{                                                                                          \
    t *elem = &l.array[i];                                                                 \
    l.on_time_event(c, elem, now_ns, now_ms);                                              \
    if (l.has_reached_end_of_life(c, elem))                                                \
    {                                                                                      \
        if (NULL != l.free_func)                                                           \
        {                                                                                  \
            l.free_func(c, elem);                                                          \
        }                                                                                  \
        l.delete_func(c, elem);                                                            \
        aeron_array_fast_unordered_remove((uint8_t *)l.array, sizeof(t), i, last_index);   \
        last_index--;                                                                      \
        l.length--;                                                                        \
    }                                                                                      \
}

void aeron_driver_conductor_on_check_managed_resources(
    aeron_driver_conductor_t *conductor, int64_t now_ns, int64_t now_ms)
{
    AERON_DRIVER_CONDUCTOR_CHECK_MANAGED_RESOURCE(
        conductor, conductor->clients, aeron_client_t, now_ns, now_ms)
    AERON_DRIVER_CONDUCTOR_CHECK_MANAGED_RESOURCE(
        conductor, conductor->ipc_publications, aeron_ipc_publication_entry_t, now_ns, now_ms)
    AERON_DRIVER_CONDUCTOR_CHECK_MANAGED_RESOURCE(
        conductor, conductor->network_publications, aeron_network_publication_entry_t, now_ns, now_ms)
    AERON_DRIVER_CONDUCTOR_CHECK_MANAGED_RESOURCE(
        conductor, conductor->send_channel_endpoints, aeron_send_channel_endpoint_entry_t, now_ns, now_ms)
    AERON_DRIVER_CONDUCTOR_CHECK_MANAGED_RESOURCE(
        conductor, conductor->receive_channel_endpoints, aeron_receive_channel_endpoint_entry_t, now_ns, now_ms)
    AERON_DRIVER_CONDUCTOR_CHECK_MANAGED_RESOURCE(
        conductor, conductor->publication_images, aeron_publication_image_entry_t, now_ns, now_ms)
    AERON_DRIVER_CONDUCTOR_CHECK_MANAGED_RESOURCE(
        conductor, conductor->lingering_resources, aeron_linger_resource_entry_t, now_ns, now_ms)
}

aeron_ipc_publication_t *aeron_driver_conductor_get_or_add_ipc_publication(
    aeron_driver_conductor_t *conductor,
    aeron_client_t *client,
    aeron_driver_uri_publication_params_t *params,
    int64_t registration_id,
    int32_t stream_id,
    size_t uri_length,
    const char *uri,
    bool is_exclusive)
{
    aeron_ipc_publication_t *publication = NULL;

    uint64_t bits[STATIC_BIT_SET_U64_LEN];
    aeron_bit_set_t session_id_offsets;
    aeron_bit_set_stack_init(
        conductor->ipc_publications.length + 1, bits, STATIC_BIT_SET_U64_LEN, false, &session_id_offsets);
    assert(conductor->ipc_publications.length < session_id_offsets.bit_set_length);

    bool is_session_id_in_use = false;

    for (size_t i = 0; i < conductor->ipc_publications.length; i++)
    {
        aeron_ipc_publication_t *pub_entry = conductor->ipc_publications.array[i].publication;

        if (stream_id == pub_entry->stream_id)
        {
            if (AERON_IPC_PUBLICATION_STATE_ACTIVE == pub_entry->conductor_fields.state &&
                NULL == publication && !is_exclusive && !pub_entry->is_exclusive)
            {
                publication = pub_entry;
            }

            if (AERON_IPC_PUBLICATION_STATE_ACTIVE == pub_entry->conductor_fields.state ||
                AERON_IPC_PUBLICATION_STATE_DRAINING == pub_entry->conductor_fields.state)
            {
                if (params->has_session_id && pub_entry->session_id == params->session_id)
                {
                    is_session_id_in_use = true;
                }

                aeron_driver_conductor_track_session_id_offsets(conductor, &session_id_offsets, pub_entry->session_id);
            }
        }
    }

    int32_t speculated_session_id = 0;
    int session_id_found = aeron_driver_conductor_speculate_next_session_id(
        conductor, &session_id_offsets, &speculated_session_id);
    aeron_bit_set_stack_free(&session_id_offsets);

    if (session_id_found < 0)
    {
        AERON_SET_ERR(EINVAL, "%s", "(BUG) Unable to allocate session-id");
        return NULL;
    }

    if (is_session_id_in_use && (is_exclusive || NULL == publication))
    {
        AERON_SET_ERR(
            EINVAL,
            "Specified session-id is already in exclusive use for channel=%.*s stream-id=%" PRId32,
            (int)uri_length, uri, stream_id);

        return NULL;
    }

    if (!is_exclusive && NULL != publication)
    {
        if (aeron_confirm_publication_match(
            params,
            publication->session_id,
            publication->log_meta_data,
            publication->initial_term_id,
            publication->starting_term_id,
            publication->starting_term_offset) < 0)
        {
            return NULL;
        }
    }

    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, client->publication_links, aeron_publication_link_t)

    if (ensure_capacity_result >= 0)
    {
        if (NULL == publication)
        {
            AERON_ARRAY_ENSURE_CAPACITY(
                ensure_capacity_result, conductor->ipc_publications, aeron_ipc_publication_entry_t)

            if (ensure_capacity_result >= 0)
            {
                if (!params->has_session_id)
                {
                    aeron_driver_conductor_update_next_session_id(conductor, speculated_session_id);
                }

                int32_t session_id = params->has_session_id ? params->session_id : speculated_session_id;
                int32_t initial_term_id = params->has_position ? params->initial_term_id : aeron_randomised_int32();

                aeron_position_t pub_pos_position;
                aeron_position_t pub_lmt_position;

                pub_pos_position.counter_id = aeron_counter_publisher_position_allocate(
                    &conductor->counters_manager, registration_id, session_id, stream_id, uri_length, uri);
                pub_pos_position.value_addr = aeron_counters_manager_addr(
                    &conductor->counters_manager, pub_pos_position.counter_id);
                pub_lmt_position.counter_id = aeron_counter_publisher_limit_allocate(
                    &conductor->counters_manager, registration_id, session_id, stream_id, uri_length, uri);
                pub_lmt_position.value_addr = aeron_counters_manager_addr(
                    &conductor->counters_manager, pub_lmt_position.counter_id);

                if (pub_pos_position.counter_id < 0 || pub_lmt_position.counter_id < 0)
                {
                    return NULL;
                }

                aeron_counters_manager_counter_owner_id(
                    &conductor->counters_manager, pub_lmt_position.counter_id, client->client_id);

                if (params->has_position)
                {
                    int64_t position = aeron_logbuffer_compute_position(
                        params->term_id,
                        (int32_t)params->term_offset,
                        (size_t)aeron_number_of_trailing_zeroes((int32_t)params->term_length),
                        initial_term_id);

                    aeron_counter_set_release(pub_pos_position.value_addr, position);
                    aeron_counter_set_release(pub_lmt_position.value_addr, position);
                }

                if (aeron_ipc_publication_create(
                    &publication,
                    conductor->context,
                    session_id,
                    stream_id,
                    registration_id,
                    &pub_pos_position,
                    &pub_lmt_position,
                    initial_term_id,
                    params,
                    is_exclusive,
                    &conductor->system_counters,
                    uri_length,
                    uri) >= 0)
                {
                    aeron_publication_link_t *link = &client->publication_links.array[client->publication_links.length];

                    AERON_PUBLICATION_LINK_INIT(link, &publication->conductor_fields.managed_resource, registration_id);
                    client->publication_links.length++;

                    conductor->ipc_publications.array[conductor->ipc_publications.length++].publication = publication;
                    publication->conductor_fields.managed_resource.time_of_last_state_change_ns =
                        aeron_clock_cached_nano_time(conductor->context->cached_clock);
                }
            }
        }
        else
        {
            aeron_publication_link_t *link = &client->publication_links.array[client->publication_links.length];

            AERON_PUBLICATION_LINK_INIT(link, &publication->conductor_fields.managed_resource, registration_id);
            client->publication_links.length++;

            AERON_DRIVER_MANAGED_RESOURCE_INCREF(&(publication->conductor_fields.managed_resource));
        }
    }

    return ensure_capacity_result >= 0 ? publication : NULL;
}

static int aeron_driver_conductor_find_response_publication_image(
    aeron_driver_conductor_t *conductor,
    const aeron_udp_channel_t *udp_channel,
    aeron_driver_uri_publication_params_t *params,
    aeron_publication_image_t **image)
{
    *image = NULL;
    if (AERON_UDP_CHANNEL_CONTROL_MODE_RESPONSE != udp_channel->control_mode)
    {
        return 0;
    }

    if (AERON_NULL_VALUE == params->response_correlation_id)
    {
        AERON_SET_ERR(EINVAL, "%s", "control-mode=response was specified, but no response-correlation-id set");
        return -1;
    }

    for (size_t i = 0; i < conductor->publication_images.length; i++)
    {
        aeron_publication_image_t *image_entry = conductor->publication_images.array[i].image;
        if (aeron_publication_image_registration_id(image_entry) == params->response_correlation_id)
        {
            if (aeron_publication_image_has_send_response_setup(image_entry))
            {
                *image = image_entry;
                return 0;
            }
            else
            {
                AERON_SET_ERR(
                    EINVAL,
                    "image.correlationId=%" PRId64 " did not request a response channel",
                    params->response_correlation_id);
                return -1;
            }
        }
    }

    AERON_SET_ERR(EINVAL, "image.correlationId=%" PRId64 " not found", params->response_correlation_id);
    return -1;
}

static aeron_publication_image_t *aeron_driver_conductor_find_publication_image(
    aeron_driver_conductor_t *conductor,
    int64_t correlation_id)
{
    for (size_t i = 0; i < conductor->publication_images.length; i++)
    {
        aeron_publication_image_t *image_entry = conductor->publication_images.array[i].image;
        if (aeron_publication_image_registration_id(image_entry) == correlation_id)
        {
            return image_entry;
        }
    }

    return NULL;
}

aeron_network_publication_t *aeron_driver_conductor_get_or_add_network_publication(
    aeron_driver_conductor_t *conductor,
    aeron_client_t *client,
    aeron_send_channel_endpoint_t *endpoint,
    size_t uri_length,
    const char *uri,
    aeron_driver_uri_publication_params_t *params,
    aeron_publication_image_t *response_publication_image,
    int64_t registration_id,
    int32_t stream_id,
    bool is_exclusive)
{
    aeron_network_publication_t *publication = NULL;
    const aeron_udp_channel_t *udp_channel = endpoint->conductor_fields.udp_channel;

    uint64_t bits[STATIC_BIT_SET_U64_LEN];
    aeron_bit_set_t session_id_offsets;
    aeron_bit_set_stack_init(
        conductor->network_publications.length + 1, bits, STATIC_BIT_SET_U64_LEN, false, &session_id_offsets);

    bool is_session_id_in_use = false;

    // TODO: Extract
    for (size_t i = 0; i < conductor->network_publications.length; i++)
    {
        aeron_network_publication_t *pub_entry = conductor->network_publications.array[i].publication;

        if (endpoint == pub_entry->endpoint && stream_id == pub_entry->stream_id)
        {
            if (AERON_NETWORK_PUBLICATION_STATE_ACTIVE == pub_entry->conductor_fields.state &&
                !is_exclusive &&
                !pub_entry->is_exclusive &&
                pub_entry->response_correlation_id == params->response_correlation_id)
            {
                publication = pub_entry;
            }

            if (params->has_session_id && pub_entry->session_id == params->session_id)
            {
                is_session_id_in_use = true;
            }

            aeron_driver_conductor_track_session_id_offsets(conductor, &session_id_offsets, pub_entry->session_id);
        }
    }

    int32_t speculated_session_id = 0;
    int session_id_found = aeron_driver_conductor_speculate_next_session_id(
        conductor, &session_id_offsets, &speculated_session_id);
    aeron_bit_set_stack_free(&session_id_offsets);

    if (session_id_found < 0)
    {
        AERON_SET_ERR(EINVAL, "%s", "(BUG) Unable to allocate session-id");
        return NULL;
    }

    if (is_session_id_in_use && (is_exclusive || NULL == publication))
    {
        AERON_SET_ERR(
            EINVAL,
            "Specified session-id is already in exclusive use for channel=%.*s stream-id=%" PRId32,
            (int)uri_length, uri, stream_id);

        return NULL;
    }

    if (!is_exclusive && NULL != publication)
    {
        if (publication->spies_simulate_connection != params->spies_simulate_connection)
        {
            AERON_SET_ERR(
                EINVAL,
                "existing publication has different spies simulate connection: requested=%s",
                params->spies_simulate_connection ? "true" : "false");

            return NULL;
        }

        if (0 != aeron_confirm_publication_match(
            params,
            publication->session_id,
            publication->log_meta_data,
            publication->initial_term_id,
            publication->starting_term_id,
            publication->starting_term_offset))
        {
            return NULL;
        }
    }

    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, client->publication_links, aeron_publication_link_t)

    if (ensure_capacity_result >= 0)
    {
        if (NULL == publication)
        {
            AERON_ARRAY_ENSURE_CAPACITY(
                ensure_capacity_result, conductor->network_publications, aeron_network_publication_entry_t)

            if (ensure_capacity_result >= 0)
            {
                if (!params->has_session_id)
                {
                    aeron_driver_conductor_update_next_session_id(conductor, speculated_session_id);
                }

                int32_t session_id = params->has_session_id ? params->session_id : speculated_session_id;
                int32_t initial_term_id = params->has_position ? params->initial_term_id : aeron_randomised_int32();

                aeron_flow_control_strategy_t *flow_control_strategy;
                if (aeron_default_multicast_flow_control_strategy_supplier(
                    &flow_control_strategy,
                    conductor->context,
                    &conductor->counters_manager,
                    udp_channel,
                    stream_id,
                    session_id,
                    registration_id,
                    initial_term_id,
                    params->term_length) < 0)
                {
                    return NULL;
                }

                aeron_position_t pub_pos_position;
                aeron_position_t pub_lmt_position;
                aeron_position_t snd_pos_position;
                aeron_position_t snd_lmt_position;
                aeron_atomic_counter_t snd_bpe_counter;
                aeron_atomic_counter_t snd_naks_received_counter;

                pub_pos_position.counter_id = aeron_counter_publisher_position_allocate(
                    &conductor->counters_manager, registration_id, session_id, stream_id, uri_length, uri);
                pub_lmt_position.counter_id = aeron_counter_publisher_limit_allocate(
                    &conductor->counters_manager, registration_id, session_id, stream_id, uri_length, uri);
                snd_pos_position.counter_id = aeron_counter_sender_position_allocate(
                    &conductor->counters_manager, registration_id, session_id, stream_id, uri_length, uri);
                snd_lmt_position.counter_id = aeron_counter_sender_limit_allocate(
                    &conductor->counters_manager, registration_id, session_id, stream_id, uri_length, uri);
                snd_bpe_counter.counter_id = aeron_counter_sender_bpe_allocate(
                    &conductor->counters_manager, registration_id, session_id, stream_id, uri_length, uri);
                snd_naks_received_counter.counter_id = aeron_counter_sender_naks_received_allocate(
                    &conductor->counters_manager, registration_id, session_id, stream_id, uri_length, uri);

                if (pub_pos_position.counter_id < 0 || pub_lmt_position.counter_id < 0 ||
                    snd_pos_position.counter_id < 0 || snd_lmt_position.counter_id < 0 ||
                    snd_bpe_counter.counter_id < 0 || snd_naks_received_counter.counter_id < 0)
                {
                    return NULL;
                }

                aeron_counters_manager_counter_owner_id(
                    &conductor->counters_manager, pub_lmt_position.counter_id, client->client_id);

                pub_pos_position.value_addr = aeron_counters_manager_addr(
                    &conductor->counters_manager, pub_pos_position.counter_id);
                pub_lmt_position.value_addr = aeron_counters_manager_addr(
                    &conductor->counters_manager, pub_lmt_position.counter_id);
                snd_pos_position.value_addr = aeron_counters_manager_addr(
                    &conductor->counters_manager, snd_pos_position.counter_id);
                snd_lmt_position.value_addr = aeron_counters_manager_addr(
                    &conductor->counters_manager, snd_lmt_position.counter_id);
                snd_bpe_counter.value_addr = aeron_counters_manager_addr(
                    &conductor->counters_manager, snd_bpe_counter.counter_id);
                snd_naks_received_counter.value_addr = aeron_counters_manager_addr(
                    &conductor->counters_manager, snd_naks_received_counter.counter_id);

                if (params->has_position)
                {
                    int64_t position = aeron_logbuffer_compute_position(
                        params->term_id,
                        (int32_t)params->term_offset,
                        (size_t)aeron_number_of_trailing_zeroes((int32_t)params->term_length),
                        initial_term_id);

                    aeron_counter_set_release(pub_pos_position.value_addr, position);
                    aeron_counter_set_release(pub_lmt_position.value_addr, position);
                    aeron_counter_set_release(snd_pos_position.value_addr, position);
                    aeron_counter_set_release(snd_lmt_position.value_addr, position);
                }

                if (pub_lmt_position.counter_id >= 0 &&
                    aeron_network_publication_create(
                        &publication,
                        endpoint,
                        conductor->context,
                        registration_id,
                        session_id,
                        stream_id,
                        initial_term_id,
                        &pub_pos_position,
                        &pub_lmt_position,
                        &snd_pos_position,
                        &snd_lmt_position,
                        &snd_bpe_counter,
                        &snd_naks_received_counter,
                        flow_control_strategy,
                        params,
                        is_exclusive,
                        &conductor->system_counters) >= 0)
                {
                    AERON_DRIVER_MANAGED_RESOURCE_INCREF(&(endpoint->conductor_fields.managed_resource));
                    aeron_driver_sender_proxy_on_add_publication(conductor->context->sender_proxy, publication);

                    aeron_publication_link_t *link = &client->publication_links.array[client->publication_links.length];

                    AERON_PUBLICATION_LINK_INIT(link, &publication->conductor_fields.managed_resource, registration_id);
                    client->publication_links.length++;

                    conductor->network_publications.array[conductor->network_publications.length++].publication =
                        publication;
                    publication->conductor_fields.managed_resource.time_of_last_state_change_ns =
                        aeron_clock_cached_nano_time(conductor->context->cached_clock);
                }
            }
        }
        else
        {
            aeron_publication_link_t *link = &client->publication_links.array[client->publication_links.length];

            AERON_PUBLICATION_LINK_INIT(link, &publication->conductor_fields.managed_resource, registration_id);
            client->publication_links.length++;

            AERON_DRIVER_MANAGED_RESOURCE_INCREF(&(publication->conductor_fields.managed_resource));
        }

        if (NULL != response_publication_image)
        {
            aeron_publication_image_set_response_session_id(
                response_publication_image, (int64_t)publication->session_id);
        }
    }

    return ensure_capacity_result >= 0 ? publication : NULL;
}

/* This should be re-usable if/when we decide to reuse transports with MDS */
int aeron_driver_conductor_update_and_check_ats_status(
    aeron_driver_context_t *context, aeron_udp_channel_t *channel, const aeron_udp_channel_t *existing_channel)
{
    if (!context->ats_enabled && AERON_URI_ATS_STATUS_ENABLED == channel->ats_status)
    {
        AERON_SET_ERR(
            EINVAL,
            "ATS is not enabled and thus ats=true not allowed: channel=%.*s",
            (int)channel->uri_length,
            channel->original_uri);
        return -1;
    }

    aeron_uri_ats_status_t context_status = context->ats_enabled ?
        AERON_URI_ATS_STATUS_ENABLED : AERON_URI_ATS_STATUS_DISABLED;
    channel->ats_status = AERON_URI_ATS_STATUS_DEFAULT == channel->ats_status ? context_status : channel->ats_status;

    if (NULL != existing_channel)
    {
        if (existing_channel->ats_status != channel->ats_status)
        {
            AERON_SET_ERR(
                EINVAL,
                "ATS mismatch: existingChannel=%.*s channel=%.*s",
                (int)existing_channel->uri_length,
                existing_channel->original_uri,
                (int)channel->uri_length,
                channel->original_uri);
            return -1;
        }
    }

    return 0;
}

aeron_send_channel_endpoint_t *aeron_driver_conductor_get_or_add_send_channel_endpoint(
    aeron_driver_conductor_t *conductor,
    aeron_udp_channel_t *channel,
    aeron_driver_uri_publication_params_t *params,
    int64_t registration_id)
{
    aeron_send_channel_endpoint_t *endpoint;
    if (aeron_driver_conductor_find_existing_send_channel_endpoint(conductor, channel, &endpoint) < 0)
    {
        goto error_cleanup;
    }

    if (aeron_driver_conductor_update_and_check_ats_status(
        conductor->context, channel, NULL == endpoint ? NULL : endpoint->conductor_fields.udp_channel) < 0)
    {
        goto error_cleanup;
    }

    if (NULL == endpoint)
    {
        if (aeron_publication_params_validate_mtu_for_sndbuf(
            params,
            0,
            channel->socket_sndbuf_length,
            conductor->context->socket_sndbuf,
            conductor->context->os_buffer_lengths.default_so_sndbuf) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error_cleanup;
        }

        int ensure_capacity_result = 0;

        AERON_ARRAY_ENSURE_CAPACITY(
            ensure_capacity_result, conductor->send_channel_endpoints, aeron_send_channel_endpoint_entry_t)

        if (ensure_capacity_result < 0)
        {
            goto error_cleanup;
        }

        if (aeron_send_channel_endpoint_create(
            &endpoint, channel, params, conductor->context, &conductor->counters_manager, registration_id) < 0)
        {
            // the `channel` is now owned by the endpoint
            return NULL;
        }

        if (aeron_str_to_ptr_hash_map_put(
            &conductor->send_channel_endpoint_by_channel_map,
            channel->canonical_form,
            channel->canonical_length,
            endpoint) < 0)
        {
            aeron_send_channel_endpoint_delete(&conductor->counters_manager, endpoint);
            return NULL;
        }

        aeron_driver_sender_proxy_on_add_endpoint(conductor->context->sender_proxy, endpoint);
        conductor->send_channel_endpoints.array[conductor->send_channel_endpoints.length++].endpoint = endpoint;

        aeron_counter_set_release(endpoint->channel_status.value_addr, AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ACTIVE);
    }
    else
    {
        if (aeron_driver_conductor_send_endpoint_has_clashing_timestamp_offsets(conductor, endpoint, channel))
        {
            AERON_APPEND_ERR("%s", "");
            goto error_cleanup;
        }

        if (aeron_publication_params_validate_mtu_for_sndbuf(
            params,
            endpoint->conductor_fields.socket_sndbuf,
            channel->socket_sndbuf_length,
            conductor->context->socket_sndbuf,
            conductor->context->os_buffer_lengths.default_so_sndbuf) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error_cleanup;
        }

        if (aeron_driver_conductor_validate_channel_buffer_length(
            AERON_URI_SOCKET_RCVBUF_KEY,
            channel->socket_rcvbuf_length,
            endpoint->conductor_fields.socket_rcvbuf,
            channel,
            endpoint->conductor_fields.udp_channel) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error_cleanup;
        }

        if (aeron_driver_conductor_validate_channel_buffer_length(
            AERON_URI_SOCKET_SNDBUF_KEY,
            channel->socket_sndbuf_length,
            endpoint->conductor_fields.socket_sndbuf,
            channel,
            endpoint->conductor_fields.udp_channel) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error_cleanup;
        }
    }

    return endpoint;

error_cleanup:
    aeron_udp_channel_delete(channel);
    return NULL;
}

aeron_receive_channel_endpoint_t *aeron_driver_conductor_get_or_add_receive_channel_endpoint(
    aeron_driver_conductor_t *conductor,
    aeron_driver_uri_subscription_params_t *params,
    aeron_udp_channel_t *channel,
    int64_t correlation_id)
{
    aeron_receive_channel_endpoint_t *endpoint;
    if (aeron_driver_conductor_find_existing_receive_channel_endpoint(conductor, channel, &endpoint) < 0)
    {
        goto error_cleanup;
    }

    if (aeron_driver_conductor_update_and_check_ats_status(
        conductor->context, channel, NULL == endpoint ? NULL : endpoint->conductor_fields.udp_channel) < 0)
    {
        goto error_cleanup;
    }

    const size_t socket_rcvbuf = aeron_udp_channel_socket_so_rcvbuf(channel, conductor->context->socket_rcvbuf);
    const size_t socket_sndbuf = aeron_udp_channel_socket_so_sndbuf(channel, conductor->context->socket_sndbuf);

    if (NULL == endpoint)
    {
        aeron_atomic_counter_t status_indicator;
        int ensure_capacity_result = 0;
        char bind_addr_and_port[AERON_NETUTIL_FORMATTED_MAX_LENGTH];
        int bind_addr_and_port_length;

        AERON_ARRAY_ENSURE_CAPACITY(
            ensure_capacity_result, conductor->receive_channel_endpoints, aeron_receive_channel_endpoint_entry_t)

        if (ensure_capacity_result < 0)
        {
            goto error_cleanup;
        }

        status_indicator.counter_id = aeron_counter_receive_channel_status_allocate(
            &conductor->counters_manager, correlation_id, channel->uri_length, channel->original_uri);

        if (status_indicator.counter_id < 0)
        {
            goto error_cleanup;
        }

        status_indicator.value_addr = aeron_counters_manager_addr(
            &conductor->counters_manager, status_indicator.counter_id);

        aeron_receive_destination_t *destination = NULL;

        if (AERON_UDP_CHANNEL_CONTROL_MODE_MANUAL != channel->control_mode)
        {
            if (aeron_receive_destination_create(
                &destination,
                channel,
                channel,
                conductor->context,
                &conductor->counters_manager,
                correlation_id,
                status_indicator.counter_id) < 0)
            {
                AERON_APPEND_ERR("correlation_id=%" PRId64, correlation_id);
                goto error_cleanup;
            }
        }

        if (aeron_driver_conductor_validate_initial_window_for_rcvbuf(
            params,
            socket_rcvbuf,
            conductor->context->os_buffer_lengths.default_so_rcvbuf,
            channel,
            NULL) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error_cleanup;
        }

        if (aeron_receive_channel_endpoint_create(
            &endpoint,
            channel,
            destination,
            &status_indicator,
            &conductor->system_counters,
            conductor->context) < 0)
        {
            aeron_receive_destination_delete(destination, &conductor->counters_manager);
            return NULL;
        }

        if ((bind_addr_and_port_length = aeron_receive_channel_endpoint_bind_addr_and_port(
            endpoint, bind_addr_and_port, sizeof(bind_addr_and_port))) < 0)
        {
            aeron_receive_channel_endpoint_delete(&conductor->counters_manager, endpoint);
            return NULL;
        }

        aeron_channel_endpoint_status_update_label(
            &conductor->counters_manager,
            status_indicator.counter_id,
            AERON_COUNTER_RECEIVE_CHANNEL_STATUS_NAME,
            channel->uri_length,
            channel->original_uri,
            bind_addr_and_port_length,
            bind_addr_and_port);

        if (aeron_str_to_ptr_hash_map_put(
            &conductor->receive_channel_endpoint_by_channel_map,
            channel->canonical_form,
            channel->canonical_length,
            endpoint) < 0)
        {
            aeron_receive_channel_endpoint_delete(&conductor->counters_manager, endpoint);
            return NULL;
        }

        conductor->receive_channel_endpoints.array[conductor->receive_channel_endpoints.length++].endpoint = endpoint;
        *status_indicator.value_addr = AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ACTIVE;
        aeron_driver_receiver_proxy_on_add_endpoint(endpoint->receiver_proxy, endpoint);
    }
    else
    {
        if (AERON_UDP_CHANNEL_CONTROL_MODE_MANUAL != channel->control_mode && 1 == endpoint->destinations.length)
        {
            const size_t socket_sndbuf_existing = aeron_udp_channel_socket_so_sndbuf(
                endpoint->conductor_fields.udp_channel, conductor->context->socket_sndbuf);
            const size_t socket_rcvbuf_existing = aeron_udp_channel_socket_so_rcvbuf(
                endpoint->conductor_fields.udp_channel, conductor->context->socket_rcvbuf);

            if (aeron_driver_conductor_validate_initial_window_for_rcvbuf(
                params,
                socket_rcvbuf,
                conductor->context->os_buffer_lengths.default_so_rcvbuf,
                channel,
                endpoint->conductor_fields.udp_channel) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                goto error_cleanup;
            }

            if (aeron_driver_conductor_validate_channel_buffer_length(
                AERON_URI_SOCKET_SNDBUF_KEY,
                socket_sndbuf,
                socket_sndbuf_existing,
                channel,
                endpoint->conductor_fields.udp_channel) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                goto error_cleanup;
            }

            if (aeron_driver_conductor_validate_channel_buffer_length(
                AERON_URI_SOCKET_RCVBUF_KEY,
                socket_rcvbuf,
                socket_rcvbuf_existing,
                channel,
                endpoint->conductor_fields.udp_channel) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                goto error_cleanup;
            }
        }
    }

    return endpoint;

error_cleanup:
    aeron_udp_channel_delete(channel);
    return NULL;
}

void aeron_driver_conductor_client_transmit(
    aeron_driver_conductor_t *conductor, int32_t msg_type_id, const void *msg, size_t length)
{
    conductor->context->log.to_client_interceptor(conductor, msg_type_id, msg, length);
    if (aeron_broadcast_transmitter_transmit(&conductor->to_clients, msg_type_id, msg, length) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to transmit message");
        aeron_driver_conductor_log_error(conductor);
    }
}

void on_error(
    aeron_driver_conductor_t *conductor,
    const int32_t error_code,
    const char *message,
    const size_t length,
    const int64_t correlation_id,
    const size_t response_length,
    char *response_buffer)
{
    aeron_error_response_t *response = (aeron_error_response_t *)response_buffer;

    response->offending_command_correlation_id = correlation_id;
    response->error_code = error_code;
    response->error_message_length = (int32_t)length;
    memcpy(response_buffer + sizeof(aeron_error_response_t), message, length);

    aeron_driver_conductor_client_transmit(conductor, AERON_RESPONSE_ON_ERROR, response, response_length);
}

void aeron_driver_conductor_on_publication_error(void *clientd, void *item)
{
    uint8_t buffer[sizeof(aeron_publication_error_t) + AERON_ERROR_MAX_TEXT_LENGTH];
    aeron_driver_conductor_t *conductor = clientd;
    aeron_command_publication_error_t *error = item;

    char log_str[AERON_ERROR_MAX_TEXT_LENGTH * 2];
    snprintf(log_str, sizeof(log_str), "onPublicationError: registrationId=%" PRIi64
        ", destinationRegistrationId=%" PRIi64
        ", sessionId=%" PRIi32
        ", streamId=%" PRIi32
        ", receiverId=%" PRIi64
        ", groupId=%" PRIi64
        ", errorCode=%" PRIi32
        ", errorMessage=%.*s",
        error->registration_id,
        error->destination_registration_id,
        error->session_id,
        error->stream_id,
        error->receiver_id,
        error->group_tag,
        error->error_code,
        error->error_length,
        error->error_text);
    aeron_driver_conductor_log_explicit_error(conductor, error->error_code, (const char *)log_str);

    aeron_publication_error_t *response = (aeron_publication_error_t *)buffer;
    response->error_code = error->error_code;
    response->registration_id = error->registration_id;
    response->destination_registration_id = error->destination_registration_id;
    response->session_id = error->session_id;
    response->stream_id = error->stream_id;
    response->receiver_id = error->receiver_id;
    response->group_tag = error->group_tag;

    memset(&response->source_address[0], 0, sizeof(response->source_address));
    if (AF_INET == error->src_address.ss_family)
    {
        struct sockaddr_in *src_addr_in = (struct sockaddr_in *)&error->src_address;
        response->address_type = AERON_RESPONSE_ADDRESS_TYPE_IPV4;
        response->source_port = ntohs(src_addr_in->sin_port);
        memcpy(&response->source_address[0], &src_addr_in->sin_addr, sizeof(src_addr_in->sin_addr));
    }
    else if (AF_INET6 == error->src_address.ss_family)
    {
        struct sockaddr_in6 *src_addr_in6 = (struct sockaddr_in6 *)&error->src_address;
        response->address_type = AERON_RESPONSE_ADDRESS_TYPE_IPV6;
        response->source_port = ntohs(src_addr_in6->sin6_port);
        memcpy(&response->source_address[0], &src_addr_in6->sin6_addr, sizeof(src_addr_in6->sin6_addr));
    }
    else
    {
        response->address_type = 0;
        response->source_port = 0;
    }

    response->error_message_length = error->error_length;
    memcpy(response->error_message, error->error_text, error->error_length);
    size_t response_length = offsetof(aeron_publication_error_values_t, error_message) + response->error_message_length;

    aeron_driver_conductor_client_transmit(conductor, AERON_RESPONSE_ON_PUBLICATION_ERROR, response, response_length);
}

void aeron_driver_conductor_on_error(
    aeron_driver_conductor_t *conductor,
    int32_t errcode,
    const char *errmsg,
    int64_t correlation_id
)
{
    int os_errno = errcode;
    int code = AERON_ERROR_CODE_GENERIC_ERROR;
    if (os_errno < 0)
    {
        code = -os_errno;
    }
    else if (AERON_FILEUTIL_ERROR_ENOSPC == os_errno)
    {
        code = AERON_ERROR_CODE_STORAGE_SPACE;
    }

    const size_t length = strlen(errmsg);
    const size_t response_length = sizeof(aeron_error_response_t) + length;

    if (response_length > sizeof(aeron_error_response_t) + AERON_ERROR_MAX_TOTAL_LENGTH)
    {
        char *buffer = NULL;
        if (aeron_alloc((void **)&buffer, response_length) < 0)
        {
            AERON_APPEND_ERR("%s", "failed to allocate response buffer");
            aeron_driver_conductor_log_error(conductor);

            goto log_error;
        }
        on_error(conductor, code, errmsg, length, correlation_id, response_length, buffer);
        aeron_free(buffer);
    }
    else
    {
        char buffer[sizeof(aeron_error_response_t) + AERON_ERROR_MAX_TOTAL_LENGTH];
        on_error(conductor, code, errmsg, length, correlation_id, response_length, buffer);
    }

log_error:
    aeron_driver_conductor_log_explicit_error(conductor, code, errmsg);
}

void on_publication_ready(
    aeron_driver_conductor_t *conductor,
    const int64_t registration_id,
    const int64_t original_registration_id,
    const int32_t stream_id,
    const int32_t session_id,
    const int32_t position_limit_counter_id,
    const int32_t channel_status_indicator_id,
    const bool is_exclusive,
    const char *log_file_name,
    const size_t log_file_name_length,
    const size_t response_length,
    char *response_buffer)
{
    aeron_publication_buffers_ready_t *response = (aeron_publication_buffers_ready_t *)response_buffer;

    response->correlation_id = registration_id;
    response->registration_id = original_registration_id;
    response->stream_id = stream_id;
    response->session_id = session_id;
    response->position_limit_counter_id = position_limit_counter_id;
    response->channel_status_indicator_id = channel_status_indicator_id;
    response->log_file_length = (int32_t)log_file_name_length;
    memcpy(response_buffer + sizeof(aeron_publication_buffers_ready_t), log_file_name, log_file_name_length);

    aeron_driver_conductor_client_transmit(
        conductor,
        is_exclusive ? AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY : AERON_RESPONSE_ON_PUBLICATION_READY,
        response,
        response_length);
}

void aeron_driver_conductor_on_publication_ready(
    aeron_driver_conductor_t *conductor,
    int64_t registration_id,
    int64_t original_registration_id,
    int32_t stream_id,
    int32_t session_id,
    int32_t position_limit_counter_id,
    int32_t channel_status_indicator_id,
    bool is_exclusive,
    const char *log_file_name,
    size_t log_file_name_length)
{
    const size_t response_length = sizeof(aeron_publication_buffers_ready_t) + log_file_name_length;

    if (response_length > sizeof(aeron_publication_buffers_ready_t) + AERON_MAX_PATH)
    {
        char *buffer = NULL;
        if (aeron_alloc((void **)&buffer, response_length) < 0)
        {
            AERON_APPEND_ERR("%s", "failed to allocate response buffer");
            aeron_driver_conductor_log_error(conductor);
            return;
        }

        on_publication_ready(
            conductor,
            registration_id,
            original_registration_id,
            stream_id,
            session_id,
            position_limit_counter_id,
            channel_status_indicator_id,
            is_exclusive,
            log_file_name,
            log_file_name_length,
            response_length,
            buffer);
        aeron_free(buffer);
    }
    else
    {
        char buffer[sizeof(aeron_publication_buffers_ready_t) + AERON_MAX_PATH];
        on_publication_ready(
            conductor,
            registration_id,
            original_registration_id,
            stream_id,
            session_id,
            position_limit_counter_id,
            channel_status_indicator_id,
            is_exclusive,
            log_file_name,
            log_file_name_length,
            response_length,
            buffer);
    }
}

void aeron_driver_conductor_on_subscription_ready(
    aeron_driver_conductor_t *conductor, int64_t registration_id, int32_t channel_status_indicator_id)
{
    char response_buffer[sizeof(aeron_correlated_command_t)];
    aeron_subscription_ready_t *response = (aeron_subscription_ready_t *)response_buffer;

    response->correlation_id = registration_id;
    response->channel_status_indicator_id = channel_status_indicator_id;

    aeron_driver_conductor_client_transmit(
        conductor, AERON_RESPONSE_ON_SUBSCRIPTION_READY, response, sizeof(aeron_subscription_ready_t));
}

void aeron_driver_conductor_on_counter_ready(
    aeron_driver_conductor_t *conductor, int64_t registration_id, int32_t counter_id)
{
    char response_buffer[sizeof(aeron_counter_update_t)];
    aeron_counter_update_t *response = (aeron_counter_update_t *)response_buffer;

    response->correlation_id = registration_id;
    response->counter_id = counter_id;

    aeron_driver_conductor_client_transmit(
        conductor, AERON_RESPONSE_ON_COUNTER_READY, response, sizeof(aeron_counter_update_t));
}

void aeron_driver_conductor_on_unavailable_counter(
    aeron_driver_conductor_t *conductor, int64_t registration_id, int32_t counter_id)
{
    char response_buffer[sizeof(aeron_counter_update_t)];
    aeron_counter_update_t *response = (aeron_counter_update_t *)response_buffer;

    response->correlation_id = registration_id;
    response->counter_id = counter_id;

    aeron_driver_conductor_client_transmit(
        conductor, AERON_RESPONSE_ON_UNAVAILABLE_COUNTER, response, sizeof(aeron_counter_update_t));
}

void aeron_driver_conductor_on_static_counter(
    aeron_driver_conductor_t *conductor, int64_t correlation_id, int32_t counter_id)
{
    char response_buffer[sizeof(aeron_static_counter_response_t)];
    aeron_static_counter_response_t *response = (aeron_static_counter_response_t *)response_buffer;

    response->correlation_id = correlation_id;
    response->counter_id = counter_id;

    aeron_driver_conductor_client_transmit(
        conductor, AERON_RESPONSE_ON_STATIC_COUNTER, response, sizeof(aeron_static_counter_response_t));
}

void aeron_driver_conductor_on_operation_succeeded(aeron_driver_conductor_t *conductor, int64_t correlation_id)
{
    char response_buffer[sizeof(aeron_correlated_command_t)];
    aeron_operation_succeeded_t *response = (aeron_operation_succeeded_t *)response_buffer;

    response->correlation_id = correlation_id;

    aeron_driver_conductor_client_transmit(
        conductor, AERON_RESPONSE_ON_OPERATION_SUCCESS, response, sizeof(aeron_operation_succeeded_t));
}

void aeron_driver_conductor_on_client_timeout(aeron_driver_conductor_t *conductor, int64_t client_id)
{
    char response_buffer[sizeof(aeron_client_timeout_t)];
    aeron_client_timeout_t *response = (aeron_client_timeout_t *)response_buffer;

    response->client_id = client_id;

    aeron_driver_conductor_client_transmit(
        conductor, AERON_RESPONSE_ON_CLIENT_TIMEOUT, response, sizeof(aeron_client_timeout_t));
}

void on_unavailable_image(
    aeron_driver_conductor_t *conductor,
    const int64_t correlation_id,
    const int64_t subscription_registration_id,
    const int32_t stream_id,
    const char *channel,
    const size_t channel_length,
    const size_t response_length,
    char *response_buffer)
{
    aeron_image_message_t *response = (aeron_image_message_t *)response_buffer;

    response->correlation_id = correlation_id;
    response->subscription_registration_id = subscription_registration_id;
    response->stream_id = stream_id;
    response->channel_length = (int32_t)channel_length;
    memcpy(response_buffer + sizeof(aeron_image_message_t), channel, channel_length);

    aeron_driver_conductor_client_transmit(conductor, AERON_RESPONSE_ON_UNAVAILABLE_IMAGE, response, response_length);
}

void aeron_driver_conductor_on_unavailable_image(
    aeron_driver_conductor_t *conductor,
    int64_t correlation_id,
    int64_t subscription_registration_id,
    int32_t stream_id,
    const char *channel,
    size_t channel_length)
{
    const size_t response_length = sizeof(aeron_image_message_t) + channel_length;

    if (response_length > sizeof(aeron_image_message_t) + AERON_MAX_PATH)
    {
        char *buffer = NULL;
        if (aeron_alloc((void **)&buffer, response_length) < 0)
        {
            AERON_APPEND_ERR("%s", "failed to allocate response buffer");
            aeron_driver_conductor_log_error(conductor);
            return;
        }

        on_unavailable_image(
            conductor,
            correlation_id,
            subscription_registration_id,
            stream_id,
            channel,
            channel_length,
            response_length,
            buffer);
        aeron_free(buffer);
    }
    else
    {
        char buffer[sizeof(aeron_image_message_t) + AERON_MAX_PATH];
        on_unavailable_image(
            conductor,
            correlation_id,
            subscription_registration_id,
            stream_id,
            channel,
            channel_length,
            response_length,
            buffer);
    }
}

static bool aeron_driver_conductor_not_accepting_client_commands(aeron_driver_conductor_t *conductor)
{
    if (conductor->async_client_command_in_flight)
    {
        return true;
    }

    aeron_mpsc_rb_t *sender_rb = conductor->context->sender_proxy->command_queue;
    aeron_mpsc_rb_t *receiver_rb = conductor->context->receiver_proxy->command_queue;
    return
        ((sender_rb->capacity - aeron_mpsc_rb_size(sender_rb)) <= AERON_COMMAND_RB_RESERVE) ||
        ((receiver_rb->capacity - aeron_mpsc_rb_size(receiver_rb)) <= AERON_COMMAND_RB_RESERVE);
}

typedef struct aeron_driver_async_command_stct
{
    aeron_udp_channel_async_parse_t async_parse;
    aeron_name_resolver_async_resolve_t async_resolve;
    aeron_send_channel_endpoint_t *endpoint;
    aeron_uri_t *uri;
    void *original_command;
    bool is_exclusive;
}
aeron_driver_async_command_t;

typedef int (*aeron_driver_async_client_command_on_execute_func_t)(aeron_driver_conductor_t *conductor, void *clientd);

typedef int (*aeron_driver_async_client_command_on_complete_func_t)(
    aeron_driver_conductor_t *conductor,
    aeron_driver_async_command_t *async_command,
    void *clientd);

typedef int (*aeron_driver_async_client_command_on_error_func_t)(
    aeron_driver_conductor_t *conductor,
    int result,
    aeron_driver_async_command_t *async_command,
    void *clientd);

typedef struct aeron_driver_async_client_command_stct
{
    aeron_correlated_command_t *correlated;
    aeron_driver_async_client_command_on_execute_func_t on_execute;
    void *on_execute_clientd; // this is passed to the on_execute callback
    aeron_driver_async_client_command_on_complete_func_t on_complete;
    aeron_driver_async_client_command_on_error_func_t on_error;
    aeron_driver_async_command_t async_command;
}
aeron_driver_async_client_command_t;

/* This is an aeron_executor 'execute' callback - it's called from an executor thread */
int aeron_driver_async_client_command_execute(void *task_clientd, void *executor_clientd)
{
    aeron_driver_async_client_command_t *async_client_command = task_clientd;
    aeron_driver_conductor_t *conductor = executor_clientd;

    if (async_client_command->on_execute(conductor, async_client_command->on_execute_clientd) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    return 0;
}

/* This is an aeron_executor 'complete' callback - it's called when 'aeron_executor_process_completions' is called */
void aeron_driver_async_client_command_complete(
    int result,
    int errcode,
    const char *errmsg,
    void *task_clientd,
    void *executor_clientd)
{
    aeron_driver_async_client_command_t *async_client_command = task_clientd;
    aeron_driver_conductor_t *conductor = executor_clientd;
    int64_t correlation_id = async_client_command->correlated->correlation_id;

    conductor->async_client_command_in_flight = false;

    if (result < 0)
    {
        if (NULL == async_client_command->on_error)
        {
            aeron_driver_conductor_on_error(conductor, errcode, errmsg, correlation_id);
        }
        else
        {
            // TODO set current errcode/errmsg to what was handed to this function???
            if (async_client_command->on_error(
                conductor,
                result,
                &async_client_command->async_command,
                async_client_command->on_execute_clientd) < 0)
            {
                aeron_driver_conductor_on_error(conductor, aeron_errcode(), aeron_errmsg(), correlation_id);
            }
        }
    }
    else if (async_client_command->on_complete(
        conductor, &async_client_command->async_command, async_client_command->on_execute_clientd) < 0)
    {
        aeron_driver_conductor_on_error(conductor, aeron_errcode(), aeron_errmsg(), correlation_id);
    }

    aeron_free(async_client_command);
}

int aeron_driver_async_client_command_allocate(
    aeron_driver_async_client_command_t **async_client_commandp,
    void *original_command,
    size_t original_command_length)
{
    aeron_driver_async_client_command_t *async_client_command;

    if (aeron_alloc(
        (void **)&async_client_command,
        AERON_PADDED_SIZEOF(aeron_driver_async_client_command_t) + original_command_length) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    async_client_command->on_error = NULL;
    async_client_command->async_command.original_command =
        (void *)((const char *)async_client_command + AERON_PADDED_SIZEOF(aeron_driver_async_client_command_t));

    memcpy(async_client_command->async_command.original_command, original_command, original_command_length);

    *async_client_commandp = async_client_command;

    return 0;
}

int aeron_driver_async_client_command_submit(
    aeron_driver_conductor_t *conductor,
    aeron_driver_async_client_command_t *async_client_command)
{
    conductor->async_client_command_in_flight = true;

    if (aeron_executor_submit(
        &conductor->executor,
        aeron_driver_async_client_command_execute,
        aeron_driver_async_client_command_complete,
        async_client_command) < 0)
    {
        conductor->async_client_command_in_flight = false;
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    return 0;
}

int aeron_driver_async_parse_udp_channel_execute(aeron_driver_conductor_t *conductor, void *clientd)
{
    aeron_udp_channel_async_parse_t *async_parse = clientd;

    if (aeron_udp_channel_finish_parse(
        &conductor->name_resolver,
        async_parse) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    return 0;
}

int aeron_driver_async_resolve_execute(aeron_driver_conductor_t *conductor, void *clientd)
{
    aeron_name_resolver_async_resolve_t *async_resolve = clientd;

    if (aeron_name_resolver_resolve_host_and_port(
        &conductor->name_resolver,
        async_resolve->endpoint_name,
        async_resolve->uri_param_name,
        async_resolve->is_re_resolution,
        &async_resolve->sockaddr) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    return 0;
}

typedef struct aeron_async_re_resolve_stct
{
    aeron_name_resolver_async_resolve_t async_resolve;
    struct sockaddr_storage existing_addr;
    void *endpoint;
    void *destination;
}
aeron_async_re_resolve_t;

int aeron_driver_async_resolve_host_and_port_execute(void *task_clientd, void *executor_clientd)
{
    aeron_async_re_resolve_t *async_cmd = task_clientd;
    aeron_driver_conductor_t *conductor = executor_clientd;

    if (aeron_driver_async_resolve_execute(conductor, &async_cmd->async_resolve) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    return 0;
}

aeron_rb_read_action_t aeron_driver_conductor_on_command(
    int32_t msg_type_id, const void *message, size_t length, void *clientd)
{
    aeron_driver_conductor_t *conductor = (aeron_driver_conductor_t *)clientd;
    int64_t correlation_id = 0;
    int result = 0;

    if (aeron_driver_conductor_not_accepting_client_commands(conductor))
    {
        return AERON_RB_ABORT;
    }

    conductor->context->log.to_driver_interceptor(msg_type_id, message, length, clientd);

    switch (msg_type_id)
    {
        case AERON_COMMAND_ADD_PUBLICATION:
        {
            aeron_publication_command_t *command = (aeron_publication_command_t *)message;

            if (length < sizeof(aeron_publication_command_t) ||
                length < (sizeof(aeron_publication_command_t) + command->channel_length))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;
            const char *channel = (const char *)message + sizeof(aeron_publication_command_t);

            if (strncmp(channel, AERON_IPC_CHANNEL, AERON_IPC_CHANNEL_LEN) == 0)
            {
                result = aeron_driver_conductor_on_add_ipc_publication(conductor, command, false);
            }
            else
            {
                result = aeron_driver_conductor_on_add_network_publication(conductor, command, false);
            }
            break;
        }

        case AERON_COMMAND_ADD_EXCLUSIVE_PUBLICATION:
        {
            aeron_publication_command_t *command = (aeron_publication_command_t *)message;

            if (length < sizeof(aeron_publication_command_t) ||
                length < (sizeof(aeron_publication_command_t) + command->channel_length))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;
            const char *channel = (const char *)message + sizeof(aeron_publication_command_t);

            if (strncmp(channel, AERON_IPC_CHANNEL, AERON_IPC_CHANNEL_LEN) == 0)
            {
                result = aeron_driver_conductor_on_add_ipc_publication(conductor, command, true);
            }
            else
            {
                result = aeron_driver_conductor_on_add_network_publication(conductor, command, true);
            }
            break;
        }

        case AERON_COMMAND_REMOVE_PUBLICATION:
        {
            aeron_remove_publication_command_t *command = (aeron_remove_publication_command_t *)message;

            aeron_remove_publication_command_t complete_command;
            aeron_remove_publication_command_t *complete_command_p;

            if (length < sizeof(aeron_remove_publication_command_t))
            {
                if (length == offsetof(aeron_remove_publication_command_t, flags)) // old aeron_remove_command_t has no flags field
                {
                    complete_command.correlated.client_id = command->correlated.client_id;
                    complete_command.correlated.correlation_id = command->correlated.correlation_id;
                    complete_command.registration_id = command->registration_id;
                    complete_command.flags = 0;

                    complete_command_p = &complete_command;
                }
                else
                {
                    goto malformed_command;
                }
            }
            else
            {
                complete_command_p = command;
            }

            correlation_id = command->correlated.correlation_id;

            result = aeron_driver_conductor_on_remove_publication(conductor, complete_command_p);
            break;
        }

        case AERON_COMMAND_ADD_SUBSCRIPTION:
        {
            aeron_subscription_command_t *command = (aeron_subscription_command_t *)message;

            if (length < sizeof(aeron_subscription_command_t) ||
                length < (sizeof(aeron_subscription_command_t) + command->channel_length))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;
            const char *channel = (const char *)message + sizeof(aeron_subscription_command_t);

            if (strncmp(channel, AERON_IPC_CHANNEL, AERON_IPC_CHANNEL_LEN) == 0)
            {
                result = aeron_driver_conductor_on_add_ipc_subscription(conductor, command);
            }
            else if (strncmp(channel, AERON_SPY_PREFIX, AERON_SPY_PREFIX_LEN) == 0)
            {
                result = aeron_driver_conductor_on_add_spy_subscription(conductor, command);
            }
            else
            {
                result = aeron_driver_conductor_on_add_network_subscription(conductor, command);
            }
            break;
        }

        case AERON_COMMAND_REMOVE_SUBSCRIPTION:
        {
            aeron_remove_subscription_command_t *command = (aeron_remove_subscription_command_t *)message;

            if (length < sizeof(aeron_remove_subscription_command_t))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;

            result = aeron_driver_conductor_on_remove_subscription(conductor, command);
            break;
        }

        case AERON_COMMAND_CLIENT_KEEPALIVE:
        {
            aeron_correlated_command_t *command = (aeron_correlated_command_t *)message;

            if (length < sizeof(aeron_correlated_command_t))
            {
                goto malformed_command;
            }

            result = aeron_driver_conductor_on_client_keepalive(conductor, command->client_id);
            break;
        }

        case AERON_COMMAND_ADD_DESTINATION:
        {
            aeron_destination_command_t *command = (aeron_destination_command_t *)message;

            if (length < sizeof(aeron_destination_command_t) ||
                length < (sizeof(aeron_destination_command_t) + command->channel_length))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;

            result = aeron_driver_conductor_on_add_send_destination(conductor, command);
            break;
        }

        case AERON_COMMAND_REMOVE_DESTINATION:
        {
            aeron_destination_command_t *command = (aeron_destination_command_t *)message;

            if (length < sizeof(aeron_destination_command_t) ||
                length < (sizeof(aeron_destination_command_t) + command->channel_length))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;

            result = aeron_driver_conductor_on_remove_send_destination(conductor, command);
            break;
        }

        case AERON_COMMAND_ADD_RCV_DESTINATION:
        {
            aeron_destination_command_t *command = (aeron_destination_command_t *)message;

            if (length < sizeof(aeron_destination_command_t) ||
                length < (sizeof(aeron_destination_command_t) + command->channel_length))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;
            const char *channel = (const char *)message + sizeof(aeron_destination_command_t);

            if (strncmp(channel, AERON_IPC_CHANNEL, AERON_IPC_CHANNEL_LEN) == 0)
            {
                result = aeron_driver_conductor_on_add_receive_ipc_destination(conductor, command);
            }
            else if (strncmp(channel, AERON_SPY_PREFIX, AERON_SPY_PREFIX_LEN) == 0)
            {
                result = aeron_driver_conductor_on_add_receive_spy_destination(conductor, command);
            }
            else
            {
                result = aeron_driver_conductor_on_add_receive_network_destination(conductor, command);
            }
            break;
        }

        case AERON_COMMAND_REMOVE_RCV_DESTINATION:
        {
            aeron_destination_command_t *command = (aeron_destination_command_t *)message;

            if (length < sizeof(aeron_destination_command_t) ||
                length < (sizeof(aeron_destination_command_t) + command->channel_length))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;
            const char *channel = (const char *)message + sizeof(aeron_destination_command_t);

            if (strncmp(channel, AERON_IPC_CHANNEL, AERON_IPC_CHANNEL_LEN) == 0)
            {
                result = aeron_driver_conductor_on_remove_receive_ipc_destination(conductor, command);
            }
            else if (strncmp(channel, AERON_SPY_PREFIX, AERON_SPY_PREFIX_LEN) == 0)
            {
                result = aeron_driver_conductor_on_remove_receive_spy_destination(conductor, command);
            }
            else
            {
                result = aeron_driver_conductor_on_remove_receive_network_destination(conductor, command);
            }
            break;
        }

        case AERON_COMMAND_ADD_COUNTER:
        {
            aeron_counter_command_t *command = (aeron_counter_command_t *)message;

            if (length < sizeof(aeron_counter_command_t))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;

            result = aeron_driver_conductor_on_add_counter(conductor, command);
            break;
        }

        case AERON_COMMAND_REMOVE_COUNTER:
        {
            aeron_remove_counter_command_t *command = (aeron_remove_counter_command_t *)message;

            if (length < sizeof(aeron_remove_counter_command_t))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;

            result = aeron_driver_conductor_on_remove_counter(conductor, command);
            break;
        }

        case AERON_COMMAND_CLIENT_CLOSE:
        {
            aeron_correlated_command_t *command = (aeron_correlated_command_t *)message;

            if (length < sizeof(aeron_correlated_command_t))
            {
                goto malformed_command;
            }

            result = aeron_driver_conductor_on_client_close(conductor, command);
            break;
        }

        case AERON_COMMAND_TERMINATE_DRIVER:
        {
            aeron_terminate_driver_command_t *command = (aeron_terminate_driver_command_t *)message;

            if (length < sizeof(aeron_terminate_driver_command_t))
            {
                goto malformed_command;
            }

            result = aeron_driver_conductor_on_terminate_driver(conductor, command);

            break;
        }

        case AERON_COMMAND_ADD_STATIC_COUNTER:
        {
            aeron_static_counter_command_t *command = (aeron_static_counter_command_t *)message;

            if (length < sizeof(aeron_static_counter_command_t))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;

            result = aeron_driver_conductor_on_add_static_counter(conductor, command);
            break;
        }

        case AERON_COMMAND_REJECT_IMAGE:
        {
            aeron_reject_image_command_t *command = (aeron_reject_image_command_t *)message;
            correlation_id = command->correlated.correlation_id;

            if (length < sizeof(aeron_reject_image_command_t))
            {
                goto malformed_command;
            }

            if (length < offsetof(aeron_reject_image_command_t, reason_text) + command->reason_length)
            {
                goto malformed_command;
            }

            result = aeron_driver_conductor_on_invalidate_image(conductor, command);

            break;
        }

        case AERON_COMMAND_REMOVE_DESTINATION_BY_ID:
        {
            aeron_destination_by_id_command_t *command = (aeron_destination_by_id_command_t *)message;

            if (length < sizeof(aeron_destination_by_id_command_t))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;

            aeron_driver_conductor_on_remove_receive_send_destination_by_id(conductor, command);
            break;
        }

        default:
            AERON_SET_ERR(-AERON_ERROR_CODE_UNKNOWN_COMMAND_TYPE_ID, "command=%d unknown", msg_type_id);
            aeron_driver_conductor_log_error(conductor);
            break;
    }

    if (result < 0)
    {
        aeron_driver_conductor_on_error(conductor, aeron_errcode(), aeron_errmsg(), correlation_id);
    }

    if (conductor->async_client_command_in_flight)
    {
        return AERON_RB_BREAK;
    }

    return AERON_RB_CONTINUE;

malformed_command:
    AERON_SET_ERR(
        -AERON_ERROR_CODE_MALFORMED_COMMAND, "command=%d too short: length=%" PRIu64, msg_type_id, (uint64_t)length);
    aeron_driver_conductor_log_error(conductor);

    return AERON_RB_CONTINUE;
}

void aeron_driver_conductor_on_command_queue(void *clientd, void *item)
{
    aeron_command_base_t *cmd = (aeron_command_base_t *)item;
    cmd->func(clientd, cmd);
}

void aeron_driver_conductor_on_check_for_blocked_driver_commands(aeron_driver_conductor_t *conductor, int64_t now_ns)
{
    int64_t consumer_position = aeron_mpsc_rb_consumer_position(&conductor->to_driver_commands);

    if (consumer_position == conductor->last_command_consumer_position &&
        aeron_mpsc_rb_producer_position(&conductor->to_driver_commands) > consumer_position)
    {
        int64_t position_change_deadline_ns = conductor->time_of_last_to_driver_position_change_ns +
            (int64_t)conductor->context->client_liveness_timeout_ns;
        if (now_ns > position_change_deadline_ns)
        {
            if (aeron_mpsc_rb_unblock(&conductor->to_driver_commands))
            {
                aeron_counter_increment_release(conductor->unblocked_commands_counter);
            }
        }
    }
    else
    {
        conductor->time_of_last_to_driver_position_change_ns = now_ns;
        conductor->last_command_consumer_position = consumer_position;
    }
}

void aeron_driver_conductor_track_time(aeron_driver_conductor_t *conductor, int64_t now_ns)
{
    aeron_clock_update_cached_nano_time(conductor->context->cached_clock, now_ns);
    aeron_duty_cycle_tracker_t *tracker = conductor->context->conductor_duty_cycle_tracker;

    tracker->measure_and_update(tracker->state, now_ns);

    if (now_ns >= conductor->clock_update_deadline_ns)
    {
        conductor->clock_update_deadline_ns = now_ns + AERON_DRIVER_CONDUCTOR_CLOCK_UPDATE_INTERNAL_NS;
        aeron_clock_update_cached_epoch_time(conductor->context->cached_clock, conductor->context->epoch_clock());
    }
}

static void aeron_driver_conductor_on_rb_command_queue(
    int32_t msg_type_id,
    const void *message,
    size_t size,
    void *clientd)
{
    aeron_command_base_t *cmd = (aeron_command_base_t *)message;
    cmd->func(clientd, cmd);
}

int aeron_driver_conductor_do_work(void *clientd)
{
    aeron_driver_conductor_t *conductor = (aeron_driver_conductor_t *)clientd;
    const int64_t now_ns = conductor->context->nano_clock();
    aeron_driver_conductor_track_time(conductor, now_ns);
    const int64_t now_ms = aeron_clock_cached_epoch_time(conductor->context->cached_clock);
    int work_count = 0;

    if (now_ns > conductor->timeout_check_deadline_ns)
    {
        aeron_mpsc_rb_consumer_heartbeat_time(&conductor->to_driver_commands, now_ms);
        aeron_driver_conductor_on_check_managed_resources(conductor, now_ns, now_ms);
        aeron_driver_conductor_on_check_for_blocked_driver_commands(conductor, now_ns);
        conductor->timeout_check_deadline_ns = now_ns + (int64_t)conductor->context->timer_interval_ns;
        work_count++;
    }
    if (!conductor->async_client_command_in_flight)
    {
        work_count += (int)aeron_mpsc_rb_controlled_read(
            &conductor->to_driver_commands, aeron_driver_conductor_on_command, conductor, AERON_COMMAND_DRAIN_LIMIT);
    }
    work_count += (int)aeron_mpsc_rb_read(
        conductor->conductor_proxy.command_queue,
        aeron_driver_conductor_on_rb_command_queue,
        conductor,
        AERON_COMMAND_DRAIN_LIMIT);

    for (size_t i = 0, length = conductor->publication_images.length; i < length; i++)
    {
        aeron_publication_image_track_rebuild(conductor->publication_images.array[i].image, now_ns);
    }

    for (size_t i = 0, length = conductor->network_publications.length; i < length; i++)
    {
        work_count +=
            aeron_network_publication_update_pub_pos_and_lmt(conductor->network_publications.array[i].publication);
    }

    for (size_t i = 0, length = conductor->ipc_publications.length; i < length; i++)
    {
        work_count += aeron_ipc_publication_update_pub_pos_and_lmt(conductor->ipc_publications.array[i].publication);
    }

    work_count += conductor->name_resolver.do_work_func(&conductor->name_resolver, now_ms);
    work_count += aeron_driver_conductor_free_end_of_life_resources(conductor);
    work_count += aeron_executor_process_completions(&conductor->executor, 1);

    return work_count;
}

void aeron_driver_conductor_on_close(void *clientd)
{
    aeron_driver_conductor_t *conductor = (aeron_driver_conductor_t *)clientd;

    aeron_executor_close(&conductor->executor);

    conductor->name_resolver.close_func(&conductor->name_resolver);

    for (size_t i = 0, length = conductor->clients.length; i < length; i++)
    {
        aeron_free(conductor->clients.array[i].publication_links.array);
        aeron_free(conductor->clients.array[i].counter_links.array);
    }
    aeron_free(conductor->clients.array);

    for (size_t i = 0, length = conductor->ipc_publications.length; i < length; i++)
    {
        aeron_ipc_publication_close(&conductor->counters_manager, conductor->ipc_publications.array[i].publication);
        aeron_ipc_publication_free(conductor->ipc_publications.array[i].publication);
    }
    aeron_free(conductor->ipc_publications.array);

    for (size_t i = 0, length = conductor->network_publications.length; i < length; i++)
    {
        aeron_network_publication_close(
            &conductor->counters_manager, conductor->network_publications.array[i].publication);
        aeron_network_publication_free(conductor->network_publications.array[i].publication);
    }
    aeron_free(conductor->network_publications.array);

    for (size_t i = 0, length = conductor->ipc_subscriptions.length; i < length; i++)
    {
        aeron_free(conductor->ipc_subscriptions.array[i].subscribable_list.array);
    }
    aeron_free(conductor->ipc_subscriptions.array);

    for (size_t i = 0, length = conductor->network_subscriptions.length; i < length; i++)
    {
        aeron_free(conductor->network_subscriptions.array[i].subscribable_list.array);
    }
    aeron_free(conductor->network_subscriptions.array);

    for (size_t i = 0, length = conductor->spy_subscriptions.length; i < length; i++)
    {
        aeron_udp_channel_delete(conductor->spy_subscriptions.array[i].spy_channel);
        aeron_free(conductor->spy_subscriptions.array[i].subscribable_list.array);
    }
    aeron_free(conductor->spy_subscriptions.array);

    for (size_t i = 0, length = conductor->send_channel_endpoints.length; i < length; i++)
    {
        aeron_send_channel_endpoint_delete(
            &conductor->counters_manager, conductor->send_channel_endpoints.array[i].endpoint);
    }
    aeron_free(conductor->send_channel_endpoints.array);

    for (size_t i = 0, length = conductor->receive_channel_endpoints.length; i < length; i++)
    {
        aeron_receive_channel_endpoint_delete(
            &conductor->counters_manager, conductor->receive_channel_endpoints.array[i].endpoint);
    }
    aeron_free(conductor->receive_channel_endpoints.array);

    for (size_t i = 0, length = conductor->publication_images.length; i < length; i++)
    {
        aeron_publication_image_close(&conductor->counters_manager, conductor->publication_images.array[i].image);
        aeron_publication_image_free(conductor->publication_images.array[i].image);
    }
    aeron_free(conductor->publication_images.array);

    aeron_deque_close(&conductor->end_of_life_queue);

    aeron_system_counters_close(&conductor->system_counters);
    aeron_counters_manager_close(&conductor->counters_manager);
    aeron_distinct_error_log_close(&conductor->error_log);

    aeron_str_to_ptr_hash_map_delete(&conductor->send_channel_endpoint_by_channel_map);
    aeron_str_to_ptr_hash_map_delete(&conductor->receive_channel_endpoint_by_channel_map);
    aeron_mpsc_rb_consumer_heartbeat_time(&conductor->to_driver_commands, AERON_NULL_VALUE);
    aeron_msync(conductor->context->cnc_map.addr, conductor->context->cnc_map.length);
}

int aeron_driver_subscribable_add_position(
    aeron_subscribable_t *subscribable,
    aeron_subscription_link_t *link,
    int32_t counter_id,
    int64_t *value_addr,
    int64_t now_ns)
{
    int ensure_capacity_result = 0, result = -1;
    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, (*subscribable), aeron_tetherable_position_t)

    if (ensure_capacity_result >= 0)
    {
        aeron_tetherable_position_t *entry = &subscribable->array[subscribable->length];
        entry->is_tether = link->is_tether;
        entry->state = AERON_SUBSCRIPTION_TETHER_ACTIVE;
        entry->counter_id = counter_id;
        entry->value_addr = value_addr;
        entry->subscription_registration_id = link->registration_id;
        entry->time_of_last_update_ns = now_ns;
        subscribable->add_position_hook_func(subscribable->clientd, value_addr);
        subscribable->length++;
        result = 0;
    }

    return result;
}

void aeron_driver_subscribable_remove_position(aeron_subscribable_t *subscribable, int32_t counter_id)
{
    for (size_t i = 0, size = subscribable->length, last_index = size - 1; i < size; i++)
    {
        aeron_tetherable_position_t *tetherable_position = &subscribable->array[i];
        if (counter_id == tetherable_position->counter_id)
        {
            if (AERON_SUBSCRIPTION_TETHER_RESTING == tetherable_position->state)
            {
                subscribable->resting_count--;
            }

            subscribable->remove_position_hook_func(subscribable->clientd, tetherable_position->value_addr);
            aeron_array_fast_unordered_remove(
                (uint8_t *)subscribable->array, sizeof(aeron_tetherable_position_t), i, last_index);
            subscribable->length--;

            break;
        }
    }
}

int aeron_driver_conductor_link_subscribable(
    aeron_driver_conductor_t *conductor,
    aeron_subscription_link_t *link,
    aeron_subscribable_t *subscribable,
    int64_t original_registration_id,
    int32_t session_id,
    int32_t stream_id,
    int64_t join_position,
    int64_t now_ns,
    size_t source_identity_length,
    const char *source_identity,
    size_t log_file_name_length,
    const char *log_file_name)
{
    int ensure_capacity_result = 0, result = -1;
    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, link->subscribable_list, aeron_subscribable_list_entry_t)

    if (ensure_capacity_result >= 0)
    {
        int64_t joining_position = join_position;
        int32_t counter_id = aeron_counter_subscription_position_allocate(
            &conductor->counters_manager,
            link->registration_id,
            session_id,
            stream_id,
            link->channel_length,
            link->channel,
            joining_position);

        if (counter_id >= 0)
        {
            aeron_counters_manager_counter_owner_id(
                &conductor->counters_manager, counter_id, link->client_id);
            aeron_counters_manager_counter_reference_id(
                &conductor->counters_manager, counter_id, original_registration_id);
            int64_t *position_addr = aeron_counters_manager_addr(&conductor->counters_manager, counter_id);

            if (aeron_driver_subscribable_add_position(subscribable, link, counter_id, position_addr, now_ns) >= 0)
            {
                aeron_subscribable_list_entry_t *entry =
                    &link->subscribable_list.array[link->subscribable_list.length++];

                aeron_counter_set_release(position_addr, joining_position);

                entry->subscribable = subscribable;
                entry->counter_id = counter_id;

                aeron_driver_conductor_on_available_image(
                    conductor,
                    original_registration_id,
                    stream_id,
                    session_id,
                    log_file_name,
                    log_file_name_length,
                    counter_id,
                    link->registration_id,
                    source_identity,
                    source_identity_length);

                result = 0;
            }
            else
            {
                aeron_counters_manager_free(&conductor->counters_manager, counter_id);
            }
        }
    }

    return result;
}

void aeron_driver_subscribable_state(
    aeron_subscribable_t *subscribable,
    aeron_tetherable_position_t *tetherable_position,
    aeron_subscription_tether_state_t state,
    int64_t now_ns)
{
    if (tetherable_position->state != state)
    {
        if (AERON_SUBSCRIPTION_TETHER_RESTING == state)
        {
            subscribable->resting_count++;
        }
        else if (AERON_SUBSCRIPTION_TETHER_RESTING == tetherable_position->state)
        {
            subscribable->resting_count--;
        }
    }

    tetherable_position->state = state;
    tetherable_position->time_of_last_update_ns = now_ns;
}

size_t aeron_driver_subscribable_working_position_count(aeron_subscribable_t *subscribable)
{
    return subscribable->length - subscribable->resting_count;
}

bool aeron_driver_subscribable_has_working_positions(aeron_subscribable_t *subscribable)
{
    return subscribable->resting_count < subscribable->length;
}

void aeron_driver_conductor_unlink_subscribable(aeron_subscription_link_t *link, aeron_subscribable_t *subscribable)
{
    for (int last_index = (int32_t)link->subscribable_list.length - 1, i = last_index; i >= 0; i--)
    {
        if (subscribable == link->subscribable_list.array[i].subscribable)
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *)link->subscribable_list.array, sizeof(aeron_subscribable_list_entry_t), i, last_index);
            link->subscribable_list.length--;
            last_index--;
        }
    }
}

void aeron_driver_conductor_unlink_all_subscribable(
    aeron_driver_conductor_t *conductor, aeron_subscription_link_t *link)
{
    for (size_t i = 0; i < link->subscribable_list.length; i++)
    {
        aeron_subscribable_list_entry_t *entry = &link->subscribable_list.array[i];

        aeron_driver_subscribable_remove_position(entry->subscribable, entry->counter_id);
        aeron_counters_manager_free(&conductor->counters_manager, entry->counter_id);
    }

    aeron_free(link->subscribable_list.array);
    link->subscribable_list.array = NULL;
    link->subscribable_list.length = 0;
    link->subscribable_list.capacity = 0;
}

int aeron_driver_conductor_link_ipc_subscriptions(
    aeron_driver_conductor_t *conductor, aeron_ipc_publication_t *publication)
{
    aeron_subscribable_t *subscribable = &publication->conductor_fields.subscribable;
    int64_t now_ns = aeron_clock_cached_nano_time(conductor->context->cached_clock);

    for (size_t i = 0; i < conductor->ipc_subscriptions.length; i++)
    {
        aeron_subscription_link_t *subscription_link = &conductor->ipc_subscriptions.array[i];

        if (aeron_driver_conductor_subscription_link_matches_ipc_publication(subscription_link, publication) &&
            !aeron_driver_conductor_is_subscribable_linked(subscription_link, subscribable) &&
            aeron_ipc_publication_is_accepting_subscriptions(publication))
        {
            if (aeron_driver_conductor_link_subscribable(
                conductor,
                subscription_link,
                subscribable,
                publication->conductor_fields.managed_resource.registration_id,
                publication->session_id,
                publication->stream_id,
                aeron_ipc_publication_join_position(publication),
                now_ns,
                AERON_IPC_CHANNEL_LEN,
                AERON_IPC_CHANNEL,
                publication->log_file_name_length,
                publication->log_file_name) < 0)
            {
                return -1;
            }
        }
    }

    return 0;
}

int aeron_driver_conductor_on_add_ipc_publication(
    aeron_driver_conductor_t *conductor, aeron_publication_command_t *command, bool is_exclusive)
{
    int64_t correlation_id = command->correlated.correlation_id;
    const char *uri = (const char *)command + sizeof(aeron_publication_command_t);
    size_t uri_length = (size_t)command->channel_length;
    aeron_uri_t aeron_uri_params;
    aeron_driver_uri_publication_params_t params;

    if (aeron_uri_parse(uri_length, uri, &aeron_uri_params) < 0 ||
        aeron_diver_uri_publication_params(
            &aeron_uri_params,
            &params,
            conductor,
            is_exclusive) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to parse IPC publication URI");
        goto error_cleanup;
    }

    aeron_client_t *client = aeron_driver_conductor_get_or_add_client(conductor, command->correlated.client_id);
    if (NULL == client)
    {
        AERON_APPEND_ERR("%s", "Failed to add client");
        goto error_cleanup;
    }

    aeron_ipc_publication_t *publication = aeron_driver_conductor_get_or_add_ipc_publication(
        conductor, client, &params, correlation_id, command->stream_id, uri_length, uri, is_exclusive);
    if (NULL == publication)
    {
        goto error_cleanup;
    }

    aeron_driver_conductor_on_publication_ready(
        conductor,
        command->correlated.correlation_id,
        publication->conductor_fields.managed_resource.registration_id,
        publication->stream_id,
        publication->session_id,
        publication->pub_lmt_position.counter_id,
        AERON_CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED,
        is_exclusive,
        publication->log_file_name,
        publication->log_file_name_length);

    if (aeron_driver_conductor_link_ipc_subscriptions(conductor, publication) < 0)
    {
        goto error_cleanup;
    }

    aeron_uri_close(&aeron_uri_params);
    return 0;

error_cleanup:
    aeron_uri_close(&aeron_uri_params);
    return -1;
}

int aeron_driver_conductor_on_add_network_publication_complete(
    aeron_driver_conductor_t *conductor,
    aeron_driver_async_command_t *async_command,
    void *on_execute_clientd)
{
    aeron_udp_channel_async_parse_t *async_parse = on_execute_clientd;
    aeron_udp_channel_t *udp_channel = async_parse->channel;
    aeron_publication_command_t *command = async_command->original_command;
    int64_t correlation_id = command->correlated.correlation_id;

    bool is_exclusive = async_command->is_exclusive;
    const char *uri = (const char *)command + sizeof(aeron_publication_command_t);
    size_t uri_length = (size_t)command->channel_length;

    aeron_driver_uri_publication_params_t params;

    if (aeron_diver_uri_publication_params(&udp_channel->uri, &params, conductor, is_exclusive) < 0 ||
        aeron_driver_conductor_validate_experimental_features(
            conductor->context->enable_experimental_features, udp_channel) < 0 ||
        aeron_driver_conductor_validate_endpoint_for_publication(udp_channel) < 0 ||
        aeron_driver_conductor_validate_control_for_publication(udp_channel) < 0 ||
        aeron_driver_conductor_validate_response_subscription(conductor, udp_channel, &params) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    aeron_client_t *client = aeron_driver_conductor_get_or_add_client(conductor, command->correlated.client_id);
    if (NULL == client)
    {
        AERON_APPEND_ERR("%s", "Failed to add client");
        goto error_cleanup;
    }

    aeron_publication_image_t *response_publication_image = NULL;
    if (aeron_driver_conductor_find_response_publication_image(
        conductor, udp_channel, &params, &response_publication_image) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    aeron_send_channel_endpoint_t *endpoint = aeron_driver_conductor_get_or_add_send_channel_endpoint(
        conductor, udp_channel, &params, correlation_id);
    if (NULL == endpoint)
    {
        AERON_APPEND_ERR("%s", "");
        udp_channel = NULL; // deleted by the previous method
        goto error_cleanup;
    }

    if (endpoint->conductor_fields.udp_channel != udp_channel)
    {
        aeron_udp_channel_delete(udp_channel);
    }
    udp_channel = NULL;

    if (AERON_SEND_CHANNEL_ENDPOINT_STATUS_ACTIVE != endpoint->conductor_fields.status)
    {
        AERON_SET_ERR(
            -AERON_ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE,
            "%s",
            "send_channel_endpoint found in CLOSING state, please retry");

        goto error_cleanup;
    }

    aeron_network_publication_t *publication = aeron_driver_conductor_get_or_add_network_publication(
        conductor,
        client,
        endpoint,
        uri_length,
        uri,
        &params,
        response_publication_image,
        correlation_id,
        command->stream_id,
        is_exclusive);

    if (NULL == publication)
    {
        AERON_APPEND_ERR("uri=%.*s", uri_length, uri);
        goto error_cleanup;
    }

    aeron_driver_conductor_on_publication_ready(
        conductor,
        correlation_id,
        publication->conductor_fields.managed_resource.registration_id,
        publication->stream_id,
        publication->session_id,
        publication->pub_lmt_position.counter_id,
        endpoint->channel_status.counter_id,
        is_exclusive,
        publication->log_file_name,
        publication->log_file_name_length);

    int64_t now_ns = aeron_clock_cached_nano_time(conductor->context->cached_clock);
    aeron_subscribable_t *subscribable = &publication->conductor_fields.subscribable;

    for (size_t i = 0; i < conductor->spy_subscriptions.length; i++)
    {
        aeron_subscription_link_t *subscription_link = &conductor->spy_subscriptions.array[i];

        if (aeron_driver_conductor_spy_subscription_link_matches(subscription_link, publication) &&
            !aeron_driver_conductor_is_subscribable_linked(subscription_link, subscribable))
        {
            if (aeron_driver_conductor_link_subscribable(
                conductor,
                subscription_link,
                &publication->conductor_fields.subscribable,
                publication->conductor_fields.managed_resource.registration_id,
                publication->session_id,
                publication->stream_id,
                aeron_network_publication_join_position(publication),
                now_ns,
                AERON_IPC_CHANNEL_LEN,
                AERON_IPC_CHANNEL,
                publication->log_file_name_length,
                publication->log_file_name) < 0)
            {
                goto error_cleanup;
            }
        }
    }

    return 0;

error_cleanup:
    aeron_udp_channel_delete(udp_channel);
    return -1;
}

int aeron_driver_conductor_on_add_network_publication(
    aeron_driver_conductor_t *conductor, aeron_publication_command_t *command, bool is_exclusive)
{
    aeron_driver_async_client_command_t *async_client_command;

    if (aeron_driver_async_client_command_allocate(
        &async_client_command,
        command,
        sizeof(aeron_publication_command_t) + command->channel_length) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (aeron_udp_channel_do_initial_parse(
        (size_t)command->channel_length,
        (const char *)command + sizeof(aeron_publication_command_t),
        &async_client_command->async_command.async_parse) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    async_client_command->async_command.async_parse.is_destination = false;

    async_client_command->async_command.is_exclusive = is_exclusive;

    async_client_command->correlated = &((aeron_publication_command_t *)async_client_command->async_command.original_command)->correlated;
    async_client_command->on_execute = aeron_driver_async_parse_udp_channel_execute;
    async_client_command->on_execute_clientd = &async_client_command->async_command.async_parse;
    async_client_command->on_complete = aeron_driver_conductor_on_add_network_publication_complete;

    if (aeron_driver_async_client_command_submit(conductor, async_client_command) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    return 0;

error_cleanup:
    aeron_free(async_client_command);

    return -1;
}

int aeron_driver_conductor_on_remove_publication(aeron_driver_conductor_t *conductor, aeron_remove_publication_command_t *command)
{
    int index = aeron_driver_conductor_find_client(conductor, command->correlated.client_id);
    if (index >= 0)
    {
        aeron_client_t *client = &conductor->clients.array[index];

        for (size_t i = 0, size = client->publication_links.length, last_index = size - 1; i < size; i++)
        {
            aeron_driver_managed_resource_t *resource = client->publication_links.array[i].resource;

            if (command->registration_id == client->publication_links.array[i].registration_id)
            {
                if (command->flags & AERON_COMMAND_REMOVE_PUBLICATION_FLAG_REVOKE)
                {
                    AERON_DRIVER_MANAGED_RESOURCE_REVOKE(resource);
                }
                AERON_DRIVER_MANAGED_RESOURCE_DECREF(resource);

                aeron_array_fast_unordered_remove(
                    (uint8_t *)client->publication_links.array, sizeof(aeron_publication_link_t), i, last_index);
                client->publication_links.length--;

                aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);
                return 0;
            }
        }
    }

    AERON_SET_ERR(
        -AERON_ERROR_CODE_UNKNOWN_PUBLICATION,
        "unknown publication client_id=%" PRId64 " registration_id=%" PRId64,
        command->correlated.client_id,
        command->registration_id);

    return -1;
}

int aeron_driver_conductor_on_add_ipc_subscription(
    aeron_driver_conductor_t *conductor, aeron_subscription_command_t *command)
{
    const char *uri = (const char *)command + sizeof(aeron_subscription_command_t);
    aeron_uri_t aeron_uri_params;
    aeron_driver_uri_subscription_params_t params;
    size_t uri_length = (size_t)command->channel_length;

    if (aeron_uri_parse(uri_length, uri, &aeron_uri_params) < 0 ||
        aeron_driver_uri_subscription_params(&aeron_uri_params, &params, conductor) < 0)
    {
        goto error_cleanup;
    }

    if (NULL == aeron_driver_conductor_get_or_add_client(conductor, command->correlated.client_id))
    {
        AERON_APPEND_ERR("%s", "Failed to add client");
        goto error_cleanup;
    }

    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, conductor->ipc_subscriptions, aeron_subscription_link_t)
    if (ensure_capacity_result < 0)
    {
        goto error_cleanup;
    }

    aeron_subscription_link_t *link = &conductor->ipc_subscriptions.array[conductor->ipc_subscriptions.length++];
    aeron_driver_init_subscription_channel(command->channel_length, uri, link);
    link->endpoint = NULL;
    link->spy_channel = NULL;
    link->stream_id = command->stream_id;
    link->has_session_id = params.has_session_id;
    link->session_id = params.session_id;
    link->client_id = command->correlated.client_id;
    link->registration_id = command->correlated.correlation_id;
    link->is_reliable = true;
    link->is_rejoin = true;
    link->is_response = false;
    link->group = AERON_INFER;
    link->is_sparse = params.is_sparse;
    link->is_tether = params.is_tether;
    link->subscribable_list.length = 0;
    link->subscribable_list.capacity = 0;
    link->subscribable_list.array = NULL;

    aeron_driver_conductor_on_subscription_ready(
        conductor, command->correlated.correlation_id, AERON_CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED);

    int64_t now_ns = aeron_clock_cached_nano_time(conductor->context->cached_clock);

    for (size_t i = 0; i < conductor->ipc_publications.length; i++)
    {
        aeron_ipc_publication_entry_t *publication_entry = &conductor->ipc_publications.array[i];
        aeron_ipc_publication_t *publication = publication_entry->publication;

        if (aeron_driver_conductor_subscription_link_matches_ipc_publication(link, publication) &&
            aeron_ipc_publication_is_accepting_subscriptions(publication))
        {
            if (aeron_driver_conductor_link_subscribable(
                conductor,
                link,
                &publication->conductor_fields.subscribable,
                publication->conductor_fields.managed_resource.registration_id,
                publication->session_id,
                publication->stream_id,
                aeron_ipc_publication_join_position(publication),
                now_ns,
                AERON_IPC_CHANNEL_LEN,
                AERON_IPC_CHANNEL,
                publication->log_file_name_length,
                publication->log_file_name) < 0)
            {
                goto error_cleanup;
            }
        }
    }

    aeron_uri_close(&aeron_uri_params);
    return 0;

error_cleanup:
    aeron_uri_close(&aeron_uri_params);
    return -1;
}

int aeron_driver_conductor_on_add_spy_subscription_complete(
    aeron_driver_conductor_t *conductor,
    aeron_driver_async_command_t *async_command,
    void *on_execute_clientd)
{
    aeron_subscription_command_t *command = async_command->original_command;
    aeron_udp_channel_async_parse_t *async_parse = on_execute_clientd;
    aeron_udp_channel_t *udp_channel = async_parse->channel;
    const char *uri = (const char *)command + sizeof(aeron_subscription_command_t);
    aeron_driver_uri_subscription_params_t params;

    if (aeron_driver_uri_subscription_params(&udp_channel->uri, &params, conductor) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (NULL == aeron_driver_conductor_get_or_add_client(conductor, command->correlated.client_id))
    {
        AERON_APPEND_ERR("%s", "Failed to add client");
        return -1;
    }

    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, conductor->spy_subscriptions, aeron_subscription_link_t)
    if (ensure_capacity_result < 0)
    {
        return -1;
    }

    aeron_subscription_link_t *link = &conductor->spy_subscriptions.array[conductor->spy_subscriptions.length++];
    aeron_driver_init_subscription_channel(command->channel_length, uri, link);
    link->endpoint = NULL;
    link->spy_channel = udp_channel;
    link->stream_id = command->stream_id;
    link->has_session_id = params.has_session_id;
    link->session_id = params.session_id;
    link->client_id = command->correlated.client_id;
    link->registration_id = command->correlated.correlation_id;
    link->is_reliable = params.is_reliable;
    link->is_sparse = params.is_sparse;
    link->is_tether = params.is_tether;
    link->is_rejoin = params.is_rejoin;
    link->is_response = false;
    link->group = AERON_INFER;
    link->subscribable_list.length = 0;
    link->subscribable_list.capacity = 0;
    link->subscribable_list.array = NULL;

    aeron_driver_conductor_on_subscription_ready(
        conductor, command->correlated.correlation_id, AERON_CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED);

    int64_t now_ns = aeron_clock_cached_nano_time(conductor->context->cached_clock);

    for (size_t i = 0, length = conductor->network_publications.length; i < length; i++)
    {
        aeron_network_publication_t *publication = conductor->network_publications.array[i].publication;

        if (aeron_driver_conductor_spy_subscription_link_matches(link, publication) &&
            aeron_network_publication_is_accepting_subscriptions(publication))
        {
            if (aeron_driver_conductor_link_subscribable(
                conductor,
                link,
                &publication->conductor_fields.subscribable,
                publication->conductor_fields.managed_resource.registration_id,
                publication->session_id,
                publication->stream_id,
                aeron_network_publication_join_position(publication),
                now_ns,
                AERON_IPC_CHANNEL_LEN,
                AERON_IPC_CHANNEL,
                publication->log_file_name_length,
                publication->log_file_name) < 0)
            {
                return -1;
            }
        }
    }

    return 0;
}

int aeron_driver_conductor_on_add_spy_subscription(
    aeron_driver_conductor_t *conductor, aeron_subscription_command_t *command)
{
    aeron_driver_async_client_command_t *async_client_command;

    if (aeron_driver_async_client_command_allocate(
        &async_client_command,
        command,
        sizeof(aeron_subscription_command_t) + command->channel_length) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (aeron_udp_channel_do_initial_parse(
        (size_t)command->channel_length - strlen(AERON_SPY_PREFIX),
        (const char *)command + sizeof(aeron_subscription_command_t) + strlen(AERON_SPY_PREFIX),
        &async_client_command->async_command.async_parse) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    async_client_command->async_command.async_parse.is_destination = false;

    async_client_command->correlated = &((aeron_subscription_command_t *)async_client_command->async_command.original_command)->correlated;
    async_client_command->on_execute = aeron_driver_async_parse_udp_channel_execute;
    async_client_command->on_execute_clientd = &async_client_command->async_command.async_parse;
    async_client_command->on_complete = aeron_driver_conductor_on_add_spy_subscription_complete;

    if (aeron_driver_async_client_command_submit(conductor, async_client_command) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    return 0;

error_cleanup:
    aeron_free(async_client_command);

    return -1;
}

int aeron_driver_conductor_add_network_subscription_to_receiver(
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id,
    bool has_session_id,
    int32_t session_id)
{
    if (has_session_id)
    {
        if (aeron_receive_channel_endpoint_incref_to_stream_and_session(endpoint, stream_id, session_id) < 0)
        {
            return -1;
        }
    }
    else
    {
        if (aeron_receive_channel_endpoint_incref_to_stream(endpoint, stream_id) < 0)
        {
            return -1;
        }
    }

    return 0;
}

int aeron_driver_conductor_on_add_network_subscription_complete(
    aeron_driver_conductor_t *conductor,
    aeron_driver_async_command_t *async_command,
    void *on_execute_clientd)
{
    aeron_subscription_command_t *command = async_command->original_command;
    aeron_udp_channel_async_parse_t *async_parse = on_execute_clientd;
    aeron_udp_channel_t *udp_channel = async_parse->channel;
    int64_t correlation_id = command->correlated.correlation_id;

    const char *uri = (const char *)command + sizeof(aeron_subscription_command_t);

    aeron_driver_uri_subscription_params_t params;
    if (aeron_driver_uri_subscription_params(&udp_channel->uri, &params, conductor) < 0 ||
        aeron_driver_conductor_validate_experimental_features(
            conductor->context->enable_experimental_features,
            udp_channel) < 0 ||
        aeron_driver_conductor_validate_control_for_subscription(udp_channel) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }
    const aeron_udp_channel_control_mode control_mode = udp_channel->control_mode;

    if (aeron_driver_conductor_has_clashing_subscription(conductor, udp_channel, command->stream_id, &params))
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    aeron_receive_channel_endpoint_t *endpoint = aeron_driver_conductor_get_or_add_receive_channel_endpoint(
        conductor, &params, udp_channel, correlation_id);
    if (NULL == endpoint)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup_skip_channel_delete;
    }

    if (endpoint->conductor_fields.udp_channel != udp_channel)
    {
        aeron_udp_channel_delete(udp_channel);
    }
    // Ownership is transferred to the channel.
    udp_channel = NULL;

    if (AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_ACTIVE != endpoint->conductor_fields.status)
    {
        AERON_SET_ERR(
            -AERON_ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE,
            "%s",
            "receive_channel_endpoint found in CLOSING state, please retry");
        goto error_cleanup_skip_channel_delete;
    }

    if (NULL == aeron_driver_conductor_get_or_add_client(conductor, command->correlated.client_id))
    {
        AERON_APPEND_ERR("%s", "Failed to add client");
        goto error_cleanup_skip_channel_delete;
    }

    if (AERON_UDP_CHANNEL_CONTROL_MODE_RESPONSE == control_mode)
    {
        if (aeron_receive_channel_endpoint_incref_to_response_stream(endpoint, command->stream_id) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error_cleanup_skip_channel_delete;
        }
    }
    else
    {
        if (aeron_driver_conductor_add_network_subscription_to_receiver(
            endpoint, command->stream_id, params.has_session_id, params.session_id) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error_cleanup_skip_channel_delete;
        }
    }

    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, conductor->network_subscriptions, aeron_subscription_link_t)

    if (ensure_capacity_result >= 0)
    {
        aeron_subscription_link_t *link =
            &conductor->network_subscriptions.array[conductor->network_subscriptions.length++];

        aeron_driver_init_subscription_channel(command->channel_length, uri, link);
        link->endpoint = endpoint;
        link->spy_channel = NULL;
        link->stream_id = command->stream_id;
        link->has_session_id = params.has_session_id;
        link->session_id = params.session_id;
        link->client_id = command->correlated.client_id;
        link->registration_id = correlation_id;
        link->is_reliable = params.is_reliable;
        link->is_sparse = params.is_sparse;
        link->is_tether = params.is_tether;
        link->is_rejoin = params.is_rejoin;
        link->is_response = params.is_response;
        link->group = params.group;
        link->subscribable_list.length = 0;
        link->subscribable_list.capacity = 0;
        link->subscribable_list.array = NULL;

        aeron_driver_conductor_on_subscription_ready(
            conductor, correlation_id, endpoint->channel_status.counter_id);

        int64_t now_ns = aeron_clock_cached_nano_time(conductor->context->cached_clock);

        for (size_t i = 0, length = conductor->publication_images.length; i < length; i++)
        {
            aeron_publication_image_t *image = conductor->publication_images.array[i].image;

            if (aeron_driver_conductor_network_subscription_link_matches_allowing_wildcard(
                link, image->conductor_fields.endpoint, image->stream_id, image->session_id) &&
                aeron_publication_image_is_accepting_subscriptions(image))
            {
                if (aeron_driver_conductor_link_subscribable(
                    conductor,
                    link,
                    &image->conductor_fields.subscribable,
                    image->conductor_fields.managed_resource.registration_id,
                    image->session_id,
                    image->stream_id,
                    aeron_publication_image_join_position(image),
                    now_ns,
                    image->source_identity_length,
                    image->source_identity,
                    image->log_file_name_length,
                    image->log_file_name) < 0)
                {
                    AERON_APPEND_ERR("%s", "");
                    goto error_cleanup_skip_channel_delete;
                }
            }
        }

        return 0;
    }

error_cleanup:
    aeron_udp_channel_delete(udp_channel);

error_cleanup_skip_channel_delete:
    return -1;
}

int aeron_driver_conductor_on_add_network_subscription(
    aeron_driver_conductor_t *conductor, aeron_subscription_command_t *command)
{
    aeron_driver_async_client_command_t *async_client_command;

    if (aeron_driver_async_client_command_allocate(
        &async_client_command,
        command,
        sizeof(aeron_subscription_command_t) + command->channel_length) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (aeron_udp_channel_do_initial_parse(
        (size_t)command->channel_length,
        (const char *)command + sizeof(aeron_subscription_command_t),
        &async_client_command->async_command.async_parse) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    async_client_command->async_command.async_parse.is_destination = false;

    async_client_command->correlated = &((aeron_subscription_command_t *)async_client_command->async_command.original_command)->correlated;
    async_client_command->on_execute = aeron_driver_async_parse_udp_channel_execute;
    async_client_command->on_execute_clientd = &async_client_command->async_command.async_parse;
    async_client_command->on_complete = aeron_driver_conductor_on_add_network_subscription_complete;

    if (aeron_driver_async_client_command_submit(conductor, async_client_command) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    return 0;

error_cleanup:
    aeron_free(async_client_command);

    return -1;
}

int aeron_driver_conductor_on_remove_subscription(
    aeron_driver_conductor_t *conductor, aeron_remove_subscription_command_t *command)
{
    bool is_any_subscription_found = false;

    for (int last_index = (int32_t)conductor->ipc_subscriptions.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_subscription_link_t *link = &conductor->ipc_subscriptions.array[i];

        if (command->registration_id == link->registration_id)
        {
            aeron_driver_conductor_unlink_all_subscribable(conductor, link);

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->ipc_subscriptions.array, sizeof(aeron_subscription_link_t), i, last_index);
            conductor->ipc_subscriptions.length--;
            last_index--;
            is_any_subscription_found = true;
        }
    }

    for (int last_index = (int32_t)conductor->network_subscriptions.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_subscription_link_t *link = &conductor->network_subscriptions.array[i];

        if (command->registration_id == link->registration_id)
        {
            aeron_driver_conductor_unlink_from_endpoint(conductor, link);

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->network_subscriptions.array, sizeof(aeron_subscription_link_t), i, last_index);
            conductor->network_subscriptions.length--;
            last_index--;
            is_any_subscription_found = true;
        }
    }

    for (int last_index = (int32_t)conductor->spy_subscriptions.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_subscription_link_t *link = &conductor->spy_subscriptions.array[i];

        if (command->registration_id == link->registration_id)
        {
            aeron_driver_conductor_unlink_all_subscribable(conductor, link);

            aeron_udp_channel_delete(link->spy_channel);
            link->spy_channel = NULL;
            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->spy_subscriptions.array, sizeof(aeron_subscription_link_t), i, last_index);
            conductor->spy_subscriptions.length--;
            last_index--;
            is_any_subscription_found = true;
        }
    }

    if (!is_any_subscription_found)
    {
        AERON_SET_ERR(
            -AERON_ERROR_CODE_UNKNOWN_SUBSCRIPTION,
            "unknown subscription client_id=%" PRId64 " registration_id=%" PRId64,
            command->correlated.client_id,
            command->registration_id);

        return -1;
    }

    aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);
    return 0;
}

int aeron_driver_conductor_on_client_keepalive(aeron_driver_conductor_t *conductor, int64_t client_id)
{
    int index = aeron_driver_conductor_find_client(conductor, client_id);
    if (index >= 0)
    {
        aeron_client_t *client = &conductor->clients.array[index];
        int64_t now_ms = aeron_clock_cached_epoch_time(conductor->context->cached_clock);
        aeron_counter_set_release(client->heartbeat_timestamp.value_addr, now_ms);
    }

    return 0;
}

int aeron_driver_conductor_on_add_send_destination_complete(
    aeron_driver_conductor_t *conductor,
    aeron_driver_async_command_t *async_command,
    void *on_execute_clientd)
{
    aeron_destination_command_t *command = async_command->original_command;
    aeron_name_resolver_async_resolve_t *async_resolve = on_execute_clientd;

    aeron_driver_sender_proxy_on_add_destination(
        conductor->context->sender_proxy,
        async_command->endpoint,
        async_command->uri,
        &async_resolve->sockaddr,
        command->correlated.correlation_id);

    aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);

    return 0;
}

int aeron_driver_conductor_on_add_send_destination_error(
    aeron_driver_conductor_t *conductor,
    int result,
    aeron_driver_async_command_t *async_command,
    void *on_execute_clientd)
{
    aeron_name_resolver_async_resolve_t *async_resolve = (aeron_name_resolver_async_resolve_t *)on_execute_clientd;

    memset(&async_resolve->sockaddr, 0, sizeof(async_resolve->sockaddr));
    async_resolve->sockaddr.ss_family = AF_UNSPEC;

    return aeron_driver_conductor_on_add_send_destination_complete(conductor, async_command, on_execute_clientd);
}

int aeron_driver_conductor_on_add_send_destination(
    aeron_driver_conductor_t *conductor, aeron_destination_command_t *command)
{
    aeron_send_channel_endpoint_t *endpoint = NULL;

    for (size_t i = 0, length = conductor->network_publications.length; i < length; i++)
    {
        aeron_network_publication_t *publication = conductor->network_publications.array[i].publication;

        if (command->registration_id == publication->conductor_fields.managed_resource.registration_id)
        {
            endpoint = publication->endpoint;
            break;
        }
    }

    if (NULL == endpoint)
    {
        AERON_SET_ERR(
            -AERON_ERROR_CODE_UNKNOWN_PUBLICATION,
            "unknown add destination client_id=%" PRId64 " registration_id=%" PRId64,
            command->correlated.client_id,
            command->registration_id);

        return -1;
    }

    aeron_uri_t *uri = NULL;
    const char *command_uri = (const char *)command + sizeof(aeron_destination_command_t);

    if (aeron_driver_conductor_validate_destination_uri_prefix(command_uri, command->channel_length, "send") < 0)
    {
        goto error_cleanup;
    }

    if (aeron_alloc((void **)&uri, sizeof(aeron_uri_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to allocate uri");
        goto error_cleanup;
    }

    size_t uri_length = (size_t)command->channel_length;
    if (aeron_uri_parse(uri_length, command_uri, uri) < 0 ||
        aeron_driver_conductor_validate_send_destination_uri(uri, uri_length) < 0)
    {
        goto error_cleanup;
    }

    if (aeron_driver_conductor_validate_destination_uri_params(uri, uri_length) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    if (NULL == endpoint->destination_tracker || !endpoint->destination_tracker->is_manual_control_mode)
    {
        AERON_SET_ERR(
            EINVAL,
            "channel does not allow manual control of destinations: %.*s",
            command->channel_length, command_uri);
        goto error_cleanup;
    }

    if (uri->type != AERON_URI_UDP || NULL == uri->params.udp.endpoint)
    {
        AERON_SET_ERR(EINVAL, "incorrect URI format for destination: %.*s", command->channel_length, command_uri);
        goto error_cleanup;
    }

    aeron_driver_async_client_command_t *async_client_command;

    if (aeron_driver_async_client_command_allocate(
        &async_client_command,
        command,
        sizeof(aeron_destination_command_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    async_client_command->async_command.async_resolve.uri_param_name = AERON_UDP_CHANNEL_ENDPOINT_KEY;
    async_client_command->async_command.async_resolve.is_re_resolution = false;
    COPY_ENDPOINT_NAME(async_client_command->async_command.async_resolve.endpoint_name, uri->params.udp.endpoint);

    // TODO instead of storing the pointer, should we be looking this up again in the on_complete callback?
    async_client_command->async_command.endpoint = endpoint;
    async_client_command->async_command.uri = uri;

    async_client_command->correlated = &((aeron_destination_command_t *)async_client_command->async_command.original_command)->correlated;
    async_client_command->on_execute = aeron_driver_async_resolve_execute;
    async_client_command->on_execute_clientd = &async_client_command->async_command.async_resolve;
    async_client_command->on_complete = aeron_driver_conductor_on_add_send_destination_complete;
    async_client_command->on_error = aeron_driver_conductor_on_add_send_destination_error;

    if (aeron_driver_async_client_command_submit(conductor, async_client_command) < 0)
    {
        aeron_free(async_client_command);
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    return 0;

error_cleanup:
    aeron_uri_close(uri);
    aeron_free(uri);
    return -1;
}

int aeron_driver_conductor_on_remove_send_destination_complete(
    aeron_driver_conductor_t *conductor,
    aeron_driver_async_command_t *async_command,
    void *on_execute_clientd)
{
    aeron_destination_command_t *command = async_command->original_command;
    aeron_name_resolver_async_resolve_t *async_resolve = on_execute_clientd;

    aeron_driver_sender_proxy_on_remove_destination(
        conductor->context->sender_proxy, async_command->endpoint, &async_resolve->sockaddr);
    aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);

    return 0;
}

int aeron_driver_conductor_on_remove_send_destination(
    aeron_driver_conductor_t *conductor, aeron_destination_command_t *command)
{
    aeron_send_channel_endpoint_t *endpoint = NULL;

    for (size_t i = 0, length = conductor->network_publications.length; i < length; i++)
    {
        aeron_network_publication_t *publication = conductor->network_publications.array[i].publication;

        if (command->registration_id == publication->conductor_fields.managed_resource.registration_id)
        {
            endpoint = publication->endpoint;
            break;
        }
    }

    if (NULL == endpoint)
    {
        AERON_SET_ERR(
            -AERON_ERROR_CODE_UNKNOWN_PUBLICATION,
            "unknown remove destination, client_id=%" PRId64 " registration_id=%" PRId64,
            command->correlated.client_id,
            command->registration_id);

        return -1;
    }

    int rc = -1;
    aeron_uri_t uri_params;
    const char *command_uri = (const char *)command + sizeof(aeron_destination_command_t);
    size_t uri_length = (size_t)command->channel_length;
    if (aeron_uri_parse(uri_length, command_uri, &uri_params) < 0)
    {
        goto cleanup;
    }

    if (NULL == endpoint->destination_tracker || !endpoint->destination_tracker->is_manual_control_mode)
    {
        AERON_SET_ERR(
            EINVAL,
            "channel does not allow manual control of destinations: %.*s",
            command->channel_length, command_uri);
        goto cleanup;
    }

    if (uri_params.type != AERON_URI_UDP || NULL == uri_params.params.udp.endpoint)
    {
        AERON_SET_ERR(EINVAL, "incorrect URI format for destination: %.*s", command->channel_length, command_uri);
        goto cleanup;
    }

    aeron_driver_async_client_command_t *async_client_command;

    if (aeron_driver_async_client_command_allocate(
        &async_client_command,
        command,
        sizeof(aeron_destination_command_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto cleanup;
    }

    async_client_command->async_command.async_resolve.uri_param_name = AERON_UDP_CHANNEL_ENDPOINT_KEY;
    async_client_command->async_command.async_resolve.is_re_resolution = true;
    COPY_ENDPOINT_NAME(async_client_command->async_command.async_resolve.endpoint_name, uri_params.params.udp.endpoint);

    async_client_command->async_command.endpoint = endpoint;
    async_client_command->async_command.uri = NULL;

    async_client_command->correlated = &((aeron_destination_command_t *)async_client_command->async_command.original_command)->correlated;
    async_client_command->on_execute = aeron_driver_async_resolve_execute;
    async_client_command->on_execute_clientd = &async_client_command->async_command.async_resolve;
    async_client_command->on_complete = aeron_driver_conductor_on_remove_send_destination_complete;

    if (aeron_driver_async_client_command_submit(conductor, async_client_command) < 0)
    {
        aeron_free(async_client_command);
        AERON_APPEND_ERR("%s", "");
        goto cleanup;
    }

    rc = 0;

cleanup:
    aeron_uri_close(&uri_params);
    return rc;
}

int aeron_driver_conductor_on_remove_receive_send_destination_by_id(
    aeron_driver_conductor_t *conductor, aeron_destination_by_id_command_t *command)
{
    int rc = -1;
    aeron_send_channel_endpoint_t *endpoint = NULL;

    for (size_t i = 0, length = conductor->network_publications.length; i < length; i++)
    {
        aeron_network_publication_t *publication = conductor->network_publications.array[i].publication;

        if (command->resource_registration_id == publication->conductor_fields.managed_resource.registration_id)
        {
            endpoint = publication->endpoint;
            break;
        }
    }

    if (NULL == endpoint)
    {
        AERON_SET_ERR(
            -AERON_ERROR_CODE_UNKNOWN_PUBLICATION,
            "unknown remove destination, client_id=%" PRId64 " registration_id=%" PRId64,
            command->correlated.client_id,
            command->resource_registration_id);

        goto cleanup;
    }

    if (NULL == endpoint->destination_tracker || !endpoint->destination_tracker->is_manual_control_mode)
    {
        AERON_SET_ERR(
            EINVAL,
            "channel does not allow manual control of destinations: %" PRId64,
            command->resource_registration_id);
        goto cleanup;
    }

    aeron_driver_sender_proxy_on_remove_destination_by_id(
        conductor->context->sender_proxy, endpoint, command->destination_registration_id);
    aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);

    rc = 0;

cleanup:
    return rc;
}

aeron_subscription_link_t *aeron_driver_conductor_find_mds_subscription(
    aeron_driver_conductor_t *conductor, int64_t client_id, int64_t registration_id)
{
    aeron_subscription_link_t *mds_subscription_link = NULL;

    for (size_t i = 0, size = conductor->network_subscriptions.length; i < size; i++)
    {
        aeron_subscription_link_t *link = &conductor->network_subscriptions.array[i];

        if (registration_id == link->registration_id)
        {
            mds_subscription_link = link;
            break;
        }
    }

    if (NULL == mds_subscription_link)
    {
        AERON_SET_ERR(
            -AERON_ERROR_CODE_UNKNOWN_SUBSCRIPTION,
            "unknown subscription client_id=%" PRId64 " registration_id=%" PRId64,
            client_id,
            registration_id);

        return NULL;
    }

    if (AERON_UDP_CHANNEL_CONTROL_MODE_MANUAL != mds_subscription_link->endpoint->conductor_fields.udp_channel->control_mode)
    {
        AERON_SET_ERR(-AERON_ERROR_CODE_INVALID_CHANNEL, "%s", "channel does not allow manual control");
        return NULL;
    }

    return mds_subscription_link;
}

int aeron_driver_conductor_on_add_receive_ipc_destination(
    aeron_driver_conductor_t *conductor, aeron_destination_command_t *command)
{
    aeron_subscription_link_t *mds_subscription_link = NULL;
    const char *command_uri = (const char *)command + sizeof(aeron_destination_command_t);
    aeron_uri_t aeron_uri_params;
    aeron_driver_uri_subscription_params_t params;
    size_t uri_length = (size_t)command->channel_length;

    if (aeron_uri_parse(uri_length, command_uri, &aeron_uri_params) < 0 ||
        aeron_driver_uri_subscription_params(&aeron_uri_params, &params, conductor) < 0)
    {
        goto error_cleanup;
    }

    mds_subscription_link = aeron_driver_conductor_find_mds_subscription(
        conductor, command->correlated.client_id, command->registration_id);
    if (NULL == mds_subscription_link)
    {
        goto error_cleanup;
    }

    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, conductor->ipc_subscriptions, aeron_subscription_link_t)
    if (ensure_capacity_result < 0)
    {
        goto error_cleanup;
    }

    aeron_subscription_link_t *link = &conductor->ipc_subscriptions.array[conductor->ipc_subscriptions.length++];
    aeron_driver_init_subscription_channel(command->channel_length, command_uri, link);
    link->endpoint = NULL;
    link->spy_channel = NULL;
    link->stream_id = mds_subscription_link->stream_id;
    link->has_session_id = params.has_session_id;
    link->session_id = params.session_id;
    link->client_id = command->correlated.client_id;
    link->registration_id = mds_subscription_link->registration_id;
    link->is_reliable = true;
    link->is_rejoin = true;
    link->is_response = false;
    link->group = AERON_INFER;
    link->is_sparse = params.is_sparse;
    link->is_tether = params.is_tether;
    link->subscribable_list.length = 0;
    link->subscribable_list.capacity = 0;
    link->subscribable_list.array = NULL;

    aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);

    int64_t now_ns = aeron_clock_cached_nano_time(conductor->context->cached_clock);

    for (size_t i = 0; i < conductor->ipc_publications.length; i++)
    {
        aeron_ipc_publication_entry_t *publication_entry = &conductor->ipc_publications.array[i];
        aeron_ipc_publication_t *publication = publication_entry->publication;

        if (aeron_driver_conductor_subscription_link_matches_ipc_publication(link, publication) &&
            aeron_ipc_publication_is_accepting_subscriptions(publication))
        {
            if (aeron_driver_conductor_link_subscribable(
                conductor,
                link,
                &publication->conductor_fields.subscribable,
                publication->conductor_fields.managed_resource.registration_id,
                publication->session_id,
                publication->stream_id,
                aeron_ipc_publication_join_position(publication),
                now_ns,
                AERON_IPC_CHANNEL_LEN,
                AERON_IPC_CHANNEL,
                publication->log_file_name_length,
                publication->log_file_name) < 0)
            {
                goto error_cleanup;
            }
        }
    }

    aeron_uri_close(&aeron_uri_params);
    return 0;

error_cleanup:
    aeron_uri_close(&aeron_uri_params);
    return -1;
}

int aeron_driver_conductor_on_add_receive_spy_destination_complete(
    aeron_driver_conductor_t *conductor,
    aeron_driver_async_command_t *async_command,
    void *on_execute_clientd)
{
    aeron_destination_command_t *command = async_command->original_command;
    aeron_udp_channel_async_parse_t *async_parse = on_execute_clientd;
    aeron_udp_channel_t *udp_channel = async_parse->channel;

    aeron_subscription_link_t *mds_subscription_link = NULL;
    const char *command_uri = (const char *)command + sizeof(aeron_destination_command_t);
    aeron_driver_uri_subscription_params_t params;

    if (aeron_driver_uri_subscription_params(&udp_channel->uri, &params, conductor) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    mds_subscription_link = aeron_driver_conductor_find_mds_subscription(
        conductor, command->correlated.client_id, command->registration_id);
    if (NULL == mds_subscription_link)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, conductor->spy_subscriptions, aeron_subscription_link_t)
    if (ensure_capacity_result < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    aeron_subscription_link_t *link = &conductor->spy_subscriptions.array[conductor->spy_subscriptions.length++];
    aeron_driver_init_subscription_channel(command->channel_length, command_uri, link);
    link->endpoint = NULL;
    link->spy_channel = udp_channel;
    link->stream_id = mds_subscription_link->stream_id;
    link->has_session_id = params.has_session_id;
    link->session_id = params.session_id;
    link->client_id = command->correlated.client_id;
    link->registration_id = mds_subscription_link->registration_id;
    link->is_reliable = params.is_reliable;
    link->is_sparse = params.is_sparse;
    link->is_tether = params.is_tether;
    link->is_rejoin = params.is_rejoin;
    link->is_response = false;
    link->group = AERON_INFER;
    link->subscribable_list.length = 0;
    link->subscribable_list.capacity = 0;
    link->subscribable_list.array = NULL;

    aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);

    int64_t now_ns = aeron_clock_cached_nano_time(conductor->context->cached_clock);

    for (size_t i = 0, length = conductor->network_publications.length; i < length; i++)
    {
        aeron_network_publication_t *publication = conductor->network_publications.array[i].publication;

        if (aeron_driver_conductor_spy_subscription_link_matches(link, publication) &&
            aeron_network_publication_is_accepting_subscriptions(publication))
        {
            if (aeron_driver_conductor_link_subscribable(
                conductor,
                link,
                &publication->conductor_fields.subscribable,
                publication->conductor_fields.managed_resource.registration_id,
                publication->session_id,
                publication->stream_id,
                aeron_network_publication_join_position(publication),
                now_ns,
                AERON_IPC_CHANNEL_LEN,
                AERON_IPC_CHANNEL,
                publication->log_file_name_length,
                publication->log_file_name) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                goto error_cleanup_skip_channel_delete;
            }
        }
    }

    return 0;

error_cleanup:
    aeron_udp_channel_delete(udp_channel);

error_cleanup_skip_channel_delete:
    return -1;
}

int aeron_driver_conductor_on_add_receive_spy_destination(
    aeron_driver_conductor_t *conductor, aeron_destination_command_t *command)
{
    aeron_driver_async_client_command_t *async_client_command;

    if (aeron_driver_async_client_command_allocate(
        &async_client_command,
        command,
        sizeof(aeron_destination_command_t) + command->channel_length) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (aeron_udp_channel_do_initial_parse(
        (size_t)command->channel_length - strlen(AERON_SPY_PREFIX),
        (const char *)command + sizeof(aeron_destination_command_t) + strlen(AERON_SPY_PREFIX),
        &async_client_command->async_command.async_parse) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    async_client_command->async_command.async_parse.is_destination = true;

    async_client_command->correlated = &((aeron_destination_command_t *)async_client_command->async_command.original_command)->correlated;
    async_client_command->on_execute = aeron_driver_async_parse_udp_channel_execute;
    async_client_command->on_execute_clientd = &async_client_command->async_command.async_parse;
    async_client_command->on_complete = aeron_driver_conductor_on_add_receive_spy_destination_complete;

    if (aeron_driver_async_client_command_submit(conductor, async_client_command) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    return 0;

error_cleanup:
    aeron_free(async_client_command);

    return -1;
}

int aeron_driver_conductor_on_add_receive_network_destination_complete(
    aeron_driver_conductor_t *conductor,
    aeron_driver_async_command_t *async_command,
    void *on_execute_clientd)
{
    aeron_destination_command_t *command = async_command->original_command;
    aeron_udp_channel_async_parse_t *async_parse = on_execute_clientd;
    aeron_udp_channel_t *udp_channel = async_parse->channel;

    aeron_subscription_link_t *mds_subscription_link = NULL;
    aeron_receive_channel_endpoint_t *endpoint = NULL;

    mds_subscription_link = aeron_driver_conductor_find_mds_subscription(
        conductor, command->correlated.client_id, command->registration_id);
    if (NULL == mds_subscription_link)
    {
        goto error_cleanup;
    }

    endpoint = mds_subscription_link->endpoint;

    if (aeron_driver_conductor_validate_destination_uri_params(&udp_channel->uri, udp_channel->uri_length) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    if (aeron_driver_conductor_update_and_check_ats_status(conductor->context, udp_channel, NULL) < 0)
    {
        goto error_cleanup;
    }

    aeron_receive_destination_t *destination = NULL;

    if (aeron_receive_destination_create(
        &destination,
        udp_channel,
        endpoint->conductor_fields.udp_channel,
        conductor->context,
        &conductor->counters_manager,
        command->registration_id,
        endpoint->channel_status.counter_id) < 0)
    {
        goto error_cleanup;
    }

    udp_channel = NULL;

    aeron_driver_receiver_proxy_on_add_destination(conductor->context->receiver_proxy, endpoint, destination);
    aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);

    return 0;

error_cleanup:
    aeron_udp_channel_delete(udp_channel);

    return -1;
}

int aeron_driver_conductor_on_add_receive_network_destination(
    aeron_driver_conductor_t *conductor, aeron_destination_command_t *command)
{
    aeron_driver_async_client_command_t *async_client_command;

    if (aeron_driver_async_client_command_allocate(
        &async_client_command,
        command,
        sizeof(aeron_destination_command_t) + command->channel_length) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (aeron_udp_channel_do_initial_parse(
        (size_t)command->channel_length,
        (const char *)command + sizeof(aeron_destination_command_t),
        &async_client_command->async_command.async_parse) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    async_client_command->async_command.async_parse.is_destination = true;

    async_client_command->correlated = &((aeron_destination_command_t *)async_client_command->async_command.original_command)->correlated;
    async_client_command->on_execute = aeron_driver_async_parse_udp_channel_execute;
    async_client_command->on_execute_clientd = &async_client_command->async_command.async_parse;
    async_client_command->on_complete = aeron_driver_conductor_on_add_receive_network_destination_complete;

    if (aeron_driver_async_client_command_submit(conductor, async_client_command) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    return 0;

error_cleanup:
    aeron_free(async_client_command);

    return -1;
}

void aeron_driver_conductor_subscription_link_notify_unavailable_images(
    aeron_driver_conductor_t *conductor, aeron_subscription_link_t *link)
{
    for (size_t i = 0; i < link->subscribable_list.length; i++)
    {
        aeron_subscribable_list_entry_t *entry = &link->subscribable_list.array[i];

        aeron_driver_conductor_on_unavailable_image(
            conductor,
            entry->subscribable->correlation_id,
            link->registration_id,
            link->stream_id,
            link->channel,
            link->channel_length);
    }
}

int aeron_driver_conductor_on_remove_receive_ipc_destination(
    aeron_driver_conductor_t *conductor, aeron_destination_command_t *command)
{
    const char *command_uri = (const char *)command + sizeof(aeron_destination_command_t);
    aeron_subscription_link_t *subscription_link = NULL;

    for (size_t i = 0, size = conductor->ipc_subscriptions.length, last_index = size - 1; i < size; i++)
    {
        aeron_subscription_link_t *link = &conductor->ipc_subscriptions.array[i];

        if (command->registration_id == link->registration_id &&
            link->channel_length == command->channel_length &&
            strncmp(link->channel, command_uri, link->channel_length) == 0)
        {
            aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);
            aeron_driver_conductor_subscription_link_notify_unavailable_images(conductor, link);
            aeron_driver_conductor_unlink_all_subscribable(conductor, link);

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->ipc_subscriptions.array, sizeof(aeron_subscription_link_t), i, last_index);
            conductor->ipc_subscriptions.length--;
            subscription_link = link;
            break;
        }
    }

    if (NULL == subscription_link)
    {
        AERON_SET_ERR(
            -AERON_ERROR_CODE_UNKNOWN_SUBSCRIPTION,
            "unknown subscription client_id=%" PRId64 " registration_id=%" PRId64,
            command->correlated.client_id,
            command->registration_id);

        return -1;
    }

    return 0;
}

int aeron_driver_conductor_on_remove_receive_spy_destination(
    aeron_driver_conductor_t *conductor, aeron_destination_command_t *command)
{
    const char *command_uri = (const char *)command + sizeof(aeron_destination_command_t);
    aeron_subscription_link_t *subscription_link = NULL;

    for (size_t i = 0, size = conductor->spy_subscriptions.length, last_index = size - 1; i < size; i++)
    {
        aeron_subscription_link_t *link = &conductor->spy_subscriptions.array[i];

        if (command->registration_id == link->registration_id &&
            link->channel_length == command->channel_length &&
            strncmp(link->channel, command_uri, link->channel_length) == 0)
        {
            aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);
            aeron_driver_conductor_subscription_link_notify_unavailable_images(conductor, link);

            aeron_driver_conductor_unlink_all_subscribable(conductor, link);
            aeron_udp_channel_delete(link->spy_channel);
            link->spy_channel = NULL;

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->spy_subscriptions.array, sizeof(aeron_subscription_link_t), i, last_index);
            conductor->spy_subscriptions.length--;
            subscription_link = link;
            break;
        }
    }

    if (NULL == subscription_link)
    {
        AERON_SET_ERR(
            -AERON_ERROR_CODE_UNKNOWN_SUBSCRIPTION,
            "unknown subscription client_id=%" PRId64 " registration_id=%" PRId64,
            command->correlated.client_id,
            command->registration_id);

        return -1;
    }

    return 0;
}

int aeron_driver_conductor_on_remove_receive_network_destination_complete(
    aeron_driver_conductor_t *conductor,
    aeron_driver_async_command_t *async_command,
    void *on_execute_clientd)
{
    aeron_destination_command_t *command = async_command->original_command;
    aeron_udp_channel_async_parse_t *async_parse = on_execute_clientd;
    aeron_udp_channel_t *udp_channel = async_parse->channel;
    aeron_subscription_link_t *mds_subscription_link = NULL;
    aeron_receive_channel_endpoint_t *endpoint = NULL;

    mds_subscription_link = aeron_driver_conductor_find_mds_subscription(
        conductor, command->correlated.client_id, command->registration_id);
    if (NULL == mds_subscription_link)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    endpoint = mds_subscription_link->endpoint;

    aeron_driver_receiver_proxy_on_remove_destination(conductor->context->receiver_proxy, endpoint, udp_channel);
    aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);

    return 0;

error_cleanup:
    aeron_udp_channel_delete(udp_channel);
    return -1;
}

int aeron_driver_conductor_on_remove_receive_network_destination(
    aeron_driver_conductor_t *conductor, aeron_destination_command_t *command)
{
    aeron_driver_async_client_command_t *async_client_command;

    if (aeron_driver_async_client_command_allocate(
        &async_client_command,
        command,
        sizeof(aeron_destination_command_t) + command->channel_length) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (aeron_udp_channel_do_initial_parse(
        (size_t)command->channel_length,
        (const char *)command + sizeof(aeron_destination_command_t),
        &async_client_command->async_command.async_parse) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    async_client_command->async_command.async_parse.is_destination = true;

    async_client_command->correlated = &((aeron_destination_command_t *)async_client_command->async_command.original_command)->correlated;
    async_client_command->on_execute = aeron_driver_async_parse_udp_channel_execute;
    async_client_command->on_execute_clientd = &async_client_command->async_command.async_parse;
    async_client_command->on_complete = aeron_driver_conductor_on_remove_receive_network_destination_complete;

    if (aeron_driver_async_client_command_submit(conductor, async_client_command) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_cleanup;
    }

    return 0;

error_cleanup:
    aeron_free(async_client_command);

    return -1;
}

void aeron_driver_conductor_on_delete_receive_destination(void *clientd, void *item)
{
    aeron_driver_conductor_t *conductor = (aeron_driver_conductor_t *)clientd;
    aeron_command_delete_destination_t *command = (aeron_command_delete_destination_t *)item;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)command->endpoint;
    aeron_receive_destination_t *destination = (aeron_receive_destination_t *)command->destination;

    endpoint->transport_bindings->close_func(&destination->transport);

    aeron_udp_channel_delete((aeron_udp_channel_t *)command->channel);
    aeron_receive_destination_delete(destination, &conductor->counters_manager);
}

void aeron_driver_conductor_on_delete_send_destination(void *clientd, void *cmd)
{
    aeron_command_base_t *command = (aeron_command_base_t *)cmd;
    aeron_uri_t *uri = (aeron_uri_t *)command->item;

    aeron_uri_close(uri);
    aeron_free(uri);
}

int aeron_driver_conductor_on_add_counter(aeron_driver_conductor_t *conductor, aeron_counter_command_t *command)
{
    aeron_client_t *client = aeron_driver_conductor_get_or_add_client(conductor, command->correlated.client_id);
    if (NULL == client)
    {
        AERON_APPEND_ERR("%s", "Failed to add client");
        return -1;
    }

    const uint8_t *cursor = (const uint8_t *)command + sizeof(aeron_counter_command_t);
    int32_t key_length;

    memcpy(&key_length, cursor, sizeof(key_length));
    const uint8_t *key = cursor + sizeof(int32_t);

    cursor = key + AERON_ALIGN(key_length, sizeof(int32_t));
    int32_t label_length;

    memcpy(&label_length, cursor, sizeof(label_length));
    const char *label = (const char *)cursor + sizeof(int32_t);

    const int32_t counter_id = aeron_counters_manager_allocate(
        &conductor->counters_manager, command->type_id, key, (size_t)key_length, label, (size_t)label_length);

    if (counter_id >= 0)
    {
        aeron_counters_manager_counter_registration_id(
            &conductor->counters_manager, counter_id, command->correlated.correlation_id);

        aeron_counters_manager_counter_owner_id(
            &conductor->counters_manager, counter_id, command->correlated.client_id);

        int ensure_capacity_result = 0;

        AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, client->counter_links, aeron_counter_link_t)
        if (ensure_capacity_result >= 0)
        {
            aeron_counter_link_t *link = &client->counter_links.array[client->counter_links.length++];

            link->registration_id = command->correlated.correlation_id;
            link->counter_id = counter_id;

            aeron_driver_conductor_on_counter_ready(conductor, command->correlated.correlation_id, counter_id);
            return 0;
        }
    }

    return -1;
}

int aeron_driver_conductor_on_remove_counter(aeron_driver_conductor_t *conductor, aeron_remove_counter_command_t *command)
{
    int index = aeron_driver_conductor_find_client(conductor, command->correlated.client_id);
    if (index >= 0)
    {
        aeron_client_t *client = &conductor->clients.array[index];

        for (size_t i = 0, size = client->counter_links.length, last_index = size - 1; i < size; i++)
        {
            aeron_counter_link_t *link = &client->counter_links.array[i];

            if (command->registration_id == link->registration_id)
            {
                aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);
                aeron_driver_conductor_on_unavailable_counter(conductor, link->registration_id, link->counter_id);

                aeron_counters_manager_free(&conductor->counters_manager, link->counter_id);

                aeron_array_fast_unordered_remove(
                    (uint8_t *)client->counter_links.array, sizeof(aeron_counter_link_t), i, last_index);
                client->counter_links.length--;

                return 0;
            }
        }
    }

    AERON_SET_ERR(
        -AERON_ERROR_CODE_UNKNOWN_COUNTER,
        "unknown counter client_id=%" PRId64 " registration_id=%" PRId64,
        command->correlated.client_id,
        command->registration_id);

    return -1;
}

int aeron_driver_conductor_on_add_static_counter(
    aeron_driver_conductor_t *conductor,
    aeron_static_counter_command_t *command)
{
    aeron_client_t *client = aeron_driver_conductor_get_or_add_client(conductor, command->correlated.client_id);
    if (NULL == client)
    {
        AERON_APPEND_ERR("%s", "Failed to add client");
        return -1;
    }

    aeron_counters_reader_t *reader = (aeron_counters_reader_t *)&conductor->counters_manager;

    int32_t counter_id = aeron_counters_reader_find_by_type_id_and_registration_id(
        reader, command->type_id, command->registration_id);
    if (AERON_NULL_COUNTER_ID != counter_id)
    {
        int64_t owner_id;
        aeron_counters_reader_counter_owner_id(reader, counter_id, &owner_id);
        if (AERON_NULL_VALUE != owner_id)
        {
            AERON_SET_ERR(-AERON_ERROR_CODE_GENERIC_ERROR,
                "cannot add static counter, because a non-static counter exists (counterId=%" PRIi32 ") for typeId=%"
                PRIi32 " and registrationId=%" PRIi64 "", counter_id, command->type_id, command->registration_id);
            return -1;
        }
    }
    else
    {
        const uint8_t *cursor = (const uint8_t *)command + sizeof(aeron_static_counter_command_t);
        int32_t key_length;

        memcpy(&key_length, cursor, sizeof(key_length));
        const uint8_t *key = cursor + sizeof(int32_t);

        cursor = key + AERON_ALIGN(key_length, sizeof(int32_t));
        int32_t label_length;

        memcpy(&label_length, cursor, sizeof(label_length));
        const char *label = (const char *)cursor + sizeof(int32_t);

        counter_id = aeron_counters_manager_allocate(
            &conductor->counters_manager, command->type_id, key, (size_t)key_length, label, (size_t)label_length);

        if (counter_id < 0)
        {
            return -1;
        }

        aeron_counters_manager_counter_registration_id(
            &conductor->counters_manager, counter_id, command->registration_id);

        aeron_counters_manager_counter_owner_id(
            &conductor->counters_manager, counter_id, AERON_NULL_VALUE);

        int64_t saved_registration_id;
        aeron_counters_reader_counter_registration_id(reader, counter_id, &saved_registration_id);

        int64_t saved_owner_id;
        aeron_counters_reader_counter_owner_id(reader, counter_id, &saved_owner_id);
    }

    aeron_driver_conductor_on_static_counter(conductor, command->correlated.correlation_id, counter_id);
    return 0;
}

int aeron_driver_conductor_on_client_close(aeron_driver_conductor_t *conductor, aeron_correlated_command_t *command)
{
    int index = aeron_driver_conductor_find_client(conductor, command->client_id);
    if (index >= 0)
    {
        aeron_client_t *client = &conductor->clients.array[index];

        client->closed_by_command = true;
        aeron_counter_set_release(client->heartbeat_timestamp.value_addr, 0);
    }

    return 0;
}

int aeron_driver_conductor_on_terminate_driver(
    aeron_driver_conductor_t *conductor, aeron_terminate_driver_command_t *command)
{
    aeron_driver_context_t *ctx = conductor->context;
    bool is_validated = false;

    if (NULL != ctx->termination_validator_func)
    {
        uint8_t *token_buffer = (uint8_t *)command + sizeof(aeron_terminate_driver_command_t);

        is_validated = ctx->termination_validator_func(
            ctx->termination_validator_state, token_buffer, command->token_length);
    }

    if (NULL != ctx->termination_hook_func && is_validated)
    {
        ctx->termination_hook_func(ctx->termination_hook_state);
    }

    return 0;
}

int aeron_driver_conductor_on_invalidate_image(
    aeron_driver_conductor_t *conductor, aeron_reject_image_command_t *command)
{
    const int64_t image_correlation_id = command->image_correlation_id;

    if (AERON_ERROR_MAX_TEXT_LENGTH < command->reason_length)
    {
        AERON_SET_ERR(AERON_ERROR_CODE_GENERIC_ERROR, "%s", "Invalidation reason_text must be 1023 bytes or less");
        return -1;
    }

    aeron_publication_image_t *image = aeron_driver_conductor_find_publication_image(
        conductor, image_correlation_id);

    if (NULL == image)
    {
        aeron_ipc_publication_t *found_publication = NULL;

        for (size_t i = 0; i < conductor->ipc_publications.length; i++)
        {
            aeron_ipc_publication_t *pub_entry = conductor->ipc_publications.array[i].publication;

            if (image_correlation_id == pub_entry->conductor_fields.managed_resource.registration_id)
            {
                found_publication = pub_entry;
                break;
            }
        }

        if (NULL == found_publication)
        {
            AERON_SET_ERR(
                AERON_ERROR_CODE_GENERIC_ERROR,
                "Unable to resolve image for correlationId=%" PRId64, image_correlation_id);
            return -1;
        }

        const char *reason_text = (const char *)command->reason_text;
        aeron_ipc_publication_reject(
            found_publication,
            command->position,
            command->reason_length,
            reason_text,
            conductor,
            aeron_clock_cached_nano_time(conductor->context->cached_clock));
    }
    else
    {
        const char *reason_text = (const char *)command->reason_text;
        aeron_driver_receiver_proxy_on_invalidate_image(
            conductor->context->receiver_proxy,
            image_correlation_id,
            command->position,
            command->reason_length,
            reason_text);
    }

    aeron_counter_increment_release(conductor->images_rejected_counter);

    aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);

    return 0;
}

void aeron_driver_conductor_unlink_ipc_subscriptions(
    aeron_driver_conductor_t *conductor, aeron_ipc_publication_t *publication)
{
    for (size_t i = 0, size = conductor->ipc_subscriptions.length; i < size; i++)
    {
        aeron_subscription_link_t *link = &conductor->ipc_subscriptions.array[i];

        if (aeron_driver_conductor_is_subscribable_linked(link, &publication->conductor_fields.subscribable))
        {
            aeron_driver_conductor_on_unavailable_image(
                conductor,
                publication->conductor_fields.managed_resource.registration_id,
                link->registration_id,
                link->stream_id,
                link->channel,
                link->channel_length);

            aeron_driver_conductor_unlink_subscribable(link, &publication->conductor_fields.subscribable);
        }
    }
}

void aeron_driver_conductor_on_create_publication_image(void *clientd, void *item)
{
    aeron_driver_conductor_t *conductor = (aeron_driver_conductor_t *)clientd;
    aeron_command_create_publication_image_t *command = (aeron_command_create_publication_image_t *)item;
    aeron_receive_channel_endpoint_t *endpoint = command->endpoint;
    aeron_receive_destination_t *destination = command->destination;

    size_t initial_window_length = aeron_udp_channel_receiver_window(
        endpoint->conductor_fields.udp_channel, conductor->context->initial_window_length);

    if (aeron_receiver_channel_endpoint_validate_sender_mtu_length(
        endpoint, (size_t)command->mtu_length, initial_window_length, conductor->context) < 0)
    {
        AERON_APPEND_ERR("stream_id=%d session_id=%d", command->stream_id, command->session_id);
        goto error_cleanup;
    }

    if (!aeron_driver_conductor_has_network_subscription_interest(
        conductor, endpoint, command->stream_id, command->session_id))
    {
        aeron_driver_receiver_proxy_on_remove_init_in_progress(
            conductor->context->receiver_proxy, endpoint, command->session_id, command->stream_id);
        return;
    }

    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, conductor->publication_images, aeron_publication_image_entry_t)
    if (ensure_capacity_result < 0)
    {
        AERON_APPEND_ERR("stream_id=%d session_id=%d", command->stream_id, command->session_id);
        goto error_cleanup;
    }

    const int64_t registration_id = aeron_mpsc_rb_next_correlation_id(&conductor->to_driver_commands);
    const int64_t join_position = aeron_logbuffer_compute_position(
        command->active_term_id,
        command->term_offset,
        (size_t)aeron_number_of_trailing_zeroes(command->term_length),
        command->initial_term_id);

    aeron_congestion_control_strategy_t *congestion_control = NULL;
    if (conductor->context->congestion_control_supplier_func(
        &congestion_control,
        endpoint->conductor_fields.udp_channel,
        command->stream_id,
        command->session_id,
        registration_id,
        command->term_length,
        command->mtu_length,
        &command->control_address,
        &command->src_address,
        conductor->context,
        &conductor->counters_manager) < 0)
    {
        AERON_APPEND_ERR("stream_id=%d session_id=%d", command->stream_id, command->session_id);
        goto error_cleanup;
    }

    aeron_subscription_link_t subscription_link = conductor->network_subscriptions.array[0];
    for (size_t i = 0; i < conductor->network_subscriptions.length; i++)
    {
        if (aeron_driver_conductor_network_subscription_link_matches_allowing_wildcard(
            &conductor->network_subscriptions.array[i], endpoint, command->stream_id, command->session_id))
        {
            subscription_link = conductor->network_subscriptions.array[i];
            break;
        }
    }

    const char *uri = subscription_link.channel;
    size_t uri_length = subscription_link.channel_length;

    aeron_position_t rcv_hwm_position;
    rcv_hwm_position.counter_id = aeron_counter_receiver_hwm_allocate(
        &conductor->counters_manager, registration_id, command->session_id, command->stream_id, uri_length, uri);
    if (rcv_hwm_position.counter_id < 0)
    {
        AERON_APPEND_ERR("stream_id=%d session_id=%d", command->stream_id, command->session_id);
        goto error_cleanup;
    }

    aeron_position_t rcv_pos_position;
    rcv_pos_position.counter_id = aeron_counter_receiver_position_allocate(
        &conductor->counters_manager, registration_id, command->session_id, command->stream_id, uri_length, uri);
    if (rcv_pos_position.counter_id < 0)
    {
        aeron_counters_manager_free(&conductor->counters_manager, rcv_hwm_position.counter_id);
        AERON_APPEND_ERR("stream_id=%d session_id=%d", command->stream_id, command->session_id);
        goto error_cleanup;
    }

    aeron_atomic_counter_t rcv_naks_sent;
    rcv_naks_sent.counter_id = aeron_counter_receiver_naks_sent_allocate(
        &conductor->counters_manager, registration_id, command->session_id, command->stream_id, uri_length, uri);
    if (rcv_naks_sent.counter_id < 0)
    {
        aeron_counters_manager_free(&conductor->counters_manager, rcv_hwm_position.counter_id);
        aeron_counters_manager_free(&conductor->counters_manager, rcv_pos_position.counter_id);
        AERON_APPEND_ERR("stream_id=%d session_id=%d", command->stream_id, command->session_id);
        goto error_cleanup;
    }

    rcv_hwm_position.value_addr = aeron_counters_manager_addr(
        &conductor->counters_manager, rcv_hwm_position.counter_id);
    rcv_pos_position.value_addr = aeron_counters_manager_addr(
        &conductor->counters_manager, rcv_pos_position.counter_id);
    rcv_naks_sent.value_addr = aeron_counters_manager_addr(
        &conductor->counters_manager, rcv_naks_sent.counter_id);

    bool is_reliable = subscription_link.is_reliable;
    bool treat_as_multicast = aeron_driver_conductor_treat_image_as_multicast(
        endpoint->conductor_fields.udp_channel, command->flags, subscription_link.group);
    bool is_oldest_subscription_sparse = aeron_driver_conductor_is_oldest_subscription_sparse(
        conductor, endpoint, command->stream_id, command->session_id, registration_id);

    aeron_publication_image_t *image = NULL;
    if (aeron_publication_image_create(
        &image,
        endpoint,
        destination,
        conductor,
        registration_id,
        command->session_id,
        command->stream_id,
        command->initial_term_id,
        command->active_term_id,
        command->term_offset,
        &rcv_hwm_position,
        &rcv_pos_position,
        &rcv_naks_sent,
        congestion_control,
        &command->control_address,
        &command->src_address,
        command->term_length,
        command->mtu_length,
        command->flags,
        &conductor->loss_reporter,
        is_reliable,
        is_oldest_subscription_sparse,
        treat_as_multicast,
        &conductor->system_counters) < 0)
    {
        aeron_counters_manager_free(&conductor->counters_manager, rcv_hwm_position.counter_id);
        aeron_counters_manager_free(&conductor->counters_manager, rcv_pos_position.counter_id);
        AERON_APPEND_ERR("stream_id=%d session_id=%d", command->stream_id, command->session_id);
        goto error_cleanup;
    }

    aeron_receive_channel_endpoint_inc_image_ref_count(endpoint);
    conductor->publication_images.array[conductor->publication_images.length++].image = image;
    int64_t now_ns = aeron_clock_cached_nano_time(conductor->context->cached_clock);

    for (size_t i = 0, length = conductor->network_subscriptions.length; i < length; i++)
    {
        aeron_subscription_link_t *link = &conductor->network_subscriptions.array[i];

        if (!aeron_driver_conductor_network_subscription_link_matches_allowing_wildcard(
            link,
            endpoint,
            command->stream_id,
            command->session_id))
        {
            continue;
        }

        if (aeron_driver_conductor_link_subscribable(
            conductor,
            link,
            &image->conductor_fields.subscribable,
            registration_id,
            command->session_id,
            command->stream_id,
            join_position,
            now_ns,
            image->source_identity_length,
            image->source_identity,
            image->log_file_name_length,
            image->log_file_name) < 0)
        {
            AERON_APPEND_ERR("stream_id=%d session_id=%d", command->stream_id, command->session_id);
            goto error_cleanup;
        }
    }

    aeron_driver_receiver_proxy_on_add_publication_image(conductor->context->receiver_proxy, endpoint, image);
    return;

error_cleanup:
    aeron_driver_conductor_log_error(conductor);
    aeron_driver_receiver_proxy_on_remove_init_in_progress(
        conductor->context->receiver_proxy, endpoint, command->session_id, command->stream_id);
}

void aeron_driver_conductor_on_re_resolve_endpoint_complete(
    int result,
    int errcode,
    const char *errmsg,
    void *task_clientd,
    void *executor_clientd)
{
    aeron_async_re_resolve_t *async_cmd = task_clientd;
    aeron_driver_conductor_t *conductor = executor_clientd;

    if (result < 0)
    {
        aeron_driver_conductor_log_explicit_error(conductor, errcode, errmsg);
    }
    else if (0 != memcmp(
        &async_cmd->async_resolve.sockaddr,
        &async_cmd->existing_addr,
        sizeof(struct sockaddr_storage)))
    {
        aeron_driver_sender_proxy_on_resolution_change(
            conductor->context->sender_proxy,
            async_cmd->async_resolve.endpoint_name,
            async_cmd->endpoint,
            &async_cmd->async_resolve.sockaddr);
    }

    aeron_free(async_cmd);
}

void aeron_driver_conductor_on_re_resolve_endpoint(void *clientd, void *item)
{
    aeron_driver_conductor_t *conductor = clientd;
    aeron_command_re_resolve_t *cmd = item;
    struct sockaddr_storage resolved_addr;
    memset(&resolved_addr, 0, sizeof(resolved_addr));
    aeron_send_channel_endpoint_t *endpoint = cmd->endpoint;

    if (AERON_SEND_CHANNEL_ENDPOINT_STATUS_ACTIVE != endpoint->conductor_fields.status)
    {
        return;
    }

    aeron_async_re_resolve_t *async_cmd;

    if (aeron_alloc((void **)&async_cmd, sizeof(aeron_async_re_resolve_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return;
    }

    async_cmd->async_resolve.uri_param_name = AERON_UDP_CHANNEL_ENDPOINT_KEY;
    async_cmd->async_resolve.is_re_resolution = true;
    COPY_ENDPOINT_NAME(async_cmd->async_resolve.endpoint_name, cmd->endpoint_name);

    memcpy(&async_cmd->existing_addr, &cmd->existing_addr, sizeof(cmd->existing_addr));
    async_cmd->endpoint = endpoint;
    async_cmd->destination = NULL;

    if (aeron_executor_submit(
        &conductor->executor,
        aeron_driver_async_resolve_host_and_port_execute,
        aeron_driver_conductor_on_re_resolve_endpoint_complete,
        async_cmd) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        aeron_driver_conductor_log_error(conductor);
    }
}

void aeron_driver_conductor_on_re_resolve_control_complete(
    int result,
    int errcode,
    const char *errmsg,
    void *task_clientd,
    void *executor_clientd)
{
    aeron_async_re_resolve_t *async_cmd = task_clientd;
    aeron_driver_conductor_t *conductor = executor_clientd;

    if (result < 0)
    {
        aeron_driver_conductor_log_explicit_error(conductor, errcode, errmsg);
    }
    else if (0 != memcmp(
        &async_cmd->async_resolve.sockaddr,
        &async_cmd->existing_addr,
        sizeof(struct sockaddr_storage)))
    {
        aeron_driver_receiver_proxy_on_resolution_change(
            conductor->context->receiver_proxy,
            async_cmd->async_resolve.endpoint_name,
            async_cmd->endpoint,
            async_cmd->destination,
            &async_cmd->async_resolve.sockaddr);
    }

    aeron_free(async_cmd);
}

void aeron_driver_conductor_on_re_resolve_control(void *clientd, void *item)
{
    aeron_driver_conductor_t *conductor = clientd;
    aeron_command_re_resolve_t *cmd = item;
    struct sockaddr_storage resolved_addr;
    memset(&resolved_addr, 0, sizeof(resolved_addr));

    aeron_async_re_resolve_t *async_cmd;

    if (aeron_alloc((void **)&async_cmd, sizeof(aeron_async_re_resolve_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return;
    }

    async_cmd->async_resolve.uri_param_name = AERON_UDP_CHANNEL_CONTROL_KEY;
    async_cmd->async_resolve.is_re_resolution = true;
    COPY_ENDPOINT_NAME(async_cmd->async_resolve.endpoint_name, cmd->endpoint_name);

    memcpy(&async_cmd->existing_addr, &cmd->existing_addr, sizeof(cmd->existing_addr));
    async_cmd->endpoint = cmd->endpoint;
    async_cmd->destination = cmd->destination;

    if (aeron_executor_submit(
        &conductor->executor,
        aeron_driver_async_resolve_host_and_port_execute,
        aeron_driver_conductor_on_re_resolve_control_complete,
        async_cmd) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        aeron_driver_conductor_log_error(conductor);
    }
}

void aeron_driver_conductor_on_receive_endpoint_removed(void *clientd, void *item)
{
    aeron_driver_conductor_t *conductor = clientd;
    aeron_command_base_t *cmd = item;
    aeron_receive_channel_endpoint_t *endpoint = cmd->item;

    if (endpoint->conductor_fields.status == AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_CLOSING)
    {
        aeron_udp_channel_t *udp_channel = endpoint->conductor_fields.udp_channel;

        aeron_str_to_ptr_hash_map_remove(
            &conductor->receive_channel_endpoint_by_channel_map,
            udp_channel->canonical_form,
            udp_channel->canonical_length);

        aeron_receive_channel_endpoint_close(endpoint);
        aeron_receive_channel_endpoint_receiver_release(endpoint);
    }
}

void aeron_driver_conductor_on_response_setup(void *clientd, void *item)
{
    aeron_driver_conductor_t *conductor = clientd;
    aeron_command_response_setup_t *cmd = item;
    const int64_t response_correlation_id = cmd->response_correlation_id;
    const int32_t response_session_id = cmd->response_session_id;

    for (size_t i = 0, length = conductor->network_subscriptions.length; i < length; i++)
    {
        aeron_subscription_link_t *subscription_link = &conductor->network_subscriptions.array[i];
        if (subscription_link->registration_id == response_correlation_id)
        {
            if (subscription_link->has_session_id)
            {
                aeron_driver_receiver_proxy_on_request_setup(
                    subscription_link->endpoint->receiver_proxy,
                    subscription_link->endpoint,
                    subscription_link->stream_id,
                    subscription_link->session_id);
            }
            else
            {
                subscription_link->has_session_id = true;
                subscription_link->session_id = response_session_id;
                subscription_link->is_response = false;

                aeron_driver_conductor_add_network_subscription_to_receiver(
                    subscription_link->endpoint,
                    subscription_link->stream_id,
                    subscription_link->has_session_id,
                    subscription_link->session_id);
                aeron_receive_channel_endpoint_decref_to_response_stream(
                    subscription_link->endpoint, subscription_link->stream_id);
            }
        }
    }
}

void aeron_driver_conductor_on_response_connected(void *clientd, void *item)
{
    aeron_driver_conductor_t *conductor = clientd;
    aeron_command_response_connected_t *cmd = item;
    const int64_t response_correlation_id = cmd->response_correlation_id;

    for (size_t i = 0, length = conductor->publication_images.length; i < length; i++)
    {
        aeron_publication_image_t *publication_image = conductor->publication_images.array[i].image;
        if (response_correlation_id == aeron_publication_image_registration_id(publication_image))
        {
            aeron_publication_image_remove_response_session_id(publication_image);
        }
    }
}

void aeron_driver_conductor_on_release_resource(void *clientd, void *item)
{
    aeron_driver_conductor_t *conductor = clientd;
    aeron_command_release_resource_t *cmd = item;
    void *managed_resource = cmd->base.item;
    aeron_driver_conductor_resource_type_t resource_type = cmd->resource_type;

    switch (resource_type)
    {
        case AERON_DRIVER_CONDUCTOR_RESOURCE_TYPE_CLIENT:
        case AERON_DRIVER_CONDUCTOR_RESOURCE_TYPE_IPC_PUBLICATION:
            break;

        case AERON_DRIVER_CONDUCTOR_RESOURCE_TYPE_NETWORK_PUBLICATION:
            aeron_network_publication_sender_release(managed_resource);
            break;

        case AERON_DRIVER_CONDUCTOR_RESOURCE_TYPE_SEND_CHANNEL_ENDPOINT:
        {
            aeron_send_channel_endpoint_t *endpoint = (aeron_send_channel_endpoint_t *)managed_resource;

            if (endpoint->conductor_fields.status == AERON_SEND_CHANNEL_ENDPOINT_STATUS_CLOSING)
            {
                aeron_str_to_ptr_hash_map_remove(
                    &conductor->send_channel_endpoint_by_channel_map,
                    endpoint->conductor_fields.udp_channel->canonical_form,
                    endpoint->conductor_fields.udp_channel->canonical_length);

                aeron_send_channel_endpoint_close(endpoint);
                aeron_send_channel_endpoint_sender_release(managed_resource);
            }

            break;
        }

        case AERON_DRIVER_CONDUCTOR_RESOURCE_TYPE_PUBLICATION_IMAGE:
            aeron_publication_image_receiver_release(managed_resource);
            break;

        case AERON_DRIVER_CONDUCTOR_RESOURCE_TYPE_LINGER_RESOURCE:
            break;
    }

}


extern void aeron_driver_subscribable_null_hook(void *clientd, volatile int64_t *value_addr);

extern bool aeron_driver_conductor_is_subscribable_linked(
    aeron_subscription_link_t *link, aeron_subscribable_t *subscribable);

extern size_t aeron_driver_conductor_num_clients(aeron_driver_conductor_t *conductor);

extern size_t aeron_driver_conductor_num_ipc_publications(aeron_driver_conductor_t *conductor);

extern size_t aeron_driver_conductor_num_ipc_subscriptions(aeron_driver_conductor_t *conductor);

extern size_t aeron_driver_conductor_num_network_publications(aeron_driver_conductor_t *conductor);

extern size_t aeron_driver_conductor_num_network_subscriptions(aeron_driver_conductor_t *conductor);

extern size_t aeron_driver_conductor_num_spy_subscriptions(aeron_driver_conductor_t *conductor);

extern size_t aeron_driver_conductor_num_send_channel_endpoints(aeron_driver_conductor_t *conductor);

extern size_t aeron_driver_conductor_num_receive_channel_endpoints(aeron_driver_conductor_t *conductor);

extern size_t aeron_driver_conductor_num_active_ipc_subscriptions(
    aeron_driver_conductor_t *conductor, int32_t stream_id);

extern size_t aeron_driver_conductor_num_active_network_subscriptions(
    aeron_driver_conductor_t *conductor, const char *original_uri, int32_t stream_id);

extern size_t aeron_driver_conductor_num_active_spy_subscriptions(
    aeron_driver_conductor_t *conductor, const char *original_uri, int32_t stream_id);

extern size_t aeron_driver_conductor_num_images(aeron_driver_conductor_t *conductor);

extern aeron_ipc_publication_t *aeron_driver_conductor_find_ipc_publication(
    aeron_driver_conductor_t *conductor, int64_t id);

extern aeron_network_publication_t *aeron_driver_conductor_find_network_publication(
    aeron_driver_conductor_t *conductor, int64_t id);

extern aeron_network_publication_t *aeron_driver_conductor_find_network_publication_by_tag(
    aeron_driver_conductor_t *conductor, int64_t tag_id);

extern void aeron_driver_init_subscription_channel(size_t uri_length, const char *uri, aeron_subscription_link_t *link);

extern void aeron_duty_cycle_stall_tracker_update(void *state, int64_t now_ns);

extern void aeron_duty_cycle_stall_tracker_measure_and_update(void *state, int64_t now_ns);
