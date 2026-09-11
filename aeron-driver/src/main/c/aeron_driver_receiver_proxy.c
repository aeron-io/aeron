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

#include "concurrent/aeron_counters_manager.h"
#include "aeron_driver_receiver_proxy.h"
#include "aeron_driver_receiver.h"
#include "aeron_alloc.h"

void aeron_driver_receiver_proxy_offer(aeron_driver_receiver_proxy_t *receiver_proxy, void *cmd, size_t length)
{
    aeron_rb_write_result_t result;
    while (AERON_RB_FULL == (result = aeron_mpsc_rb_write(receiver_proxy->command_queue, 1, cmd, length)))
    {
        aeron_counter_increment_release(receiver_proxy->fail_counter);
        sched_yield();
    }

    if (AERON_RB_ERROR == result)
    {
        aeron_distinct_error_log_record(
            receiver_proxy->receiver->error_log, EINVAL, "Error writing to receiver proxy ring buffer");
    }
}

void aeron_driver_receiver_proxy_on_add_endpoint(
    aeron_driver_receiver_proxy_t *receiver_proxy, aeron_receive_channel_endpoint_t *endpoint)
{
    receiver_proxy->log.on_add_endpoint(endpoint->conductor_fields.udp_channel);

    aeron_command_base_t cmd =
        {
            .func = aeron_driver_receiver_on_add_endpoint,
            .item = endpoint
        };

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_driver_receiver_on_add_endpoint(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_driver_receiver_proxy_offer(receiver_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_receiver_proxy_on_remove_endpoint(
    aeron_driver_receiver_proxy_t *receiver_proxy, aeron_receive_channel_endpoint_t *endpoint)
{
    receiver_proxy->log.on_remove_endpoint(endpoint->conductor_fields.udp_channel);

    aeron_command_base_t cmd =
        {
            .func = aeron_driver_receiver_on_remove_endpoint,
            .item = endpoint
        };

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_driver_receiver_on_remove_endpoint(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_driver_receiver_proxy_offer(receiver_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_receiver_proxy_on_add_subscription(
    aeron_driver_receiver_proxy_t *receiver_proxy, aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id)
{
    aeron_command_subscription_t cmd =
        {
            .base = { .func = aeron_driver_receiver_on_add_subscription, .item = NULL },
            .endpoint = endpoint,
            .stream_id = stream_id,
            .session_id = 0 // ignored
        };

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_driver_receiver_on_add_subscription(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_driver_receiver_proxy_offer(receiver_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_receiver_proxy_on_remove_subscription(
    aeron_driver_receiver_proxy_t *receiver_proxy, aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id)
{
    aeron_command_subscription_t cmd =
        {
            .base = { .func = aeron_driver_receiver_on_remove_subscription, .item = NULL },
            .endpoint = endpoint,
            .stream_id = stream_id,
            .session_id = 0 // ignored.
        };

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_driver_receiver_on_remove_subscription(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_driver_receiver_proxy_offer(receiver_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_receiver_proxy_on_add_subscription_by_session(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id,
    int32_t session_id)
{
    aeron_command_subscription_t cmd =
        {
            .base = { .func = aeron_driver_receiver_on_add_subscription_by_session, .item = NULL },
            .endpoint = endpoint,
            .stream_id = stream_id,
            .session_id = session_id
        };

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_driver_receiver_on_add_subscription_by_session(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_driver_receiver_proxy_offer(receiver_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_receiver_proxy_on_request_setup(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id,
    int32_t session_id)
{
    aeron_command_subscription_t cmd =
        {
            .base = { .func = aeron_driver_receiver_on_request_setup, .item = NULL },
            .endpoint = endpoint,
            .stream_id = stream_id,
            .session_id = session_id
        };

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_driver_receiver_on_request_setup(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_driver_receiver_proxy_offer(receiver_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_receiver_proxy_on_remove_subscription_by_session(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id,
    int32_t session_id)
{
    aeron_command_subscription_t cmd =
        {
            .base = { .func = aeron_driver_receiver_on_remove_subscription_by_session, .item = NULL },
            .endpoint = endpoint,
            .stream_id = stream_id,
            .session_id = session_id
        };

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_driver_receiver_on_remove_subscription_by_session(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_driver_receiver_proxy_offer(receiver_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_receiver_proxy_on_add_destination(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination)
{
    aeron_command_add_rcv_destination_t cmd =
        {
            .base = { .func = aeron_driver_receiver_on_add_destination, .item = NULL },
            .endpoint = endpoint,
            .destination = destination
        };

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_driver_receiver_on_add_destination(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_driver_receiver_proxy_offer(receiver_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_receiver_proxy_on_remove_destination(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_udp_channel_t *channel)
{
    aeron_command_remove_rcv_destination_t cmd =
        {
            .base = { .func = aeron_driver_receiver_on_remove_destination, .item = NULL },
            .endpoint = endpoint,
            .channel = channel
        };

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_driver_receiver_on_remove_destination(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_driver_receiver_proxy_offer(receiver_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_receiver_proxy_on_add_publication_image(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_publication_image_t *image)
{
    aeron_command_publication_image_t cmd =
        {
            .base = { .func = aeron_driver_receiver_on_add_publication_image, .item = NULL },
            .image = image
        };

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_driver_receiver_on_add_publication_image(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_driver_receiver_proxy_offer(receiver_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_receiver_proxy_on_remove_publication_image(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_publication_image_t *image)
{
    aeron_command_publication_image_t cmd =
        {
            .base = { .func = aeron_driver_receiver_on_remove_publication_image, .item = NULL },
            .image = image
        };

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_driver_receiver_on_remove_publication_image(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_driver_receiver_proxy_offer(receiver_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_receiver_proxy_on_remove_cool_down(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t session_id,
    int32_t stream_id)
{
    aeron_command_on_remove_matching_state_t cmd =
        {
            .base = { .func = aeron_driver_receiver_on_remove_matching_state, .item = NULL },
            .endpoint = endpoint,
            .session_id = session_id,
            .stream_id = stream_id,
            .state = AERON_DATA_PACKET_DISPATCHER_IMAGE_COOL_DOWN
        };

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_driver_receiver_on_remove_matching_state(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_driver_receiver_proxy_offer(receiver_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_receiver_proxy_on_remove_init_in_progress(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t session_id,
    int32_t stream_id)
{
    aeron_command_on_remove_matching_state_t cmd =
        {
            .base = { .func = aeron_driver_receiver_on_remove_matching_state, .item = NULL },
            .endpoint = endpoint,
            .session_id = session_id,
            .stream_id = stream_id,
            .state = AERON_DATA_PACKET_DISPATCHER_IMAGE_INIT_IN_PROGRESS
        };

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_driver_receiver_on_remove_matching_state(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_driver_receiver_proxy_offer(receiver_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_receiver_proxy_on_resolution_change(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    const char *endpoint_name,
    void *endpoint,
    void *destination,
    struct sockaddr_storage *new_addr)
{
    aeron_command_receiver_resolution_change_t cmd =
        {
            .base = { .func = aeron_driver_receiver_on_resolution_change, .item = NULL },
            .endpoint_name = endpoint_name,
            .endpoint = endpoint,
            .destination = destination
        };
    memcpy(&cmd.new_addr, new_addr, sizeof(cmd.new_addr));

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_driver_receiver_on_resolution_change(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_driver_receiver_proxy_offer(receiver_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_receiver_proxy_on_invalidate_image(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    int64_t image_correlation_id,
    int64_t position,
    int32_t reason_length,
    const char *reason)
{
    reason_length = reason_length <= AERON_ERROR_MAX_TEXT_LENGTH ? reason_length : AERON_ERROR_MAX_TEXT_LENGTH;
    uint8_t message_buffer[sizeof(aeron_command_base_t) + AERON_ERROR_MAX_TEXT_LENGTH + 1];
    aeron_command_receiver_invalidate_image_t *cmd = (aeron_command_receiver_invalidate_image_t *)message_buffer;

    cmd->base.func = aeron_driver_receiver_on_invalidate_image;
    cmd->base.item = NULL;
    cmd->image_correlation_id = image_correlation_id;
    cmd->position = position;
    cmd->reason_length = reason_length;
    memcpy(cmd->reason_text, reason, reason_length);
    aeron_str_null_terminate(cmd->reason_text, reason_length);

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_driver_receiver_on_invalidate_image(receiver_proxy->receiver, cmd);
    }
    else
    {
        aeron_driver_receiver_proxy_offer(receiver_proxy, cmd, sizeof(*cmd) + reason_length);
    }
}
