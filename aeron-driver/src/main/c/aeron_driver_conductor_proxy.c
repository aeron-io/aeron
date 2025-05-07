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

#include "concurrent/aeron_counters_manager.h"
#include "aeron_driver_conductor_proxy.h"
#include "aeron_alloc.h"
#include "aeron_driver_conductor.h"

void aeron_driver_conductor_proxy_offer(aeron_driver_conductor_proxy_t *conductor_proxy, void *cmd, size_t length)
{
    aeron_rb_write_result_t result;
    while (AERON_RB_FULL == (result = aeron_mpsc_rb_write(conductor_proxy->command_queue, 1, cmd, length)))
    {
        aeron_counter_increment_release(conductor_proxy->fail_counter);
        sched_yield();
    }

    if (AERON_RB_ERROR == result)
    {
        aeron_distinct_error_log_record(
            &conductor_proxy->conductor->error_log, EINVAL, "Error writing to receiver proxy ring buffer");
    }
}

void aeron_driver_conductor_proxy_on_create_publication_image_cmd(
    aeron_driver_conductor_proxy_t *conductor_proxy,
    int32_t session_id,
    int32_t stream_id,
    int32_t initial_term_id,
    int32_t active_term_id,
    int32_t term_offset,
    int32_t term_length,
    int32_t mtu_length,
    uint8_t flags,
    struct sockaddr_storage *control_address,
    struct sockaddr_storage *src_address,
    void *endpoint,
    void *destination)
{
    aeron_command_create_publication_image_t cmd =
        {
            .base = { .func = aeron_driver_conductor_on_create_publication_image, .item = NULL },
            .session_id = session_id,
            .stream_id = stream_id,
            .initial_term_id = initial_term_id,
            .active_term_id = active_term_id,
            .term_offset = term_offset,
            .term_length = term_length,
            .mtu_length = mtu_length,
            .flags = flags,
            .endpoint = endpoint,
            .destination = destination
        };
    memcpy(&cmd.control_address, control_address, sizeof(struct sockaddr_storage));
    memcpy(&cmd.src_address, src_address, sizeof(struct sockaddr_storage));

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(conductor_proxy->threading_mode))
    {
        aeron_driver_conductor_on_create_publication_image(conductor_proxy->conductor, &cmd);
    }
    else
    {
        aeron_driver_conductor_proxy_offer(conductor_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_conductor_proxy_on_re_resolve(
    aeron_driver_conductor_proxy_t *conductor_proxy,
    void (*resolve_func)(void *, void *),
    const char *endpoint_name,
    void *endpoint,
    void *destination,
    struct sockaddr_storage *existing_addr)
{
    aeron_command_re_resolve_t cmd =
        {
            .base = { .func = resolve_func, .item = NULL },
            .endpoint_name = endpoint_name,
            .endpoint = endpoint,
            .destination = destination,
        };
    memcpy(&cmd.existing_addr, existing_addr, sizeof(cmd.existing_addr));

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(conductor_proxy->threading_mode))
    {
        resolve_func(conductor_proxy->conductor, &cmd);
    }
    else
    {
        aeron_driver_conductor_proxy_offer(conductor_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_conductor_proxy_on_re_resolve_endpoint(
    aeron_driver_conductor_proxy_t *conductor_proxy,
    const char *endpoint_name,
    void *endpoint,
    struct sockaddr_storage *existing_addr)
{
    aeron_driver_conductor_proxy_on_re_resolve(
        conductor_proxy, aeron_driver_conductor_on_re_resolve_endpoint, endpoint_name, endpoint, NULL, existing_addr);
}

void aeron_driver_conductor_proxy_on_re_resolve_control(
    aeron_driver_conductor_proxy_t *conductor_proxy,
    const char *endpoint_name,
    void *endpoint,
    void *destination,
    struct sockaddr_storage *existing_addr)
{
    aeron_driver_conductor_proxy_on_re_resolve(
        conductor_proxy,
        aeron_driver_conductor_on_re_resolve_control,
        endpoint_name,
        endpoint,
        destination,
        existing_addr);
}

void aeron_driver_conductor_proxy_on_delete_receive_destination(
    aeron_driver_conductor_proxy_t *conductor_proxy, void *endpoint, void *destination, void *channel)
{
    aeron_command_delete_destination_t cmd =
        {
            .base = { .func = aeron_driver_conductor_on_delete_receive_destination, .item = NULL },
            .endpoint = endpoint,
            .destination = destination,
            .channel = channel
        };

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(conductor_proxy->threading_mode))
    {
        aeron_driver_conductor_on_delete_receive_destination(conductor_proxy->conductor, &cmd);
    }
    else
    {
        aeron_driver_conductor_proxy_offer(conductor_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_conductor_proxy_on_delete_send_destination(
    aeron_driver_conductor_proxy_t *conductor_proxy, void *removed_uri)
{
    aeron_command_base_t cmd =
        {
            .func = aeron_driver_conductor_on_delete_send_destination,
            .item = removed_uri,
        };

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(conductor_proxy->threading_mode))
    {
        aeron_driver_conductor_on_delete_send_destination(conductor_proxy->conductor, &cmd);
    }
    else
    {
        aeron_driver_conductor_proxy_offer(conductor_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_conductor_proxy_on_receive_endpoint_removed(
    aeron_driver_conductor_proxy_t *conductor_proxy, void *endpoint)
{
    aeron_command_base_t cmd =
        {
            .func = aeron_driver_conductor_on_receive_endpoint_removed,
            .item = endpoint
        };

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(conductor_proxy->threading_mode))
    {
        aeron_driver_conductor_on_receive_endpoint_removed(conductor_proxy->conductor, &cmd);
    }
    else
    {
        aeron_driver_conductor_proxy_offer(conductor_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_conductor_proxy_on_response_setup(
    aeron_driver_conductor_proxy_t *conductor_proxy,
    int64_t response_correlation_id,
    int32_t response_session_id)
{
    aeron_command_response_setup_t cmd = {
        .base = {
            .func = aeron_driver_conductor_on_response_setup,
            .item = NULL
        },
        .response_correlation_id = response_correlation_id,
        .response_session_id = response_session_id
    };

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(conductor_proxy->threading_mode))
    {
        aeron_driver_conductor_on_response_setup(conductor_proxy->conductor, &cmd);
    }
    else
    {
        aeron_driver_conductor_proxy_offer(conductor_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_conductor_proxy_on_response_connected(
    aeron_driver_conductor_proxy_t *conductor_proxy,
    int64_t response_correlation_id)
{
    aeron_command_response_connected_t cmd = {
        .base = {
            .func = aeron_driver_conductor_on_response_connected,
            .item = NULL
        },
        .response_correlation_id = response_correlation_id
    };

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(conductor_proxy->threading_mode))
    {
        aeron_driver_conductor_on_response_connected(conductor_proxy->conductor, &cmd);
    }
    else
    {
        aeron_driver_conductor_proxy_offer(conductor_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_conductor_proxy_on_release_resource(
    aeron_driver_conductor_proxy_t *conductor_proxy,
    void *managed_resource,
    aeron_driver_conductor_resource_type_t resource_type)
{
   aeron_command_release_resource_t cmd =
        {
            .base =
                {
                    .func = aeron_driver_conductor_on_release_resource,
                    .item = managed_resource
                },
            .resource_type = resource_type
        };

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(conductor_proxy->threading_mode))
    {
        aeron_driver_conductor_on_release_resource(conductor_proxy->conductor, &cmd);
    }
    else
    {
        aeron_driver_conductor_proxy_offer(conductor_proxy, &cmd, sizeof(cmd));
    }
}

void aeron_driver_conductor_proxy_on_publication_error(
    aeron_driver_conductor_proxy_t *conductor_proxy,
    const int64_t registration_id,
    const int64_t destination_registration_id,
    int32_t session_id,
    int32_t stream_id,
    int64_t receiver_id,
    int64_t group_tag,
    struct sockaddr_storage *src_address,
    int32_t error_code,
    int32_t error_length,
    const uint8_t *error_text)
{
    uint8_t buffer[AERON_COMMAND_PUBLICATION_ERROR_MAX_LENGTH];
    aeron_command_publication_error_t *error = (aeron_command_publication_error_t *)buffer;
    error_length = error_length <= AERON_ERROR_MAX_TEXT_LENGTH ? error_length : AERON_ERROR_MAX_TEXT_LENGTH;

    error->base.func = aeron_driver_conductor_on_publication_error;
    error->base.item = NULL;
    error->registration_id = registration_id;
    error->destination_registration_id = destination_registration_id;
    error->session_id = session_id;
    error->stream_id = stream_id;
    error->receiver_id = receiver_id;
    error->group_tag = group_tag;
    memset(&error->src_address, 0, sizeof(struct sockaddr_storage));
    memcpy(&error->src_address, src_address, AERON_ADDR_LEN(src_address));
    error->error_code = error_code;
    error->error_length = error_length;
    memcpy(error->error_text, error_text, (size_t)error_length);
    aeron_str_null_terminate(error->error_text, error_length);

    size_t cmd_length = sizeof(aeron_command_publication_error_t) + error_length + 1;

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(conductor_proxy->threading_mode))
    {
        aeron_driver_conductor_on_publication_error(conductor_proxy->conductor, error);
    }
    else
    {
        aeron_driver_conductor_proxy_offer(conductor_proxy, (void *)error, cmd_length);
    }
}

