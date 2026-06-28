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

#include "aeron_driver_sender.h"

void aeron_driver_sender_proxy_offer(aeron_driver_sender_proxy_t *sender_proxy, void *cmd, size_t length)
{
    while (AERON_RB_SUCCESS != aeron_spsc_rb_write(sender_proxy->command_queue, 1, cmd, length))
    {
        aeron_counter_increment_release(sender_proxy->fail_counter);
        sched_yield();
    }
}

void aeron_driver_sender_proxy_on_add_endpoint(
    aeron_driver_sender_proxy_t *sender_proxy, aeron_send_channel_endpoint_t *endpoint)
{
    sender_proxy->log.on_add_endpoint(endpoint->conductor_fields.udp_channel);
    aeron_command_base_t cmd =
        {
            .func = aeron_driver_sender_on_add_endpoint,
            .item = endpoint
        };

    aeron_driver_sender_proxy_offer(sender_proxy, &cmd, sizeof(cmd));
}

void aeron_driver_sender_proxy_on_remove_endpoint(
    aeron_driver_sender_proxy_t *sender_proxy, aeron_send_channel_endpoint_t *endpoint)
{
    sender_proxy->log.on_remove_endpoint(endpoint->conductor_fields.udp_channel);
    aeron_command_base_t cmd =
        {
            .func = aeron_driver_sender_on_remove_endpoint,
            .item = endpoint
        };

    aeron_driver_sender_proxy_offer(sender_proxy, &cmd, sizeof(cmd));
}

void aeron_driver_sender_proxy_on_add_publication(
    aeron_driver_sender_proxy_t *sender_proxy, aeron_network_publication_t *publication)
{
    aeron_command_base_t cmd =
        {
            .func = aeron_driver_sender_on_add_publication,
            .item = publication
        };

    aeron_driver_sender_proxy_offer(sender_proxy, &cmd, sizeof(cmd));
}

void aeron_driver_sender_proxy_on_remove_publication(
    aeron_driver_sender_proxy_t *sender_proxy, aeron_network_publication_t *publication)
{
    aeron_command_base_t cmd =
        {
            .func = aeron_driver_sender_on_remove_publication,
            .item = publication
        };

    aeron_driver_sender_proxy_offer(sender_proxy, &cmd, sizeof(cmd));
}

void aeron_driver_sender_proxy_on_add_destination(
    aeron_driver_sender_proxy_t *sender_proxy,
    aeron_send_channel_endpoint_t *endpoint,
    aeron_uri_t *uri,
    struct sockaddr_storage *addr,
    int64_t destination_registration_id)
{
    aeron_command_destination_t cmd =
        {
            .base = { .func = aeron_driver_sender_on_add_destination, .item = NULL },
            .destination_registration_id = destination_registration_id,
            .endpoint = endpoint,
            .uri = uri
        };
    memcpy(&cmd.control_address, addr, sizeof(cmd.control_address));

    aeron_driver_sender_proxy_offer(sender_proxy, &cmd, sizeof(cmd));
}

void aeron_driver_sender_proxy_on_remove_destination(
    aeron_driver_sender_proxy_t *sender_proxy, aeron_send_channel_endpoint_t *endpoint, struct sockaddr_storage *addr)
{
    aeron_command_destination_t cmd =
        {
            .base = { .func = aeron_driver_sender_on_remove_destination, .item = NULL },
            .endpoint = endpoint
        };
    memcpy(&cmd.control_address, addr, sizeof(cmd.control_address));

    aeron_driver_sender_proxy_offer(sender_proxy, &cmd, sizeof(cmd));
}

void aeron_driver_sender_proxy_on_remove_destination_by_id(
    aeron_driver_sender_proxy_t *sender_proxy,
    aeron_send_channel_endpoint_t *endpoint,
    int64_t destination_registration_id)
{
    aeron_command_destination_by_id_t cmd =
        {
            .base = { .func = aeron_driver_sender_on_remove_destination_by_id, .item = NULL },
            .endpoint = endpoint,
            .destination_registration_id = destination_registration_id
        };

    aeron_driver_sender_proxy_offer(sender_proxy, &cmd, sizeof(cmd));
}

void aeron_driver_sender_proxy_on_resolution_change(
    aeron_driver_sender_proxy_t *sender_proxy,
    const char *endpoint_name,
    aeron_send_channel_endpoint_t *endpoint,
    struct sockaddr_storage *new_addr)
{
    aeron_command_sender_resolution_change_t cmd =
        {
            .base = { .func = aeron_driver_sender_on_resolution_change, .item = NULL },
            .endpoint = endpoint
        };
    memcpy(&cmd.new_addr, new_addr, sizeof(cmd.new_addr));

    size_t endpoint_name_length = strlen(endpoint_name);
    memcpy(&cmd.endpoint_name, endpoint_name, endpoint_name_length);
    cmd.endpoint_name[endpoint_name_length] = '\0';

    aeron_driver_sender_proxy_offer(sender_proxy, &cmd, sizeof(cmd));
}
