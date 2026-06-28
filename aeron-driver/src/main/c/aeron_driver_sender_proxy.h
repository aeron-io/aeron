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

#ifndef AERON_DRIVER_SENDER_PROXY_H
#define AERON_DRIVER_SENDER_PROXY_H

#include "aeron_driver_context.h"
#include "concurrent/aeron_spsc_rb.h"
#include "util/aeron_parse_util.h"

typedef struct aeron_driver_sender_stct aeron_driver_sender_t;
typedef struct aeron_send_channel_endpoint_stct aeron_send_channel_endpoint_t;
typedef struct aeron_network_publication_stct aeron_network_publication_t;

typedef struct aeron_driver_sender_proxy_stct
{
    aeron_driver_sender_t *sender;
    struct
    {
        aeron_on_endpoint_change_func_t on_add_endpoint;
        aeron_on_endpoint_change_func_t on_remove_endpoint;
    } log;
    aeron_spsc_rb_t *command_queue;
    int64_t *fail_counter;
}
aeron_driver_sender_proxy_t;

void aeron_driver_sender_proxy_on_add_endpoint(
    aeron_driver_sender_proxy_t *sender_proxy, aeron_send_channel_endpoint_t *endpoint);

void aeron_driver_sender_proxy_on_remove_endpoint(
    aeron_driver_sender_proxy_t *sender_proxy, aeron_send_channel_endpoint_t *endpoint);

void aeron_driver_sender_proxy_on_add_publication(
    aeron_driver_sender_proxy_t *sender_proxy, aeron_network_publication_t *publication);

void aeron_driver_sender_proxy_on_remove_publication(
    aeron_driver_sender_proxy_t *sender_proxy, aeron_network_publication_t *publication);

void aeron_driver_sender_proxy_on_resolution_change(
    aeron_driver_sender_proxy_t *sender_proxy,
    const char *endpoint_name,
    aeron_send_channel_endpoint_t *endpoint,
    struct sockaddr_storage *new_addr);

typedef struct aeron_command_destination_stct
{
    aeron_command_base_t base;
    int64_t destination_registration_id;
    struct sockaddr_storage control_address;
    void *endpoint;
    void *uri;
}
aeron_command_destination_t;

typedef struct aeron_command_destination_by_id_stct
{
    aeron_command_base_t base;
    struct sockaddr_storage control_address;
    void *endpoint;
    int64_t destination_registration_id;
}
aeron_command_destination_by_id_t;

typedef struct aeron_command_sender_resolution_change_stct
{
    aeron_command_base_t base;
    char endpoint_name[AERON_MAX_HOST_LENGTH + 1];
    void *endpoint;
    struct sockaddr_storage new_addr;
}
aeron_command_sender_resolution_change_t;

void aeron_driver_sender_proxy_on_add_destination(
    aeron_driver_sender_proxy_t *sender_proxy,
    aeron_send_channel_endpoint_t *endpoint,
    aeron_uri_t *uri,
    struct sockaddr_storage *addr,
    int64_t destination_registration_id);

void aeron_driver_sender_proxy_on_remove_destination(
    aeron_driver_sender_proxy_t *sender_proxy, aeron_send_channel_endpoint_t *endpoint, struct sockaddr_storage *addr);

void aeron_driver_sender_proxy_on_remove_destination_by_id(
    aeron_driver_sender_proxy_t *sender_proxy,
    aeron_send_channel_endpoint_t *endpoint,
    int64_t destination_registration_id);

#endif //AERON_DRIVER_SENDER_PROXY_H
