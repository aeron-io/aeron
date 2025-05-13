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

#ifndef AERON_DRIVER_CONDUCTOR_PROXY_H
#define AERON_DRIVER_CONDUCTOR_PROXY_H

#include "aeron_driver_context.h"

typedef enum aeron_driver_conductor_resource_type_en
{
    AERON_DRIVER_CONDUCTOR_RESOURCE_TYPE_CLIENT,
    AERON_DRIVER_CONDUCTOR_RESOURCE_TYPE_IPC_PUBLICATION,
    AERON_DRIVER_CONDUCTOR_RESOURCE_TYPE_NETWORK_PUBLICATION,
    AERON_DRIVER_CONDUCTOR_RESOURCE_TYPE_SEND_CHANNEL_ENDPOINT,
    AERON_DRIVER_CONDUCTOR_RESOURCE_TYPE_PUBLICATION_IMAGE,
    AERON_DRIVER_CONDUCTOR_RESOURCE_TYPE_LINGER_RESOURCE
}
aeron_driver_conductor_resource_type_t;

typedef struct aeron_driver_conductor_stct aeron_driver_conductor_t;

typedef struct aeron_driver_conductor_proxy_stct
{
    aeron_driver_conductor_t *conductor;
    aeron_threading_mode_t threading_mode;
    aeron_mpsc_rb_t *command_queue;
    int64_t *fail_counter;
}
aeron_driver_conductor_proxy_t;

typedef struct aeron_command_create_publication_image_stct
{
    aeron_command_base_t base;
    int32_t session_id;
    int32_t stream_id;
    int32_t initial_term_id;
    int32_t active_term_id;
    int32_t term_offset;
    int32_t term_length;
    int32_t mtu_length;
    uint8_t flags;
    struct sockaddr_storage control_address;
    struct sockaddr_storage src_address;
    void *endpoint;
    void *destination;
}
aeron_command_create_publication_image_t;

typedef struct aeron_command_re_resolve_stct
{
    aeron_command_base_t base;
    const char *endpoint_name;
    void *endpoint;
    void *destination;
    struct sockaddr_storage existing_addr;
}
aeron_command_re_resolve_t;

typedef struct aeron_command_delete_destination_stct
{
    aeron_command_base_t base;
    void *endpoint;
    void *destination;
    void *channel;
}
aeron_command_delete_destination_t;

struct aeron_command_response_connected_stct
{
    aeron_command_base_t base;
    int64_t response_correlation_id;
};
typedef struct aeron_command_response_connected_stct aeron_command_response_connected_t;

struct aeron_command_response_setup_stct
{
    aeron_command_base_t base;
    int64_t response_correlation_id;
    int32_t response_session_id;
};
typedef struct aeron_command_response_setup_stct aeron_command_response_setup_t;

struct aeron_command_release_resource_stct
{
    aeron_command_base_t base;
    aeron_driver_conductor_resource_type_t resource_type;
};
typedef struct aeron_command_release_resource_stct aeron_command_release_resource_t;

struct aeron_command_publication_error_stct
{
    aeron_command_base_t base;
    int64_t registration_id;
    int64_t destination_registration_id;
    int32_t session_id;
    int32_t stream_id;
    int64_t receiver_id;
    int64_t group_tag;
    struct sockaddr_storage src_address;
    int32_t error_code;
    int32_t error_length;
    uint8_t error_text[1];
};
typedef struct aeron_command_publication_error_stct aeron_command_publication_error_t;

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
    void *destination);

void aeron_driver_conductor_proxy_on_re_resolve_endpoint(
    aeron_driver_conductor_proxy_t *conductor_proxy,
    const char *endpoint_name,
    void *endpoint,
    struct sockaddr_storage *existing_addr);

void aeron_driver_conductor_proxy_on_re_resolve_control(
    aeron_driver_conductor_proxy_t *conductor_proxy,
    const char *endpoint_name,
    void *endpoint,
    void *destination,
    struct sockaddr_storage *existing_addr);

void aeron_driver_conductor_proxy_on_delete_receive_destination(
    aeron_driver_conductor_proxy_t *conductor_proxy,
    void *endpoint,
    void *destination,
    void *channel);

void aeron_driver_conductor_proxy_on_delete_send_destination(
    aeron_driver_conductor_proxy_t *conductor_proxy,
    void *removed_uri);

void aeron_driver_conductor_proxy_on_receive_endpoint_removed(
    aeron_driver_conductor_proxy_t *conductor_proxy,
    void *endpoint);

void aeron_driver_conductor_proxy_on_response_setup(
    aeron_driver_conductor_proxy_t *conductor_proxy,
    int64_t response_correlation_id,
    int32_t response_session_id);

void aeron_driver_conductor_proxy_on_response_connected(
    aeron_driver_conductor_proxy_t *conductor_proxy,
    int64_t response_correlation_id);

void aeron_driver_conductor_proxy_on_release_resource(
    aeron_driver_conductor_proxy_t *conductor_proxy,
    void *managed_resource,
    aeron_driver_conductor_resource_type_t resource_type);

#define AERON_COMMAND_PUBLICATION_ERROR_MAX_LENGTH (sizeof(aeron_command_publication_error_t) + AERON_ERROR_MAX_TEXT_LENGTH)

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
    const uint8_t *error_text);

#endif //AERON_DRIVER_CONDUCTOR_PROXY_H
