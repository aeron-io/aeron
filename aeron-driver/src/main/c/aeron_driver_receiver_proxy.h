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

#ifndef AERON_DRIVER_RECEIVER_PROXY_H
#define AERON_DRIVER_RECEIVER_PROXY_H

#include "aeron_driver_context.h"
#include "concurrent/aeron_spsc_rb.h"
#include "util/aeron_parse_util.h"

typedef struct aeron_driver_receiver_stct aeron_driver_receiver_t;
typedef struct aeron_receive_channel_endpoint_stct aeron_receive_channel_endpoint_t;
typedef struct aeron_receive_destination_stct aeron_receive_destination_t;
typedef struct aeron_publication_image_stct aeron_publication_image_t;

typedef struct aeron_driver_receiver_proxy_stct
{
    aeron_driver_receiver_t *receiver;
    struct
    {
        aeron_on_endpoint_change_func_t on_add_endpoint;
        aeron_on_endpoint_change_func_t on_remove_endpoint;
    } log;
    aeron_spsc_rb_t *command_queue;
    int64_t *fail_counter;
}
aeron_driver_receiver_proxy_t;

void aeron_driver_receiver_proxy_on_add_endpoint(
    aeron_driver_receiver_proxy_t *receiver_proxy, aeron_receive_channel_endpoint_t *endpoint);
void aeron_driver_receiver_proxy_on_remove_endpoint(
    aeron_driver_receiver_proxy_t *receiver_proxy, aeron_receive_channel_endpoint_t *endpoint);

typedef struct aeron_command_subscription_stct
{
    aeron_command_base_t base;
    void *endpoint;
    int32_t stream_id;
    int32_t session_id;
}
aeron_command_subscription_t;

void aeron_driver_receiver_proxy_on_add_subscription(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id);

void aeron_driver_receiver_proxy_on_remove_subscription(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id);

void aeron_driver_receiver_proxy_on_add_subscription_by_session(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id,
    int32_t session_id);

void aeron_driver_receiver_proxy_on_request_setup(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id,
    int32_t session_id);

void aeron_driver_receiver_proxy_on_remove_subscription_by_session(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id,
    int32_t session_id);

typedef struct aeron_command_add_rcv_destination_stct
{
    aeron_command_base_t base;
    void *endpoint;
    void *destination;
}
aeron_command_add_rcv_destination_t;

void aeron_driver_receiver_proxy_on_add_destination(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination);

typedef struct aeron_command_remove_rcv_destination_stct
{
    aeron_command_base_t base;
    void *endpoint;
    void *channel;
}
aeron_command_remove_rcv_destination_t;

void aeron_driver_receiver_proxy_on_remove_destination(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_udp_channel_t *channel);

typedef struct aeron_command_publication_image_stct
{
    aeron_command_base_t base;
    void *image;
}
aeron_command_publication_image_t;

typedef struct aeron_command_on_remove_matching_state_stct
{
    aeron_command_base_t base;
    void *endpoint;
    int32_t session_id;
    int32_t stream_id;
    uint32_t state;
}
aeron_command_on_remove_matching_state_t;

typedef struct aeron_command_receiver_resolution_change_stct
{
    aeron_command_base_t base;
    char endpoint_name[AERON_MAX_HOST_LENGTH + 1];
    void *endpoint;
    void *destination;
    struct sockaddr_storage new_addr;
}
aeron_command_receiver_resolution_change_t;

typedef struct aeron_command_receiver_invalidate_image_stct
{
    aeron_command_base_t base;
    int64_t image_correlation_id;
    int64_t position;
    int32_t reason_length;
    uint8_t reason_text[1];
}
aeron_command_receiver_invalidate_image_t;


void aeron_driver_receiver_proxy_on_add_publication_image(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_publication_image_t *image);
void aeron_driver_receiver_proxy_on_remove_publication_image(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_publication_image_t *image);
void aeron_driver_receiver_proxy_on_remove_cool_down(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t session_id,
    int32_t stream_id);
void aeron_driver_receiver_proxy_on_remove_init_in_progress(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t session_id,
    int32_t stream_id);
void aeron_driver_receiver_proxy_on_invalidate_image(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    int64_t image_correlation_id,
    int64_t position,
    int32_t reason_length,
    const char *reason);
void aeron_driver_receiver_proxy_on_resolution_change(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    const char *endpoint_name,
    void *endpoint,
    void *destination,
    struct sockaddr_storage *new_addr);


#endif //AERON_DRIVER_RECEIVER_PROXY_H
