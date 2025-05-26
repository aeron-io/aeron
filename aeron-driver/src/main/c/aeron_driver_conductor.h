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
#ifndef AERON_DRIVER_CONDUCTOR_H
#define AERON_DRIVER_CONDUCTOR_H

#include "aeron_driver_common.h"
#include "aeron_driver_context.h"
#include "uri/aeron_uri.h"
#include "concurrent/aeron_mpsc_rb.h"
#include "concurrent/aeron_broadcast_transmitter.h"
#include "concurrent/aeron_distinct_error_log.h"
#include "concurrent/aeron_counters_manager.h"
#include "concurrent/aeron_executor.h"
#include "command/aeron_control_protocol.h"
#include "aeron_system_counters.h"
#include "aeron_ipc_publication.h"
#include "collections/aeron_str_to_ptr_hash_map.h"
#include "media/aeron_send_channel_endpoint.h"
#include "media/aeron_receive_channel_endpoint.h"
#include "aeron_driver_conductor_proxy.h"
#include "aeron_publication_image.h"
#include "reports/aeron_loss_reporter.h"
#include "util/aeron_deque.h"

#define AERON_DRIVER_CONDUCTOR_LINGER_RESOURCE_TIMEOUT_NS (5 * 1000 * 1000 * 1000LL)
#define AERON_DRIVER_CONDUCTOR_CLOCK_UPDATE_INTERNAL_NS (1000 * 1000LL)

typedef struct aeron_publication_link_stct
{
    aeron_driver_managed_resource_t *resource;
    int64_t registration_id;
}
aeron_publication_link_t;

#define AERON_PUBLICATION_LINK_INIT(_link, _resource, _registration_id) \
do {                                                                    \
    link->resource = _resource;                                         \
    link->registration_id = registration_id;                            \
} while (0)

typedef struct aeron_counter_link_stct
{
    int32_t counter_id;
    int64_t registration_id;
}
aeron_counter_link_t;

typedef struct aeron_client_stct
{
    bool reached_end_of_life;
    bool closed_by_command;
    int64_t client_id;
    int64_t client_liveness_timeout_ms;

    aeron_atomic_counter_t heartbeat_timestamp;

    struct publication_link_stct
    {
        size_t length;
        size_t capacity;
        aeron_publication_link_t *array;
    }
    publication_links;

    struct counter_link_stct
    {
        size_t length;
        size_t capacity;
        aeron_counter_link_t *array;
    }
    counter_links;
}
aeron_client_t;

typedef struct aeron_subscribable_list_entry_stct
{
    int32_t counter_id;
    aeron_subscribable_t *subscribable;
}
aeron_subscribable_list_entry_t;

typedef struct aeron_subscription_link_stct
{
    char channel[AERON_URI_MAX_LENGTH];
    bool is_tether;
    bool is_sparse;
    bool is_reliable;
    bool is_rejoin;
    bool has_session_id;
    bool is_response;
    aeron_inferable_boolean_t group;
    int32_t stream_id;
    int32_t session_id;
    int32_t channel_length;
    int64_t registration_id;
    int64_t client_id;

    aeron_receive_channel_endpoint_t *endpoint;
    aeron_udp_channel_t *spy_channel;

    struct subscribable_list_stct
    {
        size_t length;
        size_t capacity;
        aeron_subscribable_list_entry_t *array;
    }
    subscribable_list;
}
aeron_subscription_link_t;

typedef struct aeron_ipc_publication_entry_stct
{
    aeron_ipc_publication_t *publication;
}
 aeron_ipc_publication_entry_t;

typedef struct aeron_network_publication_entry_stct
{
    aeron_network_publication_t *publication;
}
aeron_network_publication_entry_t;

typedef struct aeron_send_channel_endpoint_entry_stct
{
    aeron_send_channel_endpoint_t *endpoint;
}
aeron_send_channel_endpoint_entry_t;

typedef struct aeron_receive_channel_endpoint_entry_stct
{
    aeron_receive_channel_endpoint_t *endpoint;
}
aeron_receive_channel_endpoint_entry_t;

typedef struct aeron_publication_image_entry_stct
{
    aeron_publication_image_t *image;
}
aeron_publication_image_entry_t;

typedef struct aeron_linger_resource_entry_stct
{
    bool has_reached_end_of_life;
    uint8_t *buffer;
    int64_t timeout_ns;
}
aeron_linger_resource_entry_t;

typedef bool (*aeron_end_of_life_resource_free_t)(void *resource);

struct aeron_end_of_life_resource_stct
{
    void *resource;
    aeron_end_of_life_resource_free_t free_func;
};
typedef struct aeron_end_of_life_resource_stct aeron_end_of_life_resource_t;

typedef struct aeron_driver_conductor_stct aeron_driver_conductor_t;

typedef struct aeron_driver_conductor_stct
{
    aeron_driver_context_t *context;
    aeron_mpsc_rb_t to_driver_commands;
    aeron_broadcast_transmitter_t to_clients;
    aeron_distinct_error_log_t error_log;
    aeron_counters_manager_t counters_manager;
    aeron_system_counters_t system_counters;
    aeron_driver_conductor_proxy_t conductor_proxy;
    aeron_loss_reporter_t loss_reporter;
    aeron_name_resolver_t name_resolver;
    aeron_executor_t executor;

    aeron_str_to_ptr_hash_map_t send_channel_endpoint_by_channel_map;
    aeron_str_to_ptr_hash_map_t receive_channel_endpoint_by_channel_map;

    struct client_stct
    {
        size_t length;
        size_t capacity;
        aeron_client_t *array;
        void (*on_time_event)(aeron_driver_conductor_t *, aeron_client_t *, int64_t, int64_t);
        bool (*has_reached_end_of_life)(aeron_driver_conductor_t *, aeron_client_t *);
        void (*delete_func)(aeron_driver_conductor_t *, aeron_client_t *);
        void (*free_func)(aeron_driver_conductor_t *, aeron_client_t *);
    }
    clients;

    struct ipc_subscriptions_stct
    {
        size_t length;
        size_t capacity;
        aeron_subscription_link_t *array;
    }
    ipc_subscriptions;

    struct ipc_publication_stct
    {
        size_t length;
        size_t capacity;
        aeron_ipc_publication_entry_t *array;
        void (*on_time_event)(aeron_driver_conductor_t *, aeron_ipc_publication_entry_t *, int64_t, int64_t);
        bool (*has_reached_end_of_life)(aeron_driver_conductor_t *, aeron_ipc_publication_entry_t *);
        void (*delete_func)(aeron_driver_conductor_t *, aeron_ipc_publication_entry_t *);
        void (*free_func)(aeron_driver_conductor_t *, aeron_ipc_publication_entry_t *);
    }
    ipc_publications;

    struct network_subscriptions_stct
    {
        size_t length;
        size_t capacity;
        aeron_subscription_link_t *array;
    }
    network_subscriptions;

    struct spy_subscriptions_stct
    {
        size_t length;
        size_t capacity;
        aeron_subscription_link_t *array;
    }
    spy_subscriptions;

    struct network_publication_stct
    {
        size_t length;
        size_t capacity;
        aeron_network_publication_entry_t *array;
        void (*on_time_event)(aeron_driver_conductor_t *, aeron_network_publication_entry_t *, int64_t, int64_t);
        bool (*has_reached_end_of_life)(aeron_driver_conductor_t *, aeron_network_publication_entry_t *);
        void (*delete_func)(aeron_driver_conductor_t *, aeron_network_publication_entry_t *);
        void (*free_func)(aeron_driver_conductor_t *, aeron_network_publication_entry_t *);
    }
    network_publications;

    struct send_channel_endpoint_stct
    {
        aeron_send_channel_endpoint_entry_t *array;
        size_t length;
        size_t capacity;
        void (*on_time_event)(aeron_driver_conductor_t *, aeron_send_channel_endpoint_entry_t *, int64_t, int64_t);
        bool (*has_reached_end_of_life)(aeron_driver_conductor_t *, aeron_send_channel_endpoint_entry_t *);
        void (*delete_func)(aeron_driver_conductor_t *, aeron_send_channel_endpoint_entry_t *);
        void (*free_func)(aeron_driver_conductor_t *, aeron_send_channel_endpoint_entry_t *);
    }
    send_channel_endpoints;

    struct receive_channel_endpoint_stct
    {
        size_t length;
        size_t capacity;
        aeron_receive_channel_endpoint_entry_t *array;
        void (*on_time_event)(aeron_driver_conductor_t *, aeron_receive_channel_endpoint_entry_t *, int64_t, int64_t);
        bool (*has_reached_end_of_life)(aeron_driver_conductor_t *, aeron_receive_channel_endpoint_entry_t *);
        void (*delete_func)(aeron_driver_conductor_t *, aeron_receive_channel_endpoint_entry_t *);
        void (*free_func)(aeron_driver_conductor_t *, aeron_receive_channel_endpoint_entry_t *);
    }
    receive_channel_endpoints;

    struct publication_image_stct
    {
        size_t length;
        size_t capacity;
        aeron_publication_image_entry_t *array;
        void (*on_time_event)(aeron_driver_conductor_t *, aeron_publication_image_entry_t *, int64_t, int64_t);
        bool (*has_reached_end_of_life)(aeron_driver_conductor_t *, aeron_publication_image_entry_t *);
        void (*delete_func)(aeron_driver_conductor_t *, aeron_publication_image_entry_t *);
        void (*free_func)(aeron_driver_conductor_t *, aeron_publication_image_entry_t *);
    }
    publication_images;

    struct aeron_driver_conductor_lingering_resources_stct
    {
        size_t length;
        size_t capacity;
        aeron_linger_resource_entry_t *array;
        void (*on_time_event)(aeron_driver_conductor_t *, aeron_linger_resource_entry_t *, int64_t, int64_t);
        bool (*has_reached_end_of_life)(aeron_driver_conductor_t *, aeron_linger_resource_entry_t *);
        void (*delete_func)(aeron_driver_conductor_t *, aeron_linger_resource_entry_t *);
        void (*free_func)(aeron_driver_conductor_t *, aeron_linger_resource_entry_t *);
    }
    lingering_resources;

    aeron_deque_t end_of_life_queue;

    int64_t *errors_counter;
    int64_t *images_rejected_counter;
    int64_t *unblocked_commands_counter;
    int64_t *client_timeouts_counter;

    int64_t clock_update_deadline_ns;

    int32_t next_session_id;
    int32_t publication_reserved_session_id_low;
    int32_t publication_reserved_session_id_high;
    int64_t timeout_check_deadline_ns;
    int64_t time_of_last_to_driver_position_change_ns;
    int64_t last_command_consumer_position;

    bool async_client_command_in_flight;

    uint8_t padding[AERON_CACHE_LINE_LENGTH];
}
aeron_driver_conductor_t;

void aeron_client_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_client_t *client, int64_t now_ns, int64_t now_ms);

bool aeron_client_has_reached_end_of_life(aeron_driver_conductor_t *conductor, aeron_client_t *client);

void aeron_driver_conductor_add_end_of_life_resource(
    aeron_driver_conductor_t *conductor,
    void *resource,
    aeron_end_of_life_resource_free_t free_func);

void aeron_client_delete(aeron_driver_conductor_t *conductor, aeron_client_t *);

bool aeron_client_free(void *);

void aeron_ipc_publication_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_ipc_publication_entry_t *entry, int64_t now_ns, int64_t now_ms);

bool aeron_ipc_publication_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_ipc_publication_entry_t *entry);

void aeron_ipc_publication_entry_delete(aeron_driver_conductor_t *conductor, aeron_ipc_publication_entry_t *);

void aeron_ipc_publication_entry_free(aeron_driver_conductor_t *conductor, aeron_ipc_publication_entry_t *entry);

void aeron_network_publication_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_network_publication_entry_t *entry, int64_t now_ns, int64_t now_ms);

bool aeron_network_publication_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_network_publication_entry_t *entry);

void aeron_network_publication_entry_delete(aeron_driver_conductor_t *conductor, aeron_network_publication_entry_t *);

void aeron_network_publication_entry_free(aeron_driver_conductor_t *conductor, aeron_network_publication_entry_t *entry);

void aeron_send_channel_endpoint_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_send_channel_endpoint_entry_t *entry, int64_t now_ns, int64_t now_ms);

bool aeron_send_channel_endpoint_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_send_channel_endpoint_entry_t *entry);

void aeron_send_channel_endpoint_entry_delete(
    aeron_driver_conductor_t *conductor, aeron_send_channel_endpoint_entry_t *);

void aeron_receive_channel_endpoint_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_receive_channel_endpoint_entry_t *entry, int64_t now_ns, int64_t now_ms);

bool aeron_receive_channel_endpoint_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_receive_channel_endpoint_entry_t *entry);

void aeron_receive_channel_endpoint_entry_delete(
    aeron_driver_conductor_t *conductor, aeron_receive_channel_endpoint_entry_t *);

void aeron_publication_image_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_publication_image_entry_t *entry, int64_t now_ns, int64_t now_ms);

bool aeron_publication_image_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_publication_image_entry_t *entry);

void aeron_publication_image_entry_delete(aeron_driver_conductor_t *conductor, aeron_publication_image_entry_t *);

void aeron_publication_image_entry_free(aeron_driver_conductor_t *conductor, aeron_publication_image_entry_t *entry);

void aeron_linger_resource_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_linger_resource_entry_t *entry, int64_t now_ns, int64_t now_ms);

bool aeron_linger_resource_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_linger_resource_entry_t *entry);

void aeron_linger_resource_entry_delete(aeron_driver_conductor_t *conductor, aeron_linger_resource_entry_t *);

void aeron_driver_conductor_image_transition_to_linger(
    aeron_driver_conductor_t *conductor, aeron_publication_image_t *image);

int aeron_driver_conductor_init(aeron_driver_conductor_t *conductor, aeron_driver_context_t *context);

void aeron_driver_conductor_client_transmit(
    aeron_driver_conductor_t *conductor,
    int32_t msg_type_id,
    const void *message,
    size_t length);

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
    size_t source_identity_length);

void aeron_driver_conductor_on_unavailable_image(
    aeron_driver_conductor_t *conductor,
    int64_t correlation_id,
    int64_t subscription_registration_id,
    int32_t stream_id,
    const char *channel,
    size_t channel_length);

void aeron_driver_conductor_on_counter_ready(
    aeron_driver_conductor_t *conductor, int64_t registration_id, int32_t counter_id);

void aeron_driver_conductor_on_unavailable_counter(
    aeron_driver_conductor_t *conductor, int64_t registration_id, int32_t counter_id);

void aeron_driver_conductor_on_client_timeout(aeron_driver_conductor_t *conductor, int64_t correlation_id);

void aeron_driver_conductor_on_static_counter(
    aeron_driver_conductor_t *conductor, int64_t correlation_id, int32_t counter_id);

void aeron_driver_conductor_cleanup_spies(
    aeron_driver_conductor_t *conductor, aeron_network_publication_t *publication);

void aeron_driver_conductor_cleanup_network_publication(
    aeron_driver_conductor_t *conductor, aeron_network_publication_t *publication);

aeron_rb_read_action_t aeron_driver_conductor_on_command(
    int32_t msg_type_id, const void *message, size_t length, void *clientd);

int aeron_driver_conductor_do_work(void *clientd);

void aeron_driver_conductor_on_close(void *clientd);

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
    const char *log_file_name);

void aeron_driver_conductor_unlink_subscribable(aeron_subscription_link_t *link, aeron_subscribable_t *subscribable);

void aeron_driver_conductor_unlink_all_subscribable(
    aeron_driver_conductor_t *conductor, aeron_subscription_link_t *link);

int aeron_driver_conductor_link_ipc_subscriptions(
    aeron_driver_conductor_t *conductor, aeron_ipc_publication_t *publication);

int aeron_driver_conductor_on_add_ipc_publication(
    aeron_driver_conductor_t *conductor, aeron_publication_command_t *command, bool is_exclusive);

int aeron_driver_conductor_on_add_network_publication(
    aeron_driver_conductor_t *conductor, aeron_publication_command_t *command, bool is_exclusive);

int aeron_driver_conductor_on_remove_publication(aeron_driver_conductor_t *conductor, aeron_remove_publication_command_t *command);

int aeron_driver_conductor_on_add_ipc_subscription(
    aeron_driver_conductor_t *conductor, aeron_subscription_command_t *command);

int aeron_driver_conductor_on_add_spy_subscription(
    aeron_driver_conductor_t *conductor, aeron_subscription_command_t *command);

int aeron_driver_conductor_on_add_network_subscription(
    aeron_driver_conductor_t *conductor, aeron_subscription_command_t *command);

int aeron_driver_conductor_on_remove_subscription(aeron_driver_conductor_t *conductor, aeron_remove_subscription_command_t *command);

int aeron_driver_conductor_on_client_keepalive(aeron_driver_conductor_t *conductor, int64_t client_id);

int aeron_driver_conductor_on_add_send_destination(
    aeron_driver_conductor_t *conductor, aeron_destination_command_t *command);

int aeron_driver_conductor_on_remove_send_destination(
    aeron_driver_conductor_t *conductor, aeron_destination_command_t *command);

int aeron_driver_conductor_on_remove_receive_send_destination_by_id(
    aeron_driver_conductor_t *conductor, aeron_destination_by_id_command_t *command);

int aeron_driver_conductor_on_add_receive_ipc_destination(
    aeron_driver_conductor_t *conductor,
    aeron_destination_command_t *command);

int aeron_driver_conductor_on_add_receive_spy_destination(
    aeron_driver_conductor_t *conductor,
    aeron_destination_command_t *command);

int aeron_driver_conductor_on_add_receive_network_destination(
    aeron_driver_conductor_t *conductor,
    aeron_destination_command_t *command);

int aeron_driver_conductor_on_remove_receive_ipc_destination(
    aeron_driver_conductor_t *conductor,
    aeron_destination_command_t *command);

int aeron_driver_conductor_on_remove_receive_spy_destination(
    aeron_driver_conductor_t *conductor,
    aeron_destination_command_t *command);

int aeron_driver_conductor_on_remove_receive_network_destination(
    aeron_driver_conductor_t *conductor,
    aeron_destination_command_t *command);

void aeron_driver_conductor_on_delete_receive_destination(void *clientd, void *cmd);

void aeron_driver_conductor_on_delete_send_destination(void *clientd, void *cmd);

int aeron_driver_conductor_on_add_counter(aeron_driver_conductor_t *conductor, aeron_counter_command_t *command);

int aeron_driver_conductor_on_remove_counter(aeron_driver_conductor_t *conductor, aeron_remove_counter_command_t *command);

int aeron_driver_conductor_on_add_static_counter(aeron_driver_conductor_t *conductor, aeron_static_counter_command_t *command);

int aeron_driver_conductor_on_client_close(aeron_driver_conductor_t *conductor, aeron_correlated_command_t *command);

int aeron_driver_conductor_on_terminate_driver(
    aeron_driver_conductor_t *conductor, aeron_terminate_driver_command_t *command);

int aeron_driver_conductor_on_invalidate_image(
    aeron_driver_conductor_t *conductor, aeron_reject_image_command_t *command);

void aeron_driver_conductor_unlink_ipc_subscriptions(
    aeron_driver_conductor_t *conductor, aeron_ipc_publication_t *publication);

void aeron_driver_conductor_on_create_publication_image(void *clientd, void *item);

void aeron_driver_conductor_on_re_resolve_endpoint(void *clientd, void *item);

void aeron_driver_conductor_on_re_resolve_control(void *clientd, void *item);

void aeron_driver_conductor_on_receive_endpoint_removed(void *clientd, void *item);

void aeron_driver_conductor_on_response_setup(void *clientd, void *item);

void aeron_driver_conductor_on_response_connected(void *clientd, void *item);

void aeron_driver_conductor_on_publication_error(void *clientd, void *item);

void aeron_driver_conductor_on_release_resource(void *clientd, void *item);

inline bool aeron_driver_conductor_is_subscribable_linked(
    aeron_subscription_link_t *link, aeron_subscribable_t *subscribable)
{
    bool result = false;

    for (size_t i = 0; i < link->subscribable_list.length; i++)
    {
        aeron_subscribable_list_entry_t *entry = &link->subscribable_list.array[i];

        if (subscribable == entry->subscribable)
        {
            result = true;
            break;
        }
    }

    return result;
}

inline size_t aeron_driver_conductor_num_clients(aeron_driver_conductor_t *conductor)
{
    return conductor->clients.length;
}

inline size_t aeron_driver_conductor_num_ipc_publications(aeron_driver_conductor_t *conductor)
{
    return conductor->ipc_publications.length;
}

inline size_t aeron_driver_conductor_num_ipc_subscriptions(aeron_driver_conductor_t *conductor)
{
    return conductor->ipc_subscriptions.length;
}

inline size_t aeron_driver_conductor_num_network_publications(aeron_driver_conductor_t *conductor)
{
    return conductor->network_publications.length;
}

inline size_t aeron_driver_conductor_num_network_subscriptions(aeron_driver_conductor_t *conductor)
{
    return conductor->network_subscriptions.length;
}

inline size_t aeron_driver_conductor_num_spy_subscriptions(aeron_driver_conductor_t *conductor)
{
    return conductor->spy_subscriptions.length;
}

inline size_t aeron_driver_conductor_num_send_channel_endpoints(aeron_driver_conductor_t *conductor)
{
    return conductor->send_channel_endpoints.length;
}

inline size_t aeron_driver_conductor_num_receive_channel_endpoints(aeron_driver_conductor_t *conductor)
{
    return conductor->receive_channel_endpoints.length;
}

inline size_t aeron_driver_conductor_num_images(aeron_driver_conductor_t *conductor)
{
    return conductor->publication_images.length;
}

inline size_t aeron_driver_conductor_num_active_ipc_subscriptions(
    aeron_driver_conductor_t *conductor, int32_t stream_id)
{
    size_t num = 0;

    for (size_t i = 0, length = conductor->ipc_subscriptions.length; i < length; i++)
    {
        aeron_subscription_link_t *link = &conductor->ipc_subscriptions.array[i];

        if (stream_id == link->stream_id)
        {
            num += link->subscribable_list.length;
        }
    }

    return num;
}

inline size_t aeron_driver_conductor_num_active_network_subscriptions(
    aeron_driver_conductor_t *conductor, const char *original_uri, int32_t stream_id)
{
    size_t num = 0;

    for (size_t i = 0, length = conductor->network_subscriptions.length; i < length; i++)
    {
        aeron_subscription_link_t *link = &conductor->network_subscriptions.array[i];
        aeron_udp_channel_t *udp_channel = link->endpoint->conductor_fields.udp_channel;

        if (stream_id == link->stream_id &&
            strncmp(original_uri, udp_channel->original_uri, udp_channel->uri_length) == 0)
        {
            num += link->subscribable_list.length;
        }
    }

    return num;
}

inline size_t aeron_driver_conductor_num_active_spy_subscriptions(
    aeron_driver_conductor_t *conductor, const char *original_uri, int32_t stream_id)
{
    size_t num = 0;

    for (size_t i = 0, length = conductor->spy_subscriptions.length; i < length; i++)
    {
        aeron_subscription_link_t *link = &conductor->spy_subscriptions.array[i];
        aeron_udp_channel_t *udp_channel = link->spy_channel;

        if (stream_id == link->stream_id &&
            strncmp(original_uri, udp_channel->original_uri, udp_channel->uri_length) == 0)
        {
            num += link->subscribable_list.length;
        }
    }

    return num;
}

inline aeron_send_channel_endpoint_t * aeron_driver_conductor_find_send_channel_endpoint(
    aeron_driver_conductor_t *conductor, const char *original_uri)
{
    for (size_t i = 0, length = conductor->send_channel_endpoints.length; i < length; i++)
    {
        aeron_send_channel_endpoint_t *endpoint = conductor->send_channel_endpoints.array[i].endpoint;
        const aeron_udp_channel_t *udp_channel = endpoint->conductor_fields.udp_channel;

        if (strncmp(original_uri, udp_channel->original_uri, udp_channel->uri_length) == 0)
        {
            return endpoint;
        }
    }

    return NULL;
}

inline aeron_receive_channel_endpoint_t * aeron_driver_conductor_find_receive_channel_endpoint(
    aeron_driver_conductor_t *conductor, const char *original_uri)
{
    for (size_t i = 0, length = conductor->receive_channel_endpoints.length; i < length; i++)
    {
        aeron_receive_channel_endpoint_t *endpoint = conductor->receive_channel_endpoints.array[i].endpoint;
        aeron_udp_channel_t *udp_channel = endpoint->conductor_fields.udp_channel;

        if (strncmp(original_uri, udp_channel->original_uri, udp_channel->uri_length) == 0)
        {
            return endpoint;
        }
    }

    return NULL;
}

inline aeron_ipc_publication_t * aeron_driver_conductor_find_ipc_publication(
    aeron_driver_conductor_t *conductor, int64_t id)
{
    for (size_t i = 0, length = conductor->ipc_publications.length; i < length; i++)
    {
        aeron_ipc_publication_t *publication = conductor->ipc_publications.array[i].publication;

        if (id == publication->conductor_fields.managed_resource.registration_id)
        {
            return publication;
        }
    }

    return NULL;
}

inline aeron_network_publication_t * aeron_driver_conductor_find_network_publication(
    aeron_driver_conductor_t *conductor, int64_t id)
{
    for (size_t i = 0, length = conductor->network_publications.length; i < length; i++)
    {
        aeron_network_publication_t *publication = conductor->network_publications.array[i].publication;

        if (id == publication->conductor_fields.managed_resource.registration_id)
        {
            return publication;
        }
    }

    return NULL;
}

inline aeron_network_publication_t *aeron_driver_conductor_find_network_publication_by_tag(
    aeron_driver_conductor_t *conductor, int64_t tag_id)
{
    for (size_t i = 0, length = conductor->network_publications.length; i < length; i++)
    {
        aeron_network_publication_t *publication = conductor->network_publications.array[i].publication;

        if (tag_id == publication->tag && AERON_URI_INVALID_TAG != publication->tag)
        {
            return publication;
        }
    }

    return NULL;
}

inline void aeron_driver_init_subscription_channel(size_t uri_length, const char *uri, aeron_subscription_link_t *link)
{
    size_t copy_length = sizeof(link->channel) - 1;
    copy_length = uri_length < copy_length ? uri_length : copy_length;

    strncpy(link->channel, uri, copy_length);
    link->channel[copy_length] = '\0';
    link->channel_length = (int32_t)copy_length;
}

#endif //AERON_DRIVER_CONDUCTOR_H
