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

#include <string.h>
#include <inttypes.h>

#if defined(AERON_COMPILER_MSVC)

#include <winsock2.h>
#include <inaddr.h>

#define in_addr_t ULONG
#else
#include <unistd.h>
#endif

#include "util/aeron_error.h"
#include "util/aeron_arrayutil.h"
#include "aeron_name_resolver.h"
#include "media/aeron_udp_channel_transport.h"
#include "media/aeron_udp_transport_poller.h"
#include "aeron_name_resolver_cache.h"
#include "aeron_driver_name_resolver.h"

#if !defined(HAVE_STRUCT_MMSGHDR)
struct mmsghdr
{
    struct msghdr msg_hdr;
    unsigned int msg_len;
};
#endif

#define AERON_NAME_RESOLVER_DRIVER_DUTY_CYCLE_MS (10)
#define AERON_NAME_RESOLVER_DRIVER_NUM_RECV_BUFFERS (1)

#define AERON_NAME_RESOLVER_DRIVER_MAX_BOOTSTRAP_NEIGHBORS (20)

typedef struct aeron_driver_name_resolver_neighbor_stct
{
    aeron_name_resolver_cache_addr_t cache_addr;
    int64_t time_of_last_activity_ms;
    struct sockaddr_storage socket_addr;
}
aeron_driver_name_resolver_neighbor_t;

typedef struct aeron_driver_name_resolver_stct
{
    const char *name;
    size_t name_length;
    struct sockaddr_storage local_socket_addr;
    aeron_name_resolver_cache_addr_t local_cache_addr;
    const char *bootstrap_neighbor;
    char *saved_bootstrap_neighbor;
    size_t bootstrap_neighbors_length;
    char **bootstrap_neighbors;
    struct sockaddr_storage bootstrap_neighbor_addr;
    unsigned int interface_index;
    aeron_udp_channel_transport_bindings_t *transport_bindings;
    aeron_name_resolver_t bootstrap_resolver;
    aeron_udp_channel_data_paths_t data_paths;
    aeron_udp_channel_transport_t transport;
    aeron_udp_transport_poller_t poller;
    aeron_name_resolver_cache_t cache;
    struct neighbours_stct
    {
        size_t length;
        size_t capacity;
        aeron_driver_name_resolver_neighbor_t *array;
    }
    neighbors;

    int64_t self_resolution_interval_ms;
    int64_t neighbor_resolution_interval_ms;
    int64_t neighbor_timeout_ms;

    int64_t work_deadline_ms;
    int64_t bootstrap_neighbor_resolve_deadline_ms;
    int64_t self_resolutions_deadline_ms;
    int64_t neighbor_resolutions_deadline_ms;

    int64_t *invalid_packets_counter;
    int64_t *short_sends_counter;
    int64_t *error_counter;

    aeron_clock_cache_t *cached_clock;
    aeron_counters_manager_t *counters_manager;
    aeron_position_t neighbor_counter;
    aeron_position_t cache_size_counter;
    aeron_distinct_error_log_t *error_log;

    struct
    {
        aeron_driver_name_resolver_on_neighbor_change_func_t neighbor_added;
        aeron_driver_name_resolver_on_neighbor_change_func_t neighbor_removed;
    } log;

    struct sockaddr_storage received_address;
    uint8_t *aligned_buffer;
    uint8_t buffer[AERON_MAX_UDP_PAYLOAD_LENGTH + AERON_CACHE_LINE_LENGTH];
}
aeron_driver_name_resolver_t;

void aeron_driver_name_resolver_receive(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr,
    struct timespec *media_receive_timestamp);

static int aeron_driver_name_resolver_from_sockaddr(
    struct sockaddr_storage *addr, aeron_name_resolver_cache_addr_t *cache_addr);

static int aeron_driver_name_resolver_resolve_bootstrap_neighbor(aeron_driver_name_resolver_t *driver_resolver)
{
    for (size_t i = 0; i < driver_resolver->bootstrap_neighbors_length; i++)
    {
        if (0 == aeron_name_resolver_resolve_host_and_port(
            &driver_resolver->bootstrap_resolver,
            driver_resolver->bootstrap_neighbors[i],
            "bootstrap_neighbor",
            false,
            &driver_resolver->bootstrap_neighbor_addr))
        {
            aeron_err_clear();
            return 0;
        }
    }

    return -1;
}

static const char *aeron_driver_name_resolver_build_neighbor_counter_label(
    aeron_driver_name_resolver_t *driver_resolver)
{
    static char buffer[512] = "";
    int offset = snprintf(buffer, sizeof(buffer) - 1, "Resolver neighbors: bound ");
    offset += aeron_format_source_identity(
        (char *)(buffer + offset), AERON_NETUTIL_FORMATTED_MAX_LENGTH, &driver_resolver->local_socket_addr);

    if (NULL != driver_resolver->bootstrap_neighbor)
    {
        offset += snprintf(buffer + offset, 12, " bootstrap ");
        aeron_format_source_identity(
            (char *)(buffer + offset), AERON_NETUTIL_FORMATTED_MAX_LENGTH, &driver_resolver->bootstrap_neighbor_addr);
    }

    return buffer;
}

int aeron_driver_name_resolver_free(aeron_driver_name_resolver_t *driver_resolver)
{
    if (NULL != driver_resolver->transport_bindings)
    {
        driver_resolver->transport_bindings->poller_close_func(&driver_resolver->poller);
        driver_resolver->transport_bindings->close_func(&driver_resolver->transport);
    }
    aeron_udp_channel_data_paths_delete(&driver_resolver->data_paths);
    aeron_name_resolver_cache_close(&driver_resolver->cache);
    aeron_free(driver_resolver->neighbors.array);
    aeron_free(driver_resolver->saved_bootstrap_neighbor);
    aeron_free(driver_resolver->bootstrap_neighbors);
    aeron_free(driver_resolver);

    return 0;
}

int aeron_driver_name_resolver_init(
    aeron_driver_name_resolver_t **driver_resolver,
    aeron_driver_context_t *context,
    const char *name,
    const char *interface_name,
    const char *bootstrap_neighbor)
{
    aeron_driver_name_resolver_t *_driver_resolver = NULL;

    if (aeron_alloc((void **)&_driver_resolver, sizeof(aeron_driver_name_resolver_t)) < 0)
    {
        AERON_APPEND_ERR("Failed to allocate driver resolver for: %s", name);
        goto error_cleanup;
    }
    _driver_resolver->saved_bootstrap_neighbor = NULL;
    _driver_resolver->bootstrap_neighbors = NULL;
    _driver_resolver->aligned_buffer = aeron_cache_line_align_buffer(_driver_resolver->buffer);
    _driver_resolver->name = name;
    _driver_resolver->name_length = strlen(name);

    if (aeron_find_unicast_interface(
        AF_INET, interface_name, &_driver_resolver->local_socket_addr, &_driver_resolver->interface_index) < 0)
    {
        goto error_cleanup;
    }

    if (aeron_driver_name_resolver_from_sockaddr(
        &_driver_resolver->local_socket_addr, &_driver_resolver->local_cache_addr) < 0)
    {
        goto error_cleanup;
    }

    const aeron_name_resolver_supplier_func_t bootstrap_resolver_supplier_func =
        NULL != context->driver_name_resolver_bootstrap_resolver_supplier_func ?
        context->driver_name_resolver_bootstrap_resolver_supplier_func :
        aeron_default_name_resolver_supplier;
    if (bootstrap_resolver_supplier_func(&_driver_resolver->bootstrap_resolver, NULL, context) < 0)
    {
        goto error_cleanup;
    }

    _driver_resolver->log.neighbor_added = context->log.name_resolution_on_neighbor_added;
    _driver_resolver->log.neighbor_removed = context->log.name_resolution_on_neighbor_removed;

    _driver_resolver->bootstrap_neighbor = bootstrap_neighbor;
    _driver_resolver->bootstrap_neighbors_length = 0;
    if (NULL != bootstrap_neighbor)
    {
        _driver_resolver->saved_bootstrap_neighbor = strdup(bootstrap_neighbor);
        if (NULL == _driver_resolver->saved_bootstrap_neighbor)
        {
            AERON_SET_ERR(EINVAL, "Failed to duplicate bootstrap neighbors string: %s", bootstrap_neighbor);
            goto error_cleanup;
        }
        char *bootstrap_neighbors[AERON_NAME_RESOLVER_DRIVER_MAX_BOOTSTRAP_NEIGHBORS];

        const int num_neighbors = aeron_tokenise(
            _driver_resolver->saved_bootstrap_neighbor,
            ',',
            AERON_NAME_RESOLVER_DRIVER_MAX_BOOTSTRAP_NEIGHBORS,
            bootstrap_neighbors);

        if (num_neighbors < 0)
        {
            AERON_SET_ERR(EINVAL, "Failed to parse bootstrap neighbors list: %s", bootstrap_neighbor);
            goto error_cleanup;
        }

        if (aeron_alloc(
            (void **)&_driver_resolver->bootstrap_neighbors,
            (strlen(bootstrap_neighbor) + num_neighbors) * sizeof(char)) < 0)
        {
            AERON_APPEND_ERR("%s", "Allocating bootstrap neighbors array");
            goto error_cleanup;
        }

        _driver_resolver->bootstrap_neighbors_length = (size_t)num_neighbors;
        for (int i = 0; i < num_neighbors; i++)
        {
            _driver_resolver->bootstrap_neighbors[i] = bootstrap_neighbors[num_neighbors - i - 1];
        }

        if (aeron_driver_name_resolver_resolve_bootstrap_neighbor(_driver_resolver) < 0)
        {
            goto error_cleanup;
        }
    }

    _driver_resolver->transport_bindings = context->conductor_udp_channel_transport_bindings;
    if (aeron_udp_channel_data_paths_init(
        &_driver_resolver->data_paths,
        context->udp_channel_outgoing_interceptor_bindings,
        context->udp_channel_incoming_interceptor_bindings,
        _driver_resolver->transport_bindings,
        aeron_driver_name_resolver_receive,
        context,
        AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_CONDUCTOR) < 0)
    {
        goto error_cleanup;
    }


    aeron_udp_channel_transport_params_t transport_params = {
        context->socket_rcvbuf,
        context->socket_sndbuf,
        context->mtu_length,
        _driver_resolver->interface_index,
        0,
        false,
    };

    if (_driver_resolver->transport_bindings->init_func(
        &_driver_resolver->transport,
        &_driver_resolver->local_socket_addr,
        NULL, // Unicast only.
        NULL, // No connected
        &transport_params,
        context,
        AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_CONDUCTOR) < 0)
    {
        AERON_APPEND_ERR(
            "resolver, name=%s interface_name=%s bootstrap_neighbor=%s",
            _driver_resolver->name,
            interface_name,
            bootstrap_neighbor);
        goto error_cleanup;
    }

    _driver_resolver->transport.data_paths = &_driver_resolver->data_paths;

    if (_driver_resolver->transport_bindings->poller_init_func(
        &_driver_resolver->poller, context, AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_CONDUCTOR) < 0)
    {
        goto error_cleanup;
    }

    if (_driver_resolver->transport_bindings->poller_add_func(
        &_driver_resolver->poller, &_driver_resolver->transport) < 0)
    {
        goto error_cleanup;
    }

    aeron_name_resolver_cache_init(&_driver_resolver->cache, AERON_NAME_RESOLVER_DRIVER_TIMEOUT_MS);

    int64_t now_ms = context->epoch_clock();
    _driver_resolver->cached_clock = context->cached_clock;
    _driver_resolver->neighbor_timeout_ms = AERON_NAME_RESOLVER_DRIVER_TIMEOUT_MS;
    _driver_resolver->self_resolution_interval_ms = AERON_NAME_RESOLVER_DRIVER_SELF_RESOLUTION_INTERVAL_MS;
    _driver_resolver->self_resolutions_deadline_ms = 0;
    _driver_resolver->neighbor_resolution_interval_ms = AERON_NAME_RESOLVER_DRIVER_NEIGHBOUR_RESOLUTION_INTERVAL_MS;
    _driver_resolver->neighbor_resolutions_deadline_ms = now_ms + _driver_resolver->neighbor_resolution_interval_ms;
    _driver_resolver->bootstrap_neighbor_resolve_deadline_ms = now_ms;
    _driver_resolver->work_deadline_ms = 0;

    const char *neighbor_counter_label = aeron_driver_name_resolver_build_neighbor_counter_label(_driver_resolver);
    _driver_resolver->neighbor_counter.counter_id = aeron_counters_manager_allocate(
        context->counters_manager,
        AERON_COUNTER_NAME_RESOLVER_NEIGHBORS_COUNTER_TYPE_ID,
        NULL, 0,
        neighbor_counter_label, strlen(neighbor_counter_label));
    if (_driver_resolver->neighbor_counter.counter_id < 0)
    {
        goto error_cleanup;
    }

    _driver_resolver->neighbor_counter.value_addr = aeron_counters_manager_addr(
        context->counters_manager, _driver_resolver->neighbor_counter.counter_id);
    char cache_entries_label[512];
    snprintf(
        cache_entries_label, sizeof(cache_entries_label), "Resolver cache entries: name=%s", _driver_resolver->name);
    _driver_resolver->cache_size_counter.counter_id = aeron_counters_manager_allocate(
        context->counters_manager,
        AERON_COUNTER_NAME_RESOLVER_CACHE_ENTRIES_COUNTER_TYPE_ID,
        NULL, 0,
        cache_entries_label, strlen(cache_entries_label));
    if (_driver_resolver->cache_size_counter.counter_id < 0)
    {
        goto error_cleanup;
    }
    _driver_resolver->counters_manager = context->counters_manager;

    _driver_resolver->cache_size_counter.value_addr = aeron_counters_manager_addr(
        context->counters_manager, _driver_resolver->cache_size_counter.counter_id);

    _driver_resolver->short_sends_counter = aeron_system_counter_addr(
        context->system_counters, AERON_SYSTEM_COUNTER_SHORT_SENDS);
    _driver_resolver->invalid_packets_counter = aeron_system_counter_addr(
        context->system_counters, AERON_SYSTEM_COUNTER_INVALID_PACKETS);
    _driver_resolver->error_counter = aeron_system_counter_addr(
        context->system_counters, AERON_SYSTEM_COUNTER_ERRORS);

    _driver_resolver->error_log = context->error_log;

    *driver_resolver = _driver_resolver;
    return 0;

error_cleanup:
    aeron_driver_name_resolver_free(_driver_resolver);
    return -1;
}

int aeron_driver_name_resolver_close(aeron_name_resolver_t *resolver)
{
    aeron_driver_name_resolver_t *driver_resolver = (aeron_driver_name_resolver_t *)resolver->state;
    return aeron_driver_name_resolver_free(driver_resolver);
}

static int aeron_driver_name_resolver_to_sockaddr(
    aeron_name_resolver_cache_addr_t *cache_addr, struct sockaddr_storage *addr)
{
    int result = -1;
    if (cache_addr->res_type == AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD)
    {
        struct sockaddr_in6 *addr_in6 = (struct sockaddr_in6 *)addr;
        addr_in6->sin6_family = AF_INET6;
        addr_in6->sin6_port = htons(cache_addr->port);
        memcpy(&addr_in6->sin6_addr, cache_addr->address, sizeof(addr_in6->sin6_addr));
        result = 0;
    }
    else if (cache_addr->res_type == AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD)
    {
        struct sockaddr_in *addr_in = (struct sockaddr_in *)addr;
        addr_in->sin_family = AF_INET;
        addr_in->sin_port = htons(cache_addr->port);
        memcpy(&addr_in->sin_addr, cache_addr->address, sizeof(addr_in->sin_addr));
        result = 0;
    }
    else
    {
        AERON_SET_ERR(EINVAL, "Invalid res_type: %d", cache_addr->res_type);
    }

    return result;
}

static int aeron_driver_name_resolver_from_sockaddr(
    struct sockaddr_storage *addr, aeron_name_resolver_cache_addr_t *cache_addr)
{
    int result = -1;
    if (AF_INET6 == addr->ss_family)
    {
        struct sockaddr_in6 *addr_in6 = (struct sockaddr_in6 *)addr;
        cache_addr->res_type = AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD;
        cache_addr->port = ntohs(addr_in6->sin6_port);
        memcpy(cache_addr->address, &addr_in6->sin6_addr, sizeof(addr_in6->sin6_addr));
        result = 0;
    }
    else if (AF_INET == addr->ss_family)
    {
        struct sockaddr_in *addr_in = (struct sockaddr_in *)addr;
        cache_addr->res_type = AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD;
        cache_addr->port = ntohs(addr_in->sin_port);
        memcpy(cache_addr->address, &addr_in->sin_addr, sizeof(addr_in->sin_addr));
        result = 0;
    }
    else
    {
        AERON_SET_ERR(EINVAL, "Invalid address family: %d", addr->ss_family);
    }

    return result;
}

static uint16_t aeron_driver_name_resolver_get_port(aeron_driver_name_resolver_t *resolver)
{
    uint16_t port = AF_INET6 == resolver->local_socket_addr.ss_family ?
        ((struct sockaddr_in6 *)&resolver->local_socket_addr)->sin6_port :
        ((struct sockaddr_in *)&resolver->local_socket_addr)->sin_port;

    return ntohs(port);
}

static int aeron_driver_name_resolver_find_neighbor_by_addr(
    aeron_driver_name_resolver_t *resolver, aeron_name_resolver_cache_addr_t *cache_addr)
{
    for (size_t i = 0; i < resolver->neighbors.length; i++)
    {
        aeron_driver_name_resolver_neighbor_t *neighbor = &resolver->neighbors.array[i];

        if (cache_addr->res_type == neighbor->cache_addr.res_type &&
            cache_addr->port == neighbor->cache_addr.port &&
            0 == memcmp(
                cache_addr->address,
                neighbor->cache_addr.address,
                aeron_res_header_address_length(cache_addr->res_type)))
        {
            return (int)i;
        }
    }

    return -1;
}

static int aeron_driver_name_resolver_add_neighbor(
    aeron_driver_name_resolver_t *resolver,
    aeron_name_resolver_cache_addr_t *cache_addr,
    bool is_self,
    int64_t time_of_last_activity_ms)
{
    const int neighbor_index = aeron_driver_name_resolver_find_neighbor_by_addr(resolver, cache_addr);
    if (neighbor_index < 0)
    {
        int ensure_capacity_result = 0;
        AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, resolver->neighbors, aeron_driver_name_resolver_neighbor_t)
        if (ensure_capacity_result < 0)
        {
            AERON_APPEND_ERR(
                "Failed to allocate rows for neighbors table (%" PRIu64 ", %" PRIu64 ")",
                (uint64_t)resolver->neighbors.length,
                (uint64_t)resolver->neighbors.capacity);

            return ensure_capacity_result;
        }

        aeron_driver_name_resolver_neighbor_t *new_neighbor = &resolver->neighbors.array[resolver->neighbors.length];
        if (aeron_driver_name_resolver_to_sockaddr(cache_addr, &new_neighbor->socket_addr) < 0)
        {
            return -1;
        }

        resolver->log.neighbor_added(&new_neighbor->socket_addr);

        memcpy(&new_neighbor->cache_addr, cache_addr, sizeof(new_neighbor->cache_addr));
        new_neighbor->time_of_last_activity_ms = time_of_last_activity_ms;
        resolver->neighbors.length++;
        aeron_counter_set_release(resolver->neighbor_counter.value_addr, (int64_t) resolver->neighbors.length);

        return 1;
    }
    else if (is_self)
    {
        resolver->neighbors.array[neighbor_index].time_of_last_activity_ms = time_of_last_activity_ms;

        return 2;
    }

    return 0;
}

static int aeron_driver_name_resolver_on_resolution_entry(
    aeron_driver_name_resolver_t *resolver,
    const aeron_resolution_header_t *resolution_header,
    const char *name,
    size_t name_length,
    aeron_name_resolver_cache_addr_t *cache_addr,
    bool is_self,
    int64_t now_ms)
{
    // Ignore own records that match me...
    if (cache_addr->port == aeron_driver_name_resolver_get_port(resolver) &&
        name_length && resolver->name_length &&
        0 == strncmp(resolver->name, name, name_length))
    {
        return 0;
    }

    int64_t time_of_last_activity_ms = now_ms - resolution_header->age_in_ms;

    if (aeron_name_resolver_cache_add_or_update(
        &resolver->cache,
        name,
        name_length,
        cache_addr,
        time_of_last_activity_ms,
        resolver->cache_size_counter.value_addr) < 0)
    {
        return -1;
    }

    if (aeron_driver_name_resolver_add_neighbor(resolver, cache_addr, is_self, time_of_last_activity_ms) < 0)
    {
        return -1;
    }

    return 1;
}

static bool aeron_driver_name_resolver_is_wildcard(int8_t res_type, uint8_t *address)
{
    in_addr_t ipv4_wildcard = INADDR_ANY;
    return
        (res_type == AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD && 0 == memcmp(address, &in6addr_any, sizeof(in6addr_any))) ||
        0 == memcmp(address, &ipv4_wildcard, sizeof(ipv4_wildcard));
}

static void aeron_name_resolver_log_and_clear_error(aeron_driver_name_resolver_t *resolver)
{
    aeron_distinct_error_log_record(resolver->error_log, aeron_errcode(), aeron_errmsg());
    aeron_counter_increment(resolver->error_counter);
    aeron_err_clear();
}

void aeron_driver_name_resolver_receive(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr,
    struct timespec *media_receive_timestamp)
{
    aeron_driver_name_resolver_t *resolver = receiver_clientd;
    aeron_frame_header_t *frame_header = (aeron_frame_header_t *)buffer;
    size_t remaining = length;

    if ((remaining < sizeof(aeron_frame_header_t)) || (frame_header->version != AERON_FRAME_HEADER_VERSION))
    {
        aeron_counter_increment(resolver->invalid_packets_counter);
        return;
    }

    remaining -= sizeof(aeron_frame_header_t);

    while (0 < remaining)
    {
        const size_t offset = length - remaining;
        if (AERON_HDR_TYPE_RES != frame_header->type || remaining < sizeof(aeron_resolution_header_t))
        {
            aeron_counter_increment(resolver->invalid_packets_counter);
            return;
        }

        aeron_resolution_header_t *resolution_header = (aeron_resolution_header_t *)&buffer[offset];
        aeron_name_resolver_cache_addr_t cache_addr;
        const char *name = NULL;
        size_t name_length;
        size_t entry_length;

        cache_addr.res_type = resolution_header->res_type;
        cache_addr.port = resolution_header->udp_port;

        if (AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD == resolution_header->res_type)
        {
            aeron_resolution_header_ipv4_t *ip4_hdr = (aeron_resolution_header_ipv4_t *)resolution_header;
            if (length < sizeof(aeron_resolution_header_ipv4_t) ||
                length < (entry_length = aeron_res_header_entry_length_ipv4(ip4_hdr)))
            {
                aeron_counter_increment(resolver->invalid_packets_counter);
                return;
            }

            memcpy(cache_addr.address, ip4_hdr->addr, sizeof(ip4_hdr->addr));
            name_length = (size_t)ip4_hdr->name_length;
            name = (const char *)(ip4_hdr + 1);
        }
        else if (AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD == resolution_header->res_type)
        {
            aeron_resolution_header_ipv6_t *ip6_hdr = (aeron_resolution_header_ipv6_t *)resolution_header;
            if (length < sizeof(aeron_resolution_header_ipv6_t) ||
                length < (entry_length = aeron_res_header_entry_length_ipv6(ip6_hdr)))
            {
                aeron_counter_increment(resolver->invalid_packets_counter);
                return;
            }

            memcpy(cache_addr.address, ip6_hdr->addr, sizeof(ip6_hdr->addr));
            name_length = (size_t)ip6_hdr->name_length;
            name = (const char *)(ip6_hdr + 1);
        }
        else
        {
            AERON_SET_ERR(EINVAL, "Invalid res type on entry: %d", resolution_header->res_type);
            aeron_name_resolver_log_and_clear_error(resolver);
            return;
        }

        const bool is_self = AERON_RES_HEADER_SELF_FLAG == (resolution_header->res_flags & AERON_RES_HEADER_SELF_FLAG);

        if (is_self && aeron_driver_name_resolver_is_wildcard(cache_addr.res_type, cache_addr.address))
        {
            if (aeron_driver_name_resolver_from_sockaddr(addr, &cache_addr) < 0)
            {
                AERON_APPEND_ERR("%s", "Failed to replace wildcard with source addr");
                aeron_name_resolver_log_and_clear_error(resolver);

                return;
            }
        }

        const int64_t now_ms = aeron_clock_cached_epoch_time(resolver->cached_clock);
        if (aeron_driver_name_resolver_on_resolution_entry(
            resolver, resolution_header, name, name_length, &cache_addr, is_self, now_ms) < 0)
        {
            AERON_APPEND_ERR("%s", "Failed to handle resolution entry");
            aeron_name_resolver_log_and_clear_error(resolver);
        }

        remaining -= entry_length;
    }
}

static int aeron_driver_name_resolver_poll(aeron_driver_name_resolver_t *resolver)
{
    struct mmsghdr mmsghdr[AERON_NAME_RESOLVER_DRIVER_NUM_RECV_BUFFERS];
    struct iovec iov[AERON_NAME_RESOLVER_DRIVER_NUM_RECV_BUFFERS];
    iov[0].iov_base = resolver->aligned_buffer;
    iov[0].iov_len = AERON_MAX_UDP_PAYLOAD_LENGTH;

    for (size_t i = 0; i < AERON_NAME_RESOLVER_DRIVER_NUM_RECV_BUFFERS; i++)
    {
        mmsghdr[i].msg_hdr.msg_name = &resolver->received_address;
        mmsghdr[i].msg_hdr.msg_namelen = AERON_ADDR_LEN(&resolver->received_address);
        mmsghdr[i].msg_hdr.msg_iov = &iov[i];
        mmsghdr[i].msg_hdr.msg_iovlen = 1;
        mmsghdr[i].msg_hdr.msg_flags = 0;
        mmsghdr[i].msg_hdr.msg_control = NULL;
        mmsghdr[i].msg_hdr.msg_controllen = 0;
        mmsghdr[i].msg_len = 0;
    }

    int64_t bytes_received = 0;
    int poll_result = resolver->transport_bindings->poller_poll_func(
        &resolver->poller,
        mmsghdr,
        AERON_NAME_RESOLVER_DRIVER_NUM_RECV_BUFFERS,
        &bytes_received,
        resolver->data_paths.recv_func,
        resolver->transport_bindings->recvmmsg_func,
        resolver);

    if (poll_result < 0)
    {
        AERON_APPEND_ERR("Failed to poll in driver name resolver: %s", resolver->name);
        aeron_name_resolver_log_and_clear_error(resolver);
    }

    return bytes_received > 0 ? (int)bytes_received : 0;
}

static bool aeron_driver_name_resolver_sockaddr_equals(struct sockaddr_storage *a, struct sockaddr_storage *b)
{
    if (a->ss_family != b->ss_family)
    {
        return false;
    }

    if (AF_INET == a->ss_family)
    {
        struct sockaddr_in *a_in = (struct sockaddr_in *)a;
        struct sockaddr_in *b_in = (struct sockaddr_in *)b;

        return a_in->sin_addr.s_addr == b_in->sin_addr.s_addr && a_in->sin_port == b_in->sin_port;
    }
    else if (AF_INET6 == a->ss_family)
    {
        struct sockaddr_in6 *a_in = (struct sockaddr_in6 *)a;
        struct sockaddr_in6 *b_in = (struct sockaddr_in6 *)b;

        return 0 == memcmp(&a_in->sin6_addr, &b_in->sin6_addr, sizeof(a_in->sin6_addr)) &&
            a_in->sin6_port == b_in->sin6_port;
    }

    return false;
}

static int aeron_driver_name_resolver_do_send(
    aeron_driver_name_resolver_t *resolver,
    aeron_frame_header_t *frame_header,
    ssize_t length,
    struct sockaddr_storage *neighbor_address)
{
    struct iovec iov;
    int64_t bytes_sent = 0;

    iov.iov_base = frame_header;
    iov.iov_len = (uint32_t)length;

    int send_result = resolver->data_paths.send_func(
        &resolver->data_paths, &resolver->transport, neighbor_address, &iov, 1, &bytes_sent);
    if (0 <= send_result)
    {
        if (bytes_sent < (int64_t)iov.iov_len)
        {
            aeron_counter_increment(resolver->short_sends_counter);
        }
    }
    else
    {
        char buffer[AERON_NETUTIL_FORMATTED_MAX_LENGTH] = { 0 };
        aeron_format_source_identity(buffer, AERON_NETUTIL_FORMATTED_MAX_LENGTH, neighbor_address);
        AERON_APPEND_ERR("Failed to send resolution frames to neighbor: %s (protocol_family=%i)",
            buffer, neighbor_address->ss_family);
    }

    return send_result;
}

static int aeron_driver_name_resolver_send_self_resolutions(
    aeron_driver_name_resolver_t *driver_resolver, aeron_name_resolver_t *resolver, int64_t now_ms)
{
    if (0 == driver_resolver->bootstrap_neighbors_length && 0 == driver_resolver->neighbors.length)
    {
        return 0;
    }

    const size_t entry_offset = sizeof(aeron_frame_header_t);
    uint8_t *aligned_buffer = driver_resolver->aligned_buffer;
    aeron_frame_header_t *frame_header = (aeron_frame_header_t *)&aligned_buffer[0];
    aeron_resolution_header_t *resolution_header = (aeron_resolution_header_t *)&aligned_buffer[entry_offset];

    const size_t name_length = driver_resolver->name_length; // TODO: cache name length

    int entry_length = aeron_driver_name_resolver_set_resolution_header(
        resolution_header,
        AERON_MAX_UDP_PAYLOAD_LENGTH - entry_offset,
        AERON_RES_HEADER_SELF_FLAG,
        &driver_resolver->local_cache_addr,
        driver_resolver->name,
        name_length);

    assert(0 <= entry_length || "local_cache_addr should of been correctly constructed during init");

    size_t frame_length = sizeof(aeron_frame_header_t) + (size_t)entry_length;

    frame_header->type = AERON_HDR_TYPE_RES;
    frame_header->flags = UINT8_C(0);
    frame_header->version = AERON_FRAME_HEADER_VERSION;
    frame_header->frame_length = (int32_t)frame_length;
    resolution_header->age_in_ms = 0;

    struct sockaddr_storage neighbor_sock_addr;

    bool send_to_bootstrap = driver_resolver->bootstrap_neighbors_length > 0;
    int work_count = 0;

    for (size_t k = 0; k < driver_resolver->neighbors.length; k++)
    {
        aeron_driver_name_resolver_neighbor_t *neighbor = &driver_resolver->neighbors.array[k];

        aeron_driver_name_resolver_to_sockaddr(&neighbor->cache_addr, &neighbor_sock_addr);

        if (aeron_driver_name_resolver_do_send(driver_resolver, frame_header, frame_length, &neighbor_sock_addr) < 0)
        {
            AERON_APPEND_ERR("%s", "Failed to send self resolutions");
            aeron_name_resolver_log_and_clear_error(driver_resolver);
        }
        else
        {
            work_count++;
        }

        if (send_to_bootstrap &&
            aeron_driver_name_resolver_sockaddr_equals(&driver_resolver->bootstrap_neighbor_addr, &neighbor_sock_addr))
        {
            send_to_bootstrap = false;
        }
    }

    if (send_to_bootstrap)
    {
        if (now_ms > driver_resolver->bootstrap_neighbor_resolve_deadline_ms)
        {
            struct sockaddr_storage old_address = driver_resolver->bootstrap_neighbor_addr;
            if (aeron_driver_name_resolver_resolve_bootstrap_neighbor(driver_resolver) < 0)
            {
                AERON_APPEND_ERR("failed to resolve bootstrap neighbor (%s)", driver_resolver->bootstrap_neighbor);
                return work_count;
            }

            driver_resolver->bootstrap_neighbor_resolve_deadline_ms = now_ms + AERON_NAME_RESOLVER_DRIVER_TIMEOUT_MS;

            if (!aeron_driver_name_resolver_sockaddr_equals(&driver_resolver->bootstrap_neighbor_addr, &old_address))
            {
                const char *neighbor_counter_label = aeron_driver_name_resolver_build_neighbor_counter_label(driver_resolver);
                aeron_counters_manager_update_label(
                    driver_resolver->counters_manager,
                    driver_resolver->neighbor_counter.counter_id,
                    strlen(neighbor_counter_label),
                    neighbor_counter_label);

                // avoid sending resolution frame if new bootstrap is in the neighbors list
                for (size_t k = 0; k < driver_resolver->neighbors.length; k++)
                {
                    aeron_driver_name_resolver_neighbor_t *neighbor = &driver_resolver->neighbors.array[k];

                    aeron_driver_name_resolver_to_sockaddr(&neighbor->cache_addr, &neighbor_sock_addr);

                    if (aeron_driver_name_resolver_sockaddr_equals(
                        &driver_resolver->bootstrap_neighbor_addr, &neighbor_sock_addr))
                    {
                        send_to_bootstrap = false;
                        break;
                    }
                }
            }
        }

        if (send_to_bootstrap)
        {
            if (aeron_driver_name_resolver_do_send(
                driver_resolver, frame_header, frame_length, &driver_resolver->bootstrap_neighbor_addr) < 0)
            {
                AERON_APPEND_ERR("%s", "Failed to send bootstrap resolutions");
                aeron_name_resolver_log_and_clear_error(driver_resolver);
            }
            else
            {
                work_count++;
            }
        }
    }

    return work_count;
}

static int aeron_driver_name_resolver_send_neighbor_resolutions(aeron_driver_name_resolver_t *resolver, int64_t now_ms)
{
    uint8_t *aligned_buffer = resolver->aligned_buffer;
    aeron_frame_header_t *frame_header = (aeron_frame_header_t *)aligned_buffer;
    frame_header->type = AERON_HDR_TYPE_RES;
    frame_header->flags = UINT8_C(0);
    frame_header->version = AERON_FRAME_HEADER_VERSION;

    size_t i;
    size_t j;
    int work_count = 0;
    for (i = 0; i < resolver->cache.entries.length;)
    {
        size_t entry_offset = sizeof(aeron_frame_header_t);

        for (j = i; j < resolver->cache.entries.length;)
        {
            aeron_resolution_header_t *resolution_header = (aeron_resolution_header_t *)&aligned_buffer[entry_offset];
            aeron_name_resolver_cache_entry_t *cache_entry = &resolver->cache.entries.array[j];

            int entry_length = aeron_driver_name_resolver_set_resolution_header(
                resolution_header,
                AERON_MAX_UDP_PAYLOAD_LENGTH - entry_offset,
                0,
                &cache_entry->cache_addr,
                cache_entry->name,
                cache_entry->name_length);

            assert(-1 != entry_length && "Invalid res_type crept in from somewhere");

            resolution_header->age_in_ms = (int32_t)(now_ms - cache_entry->time_of_last_activity_ms);

            if (0 == entry_length)
            {
                break;
            }

            entry_offset += entry_length;
            j++;
        }

        frame_header->frame_length = (int32_t)entry_offset;

        for (size_t k = 0; k < resolver->neighbors.length; k++)
        {
            aeron_driver_name_resolver_neighbor_t *neighbor = &resolver->neighbors.array[k];

            if (aeron_driver_name_resolver_do_send(resolver, frame_header, entry_offset, &neighbor->socket_addr) < 0)
            {
                AERON_APPEND_ERR("%s", "Failed to send neighbor resolutions");
                aeron_distinct_error_log_record(resolver->error_log, aeron_errcode(), aeron_errmsg());
            }
            else
            {
                work_count++;
            }
        }

        i = j;
    }

    return work_count;
}

static int aeron_driver_name_resolver_timeout_neighbors(aeron_driver_name_resolver_t *resolver, int64_t now_ms)
{
    int num_removed = 0;
    for (int last_index = (int)resolver->neighbors.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_driver_name_resolver_neighbor_t *entry = &resolver->neighbors.array[i];

        if ((entry->time_of_last_activity_ms + resolver->neighbor_timeout_ms) <= now_ms)
        {
            resolver->log.neighbor_removed(&entry->socket_addr);

            aeron_array_fast_unordered_remove(
                (uint8_t *)resolver->neighbors.array, sizeof(aeron_driver_name_resolver_neighbor_t), i, last_index);
            resolver->neighbors.length--;
            last_index--;
            num_removed++;
        }
    }

    if (0 != num_removed)
    {
        aeron_counter_set_release(resolver->neighbor_counter.value_addr, (int64_t) resolver->neighbors.length);
    }

    return num_removed;
}

int aeron_driver_name_resolver_set_resolution_header_from_sockaddr(
    aeron_resolution_header_t *resolution_header,
    size_t capacity,
    uint8_t flags,
    struct sockaddr_storage *addr,
    const char *name,
    size_t name_length)
{
    aeron_name_resolver_cache_addr_t cache_addr;

    if (aeron_driver_name_resolver_from_sockaddr(addr, &cache_addr) < 0)
    {
        return -1;
    }

    return aeron_driver_name_resolver_set_resolution_header(
        resolution_header, capacity, flags, &cache_addr, name, name_length);
}

int aeron_driver_name_resolver_set_resolution_header(
    aeron_resolution_header_t *resolution_header,
    size_t capacity,
    uint8_t flags,
    aeron_name_resolver_cache_addr_t *cache_addr,
    const char *name,
    size_t name_length)
{
    size_t name_offset;
    size_t entry_length;

    switch (cache_addr->res_type)
    {
        case AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD:
            entry_length = AERON_ALIGN(sizeof(aeron_resolution_header_ipv4_t) + name_length, sizeof(int64_t));
            if (capacity < entry_length)
            {
                return 0;
            }

            aeron_resolution_header_ipv4_t *hdr_ipv4 = (aeron_resolution_header_ipv4_t *)resolution_header;
            memcpy(hdr_ipv4->addr, cache_addr->address, sizeof(hdr_ipv4->addr));
            hdr_ipv4->name_length = (int16_t)name_length;
            name_offset = sizeof(aeron_resolution_header_ipv4_t);

            break;

        case AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD:
            entry_length = AERON_ALIGN(sizeof(aeron_resolution_header_ipv6_t) + name_length, sizeof(int64_t));
            if (capacity < entry_length)
            {
                return 0;
            }

            aeron_resolution_header_ipv6_t *hdr_ipv6 = (aeron_resolution_header_ipv6_t *)resolution_header;

            memcpy(hdr_ipv6->addr, cache_addr->address, sizeof(hdr_ipv6->addr));
            hdr_ipv6->name_length = (int16_t)name_length;
            name_offset = sizeof(aeron_resolution_header_ipv6_t);

            break;

        default:
            return -1;
    }

    resolution_header->res_type = cache_addr->res_type;
    resolution_header->udp_port = cache_addr->port;
    resolution_header->res_flags = flags;

    uint8_t *buffer = (uint8_t *)resolution_header;
    memcpy(buffer + name_offset, name, name_length);

    return (int)entry_length;
}

int aeron_driver_name_resolver_resolve(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    struct sockaddr_storage *sock_addr)
{
    aeron_driver_name_resolver_t *driver_resolver = resolver->state;

    const int8_t res_type = AF_INET6 == sock_addr->ss_family ?
        AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD : AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD;

    aeron_name_resolver_cache_entry_t *cache_entry;
    int result = -1;

    if (aeron_name_resolver_cache_lookup_by_name(
        &driver_resolver->cache, name, strlen(name), res_type, &cache_entry) < 0)
    {
        if (0 == strncmp(name, driver_resolver->name, driver_resolver->name_length + 1))
        {
            memcpy(sock_addr, &driver_resolver->local_socket_addr, sizeof(struct sockaddr_storage));
            result = 0;
        }
        else
        {
            result = driver_resolver->bootstrap_resolver.resolve_func(
                &driver_resolver->bootstrap_resolver, name, uri_param_name, is_re_resolution, sock_addr);
        }
    }
    else
    {
        result = aeron_driver_name_resolver_to_sockaddr(&cache_entry->cache_addr, sock_addr);
    }

    return result;
}

int aeron_driver_name_resolver_do_work(aeron_name_resolver_t *resolver, int64_t now_ms)
{
    aeron_driver_name_resolver_t *driver_resolver = resolver->state;
    int work_count = 0;

    if (now_ms > driver_resolver->work_deadline_ms)
    {
        work_count += aeron_driver_name_resolver_poll(driver_resolver);
        work_count += aeron_name_resolver_cache_timeout_old_entries(
            &driver_resolver->cache, now_ms, driver_resolver->cache_size_counter.value_addr);
        work_count += aeron_driver_name_resolver_timeout_neighbors(driver_resolver, now_ms);

        if (now_ms > driver_resolver->self_resolutions_deadline_ms)
        {
            work_count += aeron_driver_name_resolver_send_self_resolutions(driver_resolver, resolver, now_ms);

            driver_resolver->self_resolutions_deadline_ms = now_ms + driver_resolver->self_resolution_interval_ms;
        }

        if (now_ms > driver_resolver->neighbor_resolutions_deadline_ms)
        {
            work_count += aeron_driver_name_resolver_send_neighbor_resolutions(driver_resolver, now_ms);

            driver_resolver->neighbor_resolutions_deadline_ms =
                now_ms + driver_resolver->neighbor_resolution_interval_ms;
        }

        driver_resolver->work_deadline_ms = now_ms + AERON_NAME_RESOLVER_DRIVER_DUTY_CYCLE_MS;
    }

    return work_count;
}

int aeron_driver_name_resolver_supplier(
    aeron_name_resolver_t *resolver, const char *args, aeron_driver_context_t *context)
{
    aeron_driver_name_resolver_t *name_resolver = NULL;

    resolver->state = NULL;
    if (aeron_driver_name_resolver_init(
        &name_resolver, context,
        context->resolver_name,
        context->resolver_interface,
        context->resolver_bootstrap_neighbor) < 0)
    {
        return -1;
    }

    resolver->lookup_func = aeron_default_name_resolver_lookup;
    resolver->resolve_func = aeron_driver_name_resolver_resolve;
    resolver->do_work_func = aeron_driver_name_resolver_do_work;
    resolver->close_func = aeron_driver_name_resolver_close;
    resolver->state = name_resolver;
    resolver->name = "driver";

    return 0;
}
