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

#include "uri/aeron_uri.h"
#include "protocol/aeron_udp_protocol.h"
#include "util/aeron_netutil.h"
#include "util/aeron_arrayutil.h"
#include "media/aeron_send_channel_endpoint.h"
#include "aeron_driver_conductor.h"
#include "media/aeron_udp_destination_tracker.h"

#if !defined(HAVE_STRUCT_MMSGHDR)
struct mmsghdr
{
    struct msghdr msg_hdr;
    unsigned int msg_len;
};
#endif

int aeron_udp_destination_tracker_init(
    aeron_udp_destination_tracker_t *tracker,
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_clock_cache_t *cached_clock,
    bool is_manual_control_model,
    int64_t timeout_ns)
{
    tracker->cached_clock = cached_clock;
    tracker->data_paths = data_paths;
    tracker->destination_timeout_ns = timeout_ns;
    tracker->destinations.array = NULL;
    tracker->destinations.length = 0;
    tracker->destinations.capacity = 0;
    tracker->is_manual_control_mode = is_manual_control_model;
    tracker->round_robin_index = 0;

    return 0;
}

int aeron_udp_destination_tracker_close(aeron_udp_destination_tracker_t *tracker)
{
    if (NULL != tracker)
    {
        for (size_t i = 0; i < tracker->destinations.length; i++)
        {
            aeron_uri_close(tracker->destinations.array[i].uri);
            aeron_free(tracker->destinations.array[i].uri);
        }

        aeron_free(tracker->destinations.array);
    }

    return 0;
}

static void aeron_udp_destination_tracker_remove_inactive_destinations(
    aeron_udp_destination_tracker_t *tracker,
    int64_t now_ns)
{
    if (!tracker->is_manual_control_mode)
    {
        for (int last_index = (int)tracker->destinations.length - 1, i = last_index; i >= 0; i--)
        {
            aeron_udp_destination_entry_t *entry = &tracker->destinations.array[i];

            if (now_ns > (entry->time_of_last_activity_ns + tracker->destination_timeout_ns))
            {
                aeron_array_fast_unordered_remove(
                    (uint8_t *)tracker->destinations.array,
                    sizeof(aeron_udp_destination_entry_t),
                    (size_t)i,
                    (size_t)last_index);
                last_index--;
                tracker->destinations.length--;

                aeron_counter_set_release(tracker->num_destinations_addr, (int64_t) tracker->destinations.length);
            }
        }
    }
}

int aeron_udp_destination_tracker_send(
    aeron_udp_destination_tracker_t *tracker,
    aeron_udp_channel_transport_t *transport,
    struct iovec *iov,
    size_t iov_length,
    int64_t *bytes_sent)
{
    const int64_t now_ns = aeron_clock_cached_nano_time(tracker->cached_clock);
    const bool is_dynamic_control_mode = !tracker->is_manual_control_mode;
    const int length = (int)tracker->destinations.length;
    int result = (int)iov_length;
    int to_be_removed = 0;

    *bytes_sent = iov->iov_len;

    int starting_index = tracker->round_robin_index++;
    if (starting_index >= length)
    {
        tracker->round_robin_index = starting_index = 0;
    }

    for (int i = starting_index; i < length; i++)
    {
        aeron_udp_destination_entry_t *entry = &tracker->destinations.array[i];

        if (is_dynamic_control_mode && now_ns > (entry->time_of_last_activity_ns + tracker->destination_timeout_ns))
        {
            to_be_removed++;
        }
        else if (entry->addr.ss_family != AF_UNSPEC)
        {
            const int sendmsg_result = tracker->data_paths->send_func(
                tracker->data_paths, transport, &entry->addr, iov, iov_length, bytes_sent);
            if (sendmsg_result < 0)
            {
                AERON_APPEND_ERR("%s", "aeron_udp_destination_tracker_send");
                aeron_udp_channel_transport_log_error(transport);
                result = 0;
            }
        }
    }

    for (int i = 0; i < starting_index; i++)
    {
        aeron_udp_destination_entry_t *entry = &tracker->destinations.array[i];

        if (is_dynamic_control_mode && now_ns > (entry->time_of_last_activity_ns + tracker->destination_timeout_ns))
        {
            to_be_removed++;
        }
        else if (entry->addr.ss_family != AF_UNSPEC)
        {
            const int sendmsg_result = tracker->data_paths->send_func(
                tracker->data_paths, transport, &entry->addr, iov, iov_length, bytes_sent);
            if (sendmsg_result < 0)
            {
                AERON_APPEND_ERR("%s", "aeron_udp_destination_tracker_send");
                aeron_udp_channel_transport_log_error(transport);
                result = 0;
            }
        }
    }

    if (0 < to_be_removed)
    {
        aeron_udp_destination_tracker_remove_inactive_destinations(tracker, now_ns);
    }

    return result;
}

static bool aeron_udp_destination_tracker_same_port(struct sockaddr_storage *lhs, struct sockaddr_storage *rhs)
{
    bool result = false;

    if (AF_INET6 == lhs->ss_family)
    {
        struct sockaddr_in6 *lhs_in6_addr = (struct sockaddr_in6 *)lhs;
        struct sockaddr_in6 *rhs_in6_addr = (struct sockaddr_in6 *)rhs;

        return ntohs(lhs_in6_addr->sin6_port) == ntohs(rhs_in6_addr->sin6_port);
    }
    else if (AF_INET == lhs->ss_family)
    {
        struct sockaddr_in *lhs_in_addr = (struct sockaddr_in *)lhs;
        struct sockaddr_in *rhs_in_addr = (struct sockaddr_in *)rhs;

        return ntohs(lhs_in_addr->sin_port) == ntohs(rhs_in_addr->sin_port);
    }

    return result;
}

static bool aeron_udp_destination_tracker_same_addr(struct sockaddr_storage *lhs, struct sockaddr_storage *rhs)
{
    bool result = false;

    if (AF_INET6 == lhs->ss_family)
    {
        struct sockaddr_in6 *lhs_in6_addr = (struct sockaddr_in6 *)lhs;
        struct sockaddr_in6 *rhs_in6_addr = (struct sockaddr_in6 *)rhs;

        return 0 == memcmp(&lhs_in6_addr->sin6_addr, &rhs_in6_addr->sin6_addr, sizeof(struct in6_addr));
    }
    else if (AF_INET == lhs->ss_family)
    {
        struct sockaddr_in *lhs_in_addr = (struct sockaddr_in *)lhs;
        struct sockaddr_in *rhs_in_addr = (struct sockaddr_in *)rhs;

        return 0 == memcmp(&lhs_in_addr->sin_addr, &rhs_in_addr->sin_addr, sizeof(struct in_addr));
    }

    return result;
}

static bool aeron_udp_destination_tracker_is_match(
    aeron_udp_destination_entry_t *entry,
    int64_t receiver_id,
    struct sockaddr_storage *addr)
{
    return
        (entry->is_receiver_id_valid && receiver_id == entry->receiver_id &&
            aeron_udp_destination_tracker_same_port(&entry->addr, addr)) ||
        (!entry->is_receiver_id_valid && aeron_udp_destination_tracker_same_addr(&entry->addr, addr) &&
            aeron_udp_destination_tracker_same_port(&entry->addr, addr));
}

int aeron_udp_destination_tracker_add_destination(
    aeron_udp_destination_tracker_t *tracker,
    int64_t receiver_id,
    bool is_receiver_id_valid,
    int64_t now_ns,
    aeron_uri_t *uri,
    struct sockaddr_storage *addr,
    int64_t destination_registration_id)
{
    int result = 0;

    AERON_ARRAY_ENSURE_CAPACITY(result, tracker->destinations, aeron_udp_destination_entry_t)
    if (result >= 0)
    {
        aeron_udp_destination_entry_t *entry = &tracker->destinations.array[tracker->destinations.length++];

        entry->receiver_id = receiver_id;
        entry->registration_id = destination_registration_id;
        entry->is_receiver_id_valid = is_receiver_id_valid;
        entry->time_of_last_activity_ns = now_ns;
        entry->destination_timeout_ns = AERON_UDP_DESTINATION_TRACKER_DESTINATION_TIMEOUT_NS;
        entry->uri = uri;
        memcpy(&entry->addr, addr, sizeof(struct sockaddr_storage));
    }

    aeron_counter_set_release(tracker->num_destinations_addr, (int64_t) tracker->destinations.length);

    return result;
}

int aeron_udp_destination_tracker_on_status_message(
    aeron_udp_destination_tracker_t *tracker, const uint8_t *buffer, size_t len, struct sockaddr_storage *addr)
{
    const aeron_status_message_header_t *status_message_header = (aeron_status_message_header_t *)buffer;
    const int64_t now_ns = aeron_clock_cached_nano_time(tracker->cached_clock);
    const int64_t receiver_id = status_message_header->receiver_id;
    const bool is_dynamic_control_mode = !tracker->is_manual_control_mode;

    int result = 0;
    bool is_existing = false;

    for (size_t i = 0, size = tracker->destinations.length; i < size; i++)
    {
        aeron_udp_destination_entry_t *entry = &tracker->destinations.array[i];

        is_existing = aeron_udp_destination_tracker_is_match(entry, receiver_id, addr);
        if (is_existing)
        {
            if (!entry->is_receiver_id_valid)
            {
                entry->receiver_id = receiver_id;
                entry->is_receiver_id_valid = true;
            }
            entry->time_of_last_activity_ns = now_ns;

            break;
        }
    }

    if (is_dynamic_control_mode && !is_existing)
    {
        result = aeron_udp_destination_tracker_add_destination(
            tracker, receiver_id, true, now_ns, NULL, addr, AERON_NULL_VALUE);
    }

    return result;
}

int aeron_udp_destination_tracker_manual_add_destination(
    aeron_udp_destination_tracker_t *tracker,
    int64_t now_ns,
    aeron_uri_t *uri,
    struct sockaddr_storage *addr,
    int64_t destination_registration_id)
{
    if (!tracker->is_manual_control_mode)
    {
        return 0;
    }

    return aeron_udp_destination_tracker_add_destination(
        tracker, now_ns, 0, false, uri, addr, destination_registration_id);
}

int aeron_udp_destination_tracker_address_compare(struct sockaddr_storage *lhs, struct sockaddr_storage *rhs)
{
    if (lhs->ss_family == rhs->ss_family)
    {
        size_t len = AF_INET == lhs->ss_family ? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6);

        return memcmp(lhs, rhs, len);
    }

    return 1;
}

int aeron_udp_destination_tracker_remove_destination(
    aeron_udp_destination_tracker_t *tracker,
    struct sockaddr_storage *addr,
    aeron_uri_t **removed_uri)
{
    for (int last_index = (int)tracker->destinations.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_udp_destination_entry_t *entry = &tracker->destinations.array[i];

        if (aeron_udp_destination_tracker_address_compare(&entry->addr, addr) == 0)
        {
            *removed_uri = entry->uri;

            aeron_array_fast_unordered_remove(
                (uint8_t *)tracker->destinations.array,
                sizeof(aeron_udp_destination_entry_t),
                (size_t)i,
                (size_t)last_index);

            tracker->destinations.length--;
            break;
        }
    }

    aeron_counter_set_release(tracker->num_destinations_addr, (int64_t) tracker->destinations.length);

    return 0;
}

int aeron_udp_destination_tracker_remove_destination_by_id(
    aeron_udp_destination_tracker_t *tracker,
    int64_t destination_registration_id,
    aeron_uri_t **removed_uri)
{
    for (int last_index = (int)tracker->destinations.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_udp_destination_entry_t *entry = &tracker->destinations.array[i];

        if (entry->registration_id == destination_registration_id)
        {
            *removed_uri = entry->uri;

            aeron_array_fast_unordered_remove(
                (uint8_t *)tracker->destinations.array,
                sizeof(aeron_udp_destination_entry_t),
                (size_t)i,
                (size_t)last_index);

            tracker->destinations.length--;
            break;
        }
    }

    aeron_counter_set_release(tracker->num_destinations_addr, (int64_t) tracker->destinations.length);

    return 0;
}

int64_t aeron_udp_destination_tracker_find_registration_id(
    aeron_udp_destination_tracker_t *tracker,
    const uint8_t *buffer,
    size_t len,
    struct sockaddr_storage *addr)
{
    aeron_error_t *error = (aeron_error_t *)buffer;
    const int64_t receiver_id = error->receiver_id;

    for (size_t i = 0, size = tracker->destinations.length; i < size; i++)
    {
        aeron_udp_destination_entry_t *entry = &tracker->destinations.array[i];
        if (aeron_udp_destination_tracker_is_match(entry, receiver_id, addr))
        {
            return entry->registration_id;
        }
    }

    return AERON_NULL_VALUE;
}

void aeron_udp_destination_tracker_check_for_re_resolution(
    aeron_udp_destination_tracker_t *tracker,
    aeron_send_channel_endpoint_t *endpoint,
    int64_t now_ns,
    aeron_driver_conductor_proxy_t *conductor_proxy)
{
    if (tracker->is_manual_control_mode)
    {
        for (size_t i = 0; i < tracker->destinations.length; i++)
        {
            aeron_udp_destination_entry_t *destination = &tracker->destinations.array[i];

            if (now_ns > (destination->time_of_last_activity_ns + destination->destination_timeout_ns))
            {
                assert(NULL != destination->uri);

                aeron_driver_conductor_proxy_on_re_resolve_endpoint(
                    conductor_proxy, destination->uri->params.udp.endpoint, endpoint, &destination->addr);
                destination->time_of_last_activity_ns = now_ns;
            }
        }
    }
}

void aeron_udp_destination_tracker_resolution_change(
    aeron_udp_destination_tracker_t *tracker, const char *endpoint_name, struct sockaddr_storage *addr)
{
    if (tracker->is_manual_control_mode)
    {
        for (size_t i = 0; i < tracker->destinations.length; i++)
        {
            aeron_udp_destination_entry_t *destination = &tracker->destinations.array[i];
            const size_t endpoint_name_len = strlen(endpoint_name);

            if (0 == strncmp(endpoint_name, destination->uri->params.udp.endpoint, endpoint_name_len + 1))
            {
                memcpy(&destination->addr, addr, sizeof(destination->addr));
            }
        }
    }
}

extern void aeron_udp_destination_tracker_set_counter(
    aeron_udp_destination_tracker_t *tracker, aeron_atomic_counter_t *counter);
