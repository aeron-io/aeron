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

#include <aeron_socket.h>
#include <stdio.h>

#if !defined(HAVE_STRUCT_MMSGHDR)
struct mmsghdr
{
    struct msghdr msg_hdr;
    unsigned int msg_len;
};
#endif

#include "util/aeron_arrayutil.h"
#include "media/aeron_send_channel_endpoint.h"
#include "aeron_driver_sender.h"

int aeron_driver_sender_init(
    aeron_driver_sender_t *sender,
    aeron_driver_context_t *context,
    aeron_system_counters_t *system_counters,
    aeron_distinct_error_log_t *error_log)
{
    if (context->udp_channel_transport_bindings->poller_init_func(
        &sender->poller, context, AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_SENDER) < 0)
    {
        return -1;
    }

    sender->recv_buffers.vector_capacity = context->sender_io_vector_capacity;
    for (size_t i = 0; i < sender->recv_buffers.vector_capacity; i++)
    {
        size_t offset = 0;
        if (aeron_alloc_aligned(
            (void **)&sender->recv_buffers.buffers[i],
            &offset,
            context->mtu_length,
            AERON_CACHE_LINE_LENGTH) < 0)
        {
            AERON_APPEND_ERR("%s", "Failed to allocate sender->recv_buffers");
            return -1;
        }

        sender->recv_buffers.iov[i].iov_base = sender->recv_buffers.buffers[i] + offset;
        sender->recv_buffers.iov[i].iov_len = (uint32_t)context->mtu_length;
    }

    if (aeron_udp_channel_data_paths_init(
        &sender->data_paths,
        context->udp_channel_outgoing_interceptor_bindings,
        context->udp_channel_incoming_interceptor_bindings,
        context->udp_channel_transport_bindings,
        aeron_send_channel_endpoint_dispatch,
        context,
        AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_SENDER) < 0)
    {
        return -1;
    }

    sender->context = context;
    sender->poller_poll_func = context->udp_channel_transport_bindings->poller_poll_func;
    sender->recvmmsg_func = context->udp_channel_transport_bindings->recvmmsg_func;
    sender->error_log = error_log;
    sender->sender_proxy.sender = sender;
    sender->sender_proxy.command_queue = &context->sender_command_queue;
    sender->sender_proxy.fail_counter =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_SENDER_PROXY_FAILS);
    sender->sender_proxy.threading_mode = context->threading_mode;
    sender->sender_proxy.log.on_add_endpoint = context->log.sender_proxy_on_add_endpoint;
    sender->sender_proxy.log.on_remove_endpoint = context->log.sender_proxy_on_remove_endpoint;

    sender->network_publications.array = NULL;
    sender->network_publications.length = 0;
    sender->network_publications.capacity = 0;

    sender->round_robin_index = 0;
    sender->duty_cycle_counter = 0;
    sender->duty_cycle_ratio = context->send_to_sm_poll_ratio;
    sender->status_message_read_timeout_ns = (int64_t)(context->status_message_timeout_ns / 2);
    sender->control_poll_timeout_ns = 0;
    sender->total_bytes_sent_counter = aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_BYTES_SENT);
    sender->errors_counter = aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_ERRORS);
    sender->invalid_frames_counter = aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_INVALID_PACKETS);
    sender->status_messages_received_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_STATUS_MESSAGES_RECEIVED);
    sender->nak_messages_received_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_NAK_MESSAGES_RECEIVED);
    sender->error_messages_received_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_ERROR_FRAMES_RECEIVED);
    sender->resolution_changes_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_RESOLUTION_CHANGES);
    sender->short_sends_counter = aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_SHORT_SENDS);

    int64_t now_ns = context->nano_clock();
    sender->re_resolution_deadline_ns = now_ns + (int64_t)context->re_resolution_check_interval_ns;
    aeron_duty_cycle_tracker_t *dutyCycleTracker = sender->context->sender_duty_cycle_tracker;
    dutyCycleTracker->update(dutyCycleTracker->state, now_ns);

    return 0;
}

static void aeron_driver_sender_on_rb_command_queue(
    int32_t msg_type_id,
    const void *message,
    size_t size,
    void *clientd)
{
    aeron_command_base_t *cmd = (aeron_command_base_t *)message;
    cmd->func(clientd, cmd);
}

int aeron_driver_sender_do_work(void *clientd)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;

    int64_t now_ns = sender->context->nano_clock();
    aeron_clock_update_cached_nano_time(sender->context->sender_cached_clock, now_ns);

    aeron_duty_cycle_tracker_t *tracker = sender->context->sender_duty_cycle_tracker;
    tracker->measure_and_update(tracker->state, now_ns);

    int work_count = (int)aeron_mpsc_rb_read(
        sender->sender_proxy.command_queue, aeron_driver_sender_on_rb_command_queue, sender, AERON_COMMAND_DRAIN_LIMIT);

    int64_t bytes_received = 0;
    int64_t short_sends_before = aeron_counter_get_plain(sender->short_sends_counter);
    int bytes_sent = aeron_driver_sender_do_send(sender, now_ns);

    if (0 == bytes_sent ||
        ++sender->duty_cycle_counter >= sender->duty_cycle_ratio ||
        now_ns > sender->control_poll_timeout_ns ||
        short_sends_before < aeron_counter_get_plain(sender->short_sends_counter))
    {
        struct mmsghdr mmsghdr[1];
        mmsghdr[0].msg_hdr.msg_name = &sender->recv_buffers.addrs[0];
        mmsghdr[0].msg_hdr.msg_namelen = sizeof(sender->recv_buffers.addrs[0]);
        mmsghdr[0].msg_hdr.msg_iov = &sender->recv_buffers.iov[0];
        mmsghdr[0].msg_hdr.msg_iovlen = 1;
        mmsghdr[0].msg_hdr.msg_flags = 0;
        mmsghdr[0].msg_hdr.msg_control = NULL;
        mmsghdr[0].msg_hdr.msg_controllen = 0;
        mmsghdr[0].msg_len = 0;

        int poll_result = sender->poller_poll_func(
            &sender->poller,
            mmsghdr,
            1,
            &bytes_received,
            sender->data_paths.recv_func,
            sender->recvmmsg_func,
            sender);

        if (poll_result < 0)
        {
            AERON_APPEND_ERR("%s", "sender poller_poll");
            aeron_driver_sender_log_error(sender);
        }
        work_count += (poll_result < 0 ? 0 : poll_result);

        sender->duty_cycle_counter = 0;
        sender->control_poll_timeout_ns = now_ns + sender->status_message_read_timeout_ns;
    }

    if (sender->context->re_resolution_check_interval_ns > 0 && (sender->re_resolution_deadline_ns - now_ns) < 0)
    {
        sender->re_resolution_deadline_ns = (int64_t)(now_ns + sender->context->re_resolution_check_interval_ns);
        aeron_udp_transport_poller_check_send_endpoint_re_resolutions(
                &sender->poller, now_ns, sender->context->conductor_proxy);
    }

    return work_count + bytes_sent + (int)bytes_received;
}

void aeron_driver_sender_on_close(void *clientd)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;

    for (size_t i = 0; i < sender->recv_buffers.vector_capacity; i++)
    {
        aeron_free(sender->recv_buffers.buffers[i]);
    }

    aeron_udp_channel_data_paths_delete(&sender->data_paths);

    sender->context->udp_channel_transport_bindings->poller_close_func(&sender->poller);
    aeron_free(sender->network_publications.array);
}

void aeron_driver_sender_on_add_endpoint(void *clientd, void *command)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    aeron_command_base_t *cmd = (aeron_command_base_t *)command;
    aeron_send_channel_endpoint_t *endpoint = (aeron_send_channel_endpoint_t *)cmd->item;

    if (aeron_udp_channel_interceptors_transport_notifications(
        endpoint->transport.data_paths,
        &endpoint->transport,
        endpoint->conductor_fields.udp_channel,
        NULL,
        AERON_UDP_CHANNEL_INTERCEPTOR_ADD_NOTIFICATION) < 0)
    {
        AERON_APPEND_ERR("%s", "sender on_add_endpoint interceptors transport notifications");
        aeron_driver_sender_log_error(sender);
    }

    if (sender->context->udp_channel_transport_bindings->poller_add_func(&sender->poller, &endpoint->transport) < 0)
    {
        AERON_APPEND_ERR("%s", "sender on_add_endpoint");
        aeron_driver_sender_log_error(sender);
    }
}

void aeron_driver_sender_on_remove_endpoint(void *clientd, void *command)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    aeron_command_base_t *cmd = (aeron_command_base_t *)command;
    aeron_send_channel_endpoint_t *endpoint = (aeron_send_channel_endpoint_t *)cmd->item;

    if (aeron_udp_channel_interceptors_transport_notifications(
        endpoint->transport.data_paths,
        &endpoint->transport,
        endpoint->conductor_fields.udp_channel,
        NULL,
        AERON_UDP_CHANNEL_INTERCEPTOR_REMOVE_NOTIFICATION) < 0)
    {
        AERON_APPEND_ERR("%s", "sender on_remove_endpoint interceptors transport notifications");
        aeron_driver_sender_log_error(sender);
    }

    if (sender->context->udp_channel_transport_bindings->poller_remove_func(&sender->poller, &endpoint->transport) < 0)
    {
        AERON_APPEND_ERR("%s", "sender on_remove_endpoint");
        aeron_driver_sender_log_error(sender);
    }

    aeron_driver_conductor_proxy_on_release_resource(
        sender->context->conductor_proxy, endpoint, AERON_DRIVER_CONDUCTOR_RESOURCE_TYPE_SEND_CHANNEL_ENDPOINT);
}

void aeron_driver_sender_on_add_publication(void *clientd, void *command)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    aeron_command_base_t *cmd = (aeron_command_base_t *)command;
    aeron_network_publication_t *publication = (aeron_network_publication_t *)cmd->item;
    aeron_udp_channel_transport_t *transport = &publication->endpoint->transport;

    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(
        ensure_capacity_result, sender->network_publications, aeron_driver_sender_network_publication_entry_t)

    if (ensure_capacity_result < 0)
    {
        AERON_APPEND_ERR("%s", "sender on_add_publication");
        aeron_driver_sender_log_error(sender);
        return;
    }

    sender->network_publications.array[sender->network_publications.length++].publication = publication;
    if (aeron_send_channel_endpoint_add_publication(publication->endpoint, publication) < 0)
    {
        AERON_APPEND_ERR("%s", "sender on_add_publication add_publication");
        aeron_driver_sender_log_error(sender);
    }

    if (aeron_udp_channel_interceptors_publication_notifications(
        transport->data_paths, transport, publication, AERON_UDP_CHANNEL_INTERCEPTOR_ADD_NOTIFICATION) < 0)
    {
        AERON_APPEND_ERR("%s", "sender on_add_publication interceptors publication notifications");
        aeron_driver_sender_log_error(sender);
    }
}

void aeron_driver_sender_on_remove_publication(void *clientd, void *command)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    aeron_command_base_t *cmd = (aeron_command_base_t *)command;
    aeron_network_publication_t *publication = (aeron_network_publication_t *)cmd->item;
    aeron_udp_channel_transport_t *transport = &publication->endpoint->transport;

    for (size_t i = 0, size = sender->network_publications.length, last_index = size - 1; i < size; i++)
    {
        if (publication == sender->network_publications.array[i].publication)
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *)sender->network_publications.array,
                sizeof(aeron_driver_sender_network_publication_entry_t),
                i,
                last_index);
            sender->network_publications.length--;
            break;
        }
    }

    if (aeron_send_channel_endpoint_remove_publication(publication->endpoint, publication) < 0)
    {
        AERON_APPEND_ERR("%s", "sender on_remove_publication");
        aeron_driver_sender_log_error(sender);
    }

    if (aeron_udp_channel_interceptors_publication_notifications(
        transport->data_paths, transport, publication, AERON_UDP_CHANNEL_INTERCEPTOR_REMOVE_NOTIFICATION) < 0)
    {
        AERON_APPEND_ERR("%s", "sender on_remove_publication interceptors publication notifications");
        aeron_driver_sender_log_error(sender);
    }

    aeron_driver_conductor_proxy_on_release_resource(
        sender->context->conductor_proxy, publication, AERON_DRIVER_CONDUCTOR_RESOURCE_TYPE_NETWORK_PUBLICATION);
}

void aeron_driver_sender_on_add_destination(void *clientd, void *command)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    aeron_command_destination_t *cmd = (aeron_command_destination_t *)command;

    if (aeron_send_channel_endpoint_add_destination(
        cmd->endpoint, cmd->uri, &cmd->control_address, cmd->destination_registration_id) < 0)
    {
        AERON_APPEND_ERR("%s", "sender on_add_destination");
        aeron_driver_sender_log_error(sender);
    }
}

void aeron_driver_sender_on_remove_destination(void *clientd, void *command)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    aeron_command_destination_t *cmd = (aeron_command_destination_t *)command;
    aeron_uri_t *old_uri = NULL;

    if (aeron_send_channel_endpoint_remove_destination(cmd->endpoint, &cmd->control_address, &old_uri) < 0)
    {
        AERON_APPEND_ERR("%s", "sender on_remove_destination");
        aeron_driver_sender_log_error(sender);
    }

    if (NULL != old_uri)
    {
        aeron_driver_conductor_proxy_on_delete_send_destination(sender->context->conductor_proxy, old_uri);
    }
}

void aeron_driver_sender_on_remove_destination_by_id(void *clientd, void *command)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    aeron_command_destination_by_id_t *cmd = (aeron_command_destination_by_id_t *)command;
    aeron_uri_t *old_uri = NULL;

    if (aeron_send_channel_endpoint_remove_destination_by_id(
        cmd->endpoint, cmd->destination_registration_id, &old_uri) < 0)
    {
        AERON_APPEND_ERR("%s", "sender on_remove_destination");
        aeron_driver_sender_log_error(sender);
    }

    if (NULL != old_uri)
    {
        aeron_driver_conductor_proxy_on_delete_send_destination(sender->context->conductor_proxy, old_uri);
    }
}

void aeron_driver_sender_on_resolution_change(void *clientd, void *command)
{
    aeron_driver_sender_t *sender = clientd;
    aeron_command_sender_resolution_change_t *resolution_change = (aeron_command_sender_resolution_change_t *)command;
    aeron_send_channel_endpoint_t *endpoint = resolution_change->endpoint;

    if (aeron_send_channel_endpoint_resolution_change(
        sender->context, endpoint, resolution_change->endpoint_name, &resolution_change->new_addr) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        aeron_driver_sender_log_error(sender);
    }

    aeron_counter_get_and_add_release(sender->resolution_changes_counter, 1);
}

int aeron_driver_sender_do_send(aeron_driver_sender_t *sender, int64_t now_ns)
{
    int bytes_sent = 0;
    aeron_driver_sender_network_publication_entry_t *publications = sender->network_publications.array;
    size_t length = sender->network_publications.length;
    size_t starting_index = sender->round_robin_index++;

    if (starting_index >= length)
    {
        sender->round_robin_index = starting_index = 0;
    }

    for (size_t i = starting_index; i < length; i++)
    {
        int result = aeron_network_publication_send(publications[i].publication, now_ns);
        if (result < 0)
        {
            AERON_APPEND_ERR("%s", "sender do_send");
            aeron_driver_sender_log_error(sender);
        }
        else
        {
            bytes_sent += result;
        }
    }

    for (size_t i = 0; i < starting_index; i++)
    {
        int result = aeron_network_publication_send(publications[i].publication, now_ns);
        if (result < 0)
        {
            AERON_APPEND_ERR("%s", "sender do_send");
            aeron_driver_sender_log_error(sender);
        }
        else
        {
            bytes_sent += result;
        }
    }

    aeron_counter_get_and_add_release(sender->total_bytes_sent_counter, bytes_sent);

    return bytes_sent;
}

extern void aeron_driver_sender_log_error(aeron_driver_sender_t *sender);
