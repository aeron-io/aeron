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

#ifndef AERON_DRIVER_SENDER_H
#define AERON_DRIVER_SENDER_H

#include "aeron_driver_context.h"
#include "aeron_driver_sender_proxy.h"
#include "aeron_system_counters.h"
#include "media/aeron_udp_transport_poller.h"
#include "aeron_network_publication.h"
#include "concurrent/aeron_distinct_error_log.h"

typedef struct aeron_driver_sender_network_publication_entry_stct
{
    aeron_network_publication_t *publication;
}
aeron_driver_sender_network_publication_entry_t;

typedef struct aeron_driver_sender_stct
{
    aeron_driver_sender_proxy_t sender_proxy;
    aeron_udp_transport_poller_t poller;

    struct aeron_driver_sender_network_publications_stct
    {
        aeron_driver_sender_network_publication_entry_t *array;
        size_t length;
        size_t capacity;
    }
    network_publications;

    struct aeron_driver_sender_buffers_stct
    {
        size_t vector_capacity;
        uint8_t *buffers[AERON_DRIVER_SENDER_IO_VECTOR_LENGTH_MAX];
        struct iovec iov[AERON_DRIVER_SENDER_IO_VECTOR_LENGTH_MAX];
        struct sockaddr_storage addrs[AERON_DRIVER_SENDER_IO_VECTOR_LENGTH_MAX];
    }
    recv_buffers;

    aeron_udp_channel_data_paths_t data_paths;

    volatile int64_t *total_bytes_sent_counter;
    volatile int64_t *errors_counter;
    volatile int64_t *invalid_frames_counter;
    volatile int64_t *status_messages_received_counter;
    volatile int64_t *status_messages_rejected_counter;
    volatile int64_t *nak_messages_received_counter;
    volatile int64_t *error_messages_received_counter;
    volatile int64_t *resolution_changes_counter;
    volatile int64_t *short_sends_counter;

    aeron_driver_context_t *context;
    aeron_udp_transport_poller_poll_func_t poller_poll_func;
    aeron_udp_channel_transport_recvmmsg_func_t recvmmsg_func;
    aeron_distinct_error_log_t *error_log;
    int64_t status_message_read_timeout_ns;
    int64_t control_poll_timeout_ns;
    int64_t re_resolution_deadline_ns;
    size_t round_robin_index;
    size_t duty_cycle_counter;
    size_t duty_cycle_ratio;

    uint8_t padding[AERON_CACHE_LINE_LENGTH];
}
aeron_driver_sender_t;

inline void aeron_driver_sender_log_error(aeron_driver_sender_t *sender)
{
    aeron_distinct_error_log_record(sender->error_log, aeron_errcode(), aeron_errmsg());
    aeron_counter_increment(sender->errors_counter);
    aeron_err_clear();
}

int aeron_driver_sender_init(
    aeron_driver_sender_t *sender,
    aeron_driver_context_t *context,
    aeron_system_counters_t *system_counters,
    aeron_distinct_error_log_t *error_log);

int aeron_driver_sender_do_work(void *clientd);
void aeron_driver_sender_on_close(void *clientd);

void aeron_driver_sender_on_add_endpoint(void *clientd, void *command);
void aeron_driver_sender_on_remove_endpoint(void *clientd, void *command);
void aeron_driver_sender_on_add_publication(void *clientd, void *command);
void aeron_driver_sender_on_remove_publication(void *clientd, void *command);
void aeron_driver_sender_on_add_destination(void *clientd, void *command);
void aeron_driver_sender_on_remove_destination(void *clientd, void *command);
void aeron_driver_sender_on_remove_destination_by_id(void *clientd, void *command);
void aeron_driver_sender_on_resolution_change(void *clientd, void *command);

int aeron_driver_sender_do_send(aeron_driver_sender_t *sender, int64_t now_ns);

#endif //AERON_DRIVER_SENDER_H
