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
#ifndef AERON_DRIVER_POSITION_H
#define AERON_DRIVER_POSITION_H

#include "concurrent/aeron_counters_manager.h"
#include "aeron_counters.h"

int32_t aeron_stream_counter_allocate(
    aeron_counters_manager_t *counters_manager,
    const char *name,
    int32_t type_id,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel,
    const char *suffix);

int32_t aeron_channel_endpoint_status_allocate(
    aeron_counters_manager_t *counters_manager,
    const char *name,
    int32_t type_id,
    int64_t registration_id,
    size_t channel_length,
    const char *channel);

void aeron_channel_endpoint_status_update_label(
    aeron_counters_manager_t *counters_manager,
    int32_t counter_id,
    const char *name,
    size_t channel_length,
    const char *channel,
    size_t additional_length,
    const char *additional);

int32_t aeron_heartbeat_timestamp_allocate(
    aeron_counters_manager_t *counters_manager,
    const char *name,
    int32_t type_id,
    int64_t registration_id);

int32_t aeron_counter_publisher_limit_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel);

int32_t aeron_counter_sender_position_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel);

int32_t aeron_counter_sender_limit_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel);

int32_t aeron_counter_subscription_position_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel,
    int64_t joining_position);

int32_t aeron_counter_receiver_hwm_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel);

int32_t aeron_counter_receiver_position_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel);

int32_t aeron_counter_send_channel_status_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    size_t channel_length,
    const char *channel);

int32_t aeron_counter_receive_channel_status_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    size_t channel_length,
    const char *channel);

int32_t aeron_counter_local_sockaddr_indicator_allocate(
    aeron_counters_manager_t *counters_manager,
    const char *name,
    int64_t registration_id,
    int32_t channel_status_counter_id,
    const char *local_sockaddr);

int32_t aeron_counter_client_heartbeat_timestamp_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t client_id);

int32_t aeron_counter_publisher_position_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel);

int32_t aeron_counter_sender_bpe_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel);

int32_t aeron_counter_sender_naks_received_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel);

int32_t aeron_counter_receiver_naks_sent_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel);

#endif
