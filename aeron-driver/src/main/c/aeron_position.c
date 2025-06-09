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

#include <stdio.h>
#include <inttypes.h>
#include <string.h>
#include "aeron_position.h"
#include "aeron_driver_context.h"

int32_t aeron_stream_counter_allocate(
    aeron_counters_manager_t *counters_manager,
    const char *name,
    int32_t type_id,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel,
    const char *suffix)
{
    char label[AERON_COUNTER_MAX_LABEL_LENGTH];
    int total_length = snprintf(
        label, sizeof(label), "%s: %" PRId64 " %" PRId32 " %" PRId32 " %.*s %s",
        name, registration_id, session_id, stream_id, (int)channel_length, channel, suffix);
    size_t label_length = AERON_MIN(AERON_COUNTER_MAX_LABEL_LENGTH, (size_t)total_length);

    aeron_stream_position_counter_key_layout_t layout =
        {
            .registration_id = registration_id,
            .session_id = session_id,
            .stream_id = stream_id,
        };

    size_t key_channel_length = channel_length > sizeof(layout.channel) ? sizeof(layout.channel) : channel_length;
    layout.channel_length = (int32_t)key_channel_length;
    memcpy(layout.channel, channel, key_channel_length);

    int32_t counter_id = aeron_counters_manager_allocate(
        counters_manager, type_id, (const uint8_t *)&layout, sizeof(layout), label, label_length);

    if (counter_id >= 0)
    {
        aeron_counters_manager_counter_registration_id(counters_manager, counter_id, registration_id);
    }

    return counter_id;
}

int32_t aeron_counter_publisher_limit_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel)
{
    return aeron_stream_counter_allocate(
        counters_manager,
        AERON_COUNTER_PUBLISHER_LIMIT_NAME,
        AERON_COUNTER_PUBLISHER_LIMIT_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel_length,
        channel,
        "");
}

int32_t aeron_counter_subscription_position_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel,
    int64_t joining_position)
{
    char buffer[64];

    snprintf(buffer, sizeof(buffer) - 1, "@%" PRId64, joining_position);

    return aeron_stream_counter_allocate(
        counters_manager,
        AERON_COUNTER_SUBSCRIPTION_POSITION_NAME,
        AERON_COUNTER_SUBSCRIPTION_POSITION_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel_length,
        channel,
        buffer);
}

int32_t aeron_counter_sender_position_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel)
{
    return aeron_stream_counter_allocate(
        counters_manager,
        AERON_COUNTER_SENDER_POSITION_NAME,
        AERON_COUNTER_SENDER_POSITION_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel_length,
        channel,
        "");
}

int32_t aeron_counter_sender_limit_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel)
{
    return aeron_stream_counter_allocate(
        counters_manager,
        AERON_COUNTER_SENDER_LIMIT_NAME,
        AERON_COUNTER_SENDER_LIMIT_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel_length,
        channel,
        "");
}

int32_t aeron_counter_receiver_hwm_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel)
{
    return aeron_stream_counter_allocate(
        counters_manager,
        AERON_COUNTER_RECEIVER_HWM_NAME,
        AERON_COUNTER_RECEIVER_HWM_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel_length,
        channel,
        "");
}

int32_t aeron_counter_receiver_position_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel)
{
    return aeron_stream_counter_allocate(
        counters_manager,
        AERON_COUNTER_RECEIVER_POSITION_NAME,
        AERON_COUNTER_RECEIVER_POSITION_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel_length,
        channel,
        "");
}

int32_t aeron_channel_endpoint_status_allocate(
    aeron_counters_manager_t *counters_manager,
    const char *name,
    int32_t type_id,
    int64_t registration_id,
    size_t channel_length,
    const char *channel)
{
    char label[AERON_COUNTER_MAX_LABEL_LENGTH];
    int total_length = snprintf(label, sizeof(label), "%s: %.*s", name, (int)channel_length, channel);
    size_t label_length = AERON_MIN(AERON_COUNTER_MAX_LABEL_LENGTH, (size_t)total_length);

    aeron_channel_endpoint_status_key_layout_t layout;
    size_t key_channel_length = channel_length > sizeof(layout.channel) ? sizeof(layout.channel) : channel_length;
    layout.channel_length = (int32_t)key_channel_length;
    memcpy(layout.channel, channel, key_channel_length);

    int32_t counter_id = aeron_counters_manager_allocate(
        counters_manager, type_id, (const uint8_t *)&layout, sizeof(layout), label, label_length);

    if (counter_id >= 0)
    {
        aeron_counters_manager_counter_registration_id(counters_manager, counter_id, registration_id);
    }
    
    return counter_id;
}

void aeron_channel_endpoint_status_update_label(
    aeron_counters_manager_t *counters_manager,
    int32_t counter_id,
    const char *name,
    size_t channel_length,
    const char *channel,
    size_t additional_length,
    const char *additional)
{
    char label[AERON_COUNTER_MAX_LABEL_LENGTH];
    int total_length = snprintf(
        label, sizeof(label), "%s: %.*s %.*s", name, (int)channel_length, channel, (int)additional_length, additional);
    size_t label_length = AERON_MIN(AERON_COUNTER_MAX_LABEL_LENGTH, (size_t)total_length);

    aeron_counters_manager_update_label(counters_manager, counter_id, label_length, label);
}

int32_t aeron_counter_send_channel_status_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    size_t channel_length,
    const char *channel)
{
    return aeron_channel_endpoint_status_allocate(
        counters_manager,
        AERON_COUNTER_SEND_CHANNEL_STATUS_NAME,
        AERON_COUNTER_SEND_CHANNEL_STATUS_TYPE_ID,
        registration_id,
        channel_length,
        channel);
}

int32_t aeron_counter_receive_channel_status_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    size_t channel_length,
    const char *channel)
{
    return aeron_channel_endpoint_status_allocate(
        counters_manager,
        AERON_COUNTER_RECEIVE_CHANNEL_STATUS_NAME,
        AERON_COUNTER_RECEIVE_CHANNEL_STATUS_TYPE_ID,
        registration_id,
        channel_length,
        channel);
}

int32_t aeron_counter_local_sockaddr_indicator_allocate(
    aeron_counters_manager_t *counters_manager,
    const char *name,
    int64_t registration_id,
    int32_t channel_status_counter_id,
    const char *local_sockaddr)
{
    aeron_local_sockaddr_key_layout_t sockaddr_layout;
    size_t local_sockaddr_srclen = strlen(local_sockaddr);
    size_t local_sockaddr_dstlen = sizeof(sockaddr_layout.local_sockaddr) - 1;

    sockaddr_layout.channel_status_id = channel_status_counter_id,
    sockaddr_layout.local_sockaddr_len =
        (int32_t)(local_sockaddr_srclen < local_sockaddr_dstlen ? local_sockaddr_srclen : local_sockaddr_dstlen);
    memcpy(sockaddr_layout.local_sockaddr, local_sockaddr, sockaddr_layout.local_sockaddr_len);
    sockaddr_layout.local_sockaddr[sockaddr_layout.local_sockaddr_len] = '\0';

    char label[AERON_COUNTER_MAX_LABEL_LENGTH];
    int total_length = snprintf(
        label, sizeof(label), "%s: %" PRId32 " %s", name, channel_status_counter_id, local_sockaddr);
    size_t label_length = AERON_MIN(AERON_COUNTER_MAX_LABEL_LENGTH, (size_t)total_length);

    int32_t counter_id = aeron_counters_manager_allocate(
        counters_manager,
        AERON_COUNTER_LOCAL_SOCKADDR_TYPE_ID,
        (const uint8_t *)&sockaddr_layout,
        sizeof(sockaddr_layout),
        label,
        label_length);

    if (counter_id >= 0)
    {
        aeron_counters_manager_counter_registration_id(counters_manager, counter_id, registration_id);
    }

    return counter_id;
}

int32_t aeron_heartbeat_timestamp_allocate(
    aeron_counters_manager_t *counters_manager,
    const char *name,
    int32_t type_id,
    int64_t registration_id)
{
    char label[AERON_COUNTER_MAX_LABEL_LENGTH];
    int label_length = snprintf(label, sizeof(label), "%s: id=%" PRId64, name, registration_id);
    aeron_heartbeat_timestamp_key_layout_t layout =
        {
            .registration_id = registration_id
        };

    return aeron_counters_manager_allocate(
        counters_manager, type_id, (const uint8_t *)&layout, sizeof(layout), label, (size_t)label_length);
}

int32_t aeron_counter_client_heartbeat_timestamp_allocate(aeron_counters_manager_t *counters_manager, int64_t client_id)
{
    return aeron_heartbeat_timestamp_allocate(
        counters_manager,
        AERON_COUNTER_CLIENT_HEARTBEAT_TIMESTAMP_NAME,
        AERON_COUNTER_CLIENT_HEARTBEAT_TIMESTAMP_TYPE_ID,
        client_id);
}

int32_t aeron_counter_publisher_position_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel)
{
    return aeron_stream_counter_allocate(
        counters_manager,
        AERON_COUNTER_PUBLISHER_POSITION_NAME,
        AERON_COUNTER_PUBLISHER_POSITION_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel_length,
        channel,
        "");
}

int32_t aeron_counter_sender_bpe_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel)
{
    return aeron_stream_counter_allocate(
        counters_manager,
        AERON_COUNTER_SENDER_BPE_NAME,
        AERON_COUNTER_SENDER_BPE_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel_length,
        channel,
        "");
}

int32_t aeron_counter_sender_naks_received_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel)
{
    return aeron_stream_counter_allocate(
        counters_manager,
        AERON_COUNTER_SENDER_NAKS_RECEIVED_NAME,
        AERON_COUNTER_SENDER_NAKS_RECEIVED_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel_length,
        channel,
        "");
}

int32_t aeron_counter_receiver_naks_sent_allocate(
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel)
{
    return aeron_stream_counter_allocate(
        counters_manager,
        AERON_COUNTER_RECEIVER_NAKS_SENT_NAME,
        AERON_COUNTER_RECEIVER_NAKS_SENT_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel_length,
        channel,
        "");
}
