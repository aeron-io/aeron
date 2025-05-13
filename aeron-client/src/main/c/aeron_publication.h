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

#ifndef AERON_C_PUBLICATION_H
#define AERON_C_PUBLICATION_H

#include "aeronc.h"
#include "aeron_agent.h"
#include "aeron_context.h"
#include "aeron_client_conductor.h"

typedef struct aeron_publication_stct
{
    aeron_client_command_base_t command_base;
    aeron_client_conductor_t *conductor;
    const char *channel;

    aeron_log_buffer_t *log_buffer;
    aeron_logbuffer_metadata_t *log_meta_data;

    volatile int64_t *position_limit;
    volatile int64_t *channel_status_indicator;

    int64_t registration_id;
    int64_t original_registration_id;
    int32_t stream_id;
    int32_t session_id;

    int64_t max_possible_position;
    size_t max_payload_length;
    size_t max_message_length;
    size_t position_bits_to_shift;
    int32_t initial_term_id;

    int32_t position_limit_counter_id;
    int32_t channel_status_indicator_id;

    aeron_notification_t on_close_complete;
    void *on_close_complete_clientd;

    volatile bool is_closed;
}
aeron_publication_t;

int aeron_publication_create(
    aeron_publication_t **publication,
    aeron_client_conductor_t *conductor,
    const char *channel,
    int32_t stream_id,
    int32_t session_id,
    int32_t position_limit_counter_id,
    int64_t *position_limit_addr,
    int32_t channel_status_indicator_id,
    int64_t *channel_status_addr,
    aeron_log_buffer_t *log_buffer,
    int64_t original_registration_id,
    int64_t registration_id);

int aeron_publication_delete(aeron_publication_t *publication);
void aeron_publication_force_close(aeron_publication_t *publication);

inline int64_t aeron_publication_back_pressure_status(
    aeron_publication_t *publication, int64_t current_position, int32_t message_length)
{
    if ((current_position +
        (int32_t)AERON_ALIGN(message_length + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT)) >=
        publication->max_possible_position)
    {
        return AERON_PUBLICATION_MAX_POSITION_EXCEEDED;
    }

    int32_t is_connected;
    AERON_GET_ACQUIRE(is_connected, publication->log_meta_data->is_connected);
    if (1 == is_connected)
    {
        return AERON_PUBLICATION_BACK_PRESSURED;
    }

    return AERON_PUBLICATION_NOT_CONNECTED;
}

#endif //AERON_C_PUBLICATION_H
