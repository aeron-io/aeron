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

#ifndef AERON_IPC_PUBLICATION_H
#define AERON_IPC_PUBLICATION_H

#include "util/aeron_bitutil.h"
#include "uri/aeron_driver_uri.h"
#include "util/aeron_fileutil.h"
#include "aeron_driver_context.h"
#include "aeron_system_counters.h"

typedef enum aeron_ipc_publication_state_enum
{
    AERON_IPC_PUBLICATION_STATE_ACTIVE,
    AERON_IPC_PUBLICATION_STATE_DRAINING,
    AERON_IPC_PUBLICATION_STATE_LINGER,
    AERON_IPC_PUBLICATION_STATE_DONE
}
aeron_ipc_publication_state_t;

typedef struct aeron_ipc_publication_stct
{
    aeron_mapped_raw_log_t mapped_raw_log;
    aeron_logbuffer_metadata_t *log_meta_data;
    aeron_position_t pub_lmt_position;
    aeron_position_t pub_pos_position;

    struct aeron_ipc_publication_conductor_fields_stct
    {
        bool has_reached_end_of_life;
        aeron_ipc_publication_state_t state;
        int32_t refcnt;
        aeron_driver_managed_resource_t managed_resource;
        aeron_subscribable_t subscribable;
        int64_t trip_limit;
        int64_t clean_position;
        int64_t consumer_position;
        int64_t last_consumer_position;
        int64_t time_of_last_consumer_position_change_ns;
    }
    conductor_fields;

    size_t position_bits_to_shift;
    int64_t term_window_length;
    int64_t trip_gain;
    int64_t unblock_timeout_ns;
    int64_t untethered_window_limit_timeout_ns;
    int64_t untethered_resting_timeout_ns;
    int64_t liveness_timeout_ns;
    int32_t initial_term_id;
    bool is_exclusive;
    bool in_cool_down;
    int64_t cool_down_expire_time_ns;
    int64_t tag;
    int32_t session_id;
    int32_t stream_id;
    int32_t starting_term_id;
    size_t starting_term_offset;
    int32_t channel_length;
    char *channel;
    size_t log_file_name_length;
    char *log_file_name;

    aeron_raw_log_close_func_t raw_log_close_func;
    aeron_raw_log_free_func_t raw_log_free_func;
    struct
    {
        aeron_untethered_subscription_state_change_func_t untethered_subscription_state_change;
    } log;

    volatile int64_t *unblocked_publications_counter;
    volatile int64_t *mapped_bytes_counter;
}
aeron_ipc_publication_t;

int aeron_ipc_publication_create(
    aeron_ipc_publication_t **publication,
    aeron_driver_context_t *context,
    int32_t session_id,
    int32_t stream_id,
    int64_t registration_id,
    aeron_position_t *pub_pos_position,
    aeron_position_t *pub_lmt_position,
    int32_t initial_term_id,
    aeron_driver_uri_publication_params_t *params,
    bool is_exclusive,
    aeron_system_counters_t *system_counters,
    size_t channel_length,
    const char *channel);

void aeron_ipc_publication_close(aeron_counters_manager_t *counters_manager, aeron_ipc_publication_t *publication);

bool aeron_ipc_publication_free(aeron_ipc_publication_t *publication);

int aeron_ipc_publication_update_pub_pos_and_lmt(aeron_ipc_publication_t *publication);

void aeron_ipc_publication_clean_buffer(aeron_ipc_publication_t *publication, int64_t position);

void aeron_ipc_publication_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_ipc_publication_t *publication, int64_t now_ns, int64_t now_ms);

void aeron_ipc_publication_incref(void *clientd);

void aeron_ipc_publication_decref(void *clientd);

void aeron_ipc_publication_reject(
    aeron_ipc_publication_t *publication,
    int64_t position,
    int32_t reason_length,
    const char *reason,
    aeron_driver_conductor_t *conductor,
    int64_t now_ns);

void aeron_ipc_publication_check_for_blocked_publisher(
    aeron_ipc_publication_t *publication, int64_t producer_position, int64_t now_ns);

inline void aeron_ipc_publication_add_subscriber_hook(void *clientd, volatile int64_t *value_addr)
{
    aeron_ipc_publication_t *publication = (aeron_ipc_publication_t *)clientd;
    AERON_SET_RELEASE(publication->log_meta_data->is_connected, 1);
}

inline void aeron_ipc_publication_remove_subscriber_hook(void *clientd, volatile int64_t *value_addr)
{
    aeron_ipc_publication_t *publication = (aeron_ipc_publication_t *)clientd;

    aeron_ipc_publication_update_pub_pos_and_lmt(publication);

    if (1 == publication->conductor_fields.subscribable.length && NULL != publication->mapped_raw_log.mapped_file.addr)
    {
        AERON_SET_RELEASE(publication->log_meta_data->is_connected, 0);
    }
}

inline bool aeron_ipc_publication_is_possibly_blocked(
    aeron_ipc_publication_t *publication, int64_t producer_position, int64_t consumer_position)
{
    int32_t producer_term_count;

    AERON_GET_ACQUIRE(producer_term_count, publication->log_meta_data->active_term_count);
    const int32_t expected_term_count = (int32_t)(consumer_position >> publication->position_bits_to_shift);

    if (producer_term_count != expected_term_count)
    {
        return true;
    }

    return producer_position > consumer_position;
}

inline int64_t aeron_ipc_publication_producer_position(aeron_ipc_publication_t *publication)
{
    int64_t raw_tail;

    AERON_LOGBUFFER_RAWTAIL_VOLATILE(raw_tail, publication->log_meta_data);

    return aeron_logbuffer_compute_position(
        aeron_logbuffer_term_id(raw_tail),
        aeron_logbuffer_term_offset(raw_tail, (int32_t)publication->mapped_raw_log.term_length),
        publication->position_bits_to_shift,
        publication->initial_term_id);
}

inline int64_t aeron_ipc_publication_join_position(aeron_ipc_publication_t *publication)
{
    int64_t position = publication->conductor_fields.consumer_position;

    for (size_t i = 0, length = publication->conductor_fields.subscribable.length; i < length; i++)
    {
        aeron_tetherable_position_t *tetherable_position = &publication->conductor_fields.subscribable.array[i];

        if (AERON_SUBSCRIPTION_TETHER_RESTING != tetherable_position->state)
        {
            const int64_t sub_pos = aeron_counter_get_acquire(tetherable_position->value_addr);

            if (sub_pos < position)
            {
                position = sub_pos;
            }
        }
    }

    return position;
}

inline bool aeron_ipc_publication_has_reached_end_of_life(aeron_ipc_publication_t *publication)
{
    return publication->conductor_fields.has_reached_end_of_life;
}

inline bool aeron_ipc_publication_is_drained(aeron_ipc_publication_t *publication)
{
    int64_t producer_position = aeron_ipc_publication_producer_position(publication);

    for (size_t i = 0, length = publication->conductor_fields.subscribable.length; i < length; i++)
    {
        aeron_tetherable_position_t *tetherable_position = &publication->conductor_fields.subscribable.array[i];

        if (AERON_SUBSCRIPTION_TETHER_RESTING != tetherable_position->state)
        {
            const int64_t sub_pos = aeron_counter_get_acquire(tetherable_position->value_addr);

            if (sub_pos < producer_position)
            {
                return false;
            }
        }
    }

    return true;
}

inline bool aeron_ipc_publication_is_accepting_subscriptions(aeron_ipc_publication_t *publication)
{
    return !publication->in_cool_down &&
        (AERON_IPC_PUBLICATION_STATE_ACTIVE == publication->conductor_fields.state ||
        (AERON_IPC_PUBLICATION_STATE_DRAINING == publication->conductor_fields.state &&
            !aeron_ipc_publication_is_drained(publication)));
}

#endif //AERON_IPC_PUBLICATION_H
