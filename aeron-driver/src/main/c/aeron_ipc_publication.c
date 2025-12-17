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

#include <string.h>
#include <errno.h>
#include <inttypes.h>
#include "concurrent/aeron_counters_manager.h"
#include "concurrent/aeron_logbuffer_unblocker.h"
#include "aeron_ipc_publication.h"
#include "aeron_alloc.h"
#include "aeron_driver_conductor.h"

static void aeron_ipc_publication_handle_managed_resource_event(aeron_driver_managed_resource_event_t event, void *clientd);

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
    const char *channel)
{
    char path[AERON_MAX_PATH];
    int path_length = aeron_ipc_publication_location(path, sizeof(path), context->aeron_dir, registration_id);
    if (path_length < 0)
    {
        AERON_APPEND_ERR("%s", "Could not resolve IPC publication file path");
        return -1;
    }
    aeron_ipc_publication_t *_pub = NULL;
    const uint64_t log_length = aeron_logbuffer_compute_log_length(params->term_length, context->file_page_size);

    *publication = NULL;

    if (aeron_driver_context_run_storage_checks(context, log_length) < 0)
    {
        return -1;
    }

    if (aeron_alloc((void **)&_pub, sizeof(aeron_ipc_publication_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Could not allocate IPC publication");
        return -1;
    }

    _pub->log_file_name = NULL;
    if (aeron_alloc((void **)(&_pub->log_file_name), (size_t)path_length + 1) < 0)
    {
        aeron_free(_pub);
        AERON_APPEND_ERR("%s", "Could not allocate IPC publication log_file_name");
        return -1;
    }

    _pub->channel = NULL;
    if (aeron_alloc((void **)(&_pub->channel), (size_t)channel_length + 1) < 0)
    {
        aeron_free(_pub->log_file_name);
        aeron_free(_pub);
        AERON_APPEND_ERR("%s", "Could not allocate IPC publication channel");
        return -1;
    }

    if (context->raw_log_map_func(
        &_pub->mapped_raw_log, path, params->is_sparse, params->term_length, context->file_page_size) < 0)
    {
        aeron_free(_pub->log_file_name);
        aeron_free(_pub->channel);
        aeron_free(_pub);
        AERON_APPEND_ERR("error mapping IPC raw log: %s", path);
        return -1;
    }

    _pub->mapped_bytes_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_BYTES_CURRENTLY_MAPPED);
    aeron_counter_get_and_add_release(_pub->mapped_bytes_counter, (int64_t) log_length);

    _pub->raw_log_close_func = context->raw_log_close_func;
    _pub->raw_log_free_func = context->raw_log_free_func;
    _pub->log.untethered_subscription_state_change = context->log.untethered_subscription_on_state_change;
    _pub->log.publication_revoke = context->log.publication_revoke;

    strncpy(_pub->log_file_name, path, (size_t)path_length);
    _pub->log_file_name[path_length] = '\0';
    _pub->log_file_name_length = (size_t)path_length;
    _pub->log_meta_data = (aeron_logbuffer_metadata_t *)(_pub->mapped_raw_log.log_meta_data.addr);

    strncpy(_pub->channel, channel, channel_length);
    _pub->channel[channel_length] = '\0';
    _pub->channel_length = (int32_t)channel_length;

    if (params->has_position)
    {
        int32_t term_id = params->term_id;
        int32_t term_count = aeron_logbuffer_compute_term_count(params->term_id, initial_term_id);
        size_t active_index = aeron_logbuffer_index_by_term_count(term_count);

        _pub->log_meta_data->term_tail_counters[active_index] =
            (term_id * (INT64_C(1) << 32)) | (int64_t)params->term_offset;

        for (int i = 1; i < AERON_LOGBUFFER_PARTITION_COUNT; i++)
        {
            int32_t expected_term_id = (term_id + i) - AERON_LOGBUFFER_PARTITION_COUNT;
            active_index = (active_index + 1) % AERON_LOGBUFFER_PARTITION_COUNT;
            _pub->log_meta_data->term_tail_counters[active_index] = expected_term_id * (INT64_C(1) << 32);
        }

        _pub->log_meta_data->active_term_count = term_count;
    }
    else
    {
        _pub->log_meta_data->term_tail_counters[0] = initial_term_id * (INT64_C(1) << 32);
        for (int i = 1; i < AERON_LOGBUFFER_PARTITION_COUNT; i++)
        {
            int32_t expected_term_id = (initial_term_id + i) - AERON_LOGBUFFER_PARTITION_COUNT;
            _pub->log_meta_data->term_tail_counters[i] = expected_term_id * (INT64_C(1) << 32);
        }

        _pub->log_meta_data->active_term_count = 0;
    }

    int64_t now_ns = aeron_clock_cached_nano_time(context->cached_clock);

    aeron_logbuffer_metadata_init(
        _pub->mapped_raw_log.log_meta_data.addr,
        INT64_MAX,
        0,
        0,
        registration_id,
        initial_term_id,
        (int32_t)params->mtu_length,
        (int32_t)params->term_length,
        (int32_t)context->file_page_size,
        (int32_t)params->publication_window_length,
        0,
        0,
        (int32_t)context->os_buffer_lengths.default_so_sndbuf,
        (int32_t)context->os_buffer_lengths.max_so_sndbuf,
        0,
        (int32_t)context->os_buffer_lengths.default_so_rcvbuf,
        (int32_t)context->os_buffer_lengths.max_so_rcvbuf,
        (int32_t)params->max_resend,
        session_id,
        stream_id,
        (int64_t)params->entity_tag,
        (int64_t)params->response_correlation_id,
        (int64_t)params->linger_timeout_ns,
        (int64_t)params->untethered_window_limit_timeout_ns,
        (int64_t)params->untethered_linger_timeout_ns,
        (int64_t)params->untethered_resting_timeout_ns,
        (uint8_t)false,
        (uint8_t)params->is_response,
        (uint8_t)false,
        (uint8_t)false,
        (uint8_t)params->is_sparse,
        (uint8_t)params->signal_eos,
        (uint8_t)params->spies_simulate_connection,
        (uint8_t)false,
        is_exclusive ? AERON_LOGBUFFER_TYPE_EXCLUSIVE_PUBLICATION : AERON_LOGBUFFER_TYPE_CONCURRENT_PUBLICATION);

    _pub->conductor_fields.subscribable.correlation_id = registration_id;
    _pub->conductor_fields.subscribable.array = NULL;
    _pub->conductor_fields.subscribable.length = 0;
    _pub->conductor_fields.subscribable.capacity = 0;
    _pub->conductor_fields.subscribable.inactive_count = 0;
    _pub->conductor_fields.subscribable.add_position_hook_func = aeron_ipc_publication_add_subscriber_hook;
    _pub->conductor_fields.subscribable.remove_position_hook_func = aeron_ipc_publication_remove_subscriber_hook;
    _pub->conductor_fields.subscribable.clientd = _pub;
    _pub->conductor_fields.managed_resource.registration_id = registration_id;
    _pub->conductor_fields.managed_resource.clientd = _pub;
    _pub->conductor_fields.managed_resource.handle_event = aeron_ipc_publication_handle_managed_resource_event;
    _pub->conductor_fields.has_reached_end_of_life = false;
    _pub->conductor_fields.response_correlation_id = params->response_correlation_id;
    _pub->conductor_fields.trip_limit = 0;
    _pub->conductor_fields.time_of_last_consumer_position_change_ns = now_ns;
    _pub->conductor_fields.state = AERON_IPC_PUBLICATION_STATE_ACTIVE;
    _pub->conductor_fields.refcnt = 1;
    _pub->session_id = session_id;
    _pub->stream_id = stream_id;
    _pub->pub_lmt_position.counter_id = pub_lmt_position->counter_id;
    _pub->pub_lmt_position.value_addr = pub_lmt_position->value_addr;
    _pub->pub_pos_position.counter_id = pub_pos_position->counter_id;
    _pub->pub_pos_position.value_addr = pub_pos_position->value_addr;
    _pub->initial_term_id = initial_term_id;
    _pub->starting_term_id = params->has_position ? params->term_id : initial_term_id;
    _pub->starting_term_offset = params->has_position ? params->term_offset : 0;
    _pub->position_bits_to_shift = (size_t)aeron_number_of_trailing_zeroes((int32_t)params->term_length);
    _pub->term_window_length = params->publication_window_length;
    _pub->trip_gain = _pub->term_window_length / 8;
    _pub->unblock_timeout_ns = (int64_t)context->publication_unblock_timeout_ns;
    _pub->untethered_window_limit_timeout_ns = (int64_t)params->untethered_window_limit_timeout_ns;
    _pub->untethered_linger_timeout_ns = params->untethered_linger_timeout_ns;
    _pub->untethered_resting_timeout_ns = (int64_t)params->untethered_resting_timeout_ns;
    _pub->liveness_timeout_ns = (int64_t)context->image_liveness_timeout_ns;
    _pub->is_exclusive = is_exclusive;
    _pub->in_cool_down = false;
    _pub->cool_down_expire_time_ns = 0;

    _pub->conductor_fields.consumer_position = aeron_ipc_publication_producer_position(_pub);
    _pub->conductor_fields.last_consumer_position = _pub->conductor_fields.consumer_position;
    _pub->conductor_fields.clean_position = _pub->conductor_fields.consumer_position;

    _pub->unblocked_publications_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_UNBLOCKED_PUBLICATIONS);
    _pub->publications_revoked_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_PUBLICATIONS_REVOKED);

    *publication = _pub;

    return 0;
}

void aeron_ipc_publication_close(aeron_counters_manager_t *counters_manager, aeron_ipc_publication_t *publication)
{
    if (NULL != publication)
    {
        aeron_subscribable_t *subscribable = &publication->conductor_fields.subscribable;

        aeron_counters_manager_free(counters_manager, publication->pub_lmt_position.counter_id);
        aeron_counters_manager_free(counters_manager, publication->pub_pos_position.counter_id);

        for (size_t i = 0, length = subscribable->length; i < length; i++)
        {
            aeron_counters_manager_free(counters_manager, subscribable->array[i].counter_id);
        }
        aeron_free(subscribable->array);

        aeron_free(publication->channel);
    }
}

bool aeron_ipc_publication_free(aeron_ipc_publication_t *publication)
{
    if (NULL == publication)
    {
        return true;
    }

    if (!publication->raw_log_free_func(&publication->mapped_raw_log, publication->log_file_name))
    {
        return false;
    }

    aeron_counter_get_and_add_release(
        publication->mapped_bytes_counter, -((int64_t) publication->mapped_raw_log.mapped_file.length));

    aeron_free(publication->log_file_name);
    aeron_free(publication);

    return true;
}

void aeron_ipc_publication_reject(
    aeron_ipc_publication_t *publication,
    int64_t position,
    int32_t reason_length,
    const char *reason,
    aeron_driver_conductor_t *conductor,
    int64_t now_ns)
{
    {
        uint8_t buffer[AERON_COMMAND_PUBLICATION_ERROR_MAX_LENGTH];
        aeron_command_publication_error_t *error = (aeron_command_publication_error_t *)buffer;
        int32_t error_length = reason_length <= AERON_ERROR_MAX_TEXT_LENGTH ? reason_length : AERON_ERROR_MAX_TEXT_LENGTH;

        struct sockaddr_storage src_addr_storage;
        memset(&src_addr_storage, 0, sizeof(struct sockaddr_storage));

        struct sockaddr_in *src_addr_in = (struct sockaddr_in *)&src_addr_storage;
        src_addr_in->sin_family = AF_INET;
        src_addr_in->sin_port = 0;
        src_addr_in->sin_addr.s_addr = INADDR_LOOPBACK;

        error->registration_id = publication->conductor_fields.managed_resource.registration_id;
        error->destination_registration_id = AERON_NULL_VALUE;
        error->session_id = publication->session_id;
        error->stream_id = publication->stream_id;
        error->receiver_id = AERON_NULL_VALUE;
        error->group_tag =  AERON_NULL_VALUE;
        memcpy(&error->src_address, &src_addr_storage, sizeof(struct sockaddr_storage));
        error->error_code = AERON_ERROR_CODE_IMAGE_REJECTED;
        error->error_length = error_length;
        memcpy(error->error_text, reason, error_length);
        aeron_str_null_terminate(error->error_text, error_length);

        aeron_driver_conductor_on_publication_error(conductor, error);
    }

    if (!publication->in_cool_down)
    {
        AERON_SET_RELEASE(publication->log_meta_data->is_connected, 0);

        aeron_driver_conductor_unlink_ipc_subscriptions(conductor, publication);

        {
            aeron_subscribable_t *subscribable = &publication->conductor_fields.subscribable;

            for (size_t i = 0, length = subscribable->length; i < length; i++)
            {
                aeron_counters_manager_free(&conductor->counters_manager, subscribable->array[i].counter_id);
            }

            aeron_free(subscribable->array);
            subscribable->array = NULL;
            subscribable->length = 0;
            subscribable->capacity = 0;
        }

        publication->in_cool_down = true;
    }

    publication->cool_down_expire_time_ns = now_ns + publication->liveness_timeout_ns;
}

int aeron_ipc_publication_update_pub_pos_and_lmt(aeron_ipc_publication_t *publication)
{
    int work_count = 0;

    if (AERON_IPC_PUBLICATION_STATE_ACTIVE == publication->conductor_fields.state)
    {
        const int64_t producer_position = aeron_ipc_publication_producer_position(publication);
        int64_t consumer_position = publication->conductor_fields.consumer_position;

        aeron_counter_set_release(publication->pub_pos_position.value_addr, producer_position);

        if (aeron_driver_subscribable_has_working_positions(&publication->conductor_fields.subscribable))
        {
            int64_t min_sub_pos = INT64_MAX;
            int64_t max_sub_pos = consumer_position;

            for (size_t i = 0, length = publication->conductor_fields.subscribable.length; i < length; i++)
            {
                aeron_tetherable_position_t *tetherable_position = &publication->conductor_fields.subscribable.array[i];

                if (aeron_driver_subscribable_is_active_state(tetherable_position->state))
                {
                    int64_t position = aeron_counter_get_acquire(tetherable_position->value_addr);

                    min_sub_pos = position < min_sub_pos ? position : min_sub_pos;
                    max_sub_pos = position > max_sub_pos ? position : max_sub_pos;
                }
            }

            int64_t new_limit_position = min_sub_pos + publication->term_window_length;
            if (new_limit_position > publication->conductor_fields.trip_limit)
            {
                aeron_ipc_publication_clean_buffer(publication, min_sub_pos);
                aeron_counter_set_release(publication->pub_lmt_position.value_addr, new_limit_position);
                publication->conductor_fields.trip_limit = new_limit_position + publication->trip_gain;
                work_count++;
            }

            publication->conductor_fields.consumer_position = max_sub_pos;
        }
        else if (*publication->pub_lmt_position.value_addr > consumer_position)
        {
            aeron_counter_set_release(publication->pub_lmt_position.value_addr, consumer_position);
            publication->conductor_fields.trip_limit = consumer_position;
            aeron_ipc_publication_clean_buffer(publication, consumer_position);
            work_count++;
        }
    }

    return work_count;
}

void aeron_ipc_publication_clean_buffer(aeron_ipc_publication_t *publication, int64_t position)
{
    int64_t clean_position = publication->conductor_fields.clean_position;
    if (position > clean_position)
    {
        size_t dirty_index = aeron_logbuffer_index_by_position(clean_position, publication->position_bits_to_shift);
        size_t bytes_to_clean = (size_t)(position - clean_position);
        size_t term_length = publication->mapped_raw_log.term_length;
        size_t term_offset = (size_t)(clean_position & (term_length - 1));
        size_t bytes_left_in_term = term_length - term_offset;
        size_t length = bytes_to_clean < bytes_left_in_term ? bytes_to_clean : bytes_left_in_term;

        memset(
            publication->mapped_raw_log.term_buffers[dirty_index].addr + term_offset + sizeof(int64_t),
            0,
            length - sizeof(int64_t));

        volatile uint64_t *ptr = (volatile uint64_t *)(publication->mapped_raw_log.term_buffers[dirty_index].addr + term_offset);
        AERON_SET_RELEASE(*ptr, (uint64_t)0);

        publication->conductor_fields.clean_position = (int64_t)(clean_position + length);
    }
}

void aeron_ipc_publication_check_untethered_subscriptions(
    aeron_driver_conductor_t *conductor, aeron_ipc_publication_t *publication, int64_t now_ns)
{
    int64_t consumer_position = publication->conductor_fields.consumer_position;
    int64_t term_window_length = publication->term_window_length;
    int64_t untethered_window_limit = (consumer_position - term_window_length) + (term_window_length / 4);

    aeron_subscribable_t *subscribable = &publication->conductor_fields.subscribable;
    for (size_t i = 0, length = subscribable->length; i < length; i++)
    {
        aeron_tetherable_position_t *tetherable_position = &subscribable->array[i];

        if (tetherable_position->is_tether)
        {
            tetherable_position->time_of_last_update_ns = now_ns;
        }
        else
        {
            switch (tetherable_position->state)
            {
                case AERON_SUBSCRIPTION_TETHER_ACTIVE:
                    if (aeron_counter_get_acquire(tetherable_position->value_addr) > untethered_window_limit)
                    {
                        tetherable_position->time_of_last_update_ns = now_ns;
                    }
                    else if (now_ns > (tetherable_position->time_of_last_update_ns + publication->untethered_window_limit_timeout_ns))
                    {
                        aeron_driver_conductor_on_unavailable_image(
                            conductor,
                            publication->conductor_fields.managed_resource.registration_id,
                            tetherable_position->subscription_registration_id,
                            publication->stream_id,
                            AERON_IPC_CHANNEL,
                            AERON_IPC_CHANNEL_LEN);

                        aeron_driver_subscribable_state(
                            subscribable,
                            tetherable_position,
                            AERON_SUBSCRIPTION_TETHER_LINGER,
                            now_ns,
                            publication->stream_id,
                            publication->session_id,
                            publication->log.untethered_subscription_state_change);
                    }
                    break;

                case AERON_SUBSCRIPTION_TETHER_LINGER:
                    if (now_ns > (tetherable_position->time_of_last_update_ns + publication->untethered_linger_timeout_ns))
                    {
                        if (tetherable_position->is_rejoin)
                        {
                            aeron_driver_subscribable_state(
                                subscribable,
                                tetherable_position,
                                AERON_SUBSCRIPTION_TETHER_RESTING,
                                now_ns,
                                publication->stream_id,
                                publication->session_id,
                                publication->log.untethered_subscription_state_change);
                        }
                        else
                        {
                            aeron_driver_subscribable_state(
                                subscribable,
                                tetherable_position,
                                AERON_SUBSCRIPTION_TETHER_CLOSED,
                                now_ns,
                                publication->stream_id,
                                publication->session_id,
                                publication->log.untethered_subscription_state_change);

                            if (0 == aeron_counters_manager_free(&conductor->counters_manager, tetherable_position->counter_id))
                            {
                                tetherable_position->counter_id = AERON_NULL_COUNTER_ID; // prevent double free
                            }
                        }
                    }
                    break;

                case AERON_SUBSCRIPTION_TETHER_RESTING:
                    if (now_ns > (tetherable_position->time_of_last_update_ns + publication->untethered_resting_timeout_ns))
                    {
                        int64_t join_position = aeron_ipc_publication_join_position(publication);
                        aeron_counter_set_release(tetherable_position->value_addr, join_position);
                        aeron_driver_conductor_on_available_image(
                            conductor,
                            publication->conductor_fields.managed_resource.registration_id,
                            publication->stream_id,
                            publication->session_id,
                            publication->log_file_name,
                            publication->log_file_name_length,
                            tetherable_position->counter_id,
                            tetherable_position->subscription_registration_id,
                            AERON_IPC_CHANNEL,
                            AERON_IPC_CHANNEL_LEN);

                        aeron_driver_subscribable_state(
                            subscribable,
                            tetherable_position,
                            AERON_SUBSCRIPTION_TETHER_ACTIVE,
                            now_ns,
                            publication->stream_id,
                            publication->session_id,
                            publication->log.untethered_subscription_state_change);
                    }
                    break;

                case AERON_SUBSCRIPTION_TETHER_CLOSED:
                    break;
            }
        }
    }
}

void aeron_ipc_publication_check_cooldown_status(
    aeron_driver_conductor_t *conductor, aeron_ipc_publication_t *publication, int64_t now_ns)
{
    if (publication->in_cool_down && publication->cool_down_expire_time_ns < now_ns)
    {
        publication->in_cool_down = false;

        aeron_driver_conductor_link_ipc_subscriptions(conductor, publication);

        publication->cool_down_expire_time_ns = 0;
    }
}

void aeron_ipc_publication_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_ipc_publication_t *publication, int64_t now_ns, int64_t now_ms)
{
    switch (publication->conductor_fields.state)
    {
        case AERON_IPC_PUBLICATION_STATE_ACTIVE:
        {
            uint8_t is_publication_revoked;

            AERON_GET_ACQUIRE(is_publication_revoked, publication->log_meta_data->is_publication_revoked);

            if (is_publication_revoked)
            {
                int64_t revoked_position = aeron_ipc_publication_producer_position(publication);
                aeron_counter_set_release(publication->pub_lmt_position.value_addr, revoked_position);
                AERON_SET_RELEASE(publication->log_meta_data->end_of_stream_position, revoked_position);
                AERON_SET_RELEASE(publication->log_meta_data->is_connected, 0);

                for (size_t i = 0, size = conductor->ipc_subscriptions.length; i < size; i++)
                {
                    aeron_subscription_link_t *link = &conductor->ipc_subscriptions.array[i];

                    if (aeron_driver_conductor_is_subscribable_linked(
                        link,
                        &publication->conductor_fields.subscribable))
                    {
                        aeron_driver_conductor_on_unavailable_image(
                            conductor,
                            publication->conductor_fields.managed_resource.registration_id,
                            link->registration_id,
                            publication->stream_id,
                            AERON_IPC_CHANNEL,
                            AERON_IPC_CHANNEL_LEN);
                    }
                }

                publication->conductor_fields.state = AERON_IPC_PUBLICATION_STATE_LINGER;

                aeron_driver_publication_revoke_func_t publication_revoke = publication->log.publication_revoke;
                if (NULL != publication_revoke)
                {
                    publication_revoke(
                        revoked_position,
                        publication->session_id,
                        publication->stream_id,
                        publication->channel_length,
                        publication->channel);
                }

                aeron_counter_increment_release(publication->publications_revoked_counter);
            }
            else
            {
                aeron_ipc_publication_check_untethered_subscriptions(conductor, publication, now_ns);
                AERON_SET_RELEASE(
                    publication->log_meta_data->is_connected,
                    aeron_driver_subscribable_working_position_count(&publication->conductor_fields.subscribable) > 0);
                const int64_t producer_position = aeron_ipc_publication_producer_position(publication);
                aeron_counter_set_release(publication->pub_pos_position.value_addr, producer_position);

                if (!publication->is_exclusive)
                {
                    aeron_ipc_publication_check_for_blocked_publisher(publication, producer_position, now_ns);
                }

                aeron_ipc_publication_check_cooldown_status(conductor, publication, now_ns);
            }
            break;
        }

        case AERON_IPC_PUBLICATION_STATE_DRAINING:
        {
            const int64_t producer_position = aeron_ipc_publication_producer_position(publication);
            aeron_counter_set_release(publication->pub_pos_position.value_addr, producer_position);

            if (aeron_ipc_publication_is_drained(publication))
            {
                publication->conductor_fields.state = AERON_IPC_PUBLICATION_STATE_LINGER;
                publication->conductor_fields.managed_resource.time_of_last_state_change_ns = now_ns;

                for (size_t i = 0, size = conductor->ipc_subscriptions.length; i < size; i++)
                {
                    aeron_subscription_link_t *link = &conductor->ipc_subscriptions.array[i];

                    if (aeron_driver_conductor_is_subscribable_linked(
                        link,
                        &publication->conductor_fields.subscribable))
                    {
                        aeron_driver_conductor_on_unavailable_image(
                            conductor,
                            publication->conductor_fields.managed_resource.registration_id,
                            link->registration_id,
                            publication->stream_id,
                            AERON_IPC_CHANNEL,
                            AERON_IPC_CHANNEL_LEN);
                    }
                }
            }
            else if (aeron_logbuffer_unblocker_unblock(
                publication->mapped_raw_log.term_buffers,
                publication->log_meta_data,
                publication->conductor_fields.consumer_position))
            {
                aeron_counter_increment_release(publication->unblocked_publications_counter);
            }
            break;
        }

        case AERON_IPC_PUBLICATION_STATE_LINGER:
        {
            if (publication->conductor_fields.refcnt <= 0)
            {
                publication->conductor_fields.has_reached_end_of_life = true;
            }
            break;
        }

        case AERON_IPC_PUBLICATION_STATE_DONE:
            break;
    }
}

void aeron_ipc_publication_handle_managed_resource_event(aeron_driver_managed_resource_event_t event, void *clientd)
{
    aeron_ipc_publication_t *publication = (aeron_ipc_publication_t *)clientd;

    switch(event)
    {
        case AERON_DRIVER_MANAGED_RESOURCE_EVENT_INCREF:
        {
            publication->conductor_fields.refcnt++;
            break;
        }

        case AERON_DRIVER_MANAGED_RESOURCE_EVENT_DECREF:
        {
            int32_t ref_count = --publication->conductor_fields.refcnt;

            if (0 == ref_count)
            {
                int64_t producer_position = aeron_ipc_publication_producer_position(publication);

                if (aeron_counter_get_plain(publication->pub_lmt_position.value_addr) > producer_position)
                {
                    aeron_counter_set_release(publication->pub_lmt_position.value_addr, producer_position);
                }

                AERON_SET_RELEASE(publication->log_meta_data->end_of_stream_position, producer_position);

                uint8_t is_publication_revoked;

                AERON_GET_ACQUIRE(is_publication_revoked, publication->log_meta_data->is_publication_revoked);

                if (!is_publication_revoked)
                {
                    publication->conductor_fields.state = AERON_IPC_PUBLICATION_STATE_DRAINING;
                }
            }
            break;
        }

        case AERON_DRIVER_MANAGED_RESOURCE_EVENT_REVOKE:
        {
            AERON_SET_RELEASE(publication->log_meta_data->is_publication_revoked, true);
            break;
        }
    }
}

void aeron_ipc_publication_check_for_blocked_publisher(
    aeron_ipc_publication_t *publication, int64_t producer_position, int64_t now_ns)
{
    int64_t consumer_position = publication->conductor_fields.consumer_position;

    if (consumer_position == publication->conductor_fields.last_consumer_position &&
        aeron_ipc_publication_is_possibly_blocked(publication, producer_position, consumer_position))
    {
        if (now_ns >
            (publication->conductor_fields.time_of_last_consumer_position_change_ns + publication->unblock_timeout_ns))
        {
            if (aeron_logbuffer_unblocker_unblock(
                publication->mapped_raw_log.term_buffers,
                publication->log_meta_data,
                publication->conductor_fields.consumer_position))
            {
                aeron_counter_increment_release(publication->unblocked_publications_counter);
            }
        }
    }
    else
    {
        publication->conductor_fields.time_of_last_consumer_position_change_ns = now_ns;
        publication->conductor_fields.last_consumer_position = publication->conductor_fields.consumer_position;
    }
}

extern void aeron_ipc_publication_add_subscriber_hook(void *clientd, volatile int64_t *value_addr);

extern void aeron_ipc_publication_remove_subscriber_hook(void *clientd, volatile int64_t *value_addr);

extern bool aeron_ipc_publication_is_possibly_blocked(
    aeron_ipc_publication_t *publication, int64_t producer_position, int64_t consumer_position);

extern int64_t aeron_ipc_publication_producer_position(aeron_ipc_publication_t *publication);

extern int64_t aeron_ipc_publication_join_position(aeron_ipc_publication_t *publication);

extern bool aeron_ipc_publication_has_reached_end_of_life(aeron_ipc_publication_t *publication);

extern bool aeron_ipc_publication_is_drained(aeron_ipc_publication_t *publication);

extern bool aeron_ipc_publication_is_accepting_subscriptions(aeron_ipc_publication_t *publication);
