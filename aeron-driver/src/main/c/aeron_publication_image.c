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

#include <inttypes.h>
#include "util/aeron_netutil.h"
#include "util/aeron_arrayutil.h"
#include "concurrent/aeron_term_rebuilder.h"
#include "aeron_publication_image.h"
#include "aeron_driver_receiver_proxy.h"
#include "aeron_driver_conductor.h"
#include "concurrent/aeron_term_gap_filler.h"
#include "util/aeron_parse_util.h"

#define AERON_PUBLICATION_RESPONSE_NULL_RESPONSE_SESSION_ID INT64_C(0xF000000000000000)

static void aeron_publication_image_connection_set_control_address(
    aeron_publication_image_connection_t *connection, const struct sockaddr_storage *control_address)
{
    memcpy(
        &connection->resolved_control_address_for_implicit_unicast_channels,
        control_address,
        sizeof(connection->resolved_control_address_for_implicit_unicast_channels));
    connection->control_addr = &connection->resolved_control_address_for_implicit_unicast_channels;
}

static void aeron_update_active_transport_count(aeron_publication_image_t *image, int64_t now_ns)
{
    int active_transport_count = 0;

    for (size_t i = 0, len = image->connections.length; i < len; i++)
    {
        aeron_publication_image_connection_t *connection = &image->connections.array[i];
        if (now_ns < connection->time_of_last_frame_ns + image->conductor_fields.liveness_timeout_ns)
        {
            active_transport_count++;
        }
    }

    if (active_transport_count != image->log_meta_data->active_transport_count)
    {
        AERON_SET_RELEASE(image->log_meta_data->active_transport_count, active_transport_count);
    }
}

static bool aeron_publication_image_check_and_get_response_session_id(
    aeron_publication_image_t *image, int32_t *response_session_id)
{
    int64_t _response_session_id;
    AERON_GET_ACQUIRE(_response_session_id, image->response_session_id);
    *response_session_id = (int32_t)_response_session_id;
    return ((int64_t)INT32_MIN) <= _response_session_id && _response_session_id <= ((int64_t)INT32_MAX);
}

static aeron_feedback_delay_generator_state_t *aeron_publication_image_acquire_delay_generator_state(
    bool treat_as_multicast,
    aeron_driver_context_t *context,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_publication_image_t *image)
{
    if (treat_as_multicast)
    {
        return &context->multicast_delay_feedback_generator;
    }

    const char *nak_delay_string = aeron_uri_find_param_value(
        &endpoint->conductor_fields.udp_channel->uri.params.udp.additional_params,
        AERON_URI_NAK_DELAY_KEY);

    if (NULL == nak_delay_string)
    {
        return &context->unicast_delay_feedback_generator;
    }

    uint64_t nak_delay_ns;

    if (aeron_parse_duration_ns(nak_delay_string, &nak_delay_ns) < 0)
    {
        AERON_SET_ERR(EINVAL, "%s is not parseable: %s", AERON_URI_NAK_DELAY_KEY, nak_delay_string);
        return NULL;
    }

    if (aeron_feedback_delay_state_init(
         &image->feedback_delay_state,
        aeron_loss_detector_nak_unicast_delay_generator,
        (int64_t)nak_delay_ns,
        (int64_t)nak_delay_ns * (int64_t)context->nak_unicast_retry_delay_ratio,
        1) < 0)
    {
        AERON_APPEND_ERR("%s", "Could not init publication image feedback_delay_state");
        return NULL;
    }

    return &image->feedback_delay_state;
}

static void aeron_publication_image_report_loss(
    aeron_publication_image_t *image, int32_t term_id, int32_t term_offset, size_t length, size_t bytes_lost)
{
    if (image->conductor_fields.loss_report_entry_offset >= 0)
    {
        aeron_loss_reporter_record_observation(
            image->conductor_fields.loss_report, image->conductor_fields.loss_report_entry_offset, (int64_t)bytes_lost, image->epoch_clock());
    }
    else if (NULL != image->conductor_fields.loss_report)
    {
        if (NULL != image->conductor_fields.endpoint)
        {
            image->conductor_fields.loss_report_entry_offset = aeron_loss_reporter_create_entry(
                image->conductor_fields.loss_report,
                (int64_t)bytes_lost,
                image->epoch_clock(),
                image->session_id,
                image->stream_id,
                image->conductor_fields.endpoint->conductor_fields.udp_channel->original_uri,
                image->conductor_fields.endpoint->conductor_fields.udp_channel->uri_length,
                image->source_identity,
                image->source_identity_length);
        }

        if (-1 == image->conductor_fields.loss_report_entry_offset)
        {
            image->conductor_fields.loss_report = NULL;
        }
    }
    image->conductor_fields.loss_report_term_id = term_id;
    image->conductor_fields.loss_report_term_offset = term_offset;
    image->conductor_fields.loss_report_length = length;
}

int aeron_publication_image_create(
    aeron_publication_image_t **image,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    aeron_driver_conductor_t *conductor,
    int64_t correlation_id,
    int32_t session_id,
    int32_t stream_id,
    int32_t initial_term_id,
    int32_t active_term_id,
    int32_t initial_term_offset,
    aeron_position_t *rcv_hwm_position,
    aeron_position_t *rcv_pos_position,
    aeron_congestion_control_strategy_t *congestion_control,
    struct sockaddr_storage *control_address,
    struct sockaddr_storage *source_address,
    int32_t term_buffer_length,
    int32_t sender_mtu_length,
    uint8_t flags,
    aeron_loss_reporter_t *loss_reporter,
    bool is_reliable,
    bool is_sparse,
    bool treat_as_multicast,
    aeron_system_counters_t *system_counters)
{
    aeron_driver_context_t *context = conductor->context;

    aeron_publication_image_t *_image = NULL;
    const uint64_t log_length = aeron_logbuffer_compute_log_length(
        (uint64_t)term_buffer_length, context->file_page_size);

    *image = NULL;

    if (aeron_driver_context_run_storage_checks(context, log_length) < 0)
    {
        return -1;
    }

    if (aeron_alloc((void **)&_image, sizeof(aeron_publication_image_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Could not allocate publication image");
        return -1;
    }

    char path[AERON_MAX_PATH];
    int path_length = aeron_publication_image_location(path, sizeof(path), context->aeron_dir, correlation_id);
    if (path_length < 0)
    {
        AERON_APPEND_ERR("%s", "Could not resolve publication image file path");
        aeron_free(_image);
        return -1;
    }

    _image->log_file_name = NULL;
    if (aeron_alloc((void **)(&_image->log_file_name), (size_t)path_length + 1) < 0)
    {
        AERON_APPEND_ERR("%s", "Could not allocate publication image log_file_name");
        aeron_free(_image);
        return -1;
    }

    aeron_feedback_delay_generator_state_t *feedback_delay_state;
    feedback_delay_state = aeron_publication_image_acquire_delay_generator_state(treat_as_multicast, context, endpoint, _image);
    if (NULL == feedback_delay_state)
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    if (aeron_loss_detector_init(
        &_image->loss_detector,
        feedback_delay_state,
        aeron_publication_image_on_gap_detected,
        _image) < 0)
    {
        AERON_APPEND_ERR("%s", "Could not init publication image loss detector");
        goto error;
    }

    if (context->raw_log_map_func(
        &_image->mapped_raw_log, path, is_sparse, (uint64_t)term_buffer_length, context->file_page_size) < 0)
    {
        AERON_APPEND_ERR("error mapping network raw log: %s", path);
        goto error;
    }

    _image->mapped_bytes_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_BYTES_CURRENTLY_MAPPED);
    aeron_counter_add_ordered(_image->mapped_bytes_counter, (int64_t)log_length);

    _image->raw_log_close_func = context->raw_log_close_func;
    _image->raw_log_free_func = context->raw_log_free_func;
    _image->log.untethered_subscription_state_change = context->log.untethered_subscription_on_state_change;

    _image->nano_clock = context->nano_clock;
    _image->epoch_clock = context->epoch_clock;
    _image->cached_clock = context->receiver_cached_clock;

    if (aeron_publication_image_add_destination(_image, destination) < 0)
    {
        goto error;
    }
    if (!destination->has_control_addr)
    {
        aeron_publication_image_connection_set_control_address(&_image->connections.array[0], control_address);
    }

    strncpy(_image->log_file_name, path, (size_t)path_length);
    _image->log_file_name[path_length] = '\0';
    _image->log_file_name_length = (size_t)path_length;
    _image->log_meta_data = (aeron_logbuffer_metadata_t *)(_image->mapped_raw_log.log_meta_data.addr);

    aeron_driver_uri_subscription_params_t params;
    if (aeron_driver_uri_subscription_params(&endpoint->conductor_fields.udp_channel->uri, &params, conductor) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    aeron_logbuffer_metadata_init(
        _image->mapped_raw_log.log_meta_data.addr,
        INT64_MAX,
        0,
        0,
        correlation_id,
        initial_term_id,
        sender_mtu_length,
        term_buffer_length,
        (int32_t)context->file_page_size,
        0,
        (int32_t)params.initial_window_length,
        (int32_t)endpoint->conductor_fields.udp_channel->socket_sndbuf_length,
        (int32_t)context->os_buffer_lengths.default_so_sndbuf,
        (int32_t)context->os_buffer_lengths.max_so_sndbuf,
        (int32_t)endpoint->conductor_fields.udp_channel->socket_rcvbuf_length,
        (int32_t)context->os_buffer_lengths.default_so_rcvbuf,
        (int32_t)context->os_buffer_lengths.max_so_rcvbuf,
        0,
        session_id,
        stream_id,
        0,
        0,
        0,
        0,
        0,
        (uint8_t)treat_as_multicast,
        (uint8_t)params.is_response,
        (uint8_t)params.is_rejoin,
        (uint8_t)params.is_reliable,
        (uint8_t)params.is_sparse,
        (uint8_t)false,
        (uint8_t)false,
        (uint8_t)params.is_tether);

    _image->endpoint = endpoint;
    _image->conductor_fields.endpoint = endpoint;

    _image->congestion_control = congestion_control;
    _image->conductor_fields.loss_report = loss_reporter;
    _image->conductor_fields.loss_report_entry_offset = -1;
    _image->conductor_fields.loss_report_term_id = 0;
    _image->conductor_fields.loss_report_term_offset = 0;
    _image->conductor_fields.loss_report_length = 0;
    _image->conductor_fields.subscribable.correlation_id = correlation_id;
    _image->conductor_fields.subscribable.array = NULL;
    _image->conductor_fields.subscribable.length = 0;
    _image->conductor_fields.subscribable.capacity = 0;
    _image->conductor_fields.subscribable.add_position_hook_func = aeron_driver_subscribable_null_hook;
    _image->conductor_fields.subscribable.remove_position_hook_func = aeron_driver_subscribable_null_hook;
    _image->conductor_fields.subscribable.clientd = NULL;
    _image->conductor_fields.managed_resource.registration_id = correlation_id;
    _image->conductor_fields.managed_resource.clientd = _image;
    _image->conductor_fields.managed_resource.incref = NULL;
    _image->conductor_fields.managed_resource.decref = NULL;
    _image->conductor_fields.is_reliable = is_reliable;
    _image->conductor_fields.state = AERON_PUBLICATION_IMAGE_STATE_ACTIVE;
    _image->conductor_fields.liveness_timeout_ns = (int64_t)context->image_liveness_timeout_ns;
    _image->conductor_fields.flags = flags;

    uint64_t untethered_window_limit_timeout_ns = context->untethered_window_limit_timeout_ns;
    aeron_uri_get_timeout(
        &endpoint->conductor_fields.udp_channel->uri.params.udp.additional_params,
        AERON_URI_UNTETHERED_WINDOW_LIMIT_TIMEOUT_KEY,
        &untethered_window_limit_timeout_ns);
    _image->conductor_fields.untethered_window_limit_timeout_ns = (int64_t)untethered_window_limit_timeout_ns;

    uint64_t untethered_resting_timeout_ns = context->untethered_resting_timeout_ns;
    aeron_uri_get_timeout(
        &endpoint->conductor_fields.udp_channel->uri.params.udp.additional_params,
        AERON_URI_UNTETHERED_RESTING_TIMEOUT_KEY,
        &untethered_resting_timeout_ns);
    _image->conductor_fields.untethered_resting_timeout_ns = (int64_t)untethered_resting_timeout_ns;

    _image->session_id = session_id;
    _image->stream_id = stream_id;
    _image->rcv_hwm_position.counter_id = rcv_hwm_position->counter_id;
    _image->rcv_hwm_position.value_addr = rcv_hwm_position->value_addr;
    _image->rcv_pos_position.counter_id = rcv_pos_position->counter_id;
    _image->rcv_pos_position.value_addr = rcv_pos_position->value_addr;
    _image->term_length = term_buffer_length;
    _image->initial_term_id = initial_term_id;
    _image->term_length_mask = term_buffer_length - 1;
    _image->position_bits_to_shift = (size_t)aeron_number_of_trailing_zeroes((int32_t)term_buffer_length);
    _image->mtu_length = sender_mtu_length;
    _image->last_sm_change_number = -1;
    _image->last_loss_change_number = -1;
    _image->is_end_of_stream = false;
    _image->is_sending_eos_sm = false;
    _image->has_receiver_released = false;
    _image->sm_timeout_ns = (int64_t)context->status_message_timeout_ns;
    _image->invalidation_reason = NULL;

    memcpy(&_image->source_address, source_address, sizeof(_image->source_address));
    const int source_identity_length = aeron_format_source_identity(
        _image->source_identity, sizeof(_image->source_identity), source_address);
    if (source_identity_length <= 0)
    {
        AERON_APPEND_ERR("%s", "failed to format source identity");
        goto error;
    }
    _image->source_identity_length = (size_t)source_identity_length;

    _image->heartbeats_received_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_HEARTBEATS_RECEIVED);
    _image->flow_control_under_runs_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_FLOW_CONTROL_UNDER_RUNS);
    _image->flow_control_over_runs_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_FLOW_CONTROL_OVER_RUNS);
    _image->status_messages_sent_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_STATUS_MESSAGES_SENT);
    _image->nak_messages_sent_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_NAK_MESSAGES_SENT);
    _image->loss_gap_fills_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_LOSS_GAP_FILLS);

    const int64_t initial_position = aeron_logbuffer_compute_position(
        active_term_id, initial_term_offset, _image->position_bits_to_shift, initial_term_id);
    const int64_t now_ns = aeron_clock_cached_nano_time(context->cached_clock);

    _image->begin_loss_change = 0;
    _image->end_loss_change = 0;
    _image->loss_term_id = active_term_id;
    _image->loss_term_offset = initial_term_offset;
    _image->loss_length = 0;

    _image->begin_sm_change = 0;
    _image->end_sm_change = 0;
    _image->next_sm_position = initial_position;
    _image->next_sm_receiver_window_length = _image->congestion_control->initial_window_length(
        _image->congestion_control->state);
    _image->max_receiver_window_length = _image->congestion_control->max_window_length(
        _image->congestion_control->state);
    _image->last_sm_position = initial_position;
    _image->last_overrun_threshold = initial_position + (term_buffer_length / 2);
    _image->time_of_last_packet_ns = now_ns;
    _image->next_sm_deadline_ns = 0;
    _image->is_sm_enabled = true;
    _image->conductor_fields.clean_position = initial_position;
    _image->conductor_fields.time_of_last_state_change_ns = now_ns;

    aeron_publication_image_remove_response_session_id(_image);
    aeron_counter_set_ordered(_image->rcv_hwm_position.value_addr, initial_position);
    aeron_counter_set_ordered(_image->rcv_pos_position.value_addr, initial_position);

    *image = _image;

    return 0;

error:
    aeron_free(_image->connections.array);
    aeron_free(_image->log_file_name);
    aeron_free(_image);
    return -1;
}

int aeron_publication_image_close(aeron_counters_manager_t *counters_manager, aeron_publication_image_t *image)
{
    if (NULL != image)
    {
        aeron_subscribable_t *subscribable = &image->conductor_fields.subscribable;

        aeron_counters_manager_free(counters_manager, image->rcv_hwm_position.counter_id);
        aeron_counters_manager_free(counters_manager, image->rcv_pos_position.counter_id);

        for (size_t i = 0, length = subscribable->length; i < length; i++)
        {
            aeron_counters_manager_free(counters_manager, subscribable->array[i].counter_id);
        }

        aeron_free(subscribable->array);
        aeron_free(image->connections.array);

        image->congestion_control->fini(image->congestion_control);
    }

    return 0;
}

bool aeron_publication_image_free(aeron_publication_image_t *image)
{
    if (NULL == image)
    {
        return true;
    }

    if (!image->raw_log_free_func(&image->mapped_raw_log, image->log_file_name))
    {
        return false;
    }

    aeron_counter_add_ordered(image->mapped_bytes_counter, -((int64_t)image->mapped_raw_log.mapped_file.length));

    aeron_free(image->log_file_name);
    aeron_free((void *)image->invalidation_reason);
    aeron_free(image);

    return true;
}

void aeron_publication_image_clean_buffer_to(aeron_publication_image_t *image, int64_t position)
{
    int64_t clean_position = image->conductor_fields.clean_position;
    if (position > clean_position)
    {
        size_t dirty_index = aeron_logbuffer_index_by_position(clean_position, image->position_bits_to_shift);
        size_t bytes_to_clean = (size_t)(position - clean_position);
        size_t term_length = (size_t)image->term_length;
        size_t term_offset = (size_t)(clean_position & image->term_length_mask);
        size_t bytes_left_in_term = term_length - term_offset;
        size_t length = bytes_to_clean < bytes_left_in_term ? bytes_to_clean : bytes_left_in_term;

        memset(
            image->mapped_raw_log.term_buffers[dirty_index].addr + term_offset + sizeof(int64_t),
            0,
            length - sizeof(int64_t));

        uint64_t *ptr = (uint64_t *)(image->mapped_raw_log.term_buffers[dirty_index].addr + term_offset);
        AERON_SET_RELEASE(*ptr, (uint64_t)0);

        image->conductor_fields.clean_position = clean_position + (int64_t)length;
    }
}

// Called from conductor via loss detector.
void aeron_publication_image_on_gap_detected(void *clientd, int32_t term_id, int32_t term_offset, size_t length)
{
    aeron_publication_image_t *image = (aeron_publication_image_t *)clientd;
    const int64_t change_number = image->begin_loss_change + 1;

    AERON_SET_RELEASE(image->begin_loss_change, change_number);
    aeron_release();

    image->loss_term_id = term_id;
    image->loss_term_offset = term_offset;
    image->loss_length = length;

    AERON_SET_RELEASE(image->end_loss_change, change_number);

    size_t loss_report_end_offset;
    if (term_id != image->conductor_fields.loss_report_term_id ||
        (size_t)term_offset >= (loss_report_end_offset = (size_t)image->conductor_fields.loss_report_term_offset + image->conductor_fields.loss_report_length))
    {
        aeron_publication_image_report_loss(image, term_id, term_offset, length, length);
    }
    else if ((size_t)term_offset + length > loss_report_end_offset)
    {
        aeron_publication_image_report_loss(image, term_id, term_offset, length, length - (loss_report_end_offset - (size_t)term_offset));
    }
}

void aeron_publication_image_track_rebuild(aeron_publication_image_t *image, int64_t now_ns)
{
    if (aeron_driver_subscribable_has_working_positions(&image->conductor_fields.subscribable))
    {
        const int64_t hwm_position = aeron_counter_get_volatile(image->rcv_hwm_position.value_addr);
        int64_t min_sub_pos = INT64_MAX;
        int64_t max_sub_pos = 0;

        for (size_t i = 0; i < image->conductor_fields.subscribable.length; i++)
        {
            aeron_tetherable_position_t *tetherable_position = &image->conductor_fields.subscribable.array[i];

            if (AERON_SUBSCRIPTION_TETHER_RESTING != tetherable_position->state)
            {
                const int64_t position = aeron_counter_get_volatile(tetherable_position->value_addr);

                min_sub_pos = position < min_sub_pos ? position : min_sub_pos;
                max_sub_pos = position > max_sub_pos ? position : max_sub_pos;
            }
        }

        if (INT64_MAX == min_sub_pos)
        {
            return;
        }

        const int64_t rebuild_position = *image->rcv_pos_position.value_addr > max_sub_pos ?
            *image->rcv_pos_position.value_addr : max_sub_pos;

        bool loss_found = false;
        const size_t index = aeron_logbuffer_index_by_position(rebuild_position, image->position_bits_to_shift);
        const int32_t rebuild_offset = aeron_loss_detector_scan(
            &image->loss_detector,
            &loss_found,
            image->mapped_raw_log.term_buffers[index].addr,
            rebuild_position,
            hwm_position,
            now_ns,
            (size_t)image->term_length_mask,
            image->position_bits_to_shift,
            image->initial_term_id);

        const int32_t rebuild_term_offset = (int32_t)(rebuild_position & image->term_length_mask);
        const int64_t new_rebuild_position = (rebuild_position - rebuild_term_offset) + rebuild_offset;

        aeron_counter_propose_max_ordered(image->rcv_pos_position.value_addr, new_rebuild_position);

        bool should_force_send_sm = false;
        const int32_t window_length = image->congestion_control->on_track_rebuild(
            image->congestion_control->state,
            &should_force_send_sm,
            now_ns,
            min_sub_pos,
            image->next_sm_position,
            hwm_position,
            rebuild_position,
            new_rebuild_position,
            loss_found);

        const int32_t threshold = window_length / 4;

        if (should_force_send_sm ||
            (min_sub_pos > (image->next_sm_position + threshold)) ||
            window_length != image->next_sm_receiver_window_length)
        {
            aeron_publication_image_clean_buffer_to(image, min_sub_pos - image->term_length);
            aeron_publication_image_schedule_status_message(image, min_sub_pos, window_length);
        }
    }
}

static inline void aeron_publication_image_track_connection(
    aeron_publication_image_t *image,
    aeron_receive_destination_t *destination,
    struct sockaddr_storage *source_addr,
    int64_t now_ns)
{
    aeron_publication_image_connection_t *connection = NULL;
    for (size_t i = 0, len = image->connections.length; i < len; i++)
    {
        if (image->connections.array[i].destination == destination)
        {
            connection = &image->connections.array[i];
            break;
        }
    }

    if (NULL == connection)
    {
        // TODO: Might be useful to prevent inlining
        if (aeron_publication_image_add_destination(image, destination) < 0)
        {
            return;
        }

        connection = &image->connections.array[image->connections.length - 1];
    }

    if (NULL == connection->control_addr)
    {
        // TODO: Might be useful to prevent inlining
        aeron_publication_image_connection_set_control_address(connection, source_addr);
    }

    connection->time_of_last_activity_ns = now_ns;
    connection->time_of_last_frame_ns = now_ns;
}

static inline bool aeron_publication_image_all_eos(
    aeron_publication_image_t *image, aeron_receive_destination_t *destination, int64_t packet_position)
{
    bool all_eos = true;

    for (size_t i = 0, len = image->connections.length; i < len; i++)
    {
        aeron_publication_image_connection_t *connection = &image->connections.array[i];
        if (connection->destination == destination)
        {
            connection->is_eos = true;
            connection->eos_position = packet_position;
        }
        else
        {
            all_eos &= connection->is_eos;
        }
    }

    return all_eos;
}

static inline int64_t aeron_publication_find_eos_position(aeron_publication_image_t *image)
{
    int64_t eos_position = 0;

    for (size_t i = 0, len = image->connections.length; i < len; i++)
    {
        aeron_publication_image_connection_t *connection = &image->connections.array[i];
        if (connection->eos_position > eos_position)
        {
            eos_position = connection->eos_position;
        }
    }

    return eos_position;
}

static inline bool aeron_publication_image_connection_is_alive(
    const aeron_publication_image_connection_t *connection, const int64_t now_ns)
{
    return NULL != connection->control_addr &&
        now_ns < connection->time_of_last_activity_ns + AERON_RECEIVE_DESTINATION_TIMEOUT_NS;
}

void aeron_publication_image_add_connection_if_unknown(
    aeron_publication_image_t *image, aeron_receive_destination_t *destination, struct sockaddr_storage *src_addr)
{
    aeron_publication_image_track_connection(
        image, destination, src_addr, aeron_clock_cached_nano_time(image->cached_clock));
}

int aeron_publication_image_insert_packet(
    aeron_publication_image_t *image,
    aeron_receive_destination_t *destination,
    int32_t term_id,
    int32_t term_offset,
    const uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
    if (aeron_sub_wrap_i32(term_id, image->initial_term_id) < 0)
    {
        return 0;
    }

    if (NULL != image->invalidation_reason)
    {
        return 0;
    }

    const bool is_heartbeat = aeron_publication_image_is_heartbeat(buffer, length);
    const int64_t packet_position = aeron_logbuffer_compute_position(
        term_id, term_offset, image->position_bits_to_shift, image->initial_term_id);
    const int64_t proposed_position = is_heartbeat ? packet_position : packet_position + (int64_t)length;

    if (!aeron_publication_image_is_flow_control_over_run(image, proposed_position))
    {
        const int64_t now_ns = aeron_clock_cached_nano_time(image->cached_clock);

        if (is_heartbeat)
        {
            const int64_t potential_window_bottom = image->last_sm_position - (image->term_length_mask + 1);
            const int64_t publication_window_bottom = potential_window_bottom < 0 ? 0 : potential_window_bottom;

            if (packet_position >= publication_window_bottom)
            {
                aeron_publication_image_track_connection(image, destination, addr, now_ns);
                AERON_SET_RELEASE(image->time_of_last_packet_ns, now_ns);

                const bool is_eos = aeron_publication_image_is_end_of_stream(buffer, length);
                if (is_eos && !image->is_end_of_stream)
                {
                    if (aeron_publication_image_all_eos(image, destination, packet_position))
                    {
                        const int64_t eos_position = aeron_publication_find_eos_position(image);
                        AERON_SET_RELEASE(image->log_meta_data->end_of_stream_position, eos_position);
                        AERON_SET_RELEASE(image->is_end_of_stream, true);
                    }
                }

                aeron_counter_propose_max_ordered(image->rcv_hwm_position.value_addr, proposed_position);
                aeron_counter_ordered_increment(image->heartbeats_received_counter, 1);
            }
            else
            {
                aeron_counter_ordered_increment(image->flow_control_under_runs_counter, 1);
            }
        }
        else if (!aeron_publication_image_is_flow_control_under_run(image, packet_position))
        {
            aeron_publication_image_track_connection(image, destination, addr, now_ns);
            AERON_SET_RELEASE(image->time_of_last_packet_ns, now_ns);

            const size_t index = aeron_logbuffer_index_by_position(packet_position, image->position_bits_to_shift);
            uint8_t *term_buffer = image->mapped_raw_log.term_buffers[index].addr;

            aeron_term_rebuilder_insert(term_buffer + term_offset, buffer, length);

            aeron_counter_propose_max_ordered(image->rcv_hwm_position.value_addr, proposed_position);
        }
        else if (proposed_position >= (image->last_sm_position - image->max_receiver_window_length))
        {
            aeron_publication_image_track_connection(image, destination, addr, now_ns);
        }
    }

    return (int)length;
}

int aeron_publication_image_on_rttm(
    aeron_publication_image_t *image, aeron_rttm_header_t *header, struct sockaddr_storage *addr)
{
    const int64_t now_ns = image->nano_clock();
    const int64_t rtt_in_ns = now_ns - header->echo_timestamp - header->reception_delta;

    image->congestion_control->on_rttm(image->congestion_control->state, now_ns, rtt_in_ns, addr);

    return 1;
}

// Called from receiver.
int aeron_publication_image_send_pending_status_message(aeron_publication_image_t *image, int64_t now_ns)
{
    int work_count = 0;
    int64_t change_number;
    AERON_GET_ACQUIRE(change_number, image->end_sm_change);
    const bool has_sm_timed_out = image->is_sm_enabled && image->next_sm_deadline_ns < now_ns;

    if (NULL != image->invalidation_reason)
    {
        if (has_sm_timed_out)
        {
            for (size_t i = 0, len = image->connections.length; i < len; i++)
            {
                aeron_publication_image_connection_t *connection = &image->connections.array[i];
                aeron_receiver_channel_endpoint_send_error_frame(
                    image->endpoint,
                    connection->destination,
                    connection->control_addr,
                    image->session_id,
                    image->stream_id,
                    AERON_ERROR_CODE_IMAGE_REJECTED,
                    image->invalidation_reason);
            }

            image->next_sm_deadline_ns = now_ns + image->sm_timeout_ns;
        }

        return 0;
    }

    int32_t response_session_id = 0;
    if (has_sm_timed_out && aeron_publication_image_check_and_get_response_session_id(image, &response_session_id))
    {
        for (size_t i = 0, len = image->connections.length; i < len; i++)
        {
            aeron_publication_image_connection_t *connection = &image->connections.array[i];

            if (aeron_publication_image_connection_is_alive(connection, now_ns))
            {
                int send_response_setup_result = aeron_receive_channel_endpoint_send_response_setup(
                    image->endpoint,
                    connection->destination,
                    connection->control_addr,
                    image->stream_id,
                    image->session_id,
                    response_session_id);

                if (send_response_setup_result < 0)
                {
                    work_count = send_response_setup_result;
                    break;
                }

                work_count++;
            }
        }
    }

    if (work_count < 0)
    {
        return work_count;
    }

    if (change_number != image->last_sm_change_number || has_sm_timed_out)
    {
        const int64_t sm_position = image->next_sm_position;
        const int32_t receiver_window_length = image->next_sm_receiver_window_length;

        aeron_acquire();

        int64_t begin_change_number;
        AERON_GET_ACQUIRE(begin_change_number, image->begin_sm_change);

        if (change_number == begin_change_number)
        {
            const int32_t term_id = aeron_logbuffer_compute_term_id_from_position(
                sm_position, image->position_bits_to_shift, image->initial_term_id);
            const int32_t term_offset = (int32_t)(sm_position & image->term_length_mask);
            bool is_sending_eos_sm;
            AERON_GET_ACQUIRE(is_sending_eos_sm, image->is_sending_eos_sm);
            const uint8_t flags = is_sending_eos_sm ? AERON_STATUS_MESSAGE_HEADER_EOS_FLAG : 0;

            for (size_t i = 0, len = image->connections.length; i < len; i++)
            {
                aeron_publication_image_connection_t *connection = &image->connections.array[i];

                if (aeron_publication_image_connection_is_alive(connection, now_ns))
                {
                    int send_sm_result = aeron_receive_channel_endpoint_send_sm(
                        image->endpoint,
                        connection->destination,
                        connection->control_addr,
                        image->stream_id,
                        image->session_id,
                        term_id,
                        term_offset,
                        receiver_window_length,
                        flags);

                    if (send_sm_result < 0)
                    {
                        AERON_APPEND_ERR("%s", "");
                        work_count = send_sm_result;
                        break;
                    }

                    work_count++;
                    aeron_counter_ordered_increment(image->status_messages_sent_counter, 1);
                }
            }

            image->last_sm_position = sm_position;
            image->last_overrun_threshold = sm_position + (image->term_length / 2);
            image->last_sm_change_number = change_number;
            image->next_sm_deadline_ns = now_ns + image->sm_timeout_ns;

            aeron_update_active_transport_count(image, now_ns);
        }
    }

    return work_count;
}

// Called from receiver.
int aeron_publication_image_send_pending_loss(aeron_publication_image_t *image)
{
    int work_count = 0;

    int64_t change_number;
    AERON_GET_ACQUIRE(change_number, image->end_loss_change);

    if (change_number != image->last_loss_change_number)
    {
        const int32_t term_id = image->loss_term_id;
        const int32_t term_offset = image->loss_term_offset;
        const size_t length = image->loss_length;

        aeron_acquire();

        int64_t begin_change_number;
        AERON_GET_ACQUIRE(begin_change_number, image->begin_loss_change);

        if (change_number == begin_change_number)
        {
            if (image->conductor_fields.is_reliable)
            {
                const int64_t now_ns = aeron_clock_cached_nano_time(image->cached_clock);

                for (size_t i = 0, len = image->connections.length; i < len; i++)
                {
                    aeron_publication_image_connection_t *connection = &image->connections.array[i];

                    if (aeron_publication_image_connection_is_alive(connection, now_ns))
                    {
                        int send_nak_result = aeron_receive_channel_endpoint_send_nak(
                            image->endpoint,
                            connection->destination,
                            connection->control_addr,
                            image->stream_id,
                            image->session_id,
                            term_id,
                            term_offset,
                            (int32_t)length);

                        if (send_nak_result < 0)
                        {
                            work_count = send_nak_result;
                            break;
                        }

                        work_count++;
                        aeron_counter_ordered_increment(image->nak_messages_sent_counter, 1);
                    }
                }
            }
            else
            {
                const size_t index = aeron_logbuffer_index_by_term(image->initial_term_id, term_id);
                uint8_t *buffer = image->mapped_raw_log.term_buffers[index].addr;

                if (aeron_term_gap_filler_try_fill_gap(image->log_meta_data, buffer, term_id, term_offset, length))
                {
                    aeron_counter_ordered_increment(image->loss_gap_fills_counter, 1);
                }

                work_count = 1;
            }

            image->last_loss_change_number = change_number;
        }
    }

    return work_count;
}

// Called from receiver
int aeron_publication_image_initiate_rttm(aeron_publication_image_t *image, int64_t now_ns)
{
    int work_count = 0;

    if (image->congestion_control->should_measure_rtt(image->congestion_control->state, now_ns))
    {
        for (size_t i = 0, len = image->connections.length; i < len; i++)
        {
            aeron_publication_image_connection_t *connection = &image->connections.array[i];

            if (aeron_publication_image_connection_is_alive(connection, now_ns))
            {
                int send_rttm_result = aeron_receive_channel_endpoint_send_rttm(
                    image->endpoint,
                    connection->destination,
                    connection->control_addr,
                    image->stream_id,
                    image->session_id,
                    now_ns,
                    0,
                    true);

                if (send_rttm_result < 0)
                {
                    work_count = send_rttm_result;
                    break;
                }
                else
                {
                    image->congestion_control->on_rttm_sent(image->congestion_control->state, now_ns);
                }

                work_count++;
            }
        }
    }

    return work_count;
}

int aeron_publication_image_add_destination(aeron_publication_image_t *image, aeron_receive_destination_t *destination)
{
    int capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(capacity_result, image->connections, aeron_publication_image_connection_t)

    if (capacity_result < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to ensure space for image->connections");
        return -1;
    }

    aeron_publication_image_connection_t *new_connection = &image->connections.array[image->connections.length];

    new_connection->is_eos = false;
    new_connection->destination = destination;
    new_connection->control_addr = destination->has_control_addr ? &destination->current_control_addr : NULL;
    new_connection->time_of_last_activity_ns = aeron_clock_cached_nano_time(image->cached_clock);
    new_connection->time_of_last_frame_ns = 0;
    new_connection->eos_position = 0;

    image->connections.length++;

    return (int)image->connections.length;
}

int aeron_publication_image_remove_destination(aeron_publication_image_t *image, aeron_udp_channel_t *channel)
{
    int deleted = 0;

    for (int last_index = (int)image->connections.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_receive_destination_t *destination = image->connections.array[i].destination;
        if (aeron_udp_channel_equals(channel, destination->conductor_fields.udp_channel))
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *)image->connections.array, sizeof(aeron_publication_image_connection_t), i, last_index);

            --image->connections.length;
            ++deleted;
            break;
        }
    }

    aeron_update_active_transport_count(image, aeron_clock_cached_nano_time(image->cached_clock));

    return deleted;
}

void aeron_publication_image_check_untethered_subscriptions(
    aeron_driver_conductor_t *conductor, aeron_publication_image_t *image, int64_t now_ns)
{
    int64_t max_sub_pos = 0;

    aeron_subscribable_t *subscribable = &image->conductor_fields.subscribable;
    for (size_t i = 0, length = subscribable->length; i < length; i++)
    {
        aeron_tetherable_position_t *tetherable_position = &subscribable->array[i];

        if (tetherable_position->is_tether)
        {
            int64_t position = aeron_counter_get_volatile(tetherable_position->value_addr);
            max_sub_pos = position > max_sub_pos ? position : max_sub_pos;
        }
    }

    int64_t window_length = image->next_sm_receiver_window_length;
    int64_t untethered_window_limit = (max_sub_pos - window_length) + (window_length / 4);

    for (size_t i = 0, length = subscribable->length; i < length; i++)
    {
        aeron_tetherable_position_t *tetherable_position = &subscribable->array[i];

        if (tetherable_position->is_tether)
        {
            tetherable_position->time_of_last_update_ns = now_ns;
        }
        else
        {
            int64_t window_limit_timeout_ns = image->conductor_fields.untethered_window_limit_timeout_ns;
            int64_t resting_timeout_ns = image->conductor_fields.untethered_resting_timeout_ns;

            switch (tetherable_position->state)
            {
                case AERON_SUBSCRIPTION_TETHER_ACTIVE:
                    if (aeron_counter_get_volatile(tetherable_position->value_addr) > untethered_window_limit)
                    {
                        tetherable_position->time_of_last_update_ns = now_ns;
                    }
                    else if (now_ns > (tetherable_position->time_of_last_update_ns + window_limit_timeout_ns))
                    {
                        aeron_driver_conductor_on_unavailable_image(
                            conductor,
                            image->conductor_fields.managed_resource.registration_id,
                            tetherable_position->subscription_registration_id,
                            image->stream_id,
                            AERON_IPC_CHANNEL,
                            AERON_IPC_CHANNEL_LEN);

                        aeron_driver_subscribable_state(
                            subscribable, tetherable_position, AERON_SUBSCRIPTION_TETHER_LINGER, now_ns);
                    }
                    break;

                case AERON_SUBSCRIPTION_TETHER_LINGER:
                    if (now_ns > (tetherable_position->time_of_last_update_ns + window_limit_timeout_ns))
                    {
                        aeron_driver_subscribable_state(
                            subscribable, tetherable_position, AERON_SUBSCRIPTION_TETHER_RESTING, now_ns);
                    }
                    break;

                case AERON_SUBSCRIPTION_TETHER_RESTING:
                    if (now_ns > (tetherable_position->time_of_last_update_ns + resting_timeout_ns))
                    {
                        int64_t join_position = aeron_publication_image_join_position(image);
                        aeron_counter_set_ordered(tetherable_position->value_addr, join_position);
                        aeron_driver_conductor_on_available_image(
                            conductor,
                            image->conductor_fields.managed_resource.registration_id,
                            image->stream_id,
                            image->session_id,
                            image->log_file_name,
                            image->log_file_name_length,
                            tetherable_position->counter_id,
                            tetherable_position->subscription_registration_id,
                            image->source_identity,
                            image->source_identity_length);

                        aeron_driver_subscribable_state(
                            subscribable, tetherable_position, AERON_SUBSCRIPTION_TETHER_ACTIVE, now_ns);
                    }
                    break;
            }
        }
    }
}

// Called from conductor.
void aeron_publication_image_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_publication_image_t *image, int64_t now_ns, int64_t now_ms)
{
    switch (image->conductor_fields.state)
    {
        case AERON_PUBLICATION_IMAGE_STATE_ACTIVE:
        {
            int64_t last_packet_timestamp_ns;
            AERON_GET_ACQUIRE(last_packet_timestamp_ns, image->time_of_last_packet_ns);
            bool is_end_of_stream;
            AERON_GET_ACQUIRE(is_end_of_stream, image->is_end_of_stream);

            if (!aeron_driver_subscribable_has_working_positions(&image->conductor_fields.subscribable) ||
                now_ns > (last_packet_timestamp_ns + image->conductor_fields.liveness_timeout_ns) ||
                (is_end_of_stream &&
                 aeron_counter_get(image->rcv_pos_position.value_addr) >=
                 aeron_counter_get_volatile(image->rcv_hwm_position.value_addr)))
            {
                image->conductor_fields.state = AERON_PUBLICATION_IMAGE_STATE_DRAINING;
                image->conductor_fields.time_of_last_state_change_ns = now_ns;
                AERON_SET_RELEASE(image->is_sending_eos_sm, true);
            }

            aeron_publication_image_check_untethered_subscriptions(conductor, image, now_ns);
            break;
        }

        case AERON_PUBLICATION_IMAGE_STATE_DRAINING:
        {
            if (aeron_publication_image_is_drained(image) &&
                ((image->conductor_fields.time_of_last_state_change_ns +
                (AERON_IMAGE_SM_EOS_MULTIPLE * image->sm_timeout_ns)) - now_ns < 0))
            {
                image->conductor_fields.state = AERON_PUBLICATION_IMAGE_STATE_LINGER;
                image->conductor_fields.time_of_last_state_change_ns = now_ns;

                aeron_receive_channel_endpoint_dec_image_ref_count(image->endpoint);
                aeron_driver_receiver_proxy_on_remove_publication_image(conductor->context->receiver_proxy, image);
                aeron_driver_conductor_image_transition_to_linger(conductor, image);

                aeron_receive_channel_endpoint_try_remove_endpoint(image->endpoint);
            }
            break;
        }

        case AERON_PUBLICATION_IMAGE_STATE_LINGER:
        {
            if (aeron_publication_image_has_no_subscribers(image) ||
                (now_ns >
                (image->conductor_fields.time_of_last_state_change_ns + image->conductor_fields.liveness_timeout_ns)))
            {
                image->conductor_fields.state = AERON_PUBLICATION_IMAGE_STATE_DONE;
            }
            break;
        }

        case AERON_PUBLICATION_IMAGE_STATE_DONE:
            break;
    }
}

void aeron_publication_image_receiver_release(aeron_publication_image_t *image)
{
    AERON_SET_RELEASE(image->has_receiver_released, true);
}

void aeron_publication_image_invalidate(aeron_publication_image_t *image, int32_t reason_length, const char *reason)
{
    aeron_alloc((void **)&image->invalidation_reason, reason_length + 1);
    memcpy((void *)image->invalidation_reason, reason, reason_length);
}

void aeron_publication_image_remove_response_session_id(aeron_publication_image_t *image)
{
    aeron_publication_image_set_response_session_id(image, AERON_PUBLICATION_RESPONSE_NULL_RESPONSE_SESSION_ID);
}

void aeron_publication_image_stop_status_messages_if_not_active(aeron_publication_image_t *image)
{
    if (AERON_PUBLICATION_IMAGE_STATE_ACTIVE != image->conductor_fields.state)
    {
        image->is_sm_enabled = false;
    }
}

extern bool aeron_publication_image_is_heartbeat(const uint8_t *buffer, size_t length);

extern bool aeron_publication_image_is_end_of_stream(const uint8_t *buffer, size_t length);

extern bool aeron_publication_image_is_flow_control_under_run(
    aeron_publication_image_t *image, int64_t packet_position);

extern bool aeron_publication_image_is_flow_control_over_run(
    aeron_publication_image_t *image, int64_t proposed_position);

extern void aeron_publication_image_schedule_status_message(
    aeron_publication_image_t *image, int64_t sm_position, int32_t window_length);

extern bool aeron_publication_image_is_drained(aeron_publication_image_t *image);

extern bool aeron_publication_image_has_no_subscribers(aeron_publication_image_t *image);

extern bool aeron_publication_image_is_accepting_subscriptions(aeron_publication_image_t *image);

extern void aeron_publication_image_disconnect_endpoint(aeron_publication_image_t *image);

extern void aeron_publication_image_conductor_disconnect_endpoint(aeron_publication_image_t *image);

extern const char *aeron_publication_image_log_file_name(aeron_publication_image_t *image);

extern int64_t aeron_publication_image_registration_id(aeron_publication_image_t *image);

extern bool aeron_publication_image_has_send_response_setup(aeron_publication_image_t *image);

void aeron_publication_image_set_response_session_id(
    aeron_publication_image_t *image, int64_t response_session_id);

extern int64_t aeron_publication_image_join_position(aeron_publication_image_t *image);
