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

#include <errno.h>
#include <string.h>
#include <inttypes.h>
#include "aeron_socket.h"
#include "concurrent/aeron_term_scanner.h"
#include "util/aeron_error.h"
#include "aeron_network_publication.h"
#include "aeron_alloc.h"
#include "media/aeron_send_channel_endpoint.h"
#include "aeron_driver_conductor.h"
#include "concurrent/aeron_logbuffer_unblocker.h"
#include "agent/aeron_driver_agent.h"

#if !defined(HAVE_STRUCT_MMSGHDR)
struct mmsghdr
{
    struct msghdr msg_hdr;
    unsigned int msg_len;
};
#endif

static inline bool aeron_network_publication_liveness_on_remote_close(
    aeron_network_publication_t *publication,
    int64_t receiver_id)
{
    int64_t missing_value = publication->receiver_liveness_tracker.initial_value;
    return missing_value != aeron_int64_counter_map_remove(&publication->receiver_liveness_tracker, receiver_id);
}

static inline int aeron_network_publication_liveness_on_status_message(
    aeron_network_publication_t *publication,
    int64_t receiver_id,
    int64_t time_ns)
{
    if (aeron_int64_counter_map_put(&publication->receiver_liveness_tracker, receiver_id, time_ns, NULL) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    return 0;
}

static inline bool aeron_network_publication_liveness_is_expired(
    void *clientd,
    int64_t receiver_id,
    int64_t last_sm_time_ns)
{
    int64_t *expiry_time_ns = (int64_t *)clientd;
    return last_sm_time_ns <= *expiry_time_ns;
}

static inline void aeron_network_publication_liveness_on_idle(
    aeron_network_publication_t *publication,
    int64_t time_ns,
    int64_t timeout_ns)
{
    const int64_t expiry_time_ns = time_ns - timeout_ns;
    aeron_int64_counter_map_remove_if(
        &publication->receiver_liveness_tracker,
        aeron_network_publication_liveness_is_expired,
        (void *)&expiry_time_ns);
}

static void aeron_network_publication_update_has_receivers(
    aeron_network_publication_t *publication,
    const int64_t now_ns)
{
    aeron_network_publication_liveness_on_idle(publication, now_ns, publication->connection_timeout_ns);
    const bool is_live = 0 != publication->receiver_liveness_tracker.size;

    bool has_receivers;
    AERON_GET_ACQUIRE(has_receivers, publication->has_receivers);
    if (is_live != has_receivers)
    {
        AERON_SET_RELEASE(publication->has_receivers, is_live);
    }
}

int aeron_network_publication_create(
    aeron_network_publication_t **publication,
    aeron_send_channel_endpoint_t *endpoint,
    aeron_driver_context_t *context,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    int32_t initial_term_id,
    aeron_position_t *pub_pos_position,
    aeron_position_t *pub_lmt_position,
    aeron_position_t *snd_pos_position,
    aeron_position_t *snd_lmt_position,
    aeron_atomic_counter_t *snd_bpe_counter,
    aeron_flow_control_strategy_t *flow_control_strategy,
    aeron_driver_uri_publication_params_t *params,
    bool is_exclusive,
    aeron_system_counters_t *system_counters)
{
    aeron_network_publication_t *_pub = NULL;
    const uint64_t log_length = aeron_logbuffer_compute_log_length(params->term_length, context->file_page_size);

    *publication = NULL;

    if (aeron_driver_context_run_storage_checks(context, log_length) < 0)
    {
        return -1;
    }

    if (aeron_alloc((void **)&_pub, sizeof(aeron_network_publication_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Could not allocate network publication");
        return -1;
    }

    char path[AERON_MAX_PATH];
    int path_length = aeron_network_publication_location(path, sizeof(path), context->aeron_dir, registration_id);
    if (path_length < 0)
    {
        AERON_APPEND_ERR("%s", "Could not resolve network publication file path");
        aeron_free(_pub);
        return -1;
    }

    _pub->log_file_name = NULL;
    if (aeron_alloc((void **)(&_pub->log_file_name), (size_t)path_length + 1) < 0)
    {
        AERON_APPEND_ERR("%s", "Could not allocate network publication log_file_name");
        aeron_free(_pub);
        return -1;
    }

    int64_t *retransmit_overflow_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_RETRANSMIT_OVERFLOW);

    bool has_group_semantics = aeron_udp_channel_has_group_semantics(endpoint->conductor_fields.udp_channel);

    if (aeron_retransmit_handler_init(
        &_pub->retransmit_handler,
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_INVALID_PACKETS),
        context->retransmit_unicast_delay_ns,
        context->retransmit_unicast_linger_ns,
        has_group_semantics,
        params->has_max_resend ? params->max_resend : context->max_resend,
        retransmit_overflow_counter) < 0)
    {
        aeron_free(_pub->log_file_name);
        aeron_free(_pub);
        AERON_APPEND_ERR(
            "Could not init network publication retransmit handler, delay: %" PRIu64 ", linger: %" PRIu64,
            context->retransmit_unicast_delay_ns,
            context->retransmit_unicast_linger_ns);
        return -1;
    }

    if (context->raw_log_map_func(
        &_pub->mapped_raw_log, path, params->is_sparse, params->term_length, context->file_page_size) < 0)
    {
        aeron_free(_pub->log_file_name);
        aeron_free(_pub);
        AERON_APPEND_ERR("error mapping network raw log: %s", path);
        return -1;
    }

    _pub->mapped_bytes_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_BYTES_CURRENTLY_MAPPED);
    aeron_counter_add_ordered(_pub->mapped_bytes_counter, (int64_t)log_length);

    _pub->raw_log_close_func = context->raw_log_close_func;
    _pub->raw_log_free_func = context->raw_log_free_func;
    _pub->log.untethered_subscription_state_change = context->log.untethered_subscription_on_state_change;
    _pub->log.resend = context->log.resend;

    strncpy(_pub->log_file_name, path, (size_t)path_length);
    _pub->log_file_name[path_length] = '\0';
    _pub->log_file_name_length = (size_t)path_length;
    _pub->log_meta_data = (aeron_logbuffer_metadata_t *)(_pub->mapped_raw_log.log_meta_data.addr);

    if (params->has_position)
    {
        int32_t term_id = params->term_id;
        int32_t term_count = aeron_logbuffer_compute_term_count(term_id, initial_term_id);
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

    // Called from conductor thread...
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
        (int32_t)endpoint->conductor_fields.udp_channel->socket_sndbuf_length,
        (int32_t)context->os_buffer_lengths.default_so_sndbuf,
        (int32_t)context->os_buffer_lengths.max_so_sndbuf,
        (int32_t)endpoint->conductor_fields.udp_channel->socket_rcvbuf_length,
        (int32_t)context->os_buffer_lengths.default_so_rcvbuf,
        (int32_t)context->os_buffer_lengths.max_so_rcvbuf,
        (int32_t)params->max_resend,
        session_id,
        stream_id,
        (int64_t)params->entity_tag,
        (int64_t)params->response_correlation_id,
        (int64_t)params->linger_timeout_ns,
        (int64_t)params->untethered_window_limit_timeout_ns,
        (int64_t)params->untethered_resting_timeout_ns,
        (uint8_t)has_group_semantics,
        (uint8_t)params->is_response,
        (uint8_t)false,
        (uint8_t)false,
        (uint8_t)params->is_sparse,
        (uint8_t)params->signal_eos,
        (uint8_t)params->spies_simulate_connection,
        (uint8_t)false);

    _pub->endpoint = endpoint;
    _pub->flow_control = flow_control_strategy;
    // Will be called from sender thread.
    _pub->cached_clock = context->sender_cached_clock;
    _pub->conductor_fields.subscribable.array = NULL;
    _pub->conductor_fields.subscribable.length = 0;
    _pub->conductor_fields.subscribable.capacity = 0;
    _pub->conductor_fields.subscribable.add_position_hook_func = aeron_network_publication_add_subscriber_hook;
    _pub->conductor_fields.subscribable.remove_position_hook_func = aeron_network_publication_remove_subscriber_hook;
    _pub->conductor_fields.subscribable.clientd = _pub;
    _pub->conductor_fields.managed_resource.registration_id = registration_id;
    _pub->conductor_fields.managed_resource.clientd = _pub;
    _pub->conductor_fields.managed_resource.incref = aeron_network_publication_incref;
    _pub->conductor_fields.managed_resource.decref = aeron_network_publication_decref;
    _pub->conductor_fields.has_reached_end_of_life = false;
    _pub->conductor_fields.clean_position = 0;
    _pub->conductor_fields.state = AERON_NETWORK_PUBLICATION_STATE_ACTIVE;
    _pub->conductor_fields.refcnt = 1;
    _pub->conductor_fields.time_of_last_activity_ns = now_ns;
    _pub->conductor_fields.last_snd_pos = 0;
    _pub->session_id = session_id;
    _pub->stream_id = stream_id;
    _pub->pub_lmt_position.counter_id = pub_lmt_position->counter_id;
    _pub->pub_lmt_position.value_addr = pub_lmt_position->value_addr;
    _pub->pub_pos_position.counter_id = pub_pos_position->counter_id;
    _pub->pub_pos_position.value_addr = pub_pos_position->value_addr;
    _pub->snd_pos_position.counter_id = snd_pos_position->counter_id;
    _pub->snd_pos_position.value_addr = snd_pos_position->value_addr;
    _pub->snd_lmt_position.counter_id = snd_lmt_position->counter_id;
    _pub->snd_lmt_position.value_addr = snd_lmt_position->value_addr;
    _pub->snd_bpe_counter.counter_id = snd_bpe_counter->counter_id;
    _pub->snd_bpe_counter.value_addr = snd_bpe_counter->value_addr;
    _pub->tag = params->entity_tag;
    _pub->initial_term_id = initial_term_id;
    _pub->starting_term_id = params->has_position ? params->term_id : initial_term_id;
    _pub->starting_term_offset = params->has_position ? params->term_offset : 0;
    _pub->term_buffer_length = _pub->log_meta_data->term_length;
    _pub->term_length_mask = (int32_t)params->term_length - 1;
    _pub->position_bits_to_shift = (size_t)aeron_number_of_trailing_zeroes((int32_t)params->term_length);
    _pub->mtu_length = params->mtu_length;
    _pub->max_messages_per_send = context->network_publication_max_messages_per_send;
    _pub->current_messages_per_send = _pub->max_messages_per_send;
    _pub->term_window_length = params->publication_window_length;
    _pub->linger_timeout_ns = (int64_t)params->linger_timeout_ns;
    _pub->untethered_window_limit_timeout_ns = (int64_t)params->untethered_window_limit_timeout_ns;
    _pub->untethered_resting_timeout_ns = (int64_t)params->untethered_resting_timeout_ns;
    _pub->unblock_timeout_ns = (int64_t)context->publication_unblock_timeout_ns;
    _pub->connection_timeout_ns = (int64_t)context->publication_connection_timeout_ns;
    _pub->time_of_last_data_or_heartbeat_ns = now_ns - AERON_NETWORK_PUBLICATION_HEARTBEAT_TIMEOUT_NS - 1;
    _pub->time_of_last_setup_ns = now_ns - AERON_NETWORK_PUBLICATION_SETUP_TIMEOUT_NS - 1;
    _pub->status_message_deadline_ns = params->spies_simulate_connection ?
        now_ns : now_ns + (int64_t)context->publication_connection_timeout_ns;
    _pub->is_exclusive = is_exclusive;
    _pub->spies_simulate_connection = params->spies_simulate_connection;
    _pub->signal_eos = params->signal_eos;
    _pub->is_setup_elicited = false;
    _pub->has_receivers = false;
    _pub->has_spies = false;
    _pub->is_connected = false;
    _pub->is_end_of_stream = false;
    _pub->track_sender_limits = false;
    _pub->has_sender_released = false;
    _pub->has_received_unicast_eos = false;

    _pub->short_sends_counter = aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_SHORT_SENDS);
    _pub->heartbeats_sent_counter = aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_HEARTBEATS_SENT);
    _pub->sender_flow_control_limits_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_SENDER_FLOW_CONTROL_LIMITS);
    _pub->retransmits_sent_counter = aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_RETRANSMITS_SENT);
    _pub->retransmitted_bytes_counter =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_RETRANSMITTED_BYTES);
    _pub->unblocked_publications_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_UNBLOCKED_PUBLICATIONS);

    _pub->conductor_fields.last_snd_pos = aeron_counter_get(_pub->snd_pos_position.value_addr);
    _pub->conductor_fields.clean_position = _pub->conductor_fields.last_snd_pos;

    _pub->endpoint_address.ss_family = AF_UNSPEC;
    _pub->is_response = AERON_UDP_CHANNEL_CONTROL_MODE_RESPONSE == endpoint->conductor_fields.udp_channel->control_mode;
    _pub->response_correlation_id = params->response_correlation_id;

    aeron_int64_counter_map_init(&_pub->receiver_liveness_tracker, AERON_NULL_VALUE, 16, 0.6f);

    *publication = _pub;

    return 0;
}

void aeron_network_publication_close(
    aeron_counters_manager_t *counters_manager, aeron_network_publication_t *publication)
{
    if (NULL != publication)
    {
        aeron_subscribable_t *subscribable = &publication->conductor_fields.subscribable;

        aeron_counters_manager_free(counters_manager, publication->pub_pos_position.counter_id);
        aeron_counters_manager_free(counters_manager, publication->pub_lmt_position.counter_id);
        aeron_counters_manager_free(counters_manager, publication->snd_pos_position.counter_id);
        aeron_counters_manager_free(counters_manager, publication->snd_lmt_position.counter_id);
        aeron_counters_manager_free(counters_manager, publication->snd_bpe_counter.counter_id);

        for (size_t i = 0, length = subscribable->length; i < length; i++)
        {
            aeron_counters_manager_free(counters_manager, subscribable->array[i].counter_id);
        }

        aeron_free(subscribable->array);
        publication->conductor_fields.managed_resource.clientd = NULL;
        aeron_int64_counter_map_delete(&publication->receiver_liveness_tracker);

        aeron_retransmit_handler_close(&publication->retransmit_handler);
        publication->flow_control->fini(publication->flow_control);
    }
}

bool aeron_network_publication_free(aeron_network_publication_t *publication)
{
    if (NULL == publication)
    {
        return true;
    }

    if (!publication->raw_log_free_func(&publication->mapped_raw_log, publication->log_file_name))
    {
         return false;
    }

    aeron_counter_add_ordered(
        publication->mapped_bytes_counter, -((int64_t)publication->mapped_raw_log.mapped_file.length));

    aeron_free(publication->log_file_name);
    aeron_free(publication);

    return true;
}

static int aeron_network_publication_do_send(
    aeron_network_publication_t *publication,
    struct iovec *iov,
    size_t iov_length,
    int64_t *bytes_sent)
{
    if (publication->is_response)
    {
        if (AF_UNSPEC != publication->endpoint_address.ss_family)
        {
            return aeron_send_channel_send_endpoint_address(
                publication->endpoint, &publication->endpoint_address, iov, iov_length, bytes_sent);
        }
        else
        {
            return 0;
        }
    }
    else
    {
        return aeron_send_channel_send(publication->endpoint, iov, iov_length, bytes_sent);
    }
}

int aeron_network_publication_setup_message_check(
    aeron_network_publication_t *publication, int64_t now_ns, int32_t active_term_id, int32_t term_offset)
{
    int result = 0;
    int64_t bytes_sent = 0;

    if (now_ns > (publication->time_of_last_setup_ns + AERON_NETWORK_PUBLICATION_SETUP_TIMEOUT_NS))
    {
        uint8_t setup_buffer[sizeof(aeron_setup_header_t)];
        aeron_setup_header_t *setup_header = (aeron_setup_header_t *)setup_buffer;
        struct iovec iov;

        uint8_t send_response_flag = (!publication->is_response && AERON_NULL_VALUE != publication->response_correlation_id) ?
            AERON_SETUP_HEADER_SEND_RESPONSE_FLAG : 0;
        uint8_t group_flag = publication->retransmit_handler.has_group_semantics ? AERON_SETUP_HEADER_GROUP_FLAG : 0;

        setup_header->frame_header.frame_length = sizeof(aeron_setup_header_t);
        setup_header->frame_header.version = AERON_FRAME_HEADER_VERSION;
        setup_header->frame_header.flags = 0;
        setup_header->frame_header.type = AERON_HDR_TYPE_SETUP;
        setup_header->frame_header.flags = send_response_flag | group_flag;
        setup_header->term_offset = term_offset;
        setup_header->session_id = publication->session_id;
        setup_header->stream_id = publication->stream_id;
        setup_header->initial_term_id = publication->initial_term_id;
        setup_header->active_term_id = active_term_id;
        setup_header->term_length = publication->term_length_mask + 1;
        setup_header->mtu = (int32_t)publication->mtu_length;
        setup_header->ttl = publication->endpoint->conductor_fields.udp_channel->multicast_ttl;

        iov.iov_base = setup_buffer;
        iov.iov_len = sizeof(aeron_setup_header_t);

        if (publication->is_setup_elicited)
        {
            publication->flow_control->on_setup(
                publication->flow_control->state,
                setup_buffer,
                sizeof(aeron_setup_header_t),
                now_ns,
                *publication->snd_lmt_position.value_addr,
                publication->position_bits_to_shift,
                *publication->snd_pos_position.value_addr);
        }

        if (0 <= (result = aeron_network_publication_do_send(publication, &iov, 1, &bytes_sent)))
        {
            if (bytes_sent < (int64_t)iov.iov_len)
            {
                aeron_counter_increment(publication->short_sends_counter, 1);
            }
        }

        publication->time_of_last_setup_ns = now_ns;

        if (publication->has_receivers)
        {
            publication->is_setup_elicited = false;
        }
    }

    return result;
}

int aeron_network_publication_heartbeat_message_check(
    aeron_network_publication_t *publication,
    int64_t now_ns,
    int32_t active_term_id,
    int32_t term_offset,
    bool signal_end_of_stream)
{
    int result = 0;
    int64_t bytes_sent = 0;

    if (publication->has_initial_connection &&
        now_ns > (publication->time_of_last_data_or_heartbeat_ns + AERON_NETWORK_PUBLICATION_HEARTBEAT_TIMEOUT_NS))
    {
        uint8_t heartbeat_buffer[sizeof(aeron_data_header_t)];
        aeron_data_header_t *data_header = (aeron_data_header_t *)heartbeat_buffer;
        struct iovec iov;

        data_header->frame_header.frame_length = 0;
        data_header->frame_header.version = AERON_FRAME_HEADER_VERSION;
        data_header->frame_header.flags = AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_END_FLAG;
        data_header->frame_header.type = AERON_HDR_TYPE_DATA;
        data_header->term_offset = term_offset;
        data_header->session_id = publication->session_id;
        data_header->stream_id = publication->stream_id;
        data_header->term_id = active_term_id;
        data_header->reserved_value = 0l;

        if (signal_end_of_stream)
        {
            data_header->frame_header.flags =
                AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_END_FLAG | AERON_DATA_HEADER_EOS_FLAG;
        }

        iov.iov_base = heartbeat_buffer;
        iov.iov_len = sizeof(aeron_data_header_t);

        if (0 <= (result = aeron_network_publication_do_send(publication, &iov, 1, &bytes_sent)))
        {
            result = (int)bytes_sent;
            if (bytes_sent < (int64_t)iov.iov_len)
            {
                aeron_counter_increment(publication->short_sends_counter, 1);
            }
        }

        aeron_counter_ordered_increment(publication->heartbeats_sent_counter, 1);
        publication->time_of_last_data_or_heartbeat_ns = now_ns;
    }

    return result;
}

int aeron_network_publication_send_data(
    aeron_network_publication_t *publication, int64_t now_ns, int64_t snd_pos, int32_t term_offset)
{
    const int32_t term_length = publication->term_length_mask + 1;
    const size_t max_vlen = publication->current_messages_per_send;
    int result = 0, vlen = 0;
    int64_t bytes_sent = 0;
    int32_t available_window = (int32_t)(aeron_counter_get(publication->snd_lmt_position.value_addr) - snd_pos);
    int64_t highest_pos = snd_pos;
    struct iovec iov[AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND];

    for (size_t i = 0; i < max_vlen && available_window > 0; i++)
    {
        int32_t scan_limit = available_window < (int32_t)publication->mtu_length ?
           available_window : (int32_t)publication->mtu_length;
        size_t active_index = aeron_logbuffer_index_by_position(snd_pos, publication->position_bits_to_shift);
        int32_t padding = 0;

        uint8_t *ptr = publication->mapped_raw_log.term_buffers[active_index].addr + term_offset;
        const int32_t term_length_left = term_length - term_offset;
        const int32_t available = aeron_term_scanner_scan_for_availability(ptr, term_length_left, scan_limit, &padding);

        if (available > 0)
        {
            iov[i].iov_base = ptr;
            iov[i].iov_len = (uint32_t)available;
            vlen++;

            int32_t total_available = (int32_t)(available + padding);
            available_window -= total_available;
            term_offset += total_available;
            highest_pos += total_available;
        }
        else if (available < 0)
        {
            if (publication->track_sender_limits)
            {
                aeron_counter_ordered_increment(publication->snd_bpe_counter.value_addr, 1);
                aeron_counter_ordered_increment(publication->sender_flow_control_limits_counter, 1);
                publication->track_sender_limits = false;
            }
            break;
        }

        if (available == 0 || term_length == term_offset)
        {
            break;
        }
    }

    if (vlen > 0)
    {
        result = aeron_network_publication_do_send(publication, iov, vlen, &bytes_sent);
        if (result == vlen) /* assume that a partial send from a broken stack will also move the snd-pos */
        {
            publication->time_of_last_data_or_heartbeat_ns = now_ns;
            publication->track_sender_limits = true;
            publication->current_messages_per_send = publication->max_messages_per_send;
            aeron_counter_set_ordered(publication->snd_pos_position.value_addr, highest_pos);
        }
        else if (result >= 0)
        {
            publication->current_messages_per_send = 1;
            aeron_counter_increment(publication->short_sends_counter, 1);
        }
    }
    else if (publication->track_sender_limits && available_window <= 0)
    {
        aeron_counter_ordered_increment(publication->snd_bpe_counter.value_addr, 1);
        aeron_counter_ordered_increment(publication->sender_flow_control_limits_counter, 1);
        publication->track_sender_limits = false;
    }

    return result < 0 ? result : (int)bytes_sent;
}

int aeron_network_publication_send(aeron_network_publication_t *publication, int64_t now_ns)
{
    int64_t snd_pos = aeron_counter_get(publication->snd_pos_position.value_addr);
    int32_t active_term_id = aeron_logbuffer_compute_term_id_from_position(
        snd_pos, publication->position_bits_to_shift, publication->initial_term_id);
    int32_t term_offset = (int32_t)(snd_pos & publication->term_length_mask);

    if (!publication->has_initial_connection || publication->is_setup_elicited)
    {
        if (aeron_network_publication_setup_message_check(publication, now_ns, active_term_id, term_offset) < 0)
        {
            return -1;
        }
    }

    int bytes_sent = aeron_network_publication_send_data(publication, now_ns, snd_pos, term_offset);
    if (bytes_sent < 0)
    {
        return -1;
    }

    if (0 == bytes_sent)
    {
        bool is_end_of_stream;
        AERON_GET_ACQUIRE(is_end_of_stream, publication->is_end_of_stream);

        bytes_sent = aeron_network_publication_heartbeat_message_check(
            publication, now_ns, active_term_id, term_offset, publication->signal_eos && is_end_of_stream);
        if (bytes_sent < 0)
        {
            return -1;
        }

        bool has_spies;
        AERON_GET_ACQUIRE(has_spies, publication->has_spies);

        if (publication->spies_simulate_connection && has_spies && !publication->has_receivers)
        {
            const int64_t new_snd_pos = aeron_network_publication_max_spy_position(publication, snd_pos);
            aeron_counter_set_ordered(publication->snd_pos_position.value_addr, new_snd_pos);

            int64_t flow_control_position = publication->flow_control->on_idle(
                publication->flow_control->state, now_ns, new_snd_pos, new_snd_pos, is_end_of_stream);
            aeron_counter_set_ordered(publication->snd_lmt_position.value_addr, flow_control_position);
        }
        else
        {
            int64_t snd_lmt = aeron_counter_get(publication->snd_lmt_position.value_addr);
            int64_t flow_control_position = publication->flow_control->on_idle(
                publication->flow_control->state, now_ns, snd_lmt, snd_pos, is_end_of_stream);
            aeron_counter_set_ordered(publication->snd_lmt_position.value_addr, flow_control_position);
        }

        aeron_network_publication_update_has_receivers(publication, now_ns);
    }

    aeron_retransmit_handler_process_timeouts(
        &publication->retransmit_handler, now_ns, aeron_network_publication_resend, publication);

    return bytes_sent;
}

int aeron_network_publication_resend(void *clientd, int32_t term_id, int32_t term_offset, size_t length)
{
    aeron_network_publication_t *publication = (aeron_network_publication_t *)clientd;
    int64_t sender_position = aeron_counter_get(publication->snd_pos_position.value_addr);
    int64_t resend_position = aeron_logbuffer_compute_position(
        term_id, term_offset, publication->position_bits_to_shift, publication->initial_term_id);
    int32_t term_length = publication->term_length_mask + 1;
    int64_t bottom_resend_window =
        sender_position - (int64_t)(term_length >> 1) - (int64_t)aeron_compute_max_message_length(term_length);
    int result = 0;

    if (bottom_resend_window <= resend_position && resend_position < sender_position)
    {
        size_t index = aeron_logbuffer_index_by_position(resend_position, publication->position_bits_to_shift);
        size_t remaining_bytes = length;
        int32_t bytes_sent = 0;
        int32_t total_bytes_sent = 0;
        int32_t offset = term_offset;

        do
        {
            offset += bytes_sent;

            uint8_t *ptr = publication->mapped_raw_log.term_buffers[index].addr + offset;
            int32_t term_length_left = term_length - offset;
            int32_t padding = 0;
            int32_t max_length = remaining_bytes < publication->mtu_length ?
                (int32_t)remaining_bytes : (int32_t)publication->mtu_length;

            int32_t available = aeron_term_scanner_scan_for_availability(ptr, term_length_left, max_length, &padding);
            if (available <= 0)
            {
                break;
            }

            struct iovec iov;
            iov.iov_base = ptr;
            iov.iov_len = (uint32_t)available;
            int64_t msg_bytes_sent = 0;

            int sendmsg_result = aeron_network_publication_do_send(publication, &iov, 1, &msg_bytes_sent);
            if (0 <= sendmsg_result)
            {
                if (msg_bytes_sent < (int64_t)iov.iov_len)
                {
                    aeron_counter_increment(publication->short_sends_counter, 1);
                    break;
                }
            }
            else
            {
                result = -1;
                break;
            }

            bytes_sent = available + padding;
            total_bytes_sent += bytes_sent;
            remaining_bytes -= bytes_sent;
        }
        while (remaining_bytes > 0);

        if (total_bytes_sent > 0)
        {
            aeron_counter_ordered_increment(publication->retransmits_sent_counter, 1);
            aeron_counter_add_ordered(publication->retransmitted_bytes_counter, total_bytes_sent);
        }
    }

    if (NULL != publication->log.resend)
    {
        publication->log.resend(
            publication->session_id,
            publication->stream_id,
            term_id,
            term_offset,
            (int32_t)length,
            publication->endpoint->conductor_fields.udp_channel->uri_length,
            publication->endpoint->conductor_fields.udp_channel->original_uri);
    }

    return result;
}

int aeron_network_publication_on_nak(
    aeron_network_publication_t *publication, int32_t term_id, int32_t term_offset, int32_t length)
{
    int result = aeron_retransmit_handler_on_nak(
        &publication->retransmit_handler,
        term_id,
        term_offset,
        (size_t)length,
        (size_t)publication->term_length_mask + 1,
        publication->mtu_length,
        publication->flow_control,
        aeron_clock_cached_nano_time(publication->cached_clock),
        aeron_network_publication_resend,
        publication);

    if (0 != result)
    {
        AERON_APPEND_ERR("%s", "");
    }

    return result;
}

inline static bool aeron_network_publication_has_required_receivers(aeron_network_publication_t *publication)
{
    bool has_receivers;
    AERON_GET_ACQUIRE(has_receivers, publication->has_receivers);

    return has_receivers && publication->flow_control->has_required_receivers(publication->flow_control);
}

inline static void aeron_network_publication_update_connected_status(
    aeron_network_publication_t *publication,
    bool expected_status)
{
    bool is_connected;
    AERON_GET_ACQUIRE(is_connected, publication->is_connected);

    if (is_connected != expected_status)
    {
        AERON_SET_RELEASE(publication->log_meta_data->is_connected, expected_status);
        AERON_SET_RELEASE(publication->is_connected, expected_status);
    }
}

void aeron_network_publication_on_status_message(
    aeron_network_publication_t *publication,
    aeron_driver_conductor_proxy_t *conductor_proxy,
    const uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
    const int64_t time_ns = aeron_clock_cached_nano_time(publication->cached_clock);
    const aeron_status_message_header_t *sm = (aeron_status_message_header_t *)buffer;
    const bool is_eos = sm->frame_header.flags & AERON_STATUS_MESSAGE_HEADER_EOS_FLAG;
    publication->status_message_deadline_ns = time_ns + publication->connection_timeout_ns;

    if (is_eos)
    {
        aeron_network_publication_liveness_on_remote_close(publication, sm->receiver_id);

        if (aeron_send_channel_is_unicast(publication->endpoint))
        {
            AERON_SET_RELEASE(publication->has_received_unicast_eos, true);
        }
    }
    else
    {
        aeron_network_publication_liveness_on_status_message(publication, sm->receiver_id, time_ns);
    }

    const bool is_live = 0 != publication->receiver_liveness_tracker.size;
    bool existing_has_receivers;
    AERON_GET_ACQUIRE(existing_has_receivers, publication->has_receivers);

    if (!existing_has_receivers && is_live)
    {
        aeron_driver_conductor_proxy_on_response_connected(conductor_proxy, publication->response_correlation_id);
    }

    if (existing_has_receivers != is_live)
    {
        AERON_SET_RELEASE(publication->has_receivers, is_live);
    }

    if (!publication->has_initial_connection)
    {
        publication->has_initial_connection = true;
    }

    aeron_counter_set_ordered(
        publication->snd_lmt_position.value_addr,
        publication->flow_control->on_status_message(
            publication->flow_control->state,
            buffer,
            length,
            addr,
            *publication->snd_lmt_position.value_addr,
            publication->initial_term_id,
            publication->position_bits_to_shift,
            time_ns));

    aeron_network_publication_update_connected_status(
        publication,
        aeron_network_publication_has_required_receivers(publication));
}

void aeron_network_publication_on_error(
    aeron_network_publication_t *publication,
    int64_t destination_registration_id,
    const uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *src_address,
    aeron_driver_conductor_proxy_t *conductor_proxy)
{
    aeron_error_t *error = (aeron_error_t *)buffer;
    const uint8_t *error_text = (const uint8_t *)(error + 1);
    const int64_t time_ns = aeron_clock_cached_nano_time(publication->cached_clock);
    publication->flow_control->on_error(publication->flow_control->state, buffer, length, src_address, time_ns);
    if (aeron_network_publication_liveness_on_remote_close(publication, error->receiver_id))
    {
        const int64_t registration_id = aeron_network_publication_registration_id(publication);
        aeron_driver_conductor_proxy_on_publication_error(
            conductor_proxy,
            registration_id,
            destination_registration_id,
            error->session_id,
            error->stream_id,
            error->receiver_id,
            AERON_ERROR_HAS_GROUP_TAG_FLAG & error->frame_header.flags ? error->group_tag : AERON_NULL_VALUE,
            src_address,
            error->error_code,
            error->error_length,
            error_text);
    }
}

void aeron_network_publication_on_rttm(
    aeron_network_publication_t *publication, const uint8_t *buffer, size_t length, struct sockaddr_storage *addr)
{
    aeron_rttm_header_t *rttm_in_header = (aeron_rttm_header_t *)buffer;

    if (rttm_in_header->frame_header.flags & AERON_RTTM_HEADER_REPLY_FLAG)
    {
        uint8_t rttm_reply_buffer[sizeof(aeron_rttm_header_t)];
        aeron_rttm_header_t *rttm_out_header = (aeron_rttm_header_t *)rttm_reply_buffer;
        struct iovec iov;
        int64_t bytes_sent;

        rttm_out_header->frame_header.frame_length = sizeof(aeron_rttm_header_t);
        rttm_out_header->frame_header.version = AERON_FRAME_HEADER_VERSION;
        rttm_out_header->frame_header.flags = 0;
        rttm_out_header->frame_header.type = AERON_HDR_TYPE_RTTM;
        rttm_out_header->session_id = publication->session_id;
        rttm_out_header->stream_id = publication->stream_id;
        rttm_out_header->echo_timestamp = rttm_in_header->echo_timestamp;
        rttm_out_header->reception_delta = 0;
        rttm_out_header->receiver_id = rttm_in_header->receiver_id;

        iov.iov_base = rttm_reply_buffer;
        iov.iov_len = sizeof(aeron_rttm_header_t);

        if (0 <= aeron_network_publication_do_send(publication, &iov, 1, &bytes_sent))
        {
            if (bytes_sent < (int64_t)iov.iov_len)
            {
                aeron_counter_increment(publication->short_sends_counter, 1);
            }
        }
    }
}

void aeron_network_publication_clean_buffer(aeron_network_publication_t *publication, int64_t position)
{
    int64_t clean_position = publication->conductor_fields.clean_position;
    if (position > clean_position)
    {
        size_t dirty_index = aeron_logbuffer_index_by_position(clean_position, publication->position_bits_to_shift);
        size_t bytes_to_clean = (size_t)(position - clean_position);
        size_t term_length = publication->mapped_raw_log.term_length;
        size_t term_offset = (size_t)(clean_position & publication->term_length_mask);
        size_t bytes_left_in_term = term_length - term_offset;
        size_t length = bytes_to_clean < bytes_left_in_term ? bytes_to_clean : bytes_left_in_term;

        memset(
            publication->mapped_raw_log.term_buffers[dirty_index].addr + term_offset + sizeof(int64_t),
            0,
            length - sizeof(int64_t));

        uint64_t *ptr = (uint64_t *)(publication->mapped_raw_log.term_buffers[dirty_index].addr + term_offset);
        AERON_SET_RELEASE(*ptr, (uint64_t)0);

        publication->conductor_fields.clean_position = clean_position + (int64_t)length;
    }
}

int aeron_network_publication_update_pub_pos_and_lmt(aeron_network_publication_t *publication)
{
    int work_count = 0;

    if (AERON_NETWORK_PUBLICATION_STATE_ACTIVE == publication->conductor_fields.state)
    {
        const int64_t producer_position = aeron_network_publication_producer_position(publication);
        int64_t snd_pos = aeron_counter_get_volatile(publication->snd_pos_position.value_addr);

        aeron_counter_set_ordered(publication->pub_pos_position.value_addr, producer_position);

        if (aeron_network_publication_has_required_receivers(publication) ||
            (publication->spies_simulate_connection &&
            aeron_driver_subscribable_has_working_positions(&publication->conductor_fields.subscribable)))
        {
            int64_t min_consumer_position = snd_pos;
            if (publication->conductor_fields.subscribable.length > 0)
            {
                for (size_t i = 0, length = publication->conductor_fields.subscribable.length; i < length; i++)
                {
                    aeron_tetherable_position_t *tetherable_position =
                        &publication->conductor_fields.subscribable.array[i];

                    if (AERON_SUBSCRIPTION_TETHER_RESTING != tetherable_position->state)
                    {
                        int64_t position = aeron_counter_get_volatile(tetherable_position->value_addr);
                        min_consumer_position = position < min_consumer_position ? position : min_consumer_position;
                    }
                }
            }

            int64_t new_limit_position = min_consumer_position + publication->term_window_length;
            if (new_limit_position > aeron_counter_get(publication->pub_lmt_position.value_addr))
            {
                aeron_network_publication_clean_buffer(publication, min_consumer_position - publication->term_buffer_length);
                const int64_t clean_position = publication->conductor_fields.clean_position;
                const int32_t dirty_term_id = aeron_logbuffer_compute_term_id_from_position(
                    clean_position, publication->position_bits_to_shift, publication->initial_term_id);
                const int32_t active_term_id = aeron_logbuffer_compute_term_id_from_position(
                    new_limit_position, publication->position_bits_to_shift, publication->initial_term_id);
                const int32_t term_gap = aeron_logbuffer_compute_term_count(active_term_id, dirty_term_id);
                if (term_gap < 2 || (2 == term_gap && 0 != (clean_position & publication->term_length_mask)))
                {
                    aeron_counter_set_ordered(publication->pub_lmt_position.value_addr, new_limit_position);
                }
                work_count = 1;
            }
        }
        else if (*publication->pub_lmt_position.value_addr > snd_pos)
        {
            aeron_network_publication_update_connected_status(publication, false);
            aeron_counter_set_ordered(publication->pub_lmt_position.value_addr, snd_pos);
            aeron_network_publication_clean_buffer(publication, snd_pos - publication->term_buffer_length);
            work_count = 1;
        }
    }

    return work_count;
}

void aeron_network_publication_check_for_blocked_publisher(
    aeron_network_publication_t *publication, int64_t now_ns, int64_t producer_position, int64_t snd_pos)
{
    if (snd_pos == publication->conductor_fields.last_snd_pos &&
        aeron_network_publication_is_possibly_blocked(publication, producer_position, snd_pos))
    {
        if (now_ns > (publication->conductor_fields.time_of_last_activity_ns + publication->unblock_timeout_ns))
        {
            if (aeron_logbuffer_unblocker_unblock(
                publication->mapped_raw_log.term_buffers, publication->log_meta_data, snd_pos))
            {
                aeron_counter_ordered_increment(publication->unblocked_publications_counter, 1);
            }
        }
    }
    else
    {
        publication->conductor_fields.time_of_last_activity_ns = now_ns;
        publication->conductor_fields.last_snd_pos = snd_pos;
    }
}

void aeron_network_publication_incref(void *clientd)
{
    aeron_network_publication_t *publication = (aeron_network_publication_t *)clientd;
    publication->conductor_fields.refcnt++;
}

void aeron_network_publication_decref(void *clientd)
{
    aeron_network_publication_t *publication = (aeron_network_publication_t *)clientd;
    int32_t ref_count = --publication->conductor_fields.refcnt;

    if (0 == ref_count)
    {
        const int64_t producer_position = aeron_network_publication_producer_position(publication);

        publication->conductor_fields.state = AERON_NETWORK_PUBLICATION_STATE_DRAINING;
        publication->conductor_fields.time_of_last_activity_ns = aeron_clock_cached_nano_time(
            publication->cached_clock);

        aeron_counter_set_ordered(publication->pub_lmt_position.value_addr, producer_position);
        AERON_SET_RELEASE(publication->log_meta_data->end_of_stream_position, producer_position);

        if (aeron_counter_get_volatile(publication->snd_pos_position.value_addr) >= producer_position)
        {
            AERON_SET_RELEASE(publication->is_end_of_stream, true);
        }
    }
}

bool aeron_network_publication_spies_finished_consuming(
    aeron_network_publication_t *publication, aeron_driver_conductor_t *conductor, int64_t eos_pos)
{
    if (aeron_driver_subscribable_has_working_positions(&publication->conductor_fields.subscribable))
    {
        for (size_t i = 0, length = publication->conductor_fields.subscribable.length; i < length; i++)
        {
            aeron_tetherable_position_t *tetherable_position = &publication->conductor_fields.subscribable.array[i];

            if (AERON_SUBSCRIPTION_TETHER_RESTING != tetherable_position->state)
            {
                if (aeron_counter_get_volatile(tetherable_position->value_addr) < eos_pos)
                {
                    return false;
                }
            }
        }

        AERON_SET_RELEASE(publication->has_spies, false);
        aeron_driver_conductor_cleanup_spies(conductor, publication);

        for (size_t i = 0, length = publication->conductor_fields.subscribable.length; i < length; i++)
        {
            aeron_counters_manager_free(
                &conductor->counters_manager, (int32_t)publication->conductor_fields.subscribable.array[i].counter_id);
        }

        aeron_free(publication->conductor_fields.subscribable.array);
        publication->conductor_fields.subscribable.array = NULL;
        publication->conductor_fields.subscribable.length = 0;
        publication->conductor_fields.subscribable.capacity = 0;
    }

    return true;
}

void aeron_network_publication_check_untethered_subscriptions(
    aeron_driver_conductor_t *conductor, aeron_network_publication_t *publication, int64_t now_ns)
{
    const int64_t sender_position = aeron_counter_get_volatile(publication->snd_pos_position.value_addr);
    int64_t term_window_length = publication->term_window_length;
    int64_t untethered_window_limit = (sender_position - term_window_length) + (term_window_length / 4);

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
            int64_t window_limit_timeout_ns = publication->untethered_window_limit_timeout_ns;
            int64_t resting_timeout_ns = publication->untethered_resting_timeout_ns;

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
                            publication->conductor_fields.managed_resource.registration_id,
                            tetherable_position->subscription_registration_id,
                            publication->stream_id,
                            AERON_IPC_CHANNEL,
                            AERON_IPC_CHANNEL_LEN);

                        aeron_driver_subscribable_state(
                            subscribable, tetherable_position, AERON_SUBSCRIPTION_TETHER_LINGER, now_ns);

                        conductor->context->log.untethered_subscription_on_state_change(
                            tetherable_position,
                            now_ns,
                            AERON_SUBSCRIPTION_TETHER_LINGER,
                            publication->stream_id,
                            publication->session_id);
                    }
                    break;

                case AERON_SUBSCRIPTION_TETHER_LINGER:
                    if (now_ns > (tetherable_position->time_of_last_update_ns + window_limit_timeout_ns))
                    {
                        aeron_driver_subscribable_state(
                            subscribable, tetherable_position, AERON_SUBSCRIPTION_TETHER_RESTING, now_ns);

                        conductor->context->log.untethered_subscription_on_state_change(
                            tetherable_position,
                            now_ns,
                            AERON_SUBSCRIPTION_TETHER_RESTING,
                            publication->stream_id,
                            publication->session_id);
                    }
                    break;

                case AERON_SUBSCRIPTION_TETHER_RESTING:
                    if (now_ns > (tetherable_position->time_of_last_update_ns + resting_timeout_ns))
                    {
                        aeron_counter_set_ordered(tetherable_position->value_addr, sender_position);

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
                            subscribable, tetherable_position, AERON_SUBSCRIPTION_TETHER_ACTIVE, now_ns);

                        conductor->context->log.untethered_subscription_on_state_change(
                            tetherable_position,
                            now_ns,
                            AERON_SUBSCRIPTION_TETHER_ACTIVE,
                            publication->stream_id,
                            publication->session_id);
                    }
                    break;
            }
        }
    }
}

void aeron_network_publication_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_network_publication_t *publication, int64_t now_ns, int64_t now_ms)
{
    switch (publication->conductor_fields.state)
    {
        case AERON_NETWORK_PUBLICATION_STATE_ACTIVE:
        {
            aeron_network_publication_check_untethered_subscriptions(conductor, publication, now_ns);

            const bool current_connected_status =
                aeron_network_publication_has_required_receivers(publication) ||
                (publication->spies_simulate_connection &&
                aeron_driver_subscribable_has_working_positions(&publication->conductor_fields.subscribable));
            aeron_network_publication_update_connected_status(publication, current_connected_status);

            const int64_t producer_position = aeron_network_publication_producer_position(publication);
            aeron_counter_set_ordered(publication->pub_pos_position.value_addr, producer_position);

            if (!publication->is_exclusive)
            {
                aeron_network_publication_check_for_blocked_publisher(
                    publication,
                    now_ns,
                    producer_position,
                    aeron_counter_get_volatile(publication->snd_pos_position.value_addr));
            }
            break;
        }

        case AERON_NETWORK_PUBLICATION_STATE_DRAINING:
        {
            const int64_t producer_position = aeron_network_publication_producer_position(publication);
            aeron_counter_set_ordered(publication->pub_pos_position.value_addr, producer_position);

            const int64_t sender_position = aeron_counter_get_volatile(publication->snd_pos_position.value_addr);

            if (producer_position > sender_position)
            {
                if (aeron_logbuffer_unblocker_unblock(
                    publication->mapped_raw_log.term_buffers, publication->log_meta_data, sender_position))
                {
                    aeron_counter_ordered_increment(publication->unblocked_publications_counter, 1);
                    break;
                }

                bool has_receivers;
                AERON_GET_ACQUIRE(has_receivers, publication->has_receivers);
                if (has_receivers)
                {
                    break;
                }
            }
            else
            {
                AERON_SET_RELEASE(publication->is_end_of_stream, true);
            }

            if (aeron_network_publication_spies_finished_consuming(publication, conductor, producer_position))
            {
                publication->conductor_fields.time_of_last_activity_ns = now_ns;
                publication->conductor_fields.state = AERON_NETWORK_PUBLICATION_STATE_LINGER;
            }
            break;
        }

        case AERON_NETWORK_PUBLICATION_STATE_LINGER:
        {
            bool has_received_unicast_eos = false;
            AERON_GET_ACQUIRE(has_received_unicast_eos, publication->has_received_unicast_eos);

            if (has_received_unicast_eos ||
                now_ns > (publication->conductor_fields.time_of_last_activity_ns + publication->linger_timeout_ns))
            {
                aeron_driver_conductor_cleanup_network_publication(conductor, publication);
                publication->conductor_fields.state = AERON_NETWORK_PUBLICATION_STATE_DONE;
            }
            break;
        }

        case AERON_NETWORK_PUBLICATION_STATE_DONE:
            break;
    }
}

extern void aeron_network_publication_add_subscriber_hook(void *clientd, volatile int64_t *value_addr);

extern void aeron_network_publication_remove_subscriber_hook(void *clientd, volatile int64_t *value_addr);

extern bool aeron_network_publication_is_possibly_blocked(
    aeron_network_publication_t *publication, int64_t producer_position, int64_t consumer_position);

extern int64_t aeron_network_publication_producer_position(aeron_network_publication_t *publication);

extern int64_t aeron_network_publication_join_position(aeron_network_publication_t *publication);

extern void aeron_network_publication_trigger_send_setup_frame(
    aeron_network_publication_t *publication, uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

extern void aeron_network_publication_sender_release(aeron_network_publication_t *publication);

extern bool aeron_network_publication_has_sender_released(aeron_network_publication_t *publication);

extern int64_t aeron_network_publication_max_spy_position(aeron_network_publication_t *publication, int64_t snd_pos);

extern bool aeron_network_publication_is_accepting_subscriptions(aeron_network_publication_t *publication);

extern inline int64_t aeron_network_publication_registration_id(aeron_network_publication_t *publication);
