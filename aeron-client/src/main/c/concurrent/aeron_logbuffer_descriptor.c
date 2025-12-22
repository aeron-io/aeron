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

#include <errno.h>
#include <inttypes.h>
#include "util/aeron_error.h"
#include "concurrent/aeron_logbuffer_descriptor.h"

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, term_tail_counters) == 0,
    "offsetof(aeron_logbuffer_metadata_t, term_tail_counters) is wrong");
_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, active_term_count) == 24,
    "offsetof(aeron_logbuffer_metadata_t, active_term_count) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, end_of_stream_position) == 128,
    "offsetof(aeron_logbuffer_metadata_t, end_of_stream_position) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, is_connected) == 136,
    "offsetof(aeron_logbuffer_metadata_t, is_connected) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, active_transport_count) == 140,
    "offsetof(aeron_logbuffer_metadata_t, active_transport_count) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, correlation_id) == 256,
    "offsetof(aeron_logbuffer_metadata_t, correlation_id) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, initial_term_id) == 264,
    "offsetof(aeron_logbuffer_metadata_t, initial_term_id) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, default_frame_header_length) == 268,
    "offsetof(aeron_logbuffer_metadata_t, default_frame_header_length) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, mtu_length) == 272,
    "offsetof(aeron_logbuffer_metadata_t, mtu_length) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, term_length) == 276,
    "offsetof(aeron_logbuffer_metadata_t, term_length) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, page_size) == 280,
    "offsetof(aeron_logbuffer_metadata_t, page_size) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, publication_window_length) == 284,
    "offsetof(aeron_logbuffer_metadata_t, publication_window_length) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, receiver_window_length) == 288,
    "offsetof(aeron_logbuffer_metadata_t, receiver_window_length) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, socket_sndbuf_length) == 292,
    "offsetof(aeron_logbuffer_metadata_t, socket_sndbuf_length) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, os_default_socket_sndbuf_length) == 296,
    "offsetof(aeron_logbuffer_metadata_t, os_default_socket_sndbuf_length) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, os_max_socket_sndbuf_length) == 300,
    "offsetof(aeron_logbuffer_metadata_t, os_max_socket_sndbuf_length) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, socket_rcvbuf_length) == 304,
    "offsetof(aeron_logbuffer_metadata_t, socket_rcvbuf_length) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, os_default_socket_rcvbuf_length) == 308,
    "offsetof(aeron_logbuffer_metadata_t, os_default_socket_rcvbuf_length) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, os_max_socket_rcvbuf_length) == 312,
    "offsetof(aeron_logbuffer_metadata_t, os_max_socket_rcvbuf_length) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, max_resend) == 316,
    "offsetof(aeron_logbuffer_metadata_t, max_resend) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, default_header) == 320,
    "offsetof(aeron_logbuffer_metadata_t, default_header) is wrong");

_Static_assert(
    AERON_LOGBUFFER_DEFAULT_FRAME_HEADER_MAX_LENGTH >= AERON_DATA_HEADER_LENGTH,
    "AERON_LOGBUFFER_DEFAULT_FRAME_HEADER_MAX_LENGTH < AERON_DATA_HEADER_LENGTH");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, entity_tag) == 448,
    "offsetof(aeron_logbuffer_metadata_t, entity_tag) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, entity_tag) % sizeof(int64_t) == 0,
    "offsetof(aeron_logbuffer_metadata_t, entity_tag) not aligned");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, response_correlation_id) == 456,
    "offsetof(aeron_logbuffer_metadata_t, response_correlation_id) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, linger_timeout_ns) == 464,
    "offsetof(aeron_logbuffer_metadata_t, linger_timeout_ns) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, untethered_window_limit_timeout_ns) == 472,
    "offsetof(aeron_logbuffer_metadata_t, untethered_window_limit_timeout_ns) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, untethered_resting_timeout_ns) == 480,
    "offsetof(aeron_logbuffer_metadata_t, untethered_resting_timeout_ns) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, group) == 488,
    "offsetof(aeron_logbuffer_metadata_t, group) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, is_response) == 489,
    "offsetof(aeron_logbuffer_metadata_t, is_response) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, rejoin) == 490,
    "offsetof(aeron_logbuffer_metadata_t, rejoin) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, reliable) == 491,
    "offsetof(aeron_logbuffer_metadata_t, reliable) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, sparse) == 492,
    "offsetof(aeron_logbuffer_metadata_t, sparse) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, signal_eos) == 493,
    "offsetof(aeron_logbuffer_metadata_t, signal_eos) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, spies_simulate_connection) == 494,
    "offsetof(aeron_logbuffer_metadata_t, spies_simulate_connection) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, tether) == 495,
    "offsetof(aeron_logbuffer_metadata_t, tether) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, is_publication_revoked) == 496,
    "offsetof(aeron_logbuffer_metadata_t, is_publication_revoked) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, type) == 497,
    "offsetof(aeron_logbuffer_metadata_t, type) is wrong");

_Static_assert(
    offsetof(aeron_logbuffer_metadata_t, untethered_linger_timeout_ns) == 500,
    "offsetof(aeron_logbuffer_metadata_t, is_publication_revoked) is wrong");

_Static_assert(
    sizeof(aeron_logbuffer_metadata_t) == 508,
    "sizeof(aeron_logbuffer_metadata_t) is wrong");

_Static_assert(
    AERON_LOGBUFFER_META_DATA_LENGTH == AERON_PAGE_MIN_SIZE,
    "AERON_LOGBUFFER_META_DATA_LENGTH != AERON_PAGE_MIN_SIZE");

int aeron_logbuffer_check_term_length(uint64_t term_length)
{
    if (term_length < AERON_LOGBUFFER_TERM_MIN_LENGTH)
    {
        AERON_SET_ERR(
            EINVAL,
            "term length less than min length of %" PRIu64 ": length=%" PRIu64,
            AERON_LOGBUFFER_TERM_MIN_LENGTH, term_length);
        return -1;
    }

    if (term_length > AERON_LOGBUFFER_TERM_MAX_LENGTH)
    {
        AERON_SET_ERR(
            EINVAL,
            "term length greater than max length of %" PRIu64 ": length=%" PRIu64,
            AERON_LOGBUFFER_TERM_MAX_LENGTH, term_length);
        return -1;
    }

    if (!AERON_IS_POWER_OF_TWO(term_length))
    {
        AERON_SET_ERR(
            EINVAL,
            "term length not a power of 2: length=%" PRIu64,
            term_length);
        return -1;
    }

    return 0;
}

int aeron_logbuffer_check_page_size(uint64_t page_size)
{
    if (page_size < AERON_PAGE_MIN_SIZE)
    {
        AERON_SET_ERR(
            EINVAL,
            "page size less than min size of %" PRIu64 ": size=%" PRIu64,
            AERON_PAGE_MIN_SIZE, page_size);
        return -1;
    }

    if (page_size > AERON_PAGE_MAX_SIZE)
    {
        AERON_SET_ERR(
            EINVAL,
            "page size greater than max size of %" PRIu64 ": size=%" PRIu64,
            AERON_PAGE_MAX_SIZE, page_size);
        return -1;
    }

    if (!AERON_IS_POWER_OF_TWO(page_size))
    {
        AERON_SET_ERR(
            EINVAL,
            "page size not a power of 2: size=%" PRIu64,
            page_size);
        return -1;
    }

    return 0;
}

extern int32_t aeron_logbuffer_compute_term_count(int32_t term_id, int32_t initial_term_id);
extern uint64_t aeron_logbuffer_compute_log_length(uint64_t term_length, uint64_t page_size);
extern int32_t aeron_logbuffer_term_offset(int64_t raw_tail, int32_t term_length);
extern int32_t aeron_logbuffer_term_id(int64_t raw_tail);
extern size_t aeron_logbuffer_index_by_position(int64_t position, size_t position_bits_to_shift);
extern size_t aeron_logbuffer_index_by_term(int32_t initial_term_id, int32_t active_term_id);
extern size_t aeron_logbuffer_index_by_term_count(int32_t term_count);
extern int64_t aeron_logbuffer_compute_position(
    int32_t active_term_id, int32_t term_offset, size_t position_bits_to_shift, int32_t initial_term_id);
extern int64_t aeron_logbuffer_compute_term_begin_position(
    int32_t active_term_id, size_t position_bits_to_shift, int32_t initial_term_id);
extern int32_t aeron_logbuffer_compute_term_id_from_position(
    int64_t position, size_t position_bits_to_shift, int32_t initial_term_id);
extern int32_t aeron_logbuffer_compute_term_offset_from_position(int64_t position, size_t position_bits_to_shift);
extern bool aeron_logbuffer_cas_raw_tail(
    aeron_logbuffer_metadata_t *log_meta_data,
    size_t partition_index,
    int64_t expected_raw_tail,
    int64_t update_raw_tail);
extern int32_t aeron_logbuffer_active_term_count(aeron_logbuffer_metadata_t *log_meta_data);
extern bool aeron_logbuffer_cas_active_term_count(
    aeron_logbuffer_metadata_t *log_meta_data,
    int32_t expected_term_count,
    int32_t update_term_count);
extern bool aeron_logbuffer_rotate_log(
    aeron_logbuffer_metadata_t *log_meta_data, int32_t current_term_count, int32_t current_term_id);
extern void aeron_logbuffer_fill_default_header(
    uint8_t *log_meta_data_buffer, int32_t session_id, int32_t stream_id, int32_t initial_term_id);
extern void aeron_logbuffer_metadata_init(
    uint8_t *log_meta_data_buffer,
    int64_t end_of_stream_position,
    int32_t is_connected,
    int32_t active_transport_count,
    int64_t correlation_id,
    int32_t initial_term_id,
    int32_t mtu_length,
    int32_t term_length,
    int32_t page_size,
    int32_t publication_window_length,
    int32_t receiver_window_length,
    int32_t socket_sndbuf_length,
    int32_t os_default_socket_sndbuf_length,
    int32_t os_max_socket_sndbuf_length,
    int32_t socket_rcvbuf_length,
    int32_t os_default_socket_rcvbuf_length,
    int32_t os_max_socket_rcvbuf_length,
    int32_t max_resend,
    int32_t session_id,
    int32_t stream_id,
    int64_t entity_tag,
    int64_t response_correlation_id,
    int64_t linger_timeout_ns,
    int64_t untethered_window_limit_timeout_ns,
    int64_t untethered_linger_timeout_ns,
    int64_t untethered_resting_timeout_ns,
    uint8_t group,
    uint8_t is_response,
    uint8_t rejoin,
    uint8_t reliable,
    uint8_t sparse,
    uint8_t signal_eos,
    uint8_t spies_simulate_connection,
    uint8_t tether,
    uint8_t is_exclusive_publication);
extern void aeron_logbuffer_apply_default_header(uint8_t *log_meta_data_buffer, uint8_t *buffer);
extern size_t aeron_logbuffer_compute_fragmented_length(size_t length, size_t max_payload_length);
