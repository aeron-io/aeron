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

#ifndef AERON_LOGBUFFER_DESCRIPTOR_H
#define AERON_LOGBUFFER_DESCRIPTOR_H

#include <string.h>

#include "protocol/aeron_udp_protocol.h"
#include "util/aeron_bitutil.h"
#include "util/aeron_math.h"
#include "concurrent/aeron_atomic.h"

#define AERON_LOGBUFFER_PARTITION_COUNT (3)
#define AERON_LOGBUFFER_TERM_MIN_LENGTH (64 * 1024)
#define AERON_LOGBUFFER_TERM_MAX_LENGTH (1024 * 1024 * 1024)
#define AERON_PAGE_MIN_SIZE UINT32_C(4 * 1024)
#define AERON_PAGE_MAX_SIZE UINT32_C(1024 * 1024 * 1024)
#define AERON_LOGBUFFER_PADDING_SIZE UINT32_C(64)
#define AERON_LOGBUFFER_DEFAULT_FRAME_HEADER_MAX_LENGTH (AERON_CACHE_LINE_LENGTH * 2)
#define AERON_LOGBUFFER_TYPE_CONCURRENT_PUBLICATION UINT8_C(0)
#define AERON_LOGBUFFER_TYPE_EXCLUSIVE_PUBLICATION UINT8_C(1)
#define AERON_LOGBUFFER_TYPE_PUBLICATION_IMAGE UINT8_C(2)

#define AERON_MAX_UDP_PAYLOAD_LENGTH (65504)

#pragma pack(push)
#pragma pack(4)
typedef struct aeron_logbuffer_metadata_stct
{
    volatile int64_t term_tail_counters[AERON_LOGBUFFER_PARTITION_COUNT];
    volatile int32_t active_term_count;

    uint8_t pad1[(2 * AERON_LOGBUFFER_PADDING_SIZE) - ((AERON_LOGBUFFER_PARTITION_COUNT * sizeof(int64_t)) + sizeof(int32_t))];
    volatile int64_t end_of_stream_position;
    volatile int32_t is_connected;
    volatile int32_t active_transport_count;

    uint8_t pad2[(2 * AERON_LOGBUFFER_PADDING_SIZE) - (sizeof(int64_t) + (2 * sizeof(int32_t)))];
    int64_t correlation_id;
    int32_t initial_term_id;
    int32_t default_frame_header_length;
    int32_t mtu_length;
    int32_t term_length;
    int32_t page_size;
    int32_t publication_window_length;
    int32_t receiver_window_length;
    int32_t socket_sndbuf_length;
    int32_t os_default_socket_sndbuf_length;
    int32_t os_max_socket_sndbuf_length;
    int32_t socket_rcvbuf_length;
    int32_t os_default_socket_rcvbuf_length;
    int32_t os_max_socket_rcvbuf_length;
    int32_t max_resend;
    uint8_t default_header[AERON_LOGBUFFER_DEFAULT_FRAME_HEADER_MAX_LENGTH];
    int64_t entity_tag;
    int64_t response_correlation_id;
    int64_t linger_timeout_ns;
    int64_t untethered_window_limit_timeout_ns;
    int64_t untethered_resting_timeout_ns;
    uint8_t group;
    uint8_t is_response;
    uint8_t rejoin;
    uint8_t reliable;
    uint8_t sparse;
    uint8_t signal_eos;
    uint8_t spies_simulate_connection;
    uint8_t tether;
    volatile uint8_t is_publication_revoked;
    uint8_t type;
    uint8_t pad3[2 * sizeof(uint8_t)];
    int64_t untethered_linger_timeout_ns;
}
aeron_logbuffer_metadata_t;
#pragma pack(pop)

#define AERON_LOGBUFFER_META_DATA_LENGTH (AERON_PAGE_MIN_SIZE)

#define AERON_LOGBUFFER_FRAME_ALIGNMENT (32)

#define AERON_LOGBUFFER_RAWTAIL_VOLATILE(d, m) \
do \
{ \
    int32_t active_term_count; \
    AERON_GET_ACQUIRE(active_term_count, ((m)->active_term_count)); \
    size_t partition = (size_t)(active_term_count % AERON_LOGBUFFER_PARTITION_COUNT); \
    AERON_GET_ACQUIRE(d, (m)->term_tail_counters[partition]); \
} \
while (false)

int aeron_logbuffer_check_term_length(uint64_t term_length);
int aeron_logbuffer_check_page_size(uint64_t page_size);

inline uint64_t aeron_logbuffer_compute_log_length(uint64_t term_length, uint64_t page_size)
{
    return AERON_ALIGN(((term_length * AERON_LOGBUFFER_PARTITION_COUNT) + AERON_LOGBUFFER_META_DATA_LENGTH), page_size);
}

inline int32_t aeron_logbuffer_term_offset(int64_t raw_tail, int32_t term_length)
{
    int64_t offset = raw_tail & 0xFFFFFFFFL;
    return offset < term_length ? (int32_t)offset : term_length;
}

inline int32_t aeron_logbuffer_term_id(int64_t raw_tail)
{
    return (int32_t)(raw_tail >> 32);
}

inline int32_t aeron_logbuffer_compute_term_count(int32_t term_id, int32_t initial_term_id)
{
    return aeron_sub_wrap_i32(term_id, initial_term_id);
}

inline size_t aeron_logbuffer_index_by_position(int64_t position, size_t position_bits_to_shift)
{
    return (size_t)((position >> position_bits_to_shift) % AERON_LOGBUFFER_PARTITION_COUNT);
}

inline size_t aeron_logbuffer_index_by_term(int32_t initial_term_id, int32_t active_term_id)
{
    int32_t term_count = aeron_logbuffer_compute_term_count(active_term_id, initial_term_id);
    return (size_t)(term_count % AERON_LOGBUFFER_PARTITION_COUNT);
}

inline size_t aeron_logbuffer_index_by_term_count(int32_t term_count)
{
    return (size_t)(term_count % AERON_LOGBUFFER_PARTITION_COUNT);
}

inline int64_t aeron_logbuffer_compute_position(
    int32_t active_term_id, int32_t term_offset, size_t position_bits_to_shift, int32_t initial_term_id)
{
    int64_t term_count = aeron_logbuffer_compute_term_count(active_term_id, initial_term_id);
    return (term_count << position_bits_to_shift) + term_offset;
}

inline int64_t aeron_logbuffer_compute_term_begin_position(
    int32_t active_term_id, size_t position_bits_to_shift, int32_t initial_term_id)
{
    return aeron_logbuffer_compute_position(active_term_id, 0, position_bits_to_shift, initial_term_id);
}

inline int32_t aeron_logbuffer_compute_term_id_from_position(
    int64_t position, size_t position_bits_to_shift, int32_t initial_term_id)
{
    return aeron_add_wrap_i32((int32_t)(position >> position_bits_to_shift), initial_term_id);
}

inline int32_t aeron_logbuffer_compute_term_offset_from_position(int64_t position, size_t position_bits_to_shift)
{
    int64_t mask = (1u << position_bits_to_shift) - 1;

    return (int32_t)(position & mask);
}

inline bool aeron_logbuffer_cas_raw_tail(
    aeron_logbuffer_metadata_t *log_meta_data,
    size_t partition_index,
    int64_t expected_raw_tail,
    int64_t update_raw_tail)
{
    return aeron_cas_int64(&log_meta_data->term_tail_counters[partition_index], expected_raw_tail, update_raw_tail);
}

inline int32_t aeron_logbuffer_active_term_count(aeron_logbuffer_metadata_t *log_meta_data)
{
    int32_t active_term_count;
    AERON_GET_ACQUIRE(active_term_count, log_meta_data->active_term_count);
    return active_term_count;
}

inline bool aeron_logbuffer_cas_active_term_count(
    aeron_logbuffer_metadata_t *log_meta_data,
    int32_t expected_term_count,
    int32_t update_term_count)
{
    return aeron_cas_int32(&log_meta_data->active_term_count, expected_term_count, update_term_count);
}

inline bool aeron_logbuffer_rotate_log(
    aeron_logbuffer_metadata_t *log_meta_data, int32_t current_term_count, int32_t current_term_id)
{
    const int32_t next_term_id = current_term_id + 1;
    const int32_t next_term_count = current_term_count + 1;
    const size_t next_index = aeron_logbuffer_index_by_term_count(next_term_count);
    const int32_t expected_term_id = next_term_id - AERON_LOGBUFFER_PARTITION_COUNT;

    int64_t raw_tail;
    do
    {
        AERON_GET_ACQUIRE(raw_tail, log_meta_data->term_tail_counters[next_index]);
        if (expected_term_id != aeron_logbuffer_term_id(raw_tail))
        {
            break;
        }
    }
    while (!aeron_logbuffer_cas_raw_tail(
        log_meta_data, next_index, raw_tail, (int64_t)((uint64_t)next_term_id << 32u)));

    return aeron_logbuffer_cas_active_term_count(log_meta_data, current_term_count, next_term_count);
}

inline void aeron_logbuffer_fill_default_header(
    uint8_t *log_meta_data_buffer, int32_t session_id, int32_t stream_id, int32_t initial_term_id)
{
    aeron_logbuffer_metadata_t *log_meta_data = (aeron_logbuffer_metadata_t *)log_meta_data_buffer;
    aeron_data_header_t *data_header = (aeron_data_header_t *)(log_meta_data->default_header);

    log_meta_data->default_frame_header_length = AERON_DATA_HEADER_LENGTH;
    data_header->frame_header.frame_length = 0;
    data_header->frame_header.version = AERON_FRAME_HEADER_VERSION;
    data_header->frame_header.flags = AERON_DATA_HEADER_BEGIN_FLAG | AERON_DATA_HEADER_END_FLAG;
    data_header->frame_header.type = AERON_HDR_TYPE_DATA;
    data_header->stream_id = stream_id;
    data_header->session_id = session_id;
    data_header->term_id = initial_term_id;
    data_header->term_offset = 0;
    data_header->reserved_value = AERON_DATA_HEADER_DEFAULT_RESERVED_VALUE;
}

/*
 * Does NOT initialize the following fields:
 * - term_tail_counters
 * - active_term_count
 */
inline void aeron_logbuffer_metadata_init(
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
    uint8_t type)
{
    aeron_logbuffer_metadata_t *log_meta_data = (aeron_logbuffer_metadata_t *)log_meta_data_buffer;

    log_meta_data->end_of_stream_position = end_of_stream_position;
    log_meta_data->is_connected = is_connected;
    log_meta_data->active_transport_count = active_transport_count;

    log_meta_data->correlation_id = correlation_id;
    log_meta_data->initial_term_id = initial_term_id;
    log_meta_data->mtu_length = mtu_length;
    log_meta_data->term_length = term_length;
    log_meta_data->page_size = page_size;

    log_meta_data->publication_window_length = publication_window_length;
    log_meta_data->receiver_window_length = receiver_window_length;
    log_meta_data->socket_sndbuf_length = socket_sndbuf_length;
    log_meta_data->os_default_socket_sndbuf_length = os_default_socket_sndbuf_length;
    log_meta_data->os_max_socket_sndbuf_length = os_max_socket_sndbuf_length;
    log_meta_data->socket_rcvbuf_length = socket_rcvbuf_length;
    log_meta_data->os_default_socket_rcvbuf_length = os_default_socket_rcvbuf_length;
    log_meta_data->os_max_socket_rcvbuf_length = os_max_socket_rcvbuf_length;
    log_meta_data->max_resend = max_resend;

    aeron_logbuffer_fill_default_header(log_meta_data_buffer, session_id, stream_id, initial_term_id);

    log_meta_data->entity_tag = entity_tag;
    log_meta_data->response_correlation_id = response_correlation_id;
    log_meta_data->linger_timeout_ns = linger_timeout_ns;
    log_meta_data->untethered_window_limit_timeout_ns = untethered_window_limit_timeout_ns;
    log_meta_data->untethered_linger_timeout_ns = untethered_linger_timeout_ns;
    log_meta_data->untethered_resting_timeout_ns = untethered_resting_timeout_ns;
    log_meta_data->group = group;
    log_meta_data->is_response = is_response;
    log_meta_data->rejoin = rejoin;
    log_meta_data->reliable = reliable;
    log_meta_data->sparse = sparse;
    log_meta_data->signal_eos = signal_eos;
    log_meta_data->spies_simulate_connection = spies_simulate_connection;
    log_meta_data->tether = tether;
    log_meta_data->is_publication_revoked = (uint8_t)false;
    log_meta_data->type = type;
}

inline void aeron_logbuffer_apply_default_header(uint8_t *log_meta_data_buffer, uint8_t *buffer)
{
    aeron_logbuffer_metadata_t *log_meta_data = (aeron_logbuffer_metadata_t *)log_meta_data_buffer;

    memcpy(buffer, log_meta_data->default_header, (size_t)log_meta_data->default_frame_header_length);
}

inline size_t aeron_logbuffer_compute_fragmented_length(size_t length, size_t max_payload_length)
{
    const size_t num_max_payloads = length / max_payload_length;
    const size_t remaining_payload = length % max_payload_length;
    const size_t last_frame_length = (remaining_payload > 0) ?
        AERON_ALIGN(remaining_payload + AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT) : 0;

    return (num_max_payloads * (max_payload_length + AERON_DATA_HEADER_LENGTH)) + last_frame_length;
}

#endif //AERON_LOGBUFFER_DESCRIPTOR_H
