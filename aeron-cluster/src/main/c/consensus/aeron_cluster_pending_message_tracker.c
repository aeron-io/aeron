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
#include <stdint.h>
#include "aeron_cluster_pending_message_tracker.h"

/* Default ring-buffer capacity when pending_message_capacity is 0. */
#define PMT_DEFAULT_CAPACITY 1024

static inline void pmt_write_int64_le(uint8_t *buf, int offset, int64_t val)
{
    memcpy(buf + offset, &val, 8);
}

static inline int64_t pmt_read_int64_le(const uint8_t *buf, int offset)
{
    int64_t val;
    memcpy(&val, buf + offset, 8);
    return val;
}

int aeron_cluster_pending_message_tracker_init(
    aeron_cluster_pending_message_tracker_t *tracker,
    int32_t service_id,
    int64_t next_service_session_id,
    int64_t log_service_session_id,
    int64_t pending_message_capacity)
{
    tracker->service_id                 = service_id;
    tracker->next_service_session_id    = next_service_session_id;
    tracker->log_service_session_id     = log_service_session_id;
    tracker->pending_message_capacity   = pending_message_capacity;
    tracker->uncommitted_messages       = 0;
    tracker->pending_message_head_offset = 0;

    int init_cap = (pending_message_capacity > 0) ? (int)pending_message_capacity : 0;
    return aeron_expandable_ring_buffer_init(
        &tracker->pending_messages, init_cap, AERON_ERB_MAX_CAPACITY);
}

void aeron_cluster_pending_message_tracker_close(
    aeron_cluster_pending_message_tracker_t *tracker)
{
    aeron_expandable_ring_buffer_close(&tracker->pending_messages);
}

bool aeron_cluster_pending_message_tracker_should_append(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t cluster_session_id)
{
    return cluster_session_id > tracker->log_service_session_id;
}

void aeron_cluster_pending_message_tracker_on_appended(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t cluster_session_id)
{
    if (cluster_session_id >= tracker->next_service_session_id)
    {
        tracker->next_service_session_id = cluster_session_id + 1;
    }
}

void aeron_cluster_pending_message_tracker_on_committed(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t cluster_session_id)
{
    if (cluster_session_id > tracker->log_service_session_id)
    {
        tracker->log_service_session_id = cluster_session_id;
    }
}

void aeron_cluster_pending_message_tracker_load_state(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t next_service_session_id,
    int64_t log_service_session_id,
    int64_t pending_message_capacity)
{
    tracker->next_service_session_id  = next_service_session_id;
    tracker->log_service_session_id   = log_service_session_id;
    tracker->pending_message_capacity = pending_message_capacity;

    int cap = (pending_message_capacity > 0) ? (int)pending_message_capacity : 0;
    aeron_expandable_ring_buffer_reset(&tracker->pending_messages, cap);
    tracker->uncommitted_messages        = 0;
    tracker->pending_message_head_offset = 0;
}

/* -----------------------------------------------------------------------
 * enqueue_message
 * Mirrors Java PendingServiceMessageTracker.enqueueMessage().
 *
 * buffer[payload_offset - SESSION_HEADER_LENGTH .. payload_offset + payload_length - 1]
 * is the full SBE-encoded message ([session_header][payload]).
 * We assign the next service session ID, stamp it into the header, and
 * push the whole record into the ring buffer.
 * ----------------------------------------------------------------------- */
void aeron_cluster_pending_message_tracker_enqueue_message(
    aeron_cluster_pending_message_tracker_t *tracker,
    uint8_t *buffer,
    int      payload_offset,
    int      payload_length)
{
    int64_t cluster_session_id = tracker->next_service_session_id++;

    if (cluster_session_id > tracker->log_service_session_id)
    {
        /* Stamp session ID and set timestamp sentinel into the session header */
        int hdr_start = payload_offset - AERON_CLUSTER_SESSION_HEADER_LENGTH;
        pmt_write_int64_le(buffer, hdr_start + AERON_PMT_CLUSTER_SESSION_ID_OFFSET, cluster_session_id);
        pmt_write_int64_le(buffer, hdr_start + AERON_PMT_TIMESTAMP_OFFSET, AERON_PMT_NOT_APPENDED);

        /* Append [session_header][payload] to ring buffer */
        aeron_expandable_ring_buffer_append(
            &tracker->pending_messages,
            buffer,
            hdr_start,
            AERON_CLUSTER_SESSION_HEADER_LENGTH + payload_length);
    }
}

/* -----------------------------------------------------------------------
 * poll — messageAppender context passed to forEach_from
 * ----------------------------------------------------------------------- */
typedef struct
{
    aeron_cluster_pending_message_tracker_t *tracker;
    int64_t  leadership_term_id;
    int64_t  timestamp;
    void    *clientd;
    aeron_pending_message_append_fn_t append_fn;
    int      appended_count;
} pmt_poll_ctx_t;

static bool pmt_message_appender(
    void    *clientd,
    uint8_t *buffer,
    int      offset,
    int      length,
    int      head_offset)
{
    pmt_poll_ctx_t *ctx = (pmt_poll_ctx_t *)clientd;
    aeron_cluster_pending_message_tracker_t *tracker = ctx->tracker;

    /* clusterSessionId is at offset 16 into the stored message payload */
    int64_t cluster_session_id = pmt_read_int64_le(buffer, offset + AERON_PMT_CLUSTER_SESSION_ID_OFFSET);

    /* Application payload starts after the 32-byte session header */
    int payload_offset  = offset + AERON_CLUSTER_SESSION_HEADER_LENGTH;
    int payload_length  = length - AERON_CLUSTER_SESSION_HEADER_LENGTH;

    int64_t append_pos = ctx->append_fn(
        ctx->clientd,
        ctx->leadership_term_id,
        cluster_session_id,
        ctx->timestamp,
        buffer,
        payload_offset,
        payload_length);

    if (append_pos > 0)
    {
        tracker->uncommitted_messages++;
        tracker->pending_message_head_offset = head_offset;
        /* Store append position in the timestamp field (used by sweep_leader) */
        pmt_write_int64_le(buffer, offset + AERON_PMT_TIMESTAMP_OFFSET, append_pos);
        ctx->appended_count++;
        return true;
    }

    return false; /* back-pressure; stop iterating */
}

int aeron_cluster_pending_message_tracker_poll(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t leadership_term_id,
    int64_t timestamp,
    void   *clientd,
    aeron_pending_message_append_fn_t append_fn)
{
    if (aeron_expandable_ring_buffer_is_empty(&tracker->pending_messages))
    {
        return 0;
    }

    pmt_poll_ctx_t ctx = {
        .tracker             = tracker,
        .leadership_term_id  = leadership_term_id,
        .timestamp           = timestamp,
        .clientd             = clientd,
        .append_fn           = append_fn,
        .appended_count      = 0,
    };

    aeron_expandable_ring_buffer_for_each_from(
        &tracker->pending_messages,
        tracker->pending_message_head_offset,
        &ctx,
        pmt_message_appender,
        AERON_PENDING_MESSAGE_SERVICE_LIMIT);

    return ctx.appended_count;
}

/* -----------------------------------------------------------------------
 * sweep_leader_messages — leaderMessageSweeper
 * Consume messages whose append position (stored in timestamp field) has
 * been committed (commit_position >= append_position).
 * ----------------------------------------------------------------------- */
typedef struct { int64_t commit_position; int64_t *log_session_id; int *uncommitted; } pmt_leader_sweep_ctx_t;

static bool pmt_leader_message_sweeper(
    void    *clientd,
    uint8_t *buffer,
    int      offset,
    int      length,
    int      head_offset)
{
    (void)length; (void)head_offset;
    pmt_leader_sweep_ctx_t *ctx = (pmt_leader_sweep_ctx_t *)clientd;

    int64_t append_pos = pmt_read_int64_le(buffer, offset + AERON_PMT_TIMESTAMP_OFFSET);

    if (ctx->commit_position >= append_pos)
    {
        *ctx->log_session_id = pmt_read_int64_le(buffer, offset + AERON_PMT_CLUSTER_SESSION_ID_OFFSET);
        (*ctx->uncommitted)--;
        return true; /* consume */
    }

    return false; /* stop */
}

void aeron_cluster_pending_message_tracker_sweep_leader_messages(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t commit_position)
{
    if (tracker->uncommitted_messages <= 0)
    {
        return;
    }

    pmt_leader_sweep_ctx_t ctx = {
        .commit_position = commit_position,
        .log_session_id  = &tracker->log_service_session_id,
        .uncommitted     = &tracker->uncommitted_messages,
    };

    int bytes_consumed = aeron_expandable_ring_buffer_consume(
        &tracker->pending_messages, &ctx, pmt_leader_message_sweeper, INT32_MAX);

    /* Shift pending_message_head_offset by how much was consumed */
    tracker->pending_message_head_offset -= bytes_consumed;
    if (tracker->pending_message_head_offset < 0)
    {
        tracker->pending_message_head_offset = 0;
    }
}

/* -----------------------------------------------------------------------
 * sweep_follower_messages — followerMessageSweeper
 * Consume messages whose clusterSessionId <= the given value.
 * ----------------------------------------------------------------------- */
typedef struct { int64_t log_session_id; } pmt_follower_sweep_ctx_t;

static bool pmt_follower_message_sweeper(
    void    *clientd,
    uint8_t *buffer,
    int      offset,
    int      length,
    int      head_offset)
{
    (void)length; (void)head_offset;
    pmt_follower_sweep_ctx_t *ctx = (pmt_follower_sweep_ctx_t *)clientd;
    int64_t session_id = pmt_read_int64_le(buffer, offset + AERON_PMT_CLUSTER_SESSION_ID_OFFSET);
    return session_id <= ctx->log_session_id;
}

void aeron_cluster_pending_message_tracker_sweep_follower_messages(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t cluster_session_id)
{
    tracker->log_service_session_id = cluster_session_id;

    pmt_follower_sweep_ctx_t ctx = { .log_session_id = cluster_session_id };
    aeron_expandable_ring_buffer_consume(
        &tracker->pending_messages, &ctx, pmt_follower_message_sweeper, INT32_MAX);
}

/* -----------------------------------------------------------------------
 * restore_uncommitted_messages — messageReset sweep + clear counters
 * Reset the timestamp field back to NOT_APPENDED for all uncommitted messages,
 * then clear uncommitted_messages and pending_message_head_offset.
 * ----------------------------------------------------------------------- */
static bool pmt_message_reset(
    void    *clientd,
    uint8_t *buffer,
    int      offset,
    int      length,
    int      head_offset)
{
    (void)clientd; (void)length; (void)head_offset;
    int64_t append_pos = pmt_read_int64_le(buffer, offset + AERON_PMT_TIMESTAMP_OFFSET);
    if (append_pos != AERON_PMT_NOT_APPENDED)
    {
        pmt_write_int64_le(buffer, offset + AERON_PMT_TIMESTAMP_OFFSET, AERON_PMT_NOT_APPENDED);
    }
    return true;
}

void aeron_cluster_pending_message_tracker_restore_uncommitted_messages(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t commit_position)
{
    if (tracker->uncommitted_messages > 0)
    {
        pmt_leader_sweep_ctx_t sweep_ctx = {
            .commit_position = commit_position,
            .log_session_id  = &tracker->log_service_session_id,
            .uncommitted     = &tracker->uncommitted_messages,
        };

        aeron_expandable_ring_buffer_consume(
            &tracker->pending_messages, &sweep_ctx, pmt_leader_message_sweeper, INT32_MAX);

        aeron_expandable_ring_buffer_for_each(
            &tracker->pending_messages, NULL, pmt_message_reset, INT32_MAX);

        tracker->uncommitted_messages        = 0;
        tracker->pending_message_head_offset = 0;
    }
}

/* -----------------------------------------------------------------------
 * append_message — direct ring-buffer append (used in snapshot loading)
 * ----------------------------------------------------------------------- */
void aeron_cluster_pending_message_tracker_append_message(
    aeron_cluster_pending_message_tracker_t *tracker,
    const uint8_t *buffer,
    int            offset,
    int            total_length)
{
    aeron_expandable_ring_buffer_append(
        &tracker->pending_messages, buffer, offset, total_length);
}

/* -----------------------------------------------------------------------
 * reset — reset timestamp fields without consuming
 * ----------------------------------------------------------------------- */
void aeron_cluster_pending_message_tracker_reset(
    aeron_cluster_pending_message_tracker_t *tracker)
{
    aeron_expandable_ring_buffer_for_each(
        &tracker->pending_messages, NULL, pmt_message_reset, INT32_MAX);
}

/* -----------------------------------------------------------------------
 * size — return byte size of pending messages
 * ----------------------------------------------------------------------- */
int aeron_cluster_pending_message_tracker_size(
    aeron_cluster_pending_message_tracker_t *tracker)
{
    return aeron_expandable_ring_buffer_size(&tracker->pending_messages);
}

/* -----------------------------------------------------------------------
 * verify — validate pending message consistency
 * ----------------------------------------------------------------------- */
typedef struct
{
    aeron_cluster_pending_message_tracker_t *tracker;
    int message_count;
    int result;
} pmt_verify_ctx_t;

static bool pmt_verify_consumer(
    void    *clientd,
    uint8_t *buffer,
    int      offset,
    int      length,
    int      head_offset)
{
    (void)length; (void)head_offset;
    pmt_verify_ctx_t *ctx = (pmt_verify_ctx_t *)clientd;

    ctx->message_count++;

    int64_t cluster_session_id = pmt_read_int64_le(
        buffer, offset + AERON_PMT_CLUSTER_SESSION_ID_OFFSET);

    if (cluster_session_id != (ctx->tracker->log_service_session_id + ctx->message_count))
    {
        ctx->result = -1;
        return false;
    }

    return true;
}

int aeron_cluster_pending_message_tracker_verify(
    aeron_cluster_pending_message_tracker_t *tracker)
{
    pmt_verify_ctx_t ctx = {
        .tracker = tracker,
        .message_count = 0,
        .result = 0,
    };

    aeron_expandable_ring_buffer_for_each(
        &tracker->pending_messages, &ctx, pmt_verify_consumer, INT32_MAX);

    if (ctx.result != 0)
    {
        return -1;
    }

    if (tracker->next_service_session_id != (tracker->log_service_session_id + ctx.message_count + 1))
    {
        return -1;
    }

    return 0;
}
