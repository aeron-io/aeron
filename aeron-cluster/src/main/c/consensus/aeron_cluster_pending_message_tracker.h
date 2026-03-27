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

#ifndef AERON_CLUSTER_PENDING_MESSAGE_TRACKER_H
#define AERON_CLUSTER_PENDING_MESSAGE_TRACKER_H

#include <stdint.h>
#include <stdbool.h>
#include "aeronc.h"
#include "aeron_expandable_ring_buffer.h"

/* SBE MessageHeader (8) + SessionMessageHeader block (24) = 32 bytes */
#ifndef AERON_CLUSTER_SESSION_HEADER_LENGTH
#define AERON_CLUSTER_SESSION_HEADER_LENGTH 32
#endif

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Tracks pending messages from a clustered service to ensure exactly-once
 * delivery across leader changes.  Mirrors Java PendingServiceMessageTracker.
 *
 * Each service has its own tracker identified by service_id.
 * Messages sent by the service via Cluster.offer() get a monotonically
 * increasing serviceSessionId.  On log replay (leader failover), messages
 * with serviceSessionId <= logServiceSessionId have already been committed
 * and must not be re-appended.
 *
 * The ring buffer stores: [SBE SessionMessageHeader (32 bytes)][application payload].
 * The clusterSessionId field (at ring-buffer-payload offset 16) holds the
 * assigned serviceSessionId; the timestamp field (offset 24) is used
 * temporarily to store the log append position once the message is committed.
 */

/* Maximum pending service messages polled per call to poll(). Mirrors Java. */
#define AERON_PENDING_MESSAGE_SERVICE_LIMIT 20

/* Byte offset within the stored message (after the ring-buffer header) at which
 * the clusterSessionId field of the SBE SessionMessageHeader lives.
 *   = MessageHeader.ENCODED_LENGTH(8) + SessionMessageHeader.clusterSessionId offset(8)
 */
#define AERON_PMT_CLUSTER_SESSION_ID_OFFSET  16

/* Byte offset within the stored message for the timestamp / append-position field.
 *   = MessageHeader.ENCODED_LENGTH(8) + SessionMessageHeader.timestamp offset(16)
 */
#define AERON_PMT_TIMESTAMP_OFFSET           24

/* Sentinel meaning "not yet appended to log" (stored in the timestamp field). */
#define AERON_PMT_NOT_APPENDED               INT64_MAX

/**
 * Callback used by poll() to append a pending service message to the log.
 *
 * @param clientd           opaque pointer (typically the log publisher)
 * @param leadership_term_id current leadership term
 * @param cluster_session_id service session ID of the message
 * @param timestamp          cluster clock timestamp (not nano time)
 * @param buffer             pointer to ring-buffer backing store
 * @param payload_offset     offset within buffer where the application payload begins
 *                           (i.e. past the 32-byte session header)
 * @param payload_length     application payload length in bytes
 * @return positive append position on success; <= 0 if back-pressured
 */
typedef int64_t (*aeron_pending_message_append_fn_t)(
    void       *clientd,
    int64_t     leadership_term_id,
    int64_t     cluster_session_id,
    int64_t     timestamp,
    uint8_t    *buffer,
    int         payload_offset,
    int         payload_length);

typedef struct aeron_cluster_pending_message_tracker_stct
{
    int32_t  service_id;
    int64_t  next_service_session_id;   /* next ID to assign to outgoing messages */
    int64_t  log_service_session_id;    /* highest committed session ID in the log */
    int64_t  pending_message_capacity;

    /* Ring buffer storing [session_header(32)][payload] for each pending message. */
    aeron_expandable_ring_buffer_t pending_messages;

    /* Number of messages appended to the log but not yet committed. */
    int uncommitted_messages;

    /*
     * Byte offset from ring-buffer head at which the NEXT poll() should start
     * iterating.  Advances as messages are successfully appended; reset to 0
     * after a leader-change sweep.
     */
    int pending_message_head_offset;
}
aeron_cluster_pending_message_tracker_t;

int  aeron_cluster_pending_message_tracker_init(
    aeron_cluster_pending_message_tracker_t *tracker,
    int32_t service_id,
    int64_t next_service_session_id,
    int64_t log_service_session_id,
    int64_t pending_message_capacity);

void aeron_cluster_pending_message_tracker_close(
    aeron_cluster_pending_message_tracker_t *tracker);

/** Returns true if this message should be appended (not already committed). */
bool aeron_cluster_pending_message_tracker_should_append(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t cluster_session_id);

/** Advance next_service_session_id after a message is appended. */
void aeron_cluster_pending_message_tracker_on_appended(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t cluster_session_id);

/** Called on log replay to update the committed high-water mark. */
void aeron_cluster_pending_message_tracker_on_committed(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t cluster_session_id);

/** Load state from a snapshot (resets the ring buffer to pendingMessageCapacity). */
void aeron_cluster_pending_message_tracker_load_state(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t next_service_session_id,
    int64_t log_service_session_id,
    int64_t pending_message_capacity);

/**
 * Enqueue a service message from the leader path.
 * `buffer` points at `payload_offset - AERON_CLUSTER_SESSION_HEADER_LENGTH`;
 * the function reads/writes the session header that precedes the payload.
 * Mirrors Java PendingServiceMessageTracker.enqueueMessage().
 *
 * @param buffer         raw byte buffer containing [session_header][payload]
 * @param payload_offset byte offset within buffer where the application payload begins
 * @param payload_length application payload length in bytes
 */
void aeron_cluster_pending_message_tracker_enqueue_message(
    aeron_cluster_pending_message_tracker_t *tracker,
    uint8_t *buffer,
    int      payload_offset,
    int      payload_length);

/**
 * Try to append pending messages to the log publication.
 * Iterates the ring buffer starting from pending_message_head_offset and calls
 * append_fn for each message. Stops after AERON_PENDING_MESSAGE_SERVICE_LIMIT
 * messages or when append_fn returns <= 0 (back-pressure).
 *
 * Mirrors Java PendingServiceMessageTracker.poll().
 *
 * @return number of messages appended.
 */
int aeron_cluster_pending_message_tracker_poll(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t leadership_term_id,
    int64_t timestamp,
    void   *clientd,
    aeron_pending_message_append_fn_t append_fn);

/**
 * Consume committed messages from the ring buffer (leader path).
 * A message is committed once commit_position >= its stored append position
 * (written into the timestamp field by poll()).
 * Mirrors Java PendingServiceMessageTracker.sweepLeaderMessages().
 */
void aeron_cluster_pending_message_tracker_sweep_leader_messages(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t commit_position);

/**
 * Consume messages from the ring buffer that have already been replayed
 * (follower / replay path). Consumes all messages whose clusterSessionId
 * <= cluster_session_id and updates log_service_session_id.
 * Mirrors Java PendingServiceMessageTracker.sweepFollowerMessages().
 */
void aeron_cluster_pending_message_tracker_sweep_follower_messages(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t cluster_session_id);

/**
 * Reset the timestamp field of uncommitted messages back to AERON_PMT_NOT_APPENDED,
 * then clear uncommitted_messages and pending_message_head_offset.
 * Called when the leader steps down.
 * Mirrors Java PendingServiceMessageTracker.restoreUncommittedMessages().
 */
void aeron_cluster_pending_message_tracker_restore_uncommitted_messages(
    aeron_cluster_pending_message_tracker_t *tracker);

/**
 * Directly append a raw message (session_header + payload) to the ring buffer.
 * Used during snapshot loading. Unlike enqueue_message, this does NOT assign a
 * new service session ID — the session header is already fully encoded.
 * Mirrors Java PendingServiceMessageTracker.appendMessage().
 *
 * @param buffer         raw byte buffer
 * @param offset         byte offset of the first byte of [session_header][payload]
 * @param total_length   total length (session_header + payload) in bytes
 */
void aeron_cluster_pending_message_tracker_append_message(
    aeron_cluster_pending_message_tracker_t *tracker,
    const uint8_t *buffer,
    int            offset,
    int            total_length);

/* -----------------------------------------------------------------------
 * Static helpers — mirrors Java static methods
 * ----------------------------------------------------------------------- */

/**
 * Extract the serviceId from a log-message cluster session ID.
 * Java: (int)(clusterSessionId >>> 56) & 0x7F
 */
static inline int aeron_pending_message_tracker_service_id_from_log_message(int64_t cluster_session_id)
{
    return (int)((uint64_t)cluster_session_id >> 56) & 0x7F;
}

/**
 * Extract the serviceId from a service-side inter-service message.
 * Java: (int)clusterSessionId
 */
static inline int aeron_pending_message_tracker_service_id_from_service_message(int64_t cluster_session_id)
{
    return (int)cluster_session_id;
}

/**
 * Build the service session ID from serviceId and a local session counter.
 * Java: (long)serviceId << 56 | sessionId
 */
static inline int64_t aeron_pending_message_tracker_service_session_id(int32_t service_id, int64_t session_id)
{
    return ((int64_t)service_id << 56) | (session_id & 0x00FFFFFFFFFFFFFFLL);
}

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_PENDING_MESSAGE_TRACKER_H */
