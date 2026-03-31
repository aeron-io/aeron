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

#ifndef AERON_ARCHIVE_REPLICATION_SESSION_H
#define AERON_ARCHIVE_REPLICATION_SESSION_H

#include <stdint.h>
#include <stdbool.h>

#include "aeronc.h"
#include "aeron_archive_catalog.h"
#include "aeron_archive_conductor.h"
#include "aeron_archive_control_session.h"

#ifdef __cplusplus
extern "C"
{
#endif

/* Forward declarations for the archive client types. */
typedef struct aeron_archive_stct aeron_archive_t;
typedef struct aeron_archive_context_stct aeron_archive_context_t;
typedef struct aeron_archive_async_connect_stct aeron_archive_async_connect_t;

#define AERON_ARCHIVE_REPLICATION_REPLAY_REMOVE_THRESHOLD (0)
#define AERON_ARCHIVE_REPLICATION_RETRY_ATTEMPTS (3)
#define AERON_ARCHIVE_REPLICATION_SOURCE_POLL_INTERVAL_MS (100)
#define AERON_ARCHIVE_REPLICATION_LIVE_ADD_MAX_WINDOW (65536)

/**
 * Replication session states matching the Java ReplicationSession.State enum.
 */
typedef enum aeron_archive_replication_session_state_en
{
    AERON_ARCHIVE_REPLICATION_STATE_CONNECT = 0,
    AERON_ARCHIVE_REPLICATION_STATE_REPLICATE_DESCRIPTOR = 1,
    AERON_ARCHIVE_REPLICATION_STATE_SRC_RECORDING_POSITION = 2,
    AERON_ARCHIVE_REPLICATION_STATE_EXTEND = 3,
    AERON_ARCHIVE_REPLICATION_STATE_REPLAY_TOKEN = 4,
    AERON_ARCHIVE_REPLICATION_STATE_GET_ARCHIVE_PROXY = 5,
    AERON_ARCHIVE_REPLICATION_STATE_REPLAY = 6,
    AERON_ARCHIVE_REPLICATION_STATE_AWAIT_IMAGE = 7,
    AERON_ARCHIVE_REPLICATION_STATE_REPLICATE = 8,
    AERON_ARCHIVE_REPLICATION_STATE_CATCHUP = 9,
    AERON_ARCHIVE_REPLICATION_STATE_ATTEMPT_LIVE_JOIN = 10,
    AERON_ARCHIVE_REPLICATION_STATE_DONE = 11
}
aeron_archive_replication_session_state_t;

/**
 * A replication session that replicates a recording from a source archive to the
 * local archive. Optionally performs a live merge if a live destination is specified.
 *
 * Corresponds to the Java ReplicationSession class.
 *
 * State machine:
 *   CONNECT -> REPLICATE_DESCRIPTOR -> [SRC_RECORDING_POSITION] -> EXTEND ->
 *   REPLAY_TOKEN -> GET_ARCHIVE_PROXY -> REPLAY -> AWAIT_IMAGE ->
 *   REPLICATE (no live) / CATCHUP -> ATTEMPT_LIVE_JOIN -> DONE
 */
typedef struct aeron_archive_replication_session_stct
{
    /* Identifiers */
    int64_t replication_id;
    int64_t src_recording_id;
    int64_t dst_recording_id;
    int64_t channel_tag_id;
    int64_t subscription_tag_id;
    int32_t replication_session_id;

    /* Positions */
    int64_t replay_position;
    int64_t src_stop_position;
    int64_t src_recording_position;
    int64_t dst_stop_position;

    /* Correlation tracking */
    int64_t active_correlation_id;
    int64_t src_replay_session_id;
    int64_t replay_token;
    int64_t response_publication_registration_id;

    /* Timing */
    int64_t time_of_last_action_ms;
    int64_t action_timeout_ms;
    int64_t time_of_last_scheduled_src_poll_ms;

    /* Stream identifiers derived from source descriptor */
    int32_t replay_stream_id;
    int32_t replay_session_id;
    int32_t file_io_max_length;

    /* Retry tracking for live join */
    int32_t retry_attempts;

    /* Boolean flags */
    bool is_live_added;
    bool is_tagged;
    bool is_destination_recording_empty;

    /* State machine */
    aeron_archive_replication_session_state_t state;

    /* Channel strings (owned, heap-allocated) */
    char *replication_channel;
    char *live_destination;
    char *replay_destination;

    /* Aeron resources */
    aeron_t *aeron;
    aeron_archive_context_t *src_archive_ctx;
    aeron_archive_async_connect_t *async_connect;
    aeron_archive_t *src_archive;
    aeron_subscription_t *recording_subscription;
    aeron_image_t *image;
    aeron_exclusive_publication_t *response_publication;

    /* Server-side references (not owned) */
    aeron_archive_conductor_t *conductor;
    aeron_archive_control_session_t *control_session;
    aeron_archive_catalog_t *catalog;

    /* Error reporting */
    char *error_message;
}
aeron_archive_replication_session_t;

/**
 * Create and initialise a replication session.
 *
 * @param session                out param for the allocated session.
 * @param src_recording_id       the recording id on the source archive.
 * @param dst_recording_id       the destination recording id (AERON_NULL_VALUE for new).
 * @param channel_tag_id         the channel tag id (AERON_NULL_VALUE for default).
 * @param subscription_tag_id    the subscription tag id (AERON_NULL_VALUE for default).
 * @param replication_id         the unique replication id.
 * @param stop_position          the position to stop replication (AERON_NULL_POSITION for open-ended).
 * @param live_destination       the live destination for merge (NULL for no merge).
 * @param replication_channel    the channel for the replication subscription.
 * @param file_io_max_length     the maximum file I/O length.
 * @param replication_session_id the session id override for the replicated recording.
 * @param recording_summary      optional recording summary for existing destination recording (may be NULL).
 * @param src_archive_ctx        the archive client context for connecting to the source (will be duplicated).
 * @param cached_epoch_clock_ms  the current cached epoch time in milliseconds.
 * @param catalog                the local archive catalog.
 * @param control_session        the control session originating the request.
 * @param conductor              the archive conductor.
 * @param aeron                  the local Aeron client.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_replication_session_create(
    aeron_archive_replication_session_t **session,
    int64_t src_recording_id,
    int64_t dst_recording_id,
    int64_t channel_tag_id,
    int64_t subscription_tag_id,
    int64_t replication_id,
    int64_t stop_position,
    const char *live_destination,
    const char *replication_channel,
    int32_t file_io_max_length,
    int32_t replication_session_id,
    const aeron_archive_recording_summary_t *recording_summary,
    aeron_archive_context_t *src_archive_ctx,
    int64_t cached_epoch_clock_ms,
    aeron_archive_catalog_t *catalog,
    aeron_archive_control_session_t *control_session,
    aeron_archive_conductor_t *conductor,
    aeron_t *aeron);

/**
 * Perform a unit of work: drive the replication state machine.
 *
 * @param session              the replication session.
 * @param cached_epoch_clock_ms the current cached epoch time in milliseconds.
 * @return the amount of work done (0 if idle), or -1 on terminal error.
 */
int aeron_archive_replication_session_do_work(
    aeron_archive_replication_session_t *session,
    int64_t cached_epoch_clock_ms);

/**
 * Close the replication session and release all resources.
 *
 * @param session the replication session. May be NULL.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_replication_session_close(aeron_archive_replication_session_t *session);

/**
 * Abort the replication session.
 *
 * @param session the replication session.
 * @param reason  descriptive reason for the abort.
 */
void aeron_archive_replication_session_abort(
    aeron_archive_replication_session_t *session,
    const char *reason);

/**
 * Check if the replication session has reached the DONE state.
 *
 * @param session the replication session.
 * @return true if the session is done.
 */
bool aeron_archive_replication_session_is_done(const aeron_archive_replication_session_t *session);

/**
 * Get the session id (replication_id) for this replication session.
 *
 * @param session the replication session.
 * @return the session id.
 */
int64_t aeron_archive_replication_session_id(const aeron_archive_replication_session_t *session);

/**
 * Get the current replication state.
 *
 * @param session the replication session.
 * @return the current state.
 */
aeron_archive_replication_session_state_t aeron_archive_replication_session_state(
    const aeron_archive_replication_session_t *session);

/**
 * Get the destination recording id.
 *
 * @param session the replication session.
 * @return the destination recording id.
 */
int64_t aeron_archive_replication_session_dst_recording_id(
    const aeron_archive_replication_session_t *session);

/**
 * Get the source recording id.
 *
 * @param session the replication session.
 * @return the source recording id.
 */
int64_t aeron_archive_replication_session_src_recording_id(
    const aeron_archive_replication_session_t *session);

/**
 * Get the error message if the session failed, or NULL if no error.
 *
 * @param session the replication session.
 * @return the error message string, or NULL.
 */
const char *aeron_archive_replication_session_error_message(
    const aeron_archive_replication_session_t *session);

#ifdef __cplusplus
}
#endif

#endif /* AERON_ARCHIVE_REPLICATION_SESSION_H */
