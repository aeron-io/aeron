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

#ifndef AERON_ARCHIVE_CONDUCTOR_H
#define AERON_ARCHIVE_CONDUCTOR_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "aeronc.h"
#include "aeron_archive_catalog.h"
#include "aeron_archive_recording_session.h"
#include "aeron_archive_recording_events_proxy.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define AERON_ARCHIVE_CONDUCTOR_MAX_SESSIONS (1024)
#define AERON_ARCHIVE_CONDUCTOR_MAX_SUBSCRIPTIONS (256)
#define AERON_ARCHIVE_CONDUCTOR_MARK_FILE_UPDATE_INTERVAL_MS (1000)

#define AERON_ARCHIVE_NULL_VALUE (-1LL)
#define AERON_ARCHIVE_NULL_POSITION (-1LL)

/**
 * Archive error codes matching the Java ArchiveException constants.
 */
#define AERON_ARCHIVE_ERROR_GENERIC (0)
#define AERON_ARCHIVE_ERROR_ACTIVE_LISTING (1)
#define AERON_ARCHIVE_ERROR_ACTIVE_RECORDING (2)
#define AERON_ARCHIVE_ERROR_ACTIVE_SUBSCRIPTION (3)
#define AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING (4)
#define AERON_ARCHIVE_ERROR_UNKNOWN_SUBSCRIPTION (5)
#define AERON_ARCHIVE_ERROR_UNKNOWN_REPLICATION (6)
#define AERON_ARCHIVE_ERROR_MAX_RECORDINGS (7)
#define AERON_ARCHIVE_ERROR_MAX_REPLAYS (8)
#define AERON_ARCHIVE_ERROR_STORAGE_SPACE (9)
#define AERON_ARCHIVE_ERROR_EMPTY_RECORDING (10)
#define AERON_ARCHIVE_ERROR_INVALID_EXTENSION (11)

/**
 * Source location enum matching Java SourceLocation.
 * Guarded to avoid conflict when both client and server headers are included.
 */
#ifndef AERON_ARCHIVE_SOURCE_LOCATION_DEFINED
#define AERON_ARCHIVE_SOURCE_LOCATION_DEFINED
typedef enum aeron_archive_source_location_en
{
    AERON_ARCHIVE_SOURCE_LOCATION_LOCAL = 0,
    AERON_ARCHIVE_SOURCE_LOCATION_REMOTE = 1
}
aeron_archive_source_location_t;
#endif

/**
 * Recording signal code enum matching Java RecordingSignal.
 * Named aeron_archive_recording_signal_code_t to avoid conflict with the
 * client-side aeron_archive_recording_signal_t struct (which is a signal event).
 */
typedef enum aeron_archive_recording_signal_code_en
{
    AERON_ARCHIVE_RECORDING_SIGNAL_CODE_START = 0,
    AERON_ARCHIVE_RECORDING_SIGNAL_CODE_STOP = 1,
    AERON_ARCHIVE_RECORDING_SIGNAL_CODE_EXTEND = 2,
    AERON_ARCHIVE_RECORDING_SIGNAL_CODE_REPLICATE = 3,
    AERON_ARCHIVE_RECORDING_SIGNAL_CODE_MERGE = 4,
    AERON_ARCHIVE_RECORDING_SIGNAL_CODE_SYNC = 5,
    AERON_ARCHIVE_RECORDING_SIGNAL_CODE_DELETE = 6,
    AERON_ARCHIVE_RECORDING_SIGNAL_CODE_REPLICATE_END = 7
}
aeron_archive_recording_signal_code_t;

/*
 * Forward declarations for session types implemented separately.
 */
typedef struct aeron_archive_replay_session_stct aeron_archive_replay_session_t;
typedef struct aeron_archive_replication_session_stct aeron_archive_replication_session_t;
typedef struct aeron_archive_control_session_stct aeron_archive_control_session_t;

/**
 * Entry in the recording session map: recording_id -> session pointer.
 */
typedef struct aeron_archive_recording_session_entry_stct
{
    int64_t recording_id;
    aeron_archive_recording_session_t *session;
}
aeron_archive_recording_session_entry_t;

/**
 * Entry in the replay session map: replay_session_id -> session pointer.
 */
typedef struct aeron_archive_replay_session_entry_stct
{
    int64_t replay_session_id;
    aeron_archive_replay_session_t *session;
}
aeron_archive_replay_session_entry_t;

/**
 * Entry in the replication session map: replication_id -> session pointer.
 */
typedef struct aeron_archive_replication_session_entry_stct
{
    int64_t replication_id;
    aeron_archive_replication_session_t *session;
}
aeron_archive_replication_session_entry_t;

/**
 * Entry in the recording subscription map: key string -> subscription.
 */
typedef struct aeron_archive_recording_subscription_entry_stct
{
    char *key;
    aeron_subscription_t *subscription;
    int64_t ref_count;
}
aeron_archive_recording_subscription_entry_t;

/**
 * Pending task in the task queue.
 */
typedef void (*aeron_archive_conductor_task_func_t)(void *clientd);

typedef struct aeron_archive_conductor_task_stct
{
    aeron_archive_conductor_task_func_t func;
    void *clientd;
    struct aeron_archive_conductor_task_stct *next;
}
aeron_archive_conductor_task_t;

/**
 * Context configuration for the archive conductor.
 */
typedef struct aeron_archive_conductor_context_stct
{
    aeron_t *aeron;
    aeron_archive_catalog_t *catalog;
    aeron_archive_recording_events_proxy_t *recording_events_proxy;

    const char *archive_dir;
    const char *control_channel;
    int32_t control_stream_id;
    const char *local_control_channel;
    int32_t local_control_stream_id;
    const char *recording_events_channel;
    int32_t recording_events_stream_id;
    const char *replication_channel;

    int32_t segment_file_length;
    int32_t file_io_max_length;
    int32_t max_concurrent_recordings;
    int32_t max_concurrent_replays;

    int64_t connect_timeout_ms;
    int64_t session_liveness_check_interval_ms;
    int64_t replay_linger_timeout_ns;
    int64_t low_storage_space_threshold;
    int64_t archive_id;

    bool control_channel_enabled;
    bool recording_events_enabled;
    bool control_term_buffer_sparse;
    int32_t control_mtu_length;
    int32_t control_term_buffer_length;

    bool force_writes;
    bool force_metadata;
}
aeron_archive_conductor_context_t;

/**
 * Recording summary used for temporary storage during request handling.
 */
typedef struct aeron_archive_recording_summary_stct
{
    int64_t recording_id;
    int64_t start_position;
    int64_t stop_position;
    int64_t start_timestamp;
    int64_t stop_timestamp;
    int32_t initial_term_id;
    int32_t segment_file_length;
    int32_t term_buffer_length;
    int32_t mtu_length;
    int32_t session_id;
    int32_t stream_id;
}
aeron_archive_recording_summary_t;

/* Forward-declare types from other server headers to avoid circular includes. */
typedef struct aeron_archive_control_session_adapter_stct aeron_archive_control_session_adapter_t;
typedef struct aeron_archive_control_response_proxy_stct aeron_archive_control_response_proxy_t;

/**
 * Entry in the control session list managed by the conductor.
 */
typedef struct aeron_archive_control_session_entry_stct
{
    aeron_archive_control_session_t *session;
    struct aeron_archive_control_session_entry_stct *next;
}
aeron_archive_control_session_entry_t;

/**
 * The main Archive Conductor that drives all archive operations.
 *
 * Accepts control requests from clients, manages recording/replay/replication sessions,
 * updates the catalog, and handles control subscriptions and publications.
 */
typedef struct aeron_archive_conductor_stct
{
    aeron_archive_conductor_context_t *ctx;

    /* Aeron client resources */
    aeron_t *aeron;
    aeron_subscription_t *control_subscription;
    aeron_subscription_t *local_control_subscription;

    /* Async handles for subscription creation (polled until resolved) */
    aeron_async_add_subscription_t *control_subscription_async;
    aeron_async_add_subscription_t *local_control_subscription_async;

    /* Control session adapter - decodes incoming SBE requests */
    aeron_archive_control_session_adapter_t *control_session_adapter;

    /* Control response proxy - encodes outgoing SBE responses */
    aeron_archive_control_response_proxy_t *control_response_proxy;

    /* Active control sessions (singly-linked list) */
    aeron_archive_control_session_entry_t *control_sessions_head;

    /* Catalog for persisted recording metadata */
    aeron_archive_catalog_t *catalog;

    /* Recording events proxy */
    aeron_archive_recording_events_proxy_t *recording_events_proxy;

    /* Active recording sessions: array-based map */
    aeron_archive_recording_session_entry_t *recording_sessions;
    int32_t recording_session_count;
    int32_t recording_session_capacity;

    /* Active replay sessions: array-based map */
    aeron_archive_replay_session_entry_t *replay_sessions;
    int32_t replay_session_count;
    int32_t replay_session_capacity;

    /* Active replication sessions: array-based map */
    aeron_archive_replication_session_entry_t *replication_sessions;
    int32_t replication_session_count;
    int32_t replication_session_capacity;

    /* Recording subscriptions: key -> subscription mapping */
    aeron_archive_recording_subscription_entry_t *recording_subscriptions;
    int32_t recording_subscription_count;
    int32_t recording_subscription_capacity;

    /* Task queue (singly-linked list) */
    aeron_archive_conductor_task_t *task_queue_head;
    aeron_archive_conductor_task_t *task_queue_tail;

    /* Temporary recording summary for request processing */
    aeron_archive_recording_summary_t recording_summary;

    /* Session ID generator */
    int64_t next_session_id;

    /* Replay ID counter */
    int32_t replay_id;

    /* Mark file update tracking */
    int64_t mark_file_update_deadline_ms;

    /* Cached epoch clock value */
    int64_t cached_epoch_clock_ms;

    /* Flags */
    volatile bool is_abort;
    bool is_closed;
}
aeron_archive_conductor_t;

/* ---- Lifecycle ---- */

/**
 * Create and initialise the archive conductor.
 *
 * @param conductor out param for the allocated conductor.
 * @param ctx       the conductor context with all configuration.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_conductor_create(
    aeron_archive_conductor_t **conductor,
    aeron_archive_conductor_context_t *ctx);

/**
 * Perform a unit of work: poll the control adapter for new requests, drive
 * all active recording/replay/replication sessions, run queued tasks.
 *
 * @param conductor the archive conductor.
 * @return the amount of work done, or -1 on terminal error.
 */
int aeron_archive_conductor_do_work(aeron_archive_conductor_t *conductor);

/**
 * Close the archive conductor and release all resources.
 * Aborts all active sessions and closes subscriptions.
 *
 * @param conductor the conductor to close. May be NULL.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_conductor_close(aeron_archive_conductor_t *conductor);

/**
 * Signal abort to the conductor. Called from an external close handler.
 *
 * @param conductor the archive conductor.
 */
void aeron_archive_conductor_abort(aeron_archive_conductor_t *conductor);

/* ---- Control session management ---- */

/**
 * Create a new control session in response to a ConnectRequest or AuthConnectRequest.
 * Allocates the response publication asynchronously and adds the session to the conductor.
 *
 * @param conductor         the archive conductor.
 * @param correlation_id    the connect correlation id from the client.
 * @param response_stream_id the stream id for the response publication.
 * @param client_version    the client protocol version.
 * @param response_channel  the response channel URI.
 * @param encoded_credentials optional credentials (may be NULL).
 * @param credentials_length  length of encoded_credentials.
 * @param adapter           the control session adapter.
 * @param image             the image on which the request arrived (may be NULL).
 * @return the new control session, or NULL on failure.
 */
aeron_archive_control_session_t *aeron_archive_conductor_new_control_session(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int32_t response_stream_id,
    int32_t client_version,
    const char *response_channel,
    const uint8_t *encoded_credentials,
    size_t credentials_length,
    aeron_archive_control_session_adapter_t *adapter,
    aeron_image_t *image);

/* ---- Control request handlers ---- */

/**
 * Handle a start recording request: subscribe to channel/stream and create
 * a RecordingSession for each available image.
 *
 * @param conductor        the archive conductor.
 * @param correlation_id   the client correlation id.
 * @param stream_id        the stream id to record.
 * @param source_location  LOCAL or REMOTE.
 * @param auto_stop        whether to auto-stop when image becomes unavailable.
 * @param original_channel the original channel URI from the client.
 * @param control_session  the control session making the request (opaque, may be NULL).
 * @return 0 on success, -1 on error (error already sent to control session).
 */
int aeron_archive_conductor_start_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int32_t stream_id,
    aeron_archive_source_location_t source_location,
    bool auto_stop,
    const char *original_channel,
    void *control_session);

/**
 * Handle a stop recording request: find and remove the subscription for the
 * given channel/stream, aborting any associated recording sessions.
 *
 * @param conductor        the archive conductor.
 * @param correlation_id   the client correlation id.
 * @param stream_id        the stream id.
 * @param channel          the channel URI.
 * @param control_session  the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_stop_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int32_t stream_id,
    const char *channel,
    void *control_session);

/**
 * Handle a stop recording by subscription ID.
 *
 * @param conductor        the archive conductor.
 * @param correlation_id   the client correlation id.
 * @param subscription_id  the subscription registration id.
 * @param control_session  the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_stop_recording_subscription(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t subscription_id,
    void *control_session);

/**
 * Handle a stop recording by recording identity.
 *
 * @param conductor        the archive conductor.
 * @param correlation_id   the client correlation id.
 * @param recording_id     the recording id to stop.
 * @param control_session  the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_stop_recording_by_identity(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session);

/**
 * Handle a start replay request: validate the recording and create a
 * ReplaySession to send recorded data to the client.
 *
 * @param conductor           the archive conductor.
 * @param correlation_id      the client correlation id.
 * @param recording_id        the recording to replay.
 * @param position            the position to start from (AERON_ARCHIVE_NULL_POSITION for beginning).
 * @param length              the length to replay.
 * @param replay_stream_id    the stream id for the replay publication.
 * @param replay_channel      the channel for the replay publication.
 * @param control_session     the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_start_replay(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t position,
    int64_t length,
    int32_t replay_stream_id,
    const char *replay_channel,
    void *control_session);

/**
 * Handle a start bounded replay request: like start_replay but with a limit counter.
 *
 * @param conductor           the archive conductor.
 * @param correlation_id      the client correlation id.
 * @param recording_id        the recording to replay.
 * @param position            the position to start from.
 * @param length              the length to replay.
 * @param limit_counter_id    the counter id to bound the replay.
 * @param replay_stream_id    the stream id for the replay publication.
 * @param replay_channel      the channel for the replay publication.
 * @param control_session     the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_start_bounded_replay(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t position,
    int64_t length,
    int32_t limit_counter_id,
    int32_t replay_stream_id,
    const char *replay_channel,
    void *control_session);

/**
 *
 * @param conductor          the archive conductor.
 * @param correlation_id     the client correlation id.
 * @param replay_session_id  the replay session to stop.
 * @param control_session    the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_stop_replay(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t replay_session_id,
    void *control_session);

/**
 * Handle a stop all replays request for a given recording.
 *
 * @param conductor          the archive conductor.
 * @param correlation_id     the client correlation id.
 * @param recording_id       the recording id (AERON_ARCHIVE_NULL_VALUE to stop all).
 * @param control_session    the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_stop_all_replays(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session);

/**
 * Handle a list recordings request: iterate catalog entries from fromId.
 *
 * @param conductor        the archive conductor.
 * @param correlation_id   the client correlation id.
 * @param from_recording_id the first recording id to list.
 * @param record_count     the maximum number of recordings to list.
 * @param control_session  the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_list_recordings(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t from_recording_id,
    int32_t record_count,
    void *control_session);

/**
 * Handle a list recordings for URI request: filtered iteration.
 *
 * @param conductor          the archive conductor.
 * @param correlation_id     the client correlation id.
 * @param from_recording_id  the first recording id to check.
 * @param record_count       the maximum number to return.
 * @param stream_id          the stream id filter.
 * @param channel_fragment   channel substring to match.
 * @param control_session    the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_list_recordings_for_uri(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t from_recording_id,
    int32_t record_count,
    int32_t stream_id,
    const char *channel_fragment,
    void *control_session);

/**
 * Handle a list single recording by id.
 *
 * @param conductor        the archive conductor.
 * @param correlation_id   the client correlation id.
 * @param recording_id     the recording id to list.
 * @param control_session  the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_list_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session);

/**
 * Handle a find last matching recording request.
 *
 * @param conductor          the archive conductor.
 * @param correlation_id     the client correlation id.
 * @param min_recording_id   the minimum recording id.
 * @param session_id         the session id filter.
 * @param stream_id          the stream id filter.
 * @param channel_fragment   channel substring to match.
 * @param control_session    the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_find_last_matching_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t min_recording_id,
    int32_t session_id,
    int32_t stream_id,
    const char *channel_fragment,
    void *control_session);

/**
 * Get the start position of a recording.
 *
 * @param conductor        the archive conductor.
 * @param correlation_id   the client correlation id.
 * @param recording_id     the recording id.
 * @param control_session  the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_get_start_position(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session);

/**
 * Get the stop position of a recording.
 *
 * @param conductor        the archive conductor.
 * @param correlation_id   the client correlation id.
 * @param recording_id     the recording id.
 * @param control_session  the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_get_stop_position(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session);

/**
 * Get the current recording position of an active recording session.
 *
 * @param conductor        the archive conductor.
 * @param correlation_id   the client correlation id.
 * @param recording_id     the recording id.
 * @param control_session  the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_get_recording_position(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session);

/**
 * Get the max recorded position (active position or stop position).
 *
 * @param conductor        the archive conductor.
 * @param correlation_id   the client correlation id.
 * @param recording_id     the recording id.
 * @param control_session  the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_get_max_recorded_position(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session);

/**
 * Truncate a recording at the given position.
 *
 * @param conductor        the archive conductor.
 * @param correlation_id   the client correlation id.
 * @param recording_id     the recording id.
 * @param position         the position to truncate to.
 * @param control_session  the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_truncate_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t position,
    void *control_session);

/**
 * Purge a recording: mark as deleted and remove segment files.
 *
 * @param conductor        the archive conductor.
 * @param correlation_id   the client correlation id.
 * @param recording_id     the recording id.
 * @param control_session  the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_purge_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session);

/**
 * Extend an existing recording by subscribing to a new image.
 *
 * @param conductor        the archive conductor.
 * @param correlation_id   the client correlation id.
 * @param recording_id     the recording id to extend.
 * @param stream_id        the stream id.
 * @param source_location  LOCAL or REMOTE.
 * @param auto_stop        whether to auto-stop.
 * @param original_channel the channel URI.
 * @param control_session  the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_extend_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    int32_t stream_id,
    aeron_archive_source_location_t source_location,
    bool auto_stop,
    const char *original_channel,
    void *control_session);

/**
 * Start replication from a remote archive.
 *
 * @param conductor             the archive conductor.
 * @param correlation_id        the client correlation id.
 * @param src_recording_id      the source recording id.
 * @param dst_recording_id      the destination recording id (AERON_ARCHIVE_NULL_VALUE for new).
 * @param stop_position         the position to stop replication.
 * @param src_control_stream_id the source archive control stream id.
 * @param src_control_channel   the source archive control channel.
 * @param live_destination      the live destination channel (may be NULL).
 * @param replication_channel   the replication channel (may be NULL to use default).
 * @param control_session       the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_replicate(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t src_recording_id,
    int64_t dst_recording_id,
    int64_t stop_position,
    int32_t src_control_stream_id,
    const char *src_control_channel,
    const char *live_destination,
    const char *replication_channel,
    void *control_session);

/**
 * Stop a replication session.
 *
 * @param conductor        the archive conductor.
 * @param correlation_id   the client correlation id.
 * @param replication_id   the replication session id.
 * @param control_session  the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_stop_replication(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t replication_id,
    void *control_session);

/**
 * List recording subscriptions matching the given criteria.
 *
 * @param conductor          the archive conductor.
 * @param correlation_id     the client correlation id.
 * @param pseudo_index       the pseudo index to start from.
 * @param subscription_count the maximum number of subscriptions to list.
 * @param apply_stream_id    whether to filter by stream id.
 * @param stream_id          the stream id filter (only used if apply_stream_id is true).
 * @param channel_fragment   channel substring to match.
 * @param control_session    the control session.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_conductor_list_recording_subscriptions(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int32_t pseudo_index,
    int32_t subscription_count,
    bool apply_stream_id,
    int32_t stream_id,
    const char *channel_fragment,
    void *control_session);

/* ---- Session management helpers ---- */

/**
 * Enqueue a task to be run on the next duty cycle.
 *
 * @param conductor the archive conductor.
 * @param func      the task function.
 * @param clientd   opaque user data passed to the function.
 * @return 0 on success, -1 on allocation failure.
 */
int aeron_archive_conductor_enqueue_task(
    aeron_archive_conductor_t *conductor,
    aeron_archive_conductor_task_func_t func,
    void *clientd);

/**
 * Called when a recording session completes (reaches STOPPED state).
 * Updates the catalog and removes the session.
 *
 * @param conductor the archive conductor.
 * @param session   the recording session that completed.
 */
void aeron_archive_conductor_close_recording_session(
    aeron_archive_conductor_t *conductor,
    aeron_archive_recording_session_t *session);

/**
 * Find an active recording session by recording id.
 *
 * @param conductor    the archive conductor.
 * @param recording_id the recording id.
 * @return the session, or NULL if not found.
 */
aeron_archive_recording_session_t *aeron_archive_conductor_find_recording_session(
    aeron_archive_conductor_t *conductor,
    int64_t recording_id);

/**
 * Get the number of active recording sessions.
 *
 * @param conductor the archive conductor.
 * @return the number of active recording sessions.
 */
int32_t aeron_archive_conductor_recording_session_count(const aeron_archive_conductor_t *conductor);

/**
 * Get the number of active replay sessions.
 *
 * @param conductor the archive conductor.
 * @return the number of active replay sessions.
 */
int32_t aeron_archive_conductor_replay_session_count(const aeron_archive_conductor_t *conductor);

/**
 * Check whether the conductor is closed.
 *
 * @param conductor the archive conductor.
 * @return true if closed.
 */
bool aeron_archive_conductor_is_closed(const aeron_archive_conductor_t *conductor);

#ifdef __cplusplus
}
#endif

#endif /* AERON_ARCHIVE_CONDUCTOR_H */
