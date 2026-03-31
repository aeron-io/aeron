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

#ifndef AERON_ARCHIVE_CONTROL_SESSION_H
#define AERON_ARCHIVE_CONTROL_SESSION_H

#include <stdint.h>
#include <stdbool.h>

#include "aeronc.h"
#include "aeron_archive_control_response_proxy.h"

/* Forward declarations */
typedef struct aeron_archive_conductor_stct aeron_archive_conductor_t;
typedef struct aeron_archive_control_session_adapter_stct aeron_archive_control_session_adapter_t;

#define AERON_ARCHIVE_CONTROL_SESSION_RESEND_INTERVAL_MS (200L)
#define AERON_ARCHIVE_AUTHENTICATION_REJECTED (10)

/**
 * Control session states matching the Java ControlSession.State enum.
 */
typedef enum aeron_archive_control_session_state_en
{
    AERON_ARCHIVE_CONTROL_SESSION_STATE_INIT = 0,
    AERON_ARCHIVE_CONTROL_SESSION_STATE_CONNECTING = 1,
    AERON_ARCHIVE_CONTROL_SESSION_STATE_CONNECTED = 2,
    AERON_ARCHIVE_CONTROL_SESSION_STATE_CHALLENGED = 3,
    AERON_ARCHIVE_CONTROL_SESSION_STATE_AUTHENTICATED = 4,
    AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE = 5,
    AERON_ARCHIVE_CONTROL_SESSION_STATE_REJECTED = 6,
    AERON_ARCHIVE_CONTROL_SESSION_STATE_DONE = 7
}
aeron_archive_control_session_state_t;

/**
 * A pending response entry in the synchronous response queue.
 */
typedef struct aeron_archive_control_session_pending_response_stct
{
    int64_t correlation_id;
    int64_t relevant_id;
    aeron_archive_control_response_code_t code;
    char *error_message;
    struct aeron_archive_control_session_pending_response_stct *next;
}
aeron_archive_control_session_pending_response_t;

/**
 * Per-client state machine managing one archive client connection.
 *
 * Mirrors the Java ControlSession. Queues pending responses and processes them
 * one at a time. Sends responses via ControlResponseProxy.
 */
typedef struct aeron_archive_control_session_stct
{
    int64_t control_session_id;
    int64_t correlation_id;
    int64_t connect_timeout_ms;
    int64_t session_liveness_check_interval_ms;
    int64_t session_liveness_check_deadline_ms;
    int64_t resend_deadline_ms;
    int64_t activity_deadline_ms;

    aeron_archive_control_session_state_t state;

    aeron_t *aeron;
    aeron_archive_conductor_t *conductor;
    aeron_archive_control_response_proxy_t *control_response_proxy;
    aeron_archive_control_session_adapter_t *control_session_adapter;

    aeron_exclusive_publication_t *control_publication;
    int64_t control_publication_registration_id;
    int32_t control_publication_stream_id;
    char *control_publication_channel;

    char *invalid_version_message;
    char *abort_reason;

    uint8_t *encoded_principal;
    size_t encoded_principal_length;

    aeron_archive_control_session_pending_response_t *response_queue_head;
    aeron_archive_control_session_pending_response_t *response_queue_tail;
}
aeron_archive_control_session_t;

/**
 * Create a control session.
 *
 * @param session                           out param for the allocated session.
 * @param control_session_id                unique session id.
 * @param correlation_id                    the connect correlation id.
 * @param connect_timeout_ms                timeout for establishing the connection.
 * @param session_liveness_check_interval_ms interval for liveness checks.
 * @param control_publication_registration_id  the async publication registration id.
 * @param control_publication_channel       the response channel (will be copied).
 * @param control_publication_stream_id     the response stream id.
 * @param invalid_version_message           optional version error message (will be copied, may be NULL).
 * @param control_session_adapter           the owning adapter.
 * @param aeron                             the Aeron client.
 * @param conductor                         the archive conductor.
 * @param cached_epoch_clock_ms             the current epoch time in ms.
 * @param control_response_proxy            the response proxy.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_control_session_create(
    aeron_archive_control_session_t **session,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t connect_timeout_ms,
    int64_t session_liveness_check_interval_ms,
    int64_t control_publication_registration_id,
    const char *control_publication_channel,
    int32_t control_publication_stream_id,
    const char *invalid_version_message,
    aeron_archive_control_session_adapter_t *control_session_adapter,
    aeron_t *aeron,
    aeron_archive_conductor_t *conductor,
    int64_t cached_epoch_clock_ms,
    aeron_archive_control_response_proxy_t *control_response_proxy);

/**
 * Perform a unit of work for this session.
 *
 * @param session             the control session.
 * @param cached_epoch_clock_ms the current epoch time in ms.
 * @return the amount of work done.
 */
int aeron_archive_control_session_do_work(
    aeron_archive_control_session_t *session,
    int64_t cached_epoch_clock_ms);

/**
 * Abort the control session with the given reason.
 *
 * @param session the control session.
 * @param reason  descriptive reason for the abort.
 */
void aeron_archive_control_session_abort(
    aeron_archive_control_session_t *session,
    const char *reason);

/**
 * Close the control session and release all resources.
 *
 * @param session the control session. May be NULL.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_control_session_close(aeron_archive_control_session_t *session);

/**
 * Check if the session has reached the DONE state.
 */
bool aeron_archive_control_session_is_done(const aeron_archive_control_session_t *session);

/**
 * Get the session id.
 */
int64_t aeron_archive_control_session_id(const aeron_archive_control_session_t *session);

/**
 * Get the current state.
 */
aeron_archive_control_session_state_t aeron_archive_control_session_state(
    const aeron_archive_control_session_t *session);

/**
 * Get the control publication.
 */
aeron_exclusive_publication_t *aeron_archive_control_session_publication(
    const aeron_archive_control_session_t *session);

/**
 * Get the encoded principal.
 */
const uint8_t *aeron_archive_control_session_encoded_principal(
    const aeron_archive_control_session_t *session,
    size_t *out_length);

/**
 * Set the encoded principal and transition to AUTHENTICATED state.
 */
void aeron_archive_control_session_authenticate(
    aeron_archive_control_session_t *session,
    const uint8_t *encoded_principal,
    size_t encoded_principal_length);

/**
 * Transition the session to CHALLENGED state.
 */
void aeron_archive_control_session_challenged(aeron_archive_control_session_t *session);

/**
 * Transition the session to REJECTED state.
 */
void aeron_archive_control_session_reject(aeron_archive_control_session_t *session);

/* Request handlers called from the adapter when SBE messages are decoded. */

void aeron_archive_control_session_on_keep_alive(
    aeron_archive_control_session_t *session, int64_t correlation_id);

void aeron_archive_control_session_on_challenge_response(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    const uint8_t *encoded_credentials, size_t credentials_length);

void aeron_archive_control_session_on_start_recording(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int32_t stream_id, int32_t source_location, bool auto_stop, const char *channel);

void aeron_archive_control_session_on_stop_recording(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int32_t stream_id, const char *channel);

void aeron_archive_control_session_on_stop_recording_subscription(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t subscription_id);

void aeron_archive_control_session_on_stop_recording_by_identity(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id);

void aeron_archive_control_session_on_list_recordings(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t from_recording_id, int32_t record_count);

void aeron_archive_control_session_on_list_recordings_for_uri(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t from_recording_id, int32_t record_count, int32_t stream_id,
    const uint8_t *channel_fragment, size_t channel_fragment_length);

void aeron_archive_control_session_on_list_recording(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id);

void aeron_archive_control_session_on_find_last_matching_recording(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t min_recording_id, int32_t session_id, int32_t stream_id,
    const uint8_t *channel_fragment, size_t channel_fragment_length);

void aeron_archive_control_session_on_start_replay(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t recording_id, int64_t position, int64_t length,
    int32_t file_io_max_length, int32_t replay_stream_id, const char *replay_channel);

void aeron_archive_control_session_on_start_bounded_replay(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t recording_id, int64_t position, int64_t length,
    int32_t limit_counter_id, int32_t file_io_max_length,
    int32_t replay_stream_id, const char *replay_channel);

void aeron_archive_control_session_on_stop_replay(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t replay_session_id);

void aeron_archive_control_session_on_stop_all_replays(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id);

void aeron_archive_control_session_on_extend_recording(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t recording_id, int32_t stream_id, int32_t source_location,
    bool auto_stop, const char *channel);

void aeron_archive_control_session_on_get_recording_position(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id);

void aeron_archive_control_session_on_get_stop_position(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id);

void aeron_archive_control_session_on_get_max_recorded_position(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id);

void aeron_archive_control_session_on_get_start_position(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id);

void aeron_archive_control_session_on_truncate_recording(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t recording_id, int64_t position);

void aeron_archive_control_session_on_purge_recording(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id);

void aeron_archive_control_session_on_list_recording_subscriptions(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int32_t pseudo_index, int32_t subscription_count, bool apply_stream_id,
    int32_t stream_id, const char *channel_fragment);

void aeron_archive_control_session_on_replicate(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t src_recording_id, int64_t dst_recording_id, int64_t stop_position,
    int64_t channel_tag_id, int64_t subscription_tag_id,
    int32_t src_control_stream_id, int32_t file_io_max_length,
    int32_t replication_session_id,
    const char *src_control_channel, const char *live_destination,
    const char *replication_channel,
    const uint8_t *encoded_credentials, size_t encoded_credentials_length,
    const char *src_response_channel);

void aeron_archive_control_session_on_stop_replication(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t replication_id);

void aeron_archive_control_session_on_detach_segments(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t recording_id, int64_t new_start_position);

void aeron_archive_control_session_on_delete_detached_segments(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id);

void aeron_archive_control_session_on_purge_segments(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t recording_id, int64_t new_start_position);

void aeron_archive_control_session_on_attach_segments(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id);

void aeron_archive_control_session_on_migrate_segments(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t src_recording_id, int64_t dst_recording_id);

void aeron_archive_control_session_on_archive_id(
    aeron_archive_control_session_t *session, int64_t correlation_id);

/* Response senders */

void aeron_archive_control_session_send_ok_response(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t relevant_id);

void aeron_archive_control_session_send_error_response(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t relevant_id, const char *error_message);

void aeron_archive_control_session_send_recording_unknown(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id);

void aeron_archive_control_session_send_subscription_unknown(
    aeron_archive_control_session_t *session, int64_t correlation_id);

void aeron_archive_control_session_send_response(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t relevant_id,
    aeron_archive_control_response_code_t code, const char *error_message);

bool aeron_archive_control_session_send_descriptor(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    const uint8_t *descriptor_buffer, size_t descriptor_length);

bool aeron_archive_control_session_send_subscription_descriptor(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t subscription_id, int32_t stream_id, const char *channel);

void aeron_archive_control_session_send_signal(
    aeron_archive_control_session_t *session,
    int64_t correlation_id, int64_t recording_id, int64_t subscription_id,
    int64_t position, aeron_archive_recording_signal_code_t signal);

#endif /* AERON_ARCHIVE_CONTROL_SESSION_H */
