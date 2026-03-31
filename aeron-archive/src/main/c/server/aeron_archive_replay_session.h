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

#ifndef AERON_ARCHIVE_REPLAY_SESSION_H
#define AERON_ARCHIVE_REPLAY_SESSION_H

#include <stdint.h>
#include <stdbool.h>

#include "aeronc.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Replay session states matching the Java ReplaySession.State enum.
 */
typedef enum aeron_archive_replay_session_state_en
{
    AERON_ARCHIVE_REPLAY_SESSION_STATE_INIT = 0,
    AERON_ARCHIVE_REPLAY_SESSION_STATE_REPLAY = 1,
    AERON_ARCHIVE_REPLAY_SESSION_STATE_INACTIVE = 2,
    AERON_ARCHIVE_REPLAY_SESSION_STATE_DONE = 3
}
aeron_archive_replay_session_state_t;

/**
 * A replay session that reads recorded data from segment files and publishes
 * it back to a client via an exclusive publication. Corresponds to the Java
 * ReplaySession class.
 */
typedef struct aeron_archive_replay_session_stct
{
    int64_t correlation_id;
    int64_t session_id;
    int64_t recording_id;
    int64_t start_position;
    int64_t replay_position;
    int64_t stop_position;
    int64_t replay_limit;
    int64_t segment_file_base_position;
    int64_t connect_deadline_ms;

    int32_t term_base_segment_offset;
    int32_t term_offset;
    int32_t stream_id;
    int32_t term_length;
    int32_t segment_length;

    aeron_archive_replay_session_state_t state;

    aeron_exclusive_publication_t *publication;
    aeron_counter_t *limit_position;

    uint8_t *replay_buffer;
    size_t replay_buffer_length;

    int segment_fd;
    char *archive_dir;
    char *error_message;

    bool revoke_publication;
    volatile bool is_aborted;
}
aeron_archive_replay_session_t;

/**
 * Create and initialise a replay session.
 *
 * @param session              out param for the allocated session.
 * @param correlation_id       the correlation id from the control request.
 * @param recording_id         the recording id.
 * @param replay_position      the position to start the replay from.
 * @param replay_length        the length to replay (0 for unbounded).
 * @param start_position       the start position of the recording.
 * @param stop_position        the stop position of the recording.
 * @param segment_file_length  the segment file length.
 * @param term_buffer_length   the term buffer length.
 * @param stream_id            the stream id.
 * @param replay_session_id    the session id for the replay.
 * @param connect_timeout_ms   the connection timeout in milliseconds.
 * @param current_time_ms      the current epoch time in milliseconds.
 * @param publication           the exclusive publication for sending replay data.
 * @param limit_position       counter tracking the recording limit position (may be NULL).
 * @param archive_dir          the archive directory path.
 * @param replay_buffer_length the length of the replay buffer.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_replay_session_create(
    aeron_archive_replay_session_t **session,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t replay_position,
    int64_t replay_length,
    int64_t start_position,
    int64_t stop_position,
    int32_t segment_file_length,
    int32_t term_buffer_length,
    int32_t stream_id,
    int64_t replay_session_id,
    int64_t connect_timeout_ms,
    int64_t current_time_ms,
    aeron_exclusive_publication_t *publication,
    aeron_counter_t *limit_position,
    const char *archive_dir,
    size_t replay_buffer_length);

/**
 * Perform a unit of work: initialise, replay, or transition to done.
 *
 * @param session         the replay session.
 * @param current_time_ms the current epoch time in milliseconds.
 * @return the amount of work done (0 if idle).
 */
int aeron_archive_replay_session_do_work(
    aeron_archive_replay_session_t *session,
    int64_t current_time_ms);

/**
 * Close the replay session and release all resources.
 *
 * @param session the replay session. May be NULL.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_replay_session_close(aeron_archive_replay_session_t *session);

/**
 * Abort the replay session.
 *
 * @param session the replay session.
 * @param reason  descriptive reason for the abort.
 */
void aeron_archive_replay_session_abort(aeron_archive_replay_session_t *session, const char *reason);

/**
 * Check if the replay session has reached the DONE state.
 *
 * @param session the replay session.
 * @return true if the session is done.
 */
bool aeron_archive_replay_session_is_done(const aeron_archive_replay_session_t *session);

/**
 * Get the session id for this replay session.
 *
 * @param session the replay session.
 * @return the session id.
 */
int64_t aeron_archive_replay_session_session_id(const aeron_archive_replay_session_t *session);

/**
 * Get the recording id for this replay session.
 *
 * @param session the replay session.
 * @return the recording id.
 */
int64_t aeron_archive_replay_session_recording_id(const aeron_archive_replay_session_t *session);

/**
 * Get the current replay state.
 *
 * @param session the replay session.
 * @return the current state.
 */
aeron_archive_replay_session_state_t aeron_archive_replay_session_state(
    const aeron_archive_replay_session_t *session);

/**
 * Get the segment file base position.
 *
 * @param session the replay session.
 * @return the segment file base position.
 */
int64_t aeron_archive_replay_session_segment_file_base_position(
    const aeron_archive_replay_session_t *session);

/**
 * Get the error message if the session failed, or NULL if no error.
 *
 * @param session the replay session.
 * @return the error message string, or NULL.
 */
const char *aeron_archive_replay_session_error_message(const aeron_archive_replay_session_t *session);

#ifdef __cplusplus
}
#endif

#endif /* AERON_ARCHIVE_REPLAY_SESSION_H */
