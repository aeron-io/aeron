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

#ifndef AERON_ARCHIVE_RECORDING_SESSION_H
#define AERON_ARCHIVE_RECORDING_SESSION_H

#include <stdint.h>
#include <stdbool.h>

#include "aeronc.h"
#include "aeron_archive_recording_events_proxy.h"
#include "aeron_archive_recording_writer.h"

/**
 * Recording session states matching the Java RecordingSession.State enum.
 */
typedef enum aeron_archive_recording_session_state_en
{
    AERON_ARCHIVE_RECORDING_SESSION_STATE_INIT = 0,
    AERON_ARCHIVE_RECORDING_SESSION_STATE_RECORDING = 1,
    AERON_ARCHIVE_RECORDING_SESSION_STATE_INACTIVE = 2,
    AERON_ARCHIVE_RECORDING_SESSION_STATE_STOPPED = 3
}
aeron_archive_recording_session_state_t;

/**
 * Consumes an aeron_image_t and records data to file using a recording writer.
 * Called from the archive conductor's duty cycle.
 */
typedef struct aeron_archive_recording_session_stct
{
    int64_t correlation_id;
    int64_t recording_id;
    int64_t progress_event_position;
    int64_t join_position;

    size_t block_length_limit;
    bool is_auto_stop;
    volatile bool is_aborted;

    aeron_archive_recording_session_state_t state;

    aeron_archive_recording_events_proxy_t *recording_events_proxy;
    aeron_image_t *image;
    aeron_counter_t *position;
    aeron_archive_recording_writer_t *recording_writer;

    char *original_channel;
    char *error_message;
    int error_code;
}
aeron_archive_recording_session_t;

/**
 * Create and initialise a recording session.
 *
 * @param session               out param for the allocated session.
 * @param correlation_id        the correlation id from the control request.
 * @param recording_id          the recording id.
 * @param start_position        the start position of the recording.
 * @param segment_length        the segment file length.
 * @param original_channel      the original channel string (will be copied).
 * @param recording_events_proxy proxy for publishing recording events (may be NULL).
 * @param image                 the Aeron image to record from.
 * @param position              the counter tracking recording position.
 * @param archive_dir           the archive directory path.
 * @param file_io_max_length    the maximum file I/O length.
 * @param force_writes          whether to fsync after each write.
 * @param force_metadata        whether to fsync metadata.
 * @param is_auto_stop          whether to auto-stop when the image is unavailable.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_recording_session_create(
    aeron_archive_recording_session_t **session,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t start_position,
    int32_t segment_length,
    const char *original_channel,
    aeron_archive_recording_events_proxy_t *recording_events_proxy,
    aeron_image_t *image,
    aeron_counter_t *position,
    const char *archive_dir,
    int32_t file_io_max_length,
    bool force_writes,
    bool force_metadata,
    bool is_auto_stop);

/**
 * Perform a unit of work: poll image, write fragments, report progress.
 *
 * @param session the recording session.
 * @return the amount of work done (0 if idle).
 */
int aeron_archive_recording_session_do_work(aeron_archive_recording_session_t *session);

/**
 * Close the recording session and release all resources.
 *
 * @param session the recording session. May be NULL.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_recording_session_close(aeron_archive_recording_session_t *session);

/**
 * Abort-close: close the recording writer only (used during conductor shutdown).
 *
 * @param session the recording session.
 */
void aeron_archive_recording_session_abort_close(aeron_archive_recording_session_t *session);

/**
 * Check if the recording session has reached the STOPPED state.
 *
 * @param session the recording session.
 * @return true if the session is done.
 */
bool aeron_archive_recording_session_is_done(const aeron_archive_recording_session_t *session);

/**
 * Get the recording id for this session.
 *
 * @param session the recording session.
 * @return the recording id.
 */
int64_t aeron_archive_recording_session_recording_id(const aeron_archive_recording_session_t *session);

/**
 * Get the correlation id for this session.
 *
 * @param session the recording session.
 * @return the correlation id.
 */
int64_t aeron_archive_recording_session_correlation_id(const aeron_archive_recording_session_t *session);

/**
 * Get the session id (same as recording_id, matching Java Session interface).
 *
 * @param session the recording session.
 * @return the session id.
 */
int64_t aeron_archive_recording_session_session_id(const aeron_archive_recording_session_t *session);

/**
 * Abort the recording session with the given reason.
 *
 * @param session the recording session.
 * @param reason  descriptive reason for the abort.
 */
void aeron_archive_recording_session_abort(aeron_archive_recording_session_t *session, const char *reason);

/**
 * Check whether this session is configured for auto-stop.
 *
 * @param session the recording session.
 * @return true if auto-stop is enabled.
 */
bool aeron_archive_recording_session_is_auto_stop(const aeron_archive_recording_session_t *session);

/**
 * Get the current recorded position, or AERON_NULL_VALUE if the counter is closed.
 *
 * @param session the recording session.
 * @return the recorded position.
 */
int64_t aeron_archive_recording_session_recorded_position(const aeron_archive_recording_session_t *session);

/**
 * Get the error message if the session failed, or NULL if no error.
 *
 * @param session the recording session.
 * @return the error message string, or NULL.
 */
const char *aeron_archive_recording_session_error_message(const aeron_archive_recording_session_t *session);

/**
 * Get the error code if the session failed.
 *
 * @param session the recording session.
 * @return the error code.
 */
int aeron_archive_recording_session_error_code(const aeron_archive_recording_session_t *session);

#endif /* AERON_ARCHIVE_RECORDING_SESSION_H */
