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

#ifndef AERON_ARCHIVE_DELETE_SEGMENTS_SESSION_H
#define AERON_ARCHIVE_DELETE_SEGMENTS_SESSION_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C"
{
#endif

#define AERON_ARCHIVE_DELETE_SUFFIX ".del"

/**
 * Callback to send a recording signal when segment deletion completes.
 *
 * @param correlation_id  the correlation id of the request.
 * @param recording_id    the recording id.
 * @param subscription_id the subscription id (AERON_ARCHIVE_NULL_VALUE).
 * @param position        the position (AERON_ARCHIVE_NULL_VALUE).
 * @param clientd         user-supplied context (typically the control session).
 */
typedef void (*aeron_archive_delete_segments_send_signal_func_t)(
    int64_t correlation_id,
    int64_t recording_id,
    int64_t subscription_id,
    int64_t position,
    void *clientd);

/**
 * Callback to send an error response when a segment file cannot be deleted.
 *
 * @param correlation_id the correlation id.
 * @param error_code     the error code.
 * @param error_message  the error message.
 * @param clientd        user-supplied context (typically the control session).
 */
typedef void (*aeron_archive_delete_segments_send_error_func_t)(
    int64_t correlation_id,
    int32_t error_code,
    const char *error_message,
    void *clientd);

/**
 * Callback to remove the session from the conductor when closing.
 *
 * @param session the session being removed.
 * @param clientd user-supplied context (typically the conductor).
 */
typedef void (*aeron_archive_delete_segments_remove_func_t)(
    void *session,
    void *clientd);

/**
 * Error handler callback for reporting deletion errors.
 *
 * @param error_message the error message.
 * @param clientd       user-supplied context.
 */
typedef void (*aeron_archive_delete_segments_error_handler_func_t)(
    const char *error_message,
    void *clientd);

/**
 * An entry in the file queue for the delete segments session.
 */
typedef struct aeron_archive_delete_segments_file_entry_stct
{
    char *file_path;
    struct aeron_archive_delete_segments_file_entry_stct *next;
}
aeron_archive_delete_segments_file_entry_t;

/**
 * A session that deletes segment files for a recording one at a time.
 *
 * Mirrors the Java DeleteSegmentsSession. Processes one file per work cycle,
 * attempting deletion and falling back to the .del-suffixed name if the
 * original has already been renamed.
 */
typedef struct aeron_archive_delete_segments_session_stct
{
    int64_t recording_id;
    int64_t correlation_id;
    int64_t max_delete_position;

    aeron_archive_delete_segments_file_entry_t *file_queue_head;
    aeron_archive_delete_segments_file_entry_t *file_queue_tail;
    int32_t file_count;

    aeron_archive_delete_segments_send_signal_func_t send_signal;
    aeron_archive_delete_segments_send_error_func_t send_error;
    aeron_archive_delete_segments_remove_func_t remove_session;
    aeron_archive_delete_segments_error_handler_func_t error_handler;
    void *callback_clientd;
    void *error_handler_clientd;
}
aeron_archive_delete_segments_session_t;

/**
 * Create a delete-segments session.
 *
 * @param session            out param for the allocated session.
 * @param recording_id       the recording id whose segments are being deleted.
 * @param correlation_id     the correlation id from the control request.
 * @param send_signal        callback to send a DELETE signal on completion.
 * @param send_error         callback to send an error response.
 * @param remove_session     callback to remove this session from the conductor.
 * @param error_handler      callback for error reporting.
 * @param callback_clientd   context passed to send_signal, send_error, remove_session.
 * @param error_handler_clientd context passed to error_handler.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_delete_segments_session_create(
    aeron_archive_delete_segments_session_t **session,
    int64_t recording_id,
    int64_t correlation_id,
    aeron_archive_delete_segments_send_signal_func_t send_signal,
    aeron_archive_delete_segments_send_error_func_t send_error,
    aeron_archive_delete_segments_remove_func_t remove_session,
    aeron_archive_delete_segments_error_handler_func_t error_handler,
    void *callback_clientd,
    void *error_handler_clientd);

/**
 * Add a segment file path to the deletion queue.
 *
 * @param session   the delete segments session.
 * @param file_path the file path to add (will be copied).
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_delete_segments_session_add_file(
    aeron_archive_delete_segments_session_t *session,
    const char *file_path);

/**
 * Finalise the session setup after all files have been added.
 * Computes the max_delete_position from the queued file names.
 *
 * @param session the delete segments session.
 */
void aeron_archive_delete_segments_session_finalise(
    aeron_archive_delete_segments_session_t *session);

/**
 * Perform a unit of work: attempt to delete one file from the queue.
 *
 * @param session the delete segments session.
 * @return the amount of work done.
 */
int aeron_archive_delete_segments_session_do_work(aeron_archive_delete_segments_session_t *session);

/**
 * Close the session: remove from conductor and send the DELETE signal.
 *
 * @param session the session to close. May be NULL.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_delete_segments_session_close(aeron_archive_delete_segments_session_t *session);

/**
 * Abort the session (no-op for delete segments, all files will still be processed).
 *
 * @param session the delete segments session.
 * @param reason  descriptive reason for the abort.
 */
void aeron_archive_delete_segments_session_abort(
    aeron_archive_delete_segments_session_t *session, const char *reason);

/**
 * Check if the session is done (all files have been processed).
 *
 * @param session the delete segments session.
 * @return true if done.
 */
bool aeron_archive_delete_segments_session_is_done(const aeron_archive_delete_segments_session_t *session);

/**
 * Get the session id (same as recording_id).
 *
 * @param session the delete segments session.
 * @return the session id.
 */
int64_t aeron_archive_delete_segments_session_session_id(const aeron_archive_delete_segments_session_t *session);

/**
 * Get the max segment position among the files queued for deletion.
 *
 * @param session the delete segments session.
 * @return the max delete position.
 */
int64_t aeron_archive_delete_segments_session_max_delete_position(
    const aeron_archive_delete_segments_session_t *session);

#ifdef __cplusplus
}
#endif

#endif /* AERON_ARCHIVE_DELETE_SEGMENTS_SESSION_H */
