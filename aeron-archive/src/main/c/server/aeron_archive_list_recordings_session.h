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

#ifndef AERON_ARCHIVE_LIST_RECORDINGS_SESSION_H
#define AERON_ARCHIVE_LIST_RECORDINGS_SESSION_H

#include <stdint.h>
#include <stdbool.h>

#include "aeron_archive_catalog.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define AERON_ARCHIVE_LIST_RECORDINGS_MAX_SCANS_PER_WORK_CYCLE (64)

/**
 * The type of list recordings session, determining the filter applied.
 */
typedef enum aeron_archive_list_recordings_type_en
{
    /** List all recordings from a given id. */
    AERON_ARCHIVE_LIST_RECORDINGS_ALL = 0,
    /** List recordings matching a channel fragment and stream id. */
    AERON_ARCHIVE_LIST_RECORDINGS_FOR_URI = 1,
    /** List a single recording by id. */
    AERON_ARCHIVE_LIST_RECORDING_BY_ID = 2
}
aeron_archive_list_recordings_type_t;

/**
 * Callback to send a recording descriptor back to the control session.
 *
 * @param correlation_id the correlation id of the request.
 * @param descriptor     the recording descriptor to send.
 * @param clientd        user-supplied context (typically the control session).
 * @return true if the descriptor was successfully sent.
 */
typedef bool (*aeron_archive_list_recordings_send_descriptor_func_t)(
    int64_t correlation_id,
    const aeron_archive_catalog_recording_descriptor_t *descriptor,
    void *clientd);

/**
 * Callback to send a recording-unknown response to the control session.
 *
 * @param correlation_id the correlation id of the request.
 * @param recording_id   the recording id that was not found.
 * @param clientd        user-supplied context (typically the control session).
 */
typedef void (*aeron_archive_list_recordings_send_unknown_func_t)(
    int64_t correlation_id,
    int64_t recording_id,
    void *clientd);

/**
 * A list recordings session that iterates catalog entries and sends descriptors
 * back to the control session. Combines the functionality of Java's
 * AbstractListRecordingsSession, ListRecordingsSession,
 * ListRecordingsForUriSession, and ListRecordingByIdSession.
 */
typedef struct aeron_archive_list_recordings_session_stct
{
    int64_t correlation_id;
    int64_t recording_id;
    int32_t count;
    int32_t sent;

    aeron_archive_list_recordings_type_t type;

    /** Filter fields for FOR_URI type. */
    int32_t filter_stream_id;
    char *filter_channel_fragment;

    aeron_archive_catalog_t *catalog;

    aeron_archive_list_recordings_send_descriptor_func_t send_descriptor;
    aeron_archive_list_recordings_send_unknown_func_t send_unknown;
    void *callback_clientd;

    bool is_done;
}
aeron_archive_list_recordings_session_t;

/**
 * Create a list-recordings session that returns all recordings from a given id.
 *
 * @param session           out param for the allocated session.
 * @param correlation_id    the correlation id from the control request.
 * @param from_recording_id the recording id to start listing from.
 * @param count             the maximum number of descriptors to send.
 * @param catalog           the archive catalog.
 * @param send_descriptor   callback to send a descriptor.
 * @param send_unknown      callback to send a recording-unknown response.
 * @param callback_clientd  context passed to callbacks (e.g. control session).
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_list_recordings_session_create(
    aeron_archive_list_recordings_session_t **session,
    int64_t correlation_id,
    int64_t from_recording_id,
    int32_t count,
    aeron_archive_catalog_t *catalog,
    aeron_archive_list_recordings_send_descriptor_func_t send_descriptor,
    aeron_archive_list_recordings_send_unknown_func_t send_unknown,
    void *callback_clientd);

/**
 * Create a list-recordings session that filters by channel fragment and stream id.
 *
 * @param session            out param for the allocated session.
 * @param correlation_id     the correlation id from the control request.
 * @param from_recording_id  the recording id to start listing from.
 * @param count              the maximum number of descriptors to send.
 * @param channel_fragment   the channel substring to match (will be copied).
 * @param stream_id          the stream id to match.
 * @param catalog            the archive catalog.
 * @param send_descriptor    callback to send a descriptor.
 * @param send_unknown       callback to send a recording-unknown response.
 * @param callback_clientd   context passed to callbacks.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_list_recordings_for_uri_session_create(
    aeron_archive_list_recordings_session_t **session,
    int64_t correlation_id,
    int64_t from_recording_id,
    int32_t count,
    const char *channel_fragment,
    int32_t stream_id,
    aeron_archive_catalog_t *catalog,
    aeron_archive_list_recordings_send_descriptor_func_t send_descriptor,
    aeron_archive_list_recordings_send_unknown_func_t send_unknown,
    void *callback_clientd);

/**
 * Create a list-recording-by-id session that returns a single recording.
 *
 * @param session          out param for the allocated session.
 * @param correlation_id   the correlation id from the control request.
 * @param recording_id     the recording id to look up.
 * @param catalog          the archive catalog.
 * @param send_descriptor  callback to send a descriptor.
 * @param send_unknown     callback to send a recording-unknown response.
 * @param callback_clientd context passed to callbacks.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_list_recording_by_id_session_create(
    aeron_archive_list_recordings_session_t **session,
    int64_t correlation_id,
    int64_t recording_id,
    aeron_archive_catalog_t *catalog,
    aeron_archive_list_recordings_send_descriptor_func_t send_descriptor,
    aeron_archive_list_recordings_send_unknown_func_t send_unknown,
    void *callback_clientd);

/**
 * Perform a unit of work: iterate catalog entries and send descriptors.
 *
 * @param session the list recordings session.
 * @return the amount of work done (number of entries scanned).
 */
int aeron_archive_list_recordings_session_do_work(aeron_archive_list_recordings_session_t *session);

/**
 * Close the list recordings session and release resources.
 *
 * @param session the list recordings session. May be NULL.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_list_recordings_session_close(aeron_archive_list_recordings_session_t *session);

/**
 * Abort the list recordings session.
 *
 * @param session the list recordings session.
 * @param reason  descriptive reason for the abort.
 */
void aeron_archive_list_recordings_session_abort(
    aeron_archive_list_recordings_session_t *session, const char *reason);

/**
 * Check if the list recordings session is done.
 *
 * @param session the list recordings session.
 * @return true if done.
 */
bool aeron_archive_list_recordings_session_is_done(const aeron_archive_list_recordings_session_t *session);

/**
 * Get the session id (same as correlation_id).
 *
 * @param session the list recordings session.
 * @return the session id.
 */
int64_t aeron_archive_list_recordings_session_session_id(const aeron_archive_list_recordings_session_t *session);

#ifdef __cplusplus
}
#endif

#endif /* AERON_ARCHIVE_LIST_RECORDINGS_SESSION_H */
