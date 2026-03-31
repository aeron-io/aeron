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

#ifndef AERON_ARCHIVE_UPDATE_CHANNEL_SESSION_H
#define AERON_ARCHIVE_UPDATE_CHANNEL_SESSION_H

#include <stdint.h>
#include <stdbool.h>

#include "aeron_archive_catalog.h"

#ifdef __cplusplus
extern "C"
{
#endif

/* Forward declarations */
typedef struct aeron_archive_control_session_stct aeron_archive_control_session_t;

/**
 * Callback to send an OK response to the control session.
 *
 * @param correlation_id the correlation id of the request.
 * @param clientd        user-supplied context (typically the control session).
 */
typedef void (*aeron_archive_update_channel_send_ok_func_t)(
    int64_t correlation_id,
    void *clientd);

/**
 * Callback to send a recording-unknown response to the control session.
 *
 * @param correlation_id the correlation id of the request.
 * @param recording_id   the recording id that was not found.
 * @param clientd        user-supplied context (typically the control session).
 */
typedef void (*aeron_archive_update_channel_send_unknown_func_t)(
    int64_t correlation_id,
    int64_t recording_id,
    void *clientd);

/**
 * Callback to clear the active listing on the control session when the session closes.
 *
 * @param clientd user-supplied context (typically the control session).
 */
typedef void (*aeron_archive_update_channel_clear_active_listing_func_t)(
    void *clientd);

/**
 * A session that updates the channel for a recording in the catalog.
 *
 * Mirrors the Java UpdateChannelSession. Reads the existing recording descriptor,
 * replaces the channel fields, and writes the updated descriptor back.
 */
typedef struct aeron_archive_update_channel_session_stct
{
    int64_t correlation_id;
    int64_t recording_id;
    char *original_channel;
    char *stripped_channel;

    aeron_archive_catalog_t *catalog;

    aeron_archive_update_channel_send_ok_func_t send_ok;
    aeron_archive_update_channel_send_unknown_func_t send_unknown;
    aeron_archive_update_channel_clear_active_listing_func_t clear_active_listing;
    void *callback_clientd;

    bool is_done;
}
aeron_archive_update_channel_session_t;

/**
 * Create an update-channel session.
 *
 * @param session          out param for the allocated session.
 * @param correlation_id   the correlation id from the control request.
 * @param recording_id     the recording id to update.
 * @param original_channel the new original channel (will be copied).
 * @param stripped_channel the new stripped channel (will be copied).
 * @param catalog          the archive catalog.
 * @param send_ok          callback to send an OK response.
 * @param send_unknown     callback to send a recording-unknown response.
 * @param clear_active_listing callback to clear active listing on close.
 * @param callback_clientd context passed to callbacks (e.g. control session).
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_update_channel_session_create(
    aeron_archive_update_channel_session_t **session,
    int64_t correlation_id,
    int64_t recording_id,
    const char *original_channel,
    const char *stripped_channel,
    aeron_archive_catalog_t *catalog,
    aeron_archive_update_channel_send_ok_func_t send_ok,
    aeron_archive_update_channel_send_unknown_func_t send_unknown,
    aeron_archive_update_channel_clear_active_listing_func_t clear_active_listing,
    void *callback_clientd);

/**
 * Perform a unit of work: read the descriptor, update channels, write back.
 *
 * @param session the update channel session.
 * @return the amount of work done.
 */
int aeron_archive_update_channel_session_do_work(aeron_archive_update_channel_session_t *session);

/**
 * Close the update channel session and release resources.
 *
 * @param session the session to close. May be NULL.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_update_channel_session_close(aeron_archive_update_channel_session_t *session);

/**
 * Abort the session.
 *
 * @param session the update channel session.
 * @param reason  descriptive reason for the abort.
 */
void aeron_archive_update_channel_session_abort(
    aeron_archive_update_channel_session_t *session, const char *reason);

/**
 * Check if the session is done.
 *
 * @param session the update channel session.
 * @return true if done.
 */
bool aeron_archive_update_channel_session_is_done(const aeron_archive_update_channel_session_t *session);

/**
 * Get the session id (same as correlation_id).
 *
 * @param session the update channel session.
 * @return the session id.
 */
int64_t aeron_archive_update_channel_session_session_id(const aeron_archive_update_channel_session_t *session);

#ifdef __cplusplus
}
#endif

#endif /* AERON_ARCHIVE_UPDATE_CHANNEL_SESSION_H */
