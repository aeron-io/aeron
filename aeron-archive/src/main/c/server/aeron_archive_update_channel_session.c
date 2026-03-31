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
#include <stdlib.h>

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "aeron_archive_update_channel_session.h"

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
    void *callback_clientd)
{
    aeron_archive_update_channel_session_t *_session = NULL;

    if (aeron_alloc((void **)&_session, sizeof(aeron_archive_update_channel_session_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_update_channel_session_t");
        return -1;
    }

    _session->correlation_id = correlation_id;
    _session->recording_id = recording_id;
    _session->catalog = catalog;
    _session->send_ok = send_ok;
    _session->send_unknown = send_unknown;
    _session->clear_active_listing = clear_active_listing;
    _session->callback_clientd = callback_clientd;
    _session->is_done = false;
    _session->original_channel = NULL;
    _session->stripped_channel = NULL;

    if (NULL != original_channel)
    {
        _session->original_channel = strdup(original_channel);
        if (NULL == _session->original_channel)
        {
            aeron_free(_session);
            AERON_APPEND_ERR("%s", "Unable to copy original_channel");
            return -1;
        }
    }

    if (NULL != stripped_channel)
    {
        _session->stripped_channel = strdup(stripped_channel);
        if (NULL == _session->stripped_channel)
        {
            free(_session->original_channel);
            aeron_free(_session);
            AERON_APPEND_ERR("%s", "Unable to copy stripped_channel");
            return -1;
        }
    }

    *session = _session;

    return 0;
}

int aeron_archive_update_channel_session_do_work(aeron_archive_update_channel_session_t *session)
{
    if (session->is_done)
    {
        return 0;
    }

    aeron_archive_catalog_recording_descriptor_t descriptor;
    memset(&descriptor, 0, sizeof(descriptor));

    if (aeron_archive_catalog_find_recording(session->catalog, session->recording_id, &descriptor) == 0)
    {
        /*
         * Re-add the recording with the updated channel fields. The catalog's
         * add_recording allocates the next id, so we use replace semantics by
         * invalidating the old entry and writing a replacement. However, since
         * the C catalog API does not yet expose a replace_recording, we
         * invalidate and re-add with the same fields but updated channels.
         *
         * Note: a full production implementation would add a
         * aeron_archive_catalog_replace_recording function. For now we write
         * the descriptor back by invalidating the old and adding a new one,
         * preserving all fields except the channels.
         *
         * TODO: implement aeron_archive_catalog_replace_recording for in-place update.
         */
        aeron_archive_catalog_invalidate_recording(session->catalog, session->recording_id);

        aeron_archive_catalog_add_recording(
            session->catalog,
            descriptor.start_position,
            descriptor.stop_position,
            descriptor.start_timestamp,
            descriptor.stop_timestamp,
            descriptor.initial_term_id,
            descriptor.segment_file_length,
            descriptor.term_buffer_length,
            descriptor.mtu_length,
            descriptor.session_id,
            descriptor.stream_id,
            session->stripped_channel,
            session->original_channel,
            descriptor.source_identity);

        session->send_ok(session->correlation_id, session->callback_clientd);
    }
    else
    {
        session->send_unknown(session->correlation_id, session->recording_id, session->callback_clientd);
    }

    session->is_done = true;
    return 1;
}

int aeron_archive_update_channel_session_close(aeron_archive_update_channel_session_t *session)
{
    if (NULL == session)
    {
        return 0;
    }

    if (NULL != session->clear_active_listing)
    {
        session->clear_active_listing(session->callback_clientd);
    }

    free(session->original_channel);
    free(session->stripped_channel);
    aeron_free(session);

    return 0;
}

void aeron_archive_update_channel_session_abort(
    aeron_archive_update_channel_session_t *session, const char *reason)
{
    (void)reason;
    session->is_done = true;
}

bool aeron_archive_update_channel_session_is_done(const aeron_archive_update_channel_session_t *session)
{
    return session->is_done;
}

int64_t aeron_archive_update_channel_session_session_id(const aeron_archive_update_channel_session_t *session)
{
    return session->correlation_id;
}
