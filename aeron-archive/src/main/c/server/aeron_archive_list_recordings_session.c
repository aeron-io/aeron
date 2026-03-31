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
#include "aeron_archive_list_recordings_session.h"

/**
 * Accept-all filter for ListRecordingsSession (all recordings match).
 */
static bool accept_all(
    const aeron_archive_catalog_recording_descriptor_t *descriptor,
    const aeron_archive_list_recordings_session_t *session)
{
    (void)session;
    (void)descriptor;
    return true;
}

/**
 * Filter for ListRecordingsForUriSession: match stream id and channel fragment.
 */
static bool accept_for_uri(
    const aeron_archive_catalog_recording_descriptor_t *descriptor,
    const aeron_archive_list_recordings_session_t *session)
{
    if (descriptor->stream_id != session->filter_stream_id)
    {
        return false;
    }

    if (NULL != session->filter_channel_fragment && NULL != descriptor->original_channel)
    {
        return NULL != strstr(descriptor->original_channel, session->filter_channel_fragment);
    }

    return NULL == session->filter_channel_fragment;
}

typedef bool (*accept_descriptor_func_t)(
    const aeron_archive_catalog_recording_descriptor_t *descriptor,
    const aeron_archive_list_recordings_session_t *session);

/**
 * Internal iteration context for catalog for_each callbacks.
 */
typedef struct list_recordings_iterate_context_stct
{
    aeron_archive_list_recordings_session_t *session;
    accept_descriptor_func_t accept_func;
    int32_t records_scanned;
    bool stopped;
}
list_recordings_iterate_context_t;

/**
 * Common initialiser for all list-recordings session types.
 */
static int list_recordings_session_init(
    aeron_archive_list_recordings_session_t **session,
    int64_t correlation_id,
    int64_t from_recording_id,
    int32_t count,
    aeron_archive_list_recordings_type_t type,
    aeron_archive_catalog_t *catalog,
    aeron_archive_list_recordings_send_descriptor_func_t send_descriptor,
    aeron_archive_list_recordings_send_unknown_func_t send_unknown,
    void *callback_clientd)
{
    aeron_archive_list_recordings_session_t *_session = NULL;

    if (aeron_alloc((void **)&_session, sizeof(aeron_archive_list_recordings_session_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_list_recordings_session_t");
        return -1;
    }

    _session->correlation_id = correlation_id;
    _session->recording_id = from_recording_id;
    _session->count = count;
    _session->sent = 0;
    _session->type = type;
    _session->filter_stream_id = 0;
    _session->filter_channel_fragment = NULL;
    _session->catalog = catalog;
    _session->send_descriptor = send_descriptor;
    _session->send_unknown = send_unknown;
    _session->callback_clientd = callback_clientd;
    _session->is_done = false;

    *session = _session;

    return 0;
}

int aeron_archive_list_recordings_session_create(
    aeron_archive_list_recordings_session_t **session,
    int64_t correlation_id,
    int64_t from_recording_id,
    int32_t count,
    aeron_archive_catalog_t *catalog,
    aeron_archive_list_recordings_send_descriptor_func_t send_descriptor,
    aeron_archive_list_recordings_send_unknown_func_t send_unknown,
    void *callback_clientd)
{
    return list_recordings_session_init(
        session, correlation_id, from_recording_id, count,
        AERON_ARCHIVE_LIST_RECORDINGS_ALL,
        catalog, send_descriptor, send_unknown, callback_clientd);
}

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
    void *callback_clientd)
{
    int result = list_recordings_session_init(
        session, correlation_id, from_recording_id, count,
        AERON_ARCHIVE_LIST_RECORDINGS_FOR_URI,
        catalog, send_descriptor, send_unknown, callback_clientd);

    if (0 == result)
    {
        (*session)->filter_stream_id = stream_id;

        if (NULL != channel_fragment)
        {
            (*session)->filter_channel_fragment = strdup(channel_fragment);
            if (NULL == (*session)->filter_channel_fragment)
            {
                aeron_free(*session);
                *session = NULL;
                AERON_APPEND_ERR("%s", "Unable to copy channel_fragment");
                return -1;
            }
        }
    }

    return result;
}

int aeron_archive_list_recording_by_id_session_create(
    aeron_archive_list_recordings_session_t **session,
    int64_t correlation_id,
    int64_t recording_id,
    aeron_archive_catalog_t *catalog,
    aeron_archive_list_recordings_send_descriptor_func_t send_descriptor,
    aeron_archive_list_recordings_send_unknown_func_t send_unknown,
    void *callback_clientd)
{
    return list_recordings_session_init(
        session, correlation_id, recording_id, 1,
        AERON_ARCHIVE_LIST_RECORDING_BY_ID,
        catalog, send_descriptor, send_unknown, callback_clientd);
}

/**
 * doWork for the BY_ID variant: look up a single recording by id.
 */
static int do_work_by_id(aeron_archive_list_recordings_session_t *session)
{
    if (session->is_done)
    {
        return 0;
    }

    aeron_archive_catalog_recording_descriptor_t descriptor;
    memset(&descriptor, 0, sizeof(descriptor));

    if (aeron_archive_catalog_find_recording(session->catalog, session->recording_id, &descriptor) == 0)
    {
        if (session->send_descriptor(session->correlation_id, &descriptor, session->callback_clientd))
        {
            session->is_done = true;
        }
    }
    else
    {
        session->send_unknown(session->correlation_id, session->recording_id, session->callback_clientd);
        session->is_done = true;
    }

    return 1;
}

/**
 * Catalog iteration callback used by the range-based list recordings sessions.
 * Scans entries starting from recording_id, applies filter, and sends descriptors.
 */
static void list_recordings_catalog_callback(
    const aeron_archive_catalog_recording_descriptor_t *descriptor,
    void *clientd)
{
    list_recordings_iterate_context_t *ctx = (list_recordings_iterate_context_t *)clientd;
    aeron_archive_list_recordings_session_t *session = ctx->session;

    if (ctx->stopped || session->is_done)
    {
        ctx->stopped = true;
        return;
    }

    /* Skip entries before our current recording_id */
    if (descriptor->recording_id < session->recording_id)
    {
        return;
    }

    ctx->records_scanned++;

    if (ctx->records_scanned > AERON_ARCHIVE_LIST_RECORDINGS_MAX_SCANS_PER_WORK_CYCLE)
    {
        ctx->stopped = true;
        return;
    }

    if (session->sent >= session->count)
    {
        session->is_done = true;
        ctx->stopped = true;
        return;
    }

    /* Select the appropriate filter function */
    accept_descriptor_func_t accept_func = ctx->accept_func;

    if (accept_func(descriptor, session))
    {
        if (!session->send_descriptor(session->correlation_id, descriptor, session->callback_clientd))
        {
            session->is_done = true;
            ctx->stopped = true;
            return;
        }

        session->sent++;
    }

    /* Track the next recording_id to resume from */
    session->recording_id = descriptor->recording_id + 1;
}

/**
 * doWork for ALL and FOR_URI variants: iterate catalog entries with filtering.
 */
static int do_work_range(aeron_archive_list_recordings_session_t *session)
{
    if (session->is_done)
    {
        return 0;
    }

    accept_descriptor_func_t accept_func;

    switch (session->type)
    {
        case AERON_ARCHIVE_LIST_RECORDINGS_FOR_URI:
            accept_func = accept_for_uri;
            break;

        case AERON_ARCHIVE_LIST_RECORDINGS_ALL:
        default:
            accept_func = accept_all;
            break;
    }

    list_recordings_iterate_context_t ctx;
    ctx.session = session;
    ctx.accept_func = accept_func;
    ctx.records_scanned = 0;
    ctx.stopped = false;

    const int32_t iterated = aeron_archive_catalog_for_each(
        session->catalog, list_recordings_catalog_callback, &ctx);

    /*
     * If iteration completed without finding enough records and we haven't
     * sent the requested count, send recording-unknown.
     */
    if (!session->is_done && !ctx.stopped && session->sent < session->count)
    {
        session->send_unknown(session->correlation_id, session->recording_id, session->callback_clientd);
        session->is_done = true;
    }

    if (session->sent >= session->count)
    {
        session->is_done = true;
    }

    return iterated > 0 ? iterated : 0;
}

int aeron_archive_list_recordings_session_do_work(aeron_archive_list_recordings_session_t *session)
{
    if (session->is_done)
    {
        return 0;
    }

    switch (session->type)
    {
        case AERON_ARCHIVE_LIST_RECORDING_BY_ID:
            return do_work_by_id(session);

        case AERON_ARCHIVE_LIST_RECORDINGS_ALL:
        case AERON_ARCHIVE_LIST_RECORDINGS_FOR_URI:
        default:
            return do_work_range(session);
    }
}

int aeron_archive_list_recordings_session_close(aeron_archive_list_recordings_session_t *session)
{
    if (NULL == session)
    {
        return 0;
    }

    free(session->filter_channel_fragment);
    aeron_free(session);

    return 0;
}

void aeron_archive_list_recordings_session_abort(
    aeron_archive_list_recordings_session_t *session, const char *reason)
{
    (void)reason;
    session->is_done = true;
}

bool aeron_archive_list_recordings_session_is_done(const aeron_archive_list_recordings_session_t *session)
{
    return session->is_done;
}

int64_t aeron_archive_list_recordings_session_session_id(const aeron_archive_list_recordings_session_t *session)
{
    return session->correlation_id;
}
