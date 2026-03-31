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
#include <stdio.h>
#include <errno.h>
#include <limits.h>

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "aeron_archive_delete_segments_session.h"
#include "aeron_archive_conductor.h"

/**
 * Parse the segment position from a segment file name.
 *
 * Segment file names have the form: <recordingId>-<position>.rec
 * e.g. "42-131072.rec"
 *
 * @param file_name    the file name (basename only).
 * @param recording_id the recording id to compute the prefix length.
 * @return the parsed position, or INT64_MIN on parse failure.
 */
static int64_t parse_segment_position(const char *file_name, int64_t recording_id)
{
    /* Find the dash separator after the recording id prefix */
    const char *dash = strchr(file_name, '-');
    if (NULL == dash)
    {
        return INT64_MIN;
    }

    const char *pos_start = dash + 1;

    /* Find the dot before the extension */
    const char *dot = strchr(pos_start, '.');
    if (NULL == dot || dot == pos_start)
    {
        return INT64_MIN;
    }

    char buf[32];
    const size_t len = (size_t)(dot - pos_start);
    if (len >= sizeof(buf))
    {
        return INT64_MIN;
    }

    memcpy(buf, pos_start, len);
    buf[len] = '\0';

    char *endptr = NULL;
    const int64_t position = (int64_t)strtoll(buf, &endptr, 10);
    if (endptr != buf + len)
    {
        return INT64_MIN;
    }

    return position;
}

/**
 * Extract the basename from a file path.
 */
static const char *basename_of(const char *path)
{
    const char *last_sep = strrchr(path, '/');
    if (NULL != last_sep)
    {
        return last_sep + 1;
    }

#if defined(_WIN32)
    last_sep = strrchr(path, '\\');
    if (NULL != last_sep)
    {
        return last_sep + 1;
    }
#endif

    return path;
}

int aeron_archive_delete_segments_session_create(
    aeron_archive_delete_segments_session_t **session,
    int64_t recording_id,
    int64_t correlation_id,
    aeron_archive_delete_segments_send_signal_func_t send_signal,
    aeron_archive_delete_segments_send_error_func_t send_error,
    aeron_archive_delete_segments_remove_func_t remove_session,
    aeron_archive_delete_segments_error_handler_func_t error_handler,
    void *callback_clientd,
    void *error_handler_clientd)
{
    aeron_archive_delete_segments_session_t *_session = NULL;

    if (aeron_alloc((void **)&_session, sizeof(aeron_archive_delete_segments_session_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_delete_segments_session_t");
        return -1;
    }

    _session->recording_id = recording_id;
    _session->correlation_id = correlation_id;
    _session->max_delete_position = INT64_MIN;
    _session->file_queue_head = NULL;
    _session->file_queue_tail = NULL;
    _session->file_count = 0;
    _session->send_signal = send_signal;
    _session->send_error = send_error;
    _session->remove_session = remove_session;
    _session->error_handler = error_handler;
    _session->callback_clientd = callback_clientd;
    _session->error_handler_clientd = error_handler_clientd;

    *session = _session;

    return 0;
}

int aeron_archive_delete_segments_session_add_file(
    aeron_archive_delete_segments_session_t *session,
    const char *file_path)
{
    aeron_archive_delete_segments_file_entry_t *entry = NULL;

    if (aeron_alloc((void **)&entry, sizeof(aeron_archive_delete_segments_file_entry_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate delete segments file entry");
        return -1;
    }

    entry->file_path = strdup(file_path);
    if (NULL == entry->file_path)
    {
        aeron_free(entry);
        AERON_APPEND_ERR("%s", "Unable to copy file_path");
        return -1;
    }

    entry->next = NULL;

    if (NULL == session->file_queue_tail)
    {
        session->file_queue_head = entry;
        session->file_queue_tail = entry;
    }
    else
    {
        session->file_queue_tail->next = entry;
        session->file_queue_tail = entry;
    }

    session->file_count++;

    return 0;
}

void aeron_archive_delete_segments_session_finalise(
    aeron_archive_delete_segments_session_t *session)
{
    int64_t max_position = INT64_MIN;

    aeron_archive_delete_segments_file_entry_t *entry = session->file_queue_head;
    while (NULL != entry)
    {
        const char *name = basename_of(entry->file_path);
        const int64_t position = parse_segment_position(name, session->recording_id);

        if (position > max_position)
        {
            max_position = position;
        }

        entry = entry->next;
    }

    session->max_delete_position = max_position;
}

static void on_delete_error(
    aeron_archive_delete_segments_session_t *session,
    const char *file_path)
{
    char error_message[512];
    snprintf(error_message, sizeof(error_message), "unable to delete segment file: %s", file_path);

    if (NULL != session->send_error)
    {
        session->send_error(
            session->correlation_id,
            AERON_ARCHIVE_ERROR_GENERIC,
            error_message,
            session->callback_clientd);
    }

    if (NULL != session->error_handler)
    {
        session->error_handler(error_message, session->error_handler_clientd);
    }
}

/**
 * Check if a file exists on disk.
 */
static bool file_exists(const char *path)
{
    FILE *f = fopen(path, "r");
    if (NULL != f)
    {
        fclose(f);
        return true;
    }
    return false;
}

int aeron_archive_delete_segments_session_do_work(aeron_archive_delete_segments_session_t *session)
{
    int work_count = 0;

    aeron_archive_delete_segments_file_entry_t *entry = session->file_queue_head;
    if (NULL != entry)
    {
        /* Dequeue the head entry */
        session->file_queue_head = entry->next;
        if (NULL == session->file_queue_head)
        {
            session->file_queue_tail = NULL;
        }
        session->file_count--;

        if (0 != remove(entry->file_path))
        {
            if (file_exists(entry->file_path))
            {
                on_delete_error(session, entry->file_path);
            }
            else
            {
                /* Check if the file was already renamed with .del suffix */
                const size_t path_len = strlen(entry->file_path);
                const size_t suffix_len = strlen(AERON_ARCHIVE_DELETE_SUFFIX);

                if (path_len < suffix_len ||
                    0 != strcmp(entry->file_path + path_len - suffix_len, AERON_ARCHIVE_DELETE_SUFFIX))
                {
                    /* Try the .del-suffixed version */
                    char renamed_path[PATH_MAX];
                    snprintf(renamed_path, sizeof(renamed_path), "%s%s",
                        entry->file_path, AERON_ARCHIVE_DELETE_SUFFIX);

                    if (0 != remove(renamed_path) && file_exists(renamed_path))
                    {
                        on_delete_error(session, renamed_path);
                    }
                }
            }
        }

        free(entry->file_path);
        aeron_free(entry);

        work_count = 1;
    }

    return work_count;
}

int aeron_archive_delete_segments_session_close(aeron_archive_delete_segments_session_t *session)
{
    if (NULL == session)
    {
        return 0;
    }

    if (NULL != session->remove_session)
    {
        session->remove_session(session, session->callback_clientd);
    }

    if (NULL != session->send_signal)
    {
        session->send_signal(
            session->correlation_id,
            session->recording_id,
            AERON_ARCHIVE_NULL_VALUE,
            AERON_ARCHIVE_NULL_VALUE,
            session->callback_clientd);
    }

    /* Free any remaining file entries */
    aeron_archive_delete_segments_file_entry_t *entry = session->file_queue_head;
    while (NULL != entry)
    {
        aeron_archive_delete_segments_file_entry_t *next = entry->next;
        free(entry->file_path);
        aeron_free(entry);
        entry = next;
    }

    aeron_free(session);

    return 0;
}

void aeron_archive_delete_segments_session_abort(
    aeron_archive_delete_segments_session_t *session, const char *reason)
{
    (void)session;
    (void)reason;
    /* No-op: the Java version also does nothing on abort. */
}

bool aeron_archive_delete_segments_session_is_done(const aeron_archive_delete_segments_session_t *session)
{
    return NULL == session->file_queue_head;
}

int64_t aeron_archive_delete_segments_session_session_id(const aeron_archive_delete_segments_session_t *session)
{
    return session->recording_id;
}

int64_t aeron_archive_delete_segments_session_max_delete_position(
    const aeron_archive_delete_segments_session_t *session)
{
    return session->max_delete_position;
}
