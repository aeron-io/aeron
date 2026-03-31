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

#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "aeron_archive_session_worker.h"
#include "util/aeron_error.h"
#include "aeron_alloc.h"

#define AERON_ARCHIVE_SESSION_WORKER_INITIAL_CAPACITY 16

int aeron_archive_session_worker_init(
    aeron_archive_session_worker_t *worker,
    const char *role_name,
    aeron_error_handler_t error_handler,
    void *error_handler_clientd,
    aeron_archive_session_worker_close_session_func_t close_session_func,
    void *close_session_clientd)
{
    if (NULL == worker)
    {
        AERON_SET_ERR(EINVAL, "%s", "worker is NULL");
        return -1;
    }

    memset(worker, 0, sizeof(aeron_archive_session_worker_t));

    worker->role_name = role_name;
    worker->error_handler = error_handler;
    worker->error_handler_clientd = error_handler_clientd;
    worker->close_session_func = close_session_func;
    worker->close_session_clientd = close_session_clientd;

    worker->session_count = 0;
    worker->session_capacity = AERON_ARCHIVE_SESSION_WORKER_INITIAL_CAPACITY;

    if (aeron_alloc(
        (void **)&worker->sessions,
        sizeof(aeron_archive_session_worker_entry_t) * (size_t)worker->session_capacity) < 0)
    {
        AERON_SET_ERR(ENOMEM, "%s", "failed to allocate session worker sessions array");
        return -1;
    }

    worker->is_closed = false;

    return 0;
}

int aeron_archive_session_worker_do_work(aeron_archive_session_worker_t *worker)
{
    int work_count = 0;
    int32_t last_index = worker->session_count - 1;

    for (int32_t i = last_index; i >= 0; i--)
    {
        aeron_archive_session_worker_entry_t *entry = &worker->sessions[i];

        int result = entry->do_work(entry->session);
        if (result > 0)
        {
            work_count += result;
        }

        if (entry->is_done(entry->session))
        {
            /* Fast unordered remove: swap with last element */
            if (NULL != worker->close_session_func)
            {
                worker->close_session_func(worker->close_session_clientd, entry->session);
            }
            else
            {
                /* Default: just close the session directly */
                entry->close(entry->session);
            }

            if (i != last_index)
            {
                worker->sessions[i] = worker->sessions[last_index];
            }

            last_index--;
            worker->session_count--;
        }
    }

    return work_count;
}

int aeron_archive_session_worker_on_close(aeron_archive_session_worker_t *worker)
{
    if (NULL == worker || worker->is_closed)
    {
        return 0;
    }

    worker->is_closed = true;

    /* Pre-sessions close hook */
    if (NULL != worker->pre_sessions_close_func)
    {
        worker->pre_sessions_close_func(worker->pre_sessions_close_clientd);
    }

    /* Close all remaining sessions */
    for (int32_t i = 0; i < worker->session_count; i++)
    {
        aeron_archive_session_worker_entry_t *entry = &worker->sessions[i];

        if (NULL != worker->close_session_func)
        {
            worker->close_session_func(worker->close_session_clientd, entry->session);
        }
        else
        {
            entry->close(entry->session);
        }
    }

    worker->session_count = 0;

    /* Post-sessions close hook */
    if (NULL != worker->post_sessions_close_func)
    {
        worker->post_sessions_close_func(worker->post_sessions_close_clientd);
    }

    return 0;
}

int aeron_archive_session_worker_add_session(
    aeron_archive_session_worker_t *worker,
    void *session,
    aeron_archive_session_do_work_func_t do_work,
    aeron_archive_session_is_done_func_t is_done,
    aeron_archive_session_close_func_t close)
{
    if (NULL == worker || NULL == session)
    {
        AERON_SET_ERR(EINVAL, "%s", "worker or session is NULL");
        return -1;
    }

    if (worker->session_count >= worker->session_capacity)
    {
        int32_t new_capacity = worker->session_capacity * 2;
        aeron_archive_session_worker_entry_t *new_sessions = NULL;

        if (aeron_alloc(
            (void **)&new_sessions,
            sizeof(aeron_archive_session_worker_entry_t) * (size_t)new_capacity) < 0)
        {
            AERON_SET_ERR(ENOMEM, "%s", "failed to grow session worker sessions array");
            return -1;
        }

        memcpy(new_sessions, worker->sessions,
            sizeof(aeron_archive_session_worker_entry_t) * (size_t)worker->session_count);
        aeron_free(worker->sessions);
        worker->sessions = new_sessions;
        worker->session_capacity = new_capacity;
    }

    aeron_archive_session_worker_entry_t *entry = &worker->sessions[worker->session_count];
    entry->session = session;
    entry->do_work = do_work;
    entry->is_done = is_done;
    entry->close = close;

    worker->session_count++;

    return 0;
}

int32_t aeron_archive_session_worker_session_count(const aeron_archive_session_worker_t *worker)
{
    return NULL != worker ? worker->session_count : 0;
}

void aeron_archive_session_worker_fini(aeron_archive_session_worker_t *worker)
{
    if (NULL != worker)
    {
        aeron_free(worker->sessions);
        worker->sessions = NULL;
        worker->session_count = 0;
        worker->session_capacity = 0;
    }
}
