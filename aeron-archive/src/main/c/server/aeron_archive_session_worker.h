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

#ifndef AERON_ARCHIVE_SESSION_WORKER_H
#define AERON_ARCHIVE_SESSION_WORKER_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#include "aeronc.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Generic session interface used by the session worker.
 * Each concrete session type (recording, replay) provides its own do_work, is_done, and close
 * implementations via function pointers.
 */
typedef int (*aeron_archive_session_do_work_func_t)(void *session);
typedef bool (*aeron_archive_session_is_done_func_t)(const void *session);
typedef int (*aeron_archive_session_close_func_t)(void *session);

/**
 * An entry in the session worker's session array, wrapping a concrete session with
 * function pointers for the generic session interface.
 */
typedef struct aeron_archive_session_worker_entry_stct
{
    void *session;
    aeron_archive_session_do_work_func_t do_work;
    aeron_archive_session_is_done_func_t is_done;
    aeron_archive_session_close_func_t close;
}
aeron_archive_session_worker_entry_t;

/**
 * Callback invoked when a session completes (is_done returns true) and is removed
 * from the worker. The implementation may forward the session to a close queue
 * for processing on the conductor thread.
 */
typedef void (*aeron_archive_session_worker_close_session_func_t)(void *clientd, void *session);

/**
 * Callback for pre-sessions-close hook, invoked before closing all sessions during shutdown.
 */
typedef void (*aeron_archive_session_worker_hook_func_t)(void *clientd);

/**
 * A generic agent that drives a set of sessions on its own thread.
 * Mirrors the Java SessionWorker<T> class.
 *
 * Holds an array of sessions, iterates them calling do_work, removes completed ones.
 * Used for both the recorder and replayer threads in dedicated threading mode.
 */
typedef struct aeron_archive_session_worker_stct
{
    const char *role_name;

    /* Dynamic array of active sessions */
    aeron_archive_session_worker_entry_t *sessions;
    int32_t session_count;
    int32_t session_capacity;

    /* Error handler */
    aeron_error_handler_t error_handler;
    void *error_handler_clientd;

    /* Callback when a session is done and needs closing */
    aeron_archive_session_worker_close_session_func_t close_session_func;
    void *close_session_clientd;

    /* Optional hooks for subclass behavior */
    aeron_archive_session_worker_hook_func_t pre_sessions_close_func;
    void *pre_sessions_close_clientd;
    aeron_archive_session_worker_hook_func_t post_sessions_close_func;
    void *post_sessions_close_clientd;

    bool is_closed;
}
aeron_archive_session_worker_t;

/**
 * Initialise a session worker.
 *
 * @param worker                    out param, must point to pre-allocated storage.
 * @param role_name                 name for this agent (e.g. "archive-recorder", "archive-replayer").
 * @param error_handler             error handler callback.
 * @param error_handler_clientd     client data for error handler.
 * @param close_session_func        callback when a session completes.
 * @param close_session_clientd     client data for the close callback.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_session_worker_init(
    aeron_archive_session_worker_t *worker,
    const char *role_name,
    aeron_error_handler_t error_handler,
    void *error_handler_clientd,
    aeron_archive_session_worker_close_session_func_t close_session_func,
    void *close_session_clientd);

/**
 * Perform a unit of work: iterate sessions, call do_work, remove completed ones.
 *
 * @param worker the session worker.
 * @return the amount of work done.
 */
int aeron_archive_session_worker_do_work(aeron_archive_session_worker_t *worker);

/**
 * Close the session worker and all its sessions.
 * Calls pre_sessions_close, closes each session, then calls post_sessions_close.
 *
 * @param worker the session worker.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_session_worker_on_close(aeron_archive_session_worker_t *worker);

/**
 * Add a session to the worker's session array.
 *
 * @param worker  the session worker.
 * @param session the opaque session pointer.
 * @param do_work function to call for do_work on this session.
 * @param is_done function to check if the session is done.
 * @param close   function to close the session.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_session_worker_add_session(
    aeron_archive_session_worker_t *worker,
    void *session,
    aeron_archive_session_do_work_func_t do_work,
    aeron_archive_session_is_done_func_t is_done,
    aeron_archive_session_close_func_t close);

/**
 * Get the number of active sessions.
 *
 * @param worker the session worker.
 * @return the session count.
 */
int32_t aeron_archive_session_worker_session_count(const aeron_archive_session_worker_t *worker);

/**
 * Free resources held by the session worker (the sessions array).
 * Should be called after on_close.
 *
 * @param worker the session worker.
 */
void aeron_archive_session_worker_fini(aeron_archive_session_worker_t *worker);

#ifdef __cplusplus
}
#endif

#endif /* AERON_ARCHIVE_SESSION_WORKER_H */
