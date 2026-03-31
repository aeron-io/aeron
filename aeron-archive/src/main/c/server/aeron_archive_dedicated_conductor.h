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

#ifndef AERON_ARCHIVE_DEDICATED_CONDUCTOR_H
#define AERON_ARCHIVE_DEDICATED_CONDUCTOR_H

#include <stdint.h>
#include <stdbool.h>

#include "aeron_archive_conductor.h"
#include "aeron_archive_session_worker.h"
#include "aeron_archive_recording_session.h"
#include "aeron_archive_replay_session.h"
#include "aeron_agent.h"
#include "concurrent/aeron_mpsc_concurrent_array_queue.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define AERON_ARCHIVE_DEDICATED_CONDUCTOR_COMMAND_LIMIT 10
#define AERON_ARCHIVE_DEDICATED_CONDUCTOR_SESSION_QUEUE_CAPACITY 256

/**
 * Types for entries placed on the close queue, so the conductor can dispatch
 * to the correct close handler.
 */
typedef enum aeron_archive_dedicated_close_entry_type_en
{
    AERON_ARCHIVE_DEDICATED_CLOSE_RECORDING = 0,
    AERON_ARCHIVE_DEDICATED_CLOSE_REPLAY = 1,
    AERON_ARCHIVE_DEDICATED_CLOSE_OTHER = 2
}
aeron_archive_dedicated_close_entry_type_t;

/**
 * Entry placed on the close queue for thread-safe session close handoff
 * from worker threads back to the conductor.
 */
typedef struct aeron_archive_dedicated_close_entry_stct
{
    void *session;
    aeron_archive_dedicated_close_entry_type_t type;
}
aeron_archive_dedicated_close_entry_t;

/**
 * State for the dedicated mode recorder worker.
 * Runs recording sessions on its own thread with a session worker.
 * New sessions are delivered via an MPSC queue from the conductor.
 * Completed sessions are forwarded to the close queue for conductor-thread cleanup.
 */
typedef struct aeron_archive_dedicated_recorder_stct
{
    aeron_archive_session_worker_t session_worker;

    /* Queue for new recording sessions added from the conductor thread */
    aeron_mpsc_concurrent_array_queue_t sessions_queue;

    /* Queue for completed sessions forwarded back to the conductor for cleanup */
    aeron_mpsc_concurrent_array_queue_t *close_queue;

    volatile bool is_abort;
}
aeron_archive_dedicated_recorder_t;

/**
 * State for the dedicated mode replayer worker.
 * Runs replay sessions on its own thread with a session worker.
 * Same pattern as the recorder but for replay sessions.
 */
typedef struct aeron_archive_dedicated_replayer_stct
{
    aeron_archive_session_worker_t session_worker;

    /* Queue for new replay sessions added from the conductor thread */
    aeron_mpsc_concurrent_array_queue_t sessions_queue;

    /* Queue for completed sessions forwarded back to the conductor for cleanup */
    aeron_mpsc_concurrent_array_queue_t *close_queue;

    volatile bool is_abort;
}
aeron_archive_dedicated_replayer_t;

/**
 * The dedicated mode archive conductor. Extends the base archive conductor
 * with separate recorder and replayer threads.
 *
 * Recording sessions run on the recorder thread, replay sessions on the
 * replayer thread. Control requests are still processed on the conductor thread.
 * Thread-safe handoff of sessions is accomplished via MPSC queues.
 *
 * Mirrors the Java DedicatedModeArchiveConductor.
 */
typedef struct aeron_archive_dedicated_conductor_stct
{
    /* Base archive conductor (must be first for lifecycle compatibility) */
    aeron_archive_conductor_t *conductor;
    aeron_archive_conductor_context_t *ctx;

    /* Recorder: runs recording sessions on a dedicated thread */
    aeron_archive_dedicated_recorder_t recorder;
    aeron_agent_runner_t recorder_runner;

    /* Replayer: runs replay sessions on a dedicated thread */
    aeron_archive_dedicated_replayer_t replayer;
    aeron_agent_runner_t replayer_runner;

    /* Close queue: completed sessions from worker threads back to conductor */
    aeron_mpsc_concurrent_array_queue_t close_queue;
}
aeron_archive_dedicated_conductor_t;

/* ---- Lifecycle ---- */

/**
 * Create and initialise the dedicated mode archive conductor.
 *
 * @param dedicated  out param for the allocated dedicated conductor.
 * @param ctx        the conductor context with all configuration.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_dedicated_conductor_create(
    aeron_archive_dedicated_conductor_t **dedicated,
    aeron_archive_conductor_context_t *ctx);

/**
 * Start the dedicated mode conductor: initialise the base conductor and
 * launch the recorder and replayer agent runners on dedicated threads.
 *
 * @param dedicated the dedicated mode conductor.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_dedicated_conductor_on_start(aeron_archive_dedicated_conductor_t *dedicated);

/**
 * Perform a unit of work on the conductor thread: drain the close queue
 * and then perform the base conductor's do_work.
 *
 * @param dedicated the dedicated mode conductor.
 * @return the amount of work done, or -1 on terminal error.
 */
int aeron_archive_dedicated_conductor_do_work(aeron_archive_dedicated_conductor_t *dedicated);

/**
 * Close the dedicated mode conductor: stop the recorder and replayer
 * agent runners, drain any remaining close queue entries, then close
 * the base conductor.
 *
 * @param dedicated the dedicated mode conductor. May be NULL.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_dedicated_conductor_close(aeron_archive_dedicated_conductor_t *dedicated);

/**
 * Signal abort to the dedicated mode conductor and its worker threads.
 *
 * @param dedicated the dedicated mode conductor.
 */
void aeron_archive_dedicated_conductor_abort(aeron_archive_dedicated_conductor_t *dedicated);

/* ---- Session handoff (called from conductor thread) ---- */

/**
 * Add a recording session to the recorder's queue for processing on the
 * recorder thread. Called from the conductor thread.
 *
 * @param dedicated the dedicated mode conductor.
 * @param session   the recording session to hand off.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_dedicated_conductor_add_recording_session(
    aeron_archive_dedicated_conductor_t *dedicated,
    aeron_archive_recording_session_t *session);

/**
 * Add a replay session to the replayer's queue for processing on the
 * replayer thread. Called from the conductor thread.
 *
 * @param dedicated the dedicated mode conductor.
 * @param session   the replay session to hand off.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_dedicated_conductor_add_replay_session(
    aeron_archive_dedicated_conductor_t *dedicated,
    aeron_archive_replay_session_t *session);

/* ---- Recorder agent functions (for aeron_agent_runner_t) ---- */

/**
 * Recorder do_work: drain the sessions queue and drive all recording sessions.
 *
 * @param state the recorder state (aeron_archive_dedicated_recorder_t *).
 * @return the amount of work done.
 */
int aeron_archive_dedicated_recorder_do_work(void *state);

/**
 * Recorder on_close: close the session worker and its sessions.
 *
 * @param state the recorder state (aeron_archive_dedicated_recorder_t *).
 */
void aeron_archive_dedicated_recorder_on_close(void *state);

/* ---- Replayer agent functions (for aeron_agent_runner_t) ---- */

/**
 * Replayer do_work: drain the sessions queue and drive all replay sessions.
 *
 * @param state the replayer state (aeron_archive_dedicated_replayer_t *).
 * @return the amount of work done.
 */
int aeron_archive_dedicated_replayer_do_work(void *state);

/**
 * Replayer on_close: close the session worker and its sessions.
 *
 * @param state the replayer state (aeron_archive_dedicated_replayer_t *).
 */
void aeron_archive_dedicated_replayer_on_close(void *state);

#ifdef __cplusplus
}
#endif

#endif /* AERON_ARCHIVE_DEDICATED_CONDUCTOR_H */
