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

#include "aeron_archive_dedicated_conductor.h"
#include "util/aeron_error.h"
#include "aeron_alloc.h"
#include "concurrent/aeron_thread.h"
#include "concurrent/aeron_atomic.h"

/* ---- Forward declarations for recording session wrapper functions ---- */

static int aeron_archive_dedicated_recording_session_do_work_wrapper(void *session)
{
    return aeron_archive_recording_session_do_work((aeron_archive_recording_session_t *)session);
}

static bool aeron_archive_dedicated_recording_session_is_done_wrapper(const void *session)
{
    return aeron_archive_recording_session_is_done((const aeron_archive_recording_session_t *)session);
}

static int aeron_archive_dedicated_recording_session_close_wrapper(void *session)
{
    return aeron_archive_recording_session_close((aeron_archive_recording_session_t *)session);
}

/* ---- Forward declarations for replay session wrapper functions ---- */

static int aeron_archive_dedicated_replay_session_do_work_wrapper(void *session)
{
    return aeron_archive_replay_session_do_work((aeron_archive_replay_session_t *)session, 0);
}

static bool aeron_archive_dedicated_replay_session_is_done_wrapper(const void *session)
{
    return aeron_archive_replay_session_is_done((const aeron_archive_replay_session_t *)session);
}

static int aeron_archive_dedicated_replay_session_close_wrapper(void *session)
{
    return aeron_archive_replay_session_close((aeron_archive_replay_session_t *)session);
}

/* ---- Close queue helpers ---- */

/**
 * Allocate a close entry and offer it to the close queue.
 * Spins until the offer succeeds (matching Java behavior of while(!offer)).
 */
static void aeron_archive_dedicated_offer_to_close_queue(
    aeron_mpsc_concurrent_array_queue_t *close_queue,
    void *session,
    aeron_archive_dedicated_close_entry_type_t type)
{
    aeron_archive_dedicated_close_entry_t *entry = NULL;

    if (aeron_alloc((void **)&entry, sizeof(aeron_archive_dedicated_close_entry_t)) < 0)
    {
        return;
    }

    entry->session = session;
    entry->type = type;

    while (AERON_OFFER_SUCCESS != aeron_mpsc_concurrent_array_queue_offer(close_queue, entry))
    {
        aeron_micro_sleep(1);
    }
}

/* ---- Recorder close session callback ---- */

static void aeron_archive_dedicated_recorder_close_session_callback(void *clientd, void *session)
{
    aeron_archive_dedicated_recorder_t *recorder = (aeron_archive_dedicated_recorder_t *)clientd;
    aeron_archive_dedicated_offer_to_close_queue(
        recorder->close_queue, session, AERON_ARCHIVE_DEDICATED_CLOSE_RECORDING);
}

/* ---- Replayer close session callback ---- */

static void aeron_archive_dedicated_replayer_close_session_callback(void *clientd, void *session)
{
    aeron_archive_dedicated_replayer_t *replayer = (aeron_archive_dedicated_replayer_t *)clientd;
    aeron_archive_dedicated_offer_to_close_queue(
        replayer->close_queue, session, AERON_ARCHIVE_DEDICATED_CLOSE_REPLAY);
}

/* ---- Drain helpers for sessions queue ---- */

typedef struct aeron_archive_dedicated_drain_recording_context_stct
{
    aeron_archive_session_worker_t *worker;
}
aeron_archive_dedicated_drain_recording_context_t;

static void aeron_archive_dedicated_drain_recording_func(void *clientd, void *element)
{
    aeron_archive_dedicated_drain_recording_context_t *ctx =
        (aeron_archive_dedicated_drain_recording_context_t *)clientd;

    aeron_archive_session_worker_add_session(
        ctx->worker,
        element,
        aeron_archive_dedicated_recording_session_do_work_wrapper,
        aeron_archive_dedicated_recording_session_is_done_wrapper,
        aeron_archive_dedicated_recording_session_close_wrapper);
}

typedef struct aeron_archive_dedicated_drain_replay_context_stct
{
    aeron_archive_session_worker_t *worker;
}
aeron_archive_dedicated_drain_replay_context_t;

static void aeron_archive_dedicated_drain_replay_func(void *clientd, void *element)
{
    aeron_archive_dedicated_drain_replay_context_t *ctx =
        (aeron_archive_dedicated_drain_replay_context_t *)clientd;

    aeron_archive_session_worker_add_session(
        ctx->worker,
        element,
        aeron_archive_dedicated_replay_session_do_work_wrapper,
        aeron_archive_dedicated_replay_session_is_done_wrapper,
        aeron_archive_dedicated_replay_session_close_wrapper);
}

/* ---- Recorder pre-sessions-close hook ---- */

static void aeron_archive_dedicated_recorder_pre_sessions_close(void *clientd)
{
    aeron_archive_dedicated_recorder_t *recorder = (aeron_archive_dedicated_recorder_t *)clientd;
    aeron_archive_dedicated_drain_recording_context_t ctx;
    ctx.worker = &recorder->session_worker;

    aeron_mpsc_concurrent_array_queue_drain_all(
        &recorder->sessions_queue,
        aeron_archive_dedicated_drain_recording_func,
        &ctx);
}

/* ---- Recorder post-sessions-close hook ---- */

static void aeron_archive_dedicated_recorder_post_sessions_close(void *clientd)
{
    (void)clientd;
    /* In the Java version, this counts down the abortLatch if isAbort is set.
     * The C version does not use a latch; abort is signalled via the volatile flag. */
}

/* ---- Replayer pre-sessions-close hook ---- */

static void aeron_archive_dedicated_replayer_pre_sessions_close(void *clientd)
{
    aeron_archive_dedicated_replayer_t *replayer = (aeron_archive_dedicated_replayer_t *)clientd;
    aeron_archive_dedicated_drain_replay_context_t ctx;
    ctx.worker = &replayer->session_worker;

    aeron_mpsc_concurrent_array_queue_drain_all(
        &replayer->sessions_queue,
        aeron_archive_dedicated_drain_replay_func,
        &ctx);
}

/* ---- Replayer post-sessions-close hook ---- */

static void aeron_archive_dedicated_replayer_post_sessions_close(void *clientd)
{
    (void)clientd;
}

/* ---- Recorder agent functions ---- */

int aeron_archive_dedicated_recorder_do_work(void *state)
{
    aeron_archive_dedicated_recorder_t *recorder = (aeron_archive_dedicated_recorder_t *)state;

    bool is_abort;
    AERON_GET_ACQUIRE(is_abort, recorder->is_abort);
    if (is_abort)
    {
        return -1;
    }

    /* Drain new sessions from the conductor thread */
    aeron_archive_dedicated_drain_recording_context_t ctx;
    ctx.worker = &recorder->session_worker;

    int work_count = (int)aeron_mpsc_concurrent_array_queue_drain_all(
        &recorder->sessions_queue,
        aeron_archive_dedicated_drain_recording_func,
        &ctx);

    /* Drive all active recording sessions */
    work_count += aeron_archive_session_worker_do_work(&recorder->session_worker);

    return work_count;
}

void aeron_archive_dedicated_recorder_on_close(void *state)
{
    aeron_archive_dedicated_recorder_t *recorder = (aeron_archive_dedicated_recorder_t *)state;

    aeron_archive_session_worker_on_close(&recorder->session_worker);
    aeron_archive_session_worker_fini(&recorder->session_worker);
    aeron_mpsc_concurrent_array_queue_close(&recorder->sessions_queue);
}

/* ---- Replayer agent functions ---- */

int aeron_archive_dedicated_replayer_do_work(void *state)
{
    aeron_archive_dedicated_replayer_t *replayer = (aeron_archive_dedicated_replayer_t *)state;

    bool is_abort;
    AERON_GET_ACQUIRE(is_abort, replayer->is_abort);
    if (is_abort)
    {
        return -1;
    }

    /* Drain new sessions from the conductor thread */
    aeron_archive_dedicated_drain_replay_context_t ctx;
    ctx.worker = &replayer->session_worker;

    int work_count = (int)aeron_mpsc_concurrent_array_queue_drain_all(
        &replayer->sessions_queue,
        aeron_archive_dedicated_drain_replay_func,
        &ctx);

    /* Drive all active replay sessions */
    work_count += aeron_archive_session_worker_do_work(&replayer->session_worker);

    return work_count;
}

void aeron_archive_dedicated_replayer_on_close(void *state)
{
    aeron_archive_dedicated_replayer_t *replayer = (aeron_archive_dedicated_replayer_t *)state;

    aeron_archive_session_worker_on_close(&replayer->session_worker);
    aeron_archive_session_worker_fini(&replayer->session_worker);
    aeron_mpsc_concurrent_array_queue_close(&replayer->sessions_queue);
}

/* ---- Close queue processing on the conductor thread ---- */

typedef struct aeron_archive_dedicated_close_drain_context_stct
{
    aeron_archive_dedicated_conductor_t *dedicated;
    int count;
}
aeron_archive_dedicated_close_drain_context_t;

static void aeron_archive_dedicated_close_drain_func(void *clientd, void *element)
{
    aeron_archive_dedicated_close_drain_context_t *ctx =
        (aeron_archive_dedicated_close_drain_context_t *)clientd;
    aeron_archive_dedicated_close_entry_t *entry = (aeron_archive_dedicated_close_entry_t *)element;

    if (AERON_ARCHIVE_DEDICATED_CLOSE_RECORDING == entry->type)
    {
        aeron_archive_conductor_close_recording_session(
            ctx->dedicated->conductor,
            (aeron_archive_recording_session_t *)entry->session);
    }
    else if (AERON_ARCHIVE_DEDICATED_CLOSE_REPLAY == entry->type)
    {
        aeron_archive_replay_session_close((aeron_archive_replay_session_t *)entry->session);
    }
    else
    {
        aeron_archive_recording_session_close((aeron_archive_recording_session_t *)entry->session);
    }

    aeron_free(entry);
    ctx->count++;
}

static int aeron_archive_dedicated_conductor_process_close_queue(
    aeron_archive_dedicated_conductor_t *dedicated)
{
    aeron_archive_dedicated_close_drain_context_t ctx;
    ctx.dedicated = dedicated;
    ctx.count = 0;

    aeron_mpsc_concurrent_array_queue_drain(
        &dedicated->close_queue,
        aeron_archive_dedicated_close_drain_func,
        &ctx,
        AERON_ARCHIVE_DEDICATED_CONDUCTOR_COMMAND_LIMIT);

    return ctx.count;
}

/* ---- Lifecycle ---- */

int aeron_archive_dedicated_conductor_create(
    aeron_archive_dedicated_conductor_t **dedicated,
    aeron_archive_conductor_context_t *ctx)
{
    aeron_archive_dedicated_conductor_t *d = NULL;

    if (NULL == dedicated || NULL == ctx)
    {
        AERON_SET_ERR(EINVAL, "%s", "dedicated or ctx is NULL");
        return -1;
    }

    if (aeron_alloc((void **)&d, sizeof(aeron_archive_dedicated_conductor_t)) < 0)
    {
        AERON_SET_ERR(ENOMEM, "%s", "failed to allocate dedicated mode conductor");
        return -1;
    }

    memset(d, 0, sizeof(aeron_archive_dedicated_conductor_t));

    d->ctx = ctx;

    /* Create the base conductor */
    if (aeron_archive_conductor_create(&d->conductor, ctx) < 0)
    {
        aeron_free(d);
        return -1;
    }

    /* Initialise the close queue shared between recorder/replayer and conductor */
    if (aeron_mpsc_concurrent_array_queue_init(
        &d->close_queue,
        AERON_ARCHIVE_DEDICATED_CONDUCTOR_SESSION_QUEUE_CAPACITY) < 0)
    {
        aeron_archive_conductor_close(d->conductor);
        aeron_free(d);
        AERON_SET_ERR(ENOMEM, "%s", "failed to init close queue");
        return -1;
    }

    /* Initialise recorder */
    d->recorder.is_abort = false;
    d->recorder.close_queue = &d->close_queue;

    if (aeron_mpsc_concurrent_array_queue_init(
        &d->recorder.sessions_queue,
        AERON_ARCHIVE_DEDICATED_CONDUCTOR_SESSION_QUEUE_CAPACITY) < 0)
    {
        aeron_mpsc_concurrent_array_queue_close(&d->close_queue);
        aeron_archive_conductor_close(d->conductor);
        aeron_free(d);
        AERON_SET_ERR(ENOMEM, "%s", "failed to init recorder sessions queue");
        return -1;
    }

    if (aeron_archive_session_worker_init(
        &d->recorder.session_worker,
        "archive-recorder",
        NULL,
        NULL,
        aeron_archive_dedicated_recorder_close_session_callback,
        &d->recorder) < 0)
    {
        aeron_mpsc_concurrent_array_queue_close(&d->recorder.sessions_queue);
        aeron_mpsc_concurrent_array_queue_close(&d->close_queue);
        aeron_archive_conductor_close(d->conductor);
        aeron_free(d);
        return -1;
    }

    d->recorder.session_worker.pre_sessions_close_func = aeron_archive_dedicated_recorder_pre_sessions_close;
    d->recorder.session_worker.pre_sessions_close_clientd = &d->recorder;
    d->recorder.session_worker.post_sessions_close_func = aeron_archive_dedicated_recorder_post_sessions_close;
    d->recorder.session_worker.post_sessions_close_clientd = &d->recorder;

    /* Initialise replayer */
    d->replayer.is_abort = false;
    d->replayer.close_queue = &d->close_queue;

    if (aeron_mpsc_concurrent_array_queue_init(
        &d->replayer.sessions_queue,
        AERON_ARCHIVE_DEDICATED_CONDUCTOR_SESSION_QUEUE_CAPACITY) < 0)
    {
        aeron_archive_session_worker_fini(&d->recorder.session_worker);
        aeron_mpsc_concurrent_array_queue_close(&d->recorder.sessions_queue);
        aeron_mpsc_concurrent_array_queue_close(&d->close_queue);
        aeron_archive_conductor_close(d->conductor);
        aeron_free(d);
        AERON_SET_ERR(ENOMEM, "%s", "failed to init replayer sessions queue");
        return -1;
    }

    if (aeron_archive_session_worker_init(
        &d->replayer.session_worker,
        "archive-replayer",
        NULL,
        NULL,
        aeron_archive_dedicated_replayer_close_session_callback,
        &d->replayer) < 0)
    {
        aeron_mpsc_concurrent_array_queue_close(&d->replayer.sessions_queue);
        aeron_archive_session_worker_fini(&d->recorder.session_worker);
        aeron_mpsc_concurrent_array_queue_close(&d->recorder.sessions_queue);
        aeron_mpsc_concurrent_array_queue_close(&d->close_queue);
        aeron_archive_conductor_close(d->conductor);
        aeron_free(d);
        return -1;
    }

    d->replayer.session_worker.pre_sessions_close_func = aeron_archive_dedicated_replayer_pre_sessions_close;
    d->replayer.session_worker.pre_sessions_close_clientd = &d->replayer;
    d->replayer.session_worker.post_sessions_close_func = aeron_archive_dedicated_replayer_post_sessions_close;
    d->replayer.session_worker.post_sessions_close_clientd = &d->replayer;

    *dedicated = d;

    return 0;
}

int aeron_archive_dedicated_conductor_on_start(aeron_archive_dedicated_conductor_t *dedicated)
{
    if (NULL == dedicated)
    {
        AERON_SET_ERR(EINVAL, "%s", "dedicated conductor is NULL");
        return -1;
    }

    /* Initialise the recorder agent runner */
    if (aeron_agent_init(
        &dedicated->recorder_runner,
        "archive-recorder",
        &dedicated->recorder,
        NULL,   /* on_start */
        NULL,   /* on_start_state */
        aeron_archive_dedicated_recorder_do_work,
        aeron_archive_dedicated_recorder_on_close,
        aeron_idle_strategy_sleeping_idle,
        NULL) < 0)
    {
        return -1;
    }

    /* Initialise the replayer agent runner */
    if (aeron_agent_init(
        &dedicated->replayer_runner,
        "archive-replayer",
        &dedicated->replayer,
        NULL,   /* on_start */
        NULL,   /* on_start_state */
        aeron_archive_dedicated_replayer_do_work,
        aeron_archive_dedicated_replayer_on_close,
        aeron_idle_strategy_sleeping_idle,
        NULL) < 0)
    {
        return -1;
    }

    /* Start the recorder and replayer on their dedicated threads */
    if (aeron_agent_start(&dedicated->recorder_runner) < 0)
    {
        return -1;
    }

    if (aeron_agent_start(&dedicated->replayer_runner) < 0)
    {
        aeron_agent_stop(&dedicated->recorder_runner);
        aeron_agent_close(&dedicated->recorder_runner);
        return -1;
    }

    return 0;
}

int aeron_archive_dedicated_conductor_do_work(aeron_archive_dedicated_conductor_t *dedicated)
{
    int work_count = aeron_archive_dedicated_conductor_process_close_queue(dedicated);
    work_count += aeron_archive_conductor_do_work(dedicated->conductor);

    return work_count;
}

int aeron_archive_dedicated_conductor_close(aeron_archive_dedicated_conductor_t *dedicated)
{
    if (NULL == dedicated)
    {
        return 0;
    }

    /* Stop the recorder and replayer agent runners */
    aeron_agent_stop(&dedicated->recorder_runner);
    aeron_agent_close(&dedicated->recorder_runner);

    aeron_agent_stop(&dedicated->replayer_runner);
    aeron_agent_close(&dedicated->replayer_runner);

    /* Drain remaining close queue entries */
    while (aeron_archive_dedicated_conductor_process_close_queue(dedicated) > 0 ||
           aeron_mpsc_concurrent_array_queue_size(&dedicated->close_queue) > 0)
    {
        aeron_micro_sleep(1);
    }

    /* Close the close queue */
    aeron_mpsc_concurrent_array_queue_close(&dedicated->close_queue);

    /* Close the base conductor */
    aeron_archive_conductor_close(dedicated->conductor);

    aeron_free(dedicated);

    return 0;
}

void aeron_archive_dedicated_conductor_abort(aeron_archive_dedicated_conductor_t *dedicated)
{
    if (NULL != dedicated)
    {
        AERON_SET_RELEASE(dedicated->recorder.is_abort, true);
        AERON_SET_RELEASE(dedicated->replayer.is_abort, true);
        aeron_archive_conductor_abort(dedicated->conductor);
    }
}

/* ---- Session handoff ---- */

int aeron_archive_dedicated_conductor_add_recording_session(
    aeron_archive_dedicated_conductor_t *dedicated,
    aeron_archive_recording_session_t *session)
{
    if (NULL == dedicated || NULL == session)
    {
        AERON_SET_ERR(EINVAL, "%s", "dedicated conductor or session is NULL");
        return -1;
    }

    /* Offer the session to the recorder's sessions queue.
     * Spin until accepted, matching the Java DedicatedModeRecorder.send() behavior. */
    while (AERON_OFFER_SUCCESS !=
        aeron_mpsc_concurrent_array_queue_offer(&dedicated->recorder.sessions_queue, session))
    {
        aeron_micro_sleep(1);
    }

    return 0;
}

int aeron_archive_dedicated_conductor_add_replay_session(
    aeron_archive_dedicated_conductor_t *dedicated,
    aeron_archive_replay_session_t *session)
{
    if (NULL == dedicated || NULL == session)
    {
        AERON_SET_ERR(EINVAL, "%s", "dedicated conductor or session is NULL");
        return -1;
    }

    /* Offer the session to the replayer's sessions queue.
     * Spin until accepted, matching the Java DedicatedModeReplayer.send() behavior. */
    while (AERON_OFFER_SUCCESS !=
        aeron_mpsc_concurrent_array_queue_offer(&dedicated->replayer.sessions_queue, session))
    {
        aeron_micro_sleep(1);
    }

    return 0;
}
