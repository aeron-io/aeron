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
#include <stdio.h>
#include <inttypes.h>
#include <time.h>
#include <errno.h>

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "concurrent/aeron_thread.h"
#include "aeron_archive_conductor.h"
#include "aeron_archive_control_session.h"
#include "aeron_archive_control_session_adapter.h"
#include "aeron_archive_control_response_proxy.h"

#define AERON_ARCHIVE_CONDUCTOR_INITIAL_SESSION_CAPACITY (16)
#define AERON_ARCHIVE_CONDUCTOR_INITIAL_SUBSCRIPTION_CAPACITY (16)

#define AERON_ARCHIVE_SPY_PREFIX "aeron-spy:"

#define AERON_ARCHIVE_FRAME_ALIGNMENT (32)

#if defined(__GNUC__) || defined(__clang__)
#define AERON_ARCHIVE_MAYBE_UNUSED __attribute__((unused))
#else
#define AERON_ARCHIVE_MAYBE_UNUSED
#endif

/* ---- static helpers ---- */

static int64_t aeron_archive_conductor_epoch_clock_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + (int64_t)ts.tv_nsec / 1000000;
}

/**
 * Build a subscription key from stream_id and channel, matching the Java makeKey() logic.
 * Format: "<streamId>:<channel>"
 */
static char *aeron_archive_conductor_make_key(int32_t stream_id, const char *channel)
{
    const size_t channel_len = strlen(channel);
    /* stream_id string: up to 11 chars, plus ':' plus channel plus NUL */
    const size_t key_len = 12 + 1 + channel_len + 1;
    char *key = NULL;

    if (aeron_alloc((void **)&key, key_len) < 0)
    {
        return NULL;
    }

    snprintf(key, key_len, "%d:%s", stream_id, channel);
    return key;
}

/* ---- Recording session map operations ---- */

AERON_ARCHIVE_MAYBE_UNUSED
static int aeron_archive_conductor_ensure_recording_session_capacity(
    aeron_archive_conductor_t *conductor)
{
    if (conductor->recording_session_count >= conductor->recording_session_capacity)
    {
        const int32_t new_capacity = conductor->recording_session_capacity * 2;
        aeron_archive_recording_session_entry_t *new_entries = NULL;

        if (aeron_alloc((void **)&new_entries, sizeof(aeron_archive_recording_session_entry_t) * (size_t)new_capacity) < 0)
        {
            AERON_SET_ERR(ENOMEM, "%s", "failed to grow recording session array");
            return -1;
        }

        memcpy(new_entries, conductor->recording_sessions,
            sizeof(aeron_archive_recording_session_entry_t) * (size_t)conductor->recording_session_count);
        aeron_free(conductor->recording_sessions);
        conductor->recording_sessions = new_entries;
        conductor->recording_session_capacity = new_capacity;
    }

    return 0;
}

AERON_ARCHIVE_MAYBE_UNUSED
static int aeron_archive_conductor_add_recording_session(
    aeron_archive_conductor_t *conductor,
    int64_t recording_id,
    aeron_archive_recording_session_t *session)
{
    if (aeron_archive_conductor_ensure_recording_session_capacity(conductor) < 0)
    {
        return -1;
    }

    aeron_archive_recording_session_entry_t *entry =
        &conductor->recording_sessions[conductor->recording_session_count];
    entry->recording_id = recording_id;
    entry->session = session;
    conductor->recording_session_count++;

    return 0;
}

static aeron_archive_recording_session_t *aeron_archive_conductor_find_recording_session_internal(
    aeron_archive_conductor_t *conductor,
    int64_t recording_id)
{
    for (int32_t i = 0; i < conductor->recording_session_count; i++)
    {
        if (conductor->recording_sessions[i].recording_id == recording_id)
        {
            return conductor->recording_sessions[i].session;
        }
    }

    return NULL;
}

static void aeron_archive_conductor_remove_recording_session(
    aeron_archive_conductor_t *conductor,
    int64_t recording_id)
{
    for (int32_t i = 0; i < conductor->recording_session_count; i++)
    {
        if (conductor->recording_sessions[i].recording_id == recording_id)
        {
            /* Swap with last entry */
            conductor->recording_session_count--;
            if (i < conductor->recording_session_count)
            {
                conductor->recording_sessions[i] =
                    conductor->recording_sessions[conductor->recording_session_count];
            }
            return;
        }
    }
}

/* ---- Replay session map operations ---- */

AERON_ARCHIVE_MAYBE_UNUSED
static int aeron_archive_conductor_ensure_replay_session_capacity(aeron_archive_conductor_t *conductor)
{
    if (conductor->replay_session_count >= conductor->replay_session_capacity)
    {
        const int32_t new_capacity = conductor->replay_session_capacity * 2;
        aeron_archive_replay_session_entry_t *new_entries = NULL;

        if (aeron_alloc((void **)&new_entries, sizeof(aeron_archive_replay_session_entry_t) * (size_t)new_capacity) < 0)
        {
            AERON_SET_ERR(ENOMEM, "%s", "failed to grow replay session array");
            return -1;
        }

        memcpy(new_entries, conductor->replay_sessions,
            sizeof(aeron_archive_replay_session_entry_t) * (size_t)conductor->replay_session_count);
        aeron_free(conductor->replay_sessions);
        conductor->replay_sessions = new_entries;
        conductor->replay_session_capacity = new_capacity;
    }

    return 0;
}

AERON_ARCHIVE_MAYBE_UNUSED
static void aeron_archive_conductor_remove_replay_session(
    aeron_archive_conductor_t *conductor,
    int64_t replay_session_id)
{
    for (int32_t i = 0; i < conductor->replay_session_count; i++)
    {
        if (conductor->replay_sessions[i].replay_session_id == replay_session_id)
        {
            conductor->replay_session_count--;
            if (i < conductor->replay_session_count)
            {
                conductor->replay_sessions[i] =
                    conductor->replay_sessions[conductor->replay_session_count];
            }
            return;
        }
    }
}

/* ---- Replication session map operations ---- */

AERON_ARCHIVE_MAYBE_UNUSED
static int aeron_archive_conductor_ensure_replication_session_capacity(aeron_archive_conductor_t *conductor)
{
    if (conductor->replication_session_count >= conductor->replication_session_capacity)
    {
        const int32_t new_capacity = conductor->replication_session_capacity * 2;
        aeron_archive_replication_session_entry_t *new_entries = NULL;

        if (aeron_alloc(
            (void **)&new_entries,
            sizeof(aeron_archive_replication_session_entry_t) * (size_t)new_capacity) < 0)
        {
            AERON_SET_ERR(ENOMEM, "%s", "failed to grow replication session array");
            return -1;
        }

        memcpy(new_entries, conductor->replication_sessions,
            sizeof(aeron_archive_replication_session_entry_t) * (size_t)conductor->replication_session_count);
        aeron_free(conductor->replication_sessions);
        conductor->replication_sessions = new_entries;
        conductor->replication_session_capacity = new_capacity;
    }

    return 0;
}

static void aeron_archive_conductor_remove_replication_session(
    aeron_archive_conductor_t *conductor,
    int64_t replication_id)
{
    for (int32_t i = 0; i < conductor->replication_session_count; i++)
    {
        if (conductor->replication_sessions[i].replication_id == replication_id)
        {
            conductor->replication_session_count--;
            if (i < conductor->replication_session_count)
            {
                conductor->replication_sessions[i] =
                    conductor->replication_sessions[conductor->replication_session_count];
            }
            return;
        }
    }
}

/* ---- Subscription map operations ---- */

static int aeron_archive_conductor_ensure_subscription_capacity(aeron_archive_conductor_t *conductor)
{
    if (conductor->recording_subscription_count >= conductor->recording_subscription_capacity)
    {
        const int32_t new_capacity = conductor->recording_subscription_capacity * 2;
        aeron_archive_recording_subscription_entry_t *new_entries = NULL;

        if (aeron_alloc(
            (void **)&new_entries,
            sizeof(aeron_archive_recording_subscription_entry_t) * (size_t)new_capacity) < 0)
        {
            AERON_SET_ERR(ENOMEM, "%s", "failed to grow recording subscription array");
            return -1;
        }

        memcpy(new_entries, conductor->recording_subscriptions,
            sizeof(aeron_archive_recording_subscription_entry_t) * (size_t)conductor->recording_subscription_count);
        aeron_free(conductor->recording_subscriptions);
        conductor->recording_subscriptions = new_entries;
        conductor->recording_subscription_capacity = new_capacity;
    }

    return 0;
}

static aeron_archive_recording_subscription_entry_t *aeron_archive_conductor_find_subscription_by_key(
    aeron_archive_conductor_t *conductor,
    const char *key)
{
    for (int32_t i = 0; i < conductor->recording_subscription_count; i++)
    {
        if (0 == strcmp(conductor->recording_subscriptions[i].key, key))
        {
            return &conductor->recording_subscriptions[i];
        }
    }

    return NULL;
}

static aeron_archive_recording_subscription_entry_t *aeron_archive_conductor_find_subscription_by_id(
    aeron_archive_conductor_t *conductor,
    int64_t subscription_id)
{
    for (int32_t i = 0; i < conductor->recording_subscription_count; i++)
    {
        aeron_subscription_constants_t constants;
        if (aeron_subscription_constants(conductor->recording_subscriptions[i].subscription, &constants) == 0)
        {
            if (constants.registration_id == subscription_id)
            {
                return &conductor->recording_subscriptions[i];
            }
        }
    }

    return NULL;
}

static void aeron_archive_conductor_remove_subscription_by_key(
    aeron_archive_conductor_t *conductor,
    const char *key)
{
    for (int32_t i = 0; i < conductor->recording_subscription_count; i++)
    {
        if (0 == strcmp(conductor->recording_subscriptions[i].key, key))
        {
            aeron_free(conductor->recording_subscriptions[i].key);
            conductor->recording_subscription_count--;
            if (i < conductor->recording_subscription_count)
            {
                conductor->recording_subscriptions[i] =
                    conductor->recording_subscriptions[conductor->recording_subscription_count];
            }
            return;
        }
    }
}

static aeron_archive_recording_subscription_entry_t *aeron_archive_conductor_remove_subscription_by_id(
    aeron_archive_conductor_t *conductor,
    int64_t subscription_id)
{
    for (int32_t i = 0; i < conductor->recording_subscription_count; i++)
    {
        aeron_subscription_constants_t constants;
        if (aeron_subscription_constants(conductor->recording_subscriptions[i].subscription, &constants) == 0 &&
            constants.registration_id == subscription_id)
        {
            /* Copy entry data before removing (caller must use before next remove) */
            aeron_free(conductor->recording_subscriptions[i].key);
            conductor->recording_subscriptions[i].key = NULL;
            conductor->recording_subscription_count--;
            if (i < conductor->recording_subscription_count)
            {
                conductor->recording_subscriptions[i] =
                    conductor->recording_subscriptions[conductor->recording_subscription_count];
            }
            return &conductor->recording_subscriptions[i];
        }
    }

    return NULL;
}

/* ---- Task queue operations ---- */

static int aeron_archive_conductor_run_tasks(aeron_archive_conductor_t *conductor)
{
    int work_count = 0;

    while (NULL != conductor->task_queue_head)
    {
        aeron_archive_conductor_task_t *task = conductor->task_queue_head;
        conductor->task_queue_head = task->next;
        if (NULL == conductor->task_queue_head)
        {
            conductor->task_queue_tail = NULL;
        }

        task->func(task->clientd);
        aeron_free(task);
        work_count++;
    }

    return work_count;
}

/* ---- Recording session callback helpers ---- */

/**
 * Populate the recording summary from a catalog descriptor.
 */
static int aeron_archive_conductor_recording_summary(
    aeron_archive_conductor_t *conductor,
    int64_t recording_id,
    aeron_archive_recording_summary_t *summary)
{
    aeron_archive_catalog_recording_descriptor_t descriptor;
    if (aeron_archive_catalog_find_recording(conductor->catalog, recording_id, &descriptor) < 0)
    {
        return -1;
    }

    summary->recording_id = descriptor.recording_id;
    summary->start_position = descriptor.start_position;
    summary->stop_position = descriptor.stop_position;
    summary->start_timestamp = descriptor.start_timestamp;
    summary->stop_timestamp = descriptor.stop_timestamp;
    summary->initial_term_id = descriptor.initial_term_id;
    summary->segment_file_length = descriptor.segment_file_length;
    summary->term_buffer_length = descriptor.term_buffer_length;
    summary->mtu_length = descriptor.mtu_length;
    summary->session_id = descriptor.session_id;
    summary->stream_id = descriptor.stream_id;

    return 0;
}

static bool aeron_archive_conductor_has_recording(
    aeron_archive_conductor_t *conductor,
    int64_t recording_id)
{
    aeron_archive_catalog_recording_descriptor_t descriptor;
    return aeron_archive_catalog_find_recording(conductor->catalog, recording_id, &descriptor) == 0;
}

/**
 * Drive all active recording sessions, removing any that are done.
 */
static int aeron_archive_conductor_drive_recording_sessions(aeron_archive_conductor_t *conductor)
{
    int work_count = 0;

    for (int32_t i = conductor->recording_session_count - 1; i >= 0; i--)
    {
        aeron_archive_recording_session_t *session = conductor->recording_sessions[i].session;
        const int result = aeron_archive_recording_session_do_work(session);

        if (result > 0)
        {
            work_count += result;
        }

        if (aeron_archive_recording_session_is_done(session))
        {
            aeron_archive_conductor_close_recording_session(conductor, session);
        }
    }

    return work_count;
}

/**
 * Abort all recording sessions on a given subscription.
 */
static void aeron_archive_conductor_abort_recording_sessions_for_subscription(
    aeron_archive_conductor_t *conductor,
    aeron_subscription_t *subscription)
{
    for (int32_t i = 0; i < conductor->recording_session_count; i++)
    {
        aeron_archive_recording_session_t *session = conductor->recording_sessions[i].session;
        /* Compare subscription pointers */
        (void)subscription;
        /* In the full implementation, each recording session would hold a reference to its subscription.
         * For now, abort all recording sessions as a conservative approach when a subscription is removed. */
        aeron_archive_recording_session_abort(session, "stop recording");
    }
}

/* ---- Lifecycle ---- */

int aeron_archive_conductor_create(
    aeron_archive_conductor_t **conductor,
    aeron_archive_conductor_context_t *ctx)
{
    aeron_archive_conductor_t *c = NULL;

    if (NULL == conductor || NULL == ctx)
    {
        AERON_SET_ERR(EINVAL, "%s", "conductor or ctx is NULL");
        return -1;
    }

    if (aeron_alloc((void **)&c, sizeof(aeron_archive_conductor_t)) < 0)
    {
        AERON_SET_ERR(ENOMEM, "%s", "failed to allocate archive conductor");
        return -1;
    }

    memset(c, 0, sizeof(aeron_archive_conductor_t));

    c->ctx = ctx;
    c->aeron = ctx->aeron;
    c->catalog = ctx->catalog;
    c->recording_events_proxy = ctx->recording_events_proxy;

    c->is_abort = false;
    c->is_closed = false;

    /* Generate initial session ID from random source */
    srand((unsigned int)time(NULL));
    c->next_session_id = (int64_t)(rand() % INT32_MAX);
    c->replay_id = 1;
    c->mark_file_update_deadline_ms = 0;
    c->cached_epoch_clock_ms = aeron_archive_conductor_epoch_clock_ms();

    /* Allocate recording session array */
    c->recording_session_capacity = AERON_ARCHIVE_CONDUCTOR_INITIAL_SESSION_CAPACITY;
    if (aeron_alloc(
        (void **)&c->recording_sessions,
        sizeof(aeron_archive_recording_session_entry_t) * (size_t)c->recording_session_capacity) < 0)
    {
        goto error_cleanup;
    }
    c->recording_session_count = 0;

    /* Allocate replay session array */
    c->replay_session_capacity = AERON_ARCHIVE_CONDUCTOR_INITIAL_SESSION_CAPACITY;
    if (aeron_alloc(
        (void **)&c->replay_sessions,
        sizeof(aeron_archive_replay_session_entry_t) * (size_t)c->replay_session_capacity) < 0)
    {
        goto error_cleanup;
    }
    c->replay_session_count = 0;

    /* Allocate replication session array */
    c->replication_session_capacity = AERON_ARCHIVE_CONDUCTOR_INITIAL_SESSION_CAPACITY;
    if (aeron_alloc(
        (void **)&c->replication_sessions,
        sizeof(aeron_archive_replication_session_entry_t) * (size_t)c->replication_session_capacity) < 0)
    {
        goto error_cleanup;
    }
    c->replication_session_count = 0;

    /* Allocate recording subscription array */
    c->recording_subscription_capacity = AERON_ARCHIVE_CONDUCTOR_INITIAL_SUBSCRIPTION_CAPACITY;
    if (aeron_alloc(
        (void **)&c->recording_subscriptions,
        sizeof(aeron_archive_recording_subscription_entry_t) * (size_t)c->recording_subscription_capacity) < 0)
    {
        goto error_cleanup;
    }
    c->recording_subscription_count = 0;

    /* Task queue starts empty */
    c->task_queue_head = NULL;
    c->task_queue_tail = NULL;

    /* Set up control subscriptions */
    c->control_subscription = NULL;
    c->local_control_subscription = NULL;
    c->control_subscription_async = NULL;
    c->local_control_subscription_async = NULL;
    c->control_session_adapter = NULL;
    c->control_response_proxy = NULL;
    c->control_sessions_head = NULL;

    /* Create the control response proxy */
    if (aeron_archive_control_response_proxy_create(&c->control_response_proxy) < 0)
    {
        goto error_cleanup;
    }

    /* Create control subscriptions asynchronously.
     * The do_work loop will poll these async handles and create the adapter once ready. */
    if (ctx->control_channel_enabled && NULL != ctx->control_channel)
    {
        if (aeron_async_add_subscription(
            &c->control_subscription_async, c->aeron, ctx->control_channel, ctx->control_stream_id,
            NULL, NULL, NULL, NULL) < 0)
        {
            /* Non-fatal */
        }
    }

    if (NULL != ctx->local_control_channel)
    {
        if (aeron_async_add_subscription(
            &c->local_control_subscription_async, c->aeron, ctx->local_control_channel, ctx->local_control_stream_id,
            NULL, NULL, NULL, NULL) < 0)
        {
            /* Non-fatal */
        }
    }

    *conductor = c;
    return 0;

error_cleanup:
    aeron_archive_control_response_proxy_close(c->control_response_proxy);
    aeron_free(c->recording_sessions);
    aeron_free(c->replay_sessions);
    aeron_free(c->replication_sessions);
    aeron_free(c->recording_subscriptions);
    aeron_free(c);
    AERON_SET_ERR(ENOMEM, "%s", "failed to allocate archive conductor resources");
    return -1;
}

int aeron_archive_conductor_do_work(aeron_archive_conductor_t *conductor)
{
    int work_count = 0;

    if (NULL == conductor)
    {
        return -1;
    }

    if (conductor->is_abort)
    {
        AERON_SET_ERR(EINTR, "%s", "archive conductor aborted");
        return -1;
    }

    const int64_t now_ms = aeron_archive_conductor_epoch_clock_ms();

    if (conductor->cached_epoch_clock_ms != now_ms)
    {
        conductor->cached_epoch_clock_ms = now_ms;

        if (now_ms >= conductor->mark_file_update_deadline_ms)
        {
            conductor->mark_file_update_deadline_ms =
                now_ms + AERON_ARCHIVE_CONDUCTOR_MARK_FILE_UPDATE_INTERVAL_MS;
            /* Mark file update would happen here via mark_file->update_activity_timestamp(now_ms) */
        }
    }

    {
    }

    /*
     * Poll async subscription handles until resolved, then create the adapter.
     */
    if (NULL != conductor->control_subscription_async && NULL == conductor->control_subscription)
    {
        aeron_subscription_t *sub = NULL;
        int rc = aeron_async_add_subscription_poll(&sub, conductor->control_subscription_async);
        if (rc > 0 && NULL != sub)
        {
            conductor->control_subscription = sub;
            conductor->control_subscription_async = NULL;
            work_count++;
        }
        else if (rc < 0)
        {
            conductor->control_subscription_async = NULL;
        }
    }

    if (NULL != conductor->local_control_subscription_async && NULL == conductor->local_control_subscription)
    {
        aeron_subscription_t *sub = NULL;
        int rc = aeron_async_add_subscription_poll(&sub, conductor->local_control_subscription_async);
        if (rc > 0 && NULL != sub)
        {
            conductor->local_control_subscription = sub;
            conductor->local_control_subscription_async = NULL;
            work_count++;
        }
        else if (rc < 0)
        {
            conductor->local_control_subscription_async = NULL;
        }
    }

    /*
     * Create the control session adapter once subscriptions are resolved.
     * We need at least the local control subscription, or the control subscription if enabled.
     */
    if (NULL == conductor->control_session_adapter)
    {
        bool control_ready = !conductor->ctx->control_channel_enabled ||
            NULL != conductor->control_subscription ||
            NULL == conductor->control_subscription_async;
        bool local_ready = NULL != conductor->local_control_subscription ||
            NULL == conductor->local_control_subscription_async;

        if (control_ready && local_ready &&
            (NULL != conductor->control_subscription || NULL != conductor->local_control_subscription))
        {
            if (aeron_archive_control_session_adapter_create(
                &conductor->control_session_adapter,
                conductor->control_subscription,
                conductor->local_control_subscription,
                conductor) == 0)
            {
                work_count++;
            }
        }
    }

    /*
     * Poll control subscriptions for new requests via the adapter.
     */
    if (NULL != conductor->control_session_adapter)
    {
        work_count += aeron_archive_control_session_adapter_poll(conductor->control_session_adapter);
    }

    /*
     * Drive all active control sessions (state machine: INIT -> CONNECTING -> CONNECTED -> ACTIVE).
     */
    {
        aeron_archive_control_session_entry_t **prev_ptr = &conductor->control_sessions_head;
        aeron_archive_control_session_entry_t *entry = conductor->control_sessions_head;

        while (NULL != entry)
        {
            aeron_archive_control_session_entry_t *next = entry->next;
            int session_work = aeron_archive_control_session_do_work(entry->session, conductor->cached_epoch_clock_ms);

            if (session_work > 0)
            {
                work_count += session_work;
            }

            if (aeron_archive_control_session_is_done(entry->session))
            {
                *prev_ptr = next;
                aeron_archive_control_session_close(entry->session);
                aeron_free(entry);
            }
            else
            {
                prev_ptr = &entry->next;
            }

            entry = next;
        }
    }

    /* Drive active recording sessions */
    work_count += aeron_archive_conductor_drive_recording_sessions(conductor);

    /* Run queued tasks (image-available callbacks enqueue recording start tasks) */
    work_count += aeron_archive_conductor_run_tasks(conductor);

    return work_count;
}

int aeron_archive_conductor_close(aeron_archive_conductor_t *conductor)
{
    if (NULL == conductor)
    {
        return 0;
    }

    if (conductor->is_closed)
    {
        return 0;
    }

    conductor->is_closed = true;

    /* Abort and close all recording sessions */
    for (int32_t i = 0; i < conductor->recording_session_count; i++)
    {
        aeron_archive_recording_session_t *session = conductor->recording_sessions[i].session;
        if (conductor->is_abort)
        {
            aeron_archive_recording_session_abort_close(session);
        }
        else
        {
            aeron_archive_recording_session_close(session);
        }
    }

    /* Close recording subscriptions */
    for (int32_t i = 0; i < conductor->recording_subscription_count; i++)
    {
        if (NULL != conductor->recording_subscriptions[i].subscription)
        {
            aeron_subscription_close(conductor->recording_subscriptions[i].subscription, NULL, NULL);
        }
        aeron_free(conductor->recording_subscriptions[i].key);
    }

    /* Close and free all control sessions */
    {
        aeron_archive_control_session_entry_t *entry = conductor->control_sessions_head;
        while (NULL != entry)
        {
            aeron_archive_control_session_entry_t *next = entry->next;
            aeron_archive_control_session_close(entry->session);
            aeron_free(entry);
            entry = next;
        }
        conductor->control_sessions_head = NULL;
    }

    /* Close the control session adapter */
    aeron_archive_control_session_adapter_close(conductor->control_session_adapter);
    conductor->control_session_adapter = NULL;

    /* Close the control response proxy */
    aeron_archive_control_response_proxy_close(conductor->control_response_proxy);
    conductor->control_response_proxy = NULL;

    /* Close control subscriptions */
    if (NULL != conductor->local_control_subscription)
    {
        aeron_subscription_close(conductor->local_control_subscription, NULL, NULL);
    }

    if (NULL != conductor->control_subscription)
    {
        aeron_subscription_close(conductor->control_subscription, NULL, NULL);
    }

    /* Drain task queue */
    while (NULL != conductor->task_queue_head)
    {
        aeron_archive_conductor_task_t *task = conductor->task_queue_head;
        conductor->task_queue_head = task->next;
        aeron_free(task);
    }

    /* Free arrays */
    aeron_free(conductor->recording_sessions);
    aeron_free(conductor->replay_sessions);
    aeron_free(conductor->replication_sessions);
    aeron_free(conductor->recording_subscriptions);

    conductor->recording_sessions = NULL;
    conductor->replay_sessions = NULL;
    conductor->replication_sessions = NULL;
    conductor->recording_subscriptions = NULL;

    aeron_free(conductor);

    return 0;
}

void aeron_archive_conductor_abort(aeron_archive_conductor_t *conductor)
{
    if (NULL != conductor)
    {
        conductor->is_abort = true;
    }
}

/* ---- Control session creation ---- */

aeron_archive_control_session_t *aeron_archive_conductor_new_control_session(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int32_t response_stream_id,
    int32_t client_version,
    const char *response_channel,
    const uint8_t *encoded_credentials,
    size_t credentials_length,
    aeron_archive_control_session_adapter_t *adapter,
    aeron_image_t *image)
{
    (void)encoded_credentials;
    (void)credentials_length;
    (void)image;

    /*
     * Check the client protocol major version.
     * PROTOCOL_MAJOR_VERSION = 1, encoded as (major << 16 | minor << 8 | patch).
     */
    const int32_t client_major = (client_version >> 16) & 0xFF;
    const int32_t expected_major = 1;
    char *invalid_version_message = NULL;

    if (client_major != expected_major)
    {
        char msg[256];
        snprintf(msg, sizeof(msg),
            "invalid client version %d.%d.%d, archive expects major version %d",
            client_major, (client_version >> 8) & 0xFF, client_version & 0xFF, expected_major);

        const size_t msg_len = strlen(msg);
        if (aeron_alloc((void **)&invalid_version_message, msg_len + 1) == 0)
        {
            memcpy(invalid_version_message, msg, msg_len + 1);
        }
    }

    const int64_t control_session_id = conductor->next_session_id++;

    /*
     * Create the response publication synchronously (spin-poll until ready).
     * C aeron client doesn't have Java's thread-safe getExclusivePublication(),
     * so we must complete the async add on the same thread before proceeding.
     * This matches the pattern used by the official C archive client.
     */
    aeron_async_add_exclusive_publication_t *async_pub = NULL;
    if (aeron_async_add_exclusive_publication(
        &async_pub, conductor->aeron, response_channel, response_stream_id) < 0)
    {
        aeron_free(invalid_version_message);
        return NULL;
    }

    aeron_exclusive_publication_t *control_publication = NULL;
    if (aeron_async_add_exclusive_publication_poll(&control_publication, async_pub) < 0)
    {
        aeron_free(invalid_version_message);
        return NULL;
    }
    for (int i = 0; NULL == control_publication && i < 1000; i++)
    {
        aeron_micro_sleep(1000);
        if (aeron_async_add_exclusive_publication_poll(&control_publication, async_pub) < 0)
        {
            aeron_free(invalid_version_message);
            return NULL;
        }
    }
    if (NULL == control_publication)
    {
        AERON_SET_ERR(ETIMEDOUT, "%s", "timed out creating control response publication");
        aeron_free(invalid_version_message);
        return NULL;
    }

    /*
     * Create the control session with the resolved publication.
     */
    aeron_archive_control_session_t *control_session = NULL;
    if (aeron_archive_control_session_create(
        &control_session,
        control_session_id,
        correlation_id,
        conductor->ctx->connect_timeout_ms,
        conductor->ctx->session_liveness_check_interval_ms,
        0, /* registration_id not needed — publication already resolved */
        response_channel,
        response_stream_id,
        invalid_version_message,
        adapter,
        conductor->aeron,
        conductor,
        conductor->cached_epoch_clock_ms,
        conductor->control_response_proxy) < 0)
    {
        aeron_free(invalid_version_message);
        return NULL;
    }

    /* Set the publication directly — skip the async init step */
    control_session->control_publication = control_publication;

    aeron_free(invalid_version_message);

    /* Add the session to the conductor's linked list */
    aeron_archive_control_session_entry_t *entry = NULL;
    if (aeron_alloc((void **)&entry, sizeof(aeron_archive_control_session_entry_t)) < 0)
    {
        aeron_archive_control_session_close(control_session);
        return NULL;
    }

    entry->session = control_session;
    entry->next = conductor->control_sessions_head;
    conductor->control_sessions_head = entry;

    return control_session;
}

/* ---- Control request handlers ---- */

int aeron_archive_conductor_start_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int32_t stream_id,
    aeron_archive_source_location_t source_location,
    bool auto_stop,
    const char *original_channel,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (conductor->recording_session_count >= conductor->ctx->max_concurrent_recordings)
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_MAX_RECORDINGS,
            "max concurrent recordings reached: %d",
            conductor->ctx->max_concurrent_recordings);
        return -1;
    }

    char *key = aeron_archive_conductor_make_key(stream_id, original_channel);
    if (NULL == key)
    {
        return -1;
    }

    /* Check for existing subscription with the same key */
    if (NULL != aeron_archive_conductor_find_subscription_by_key(conductor, key))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_ACTIVE_SUBSCRIPTION,
            "recording exists for streamId=%d channel=%s",
            stream_id, original_channel);
        aeron_free(key);
        return -1;
    }

    /*
     * Build the subscription channel. For LOCAL source location on UDP, prepend spy prefix.
     * The stripped channel logic in Java removes non-essential URI parameters. Here we use
     * the original channel as-is for simplicity; a full implementation would strip parameters.
     */
    const char *subscribe_channel = original_channel;
    char spy_channel[1024];
    if (AERON_ARCHIVE_SOURCE_LOCATION_LOCAL == source_location &&
        NULL != strstr(original_channel, "udp"))
    {
        snprintf(spy_channel, sizeof(spy_channel), "%s%s", AERON_ARCHIVE_SPY_PREFIX, original_channel);
        subscribe_channel = spy_channel;
    }

    /*
     * Add the subscription via Aeron. When images become available, recording sessions
     * will be started via the available image handler (enqueued as a task).
     *
     * In the full implementation, we would pass an available image handler that enqueues
     * a startRecordingSession task. For now, we create the subscription and store it.
     */
    aeron_async_add_subscription_t *async = NULL;
    if (aeron_async_add_subscription(
        &async, conductor->aeron, subscribe_channel, stream_id,
        NULL, NULL, NULL, NULL) < 0)
    {
        AERON_SET_ERR(errno, "failed to add subscription for channel=%s streamId=%d", subscribe_channel, stream_id);
        aeron_free(key);
        return -1;
    }

    /*
     * Store the subscription entry. The subscription pointer will be resolved
     * asynchronously; for now we store NULL and the async handle.
     * A full implementation would poll the async add until resolved.
     */
    if (aeron_archive_conductor_ensure_subscription_capacity(conductor) < 0)
    {
        aeron_free(key);
        return -1;
    }

    aeron_archive_recording_subscription_entry_t *entry =
        &conductor->recording_subscriptions[conductor->recording_subscription_count];
    entry->key = key;
    entry->subscription = NULL;  /* Will be resolved from async add */
    entry->ref_count = 1;
    conductor->recording_subscription_count++;

    aeron_archive_control_session_send_ok_response(
        session, correlation_id, (int64_t)conductor->recording_subscription_count);

    return 0;
}

int aeron_archive_conductor_stop_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int32_t stream_id,
    const char *channel,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    char *key = aeron_archive_conductor_make_key(stream_id, channel);
    if (NULL == key)
    {
        return -1;
    }

    aeron_archive_recording_subscription_entry_t *entry =
        aeron_archive_conductor_find_subscription_by_key(conductor, key);

    if (NULL == entry)
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_SUBSCRIPTION,
            "no recording found for streamId=%d channel=%s",
            stream_id, channel);
        aeron_free(key);
        return -1;
    }

    /* Abort all recording sessions associated with this subscription */
    if (NULL != entry->subscription)
    {
        aeron_archive_conductor_abort_recording_sessions_for_subscription(conductor, entry->subscription);

        entry->ref_count--;
        if (entry->ref_count <= 0)
        {
            aeron_subscription_close(entry->subscription, NULL, NULL);
        }
    }

    aeron_archive_conductor_remove_subscription_by_key(conductor, key);
    aeron_free(key);

    aeron_archive_control_session_send_ok_response(session, correlation_id, 0);

    return 0;
}

int aeron_archive_conductor_stop_recording_subscription(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t subscription_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    aeron_archive_recording_subscription_entry_t *entry =
        aeron_archive_conductor_find_subscription_by_id(conductor, subscription_id);

    if (NULL == entry)
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_SUBSCRIPTION,
            "no recording subscription found for subscriptionId=%" PRId64,
            subscription_id);
        return -1;
    }

    if (NULL != entry->subscription)
    {
        aeron_archive_conductor_abort_recording_sessions_for_subscription(conductor, entry->subscription);

        entry->ref_count--;
        if (entry->ref_count <= 0)
        {
            aeron_subscription_close(entry->subscription, NULL, NULL);
        }
    }

    /* Remove by rebuilding without this entry */
    aeron_archive_conductor_remove_subscription_by_id(conductor, subscription_id);

    aeron_archive_control_session_send_ok_response(session, correlation_id, 0);

    return 0;
}

int aeron_archive_conductor_stop_recording_by_identity(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording id: %" PRId64,
            recording_id);
        return -1;
    }

    aeron_archive_recording_session_t *recording_session =
        aeron_archive_conductor_find_recording_session_internal(conductor, recording_id);

    if (NULL != recording_session)
    {
        aeron_archive_recording_session_abort(recording_session, "stop recording by identity");
    }

    aeron_archive_control_session_send_ok_response(session, correlation_id, 0);

    return 0;
}

int aeron_archive_conductor_start_replay(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t position,
    int64_t length,
    int32_t replay_stream_id,
    const char *replay_channel,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (conductor->replay_session_count >= conductor->ctx->max_concurrent_replays)
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_MAX_REPLAYS,
            "max concurrent replays reached: %d",
            conductor->ctx->max_concurrent_replays);
        return -1;
    }

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording id: %" PRId64,
            recording_id);
        return -1;
    }

    if (aeron_archive_conductor_recording_summary(conductor, recording_id, &conductor->recording_summary) < 0)
    {
        return -1;
    }

    const int64_t start_position = conductor->recording_summary.start_position;
    int64_t replay_position = start_position;

    if (AERON_ARCHIVE_NULL_POSITION != position)
    {
        /* Validate replay position */
        if ((position & (AERON_ARCHIVE_FRAME_ALIGNMENT - 1)) != 0)
        {
            AERON_SET_ERR(
                AERON_ARCHIVE_ERROR_GENERIC,
                "requested replay start position=%" PRId64 " is not frame-aligned for recording %" PRId64,
                position, recording_id);
            return -1;
        }

        if (position < start_position)
        {
            AERON_SET_ERR(
                AERON_ARCHIVE_ERROR_GENERIC,
                "requested replay start position=%" PRId64 " < recording start=%" PRId64 " for recording %" PRId64,
                position, start_position, recording_id);
            return -1;
        }

        const int64_t stop_position = conductor->recording_summary.stop_position;
        if (AERON_ARCHIVE_NULL_POSITION != stop_position && position >= stop_position)
        {
            AERON_SET_ERR(
                AERON_ARCHIVE_ERROR_GENERIC,
                "requested replay start position=%" PRId64 " >= stop=%" PRId64 " for recording %" PRId64,
                position, stop_position, recording_id);
            return -1;
        }

        replay_position = position;
    }

    /*
     * In the full implementation, this would:
     * 1. Create an exclusive publication on the replay channel
     * 2. Create a ReplaySession with the recording reader
     * 3. Add it to the replay session map
     *
     * The ReplaySession type is forward-declared and will be implemented separately.
     * For now, send an OK response with a replay session id.
     */
    (void)replay_position;
    (void)length;
    (void)replay_stream_id;
    (void)replay_channel;

    const int64_t replay_session_id = (int64_t)conductor->replay_id++;
    aeron_archive_control_session_send_ok_response(session, correlation_id, replay_session_id);

    return 0;
}

int aeron_archive_conductor_start_bounded_replay(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t position,
    int64_t length,
    int32_t limit_counter_id,
    int32_t replay_stream_id,
    const char *replay_channel,
    void *control_session)
{
    (void)limit_counter_id;

    /*
     * A bounded replay is identical to a normal replay except it is bounded by
     * a counter value.  Delegate to start_replay for validation; the limit
     * counter will be wired when ReplaySession is fully implemented.
     */
    return aeron_archive_conductor_start_replay(
        conductor, correlation_id, recording_id, position, length,
        replay_stream_id, replay_channel, control_session);
}

int aeron_archive_conductor_stop_replay(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t replay_session_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    /* Find and abort the replay session */
    for (int32_t i = 0; i < conductor->replay_session_count; i++)
    {
        if (conductor->replay_sessions[i].replay_session_id == replay_session_id)
        {
            /*
             * In the full implementation:
             * aeron_archive_replay_session_abort(conductor->replay_sessions[i].session, "stop replay");
             */
            aeron_archive_control_session_send_ok_response(session, correlation_id, 0);
            return 0;
        }
    }

    /* Replay not found is not an error per the Java implementation - just send OK */
    aeron_archive_control_session_send_ok_response(session, correlation_id, 0);
    return 0;
}

int aeron_archive_conductor_stop_all_replays(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    for (int32_t i = 0; i < conductor->replay_session_count; i++)
    {
        /*
         * In the full implementation, check if recording_id matches or is NULL_VALUE:
         * if (AERON_ARCHIVE_NULL_VALUE == recording_id ||
         *     conductor->replay_sessions[i].session->recording_id == recording_id)
         * {
         *     aeron_archive_replay_session_abort(conductor->replay_sessions[i].session, "stop all replays");
         * }
         */
        (void)recording_id;
    }

    aeron_archive_control_session_send_ok_response(session, correlation_id, 0);

    return 0;
}

/* ---- List recordings ---- */

/**
 * Context passed to the catalog iteration callback for list_recordings.
 */
typedef struct aeron_archive_list_recordings_context_stct
{
    int64_t from_recording_id;
    int32_t max_count;
    int32_t count;
    void *control_session;
}
aeron_archive_list_recordings_context_t;

static void list_recordings_callback(
    const aeron_archive_catalog_recording_descriptor_t *descriptor,
    void *clientd)
{
    aeron_archive_list_recordings_context_t *ctx = (aeron_archive_list_recordings_context_t *)clientd;

    if (ctx->count >= ctx->max_count)
    {
        return;
    }

    if (descriptor->recording_id >= ctx->from_recording_id)
    {
        /*
         * In the full implementation, encode the descriptor as an SBE message and send
         * it via the control session's response publication.
         */
        ctx->count++;
    }
}

int aeron_archive_conductor_list_recordings(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t from_recording_id,
    int32_t record_count,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    aeron_archive_list_recordings_context_t ctx;
    ctx.from_recording_id = from_recording_id;
    ctx.max_count = record_count;
    ctx.count = 0;
    ctx.control_session = control_session;

    const int32_t iterated = aeron_archive_catalog_for_each(
        conductor->catalog,
        list_recordings_callback,
        &ctx);

    if (iterated < 0)
    {
        AERON_SET_ERR(AERON_ARCHIVE_ERROR_GENERIC, "%s", "catalog iteration failed during list recordings");
        return -1;
    }

    aeron_archive_control_session_send_ok_response(session, correlation_id, (int64_t)ctx.count);

    return 0;
}

/**
 * Context for filtered listing by URI.
 */
typedef struct aeron_archive_list_recordings_for_uri_context_stct
{
    int64_t from_recording_id;
    int32_t max_count;
    int32_t count;
    int32_t stream_id;
    const char *channel_fragment;
    void *control_session;
}
aeron_archive_list_recordings_for_uri_context_t;

static void list_recordings_for_uri_callback(
    const aeron_archive_catalog_recording_descriptor_t *descriptor,
    void *clientd)
{
    aeron_archive_list_recordings_for_uri_context_t *ctx =
        (aeron_archive_list_recordings_for_uri_context_t *)clientd;

    if (ctx->count >= ctx->max_count)
    {
        return;
    }

    if (descriptor->recording_id < ctx->from_recording_id)
    {
        return;
    }

    /* Match stream_id */
    if (descriptor->stream_id != ctx->stream_id)
    {
        return;
    }

    /* Match channel fragment if provided */
    if (NULL != ctx->channel_fragment && '\0' != ctx->channel_fragment[0])
    {
        if (NULL != descriptor->stripped_channel &&
            NULL == strstr(descriptor->stripped_channel, ctx->channel_fragment))
        {
            /* Also check original channel */
            if (NULL != descriptor->original_channel &&
                NULL == strstr(descriptor->original_channel, ctx->channel_fragment))
            {
                return;
            }
        }
    }

    /*
     * In the full implementation, encode and send the descriptor via the control session.
     */
    ctx->count++;
}

int aeron_archive_conductor_list_recordings_for_uri(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t from_recording_id,
    int32_t record_count,
    int32_t stream_id,
    const char *channel_fragment,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    aeron_archive_list_recordings_for_uri_context_t ctx;
    ctx.from_recording_id = from_recording_id;
    ctx.max_count = record_count;
    ctx.count = 0;
    ctx.stream_id = stream_id;
    ctx.channel_fragment = channel_fragment;
    ctx.control_session = control_session;

    const int32_t iterated = aeron_archive_catalog_for_each(
        conductor->catalog,
        list_recordings_for_uri_callback,
        &ctx);

    if (iterated < 0)
    {
        AERON_SET_ERR(AERON_ARCHIVE_ERROR_GENERIC, "%s", "catalog iteration failed during list recordings for URI");
        return -1;
    }

    aeron_archive_control_session_send_ok_response(session, correlation_id, (int64_t)ctx.count);

    return 0;
}

int aeron_archive_conductor_list_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        aeron_archive_control_session_send_recording_unknown(session, correlation_id, recording_id);
        return 0;
    }

    /*
     * In the full implementation, read the descriptor from the catalog and
     * send it to the control session via send_descriptor.
     */
    aeron_archive_control_session_send_ok_response(session, correlation_id, 1);
    return 0;
}

int aeron_archive_conductor_find_last_matching_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t min_recording_id,
    int32_t session_id,
    int32_t stream_id,
    const char *channel_fragment,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (min_recording_id < 0)
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "min_recording_id=%" PRId64 " < 0",
            min_recording_id);
        return -1;
    }

    /*
     * In the full implementation, this would call catalog->findLast()
     * which scans from the highest recording ID backwards to find the
     * last recording matching the session_id, stream_id, and channel_fragment.
     *
     * The result recording_id (or -1 if not found) would be sent as the
     * response value via the control session.
     */
    (void)session_id;
    (void)stream_id;
    (void)channel_fragment;

    aeron_archive_control_session_send_ok_response(session, correlation_id, AERON_ARCHIVE_NULL_VALUE);

    return 0;
}

/* ---- Position queries ---- */

int aeron_archive_conductor_get_start_position(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording id: %" PRId64,
            recording_id);
        return -1;
    }

    aeron_archive_catalog_recording_descriptor_t descriptor;
    if (aeron_archive_catalog_find_recording(conductor->catalog, recording_id, &descriptor) < 0)
    {
        return -1;
    }

    aeron_archive_control_session_send_ok_response(session, correlation_id, descriptor.start_position);
    return 0;
}

int aeron_archive_conductor_get_stop_position(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording id: %" PRId64,
            recording_id);
        return -1;
    }

    aeron_archive_catalog_recording_descriptor_t descriptor;
    if (aeron_archive_catalog_find_recording(conductor->catalog, recording_id, &descriptor) < 0)
    {
        return -1;
    }

    aeron_archive_control_session_send_ok_response(session, correlation_id, descriptor.stop_position);
    return 0;
}

int aeron_archive_conductor_get_recording_position(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording id: %" PRId64,
            recording_id);
        return -1;
    }

    aeron_archive_recording_session_t *recording_session =
        aeron_archive_conductor_find_recording_session_internal(conductor, recording_id);

    if (NULL == recording_session)
    {
        /* No active recording session - return NULL_POSITION */
        aeron_archive_control_session_send_ok_response(session, correlation_id, AERON_ARCHIVE_NULL_POSITION);
    }
    else
    {
        /*
         * In the full implementation:
         * int64_t pos = aeron_archive_recording_session_recorded_position(recording_session);
         * For now, return NULL_POSITION as a placeholder.
         */
        aeron_archive_control_session_send_ok_response(session, correlation_id, AERON_ARCHIVE_NULL_POSITION);
    }

    return 0;
}

int aeron_archive_conductor_get_max_recorded_position(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording id: %" PRId64,
            recording_id);
        return -1;
    }

    aeron_archive_recording_session_t *recording_session =
        aeron_archive_conductor_find_recording_session_internal(conductor, recording_id);

    if (NULL != recording_session)
    {
        /*
         * Active recording - use live position.
         * int64_t pos = aeron_archive_recording_session_recorded_position(recording_session);
         * For now, return NULL_POSITION as a placeholder.
         */
        aeron_archive_control_session_send_ok_response(session, correlation_id, AERON_ARCHIVE_NULL_POSITION);
    }
    else
    {
        /* Not active - use stop position from catalog */
        aeron_archive_catalog_recording_descriptor_t descriptor;
        if (aeron_archive_catalog_find_recording(conductor->catalog, recording_id, &descriptor) < 0)
        {
            return -1;
        }
        aeron_archive_control_session_send_ok_response(session, correlation_id, descriptor.stop_position);
    }

    return 0;
}

/* ---- Truncate / Purge ---- */

int aeron_archive_conductor_truncate_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t position,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording id: %" PRId64,
            recording_id);
        return -1;
    }

    /* Validate: no active replay for this recording */
    for (int32_t i = 0; i < conductor->replay_session_count; i++)
    {
        /*
         * In the full implementation, check:
         * if (conductor->replay_sessions[i].session->recording_id == recording_id)
         * {
         *     send error: "cannot truncate recording with active replay"
         *     return -1;
         * }
         */
    }

    if (aeron_archive_conductor_recording_summary(conductor, recording_id, &conductor->recording_summary) < 0)
    {
        return -1;
    }

    const int64_t stop_position = conductor->recording_summary.stop_position;
    const int64_t start_position = conductor->recording_summary.start_position;

    /* Cannot truncate active recording */
    if (AERON_ARCHIVE_NULL_POSITION == stop_position)
    {
        AERON_SET_ERR(AERON_ARCHIVE_ERROR_ACTIVE_RECORDING, "%s", "cannot truncate active recording");
        return -1;
    }

    /* Validate position bounds and alignment */
    if (position < start_position || position > stop_position ||
        ((position & (AERON_ARCHIVE_FRAME_ALIGNMENT - 1)) != 0))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_GENERIC,
            "invalid truncate position %" PRId64 ": start=%" PRId64 " stop=%" PRId64,
            position, start_position, stop_position);
        return -1;
    }

    /* Update the catalog stop position */
    if (aeron_archive_catalog_update_recording_position(
        conductor->catalog, recording_id, position, conductor->cached_epoch_clock_ms) < 0)
    {
        return -1;
    }

    /*
     * In the full implementation, segment files after the truncation point
     * would be deleted, and the segment at the truncation point would be
     * truncated/erased as needed.
     */

    aeron_archive_control_session_send_ok_response(session, correlation_id, position);

    return 0;
}

int aeron_archive_conductor_purge_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording id: %" PRId64,
            recording_id);
        return -1;
    }

    /* Validate: no active replay for this recording */
    for (int32_t i = 0; i < conductor->replay_session_count; i++)
    {
        /*
         * In the full implementation:
         * if (conductor->replay_sessions[i].session->recording_id == recording_id)
         * {
         *     send error: "cannot purge recording with active replay"
         *     return -1;
         * }
         */
    }

    if (aeron_archive_conductor_recording_summary(conductor, recording_id, &conductor->recording_summary) < 0)
    {
        return -1;
    }

    if (AERON_ARCHIVE_NULL_POSITION == conductor->recording_summary.stop_position)
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_ACTIVE_RECORDING,
            "cannot purge active recording %" PRId64,
            recording_id);
        return -1;
    }

    /* Mark as deleted in the catalog */
    if (aeron_archive_catalog_invalidate_recording(conductor->catalog, recording_id) < 0)
    {
        return -1;
    }

    /*
     * In the full implementation, list and delete all segment files for this recording.
     */

    aeron_archive_control_session_send_ok_response(session, correlation_id, 0);

    return 0;
}

/* ---- Extend recording ---- */

int aeron_archive_conductor_extend_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    int32_t stream_id,
    aeron_archive_source_location_t source_location,
    bool auto_stop,
    const char *original_channel,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;
    (void)auto_stop;

    if (conductor->recording_session_count >= conductor->ctx->max_concurrent_recordings)
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_MAX_RECORDINGS,
            "max concurrent recordings reached at %d",
            conductor->ctx->max_concurrent_recordings);
        return -1;
    }

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording id: %" PRId64,
            recording_id);
        return -1;
    }

    if (aeron_archive_conductor_recording_summary(conductor, recording_id, &conductor->recording_summary) < 0)
    {
        return -1;
    }

    if (stream_id != conductor->recording_summary.stream_id)
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "cannot extend recording %" PRId64 " with streamId=%d for existing streamId=%d",
            recording_id, stream_id, conductor->recording_summary.stream_id);
        return -1;
    }

    if (NULL != aeron_archive_conductor_find_recording_session_internal(conductor, recording_id))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_ACTIVE_RECORDING,
            "cannot extend active recording %" PRId64,
            recording_id);
        return -1;
    }

    /*
     * In the full implementation, create a subscription (with spy prefix if local UDP),
     * register an available image handler that calls extendRecordingSession,
     * and store the subscription mapping.
     */
    (void)source_location;
    (void)original_channel;

    aeron_archive_control_session_send_ok_response(session, correlation_id, recording_id);

    return 0;
}

/* ---- Replication ---- */

int aeron_archive_conductor_replicate(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t src_recording_id,
    int64_t dst_recording_id,
    int64_t stop_position,
    int32_t src_control_stream_id,
    const char *src_control_channel,
    const char *live_destination,
    const char *replication_channel,
    void *control_session)
{
    (void)correlation_id;
    (void)src_recording_id;
    (void)stop_position;
    (void)src_control_stream_id;
    (void)src_control_channel;
    (void)live_destination;
    (void)replication_channel;
    (void)control_session;

    /* Validate destination recording if specified */
    if (AERON_ARCHIVE_NULL_VALUE != dst_recording_id)
    {
        if (!aeron_archive_conductor_has_recording(conductor, dst_recording_id))
        {
            AERON_SET_ERR(
                AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
                "unknown destination recording id %" PRId64,
                dst_recording_id);
            return -1;
        }

        if (aeron_archive_conductor_recording_summary(conductor, dst_recording_id, &conductor->recording_summary) < 0)
        {
            return -1;
        }

        if (AERON_ARCHIVE_NULL_POSITION == conductor->recording_summary.stop_position ||
            NULL != aeron_archive_conductor_find_recording_session_internal(conductor, dst_recording_id))
        {
            AERON_SET_ERR(
                AERON_ARCHIVE_ERROR_ACTIVE_RECORDING,
                "cannot replicate to active recording %" PRId64,
                dst_recording_id);
            return -1;
        }
    }

    /*
     * In the full implementation:
     * 1. Create a ReplicationSession with the remote archive context
     * 2. Add to replication_sessions map
     * 3. Send OK response with the replication session ID
     */

    return 0;
}

int aeron_archive_conductor_stop_replication(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t replication_id,
    void *control_session)
{
    (void)correlation_id;
    (void)control_session;

    for (int32_t i = 0; i < conductor->replication_session_count; i++)
    {
        if (conductor->replication_sessions[i].replication_id == replication_id)
        {
            /*
             * In the full implementation:
             * aeron_archive_replication_session_abort(conductor->replication_sessions[i].session, "stop replication");
             */
            aeron_archive_conductor_remove_replication_session(conductor, replication_id);
            return 0;
        }
    }

    AERON_SET_ERR(
        AERON_ARCHIVE_ERROR_UNKNOWN_REPLICATION,
        "unknown replication id %" PRId64,
        replication_id);
    return -1;
}

int aeron_archive_conductor_list_recording_subscriptions(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int32_t pseudo_index,
    int32_t subscription_count,
    bool apply_stream_id,
    int32_t stream_id,
    const char *channel_fragment,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;
    int32_t count = 0;

    for (int32_t i = pseudo_index;
         i < conductor->recording_subscription_count && count < subscription_count;
         i++)
    {
        const aeron_archive_recording_subscription_entry_t *entry = &conductor->recording_subscriptions[i];

        if (NULL == entry->key)
        {
            continue;
        }

        if (NULL != channel_fragment && strlen(channel_fragment) > 0 &&
            NULL == strstr(entry->key, channel_fragment))
        {
            continue;
        }

        if (apply_stream_id)
        {
            /*
             * The key format is "<streamId>:<channel>". Parse out the stream id
             * and compare.
             */
            int32_t entry_stream_id = 0;
            if (sscanf(entry->key, "%d:", &entry_stream_id) == 1 && entry_stream_id != stream_id)
            {
                continue;
            }
        }

        /*
         * In the full implementation, send each subscription descriptor via the
         * control session. For now just count.
         */
        count++;
    }

    aeron_archive_control_session_send_ok_response(session, correlation_id, (int64_t)count);
    return 0;
}

/* ---- Task queue ---- */

int aeron_archive_conductor_enqueue_task(
    aeron_archive_conductor_t *conductor,
    aeron_archive_conductor_task_func_t func,
    void *clientd)
{
    aeron_archive_conductor_task_t *task = NULL;

    if (aeron_alloc((void **)&task, sizeof(aeron_archive_conductor_task_t)) < 0)
    {
        AERON_SET_ERR(ENOMEM, "%s", "failed to allocate conductor task");
        return -1;
    }

    task->func = func;
    task->clientd = clientd;
    task->next = NULL;

    if (NULL == conductor->task_queue_tail)
    {
        conductor->task_queue_head = task;
        conductor->task_queue_tail = task;
    }
    else
    {
        conductor->task_queue_tail->next = task;
        conductor->task_queue_tail = task;
    }

    return 0;
}

/* ---- Session management ---- */

void aeron_archive_conductor_close_recording_session(
    aeron_archive_conductor_t *conductor,
    aeron_archive_recording_session_t *session)
{
    if (conductor->is_abort)
    {
        aeron_archive_recording_session_abort_close(session);
        return;
    }

    const int64_t recording_id = aeron_archive_recording_session_recording_id(session);
    const int64_t position = aeron_archive_recording_session_recorded_position(session);

    /* Update catalog with the stopped position */
    aeron_archive_catalog_update_recording_position(
        conductor->catalog,
        recording_id,
        position,
        conductor->cached_epoch_clock_ms);

    /* Remove from the active session map */
    aeron_archive_conductor_remove_recording_session(conductor, recording_id);

    /* Close the session itself */
    aeron_archive_recording_session_close(session);
}

aeron_archive_recording_session_t *aeron_archive_conductor_find_recording_session(
    aeron_archive_conductor_t *conductor,
    int64_t recording_id)
{
    return aeron_archive_conductor_find_recording_session_internal(conductor, recording_id);
}

int32_t aeron_archive_conductor_recording_session_count(const aeron_archive_conductor_t *conductor)
{
    return NULL != conductor ? conductor->recording_session_count : 0;
}

int32_t aeron_archive_conductor_replay_session_count(const aeron_archive_conductor_t *conductor)
{
    return NULL != conductor ? conductor->replay_session_count : 0;
}

bool aeron_archive_conductor_is_closed(const aeron_archive_conductor_t *conductor)
{
    return NULL == conductor || conductor->is_closed;
}
