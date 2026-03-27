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

#ifndef AERON_CLUSTER_LOG_REPLAY_H
#define AERON_CLUSTER_LOG_REPLAY_H

#include <stdint.h>
#include <stdbool.h>
#include "aeronc.h"
#include "aeron_archive.h"
#include "aeron_cluster_log_adapter.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Drives replay of a recorded log segment from the archive.
 * Mirrors Java's LogReplay.
 *
 * Lifecycle:
 *   create  → do_work (until is_done) → close
 */
typedef struct aeron_cluster_log_replay_stct
{
    aeron_archive_t               *archive;
    aeron_t                       *aeron;
    aeron_cluster_log_adapter_t   *log_adapter;
    aeron_subscription_t          *log_subscription;
    aeron_async_add_subscription_t *async_subscription;
    int64_t                        replay_session_id;
    int32_t                        log_session_id;
    int64_t                        start_position;
    int64_t                        stop_position;
}
aeron_cluster_log_replay_t;

/**
 * Create and start a log replay.
 *
 * Calls archive.startReplay() synchronously and begins async subscription creation.
 * The replay_channel will have `?sessionId=<id>` (or `|sessionId=<id>`) appended
 * so that only the replay stream is received.
 *
 * @return 0 on success, -1 on error.
 */
int aeron_cluster_log_replay_create(
    aeron_cluster_log_replay_t **replay,
    aeron_archive_t *archive,
    aeron_t *aeron,
    int64_t recording_id,
    int64_t start_position,
    int64_t stop_position,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_cluster_log_adapter_t *log_adapter);

/**
 * Stop the replay and disconnect the log adapter.
 */
int aeron_cluster_log_replay_close(aeron_cluster_log_replay_t *replay);

/**
 * Drive progress: poll subscription creation, image join, and log adapter.
 * @return fragments/events processed (> 0 if work done).
 */
int aeron_cluster_log_replay_do_work(aeron_cluster_log_replay_t *replay);

/** True when log_adapter.position() >= stop_position. */
bool aeron_cluster_log_replay_is_done(aeron_cluster_log_replay_t *replay);

/** Current replay position. */
int64_t aeron_cluster_log_replay_position(aeron_cluster_log_replay_t *replay);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_LOG_REPLAY_H */
