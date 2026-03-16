/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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

#ifndef AERON_AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_H
#define AERON_AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_H

#include "aeronc.h"

#define AERON_PERSISTENT_SUBSCRIPTION_FROM_START (INT64_C(-1))
#define AERON_PERSISTENT_SUBSCRIPTION_FROM_LIVE  (INT64_C(-2))

typedef struct aeron_archive_persistent_subscription_context_stct aeron_archive_persistent_subscription_context_t;
typedef struct aeron_archive_persistent_subscription_stct aeron_archive_persistent_subscription_t;

/**
 * Create and initialize a persistent subscription context.
 *
 * @param context to set if completed successfully.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_init(aeron_archive_persistent_subscription_context_t **context);

/**
 * Close and dispose of all resources held by the persistent subscription context.
 *
 * @param context to close.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_close(aeron_archive_persistent_subscription_context_t *context);

/**
 * Set the Aeron client that will be used by the persistent subscription.
 *
 * @param context to configure.
 * @param aeron the Aeron client to use.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_aeron(
    aeron_archive_persistent_subscription_context_t *context,
    aeron_t *aeron);

/**
 * Set the Aeron Archive client context that will be used by the persistent subscription.
 *
 * @param context to configure.
 * @param archive_context the Aeron Archive client context to use.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_archive_context(
    aeron_archive_persistent_subscription_context_t *context,
    aeron_archive_context_t *archive_context);

/**
 * Set the id of the live stream recording that will be used by the persistent subscription to catch up.
 *
 * @param context to configure.
 * @param recording_id the recording id to use.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_recording_id(
    aeron_archive_persistent_subscription_context_t *context,
    int64_t recording_id);

/**
 * Set the live channel.
 *
 * @param context to configure.
 * @param live_channel the live channel which will be copied.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_live_channel(
    aeron_archive_persistent_subscription_context_t *context,
    const char *live_channel);

/**
 * Set the id of the live stream.
 *
 * @param context to configure.
 * @param live_stream_id the live stream id.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_live_stream_id(
    aeron_archive_persistent_subscription_context_t *context,
    int32_t live_stream_id);

/**
 * Set the channel used for replays.
 *
 * @param context to configure.
 * @param replay_channel the channel to use for replays which will be copied.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_replay_channel(
    aeron_archive_persistent_subscription_context_t *context,
    const char *replay_channel);

/**
 * Set the id of the stream used for replays.
 *
 * @param context to configure.
 * @param replay_stream_id the stream id to use for replays.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_replay_stream_id(
    aeron_archive_persistent_subscription_context_t *context,
    int32_t replay_stream_id);

/**
 * Set the position to start the subscription from, can be an actual position or
 * AERON_PERSISTENT_SUBSCRIPTION_FROM_START or AERON_PERSISTENT_SUBSCRIPTION_FROM_LIVE.
 *
 * @param context to configure.
 * @param start_position the position to start the subscription from.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_start_position(
    aeron_archive_persistent_subscription_context_t *context,
    int64_t start_position);

/**
 * Create a persistent subscription.
 * TODO something about context ownership
 *
 * @param persistent_subscription to set if completed successfully.
 * @param context with the configuration of a persistent subscription to be created.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_create(
    aeron_archive_persistent_subscription_t **persistent_subscription,
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Close a persistent subscription and dispose of all resources and memory held by it.
 *
 * @param persistent_subscription to close.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_close(aeron_archive_persistent_subscription_t *persistent_subscription);

/**
 * Poll in a controlled manner a persistent subscription for available messages.
 * <p>
 * Delivers assembled messages to the handler, so the handler shouldn't be a fragment assembler.
 *
 * @param persistent_subscription to poll.
 * @param handler for handling read messages.
 * @param clientd to pass to the handler.
 * @param fragment_limit maximum number of message fragments to read when polling.
 * @return positive number if fragments have been read or the persistent subscription has done other work,
 * 0 if no fragments have been read and no work has been done, negative on error.
 */
int aeron_archive_persistent_subscription_controlled_poll(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    aeron_controlled_fragment_handler_t handler,
    void *clientd,
    size_t fragment_limit);

/**
 * Check if persistent subscription is live, i.e. reading messages from the live subscription without having a replay
 * subscription.
 *
 * @param persistent_subscription to check.
 * @return true if live, false otherwise.
 */
bool aeron_archive_persistent_subscription_is_live(aeron_archive_persistent_subscription_t *persistent_subscription);

/**
 * Indicates if the persistent subscription is replaying from a recording.
 *
 * @param persistent_subscription to check.
 * @return true if replaying, false otherwise.
 */
bool aeron_archive_persistent_subscription_is_replaying(aeron_archive_persistent_subscription_t *persistent_subscription);

/**
 * Indicates if the persistent subscription has failed.
 *
 * @param persistent_subscription to check.
 * @return true if failed, false otherwise.
 */
bool aeron_archive_persistent_subscription_has_failed(aeron_archive_persistent_subscription_t *persistent_subscription);

#endif //AERON_AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_H
