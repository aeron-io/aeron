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
#include "aeron_archive.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_START (INT64_C(-1))
#define AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE  (INT64_C(-2))

typedef struct aeron_archive_persistent_subscription_context_stct aeron_archive_persistent_subscription_context_t;
typedef struct aeron_archive_persistent_subscription_stct aeron_archive_persistent_subscription_t;

/**
 * Listener for events from a persistent subscription.
 * <p>
 * All callbacks are invoked from the thread that calls aeron_archive_persistent_subscription_poll
 * or aeron_archive_persistent_subscription_controlled_poll.
 */
typedef struct aeron_archive_persistent_subscription_listener_stct
{
    /**
     * Invoked when the persistent subscription transitions to the live state.
     *
     * @param clientd the clientd set on this listener.
     */
    void (*on_live_joined)(void *clientd);

    /**
     * Invoked when the persistent subscription transitions from the live state.
     *
     * @param clientd the clientd set on this listener.
     */
    void (*on_live_left)(void *clientd);

    /**
     * Invoked when the persistent subscription encounters an error.
     * <p>
     * The message pointer refers to a stack-allocated buffer owned by the caller and is only valid
     * for the duration of this callback.
     *
     * @param clientd the clientd set on this listener.
     * @param errcode the error code describing the failure.
     * @param message a human-readable error message. Valid only for the duration of this callback;
     *                copy if it must be retained.
     */
    void (*on_error)(void *clientd, int errcode, const char *message);

    /**
     * Opaque user data passed to each of the callbacks above. The persistent subscription does not
     * dereference, copy, or take ownership of this pointer. The caller is responsible for ensuring
     * it remains valid for the lifetime of the persistent subscription.
     */
    void *clientd;
}
aeron_archive_persistent_subscription_listener_t;

/**
 * Create and initialize a persistent subscription context.
 *
 * @param context to set if completed successfully.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_init(aeron_archive_persistent_subscription_context_t **context);

/**
 * Close and dispose of all resources held by the persistent subscription context.
 * <p>
 * If the context created its own Aeron client (i.e. none was set via
 * aeron_archive_persistent_subscription_context_set_aeron), that client will be closed here.
 *
 * @param context to close.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_close(aeron_archive_persistent_subscription_context_t *context);

/**
 * Set the Aeron client that will be used by the persistent subscription.
 * <p>
 * If not set, the persistent subscription will create and own its own Aeron client when
 * aeron_archive_persistent_subscription_create is called. In that case, the client will be
 * closed when the context is closed via aeron_archive_persistent_subscription_context_close.
 *
 * @param context to configure.
 * @param aeron the Aeron client to use.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_aeron(
    aeron_archive_persistent_subscription_context_t *context,
    aeron_t *aeron);

/**
 * Set the Aeron directory name to use when the persistent subscription creates its own Aeron client.
 * Has no effect if an Aeron client is set via aeron_archive_persistent_subscription_context_set_aeron.
 * <p>
 * The directory name is copied into the context. The caller retains ownership of the supplied string.
 *
 * @param context to configure.
 * @param aeron_directory_name the Aeron directory name.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_aeron_directory_name(
    aeron_archive_persistent_subscription_context_t *context,
    const char *aeron_directory_name);

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
 * Get the id of the live stream recording that will be used by the persistent subscription to catch up.
 *
 * @param context to query.
 * @return the recording id.
 * @see aeron_archive_persistent_subscription_context_set_recording_id
 */
int64_t aeron_archive_persistent_subscription_context_get_recording_id(
    aeron_archive_persistent_subscription_context_t *context);

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
 * Get the live channel.
 *
 * @param context to query.
 * @return the live channel.
 * @see aeron_archive_persistent_subscription_context_set_live_channel
 */
const char *aeron_archive_persistent_subscription_context_get_live_channel(
    aeron_archive_persistent_subscription_context_t *context);

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
 * Get the id of the live stream.
 *
 * @param context to query.
 * @return the live stream id.
 * @see aeron_archive_persistent_subscription_context_set_live_stream_id
 */
int32_t aeron_archive_persistent_subscription_context_get_live_stream_id(
    aeron_archive_persistent_subscription_context_t *context);

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
 * Get the channel used for replays.
 *
 * @param context to query.
 * @return the replay channel.
 * @see aeron_archive_persistent_subscription_context_set_replay_channel
 */
const char *aeron_archive_persistent_subscription_context_get_replay_channel(
    aeron_archive_persistent_subscription_context_t *context);

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
 * Get the id of the stream used for replays.
 *
 * @param context to query.
 * @return the replay stream id.
 * @see aeron_archive_persistent_subscription_context_set_replay_stream_id
 */
int32_t aeron_archive_persistent_subscription_context_get_replay_stream_id(
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Set the position to start the subscription from, can be an actual position or
 * AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_START or AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE.
 *
 * @param context to configure.
 * @param start_position the position to start the subscription from.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_start_position(
    aeron_archive_persistent_subscription_context_t *context,
    int64_t start_position);

/**
 * Get the position to start the subscription from.
 *
 * @param context to query.
 * @return the start position.
 * @see aeron_archive_persistent_subscription_context_set_start_position
 */
int64_t aeron_archive_persistent_subscription_context_get_start_position(
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Set the listener for events from the persistent subscription.
 *
 * @param context to configure.
 * @param listener the listener to set. The content of the listener struct is copied by value.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_listener(
    aeron_archive_persistent_subscription_context_t *context,
    const aeron_archive_persistent_subscription_listener_t *listener);

/**
 * Set the counter for tracking the current state of the persistent subscription.
 *
 * @param context to configure.
 * @param counter the state counter.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_state_counter(
    aeron_archive_persistent_subscription_context_t *context,
    aeron_counter_t *counter);

/**
 * Get the counter for tracking the current state of the persistent subscription.
 *
 * @param context to query.
 * @return the state counter.
 * @see aeron_archive_persistent_subscription_context_set_state_counter
 */
aeron_counter_t *aeron_archive_persistent_subscription_context_get_state_counter(
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Set the counter for tracking the join difference of the persistent subscription.
 * The join difference is the difference between the live position and the replay position when
 * transitioning. When not live, the value is INT64_MIN.
 *
 * @param context to configure.
 * @param counter the join difference counter.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_join_difference_counter(
    aeron_archive_persistent_subscription_context_t *context,
    aeron_counter_t *counter);

/**
 * Get the counter for tracking the join difference of the persistent subscription.
 *
 * @param context to query.
 * @return the join difference counter.
 * @see aeron_archive_persistent_subscription_context_set_join_difference_counter
 */
aeron_counter_t *aeron_archive_persistent_subscription_context_get_join_difference_counter(
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Set the counter for tracking the number of times the live stream has been left.
 *
 * @param context to configure.
 * @param counter the live left counter.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_live_left_counter(
    aeron_archive_persistent_subscription_context_t *context,
    aeron_counter_t *counter);

/**
 * Get the counter for tracking the number of times the live stream has been left.
 *
 * @param context to query.
 * @return the live left counter.
 * @see aeron_archive_persistent_subscription_context_set_live_left_counter
 */
aeron_counter_t *aeron_archive_persistent_subscription_context_get_live_left_counter(
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Set the counter for tracking the number of times live has been joined.
 *
 * @param context to configure.
 * @param counter the live joined counter.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_live_joined_counter(
    aeron_archive_persistent_subscription_context_t *context,
    aeron_counter_t *counter);

/**
 * Get the counter for tracking the number of times live has been joined.
 *
 * @param context to query.
 * @return the live joined counter.
 * @see aeron_archive_persistent_subscription_context_set_live_joined_counter
 */
aeron_counter_t *aeron_archive_persistent_subscription_context_get_live_joined_counter(
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Create a persistent subscription.
 * <p>
 * If creating a subscription succeeds, then the subscription will own the context. And closing the
 * subscription, will close the context.
 * <p>
 * If no Aeron client is set on the context, one will be created and owned by the context,
 * and will be closed when the context is closed via aeron_archive_persistent_subscription_context_close.
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
 * Poll a persistent subscription for available messages.
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
int aeron_archive_persistent_subscription_poll(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    aeron_fragment_handler_t handler,
    void *clientd,
    size_t fragment_limit);

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
 * <p>
 * The listener will be notified of any terminal errors that can cause the persistent subscription to fail.
 *
 * @param persistent_subscription to check.
 * @return true if failed, false otherwise.
 * @see aeron_archive_persistent_subscription_context_set_listener
 */
bool aeron_archive_persistent_subscription_has_failed(aeron_archive_persistent_subscription_t *persistent_subscription);

/**
 * Get the terminal error that caused the persistent subscription to fail.
 * Only meaningful when aeron_archive_persistent_subscription_has_failed returns true.
 *
 * @param persistent_subscription to query.
 * @param out_errcode optional pointer to receive the error code, may be NULL.
 * @param out_message optional pointer to receive the error message, may be NULL.
 * @return true if in the failed state, false otherwise.
 * @see aeron_archive_persistent_subscription_has_failed
 */
bool aeron_archive_persistent_subscription_failure_reason(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    int *out_errcode,
    const char **out_message);

#ifdef __cplusplus
}
#endif

#endif //AERON_AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_H
