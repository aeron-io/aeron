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

#include <errno.h>
#include <stdio.h>
#include <c/aeron_archive_client/controlResponseCode.h>

#include "aeron_alloc.h"
#include "aeron_archive.h"
#include "aeron_archive_persistent_subscription.h"
#include "aeron_archive_context.h"
#include "aeron_counter.h"
#include "aeron_fragment_assembler.h"
#include "aeron_subscription.h"
#include "aeron_counters.h"
#include "concurrent/aeron_counters_manager.h"
#include "concurrent/aeron_thread.h"
#include "util/aeron_error.h"


static inline int aeron_archive_persistent_subscription_errcode(void)
{
    int errcode = aeron_errcode();
    return errcode < 0 ? -errcode : errcode;
}

static void aeron_archive_persistent_subscription_async_archive_op_init(
    aeron_archive_persistent_subscription_async_archive_op_t *op,
    int64_t correlation_id,
    int64_t deadline_ns)
{
    op->correlation_id = correlation_id;
    op->deadline_ns = deadline_ns;
    op->error_message[0] = '\0';
    op->response_received = false;
}

static void aeron_archive_persistent_subscription_async_archive_op_on_control_response(
    aeron_archive_persistent_subscription_async_archive_op_t *op,
    int64_t relevant_id,
    int32_t code,
    const char *error_message)
{
    op->relevant_id = relevant_id;
    op->code = code;
    strncpy(op->error_message, NULL != error_message ? error_message : "", AERON_ERROR_MAX_TOTAL_LENGTH - 1);
    op->error_message[AERON_ERROR_MAX_TOTAL_LENGTH - 1] = '\0';
    op->response_received = true;
}

static void aeron_archive_persistent_subscription_max_recorded_position_reset(
    max_recorded_position_t *max_recorded_position,
    int32_t close_enough_threshold)
{
    max_recorded_position->state = REQUEST_MAX_POSITION;
    max_recorded_position->close_enough_threshold = close_enough_threshold;
}

static void aeron_archive_persistent_subscription_transition(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    aeron_archive_persistent_subscription_state_t new_state)
{
    persistent_subscription->state = new_state;
    aeron_counter_t *state_counter = persistent_subscription->context->state_counter;
    if (!aeron_counter_is_closed(state_counter))
    {
        aeron_counter_set_release(aeron_counter_addr(state_counter), new_state);
    }
    if (FAILED == new_state)
    {
        aeron_archive_async_client_close(persistent_subscription->archive);
    }
}

int aeron_archive_persistent_subscription_context_init(aeron_archive_persistent_subscription_context_t **context)
{
    if (NULL == context)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, context: %s",
            AERON_NULL_STR(context));
        return -1;
    }

    aeron_archive_persistent_subscription_context_t *_context = NULL;

    if (aeron_alloc((void **)&_context, sizeof(aeron_archive_persistent_subscription_context_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to allocate aeron_archive_persistent_subscription_context_t");
        return -1;
    }

    _context->recording_id = AERON_NULL_VALUE;
    _context->live_stream_id = AERON_NULL_VALUE;
    _context->replay_stream_id = AERON_NULL_VALUE;
    _context->start_position = AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE;

    *context = _context;

    return 0;
}

int aeron_archive_persistent_subscription_context_close(aeron_archive_persistent_subscription_context_t *context)
{
    if (NULL != context)
    {
        aeron_counter_close(context->state_counter, NULL, NULL);
        aeron_counter_close(context->join_difference_counter, NULL, NULL);
        aeron_counter_close(context->live_left_counter, NULL, NULL);
        aeron_counter_close(context->live_joined_counter, NULL, NULL);
        free(context->aeron_directory_name);
        free(context->live_channel);
        free(context->replay_channel);
        if (context->owns_aeron_client)
        {
            aeron_close(context->aeron);
            aeron_context_close(context->aeron_ctx);
        }
        aeron_free(context);
    }

    return 0;
}

static int aeron_archive_persistent_subscription_set_string(char **ptr, const char *val)
{
    free(*ptr);
    *ptr = NULL;

    if (NULL != val)
    {
        *ptr = strdup(val);
        if (NULL == *ptr)
        {
            AERON_SET_ERR(errno, "%s", "Failed to duplicate string");
            return -1;
        }
    }

    return 0;
}

int aeron_archive_persistent_subscription_context_set_aeron(
    aeron_archive_persistent_subscription_context_t *context,
    aeron_t *aeron)
{
    context->aeron = aeron;

    return 0;
}

int aeron_archive_persistent_subscription_context_set_aeron_directory_name(
    aeron_archive_persistent_subscription_context_t *context,
    const char *aeron_directory_name)
{
    return aeron_archive_persistent_subscription_set_string(&context->aeron_directory_name, aeron_directory_name);
}

int aeron_archive_persistent_subscription_context_set_archive_context(
    aeron_archive_persistent_subscription_context_t *context,
    aeron_archive_context_t *archive_context)
{
    context->archive_context = archive_context;

    return 0;
}

int aeron_archive_persistent_subscription_context_set_recording_id(
    aeron_archive_persistent_subscription_context_t *context,
    int64_t recording_id)
{
    context->recording_id = recording_id;

    return 0;
}

int64_t aeron_archive_persistent_subscription_context_get_recording_id(
    aeron_archive_persistent_subscription_context_t *context)
{
    return context->recording_id;
}

int aeron_archive_persistent_subscription_context_set_live_channel(
    aeron_archive_persistent_subscription_context_t *context,
    const char *live_channel)
{
    return aeron_archive_persistent_subscription_set_string(&context->live_channel, live_channel);
}

const char *aeron_archive_persistent_subscription_context_get_live_channel(
    aeron_archive_persistent_subscription_context_t *context)
{
    return context->live_channel;
}

int aeron_archive_persistent_subscription_context_set_live_stream_id(
    aeron_archive_persistent_subscription_context_t *context,
    int32_t live_stream_id)
{
    context->live_stream_id = live_stream_id;

    return 0;
}

int32_t aeron_archive_persistent_subscription_context_get_live_stream_id(
    aeron_archive_persistent_subscription_context_t *context)
{
    return context->live_stream_id;
}

int aeron_archive_persistent_subscription_context_set_replay_channel(
    aeron_archive_persistent_subscription_context_t *context,
    const char *replay_channel)
{
    return aeron_archive_persistent_subscription_set_string(&context->replay_channel, replay_channel);
}

const char *aeron_archive_persistent_subscription_context_get_replay_channel(
    aeron_archive_persistent_subscription_context_t *context)
{
    return context->replay_channel;
}

int aeron_archive_persistent_subscription_context_set_replay_stream_id(
    aeron_archive_persistent_subscription_context_t *context,
    int32_t replay_stream_id)
{
    context->replay_stream_id = replay_stream_id;

    return 0;
}

int32_t aeron_archive_persistent_subscription_context_get_replay_stream_id(
    aeron_archive_persistent_subscription_context_t *context)
{
    return context->replay_stream_id;
}

int aeron_archive_persistent_subscription_context_set_start_position(
    aeron_archive_persistent_subscription_context_t *context,
    int64_t start_position)
{
    context->start_position = start_position;

    return 0;
}

int64_t aeron_archive_persistent_subscription_context_get_start_position(
    aeron_archive_persistent_subscription_context_t *context)
{
    return context->start_position;
}

int aeron_archive_persistent_subscription_context_set_listener(
    aeron_archive_persistent_subscription_context_t *context,
    const aeron_archive_persistent_subscription_listener_t *listener)
{
    if (NULL == listener)
    {
        AERON_SET_ERR(EINVAL, "%s", "listener must not be null");
        return -1;
    }

    context->listener = *listener;

    return 0;
}

aeron_counter_t *aeron_archive_persistent_subscription_context_get_state_counter(
    aeron_archive_persistent_subscription_context_t *context)
{
    return context->state_counter;
}

int aeron_archive_persistent_subscription_context_set_state_counter(
    aeron_archive_persistent_subscription_context_t *context,
    aeron_counter_t *counter)
{
    context->state_counter = counter;

    return 0;
}

int aeron_archive_persistent_subscription_context_set_join_difference_counter(
    aeron_archive_persistent_subscription_context_t *context,
    aeron_counter_t *counter)
{
    context->join_difference_counter = counter;

    return 0;
}

aeron_counter_t *aeron_archive_persistent_subscription_context_get_join_difference_counter(
    aeron_archive_persistent_subscription_context_t *context)
{
    return context->join_difference_counter;
}

int aeron_archive_persistent_subscription_context_set_live_left_counter(
    aeron_archive_persistent_subscription_context_t *context,
    aeron_counter_t *counter)
{
    context->live_left_counter = counter;

    return 0;
}

aeron_counter_t *aeron_archive_persistent_subscription_context_get_live_left_counter(
    aeron_archive_persistent_subscription_context_t *context)
{
    return context->live_left_counter;
}

int aeron_archive_persistent_subscription_context_set_live_joined_counter(
    aeron_archive_persistent_subscription_context_t *context,
    aeron_counter_t *counter)
{
    context->live_joined_counter = counter;

    return 0;
}

aeron_counter_t *aeron_archive_persistent_subscription_context_get_live_joined_counter(
    aeron_archive_persistent_subscription_context_t *context)
{
    return context->live_joined_counter;
}

static int aeron_archive_persistent_subscription_persistent_subscription_allocate_counter(
    aeron_counter_t **counter_out,
    aeron_t *aeron,
    int32_t type_id,
    const char *name,
    int32_t replay_stream_id,
    const char *replay_channel,
    int32_t live_stream_id,
    const char *live_channel)
{
    char label[AERON_COUNTER_MAX_LABEL_LENGTH];
    int label_length = snprintf(
        label, sizeof(label), "%s: %d %s %d %s",
        name, replay_stream_id, replay_channel, live_stream_id, live_channel);

    if (label_length < 0 || (size_t)label_length >= sizeof(label))
    {
        label_length = (int)(sizeof(label) - 1);
    }

    aeron_async_add_counter_t *async = NULL;
    if (aeron_async_add_counter(&async, aeron, type_id, NULL, 0, label, (size_t)label_length) < 0)
    {
        return -1;
    }

    aeron_context_t *ctx = aeron_context(aeron);
    bool use_agent_invoker = aeron_context_get_use_conductor_agent_invoker(ctx);

    aeron_counter_t *counter = NULL;
    while (NULL == counter)
    {
        int result = aeron_async_add_counter_poll(&counter, async);
        if (result < 0)
        {
            return -1;
        }
        if (0 == result)
        {
            if (use_agent_invoker)
            {
                aeron_main_do_work(aeron);
            }
            else
            {
                sched_yield();
            }
        }
    }

    *counter_out = counter;
    return 0;
}

int aeron_archive_persistent_subscription_context_conclude(aeron_archive_persistent_subscription_context_t *context)
{
    if (NULL == context->archive_context)
    {
        AERON_SET_ERR(EINVAL, "%s", "archive_context must be set");
        return -1;
    }

    if (AERON_NULL_VALUE == context->recording_id)
    {
        AERON_SET_ERR(EINVAL, "%s", "recording_id must be set");
        return -1;
    }

    if (context->recording_id < 0)
    {
        AERON_SET_ERR(EINVAL, "invalid recording_id %" PRIi64, context->recording_id);
        return -1;
    }

    if (NULL == context->live_channel)
    {
        AERON_SET_ERR(EINVAL, "%s", "live_channel must be set");
        return -1;
    }

    if (AERON_NULL_VALUE == context->live_stream_id)
    {
        AERON_SET_ERR(EINVAL, "%s", "live_stream_id must be set");
        return -1;
    }

    if (NULL == context->replay_channel)
    {
        AERON_SET_ERR(EINVAL, "%s", "replay_channel must be set");
        return -1;
    }

    if (AERON_NULL_VALUE == context->replay_stream_id)
    {
        AERON_SET_ERR(EINVAL, "%s", "replay_stream_id must be set");
        return -1;
    }

    if (AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE > context->start_position)
    {
        AERON_SET_ERR(EINVAL, "invalid start_position %" PRIi64, context->start_position);
        return -1;
    }

    aeron_uri_t uri;
    if (aeron_uri_parse(strlen(context->replay_channel), context->replay_channel, &uri) < 0)
    {
        aeron_uri_close(&uri);
        AERON_APPEND_ERR("failed to parse replay channel '%s'", context->replay_channel);
        return -1;
    }
    const char *control_mode = AERON_URI_UDP == uri.type ? uri.params.udp.control_mode
        : AERON_URI_IPC == uri.type ? uri.params.ipc.control_mode : NULL;
    const bool is_response = NULL != control_mode &&
        0 == strcmp(AERON_UDP_CHANNEL_CONTROL_MODE_RESPONSE_VALUE, control_mode);
    if (is_response)
    {
        const char *control_request_channel = aeron_archive_context_get_control_request_channel(context->archive_context);
        if (NULL != control_request_channel)
        {
            const aeron_uri_type_t replay_media = uri.type;
            aeron_uri_close(&uri);
            if (aeron_uri_parse(strlen(control_request_channel), control_request_channel, &uri) < 0)
            {
                aeron_uri_close(&uri);
                AERON_APPEND_ERR("failed to parse control request channel '%s'", control_request_channel);
                return -1;
            }
            if (replay_media != uri.type)
            {
                aeron_uri_close(&uri);
                AERON_SET_ERR(EINVAL, "%s", "Channel media type mismatch. "
                            "When using `control-mode=response`, the replay channel media type must match the media"
                            " type for the archive control channel.");
                return -1;
            }
        }
    }
    aeron_uri_close(&uri);

    aeron_uri_string_builder_t builder;
    char rebuilt[AERON_URI_MAX_LENGTH];
    if (aeron_uri_string_builder_init_on_string(&builder, context->replay_channel) < 0)
    {
        AERON_APPEND_ERR("failed to parse replay channel '%s'", context->replay_channel);
        return -1;
    }
    if (aeron_uri_string_builder_put(&builder, AERON_URI_REJOIN_KEY, "false") < 0 ||
        aeron_uri_string_builder_sprint(&builder, rebuilt, sizeof(rebuilt)) < 0)
    {
        aeron_uri_string_builder_close(&builder);
        AERON_APPEND_ERR("failed to set rejoin=false on replay channel '%s'", context->replay_channel);
        return -1;
    }
    aeron_uri_string_builder_close(&builder);
    if (aeron_archive_persistent_subscription_set_string(&context->replay_channel, rebuilt) < 0)
    {
        AERON_APPEND_ERR("failed to set replay channel '%s'", rebuilt);
        return -1;
    }

    if (NULL == context->aeron)
    {
        aeron_context_t *aeron_ctx;
        if (aeron_context_init(&aeron_ctx) < 0)
        {
            AERON_APPEND_ERR("%s", "Failed to init aeron context");
            return -1;
        }
        if (aeron_context_set_use_conductor_agent_invoker(aeron_ctx, true) < 0)
        {
            aeron_context_close(aeron_ctx);
            AERON_APPEND_ERR("%s", "Failed to set use_conductor_agent_invoker");
            return -1;
        }
        if (NULL != context->aeron_directory_name)
        {
            aeron_context_set_dir(aeron_ctx, context->aeron_directory_name);
        }
        if (aeron_init(&context->aeron, aeron_ctx) < 0)
        {
            aeron_context_close(aeron_ctx);
            AERON_APPEND_ERR("%s", "Failed to init aeron");
            return -1;
        }
        if (aeron_start(context->aeron) < 0)
        {
            aeron_close(context->aeron);
            context->aeron = NULL;
            aeron_context_close(aeron_ctx);
            AERON_APPEND_ERR("%s", "Failed to start aeron");
            return -1;
        }
        context->aeron_ctx = aeron_ctx;
        context->owns_aeron_client = true;
    }

    // Auto-allocate counters if not user-provided
    if (NULL == context->state_counter)
    {
        if (aeron_archive_persistent_subscription_persistent_subscription_allocate_counter(&context->state_counter, context->aeron,
            AERON_PERSISTENT_SUBSCRIPTION_STATE_TYPE_ID, "Persistent Subscription State",
            context->replay_stream_id, context->replay_channel,
            context->live_stream_id, context->live_channel) < 0)
        {
            AERON_APPEND_ERR("%s", "Failed to add state counter");
            return -1;
        }
    }
    if (NULL == context->join_difference_counter)
    {
        if (aeron_archive_persistent_subscription_persistent_subscription_allocate_counter(&context->join_difference_counter, context->aeron,
            AERON_PERSISTENT_SUBSCRIPTION_JOIN_DIFFERENCE_TYPE_ID, "Persistent Subscription Join Difference",
            context->replay_stream_id, context->replay_channel,
            context->live_stream_id, context->live_channel) < 0)
        {
            AERON_APPEND_ERR("%s", "Failed to add join difference counter");
            return -1;
        }
    }
    if (NULL == context->live_left_counter)
    {
        if (aeron_archive_persistent_subscription_persistent_subscription_allocate_counter(&context->live_left_counter, context->aeron,
            AERON_PERSISTENT_SUBSCRIPTION_LIVE_LEFT_COUNT_TYPE_ID, "Persistent Subscription Live Left Count",
            context->replay_stream_id, context->replay_channel,
            context->live_stream_id, context->live_channel) < 0)
        {
            AERON_APPEND_ERR("%s", "Failed to add live left counter");
            return -1;
        }
    }
    if (NULL == context->live_joined_counter)
    {
        if (aeron_archive_persistent_subscription_persistent_subscription_allocate_counter(&context->live_joined_counter, context->aeron,
            AERON_PERSISTENT_SUBSCRIPTION_LIVE_JOINED_COUNT_TYPE_ID, "Persistent Subscription Live Joined Count",
            context->replay_stream_id, context->replay_channel,
            context->live_stream_id, context->live_channel) < 0)
        {
            AERON_APPEND_ERR("%s", "Failed to add live joined counter");
            return -1;
        }
    }

    return 0;
}

static void aeron_archive_persistent_subscription_on_terminal_error(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    int errcode,
    const char *message)
{
    persistent_subscription->failure_errcode = errcode;
    strncpy(persistent_subscription->failure_message, message, sizeof(persistent_subscription->failure_message) - 1);
    persistent_subscription->failure_message[sizeof(persistent_subscription->failure_message) - 1] = '\0';

    if (NULL != persistent_subscription->listener.on_error)
    {
        persistent_subscription->listener.on_error(persistent_subscription->listener.clientd, errcode, message);
    }
}

static bool aeron_archive_persistent_subscription_max_recorded_position_request_max_position(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    int64_t correlation_id = aeron_next_correlation_id(persistent_subscription->context->aeron);

    if (aeron_archive_async_client_try_send_max_recorded_position_request(
        persistent_subscription->archive,
        correlation_id,
        persistent_subscription->context->recording_id))
    {
        int64_t deadline_ns = aeron_nano_clock() + persistent_subscription->message_timeout_ns;
        aeron_archive_persistent_subscription_async_archive_op_init(&persistent_subscription->max_recorded_position.op, correlation_id, deadline_ns);
        persistent_subscription->max_recorded_position.state = AWAIT_MAX_POSITION;
    }

    return false;
}

static bool aeron_archive_persistent_subscription_max_recorded_position_await_max_position(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    int64_t replayed_position)
{
    max_recorded_position_t *max_recorded_position = &persistent_subscription->max_recorded_position;

    if (max_recorded_position->op.response_received)
    {
        if (max_recorded_position->op.code == aeron_archive_client_controlResponseCode_OK)
        {
            max_recorded_position->max_recorded_position = max_recorded_position->op.relevant_id;

            if (replayed_position >=
                max_recorded_position->max_recorded_position - max_recorded_position->close_enough_threshold)
            {
                return true;
            }
            else
            {
                max_recorded_position->state = RECHECK_REQUIRED;

                return false;
            }
        }
        else
        {
            aeron_archive_persistent_subscription_transition(persistent_subscription, FAILED);

            char message[AERON_ERROR_MAX_TOTAL_LENGTH + 42];
            snprintf(message, sizeof(message), "Get max recorded position request failed: %s",
                max_recorded_position->op.error_message);

            aeron_archive_persistent_subscription_on_terminal_error(
                persistent_subscription,
                (int)max_recorded_position->op.relevant_id,
                message);
        }
    }
    else
    {
        if (aeron_nano_clock() - max_recorded_position->op.deadline_ns >= 0)
        {
            max_recorded_position->state = REQUEST_MAX_POSITION;
        }
    }

    return false;
}

static bool aeron_archive_persistent_subscription_max_recorded_position_recheck_required(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    int64_t replayed_position)
{
    if (replayed_position >= persistent_subscription->max_recorded_position.max_recorded_position)
    {
        persistent_subscription->max_recorded_position.state = REQUEST_MAX_POSITION;
    }

    return false;
}

static bool aeron_archive_persistent_subscription_max_recorded_position_is_caught_up(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    int64_t replayed_position)
{
    switch (persistent_subscription->max_recorded_position.state)
    {
        case REQUEST_MAX_POSITION:
            return aeron_archive_persistent_subscription_max_recorded_position_request_max_position(persistent_subscription);
        case AWAIT_MAX_POSITION:
            return aeron_archive_persistent_subscription_max_recorded_position_await_max_position(persistent_subscription, replayed_position);
        case RECHECK_REQUIRED:
            return aeron_archive_persistent_subscription_max_recorded_position_recheck_required(persistent_subscription, replayed_position);
    }

    return false;
}

static void aeron_archive_persistent_subscription_clean_up_request_publication(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    if (NULL != persistent_subscription->add_request_publication)
    {
        aeron_async_add_exclusive_publication_cancel(
            persistent_subscription->context->aeron,
            persistent_subscription->add_request_publication);
    }

    if (NULL != persistent_subscription->request_publication)
    {
        aeron_exclusive_publication_close(persistent_subscription->request_publication, NULL, NULL);
    }

    if (NULL != persistent_subscription->response_channel_archive_proxy)
    {
        aeron_archive_proxy_delete(persistent_subscription->response_channel_archive_proxy);
    }

    persistent_subscription->add_request_publication = NULL;
    persistent_subscription->request_publication = NULL;
    persistent_subscription->response_channel_archive_proxy = NULL;
}

static void aeron_archive_persistent_subscription_clean_up_replay(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    if (AERON_NULL_VALUE != persistent_subscription->replay_session_id)
    {
        aeron_archive_async_client_try_send_stop_replay_request(
            persistent_subscription->archive,
            aeron_next_correlation_id(persistent_subscription->context->aeron),
            persistent_subscription->replay_session_id);

        persistent_subscription->replay_session_id = AERON_NULL_VALUE;
    }
}

static void aeron_archive_persistent_subscription_clean_up_replay_subscription(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    if (NULL != persistent_subscription->add_replay_subscription)
    {
        aeron_async_add_subscription_cancel(
            persistent_subscription->context->aeron,
            persistent_subscription->add_replay_subscription);
    }

    if (NULL != persistent_subscription->replay_subscription)
    {
        aeron_subscription_close(persistent_subscription->replay_subscription, NULL, NULL);
    }

    persistent_subscription->add_replay_subscription = NULL;
    persistent_subscription->replay_subscription = NULL;
    persistent_subscription->replay_image = NULL;
}

static void aeron_archive_persistent_subscription_clean_up_live_subscription(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    if (NULL != persistent_subscription->add_live_subscription)
    {
        aeron_async_add_subscription_cancel(
            persistent_subscription->context->aeron,
            persistent_subscription->add_live_subscription);
    }

    if (NULL != persistent_subscription->live_subscription)
    {
        aeron_subscription_close(persistent_subscription->live_subscription, NULL, NULL);
    }

    persistent_subscription->add_live_subscription = NULL;
    persistent_subscription->live_subscription = NULL;
    persistent_subscription->live_image = NULL;
}

static void aeron_archive_persistent_subscription_set_live_image_deadline(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    persistent_subscription->live_image_deadline_ns = aeron_nano_clock() + persistent_subscription->message_timeout_ns;
    persistent_subscription->live_image_deadline_breached = false;
}

static void aeron_archive_persistent_subscription_on_live_image_deadline_breached(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    persistent_subscription->live_image_deadline_breached = true;

    if (NULL != persistent_subscription->listener.on_error)
    {
        persistent_subscription->listener.on_error(
            persistent_subscription->listener.clientd,
            ETIMEDOUT,
            "No image became available on the live subscription within the message timeout.");
    }
}

static void aeron_archive_persistent_subscription_on_archive_connected(void *clientd)
{
}

static void aeron_archive_persistent_subscription_on_archive_disconnected(void *clientd)
{
    aeron_archive_persistent_subscription_t *persistent_subscription = clientd;

    aeron_archive_persistent_subscription_state_t state = persistent_subscription->state;
    if (AWAIT_ARCHIVE_CONNECTION == state ||
        ATTEMPT_SWITCH == state ||
        LIVE == state ||
        FAILED== state )
    {
        return;
    }

    aeron_image_t *replay_image = persistent_subscription->replay_image;
    if (NULL != replay_image)
    {
        persistent_subscription->position = aeron_image_position(replay_image);
    }

    aeron_archive_persistent_subscription_clean_up_live_subscription(persistent_subscription);
    aeron_archive_persistent_subscription_clean_up_request_publication(persistent_subscription);
    aeron_archive_persistent_subscription_clean_up_replay(persistent_subscription);
    aeron_archive_persistent_subscription_clean_up_replay_subscription(persistent_subscription);

    aeron_archive_persistent_subscription_transition(persistent_subscription, AWAIT_ARCHIVE_CONNECTION);
}

static void aeron_archive_persistent_subscription_on_archive_control_response(
    void *clientd,
    int64_t correlation_id,
    int64_t relevant_id,
    int32_t code,
    const char *error_message)
{
    aeron_archive_persistent_subscription_t *persistent_subscription = clientd;

    if (correlation_id == persistent_subscription->max_recorded_position.op.correlation_id)
    {
        aeron_archive_persistent_subscription_async_archive_op_on_control_response(
            &persistent_subscription->max_recorded_position.op, relevant_id, code, error_message);
    }
    else if (correlation_id == persistent_subscription->list_recording_request.op.correlation_id)
    {
        aeron_archive_persistent_subscription_async_archive_op_on_control_response(
            &persistent_subscription->list_recording_request.op, relevant_id, code, error_message);
    }
    else if (correlation_id == persistent_subscription->replay_request.correlation_id)
    {
        aeron_archive_persistent_subscription_async_archive_op_on_control_response(
            &persistent_subscription->replay_request, relevant_id, code, error_message);
    }
    else if (correlation_id == persistent_subscription->replay_token_request.correlation_id)
    {
        aeron_archive_persistent_subscription_async_archive_op_on_control_response(
            &persistent_subscription->replay_token_request, relevant_id, code, error_message);
    }
}

static void aeron_archive_persistent_subscription_on_archive_recording_descriptor(
    void *clientd,
    aeron_archive_recording_descriptor_t *recording_descriptor)
{
    aeron_archive_persistent_subscription_t *persistent_subscription = clientd;

    aeron_archive_persistent_subscription_list_recording_request_t *list_recording_request = &persistent_subscription->list_recording_request;
    if (recording_descriptor->correlation_id == list_recording_request->op.correlation_id)
    {
        list_recording_request->recording_id = recording_descriptor->recording_id;
        list_recording_request->start_position = recording_descriptor->start_position;
        list_recording_request->stop_position = recording_descriptor->stop_position;
        list_recording_request->term_buffer_length = recording_descriptor->term_buffer_length;
        list_recording_request->stream_id = recording_descriptor->stream_id;

        if (--list_recording_request->remaining == 0)
        {
            list_recording_request->op.response_received = true;
        }
    }
}

static void aeron_archive_persistent_subscription_on_archive_error(void *clientd, int errcode, const char *errmsg)
{
    aeron_archive_persistent_subscription_t *persistent_subscription = clientd;

    if (aeron_archive_async_client_is_closed(persistent_subscription->archive))
    {
        aeron_archive_persistent_subscription_transition(persistent_subscription, FAILED);

        aeron_archive_persistent_subscription_on_terminal_error(
            persistent_subscription,
            errcode,
            errmsg);
    }
    else
    {
        if (NULL != persistent_subscription->listener.on_error)
        {
            persistent_subscription->listener.on_error(
                persistent_subscription->listener.clientd,
                errcode,
                errmsg);
        }
    }
}

static void aeron_archive_persistent_subscription_set_join_difference(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    int64_t join_difference)
{
    persistent_subscription->join_difference = join_difference;

    aeron_counter_t *join_difference_counter = persistent_subscription->context->join_difference_counter;
    if (!aeron_counter_is_closed(join_difference_counter))
    {
        aeron_counter_set_release(aeron_counter_addr(join_difference_counter), join_difference);
    }
}

static void aeron_archive_persistent_subscription_on_live_joined(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    aeron_counter_t *live_joined_counter = persistent_subscription->context->live_joined_counter;
    if (!aeron_counter_is_closed(live_joined_counter))
    {
        aeron_counter_increment_release(aeron_counter_addr(live_joined_counter));
    }

    aeron_archive_persistent_subscription_listener_t *listener = &persistent_subscription->listener;
    if (NULL != listener->on_live_joined)
    {
        listener->on_live_joined(listener->clientd);
    }
}

static void aeron_archive_persistent_subscription_on_live_left(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    aeron_counter_t *live_left_counter = persistent_subscription->context->live_left_counter;
    if (!aeron_counter_is_closed(live_left_counter))
    {
        aeron_counter_increment_release(aeron_counter_addr(live_left_counter));
    }

    aeron_archive_persistent_subscription_listener_t *listener = &persistent_subscription->listener;
    if (NULL != listener->on_live_left)
    {
        listener->on_live_left(listener->clientd);
    }
}

int aeron_archive_persistent_subscription_create(
    aeron_archive_persistent_subscription_t **persistent_subscription,
    aeron_archive_persistent_subscription_context_t *context)
{
    if (NULL == persistent_subscription || NULL == context)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, persistent_subscription: %s, context: %s",
            AERON_NULL_STR(persistent_subscription),
            AERON_NULL_STR(context));
        return -1;
    }

    if (aeron_archive_persistent_subscription_context_conclude(context) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    aeron_archive_persistent_subscription_t *_persistent_subscription;

    if (aeron_alloc((void **)&_persistent_subscription, sizeof(aeron_archive_persistent_subscription_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to allocate aeron_archive_persistent_subscription_t");
        return -1;
    }

    if (aeron_image_controlled_fragment_assembler_create(&_persistent_subscription->assembler, NULL, NULL) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to create controlled fragment assembler");
        goto error;
    }

    if (aeron_image_fragment_assembler_create(&_persistent_subscription->uncontrolled_assembler, NULL, NULL) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to create fragment assembler");
        goto error;
    }

    _persistent_subscription->archive_listener.clientd = _persistent_subscription;
    _persistent_subscription->archive_listener.on_connected = aeron_archive_persistent_subscription_on_archive_connected;
    _persistent_subscription->archive_listener.on_disconnected = aeron_archive_persistent_subscription_on_archive_disconnected;
    _persistent_subscription->archive_listener.on_control_response = aeron_archive_persistent_subscription_on_archive_control_response;
    _persistent_subscription->archive_listener.on_recording_descriptor = aeron_archive_persistent_subscription_on_archive_recording_descriptor;
    _persistent_subscription->archive_listener.on_error = aeron_archive_persistent_subscription_on_archive_error;

    aeron_archive_context_set_aeron(context->archive_context, context->aeron);

    if (aeron_archive_async_client_create(
        &_persistent_subscription->archive,
        context->archive_context,
        &_persistent_subscription->archive_listener) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to create async archive client");
        goto error;
    }

    _persistent_subscription->context = context;
    _persistent_subscription->listener = context->listener;
    _persistent_subscription->message_timeout_ns = aeron_archive_context_get_message_timeout_ns(context->archive_context);
    _persistent_subscription->replay_session_id = AERON_NULL_VALUE;
    _persistent_subscription->position = context->start_position;
    _persistent_subscription->use_aeron_agent_invoker = aeron_context_get_use_conductor_agent_invoker(aeron_context(context->aeron));
    _persistent_subscription->last_observed_live_position = AERON_NULL_VALUE;

    // Determine replay channel type and copy the URI
    {
        aeron_uri_string_builder_t builder;
        if (aeron_uri_string_builder_init_on_string(&builder, context->replay_channel) < 0)
        {
            AERON_APPEND_ERR("%s", "Failed to parse replay_channel URI");
            goto error;
        }

        const char *media = aeron_uri_string_builder_get(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY);
        const char *endpoint = aeron_uri_string_builder_get(&builder, AERON_UDP_CHANNEL_ENDPOINT_KEY);
        const char *control_mode = aeron_uri_string_builder_get(&builder, AERON_UDP_CHANNEL_CONTROL_MODE_KEY);

        _persistent_subscription->replay_channel_is_ipc = NULL != media && strcmp(media, "ipc") == 0;

        if (NULL != control_mode && strcmp(control_mode, AERON_UDP_CHANNEL_CONTROL_MODE_RESPONSE_VALUE) == 0)
        {
            _persistent_subscription->replay_channel_type = REPLAY_CHANNEL_RESPONSE_CHANNEL;
        }
        else if (NULL != media && strcmp(media, "udp") == 0 &&
            NULL != endpoint && strlen(endpoint) >= 2 &&
            strcmp(endpoint + strlen(endpoint) - 2, ":0") == 0)
        {
            _persistent_subscription->replay_channel_type = REPLAY_CHANNEL_DYNAMIC_PORT;
        }
        else
        {
            _persistent_subscription->replay_channel_type = REPLAY_CHANNEL_SESSION_SPECIFIC;
        }

        aeron_uri_string_builder_close(&builder);
    }

    strncpy(_persistent_subscription->replay_channel_uri, context->replay_channel, AERON_URI_MAX_LENGTH - 1);
    _persistent_subscription->replay_channel_uri[AERON_URI_MAX_LENGTH - 1] = '\0';

    aeron_archive_persistent_subscription_set_join_difference(_persistent_subscription, INT64_MIN);

    aeron_archive_persistent_subscription_transition(_persistent_subscription, AWAIT_ARCHIVE_CONNECTION);

    *persistent_subscription = _persistent_subscription;

    return 0;

error:
    aeron_image_fragment_assembler_delete(_persistent_subscription->uncontrolled_assembler);
    aeron_image_controlled_fragment_assembler_delete(_persistent_subscription->assembler);
    aeron_free(_persistent_subscription);

    return -1;
}

int aeron_archive_persistent_subscription_close(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    if (NULL != persistent_subscription)
    {
        aeron_archive_persistent_subscription_clean_up_live_subscription(persistent_subscription);
        aeron_archive_persistent_subscription_clean_up_request_publication(persistent_subscription);
        aeron_archive_persistent_subscription_clean_up_replay(persistent_subscription);
        aeron_archive_persistent_subscription_clean_up_replay_subscription(persistent_subscription);
        aeron_archive_async_client_destroy(persistent_subscription->archive);
        aeron_archive_persistent_subscription_context_close(persistent_subscription->context);
        aeron_image_fragment_assembler_delete(persistent_subscription->uncontrolled_assembler);
        aeron_image_controlled_fragment_assembler_delete(persistent_subscription->assembler);
        aeron_free(persistent_subscription);
    }

    return 0;
}

static int aeron_archive_persistent_subscription_await_archive_connection(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    bool connected = aeron_archive_async_client_is_connected(persistent_subscription->archive);
    if (!connected)
    {
        return 0;
    }

    aeron_archive_persistent_subscription_transition(persistent_subscription, SEND_LIST_RECORDING_REQUEST);
    return 1;
}

static int aeron_archive_persistent_subscription_send_list_recording_request(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    int64_t correlation_id = aeron_next_correlation_id(persistent_subscription->context->aeron);

    if (!aeron_archive_async_client_try_send_list_recording_request(
        persistent_subscription->archive,
        correlation_id,
        persistent_subscription->context->recording_id))
    {
        if (aeron_archive_async_client_is_connected(persistent_subscription->archive))
        {
            return 0;
        }

        aeron_archive_persistent_subscription_transition(persistent_subscription, AWAIT_ARCHIVE_CONNECTION);

        return 1;
    }

    int64_t deadline_ns = aeron_nano_clock() + persistent_subscription->message_timeout_ns;
    aeron_archive_persistent_subscription_async_archive_op_init(&persistent_subscription->list_recording_request.op, correlation_id, deadline_ns);
    persistent_subscription->list_recording_request.remaining = 1;

    aeron_archive_persistent_subscription_transition(persistent_subscription, AWAIT_LIST_RECORDING_RESPONSE);

    return 1;
}

static void aeron_archive_persistent_subscription_reset_replay_catchup_state(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    aeron_archive_persistent_subscription_set_join_difference(persistent_subscription, INT64_MIN);

    aeron_archive_persistent_subscription_max_recorded_position_reset(
        &persistent_subscription->max_recorded_position,
        persistent_subscription->list_recording_request.term_buffer_length >> 2);
}

static void aeron_archive_persistent_subscription_set_up_replay(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    aeron_archive_persistent_subscription_reset_replay_catchup_state(persistent_subscription);

    if (persistent_subscription->replay_channel_type == REPLAY_CHANNEL_SESSION_SPECIFIC)
    {
        aeron_archive_persistent_subscription_transition(persistent_subscription, SEND_REPLAY_REQUEST);
    }
    else
    {
        aeron_archive_persistent_subscription_transition(persistent_subscription, ADD_REPLAY_SUBSCRIPTION);
    }
}

static void aeron_archive_persistent_subscription_refresh_recording_descriptor(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    aeron_archive_persistent_subscription_reset_replay_catchup_state(persistent_subscription);

    aeron_archive_persistent_subscription_transition(persistent_subscription, SEND_LIST_RECORDING_REQUEST);
}

static void aeron_archive_persistent_subscription_advance_last_observed_live_position(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    int64_t live_position)
{
    if (live_position > persistent_subscription->last_observed_live_position)
    {
        persistent_subscription->last_observed_live_position = live_position;
    }
}

static bool aeron_archive_persistent_subscription_validate_descriptor(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    aeron_archive_persistent_subscription_list_recording_request_t *req = &persistent_subscription->list_recording_request;

    if (req->remaining == 0)
    {
        if (persistent_subscription->context->live_stream_id != req->stream_id)
        {
            aeron_archive_persistent_subscription_transition(persistent_subscription, FAILED);

            char message[256];
            snprintf(message, sizeof(message),
                "Requested live stream with ID: %d does not match stream ID: %d for recording: %" PRIi64,
                persistent_subscription->context->live_stream_id,
                req->stream_id,
                persistent_subscription->context->recording_id);

            aeron_archive_persistent_subscription_on_terminal_error(
                persistent_subscription,
                EINVAL,
                message);

            return false;
        }

        if (AERON_NULL_VALUE != req->stop_position &&
            persistent_subscription->last_observed_live_position > req->stop_position)
        {
            aeron_archive_persistent_subscription_transition(persistent_subscription, FAILED);

            if (NULL != persistent_subscription->listener.on_error)
            {
                char message[256];
                snprintf(message, sizeof(message),
                    "recording %" PRIi64 " stopped at position=%" PRIi64
                    " which is earlier than last observed live position=%" PRIi64,
                    persistent_subscription->context->recording_id,
                    req->stop_position,
                    persistent_subscription->last_observed_live_position);

                persistent_subscription->listener.on_error(
                    persistent_subscription->listener.clientd,
                    EINVAL,
                    message);
            }

            return false;
        }

        if (persistent_subscription->position >= 0)
        {
            if (persistent_subscription->position < req->start_position)
            {
                aeron_archive_persistent_subscription_transition(persistent_subscription, FAILED);

                char message[256];
                snprintf(message, sizeof(message),
                    "requested replay start position=%" PRIi64 " is less than recording start position=%" PRIi64 " for recording %" PRIi64,
                    persistent_subscription->position,
                    req->start_position,
                    persistent_subscription->context->recording_id);

                aeron_archive_persistent_subscription_on_terminal_error(
                    persistent_subscription,
                    EINVAL,
                    message);

                return false;
            }

            if (AERON_NULL_VALUE != req->stop_position && persistent_subscription->position > req->stop_position)
            {
                aeron_archive_persistent_subscription_transition(persistent_subscription, FAILED);

                char message[256];
                snprintf(message, sizeof(message),
                    "requested replay start position=%" PRIi64 " must be less than the limit position=%" PRIi64 " for recording %" PRIi64,
                    persistent_subscription->position,
                    req->stop_position,
                    persistent_subscription->context->recording_id);

                aeron_archive_persistent_subscription_on_terminal_error(
                    persistent_subscription,
                    EINVAL,
                    message);

                return false;
            }
        }
        else if (AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_START == persistent_subscription->position)
        {
            persistent_subscription->position = req->start_position;
        }
    }
    else
    {
        aeron_archive_persistent_subscription_transition(persistent_subscription, FAILED);

        char message[64];
        snprintf(message, sizeof(message),
            "unknown recording id: %" PRIi64,
            persistent_subscription->context->recording_id);

        aeron_archive_persistent_subscription_on_terminal_error(
            persistent_subscription,
            EINVAL,
            message);

        return false;
    }

    return true;
}

static int aeron_archive_persistent_subscription_await_list_recording_response(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    if (!persistent_subscription->list_recording_request.op.response_received)
    {
        if (aeron_nano_clock() - persistent_subscription->list_recording_request.op.deadline_ns >= 0)
        {
            if (aeron_archive_async_client_is_connected(persistent_subscription->archive))
            {
                aeron_archive_persistent_subscription_transition(persistent_subscription, SEND_LIST_RECORDING_REQUEST);
            }
            else
            {
                aeron_archive_persistent_subscription_transition(persistent_subscription, AWAIT_ARCHIVE_CONNECTION);
            }

            return 1;
        }

        return 0;
    }

    if (!aeron_archive_persistent_subscription_validate_descriptor(persistent_subscription))
    {
        return 1;
    }

    if (AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE == persistent_subscription->position ||
        (AERON_NULL_VALUE != persistent_subscription->list_recording_request.stop_position &&
         persistent_subscription->position == persistent_subscription->list_recording_request.stop_position))
    {
        aeron_archive_persistent_subscription_transition(persistent_subscription, ADD_LIVE_SUBSCRIPTION);
    }
    else
    {
        aeron_archive_persistent_subscription_set_up_replay(persistent_subscription);
    }

    return 1;
}

static int aeron_archive_persistent_subscription_send_replay_request(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    int64_t correlation_id = aeron_next_correlation_id(persistent_subscription->context->aeron);

    aeron_archive_replay_params_t params;
    aeron_archive_replay_params_init(&params);
    params.position = persistent_subscription->position;
    params.length = ARCHIVE_REPLAY_ALL_AND_FOLLOW;

    aeron_archive_proxy_t *archive_proxy = NULL;
    if (persistent_subscription->replay_channel_type == REPLAY_CHANNEL_RESPONSE_CHANNEL)
    {
        archive_proxy = persistent_subscription->response_channel_archive_proxy;
        params.replay_token = persistent_subscription->replay_token;
    }

    const char *channel = persistent_subscription->replay_channel_type == REPLAY_CHANNEL_SESSION_SPECIFIC ?
        persistent_subscription->context->replay_channel : persistent_subscription->replay_channel_uri;

    if (!aeron_archive_async_client_try_send_replay_request(
        persistent_subscription->archive,
        archive_proxy,
        correlation_id,
        persistent_subscription->context->recording_id,
        channel,
        persistent_subscription->context->replay_stream_id,
        &params))
    {
        if (aeron_archive_async_client_is_connected(persistent_subscription->archive))
        {
            return 0;
        }

        aeron_archive_persistent_subscription_clean_up_request_publication(persistent_subscription);
        aeron_archive_persistent_subscription_clean_up_replay_subscription(persistent_subscription);
        aeron_archive_persistent_subscription_transition(persistent_subscription, AWAIT_ARCHIVE_CONNECTION);

        return 1;
    }

    int64_t deadline_ns = aeron_nano_clock() + persistent_subscription->message_timeout_ns;
    aeron_archive_persistent_subscription_async_archive_op_init(&persistent_subscription->replay_request, correlation_id, deadline_ns);

    aeron_archive_persistent_subscription_transition(persistent_subscription, AWAIT_REPLAY_RESPONSE);

    return 1;
}

static int aeron_archive_persistent_subscription_await_replay_response(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    if (!persistent_subscription->replay_request.response_received)
    {
        if (aeron_nano_clock() - persistent_subscription->replay_request.deadline_ns >= 0)
        {
            aeron_archive_persistent_subscription_clean_up_request_publication(persistent_subscription);
            aeron_archive_persistent_subscription_clean_up_replay_subscription(persistent_subscription);

            if (aeron_archive_async_client_is_connected(persistent_subscription->archive))
            {
                aeron_archive_persistent_subscription_set_up_replay(persistent_subscription);
            }
            else
            {
                aeron_archive_persistent_subscription_transition(persistent_subscription, AWAIT_ARCHIVE_CONNECTION);
            }

            return 1;
        }

        return 0;
    }

    if (persistent_subscription->replay_request.code != aeron_archive_client_controlResponseCode_OK)
    {
        aeron_archive_persistent_subscription_transition(persistent_subscription, FAILED);

        aeron_archive_persistent_subscription_clean_up_request_publication(persistent_subscription);
        aeron_archive_persistent_subscription_clean_up_replay_subscription(persistent_subscription);

        char message[AERON_ERROR_MAX_TOTAL_LENGTH + 23];
        snprintf(message, sizeof(message), "Replay request failed: %s",
            persistent_subscription->replay_request.error_message);

        aeron_archive_persistent_subscription_on_terminal_error(
            persistent_subscription,
            (int)persistent_subscription->replay_request.relevant_id,
            message);

        return 1;
    }

    persistent_subscription->replay_session_id = persistent_subscription->replay_request.relevant_id;

    if (persistent_subscription->replay_channel_type == REPLAY_CHANNEL_SESSION_SPECIFIC)
    {
        aeron_uri_string_builder_t builder;
        if (aeron_uri_string_builder_init_on_string(&builder, persistent_subscription->replay_channel_uri) < 0)
        {
            AERON_APPEND_ERR("%s", "Failed to parse replay_channel_uri");
            aeron_archive_persistent_subscription_transition(persistent_subscription, FAILED);
            return 1;
        }

        aeron_uri_string_builder_put_int32(
            &builder,
            AERON_URI_SESSION_ID_KEY,
            (int32_t)persistent_subscription->replay_session_id);

        aeron_uri_string_builder_sprint(
            &builder,
            persistent_subscription->replay_channel_uri,
            sizeof(persistent_subscription->replay_channel_uri));

        aeron_uri_string_builder_close(&builder);

        aeron_archive_persistent_subscription_transition(persistent_subscription, ADD_REPLAY_SUBSCRIPTION);
    }
    else
    {
        aeron_archive_persistent_subscription_clean_up_request_publication(persistent_subscription);

        persistent_subscription->replay_image_deadline_ns =
            aeron_nano_clock() + persistent_subscription->message_timeout_ns;

        aeron_archive_persistent_subscription_transition(persistent_subscription, REPLAY);
    }

    return 1;
}

static int aeron_archive_persistent_subscription_add_replay_subscription(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    // Dynamic port: use the raw context channel (with :0) so the OS assigns a free port.
    // Session-specific: use replay_channel_uri which now has the session id injected.
    const char *channel = REPLAY_CHANNEL_SESSION_SPECIFIC == persistent_subscription->replay_channel_type
        ? persistent_subscription->replay_channel_uri
        : persistent_subscription->context->replay_channel;

    if (aeron_async_add_subscription(
        &persistent_subscription->add_replay_subscription,
        persistent_subscription->context->aeron,
        channel,
        persistent_subscription->context->replay_stream_id,
        NULL,
        NULL,
        NULL,
        NULL) < 0)
    {
        aeron_archive_persistent_subscription_transition(persistent_subscription, FAILED);

        aeron_archive_persistent_subscription_on_terminal_error(
            persistent_subscription,
            aeron_archive_persistent_subscription_errcode(),
            aeron_errmsg());

        return 1;
    }

    aeron_archive_persistent_subscription_transition(persistent_subscription, AWAIT_REPLAY_SUBSCRIPTION);

    return 1;
}

static int aeron_archive_persistent_subscription_await_replay_subscription(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    if (aeron_async_add_subscription_poll(
        &persistent_subscription->replay_subscription,
        persistent_subscription->add_replay_subscription) < 0)
    {
        int errcode = aeron_archive_persistent_subscription_errcode();
        const char *errmsg = aeron_errmsg();
        persistent_subscription->add_replay_subscription = NULL;
        aeron_archive_persistent_subscription_clean_up_replay(persistent_subscription);

        if (AERON_ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE == errcode)
        {

            aeron_archive_persistent_subscription_set_up_replay(persistent_subscription);

            if (NULL != persistent_subscription->listener.on_error)
            {
                persistent_subscription->listener.on_error(
                    persistent_subscription->listener.clientd,
                    errcode,
                    errmsg);
            }
        }
        else
        {
            aeron_archive_persistent_subscription_transition(persistent_subscription, FAILED);

            aeron_archive_persistent_subscription_on_terminal_error(
                persistent_subscription,
                errcode,
                errmsg);
        }

        return 1;
    }

    if (NULL == persistent_subscription->replay_subscription)
    {
        return 0;
    }

    persistent_subscription->add_replay_subscription = NULL;
    if (REPLAY_CHANNEL_SESSION_SPECIFIC == persistent_subscription->replay_channel_type)
    {
        persistent_subscription->replay_image_deadline_ns =
            aeron_nano_clock() + persistent_subscription->message_timeout_ns;
    }

    if (REPLAY_CHANNEL_SESSION_SPECIFIC == persistent_subscription->replay_channel_type)
    {
        aeron_archive_persistent_subscription_transition(persistent_subscription, REPLAY);
    }
    else if (REPLAY_CHANNEL_DYNAMIC_PORT == persistent_subscription->replay_channel_type)
    {
        aeron_archive_persistent_subscription_transition(persistent_subscription, AWAIT_REPLAY_CHANNEL_ENDPOINT);
    }
    else
    {
        aeron_archive_persistent_subscription_transition(persistent_subscription, ADD_REQUEST_PUBLICATION);
    }

    return 1;
}

static int aeron_archive_persistent_subscription_await_replay_channel_endpoint(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    if (aeron_subscription_try_resolve_channel_endpoint_port(
        persistent_subscription->replay_subscription,
        persistent_subscription->replay_channel_uri,
        sizeof(persistent_subscription->replay_channel_uri)) <= 0)
    {
        // Not yet resolved, try again next poll
        return 0;
    }

    aeron_archive_persistent_subscription_transition(persistent_subscription, SEND_REPLAY_REQUEST);

    return 1;
}

static int aeron_archive_persistent_subscription_add_request_publication(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    char *control_request_channel = persistent_subscription->context->archive_context->control_request_channel;
    int32_t stream_id = persistent_subscription->context->archive_context->control_request_stream_id;
    int32_t term_buffer_length = (int32_t)persistent_subscription->context->archive_context->control_term_buffer_length;
    int64_t subscription_id = persistent_subscription->replay_subscription->registration_id;

    aeron_uri_string_builder_t builder;
    char channel[AERON_URI_MAX_LENGTH];

    bool failed = aeron_uri_string_builder_init_on_string(&builder, control_request_channel) < 0 ||
        aeron_uri_string_builder_put(&builder, AERON_URI_SESSION_ID_KEY, NULL) < 0 ||
        aeron_uri_string_builder_put_int64(&builder, AERON_URI_RESPONSE_CORRELATION_ID_KEY, subscription_id) < 0 ||
        aeron_uri_string_builder_put(&builder, AERON_URI_INITIAL_TERM_ID_KEY, NULL) < 0 ||
        aeron_uri_string_builder_put(&builder, AERON_URI_TERM_ID_KEY, NULL) < 0 ||
        aeron_uri_string_builder_put(&builder, AERON_URI_TERM_OFFSET_KEY, NULL) < 0 ||
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_TERM_LENGTH_KEY, term_buffer_length) < 0 ||
        aeron_uri_string_builder_put(&builder, AERON_URI_SPIES_SIMULATE_CONNECTION_KEY, "false") < 0 ||
        aeron_uri_string_builder_sprint(&builder, channel, sizeof(channel)) < 0;

    aeron_uri_string_builder_close(&builder);

    if (failed)
    {
        AERON_APPEND_ERR("%s", "failed to build request publication channel");
        aeron_archive_persistent_subscription_transition(persistent_subscription, FAILED);

        aeron_archive_persistent_subscription_on_terminal_error(
            persistent_subscription,
            aeron_archive_persistent_subscription_errcode(),
            aeron_errmsg());

        return 1;
    }

    if (aeron_async_add_exclusive_publication(
        &persistent_subscription->add_request_publication,
        persistent_subscription->context->aeron,
        channel,
        stream_id) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to add request publication");
        aeron_archive_persistent_subscription_transition(persistent_subscription, FAILED);

        aeron_archive_persistent_subscription_on_terminal_error(
            persistent_subscription,
            aeron_archive_persistent_subscription_errcode(),
            aeron_errmsg());

        return 1;
    }

    aeron_archive_persistent_subscription_transition(persistent_subscription, AWAIT_REQUEST_PUBLICATION);
    return 1;
}

static int aeron_archive_persistent_subscription_await_request_publication(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    int result = aeron_async_add_exclusive_publication_poll(
        &persistent_subscription->request_publication,
        persistent_subscription->add_request_publication);

    if (0 == result)
    {
        return 0;
    }

    persistent_subscription->add_request_publication = NULL;

    if (result < 0)
    {
        int errcode = aeron_archive_persistent_subscription_errcode();
        const char *errmsg = aeron_errmsg();

        aeron_archive_persistent_subscription_clean_up_replay_subscription(persistent_subscription);

        if (AERON_ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE == errcode)
        {
            aeron_archive_persistent_subscription_set_up_replay(persistent_subscription);

            if (NULL != persistent_subscription->listener.on_error)
            {
                persistent_subscription->listener.on_error(
                    persistent_subscription->listener.clientd,
                    errcode,
                    errmsg);
            }
        }
        else
        {
            aeron_archive_persistent_subscription_transition(persistent_subscription, FAILED);

            aeron_archive_persistent_subscription_on_terminal_error(
                persistent_subscription,
                errcode,
                errmsg);
        }

        return 1;
    }

    if (aeron_archive_proxy_create(
        &persistent_subscription->response_channel_archive_proxy,
        persistent_subscription->context->archive_context,
        persistent_subscription->request_publication,
        AERON_ARCHIVE_MESSAGE_RETRY_ATTEMPTS_DEFAULT) < 0)
    {

        aeron_archive_persistent_subscription_transition(persistent_subscription, FAILED);

        aeron_archive_persistent_subscription_on_terminal_error(
            persistent_subscription,
            aeron_archive_persistent_subscription_errcode(),
            aeron_errmsg());

        return 1;
    }

    aeron_archive_proxy_set_control_session_id(
        persistent_subscription->response_channel_archive_proxy,
        aeron_archive_async_client_get_control_session_id(persistent_subscription->archive));

    aeron_archive_persistent_subscription_transition(persistent_subscription, SEND_REPLAY_TOKEN_REQUEST);
    return 1;
}

static int aeron_archive_persistent_subscription_send_replay_token_request(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    int64_t correlation_id = aeron_next_correlation_id(persistent_subscription->context->aeron);

    if (!aeron_archive_async_client_try_send_replay_token_request(
        persistent_subscription->archive,
        correlation_id,
        persistent_subscription->context->recording_id))
    {
        if (aeron_archive_async_client_is_connected(persistent_subscription->archive))
        {
            return 0;
        }

        aeron_archive_persistent_subscription_clean_up_request_publication(persistent_subscription);
        aeron_archive_persistent_subscription_clean_up_replay_subscription(persistent_subscription);

        aeron_archive_persistent_subscription_transition(persistent_subscription, AWAIT_ARCHIVE_CONNECTION);

        return 1;
    }

    int64_t deadline_ns = aeron_nano_clock() + persistent_subscription->message_timeout_ns;
    aeron_archive_persistent_subscription_async_archive_op_init(&persistent_subscription->replay_token_request, correlation_id, deadline_ns);

    aeron_archive_persistent_subscription_transition(persistent_subscription, AWAIT_REPLAY_TOKEN);

    return 1;
}

static int aeron_archive_persistent_subscription_await_replay_token(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    if (!persistent_subscription->replay_token_request.response_received)
    {
        if (aeron_nano_clock() - persistent_subscription->replay_token_request.deadline_ns >= 0)
        {
            aeron_archive_persistent_subscription_clean_up_request_publication(persistent_subscription);
            aeron_archive_persistent_subscription_clean_up_replay_subscription(persistent_subscription);

            if (aeron_archive_async_client_is_connected(persistent_subscription->archive))
            {
                aeron_archive_persistent_subscription_set_up_replay(persistent_subscription);
            }
            else
            {
                aeron_archive_persistent_subscription_transition(persistent_subscription, AWAIT_ARCHIVE_CONNECTION);
            }

            return 1;
        }

        return 0;
    }

    if (aeron_archive_client_controlResponseCode_OK != persistent_subscription->replay_token_request.code)
    {
        aeron_archive_persistent_subscription_transition(persistent_subscription, FAILED);

        aeron_archive_persistent_subscription_clean_up_request_publication(persistent_subscription);
        aeron_archive_persistent_subscription_clean_up_replay_subscription(persistent_subscription);

        char message[AERON_ERROR_MAX_TOTAL_LENGTH + 29];
        snprintf(message, sizeof(message), "Replay token request failed: %s",
            persistent_subscription->replay_token_request.error_message);

        aeron_archive_persistent_subscription_on_terminal_error(
            persistent_subscription,
            (int)persistent_subscription->replay_token_request.relevant_id,
            message);

        return 1;
    }

    persistent_subscription->replay_token = persistent_subscription->replay_token_request.relevant_id;

    if (persistent_subscription->replay_channel_is_ipc)
    {
        aeron_archive_persistent_subscription_transition(persistent_subscription, SEND_REPLAY_REQUEST);
    }
    else
    {
        aeron_archive_persistent_subscription_transition(persistent_subscription, AWAIT_REPLAY_CHANNEL_ENDPOINT);
    }

    return 1;
}

static bool aeron_archive_persistent_subscription_do_add_live_subscription(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    persistent_subscription->live_image = NULL;
    persistent_subscription->live_subscription = NULL;
    persistent_subscription->add_live_subscription = NULL;

    if (aeron_async_add_subscription(
        &persistent_subscription->add_live_subscription,
        persistent_subscription->context->aeron,
        persistent_subscription->context->live_channel,
        persistent_subscription->context->live_stream_id,
        NULL,
        NULL,
        NULL,
        NULL) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to add live subscription");
        aeron_archive_persistent_subscription_transition(persistent_subscription, FAILED);

        aeron_archive_persistent_subscription_on_terminal_error(
            persistent_subscription,
            aeron_archive_persistent_subscription_errcode(),
            aeron_errmsg());

        return false;
    }

    return true;
}

static int aeron_archive_persistent_subscription_do_poll(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    aeron_image_t *image,
    aeron_archive_persistent_subscription_poll_ctx_t *poll_ctx)
{
    if (poll_ctx->controlled)
    {
        persistent_subscription->assembler->delegate = poll_ctx->handler.controlled;
        persistent_subscription->assembler->delegate_clientd = poll_ctx->clientd;
        return aeron_image_controlled_poll(
            image,
            aeron_image_controlled_fragment_assembler_handler,
            persistent_subscription->assembler,
            poll_ctx->fragment_limit);
    }
    else
    {
        persistent_subscription->uncontrolled_assembler->delegate = poll_ctx->handler.uncontrolled;
        persistent_subscription->uncontrolled_assembler->delegate_clientd = poll_ctx->clientd;
        return aeron_image_poll(
            image,
            aeron_image_fragment_assembler_handler,
            persistent_subscription->uncontrolled_assembler,
            poll_ctx->fragment_limit);
    }
}

static int aeron_archive_persistent_subscription_replay(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    aeron_archive_persistent_subscription_poll_ctx_t *poll_ctx)
{
    aeron_image_t *image = persistent_subscription->replay_image;

    if (NULL == image)
    {
        image = aeron_subscription_image_by_session_id(
            persistent_subscription->replay_subscription,
            (int32_t)persistent_subscription->replay_session_id);

        if (NULL == image)
        {
            if (aeron_nano_clock() - persistent_subscription->replay_image_deadline_ns >= 0)
            {
                aeron_archive_persistent_subscription_clean_up_replay(persistent_subscription);
                aeron_archive_persistent_subscription_clean_up_replay_subscription(persistent_subscription);
                aeron_archive_persistent_subscription_set_up_replay(persistent_subscription);

                return 1;
            }

            return 0;
        }

        persistent_subscription->replay_image = image;
    }

    if (aeron_image_is_closed(image))
    {
        persistent_subscription->position = aeron_image_position(image);
        aeron_archive_persistent_subscription_clean_up_live_subscription(persistent_subscription);
        aeron_archive_persistent_subscription_clean_up_replay(persistent_subscription);
        aeron_archive_persistent_subscription_clean_up_replay_subscription(persistent_subscription);
        aeron_archive_persistent_subscription_set_up_replay(persistent_subscription);

        return 1;
    }

    if (NULL == persistent_subscription->live_subscription && NULL != persistent_subscription->add_live_subscription)
    {
        if (aeron_async_add_subscription_poll(
            &persistent_subscription->live_subscription,
            persistent_subscription->add_live_subscription) < 0)
        {
            int errcode = aeron_archive_persistent_subscription_errcode();
            const char *errmsg = aeron_errmsg();
            persistent_subscription->add_live_subscription = NULL;

            AERON_APPEND_ERR("%s", "failed to add live subscription");
            if (AERON_ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE != errcode)
            {
                aeron_archive_persistent_subscription_clean_up_replay(persistent_subscription);
                aeron_archive_persistent_subscription_clean_up_replay_subscription(persistent_subscription);
                aeron_archive_persistent_subscription_transition(persistent_subscription, FAILED);

                aeron_archive_persistent_subscription_on_terminal_error(
                    persistent_subscription,
                    errcode,
                    errmsg);
            }
            else
            {
                if (NULL != persistent_subscription->listener.on_error)
                {
                    persistent_subscription->listener.on_error(
                        persistent_subscription->listener.clientd,
                        errcode,
                        errmsg);
                }
            }

            return 1;
        }

        if (NULL != persistent_subscription->live_subscription)
        {
            persistent_subscription->add_live_subscription = NULL;
            aeron_archive_persistent_subscription_set_live_image_deadline(persistent_subscription);
        }
    }

    if (NULL != persistent_subscription->live_subscription)
    {
        if (aeron_subscription_image_count(persistent_subscription->live_subscription) > 0)
        {
            persistent_subscription->live_image = aeron_subscription_image_at_index(
                persistent_subscription->live_subscription,
                0);

            int64_t live_position = aeron_image_position(persistent_subscription->live_image);
            int64_t replay_position = aeron_image_position(image);
            aeron_archive_persistent_subscription_set_join_difference(
                persistent_subscription, live_position - replay_position);

            aeron_archive_persistent_subscription_transition(persistent_subscription, ATTEMPT_SWITCH);

            return 1;
        }
        else if (!persistent_subscription->live_image_deadline_breached &&
                 aeron_nano_clock() - persistent_subscription->live_image_deadline_ns >= 0)
        {
            aeron_archive_persistent_subscription_on_live_image_deadline_breached(persistent_subscription);
        }
    }

    int fragments = aeron_archive_persistent_subscription_do_poll(persistent_subscription, image, poll_ctx);

    persistent_subscription->position = aeron_image_position(image);

    if (NULL == persistent_subscription->add_live_subscription &&
        NULL == persistent_subscription->live_subscription &&
        aeron_archive_persistent_subscription_max_recorded_position_is_caught_up(persistent_subscription, persistent_subscription->position))
    {
        if (!aeron_archive_persistent_subscription_do_add_live_subscription(persistent_subscription))
        {
            aeron_archive_persistent_subscription_clean_up_replay(persistent_subscription);
            aeron_archive_persistent_subscription_clean_up_replay_subscription(persistent_subscription);
            return 1;
        }
    }

    return fragments;
}

static aeron_controlled_fragment_handler_action_t aeron_archive_persistent_subscription_live_catchup_fragment_handler(
    void *clientd,
    const uint8_t *buffer,
    size_t length,
    aeron_header_t *header)
{
    aeron_archive_persistent_subscription_t *persistent_subscription = clientd;
    int64_t current_live_position = aeron_header_position(header);
    int64_t last_replay_position = aeron_image_position(persistent_subscription->replay_image);
    if (current_live_position <= last_replay_position)
    {
        return AERON_ACTION_CONTINUE;
    }
    persistent_subscription->next_live_position = current_live_position;
    return AERON_ACTION_ABORT;
}

static aeron_controlled_fragment_handler_action_t aeron_archive_persistent_subscription_replay_catchup_controlled_fragment_handler(
    void *clientd,
    const uint8_t *buffer,
    size_t length,
    aeron_header_t *header)
{
    aeron_archive_persistent_subscription_t *persistent_subscription = clientd;
    int64_t current_replay_position = aeron_header_position(header);
    if (current_replay_position == persistent_subscription->next_live_position)
    {
        aeron_archive_persistent_subscription_transition(persistent_subscription, LIVE);
        return AERON_ACTION_ABORT;
    }
    return aeron_image_controlled_fragment_assembler_handler(
        persistent_subscription->assembler,
        buffer,
        length,
        header);
}

static aeron_controlled_fragment_handler_action_t aeron_archive_persistent_subscription_replay_catchup_uncontrolled_fragment_handler(
    void *clientd,
    const uint8_t *buffer,
    size_t length,
    aeron_header_t *header)
{
    aeron_archive_persistent_subscription_t *persistent_subscription = clientd;
    int64_t current_replay_position = aeron_header_position(header);
    if (current_replay_position == persistent_subscription->next_live_position)
    {
        aeron_archive_persistent_subscription_transition(persistent_subscription, LIVE);
        return AERON_ACTION_ABORT;
    }
    aeron_image_fragment_assembler_handler(
        persistent_subscription->uncontrolled_assembler,
        buffer,
        length,
        header);
    return AERON_ACTION_CONTINUE;
}

static int aeron_archive_persistent_subscription_attempt_switch(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    aeron_archive_persistent_subscription_poll_ctx_t *poll_ctx)
{
    int fragments = 0;

    aeron_image_t *live_image = persistent_subscription->live_image;
    aeron_image_t *replay_image = persistent_subscription->replay_image;

    int64_t live_position = aeron_image_position(live_image);
    int64_t replay_position = aeron_image_position(replay_image);

    if (replay_position == live_position)
    {
        aeron_archive_persistent_subscription_transition(persistent_subscription, LIVE);
    }
    else
    {
        if (aeron_image_is_closed(replay_image))
        {
            persistent_subscription->position = replay_position;
            aeron_archive_persistent_subscription_advance_last_observed_live_position(persistent_subscription, live_position);
            aeron_archive_persistent_subscription_clean_up_live_subscription(persistent_subscription);
            aeron_archive_persistent_subscription_clean_up_replay(persistent_subscription);
            aeron_archive_persistent_subscription_clean_up_replay_subscription(persistent_subscription);
            aeron_archive_persistent_subscription_refresh_recording_descriptor(persistent_subscription);

            return 1;
        }

        if (aeron_image_is_closed(live_image))
        {
            aeron_archive_persistent_subscription_clean_up_live_subscription(persistent_subscription);
            aeron_archive_persistent_subscription_reset_replay_catchup_state(persistent_subscription);
            aeron_archive_persistent_subscription_transition(persistent_subscription, REPLAY);

            return 1;
        }

        fragments += aeron_image_controlled_poll(
            live_image,
            aeron_archive_persistent_subscription_live_catchup_fragment_handler,
            persistent_subscription,
            poll_ctx->fragment_limit);

        if (poll_ctx->controlled)
        {
            persistent_subscription->assembler->delegate = poll_ctx->handler.controlled;
            persistent_subscription->assembler->delegate_clientd = poll_ctx->clientd;
        }
        else
        {
            persistent_subscription->uncontrolled_assembler->delegate = poll_ctx->handler.uncontrolled;
            persistent_subscription->uncontrolled_assembler->delegate_clientd = poll_ctx->clientd;
        }

        fragments += aeron_image_controlled_poll(
            replay_image,
            poll_ctx->controlled
            ? aeron_archive_persistent_subscription_replay_catchup_controlled_fragment_handler
            : aeron_archive_persistent_subscription_replay_catchup_uncontrolled_fragment_handler,
            persistent_subscription,
            poll_ctx->fragment_limit);
    }

    if (aeron_archive_persistent_subscription_is_live(persistent_subscription))
    {
        aeron_archive_persistent_subscription_clean_up_replay(persistent_subscription);
        aeron_archive_persistent_subscription_clean_up_replay_subscription(persistent_subscription);
        aeron_archive_persistent_subscription_on_live_joined(persistent_subscription);
    }

    return fragments;
}

static int aeron_archive_persistent_subscription_add_live_subscription(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    if (aeron_archive_persistent_subscription_do_add_live_subscription(persistent_subscription))
    {
        aeron_archive_persistent_subscription_transition(persistent_subscription, AWAIT_LIVE);
    }

    return 1;
}

static int aeron_archive_persistent_subscription_await_live(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    if (NULL == persistent_subscription->live_subscription)
    {
        if (aeron_async_add_subscription_poll(
            &persistent_subscription->live_subscription,
            persistent_subscription->add_live_subscription) < 0)
        {
            int errcode = aeron_archive_persistent_subscription_errcode();
            const char *errmsg = aeron_errmsg();

            persistent_subscription->add_live_subscription = NULL;

            if (AERON_ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE == errcode)
            {

                aeron_archive_persistent_subscription_transition(persistent_subscription, ADD_LIVE_SUBSCRIPTION);

                if (NULL != persistent_subscription->listener.on_error)
                {
                    persistent_subscription->listener.on_error(
                        persistent_subscription->listener.clientd,
                        errcode,
                        errmsg);
                }
            }
            else
            {
                aeron_archive_persistent_subscription_transition(persistent_subscription, FAILED);


                aeron_archive_persistent_subscription_on_terminal_error(
                    persistent_subscription,
                    errcode,
                    errmsg);
            }

            return 1;
        }

        if (NULL != persistent_subscription->live_subscription)
        {
            persistent_subscription->add_live_subscription = NULL;
            aeron_archive_persistent_subscription_set_live_image_deadline(persistent_subscription);
        }
    }

    if (NULL != persistent_subscription->live_subscription)
    {
        if (aeron_subscription_image_count(persistent_subscription->live_subscription) > 0)
        {
            aeron_image_t *image = aeron_subscription_image_at_index(persistent_subscription->live_subscription, 0);
            int64_t live_position = aeron_image_position(image);
            aeron_archive_persistent_subscription_advance_last_observed_live_position(persistent_subscription, live_position);

            if (persistent_subscription->position >= 0 && live_position > persistent_subscription->position)
            {
                aeron_archive_persistent_subscription_clean_up_live_subscription(persistent_subscription);
                persistent_subscription->live_image = NULL;
                aeron_archive_persistent_subscription_refresh_recording_descriptor(persistent_subscription);

                return 1;
            }

            persistent_subscription->live_image = image;
            persistent_subscription->position = live_position;
            aeron_archive_persistent_subscription_set_join_difference(persistent_subscription, 0);
            aeron_archive_persistent_subscription_transition(persistent_subscription, LIVE);
            aeron_archive_persistent_subscription_on_live_joined(persistent_subscription);

            return 1;
        }
        else if (!persistent_subscription->live_image_deadline_breached &&
                 aeron_nano_clock() - persistent_subscription->live_image_deadline_ns >= 0)
        {
            aeron_archive_persistent_subscription_on_live_image_deadline_breached(persistent_subscription);
        }
    }

    return 0;
}

static int aeron_archive_persistent_subscription_live(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    aeron_archive_persistent_subscription_poll_ctx_t *poll_ctx)
{
    aeron_image_t *image = persistent_subscription->live_image;

    int fragments = aeron_archive_persistent_subscription_do_poll(persistent_subscription, image, poll_ctx);

    if (fragments == 0 && aeron_image_is_closed(image))
    {
        int64_t final_position = aeron_image_position(image);
        aeron_archive_persistent_subscription_advance_last_observed_live_position(persistent_subscription, final_position);
        persistent_subscription->position = final_position;
        aeron_archive_persistent_subscription_clean_up_live_subscription(persistent_subscription);
        aeron_archive_persistent_subscription_refresh_recording_descriptor(persistent_subscription);
        aeron_archive_persistent_subscription_on_live_left(persistent_subscription);

        return 1;
    }

    return fragments;
}

static int aeron_archive_persistent_subscription_do_work(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    aeron_archive_persistent_subscription_poll_ctx_t *poll_ctx)
{
    int work_count = 0;

    if (persistent_subscription->use_aeron_agent_invoker)
    {
        work_count += aeron_main_do_work(persistent_subscription->context->aeron);
    }

    work_count += aeron_archive_async_client_poll(persistent_subscription->archive);

    switch (persistent_subscription->state)
    {
        case AWAIT_ARCHIVE_CONNECTION:
            work_count += aeron_archive_persistent_subscription_await_archive_connection(persistent_subscription);
            break;
        case SEND_LIST_RECORDING_REQUEST:
            work_count += aeron_archive_persistent_subscription_send_list_recording_request(persistent_subscription);
            break;
        case AWAIT_LIST_RECORDING_RESPONSE:
            work_count += aeron_archive_persistent_subscription_await_list_recording_response(persistent_subscription);
            break;
        case SEND_REPLAY_REQUEST:
            work_count += aeron_archive_persistent_subscription_send_replay_request(persistent_subscription);
            break;
        case AWAIT_REPLAY_RESPONSE:
            work_count += aeron_archive_persistent_subscription_await_replay_response(persistent_subscription);
            break;
        case ADD_REPLAY_SUBSCRIPTION:
            work_count += aeron_archive_persistent_subscription_add_replay_subscription(persistent_subscription);
            break;
        case AWAIT_REPLAY_SUBSCRIPTION:
            work_count += aeron_archive_persistent_subscription_await_replay_subscription(persistent_subscription);
            break;
        case AWAIT_REPLAY_CHANNEL_ENDPOINT:
            work_count += aeron_archive_persistent_subscription_await_replay_channel_endpoint(persistent_subscription);
            break;
        case ADD_REQUEST_PUBLICATION:
            work_count += aeron_archive_persistent_subscription_add_request_publication(persistent_subscription);
            break;
        case AWAIT_REQUEST_PUBLICATION:
            work_count += aeron_archive_persistent_subscription_await_request_publication(persistent_subscription);
            break;
        case SEND_REPLAY_TOKEN_REQUEST:
            work_count += aeron_archive_persistent_subscription_send_replay_token_request(persistent_subscription);
            break;
        case AWAIT_REPLAY_TOKEN:
            work_count += aeron_archive_persistent_subscription_await_replay_token(persistent_subscription);
            break;
        case REPLAY:
            work_count += aeron_archive_persistent_subscription_replay(persistent_subscription, poll_ctx);
            break;
        case ATTEMPT_SWITCH:
            work_count += aeron_archive_persistent_subscription_attempt_switch(persistent_subscription, poll_ctx);
            break;
        case ADD_LIVE_SUBSCRIPTION:
            work_count += aeron_archive_persistent_subscription_add_live_subscription(persistent_subscription);
            break;
        case AWAIT_LIVE:
            work_count += aeron_archive_persistent_subscription_await_live(persistent_subscription);
            break;
        case LIVE:
            work_count += aeron_archive_persistent_subscription_live(persistent_subscription, poll_ctx);
            break;
        case FAILED:
            break;
    }

    return work_count;
}

int aeron_archive_persistent_subscription_poll(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    aeron_fragment_handler_t handler,
    void *clientd,
    size_t fragment_limit)
{
    aeron_archive_persistent_subscription_poll_ctx_t poll_ctx =
        {
            .controlled = false,
            .handler.uncontrolled = handler,
            .clientd = clientd,
            .fragment_limit = fragment_limit,
        };

    return aeron_archive_persistent_subscription_do_work(persistent_subscription, &poll_ctx);
}

int aeron_archive_persistent_subscription_controlled_poll(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    aeron_controlled_fragment_handler_t handler,
    void *clientd,
    size_t fragment_limit)
{
    aeron_archive_persistent_subscription_poll_ctx_t poll_ctx =
        {
            .controlled = true,
            .handler.controlled = handler,
            .clientd = clientd,
            .fragment_limit = fragment_limit,
        };

    return aeron_archive_persistent_subscription_do_work(persistent_subscription, &poll_ctx);
}

bool aeron_archive_persistent_subscription_is_live(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    return LIVE == persistent_subscription->state;
}

bool aeron_archive_persistent_subscription_is_replaying(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    return REPLAY == persistent_subscription->state ||
           ATTEMPT_SWITCH == persistent_subscription->state;
}

bool aeron_archive_persistent_subscription_has_failed(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    return FAILED == persistent_subscription->state;
}

bool aeron_archive_persistent_subscription_failure_reason(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    int *out_errcode,
    const char **out_message)
{
    if (FAILED != persistent_subscription->state)
    {
        return false;
    }

    if (NULL != out_errcode)
    {
        *out_errcode = persistent_subscription->failure_errcode;
    }

    if (NULL != out_message)
    {
        *out_message = persistent_subscription->failure_message;
    }

    return true;
}

int64_t aeron_archive_persistent_subscription_join_difference(
    aeron_archive_persistent_subscription_t *persistent_subscription)
{
    return persistent_subscription->join_difference;
}

void aeron_archive_persistent_subscription_set_message_timeout_ns_for_testing(
    aeron_archive_persistent_subscription_t *persistent_subscription, uint64_t message_timeout_ns)
{
    persistent_subscription->message_timeout_ns = message_timeout_ns;
}
