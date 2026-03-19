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
#include <stdio.h> // TODO remove
#include <c/aeron_archive_client/controlResponseCode.h>

#include "aeron_alloc.h"
#include "aeron_archive_async_client.h"
#include "aeron_archive_persistent_subscription.h"
#include "aeron_fragment_assembler.h"
#include "uri/aeron_uri_string_builder.h"
#include "util/aeron_error.h"

#define transition(persistent_subscription, new_state) \
    do {                                               \
        printf("-> " #new_state "\n");                 \
        fflush(stdout);                                \
        persistent_subscription->state = new_state;    \
    } while (0)

struct aeron_archive_persistent_subscription_context_stct
{
    aeron_t *aeron;
    bool owns_aeron_client;
    char aeron_directory_name[AERON_MAX_PATH];
    aeron_archive_context_t *archive_context;
    int64_t recording_id;
    char *live_channel;
    int32_t live_stream_id;
    char *replay_channel;
    int32_t replay_stream_id;
    int64_t start_position;
    aeron_archive_persistent_subscription_listener_t listener;
};

struct async_archive_op
{
    int64_t correlation_id;
    int64_t deadline_ns;

    int64_t relevant_id;
    int32_t code;

    bool response_received;
};

static void async_archive_op_init(struct async_archive_op *op, int64_t correlation_id, int64_t deadline_ns)
{
    op->correlation_id = correlation_id;
    op->deadline_ns = deadline_ns;
    op->response_received = false;
}

static void async_archive_op_on_control_response(
    struct async_archive_op *op,
    int64_t correlation_id,
    int64_t relevant_id,
    int32_t code)
{
    if (correlation_id == op->correlation_id)
    {
        op->relevant_id = relevant_id;
        op->code = code;
        op->response_received = true;
    }
}

struct list_recording_request
{
    struct async_archive_op op;

    int remaining;

    int64_t recording_id;
    int64_t start_position;
    int64_t stop_position;
    int32_t term_buffer_length;
    int32_t stream_id;
};

enum max_recorded_position_state
{
    REQUEST_MAX_POSITION,
    AWAIT_MAX_POSITION,
    RECHECK_REQUIRED,
};

struct max_recorded_position
{
    struct async_archive_op op;

    enum max_recorded_position_state state;
    int64_t max_recorded_position;
    int32_t close_enough_threshold;
};

static void max_recorded_position_reset(
    struct max_recorded_position *max_recorded_position,
    int32_t close_enough_threshold)
{
    max_recorded_position->state = REQUEST_MAX_POSITION;
    max_recorded_position->close_enough_threshold = close_enough_threshold;
}

typedef enum aeron_archive_replay_channel_type_en
{
    REPLAY_CHANNEL_SESSION_SPECIFIC,
    REPLAY_CHANNEL_DYNAMIC_PORT,
}
aeron_archive_replay_channel_type_t;

typedef enum aeron_archive_persistent_subscription_state_en
{
    AWAIT_ARCHIVE_CONNECTION,
    SEND_LIST_RECORDING_REQUEST,
    AWAIT_LIST_RECORDING_RESPONSE,
    SEND_REPLAY_REQUEST,
    AWAIT_REPLAY_RESPONSE,
    ADD_REPLAY_SUBSCRIPTION,
    AWAIT_REPLAY_SUBSCRIPTION,
    AWAIT_REPLAY_CHANNEL_ENDPOINT,
    REPLAY,
    ATTEMPT_SWITCH,
    ADD_LIVE_SUBSCRIPTION,
    AWAIT_LIVE,
    LIVE,
    FAILED,
}
aeron_archive_persistent_subscription_state_t;

struct aeron_archive_persistent_subscription_stct
{
    aeron_archive_persistent_subscription_context_t *context;
    aeron_archive_persistent_subscription_listener_t listener;
    uint64_t message_timeout_ns;
    aeron_archive_async_client_t *archive;
    aeron_archive_async_client_listener_t archive_listener;
    aeron_archive_persistent_subscription_state_t state;
    aeron_archive_replay_channel_type_t replay_channel_type;
    char replay_channel_uri[AERON_URI_MAX_LENGTH];
    struct list_recording_request list_recording_request;
    struct max_recorded_position max_recorded_position;
    struct async_archive_op replay_request;
    int64_t replay_session_id;
    int64_t replay_image_deadline_ns;
    int64_t join_error;
    aeron_async_add_subscription_t *add_replay_subscription;
    aeron_subscription_t *replay_subscription;
    aeron_image_t *replay_image;
    int64_t live_image_deadline_ns;
    bool live_image_deadline_breached;
    aeron_async_add_subscription_t *add_live_subscription;
    aeron_subscription_t *live_subscription;
    aeron_image_t *live_image;
    aeron_image_controlled_fragment_assembler_t *assembler;
    int64_t next_live_position;
    int64_t position;
};

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

    _context->aeron_directory_name[0] = '\0';
    _context->recording_id = AERON_NULL_VALUE;
    _context->live_stream_id = AERON_NULL_VALUE;
    _context->replay_stream_id = AERON_NULL_VALUE;
    _context->start_position = AERON_PERSISTENT_SUBSCRIPTION_FROM_LIVE;

    *context = _context;

    return 0;
}

int aeron_archive_persistent_subscription_context_close(aeron_archive_persistent_subscription_context_t *context)
{
    if (NULL != context)
    {
        free(context->live_channel);
        free(context->replay_channel);
        if (context->owns_aeron_client)
        {
            aeron_close(context->aeron);
        }
        aeron_free(context);
    }

    return 0;
}

static int set_string(char **ptr, const char *val)
{
    free(*ptr);

    *ptr = strdup(val);
    if (NULL == *ptr)
    {
        AERON_SET_ERR(errno, "%s", "Failed to duplicate string");
        return -1;
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
    strncpy(context->aeron_directory_name, aeron_directory_name, sizeof(context->aeron_directory_name) - 1);
    context->aeron_directory_name[sizeof(context->aeron_directory_name) - 1] = '\0';
    return 0;
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

int aeron_archive_persistent_subscription_context_set_live_channel(
    aeron_archive_persistent_subscription_context_t *context,
    const char *live_channel)
{
    return set_string(&context->live_channel, live_channel);
}

int aeron_archive_persistent_subscription_context_set_live_stream_id(
    aeron_archive_persistent_subscription_context_t *context,
    int32_t live_stream_id)
{
    context->live_stream_id = live_stream_id;

    return 0;
}

int aeron_archive_persistent_subscription_context_set_replay_channel(
    aeron_archive_persistent_subscription_context_t *context,
    const char *replay_channel)
{
    return set_string(&context->replay_channel, replay_channel);
}

int aeron_archive_persistent_subscription_context_set_replay_stream_id(
    aeron_archive_persistent_subscription_context_t *context,
    int32_t replay_stream_id)
{
    context->replay_stream_id = replay_stream_id;

    return 0;
}

int aeron_archive_persistent_subscription_context_set_start_position(
    aeron_archive_persistent_subscription_context_t *context,
    int64_t start_position)
{
    context->start_position = start_position;

    return 0;
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

static int aeron_archive_persistent_subscription_context_conclude(
    aeron_archive_persistent_subscription_context_t *context)
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

    if (AERON_PERSISTENT_SUBSCRIPTION_FROM_LIVE > context->start_position)
    {
        AERON_SET_ERR(EINVAL, "invalid start_position %" PRIi64, context->start_position);
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
        if ('\0' != context->aeron_directory_name[0])
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
        context->owns_aeron_client = true;
    }

    return 0;
}

static bool max_recorded_position_request_max_position(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    int64_t correlation_id = aeron_next_correlation_id(persistent_subscription->context->aeron);

    if (aeron_archive_async_client_try_send_max_recorded_position_request(
        persistent_subscription->archive,
        correlation_id,
        persistent_subscription->context->recording_id))
    {
        int64_t deadline_ns = aeron_nano_clock() + persistent_subscription->message_timeout_ns;
        async_archive_op_init(&persistent_subscription->max_recorded_position.op, correlation_id, deadline_ns);
        persistent_subscription->max_recorded_position.state = AWAIT_MAX_POSITION;
    }

    return false;
}

static bool max_recorded_position_await_max_position(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    int64_t replayed_position)
{
    struct max_recorded_position *max_recorded_position = &persistent_subscription->max_recorded_position;

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
            transition(persistent_subscription, FAILED);

            if (NULL != persistent_subscription->listener.on_error)
            {
                persistent_subscription->listener.on_error(
                    persistent_subscription->listener.clientd,
                    (int)max_recorded_position->op.relevant_id,
                    "get max recorded position request failed");
            }
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

static bool max_recorded_position_recheck_required(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    int64_t replayed_position)
{
    if (replayed_position >= persistent_subscription->max_recorded_position.max_recorded_position)
    {
        persistent_subscription->max_recorded_position.state = REQUEST_MAX_POSITION;
    }

    return false;
}

static bool max_recorded_position_caught_up(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    int64_t replayed_position)
{
    switch (persistent_subscription->max_recorded_position.state)
    {
        case REQUEST_MAX_POSITION:
            return max_recorded_position_request_max_position(persistent_subscription);
        case AWAIT_MAX_POSITION:
            return max_recorded_position_await_max_position(persistent_subscription, replayed_position);
        case RECHECK_REQUIRED:
            return max_recorded_position_recheck_required(persistent_subscription, replayed_position);
    }

    return false;
}

static void clean_up_replay(aeron_archive_persistent_subscription_t *persistent_subscription)
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

static void clean_up_replay_subscription(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    if (NULL != persistent_subscription->add_replay_subscription)
    {
        // TODO how to cancel and delete this?
    }

    if (NULL != persistent_subscription->replay_subscription)
    {
        aeron_subscription_close(persistent_subscription->replay_subscription, NULL, NULL);
    }

    persistent_subscription->add_replay_subscription = NULL;
    persistent_subscription->replay_subscription = NULL;
    persistent_subscription->replay_image = NULL;
}

static void clean_up_live_subscription(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    if (NULL != persistent_subscription->add_live_subscription)
    {
        // TODO how to cancel and delete this?
    }

    if (NULL != persistent_subscription->live_subscription)
    {
        aeron_subscription_close(persistent_subscription->live_subscription, NULL, NULL);
    }

    persistent_subscription->add_live_subscription = NULL;
    persistent_subscription->live_subscription = NULL;
    persistent_subscription->live_image = NULL;
}

static void set_live_image_deadline(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    persistent_subscription->live_image_deadline_ns = aeron_nano_clock() + persistent_subscription->message_timeout_ns;
    persistent_subscription->live_image_deadline_breached = false;
}

static void on_live_image_deadline_breached(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    persistent_subscription->live_image_deadline_breached = true;

    if (NULL != persistent_subscription->listener.on_error)
    {
        persistent_subscription->listener.on_error(
            persistent_subscription->listener.clientd,
            ETIMEDOUT,
            "no image became available on the live subscription within the message timeout. "
            "this could be caused by the publisher being down, or by a misconfiguration of the "
            "subscriber or a firewall between them.");
    }
}

static void on_archive_connected(void *clientd)
{
}

static void on_archive_disconnected(void *clientd)
{
    aeron_archive_persistent_subscription_t *persistent_subscription = clientd;

    aeron_archive_persistent_subscription_state_t state = persistent_subscription->state;
    if (state == AWAIT_ARCHIVE_CONNECTION ||
        state == ATTEMPT_SWITCH ||
        state == LIVE ||
        state == FAILED)
    {
        return;
    }

    clean_up_live_subscription(persistent_subscription);
    clean_up_replay(persistent_subscription);
    clean_up_replay_subscription(persistent_subscription);

    transition(persistent_subscription, AWAIT_ARCHIVE_CONNECTION);
}

static void on_archive_control_response(
    void *clientd,
    int64_t correlation_id,
    int64_t relevant_id,
    int32_t code,
    const char *error_message)
{
    // TODO pass error_message

    aeron_archive_persistent_subscription_t *persistent_subscription = clientd;

    async_archive_op_on_control_response(
        &persistent_subscription->max_recorded_position.op,
        correlation_id,
        relevant_id,
        code);

    async_archive_op_on_control_response(
        &persistent_subscription->list_recording_request.op,
        correlation_id,
        relevant_id,
        code);

    async_archive_op_on_control_response(
        &persistent_subscription->replay_request,
        correlation_id,
        relevant_id,
        code);
}

static void on_archive_recording_descriptor(void *clientd, aeron_archive_recording_descriptor_t *recording_descriptor)
{
    aeron_archive_persistent_subscription_t *persistent_subscription = clientd;

    struct list_recording_request *list_recording_request = &persistent_subscription->list_recording_request;
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
        AERON_APPEND_ERR("%s", "Failed to create fragment assembler");
        goto error;
    }

    _persistent_subscription->archive_listener.clientd = _persistent_subscription;
    _persistent_subscription->archive_listener.on_connected = on_archive_connected;
    _persistent_subscription->archive_listener.on_disconnected = on_archive_disconnected;
    _persistent_subscription->archive_listener.on_control_response = on_archive_control_response;
    _persistent_subscription->archive_listener.on_recording_descriptor = on_archive_recording_descriptor;

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

        if (media != NULL && strcmp(media, "udp") == 0 &&
            endpoint != NULL && strlen(endpoint) >= 2 &&
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

    transition(_persistent_subscription, AWAIT_ARCHIVE_CONNECTION);

    *persistent_subscription = _persistent_subscription;

    return 0;

error:
    aeron_image_controlled_fragment_assembler_delete(_persistent_subscription->assembler);
    aeron_free(_persistent_subscription);

    return -1;
}

int aeron_archive_persistent_subscription_close(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    if (NULL != persistent_subscription)
    {
        clean_up_live_subscription(persistent_subscription);
        clean_up_replay(persistent_subscription);
        clean_up_replay_subscription(persistent_subscription);
        aeron_archive_async_client_close(persistent_subscription->archive);
        aeron_image_controlled_fragment_assembler_delete(persistent_subscription->assembler);
        aeron_free(persistent_subscription);
    }

    return 0;
}

static int await_archive_connection(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    static int64_t check_count = 0;
    bool connected = aeron_archive_async_client_is_connected(persistent_subscription->archive);
    if (++check_count == 1 || check_count % 100000 == 0)
    {
        printf("await_archive_connection: count=%" PRId64 ", connected=%d\n",
            check_count, connected);
        fflush(stdout);
    }

    if (!connected)
    {
        return 0;
    }

    transition(persistent_subscription, SEND_LIST_RECORDING_REQUEST);
    return 1;
}

static int send_list_recording_request(aeron_archive_persistent_subscription_t *persistent_subscription)
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

        transition(persistent_subscription, AWAIT_ARCHIVE_CONNECTION);

        return 1;
    }

    int64_t deadline_ns = aeron_nano_clock() + persistent_subscription->message_timeout_ns;
    async_archive_op_init(&persistent_subscription->list_recording_request.op, correlation_id, deadline_ns);
    persistent_subscription->list_recording_request.remaining = 1;

    transition(persistent_subscription, AWAIT_LIST_RECORDING_RESPONSE);

    return 1;
}

static void set_up_replay(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    max_recorded_position_reset(
        &persistent_subscription->max_recorded_position,
        persistent_subscription->list_recording_request.term_buffer_length >> 2);

    persistent_subscription->join_error = INT64_MIN;

    if (persistent_subscription->replay_channel_type == REPLAY_CHANNEL_DYNAMIC_PORT)
    {
        transition(persistent_subscription, ADD_REPLAY_SUBSCRIPTION);
    }
    else
    {
        transition(persistent_subscription, SEND_REPLAY_REQUEST);
    }
}

static bool validate_descriptor(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    struct list_recording_request *req = &persistent_subscription->list_recording_request;

    if (req->remaining == 0)
    {
        if (persistent_subscription->context->live_stream_id != req->stream_id)
        {
            transition(persistent_subscription, FAILED);

            if (NULL != persistent_subscription->listener.on_error)
            {
                persistent_subscription->listener.on_error(
                    persistent_subscription->listener.clientd,
                    EINVAL,
                    "live stream id does not match stream id of recording");
            }

            return false;
        }

        if (persistent_subscription->position >= 0)
        {
            if (persistent_subscription->position < req->start_position)
            {
                transition(persistent_subscription, FAILED);

                if (NULL != persistent_subscription->listener.on_error)
                {
                    persistent_subscription->listener.on_error(
                        persistent_subscription->listener.clientd,
                        EINVAL,
                        "requested position is lower than start position of recording");
                }

                return false;
            }

            if (req->stop_position != AERON_NULL_VALUE &&
                persistent_subscription->position > req->stop_position)
            {
                transition(persistent_subscription, FAILED);

                if (NULL != persistent_subscription->listener.on_error)
                {
                    persistent_subscription->listener.on_error(
                        persistent_subscription->listener.clientd,
                        EINVAL,
                        "requested position is greater than stop position of recording");
                }

                return false;
            }
        }
        else if (persistent_subscription->position == AERON_PERSISTENT_SUBSCRIPTION_FROM_START)
        {
            persistent_subscription->position = req->start_position;
        }
    }
    else
    {
        transition(persistent_subscription, FAILED);

        if (NULL != persistent_subscription->listener.on_error)
        {
            persistent_subscription->listener.on_error(
                persistent_subscription->listener.clientd,
                EINVAL,
                "no recording found with requested recording id");
        }

        return false;
    }

    return true;
}

static int await_list_recording_response(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    if (!persistent_subscription->list_recording_request.op.response_received)
    {
        if (aeron_nano_clock() - persistent_subscription->list_recording_request.op.deadline_ns >= 0)
        {
            if (aeron_archive_async_client_is_connected(persistent_subscription->archive))
            {
                transition(persistent_subscription, SEND_LIST_RECORDING_REQUEST);
            }
            else
            {
                transition(persistent_subscription, AWAIT_ARCHIVE_CONNECTION);
            }

            return 1;
        }

        return 0;
    }

    if (!validate_descriptor(persistent_subscription))
    {
        return 1;
    }

    if (AERON_PERSISTENT_SUBSCRIPTION_FROM_LIVE == persistent_subscription->position)
    {
        transition(persistent_subscription, ADD_LIVE_SUBSCRIPTION);
    }
    else
    {
        set_up_replay(persistent_subscription);
    }

    return 1;
}

static int send_replay_request(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    int64_t correlation_id = aeron_next_correlation_id(persistent_subscription->context->aeron);

    aeron_archive_replay_params_t params;
    aeron_archive_replay_params_init(&params);
    params.position = persistent_subscription->position;

    if (!aeron_archive_async_client_try_send_replay_request(
        persistent_subscription->archive,
        correlation_id,
        persistent_subscription->context->recording_id,
        persistent_subscription->replay_channel_uri,
        persistent_subscription->context->replay_stream_id,
        &params))
    {
        if (aeron_archive_async_client_is_connected(persistent_subscription->archive))
        {
            return 0;
        }

        clean_up_replay_subscription(persistent_subscription);
        transition(persistent_subscription, AWAIT_ARCHIVE_CONNECTION);

        return 1;
    }

    int64_t deadline_ns = aeron_nano_clock() + persistent_subscription->message_timeout_ns;
    async_archive_op_init(&persistent_subscription->replay_request, correlation_id, deadline_ns);

    transition(persistent_subscription, AWAIT_REPLAY_RESPONSE);

    return 1;
}

static int await_replay_response(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    if (!persistent_subscription->replay_request.response_received)
    {
        if (aeron_nano_clock() - persistent_subscription->replay_request.deadline_ns >= 0)
        {
            if (aeron_archive_async_client_is_connected(persistent_subscription->archive))
            {
                clean_up_replay_subscription(persistent_subscription);
                set_up_replay(persistent_subscription);
            }
            else
            {
                transition(persistent_subscription, AWAIT_ARCHIVE_CONNECTION);
            }

            return 1;
        }

        return 0;
    }

    if (persistent_subscription->replay_request.code != aeron_archive_client_controlResponseCode_OK)
    {
        transition(persistent_subscription, FAILED);

        clean_up_replay_subscription(persistent_subscription);

        if (NULL != persistent_subscription->listener.on_error)
        {
            persistent_subscription->listener.on_error(
                persistent_subscription->listener.clientd,
                (int)persistent_subscription->replay_request.relevant_id,
                "replay request failed");
        }

        return 1;
    }

    persistent_subscription->replay_session_id = persistent_subscription->replay_request.relevant_id;

    if (persistent_subscription->replay_channel_type == REPLAY_CHANNEL_SESSION_SPECIFIC)
    {
        // Inject the session id into the replay channel URI so the subscription only accepts this replay session
        aeron_uri_string_builder_t builder;
        if (aeron_uri_string_builder_init_on_string(&builder, persistent_subscription->replay_channel_uri) < 0)
        {
            AERON_APPEND_ERR("%s", "Failed to parse replay_channel_uri");
            transition(persistent_subscription, FAILED);
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

        transition(persistent_subscription, ADD_REPLAY_SUBSCRIPTION);
    }
    else
    {
        persistent_subscription->replay_image_deadline_ns =
            aeron_nano_clock() + persistent_subscription->message_timeout_ns;

        transition(persistent_subscription, REPLAY);
    }

    return 1;
}

static int add_replay_subscription(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    // Dynamic port: use the raw context channel (with :0) so the OS assigns a free port.
    // Session-specific: use replay_channel_uri which now has the session id injected.
    const char *channel = persistent_subscription->replay_channel_type == REPLAY_CHANNEL_DYNAMIC_PORT
        ? persistent_subscription->context->replay_channel
        : persistent_subscription->replay_channel_uri;

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
        transition(persistent_subscription, FAILED);

        if (NULL != persistent_subscription->listener.on_error)
        {
            persistent_subscription->listener.on_error(
                persistent_subscription->listener.clientd,
                aeron_errcode(),
                aeron_errmsg());
        }

        return 1;
    }

    transition(persistent_subscription, AWAIT_REPLAY_SUBSCRIPTION);

    return 1;
}

static int await_replay_subscription(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    if (aeron_async_add_subscription_poll(
        &persistent_subscription->replay_subscription,
        persistent_subscription->add_replay_subscription) < 0)
    {
        int errcode = aeron_errcode();

        if (errcode == ENOTCONN)
        {
            clean_up_replay(persistent_subscription);
            set_up_replay(persistent_subscription);
        }
        else
        {
            transition(persistent_subscription, FAILED);
        }

        if (NULL != persistent_subscription->listener.on_error)
        {
            persistent_subscription->listener.on_error(
                persistent_subscription->listener.clientd,
                errcode,
                aeron_errmsg());
        }

        return 1;
    }

    if (NULL == persistent_subscription->replay_subscription)
    {
        return 0;
    }

    persistent_subscription->add_replay_subscription = NULL;
    persistent_subscription->replay_image_deadline_ns = aeron_nano_clock() + persistent_subscription->message_timeout_ns;

    if (persistent_subscription->replay_channel_type == REPLAY_CHANNEL_SESSION_SPECIFIC)
    {
        transition(persistent_subscription, REPLAY);
    }
    else
    {
        transition(persistent_subscription, AWAIT_REPLAY_CHANNEL_ENDPOINT);
    }

    return 1;
}

static int await_replay_channel_endpoint(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    if (aeron_subscription_try_resolve_channel_endpoint_port(
        persistent_subscription->replay_subscription,
        persistent_subscription->replay_channel_uri,
        sizeof(persistent_subscription->replay_channel_uri)) < 0)
    {
        // Not yet resolved, try again next poll
        return 0;
    }

    transition(persistent_subscription, SEND_REPLAY_REQUEST);

    return 1;
}

static void do_add_live_subscription(aeron_archive_persistent_subscription_t *persistent_subscription)
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
        // TODO
    }
}

static int replay(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    aeron_controlled_fragment_handler_t handler,
    void *clientd,
    size_t fragment_limit)
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
                clean_up_replay(persistent_subscription);
                clean_up_replay_subscription(persistent_subscription);
                set_up_replay(persistent_subscription);

                return 1;
            }

            return 0;
        }

        persistent_subscription->replay_image = image;
    }

    if (aeron_image_is_closed(image))
    {
        clean_up_live_subscription(persistent_subscription);
        clean_up_replay(persistent_subscription);
        clean_up_replay_subscription(persistent_subscription);
        set_up_replay(persistent_subscription);

        return 1;
    }

    if (NULL == persistent_subscription->live_subscription && NULL != persistent_subscription->add_live_subscription)
    {
        if (aeron_async_add_subscription_poll(
            &persistent_subscription->live_subscription,
            persistent_subscription->add_live_subscription) < 0)
        {
            // TODO
        }

        if (NULL != persistent_subscription->live_subscription)
        {
            persistent_subscription->add_live_subscription = NULL;
            set_live_image_deadline(persistent_subscription);
        }
    }

    if (NULL != persistent_subscription->live_subscription)
    {
        if (aeron_subscription_image_count(persistent_subscription->live_subscription) > 0)
        {
            persistent_subscription->live_image = aeron_subscription_image_at_index(
                persistent_subscription->live_subscription,
                0);

            persistent_subscription->join_error = aeron_image_position(persistent_subscription->live_image) -
                                      aeron_image_position(image);

            transition(persistent_subscription, ATTEMPT_SWITCH);

            return 1;
        }
        else if (!persistent_subscription->live_image_deadline_breached &&
                 aeron_nano_clock() - persistent_subscription->live_image_deadline_ns >= 0)
        {
            on_live_image_deadline_breached(persistent_subscription);
        }
    }

    aeron_image_controlled_fragment_assembler_t *assembler = persistent_subscription->assembler;
    assembler->delegate = handler;
    assembler->delegate_clientd = clientd;

    int fragments = aeron_image_controlled_poll(
        image,
        aeron_image_controlled_fragment_assembler_handler,
        assembler,
        fragment_limit);

    int64_t position = aeron_image_position(image);

    if (NULL == persistent_subscription->add_live_subscription &&
        NULL == persistent_subscription->live_subscription &&
        max_recorded_position_caught_up(persistent_subscription, position))
    {
        do_add_live_subscription(persistent_subscription);
    }

    return fragments;
}

static aeron_controlled_fragment_handler_action_t live_catchup_fragment_handler(
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

static aeron_controlled_fragment_handler_action_t replay_catchup_fragment_handler(
    void *clientd,
    const uint8_t *buffer,
    size_t length,
    aeron_header_t *header)
{
    aeron_archive_persistent_subscription_t *persistent_subscription = clientd;
    int64_t current_replay_position = aeron_header_position(header);
    if (current_replay_position == persistent_subscription->next_live_position)
    {
        transition(persistent_subscription, LIVE);
        return AERON_ACTION_ABORT;
    }
    return aeron_image_controlled_fragment_assembler_handler(
        persistent_subscription->assembler,
        buffer,
        length,
        header);
}

static int attempt_switch(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    aeron_controlled_fragment_handler_t handler,
    void *clientd,
    size_t fragment_limit)
{
    int fragments = 0;

    aeron_image_t *live_image = persistent_subscription->live_image;
    aeron_image_t *replay_image = persistent_subscription->replay_image;

    int64_t live_position = aeron_image_position(live_image);
    int64_t replay_position = aeron_image_position(replay_image);

    if (replay_position == live_position)
    {
        transition(persistent_subscription, LIVE);
    }
    else
    {
        if (aeron_image_is_closed(replay_image))
        {
            persistent_subscription->position = replay_position;
            clean_up_live_subscription(persistent_subscription);
            clean_up_replay(persistent_subscription);
            clean_up_replay_subscription(persistent_subscription);
            set_up_replay(persistent_subscription);

            return 1;
        }

        if (aeron_image_is_closed(live_image))
        {
            clean_up_live_subscription(persistent_subscription);

            persistent_subscription->join_error = INT64_MIN;
            max_recorded_position_reset(
                &persistent_subscription->max_recorded_position,
                persistent_subscription->list_recording_request.term_buffer_length >> 2);

            transition(persistent_subscription, REPLAY);

            return 1;
        }

        fragments += aeron_image_controlled_poll(
            live_image,
            live_catchup_fragment_handler,
            persistent_subscription,
            fragment_limit);

        aeron_image_controlled_fragment_assembler_t *assembler = persistent_subscription->assembler;
        assembler->delegate = handler;
        assembler->delegate_clientd = clientd;

        fragments += aeron_image_controlled_poll(
            replay_image,
            replay_catchup_fragment_handler,
            persistent_subscription,
            fragment_limit);
    }

    if (aeron_archive_persistent_subscription_is_live(persistent_subscription))
    {
        clean_up_replay(persistent_subscription);
        clean_up_replay_subscription(persistent_subscription);

        if (NULL != persistent_subscription->listener.on_live_joined)
        {
            persistent_subscription->listener.on_live_joined(persistent_subscription->listener.clientd);
        }
    }

    return fragments;
}

static int add_live_subscription(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    do_add_live_subscription(persistent_subscription);

    transition(persistent_subscription, AWAIT_LIVE);

    return 1;
}

static int await_live(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    // awaiting live subscription or its image before going directly to live (no replay or switch)

    if (NULL == persistent_subscription->live_subscription)
    {
        if (aeron_async_add_subscription_poll(
            &persistent_subscription->live_subscription,
            persistent_subscription->add_live_subscription) < 0)
        {
            int errcode = aeron_errcode();

            persistent_subscription->add_live_subscription = NULL;

            if (errcode == ENOTCONN)
            {
                transition(persistent_subscription, ADD_LIVE_SUBSCRIPTION);
            }
            else
            {
                transition(persistent_subscription, FAILED);
            }

            if (NULL != persistent_subscription->listener.on_error)
            {
                persistent_subscription->listener.on_error(
                    persistent_subscription->listener.clientd,
                    errcode,
                    aeron_errmsg());
            }

            return 1;
        }

        if (NULL != persistent_subscription->live_subscription)
        {
            persistent_subscription->add_live_subscription = NULL;
            set_live_image_deadline(persistent_subscription);
        }
    }

    if (NULL != persistent_subscription->live_subscription)
    {
        if (aeron_subscription_image_count(persistent_subscription->live_subscription) > 0)
        {
            aeron_image_t *image = aeron_subscription_image_at_index(persistent_subscription->live_subscription, 0);
            persistent_subscription->live_image = image;
            persistent_subscription->position = aeron_image_position(image);

            persistent_subscription->join_error = 0;
            transition(persistent_subscription, LIVE);

            if (NULL != persistent_subscription->listener.on_live_joined)
            {
                persistent_subscription->listener.on_live_joined(persistent_subscription->listener.clientd);
            }

            return 1;
        }
        else if (!persistent_subscription->live_image_deadline_breached &&
                 aeron_nano_clock() - persistent_subscription->live_image_deadline_ns >= 0)
        {
            on_live_image_deadline_breached(persistent_subscription);
        }
    }

    return 0;
}

static int live(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    aeron_controlled_fragment_handler_t handler,
    void *clientd,
    size_t fragment_limit)
{
    aeron_image_t *image = persistent_subscription->live_image;

    aeron_image_controlled_fragment_assembler_t *assembler = persistent_subscription->assembler;
    assembler->delegate = handler;
    assembler->delegate_clientd = clientd;

    int fragments = aeron_image_controlled_poll(
        image,
        aeron_image_controlled_fragment_assembler_handler,
        assembler,
        fragment_limit);

    if (fragments == 0 && aeron_image_is_closed(image))
    {
        persistent_subscription->position = aeron_image_position(image);
        clean_up_live_subscription(persistent_subscription);
        set_up_replay(persistent_subscription);

        if (NULL != persistent_subscription->listener.on_live_left)
        {
            persistent_subscription->listener.on_live_left(persistent_subscription->listener.clientd);
        }

        return 1;
    }

    return fragments;
}

int aeron_archive_persistent_subscription_controlled_poll(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    aeron_controlled_fragment_handler_t handler,
    void *clientd,
    size_t fragment_limit)
{
    // todo: this call is a bit fishy because the function can return a negative value
    int work_count = aeron_archive_async_client_poll(persistent_subscription->archive);


    switch (persistent_subscription->state)
    {
        case AWAIT_ARCHIVE_CONNECTION:
            work_count += await_archive_connection(persistent_subscription);
            break;
        case SEND_LIST_RECORDING_REQUEST:
            work_count += send_list_recording_request(persistent_subscription);
            break;
        case AWAIT_LIST_RECORDING_RESPONSE:
            work_count += await_list_recording_response(persistent_subscription);
            break;
        case SEND_REPLAY_REQUEST:
            work_count += send_replay_request(persistent_subscription);
            break;
        case AWAIT_REPLAY_RESPONSE:
            work_count += await_replay_response(persistent_subscription);
            break;
        case ADD_REPLAY_SUBSCRIPTION:
            work_count += add_replay_subscription(persistent_subscription);
            break;
        case AWAIT_REPLAY_SUBSCRIPTION:
            work_count += await_replay_subscription(persistent_subscription);
            break;
        case AWAIT_REPLAY_CHANNEL_ENDPOINT:
            work_count += await_replay_channel_endpoint(persistent_subscription);
            break;
        case REPLAY:
            work_count += replay(persistent_subscription, handler, clientd, fragment_limit);
            break;
        case ATTEMPT_SWITCH:
            work_count += attempt_switch(persistent_subscription, handler, clientd, fragment_limit);
            break;
        case ADD_LIVE_SUBSCRIPTION:
            work_count += add_live_subscription(persistent_subscription);
            break;
        case AWAIT_LIVE:
            work_count += await_live(persistent_subscription);
            break;
        case LIVE:
            work_count += live(persistent_subscription, handler, clientd, fragment_limit);
            break;
        case FAILED:
            break;
    }

    return work_count < 0 ? 0 : work_count;
}

bool aeron_archive_persistent_subscription_is_live(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    return persistent_subscription->state == LIVE;
}

bool aeron_archive_persistent_subscription_is_replaying(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    return persistent_subscription->state == REPLAY ||
           persistent_subscription->state == ATTEMPT_SWITCH;
}

bool aeron_archive_persistent_subscription_has_failed(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    return persistent_subscription->state == FAILED;
}

int64_t aeron_archive_persistent_subscription_join_error(aeron_archive_persistent_subscription_t *persistent_subscription)
{
    return persistent_subscription->join_error;
}