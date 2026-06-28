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

#include <errno.h>
#include <stdio.h>
#include <inttypes.h>

#include "aeron_archive.h"
#include "aeron_archive_context.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_parse_util.h"
#include "aeron_agent.h"
#include "protocol/aeron_udp_protocol.h"
#include "concurrent/aeron_logbuffer_descriptor.h"
#include "uri/aeron_uri_string_builder.h"
#include "command/aeron_control_protocol.h"

int aeron_archive_context_init(aeron_archive_context_t **ctx)
{
    aeron_archive_context_t *_ctx = NULL;

    if (NULL == ctx)
    {
        AERON_SET_ERR(EINVAL, "%s", "aeron_archive_context_init(NULL)");
        return -1;
    }

    if (aeron_alloc((void **)&_ctx, sizeof(aeron_archive_context_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_context_init");
        return -1;
    }

    _ctx->aeron = NULL;
    if (aeron_default_path(_ctx->aeron_directory_name, sizeof(_ctx->aeron_directory_name)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to resolve default aeron directory path");
        return -1;
    }

    _ctx->owns_aeron_client = false;

    _ctx->control_request_channel = NULL;
    _ctx->control_request_channel_length = 0;
    _ctx->control_request_stream_id = AERON_ARCHIVE_CONTROL_STREAM_ID_DEFAULT;

    _ctx->control_response_channel = NULL;
    _ctx->control_response_channel_length = 0;
    _ctx->control_response_stream_id = AERON_ARCHIVE_CONTROL_RESPONSE_STREAM_ID_DEFAULT;

    _ctx->recording_events_channel = NULL;
    _ctx->recording_events_channel_length = 0;
    _ctx->recording_events_stream_id = AERON_ARCHIVE_RECORDING_EVENTS_STREAM_ID_DEFAULT;

    _ctx->message_timeout_ns = AERON_ARCHIVE_MESSAGE_TIMEOUT_NS_DEFAULT;
    _ctx->message_retry_attempts = AERON_ARCHIVE_MESSAGE_RETRY_ATTEMPTS_DEFAULT;

    _ctx->control_term_buffer_sparse = AERON_ARCHIVE_CONTROL_TERM_BUFFER_SPARSE_DEFAULT;
    _ctx->control_term_buffer_length = AERON_ARCHIVE_CONTROL_TERM_BUFFER_LENGTH_DEFAULT;

    _ctx->idle_strategy_func = NULL;
    _ctx->idle_strategy_state = NULL;
    _ctx->owns_idle_strategy = false;

    aeron_archive_context_set_credentials_supplier(_ctx, NULL, NULL, NULL, NULL);

    _ctx->error_handler = NULL;
    _ctx->error_handler_clientd = NULL;

    _ctx->delegating_invoker_func = NULL;
    _ctx->delegating_invoker_func_clientd = NULL;

    _ctx->on_recording_signal = NULL;
    _ctx->on_recording_signal_clientd = NULL;

    // like in Java the default value of the control MTU is defined by the driver configuration
    _ctx->control_mtu_length = aeron_config_parse_size64(
        "AERON_MTU_LENGTH",
        getenv("AERON_MTU_LENGTH"),
        1408,
        AERON_DATA_HEADER_LENGTH,
        AERON_MAX_UDP_PAYLOAD_LENGTH);

    _ctx->client_name[0] = '\0';

    char *value = NULL;

    if ((value = getenv(AERON_DIR_ENV_VAR)))
    {
        aeron_archive_context_set_aeron_directory_name(_ctx, value);
    }

    if ((value = getenv(AERON_ARCHIVE_CLIENT_NAME_ENV_VAR)))
    {
        aeron_archive_context_set_client_name(_ctx, value);
    }

    if ((value = getenv(AERON_ARCHIVE_CONTROL_CHANNEL_ENV_VAR)))
    {
        if (aeron_archive_context_set_control_request_channel(_ctx, value) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }

    _ctx->control_request_stream_id = aeron_config_parse_int32(
        AERON_ARCHIVE_CONTROL_STREAM_ID_ENV_VAR,
        getenv(AERON_ARCHIVE_CONTROL_STREAM_ID_ENV_VAR),
        _ctx->control_request_stream_id,
        INT32_MIN,
        INT32_MAX);

    if ((value = getenv(AERON_ARCHIVE_CONTROL_RESPONSE_CHANNEL_ENV_VAR)))
    {
        if (aeron_archive_context_set_control_response_channel(_ctx, value) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }

    _ctx->control_response_stream_id = aeron_config_parse_int32(
        AERON_ARCHIVE_CONTROL_RESPONSE_STREAM_ID_ENV_VAR,
        getenv(AERON_ARCHIVE_CONTROL_RESPONSE_STREAM_ID_ENV_VAR),
        _ctx->control_response_stream_id,
        INT32_MIN,
        INT32_MAX);

    if ((value = getenv(AERON_ARCHIVE_RECORDING_EVENTS_CHANNEL_ENV_VAR)))
    {
        if (aeron_archive_context_set_recording_events_channel(_ctx, value) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }

    _ctx->recording_events_stream_id = aeron_config_parse_int32(
        AERON_ARCHIVE_RECORDING_EVENTS_STREAM_ID_ENV_VAR,
        getenv(AERON_ARCHIVE_RECORDING_EVENTS_STREAM_ID_ENV_VAR),
        _ctx->recording_events_stream_id,
        INT32_MIN,
        INT32_MAX);

    _ctx->message_timeout_ns = aeron_config_parse_duration_ns(
        AERON_ARCHIVE_MESSAGE_TIMEOUT_ENV_VAR,
        getenv(AERON_ARCHIVE_MESSAGE_TIMEOUT_ENV_VAR),
        _ctx->message_timeout_ns,
        1000,
        INT64_MAX);

    _ctx->message_retry_attempts = aeron_config_parse_duration_ns(
        AERON_ARCHIVE_MESSAGE_RETRY_ATTEMPTS_ENV_VAR,
        getenv(AERON_ARCHIVE_MESSAGE_RETRY_ATTEMPTS_ENV_VAR),
        _ctx->message_retry_attempts,
        0,
        INT32_MAX);

    _ctx->control_term_buffer_length = aeron_config_parse_size64(
        AERON_ARCHIVE_CONTROL_TERM_BUFFER_LENGTH_ENV_VAR,
        getenv(AERON_ARCHIVE_CONTROL_TERM_BUFFER_LENGTH_ENV_VAR),
        _ctx->control_term_buffer_length,
        AERON_LOGBUFFER_TERM_MIN_LENGTH,
        AERON_LOGBUFFER_TERM_MAX_LENGTH);

    _ctx->control_mtu_length = aeron_config_parse_size64(
        AERON_ARCHIVE_CONTROL_MTU_LENGTH_ENV_VAR,
        getenv(AERON_ARCHIVE_CONTROL_MTU_LENGTH_ENV_VAR),
        _ctx->control_mtu_length,
        AERON_DATA_HEADER_LENGTH,
        AERON_MAX_UDP_PAYLOAD_LENGTH);

    _ctx->control_term_buffer_sparse = aeron_parse_bool(
        getenv(AERON_ARCHIVE_CONTROL_TERM_BUFFER_SPARSE_ENV_VAR), _ctx->control_term_buffer_sparse);

    *ctx = _ctx;
    return 0;

error:
    aeron_free(_ctx);
    return -1;
}

int aeron_archive_context_close(aeron_archive_context_t *ctx)
{
    if (NULL != ctx)
    {
        if (ctx->owns_aeron_client)
        {
            aeron_context_t *aeron_ctx = aeron_context(ctx->aeron);

            aeron_close(ctx->aeron);
            ctx->aeron = NULL;

            aeron_context_close(aeron_ctx);
        }

        aeron_free(ctx->control_request_channel);
        aeron_free(ctx->control_response_channel);
        aeron_free(ctx->recording_events_channel);

        if (ctx->owns_idle_strategy)
        {
            aeron_free(ctx->idle_strategy_state);
        }

        aeron_free(ctx);
    }

    return 0;
}

int aeron_archive_context_duplicate(aeron_archive_context_t **dest_p, aeron_archive_context_t *src)
{
    aeron_archive_context_t *_ctx = NULL;

    if (aeron_alloc((void **)&_ctx, sizeof(aeron_archive_context_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_context_init");
        return -1;
    }

    memcpy(_ctx, src, sizeof(aeron_archive_context_t));

    _ctx->control_request_channel = NULL;
    if (aeron_archive_context_set_control_request_channel(_ctx, src->control_request_channel) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    _ctx->control_response_channel = NULL;
    if(aeron_archive_context_set_control_response_channel(_ctx, src->control_response_channel) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    _ctx->recording_events_channel = NULL;
    if (aeron_archive_context_set_recording_events_channel(_ctx, src->recording_events_channel) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    _ctx->owns_idle_strategy = false;

    *dest_p = _ctx;

    return 0;
}

static int aeron_archive_apply_default_parameters(aeron_archive_context_t *ctx, const char* uri, aeron_uri_string_builder_t *builder)
{

    if (aeron_uri_string_builder_init_on_string(builder, uri) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    if (NULL == aeron_uri_string_builder_get(builder, AERON_URI_TERM_LENGTH_KEY))
    {
        if (aeron_uri_string_builder_put_int32(builder, AERON_URI_TERM_LENGTH_KEY, (int32_t)ctx->control_term_buffer_length) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }

    if (NULL == aeron_uri_string_builder_get(builder, AERON_URI_MTU_LENGTH_KEY))
    {
        if (aeron_uri_string_builder_put_int32(builder, AERON_URI_MTU_LENGTH_KEY, (int32_t)ctx->control_mtu_length) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }

    if (NULL == aeron_uri_string_builder_get(builder, AERON_URI_SPARSE_TERM_KEY))
    {
        if (aeron_uri_string_builder_put(builder, AERON_URI_SPARSE_TERM_KEY, ctx->control_term_buffer_sparse ? "true" : "false") < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }

    return 0;

error:
    aeron_uri_string_builder_close(builder);
    return -1;
}

int aeron_archive_context_conclude(aeron_archive_context_t *ctx)
{
    if (NULL == ctx->control_request_channel)
    {
        AERON_SET_ERR(EINVAL, "%s", "control request channel is required");
        goto error;
    }

    if (NULL == ctx->control_response_channel)
    {
        AERON_SET_ERR(EINVAL, "%s", "control response channel is required");
        goto error;
    }

    if (0 == ctx->message_retry_attempts)
    {
        AERON_SET_ERR(EINVAL, "%s", "message_retry_attempts must be > 0");
        goto error;
    }

    if (NULL == ctx->aeron)
    {
        ctx->owns_aeron_client = true;

        aeron_context_t *aeron_ctx;
        if (aeron_context_init(&aeron_ctx) < 0 ||
            aeron_context_set_dir(aeron_ctx, ctx->aeron_directory_name) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }

        if (aeron_context_set_client_name(aeron_ctx, "archive-client") < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }

        if (aeron_init(&ctx->aeron, aeron_ctx) < 0 ||
            aeron_start(ctx->aeron) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }

    aeron_uri_string_builder_t request_channel;
    if (aeron_archive_apply_default_parameters(ctx, ctx->control_request_channel, &request_channel) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    aeron_uri_string_builder_t response_channel;
    if (aeron_archive_apply_default_parameters(ctx, ctx->control_response_channel, &response_channel) < 0)
    {
        aeron_uri_string_builder_close(&request_channel);
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    const char* control_mode =
        aeron_uri_string_builder_get(&response_channel, AERON_UDP_CHANNEL_CONTROL_MODE_KEY);
    if (NULL == control_mode ||
        0 != strcmp(AERON_UDP_CHANNEL_CONTROL_MODE_RESPONSE_VALUE, control_mode))
    {
        aeron_async_get_next_available_session_id_t *async = NULL;
        if (aeron_async_next_session_id(&async, ctx->aeron, ctx->control_request_stream_id) < 0)
        {
            AERON_APPEND_ERR("%s", "Failed to fetch next session-id");
            goto error_close_uri_builders;
        }

        int result = 0;
        int32_t session_id;
        do
        {
            aeron_archive_context_invoke_aeron_client(ctx);

            result = aeron_async_next_session_id_poll(&session_id, async);
            if (result < 0)
            {
                AERON_APPEND_ERR("%s", "Failed to fetch next session-id");
                goto error_close_uri_builders;
            }
        } while (0 == result);

        if (aeron_uri_string_builder_put_int32(&request_channel, AERON_URI_SESSION_ID_KEY, session_id) < 0 ||
            aeron_uri_string_builder_put_int32(&response_channel, AERON_URI_SESSION_ID_KEY, session_id) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error_close_uri_builders;
        }
    }

    char uri[AERON_URI_MAX_LENGTH];
    if (aeron_uri_string_builder_sprint(&request_channel, uri, sizeof(uri)) < 0 ||
        aeron_archive_context_set_control_request_channel(ctx, uri) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_close_uri_builders;
    }

    if (aeron_uri_string_builder_sprint(&response_channel, uri, sizeof(uri)) < 0 ||
        aeron_archive_context_set_control_response_channel(ctx, uri) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error_close_uri_builders;
    }

    aeron_uri_string_builder_close(&request_channel);
    aeron_uri_string_builder_close(&response_channel);

    if (NULL == ctx->idle_strategy_func)
    {
        ctx->owns_idle_strategy = true;
        if (NULL == (ctx->idle_strategy_func = aeron_idle_strategy_load(
            "backoff",
            &ctx->idle_strategy_state,
            "AERON_ARCHIVE_IDLE_STRATEGY",
            NULL)))
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }

    return 0;

error_close_uri_builders:
    aeron_uri_string_builder_close(&request_channel);
    aeron_uri_string_builder_close(&response_channel);
error:
    if (ctx->owns_aeron_client && NULL != ctx->aeron)
    {
        aeron_close(ctx->aeron);
        ctx->aeron = NULL;
    }

    return -1;
}

void aeron_archive_context_idle(aeron_archive_context_t *ctx)
{
    ctx->idle_strategy_func(ctx->idle_strategy_state, 0);
}

void aeron_archive_context_invoke_aeron_client(aeron_archive_context_t *ctx)
{
    if (aeron_context_get_use_conductor_agent_invoker(aeron_context(ctx->aeron)))
    {
        aeron_main_do_work(ctx->aeron);
    }

    if (NULL != ctx->delegating_invoker_func)
    {
        ctx->delegating_invoker_func(ctx->delegating_invoker_func_clientd);
    }
}

void aeron_archive_context_invoke_error_handler(
    aeron_archive_context_t *ctx, int64_t correlation_id, int32_t error_code, const char *error_message)
{
    size_t formatted_message_length = strlen(error_message) + 100;
    char *formatted_error_message;
    aeron_alloc((void **)&formatted_error_message, formatted_message_length);

    snprintf(
        formatted_error_message,
        formatted_message_length,
        "response for correlationId=%" PRIi64 ", errorCode=%" PRIi32 ", error: %s",
        correlation_id,
        error_code,
        error_message);

    ctx->error_handler(
        ctx->error_handler_clientd,
        AERON_ERROR_CODE_GENERIC_ERROR,
        formatted_error_message);

    aeron_free(formatted_error_message);
}

int aeron_archive_context_set_aeron(aeron_archive_context_t *ctx, aeron_t *aeron)
{
    ctx->aeron = aeron;

    return 0;
}

aeron_t *aeron_archive_context_get_aeron(aeron_archive_context_t *ctx)
{
    return ctx->aeron;
}

int aeron_archive_context_set_owns_aeron_client(aeron_archive_context_t *ctx, bool owns_aeron_client)
{
    ctx->owns_aeron_client = owns_aeron_client;

    return 0;
}

bool aeron_archive_context_get_owns_aeron_client(aeron_archive_context_t *ctx)
{
    return ctx->owns_aeron_client;
}

int aeron_archive_context_set_aeron_directory_name(aeron_archive_context_t *ctx, const char *aeron_directory_name)
{
    snprintf(ctx->aeron_directory_name, sizeof(ctx->aeron_directory_name), "%s", aeron_directory_name);

    return 0;
}

const char *aeron_archive_context_get_aeron_directory_name(aeron_archive_context_t *ctx)
{
    return ctx->aeron_directory_name;
}

static int aeron_archive_context_set_channel(char **target_channel, size_t *target_channel_length, const char *channel)
{
    if (NULL == channel)
    {
        aeron_free(*target_channel);
        *target_channel = NULL;
        *target_channel_length = 0;
    }
    else
    {
        const size_t channel_length = 1 + strlen(channel);
        char *temp = *target_channel;

        if (NULL == temp)
        {
            if (aeron_alloc((void **)&temp, channel_length) < 0)
            {
                AERON_SET_ERR(ENOMEM, "%s", "unable to allocate control_response_channel");
                return -1;
            }
        }
        else if (channel_length > *target_channel_length)
        {
            if (aeron_reallocf((void **)&temp, channel_length) < 0)
            {
                AERON_APPEND_ERR("%s", "unable to reallocate control_response_channel");
                return -1;
            }
        }

        snprintf(temp, channel_length, "%s", channel);

        *target_channel = temp;
        *target_channel_length = channel_length;
    }

    return 0;
}

int aeron_archive_context_set_control_request_channel(
    aeron_archive_context_t *ctx,
    const char *control_request_channel)
{
    return aeron_archive_context_set_channel(
        &ctx->control_request_channel, &ctx->control_request_channel_length, control_request_channel);
}

const char *aeron_archive_context_get_control_request_channel(aeron_archive_context_t *ctx)
{
    return ctx->control_request_channel;
}

int aeron_archive_context_set_control_request_stream_id(aeron_archive_context_t *ctx, int32_t control_request_stream_id)
{
    ctx->control_request_stream_id = control_request_stream_id;

    return 0;
}

int32_t aeron_archive_context_get_control_request_stream_id(aeron_archive_context_t *ctx)
{
    return ctx->control_request_stream_id;
}

int aeron_archive_context_set_control_response_channel(
    aeron_archive_context_t *ctx,
    const char *control_response_channel)
{
    return aeron_archive_context_set_channel(
        &ctx->control_response_channel, &ctx->control_response_channel_length, control_response_channel);
}

const char *aeron_archive_context_get_control_response_channel(aeron_archive_context_t *ctx)
{
    return ctx->control_response_channel;
}

int aeron_archive_context_set_control_response_stream_id(aeron_archive_context_t *ctx, int32_t control_response_stream_id)
{
    ctx->control_response_stream_id = control_response_stream_id;

    return 0;
}

int32_t aeron_archive_context_get_control_response_stream_id(aeron_archive_context_t *ctx)
{
    return ctx->control_response_stream_id;
}

int aeron_archive_context_set_control_term_buffer_length(aeron_archive_context_t *ctx, size_t control_term_buffer_length)
{
    ctx->control_term_buffer_length = control_term_buffer_length;

    return 0;
}

size_t aeron_archive_context_get_control_term_buffer_length(aeron_archive_context_t *ctx)
{
    return ctx->control_term_buffer_length;
}

int aeron_archive_context_set_control_term_buffer_sparse(aeron_archive_context_t *ctx, bool control_term_buffer_sparse)
{
    ctx->control_term_buffer_sparse = control_term_buffer_sparse;

    return 0;
}

bool aeron_archive_context_get_control_term_buffer_sparse(aeron_archive_context_t *ctx)
{
    return ctx->control_term_buffer_sparse;
}

int aeron_archive_context_set_control_mtu_length(aeron_archive_context_t *ctx, size_t control_mtu_length)
{
    ctx->control_mtu_length = control_mtu_length;

    return 0;
}

size_t aeron_archive_context_get_control_mtu_length(aeron_archive_context_t *ctx)
{
    return ctx->control_mtu_length;
}

int aeron_archive_context_set_client_name(aeron_archive_context_t *context, const char *value)
{
    size_t copy_length = 0;
    if (!aeron_str_length(value, AERON_COUNTER_MAX_CLIENT_NAME_LENGTH + 1, &copy_length))
    {
        AERON_SET_ERR(EINVAL, "client_name length must <= %d", AERON_COUNTER_MAX_CLIENT_NAME_LENGTH);
        return -1;
    }

    memcpy(context->client_name, value, copy_length);
    context->client_name[copy_length] = '\0';
    return 0;
}

const char *aeron_archive_context_get_client_name(aeron_archive_context_t *context)
{
    return context->client_name;
}

int aeron_archive_context_set_recording_events_channel(
    aeron_archive_context_t *ctx,
    const char *recording_events_channel)
{
    return aeron_archive_context_set_channel(
        &ctx->recording_events_channel, &ctx->recording_events_channel_length, recording_events_channel);
}

const char *aeron_archive_context_get_recording_events_channel(aeron_archive_context_t *ctx)
{
    return ctx->recording_events_channel;
}

int aeron_archive_context_set_recording_events_stream_id(aeron_archive_context_t *ctx, int32_t recording_events_stream_id)
{
    ctx->recording_events_stream_id = recording_events_stream_id;

    return 0;
}

int32_t aeron_archive_context_get_recording_events_stream_id(aeron_archive_context_t *ctx)
{
    return ctx->recording_events_stream_id;
}

int aeron_archive_context_set_message_timeout_ns(aeron_archive_context_t *ctx, uint64_t message_timeout_ns)
{
    ctx->message_timeout_ns = message_timeout_ns;

    return 0;
}

uint64_t aeron_archive_context_get_message_timeout_ns(aeron_archive_context_t *ctx)
{
    return ctx->message_timeout_ns;
}

int aeron_archive_context_set_message_retry_attempts(aeron_archive_context_t *ctx, uint32_t message_retry_attempts)
{
    ctx->message_retry_attempts = message_retry_attempts;
    return 0;
}

uint32_t aeron_archive_context_get_message_retry_attempts(aeron_archive_context_t *ctx)
{
    return ctx->message_retry_attempts;
}

int aeron_archive_context_set_idle_strategy(
    aeron_archive_context_t *ctx,
    aeron_idle_strategy_func_t idle_strategy_func,
    void *idle_strategy_state)
{
    ctx->idle_strategy_func = idle_strategy_func;
    ctx->idle_strategy_state = idle_strategy_state;

    return 0;
}

int aeron_archive_context_set_credentials_supplier(
    aeron_archive_context_t *ctx,
    aeron_archive_credentials_encoded_credentials_supplier_func_t encoded_credentials,
    aeron_archive_credentials_challenge_supplier_func_t  on_challenge,
    aeron_archive_credentials_free_func_t on_free,
    void *clientd)
{
    ctx->credentials_supplier.encoded_credentials = encoded_credentials;
    ctx->credentials_supplier.on_challenge = on_challenge;
    ctx->credentials_supplier.on_free = on_free;
    ctx->credentials_supplier.clientd = clientd;

    return 0;
}

int aeron_archive_context_set_recording_signal_consumer(
    aeron_archive_context_t *ctx,
    aeron_archive_recording_signal_consumer_func_t on_recording_signal,
    void *clientd)
{
    ctx->on_recording_signal = on_recording_signal;
    ctx->on_recording_signal_clientd = clientd;

    return 0;
}

int aeron_archive_context_set_error_handler(
    aeron_archive_context_t *ctx,
    aeron_error_handler_t error_handler,
    void *clientd)
{
    ctx->error_handler = error_handler;
    ctx->error_handler_clientd = clientd;

    return 0;
}

int aeron_archive_context_set_delegating_invoker(
    aeron_archive_context_t *ctx,
    aeron_archive_delegating_invoker_func_t delegating_invoker_func,
    void *clientd)
{
    ctx->delegating_invoker_func = delegating_invoker_func;
    ctx->delegating_invoker_func_clientd = clientd;

    return 0;
}
