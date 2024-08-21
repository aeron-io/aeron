/*
 * Copyright 2014-2024 Real Logic Limited.
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

#include "aeron_archive.h"
#include "aeron_archive_context.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

#define AERON_ARCHIVE_CONTEXT_MESSAGE_TIMEOUT_NS_DEFAULT  (10 * 1000 * 1000 * 1000LL) // 10 seconds
#define AERON_ARCHIVE_CONTEXT_CONTROL_REQUEST_CHANNEL_DEFAULT "aeron:udp?endpoint=localhost:8010"
#define AERON_ARCHIVE_CONTEXT_CONTROL_REQUEST_STREAM_ID_DEFAULT (10)
#define AERON_ARCHIVE_CONTEXT_CONTROL_RESPONSE_CHANNEL_DEFAULT "aeron:udp?endpoint=localhost:0"
#define AERON_ARCHIVE_CONTEXT_CONTROL_RESPONSE_STREAM_ID_DEFAULT (20)

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
    aeron_default_path(_ctx->aeron_directory_name, AERON_MAX_PATH - 1);
    _ctx->owns_aeron_client = false;

    snprintf(_ctx->control_request_channel, AERON_MAX_PATH - 1, "%s", AERON_ARCHIVE_CONTEXT_CONTROL_REQUEST_CHANNEL_DEFAULT);
    _ctx->control_request_stream_id = AERON_ARCHIVE_CONTEXT_CONTROL_REQUEST_STREAM_ID_DEFAULT;
    snprintf(_ctx->control_response_channel, AERON_MAX_PATH - 1, "%s", AERON_ARCHIVE_CONTEXT_CONTROL_RESPONSE_CHANNEL_DEFAULT);
    _ctx->control_response_stream_id = AERON_ARCHIVE_CONTEXT_CONTROL_RESPONSE_STREAM_ID_DEFAULT;

    _ctx->message_timeout_ns = AERON_ARCHIVE_CONTEXT_MESSAGE_TIMEOUT_NS_DEFAULT;

    /*
     * TODO here's where we'd read env variables to overwrite the defaults
     */

    *ctx = _ctx;

    return 0;
}

int aeron_archive_context_close(aeron_archive_context_t *ctx)
{
    if (NULL != ctx)
    {
        if (ctx->owns_aeron_client)
        {
            aeron_close(ctx->aeron);
            ctx->aeron = NULL;

            aeron_context_close(ctx->aeron_ctx);
            ctx->aeron_ctx = NULL;
        }

        aeron_free(ctx);
    }

    return 0;
}

int aeron_archive_context_conclude(aeron_archive_context_t *ctx)
{
    if (NULL == ctx->aeron)
    {
        if (aeron_context_init(&ctx->aeron_ctx) < 0 ||
            aeron_context_set_dir(ctx->aeron_ctx, ctx->aeron_directory_name) < 0 ||
            aeron_init(&ctx->aeron, ctx->aeron_ctx) < 0 ||
            aeron_start(ctx->aeron) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto cleanup;
        }

        ctx->owns_aeron_client = true;
    }

    ctx->error_handler = aeron_context_get_error_handler(ctx->aeron_ctx);
    ctx->error_handler_clientd = aeron_context_get_error_handler_clientd(ctx->aeron_ctx);

    return 0;

cleanup:
    if (NULL != ctx->aeron)
    {
        aeron_close(ctx->aeron);
    }

    if (NULL != ctx->aeron_ctx)
    {
        aeron_context_close(ctx->aeron_ctx);
    }

    return -1;
}

int aeron_archive_context_set_message_timeout_ns(aeron_archive_context_t *ctx, int64_t message_timeout_ns)
{
    ctx->message_timeout_ns = message_timeout_ns;

    return 0;
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

int aeron_archive_context_set_control_request_channel(
    aeron_archive_context_t *ctx,
    const char *control_request_channel)
{
    strncpy(ctx->control_request_channel, control_request_channel, sizeof(ctx->control_request_channel));

    return 0;
}

int aeron_archive_context_set_control_response_channel(
    aeron_archive_context_t *ctx,
    const char *control_response_channel)
{
    strncpy(ctx->control_response_channel, control_response_channel, sizeof(ctx->control_response_channel));

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

int32_t aeron_archive_context_get_control_request_stream_id(aeron_archive_context_t *ctx)
{
    return ctx->control_request_stream_id;
}

const char *aeron_archive_context_get_control_request_channel(aeron_archive_context_t *ctx)
{
    return ctx->control_request_channel;
}

void aeron_archive_context_idle(aeron_archive_context_t *ctx)
{
    ctx->idle_strategy_func(ctx->idle_strategy_state, 0);
}

void aeron_archive_context_invoke_aeron_client(aeron_archive_context_t *ctx)
{
    if (aeron_context_get_use_conductor_agent_invoker(ctx->aeron_ctx))
    {
        aeron_main_do_work(ctx->aeron);
    }

    if (NULL != ctx->delegating_invoker_func)
    {
        ctx->delegating_invoker_func(ctx->delegating_invoker_func_clientd);
    }
}
