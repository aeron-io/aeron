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
#include "aeron_archive_async_connect.h"
#include "aeron_archive_context.h"
#include "aeron_archive_client.h"
#include "aeron_archive_recording_signal.h"
#include "aeron_archive_replay_params.h"

#include "aeron_alloc.h"
#include "aeron_agent.h"
#include "aeron_counters.h"
#include "util/aeron_error.h"
#include "uri/aeron_uri_string_builder.h"
#include "command/aeron_control_protocol.h"

#define ENSURE_NOT_REENTRANT_CHECK_RETURN(_aa, _rc) \
do { \
    if ((_aa)->is_in_callback) \
    { \
        AERON_SET_ERR(-AERON_ERROR_CODE_GENERIC_ERROR, "%s", "client cannot be invoked within callback"); \
        return (_rc); \
    } \
} while (0)

static void aeron_archive_handle_control_response_with_error_handler(
    aeron_archive_context_t *ctx, aeron_archive_control_response_poller_t *poller)
{
    aeron_archive_context_invoke_error_handler(
        ctx, poller->correlation_id, (int32_t)poller->relevant_id, poller->error_message);
}

static void aeron_archive_dispatch_recording_signal(aeron_archive_t *aeron_archive)
{
    aeron_archive_control_response_poller_t *poller = aeron_archive->control_response_poller;

    aeron_archive_recording_signal_t signal;

    signal.control_session_id = poller->control_session_id;
    signal.recording_id = poller->recording_id;
    signal.subscription_id = poller->subscription_id;
    signal.position = poller->position;
    signal.recording_signal_code = poller->recording_signal_code;

    aeron_archive->is_in_callback = true;

    aeron_archive_recording_signal_dispatch_signal(aeron_archive->ctx, &signal);

    aeron_archive->is_in_callback = false;
}

static int aeron_archive_poll_next_response(
    aeron_archive_t *aeron_archive,
    const char *operation_name,
    int64_t correlation_id,
    int64_t deadline_ns)
{
    aeron_archive_control_response_poller_t *poller = aeron_archive->control_response_poller;

    while (true)
    {
        int fragments = aeron_archive_control_response_poller_poll(poller);

        if (fragments < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        if (poller->is_poll_complete)
        {
            if (poller->is_recording_signal &&
                poller->control_session_id == aeron_archive->control_session_id)
            {
                aeron_archive_dispatch_recording_signal(aeron_archive);

                continue;
            }

            break;
        }

        if (fragments > 0)
        {
            continue;
        }

        {
            if (!aeron_subscription_is_connected(poller->subscription))
            {
                AERON_SET_ERR(-AERON_ERROR_CODE_GENERIC_ERROR, "%s", "subscription to archive is not connected");
                return -1;
            }
        }

        if (aeron_nano_clock() > deadline_ns)
        {
            AERON_SET_ERR(ETIMEDOUT, "%s awaiting response - correlationId=%" PRIi64, operation_name, correlation_id);
            return -1;
        }

        aeron_archive_idle(aeron_archive);

        aeron_archive_context_invoke_aeron_client(aeron_archive->ctx);
    }

    return 0;
}

static int aeron_archive_poll_for_response_allowing_error(
    bool *success_p,
    aeron_archive_t *aeron_archive,
    const char *operation_name,
    int64_t correlation_id,
    int32_t expected_error_code)
{
    aeron_archive_control_response_poller_t *poller = aeron_archive->control_response_poller;

    int64_t deadline_ns = aeron_nano_clock() + aeron_archive->ctx->message_timeout_ns;

    while (true)
    {
        if (aeron_archive_poll_next_response(
            aeron_archive,
            operation_name,
            correlation_id,
            deadline_ns) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        // make sure the session id matches
        if (poller->control_session_id != aeron_archive->control_session_id)
        {
            aeron_archive_context_invoke_aeron_client(aeron_archive->ctx);
            continue;
        }

        if (poller->is_code_error)
        {
            if (poller->correlation_id == correlation_id)
            {
                if (poller->relevant_id == expected_error_code)
                {
                    *success_p = false;
                    return 0;
                }

                // got an error, and the correlation ids match
                AERON_SET_ERR(
                    -AERON_ERROR_CODE_GENERIC_ERROR,
                    "response for correlationId=%" PRIi64 ", errorCode=%" PRIi64 ", error: %s",
                    correlation_id,
                    poller->relevant_id,
                    poller->error_message);

                return -1;
            }
            else if (NULL != aeron_archive->ctx->error_handler)
            {
                aeron_archive_handle_control_response_with_error_handler(aeron_archive->ctx, poller);
            }
        }
        else if (poller->correlation_id == correlation_id)
        {
            if (!poller->is_code_ok)
            {
                AERON_SET_ERR(-AERON_ERROR_CODE_GENERIC_ERROR, "unexpected response code: %i", poller->code_value);
                return -1;
            }

            *success_p = true;
            return 0;
        }
    }
}

int aeron_archive_poll_for_descriptors(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    const char *operation_name,
    int64_t correlation_id,
    int32_t record_count,
    aeron_archive_recording_descriptor_consumer_func_t recording_descriptor_consumer,
    void *recording_descriptor_consumer_clientd);

int aeron_archive_poll_for_subscription_descriptors(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    const char *operation_name,
    int64_t correlation_id,
    int32_t subscription_count,
    aeron_archive_recording_subscription_descriptor_consumer_func_t recording_subscription_descriptor_consumer,
    void *recording_subscription_descriptor_consumer_clientd);

int aeron_archive_replay_via_response_channel(
    aeron_subscription_t **subscription_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params);

int aeron_archive_start_replay_via_response_channel(
    int64_t *replay_session_id_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params);

int aeron_archive_initiate_replay_via_response_channel(
    int64_t *replay_session_id_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params,
    int64_t subscription_id,
    int64_t deadline_ns);

int aeron_archive_channel_with_session_id(char *out, size_t out_len, const char *in, int32_t session_id);

/* **************** */

int aeron_archive_connect(aeron_archive_t **aeron_archive_p, aeron_archive_context_t *ctx)
{
    aeron_archive_async_connect_t *async;

    if (aeron_archive_async_connect(&async, ctx) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    uint8_t previous_step = aeron_archive_async_connect_step(async);
    if (aeron_archive_async_connect_poll(aeron_archive_p, async) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    while (NULL == *aeron_archive_p)
    {
        if (aeron_archive_async_connect_step(async) == previous_step)
        {
            aeron_archive_context_idle(ctx);
        }
        else
        {
            previous_step = aeron_archive_async_connect_step(async);
        }

        aeron_archive_context_invoke_aeron_client(ctx);

        if (aeron_archive_async_connect_poll(aeron_archive_p, async) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        // _poll returned either 0 or 1
        // 0 leaves *aeron_archive as NULL, which is basically 'continue'
        // 1 sets *aeron_archive, which is basically a 'break'
        // if we sit in this loop for 'too long', eventually _poll will return -1
    }

    return 0;
}

int aeron_archive_create(
    aeron_archive_t **aeron_archive,
    aeron_archive_context_t *ctx,
    aeron_archive_proxy_t *archive_proxy,
    aeron_subscription_t *subscription,
    aeron_archive_control_response_poller_t *control_response_poller,
    aeron_archive_recording_descriptor_poller_t *recording_descriptor_poller,
    aeron_archive_recording_subscription_descriptor_poller_t *recording_subscription_descriptor_poller,
    int64_t control_session_id,
    int64_t archive_id)
{
    aeron_archive_t *_aeron_archive = NULL;

    if (aeron_alloc((void **)&_aeron_archive, sizeof(aeron_archive_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_t");
        return -1;
    }

    _aeron_archive->owns_ctx = true;
    _aeron_archive->ctx = ctx;

    aeron_mutex_init(&_aeron_archive->lock, NULL);

    _aeron_archive->archive_proxy = archive_proxy;
    _aeron_archive->owns_control_response_subscription = true;
    _aeron_archive->subscription = subscription;
    _aeron_archive->control_response_poller = control_response_poller;
    _aeron_archive->recording_descriptor_poller = recording_descriptor_poller;
    _aeron_archive->recording_subscription_descriptor_poller = recording_subscription_descriptor_poller;
    _aeron_archive->control_session_id = control_session_id;
    _aeron_archive->archive_id = archive_id;
    _aeron_archive->is_in_callback = false;

    *aeron_archive = _aeron_archive;
    return 0;
}

int aeron_archive_close(aeron_archive_t *aeron_archive)
{
    if (aeron_exclusive_publication_is_connected(aeron_archive->archive_proxy->exclusive_publication))
    {
        aeron_archive_proxy_close_session(aeron_archive->archive_proxy);
    }

    aeron_archive_proxy_delete(aeron_archive->archive_proxy);
    aeron_archive->archive_proxy = NULL;

    aeron_archive_control_response_poller_close(aeron_archive->control_response_poller);
    aeron_archive->control_response_poller = NULL;

    aeron_archive_recording_descriptor_poller_close(aeron_archive->recording_descriptor_poller);
    aeron_archive->recording_descriptor_poller = NULL;

    aeron_archive_recording_subscription_descriptor_poller_close(aeron_archive->recording_subscription_descriptor_poller);
    aeron_archive->recording_subscription_descriptor_poller = NULL;

    if (aeron_archive->owns_control_response_subscription)
    {
        aeron_subscription_close(aeron_archive->subscription, NULL, NULL);
    }
    aeron_archive->subscription = NULL;

    if (aeron_archive->owns_ctx)
    {
        aeron_archive_context_close(aeron_archive->ctx);
    }
    aeron_archive->ctx = NULL;

    aeron_mutex_destroy(&aeron_archive->lock);

    aeron_free(aeron_archive);

    return 0;
}

void aeron_archive_idle(aeron_archive_t *aeron_archive)
{
    aeron_archive_context_idle(aeron_archive->ctx);
}

aeron_archive_control_response_poller_t *aeron_archive_control_response_poller(aeron_archive_t *aeron_archive)
{
    return aeron_archive->control_response_poller;
}

aeron_archive_proxy_t *aeron_archive_proxy(aeron_archive_t *aeron_archive)
{
    return aeron_archive->archive_proxy;
}

int64_t aeron_archive_control_session_id(aeron_archive_t *aeron_archive)
{
    return aeron_archive->control_session_id;
}

int64_t aeron_archive_next_correlation_id(aeron_archive_t *aeron_archive)
{
    return aeron_next_correlation_id(aeron_archive->ctx->aeron);
}

int aeron_archive_poll_for_recording_signals(int32_t *count_p, aeron_archive_t *aeron_archive)
{
    aeron_mutex_lock(&aeron_archive->lock);

    int32_t count = 0;
    int rc = 0;

    aeron_archive_control_response_poller_t *poller = aeron_archive->control_response_poller;

    if (aeron_archive_control_response_poller_poll(poller) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (poller->is_poll_complete &&
        poller->control_session_id == aeron_archive->control_session_id)
    {
        if (poller->is_control_response && poller->is_code_error)
        {
            if (NULL == aeron_archive->ctx->error_handler)
            {
                AERON_SET_ERR(
                    -AERON_ERROR_CODE_GENERIC_ERROR,
                    "response for correlationId=%" PRIi64 ", errorCode=%" PRIi64 ", error: %s",
                    poller->correlation_id,
                    poller->relevant_id,
                    poller->error_message);
                rc = -1;
            }
            else
            {
               aeron_archive_handle_control_response_with_error_handler(aeron_archive->ctx, poller);
            }
        }
        else if (poller->is_recording_signal)
        {
            aeron_archive_dispatch_recording_signal(aeron_archive);

            count++;
        }
    }

    aeron_mutex_unlock(&aeron_archive->lock);

    if (NULL != count_p)
    {
        *count_p = count;
    }

    return rc;
}

int aeron_archive_poll_for_error_response(aeron_archive_t *aeron_archive, char *buffer, size_t buffer_length)
{
    int rc = 0;

    aeron_mutex_lock(&aeron_archive->lock);

    if (!aeron_subscription_is_connected(aeron_archive->subscription))
    {
        AERON_SET_ERR(-AERON_ERROR_CODE_GENERIC_ERROR, "%s", "subscription to archive is not connected");
        rc = -1;
        goto cleanup;
    }

    aeron_archive_control_response_poller_t *poller = aeron_archive->control_response_poller;

    if (aeron_archive_control_response_poller_poll(poller) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
        goto cleanup;
    }

    if (poller->is_poll_complete &&
        poller->control_session_id == aeron_archive->control_session_id)
    {
        if (poller->is_control_response &&
            poller->is_code_error)
        {
            snprintf(buffer, buffer_length, "%s", poller->error_message);

            goto cleanup;
        }
        else if (poller->is_recording_signal)
        {
            aeron_archive_dispatch_recording_signal(aeron_archive);
        }
    }

    if (buffer_length > 0)
    {
        buffer[0] = '\0';
    }

cleanup:
    aeron_mutex_unlock(&aeron_archive->lock);

    return rc;
}

int aeron_archive_check_for_error_response(aeron_archive_t *aeron_archive)
{
    aeron_mutex_lock(&aeron_archive->lock);

    int rc = 0;

    if (!aeron_subscription_is_connected(aeron_archive->subscription))
    {
        if (NULL == aeron_archive->ctx->error_handler)
        {
            AERON_SET_ERR(-AERON_ERROR_CODE_GENERIC_ERROR, "%s", "not connected");
            rc = -1;
        }
        else
        {
            aeron_archive->ctx->error_handler(
                aeron_archive->ctx->error_handler_clientd,
                AERON_ERROR_CODE_GENERIC_ERROR,
                "not connected");
        }

        goto cleanup;
    }

    aeron_archive_control_response_poller_t *poller = aeron_archive->control_response_poller;

    if (aeron_archive_control_response_poller_poll(poller) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
        goto cleanup;
    }

    if (poller->is_poll_complete &&
        poller->control_session_id == aeron_archive->control_session_id)
    {
        if (poller->is_control_response &&
            poller->is_code_error)
        {
            if (NULL == aeron_archive->ctx->error_handler)
            {
                AERON_SET_ERR(
                    -AERON_ERROR_CODE_GENERIC_ERROR,
                    "response for correlationId=%" PRIi64 ", errorCode=%" PRIi64 ", error: %s",
                    poller->correlation_id,
                    poller->relevant_id,
                    poller->error_message);

                rc = -1;
            }
            else
            {
               aeron_archive_handle_control_response_with_error_handler(aeron_archive->ctx, poller);
            }
        }
        else if (poller->is_recording_signal)
        {
            aeron_archive_dispatch_recording_signal(aeron_archive);
        }
    }

cleanup:
    aeron_mutex_unlock(&aeron_archive->lock);

    return rc;
}

int aeron_archive_add_recorded_publication(
    aeron_publication_t **publication_p,
    aeron_archive_t *aeron_archive,
    const char *channel,
    int32_t stream_id)
{
    aeron_async_add_publication_t *async;
    aeron_publication_t *_publication = NULL;

    if (aeron_async_add_publication(
        &async,
        aeron_archive->ctx->aeron,
        channel,
        stream_id) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (aeron_async_add_publication_poll(&_publication, async) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }
    while (NULL == _publication)
    {
        aeron_archive_idle(aeron_archive);

        if (aeron_async_add_publication_poll(&_publication, async) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
    }

    aeron_publication_constants_t constants;
    aeron_publication_constants(_publication, &constants);
    if (constants.original_registration_id != constants.registration_id)
    {
        // not original
        AERON_SET_ERR(-AERON_ERROR_CODE_GENERIC_ERROR, "publication already added for channel=%s streamId=%" PRIi32, channel, stream_id);
        return -1;
    }

    char *recording_channel;
    size_t recording_channel_len = strlen(channel) + 50; // add enough space for an additional session id
    aeron_alloc((void **)&recording_channel, recording_channel_len);

    int rc = 0;

    if (aeron_archive_channel_with_session_id(
        recording_channel,
        recording_channel_len,
        channel,
        constants.session_id) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
        goto cleanup;
    }

    if (aeron_archive_start_recording(
        NULL,
        aeron_archive,
        recording_channel,
        stream_id,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
        goto cleanup;
    }

    *publication_p = _publication;

cleanup:
    aeron_free(recording_channel);
    return rc;
}

int aeron_archive_add_recorded_exclusive_publication(
    aeron_exclusive_publication_t **exclusive_publication_p,
    aeron_archive_t *aeron_archive,
    const char *channel,
    int32_t stream_id)
{
    aeron_async_add_exclusive_publication_t *async;
    aeron_exclusive_publication_t *_exclusive_publication = NULL;

    if (aeron_async_add_exclusive_publication(
        &async,
        aeron_archive->ctx->aeron,
        channel,
        stream_id) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (aeron_async_add_exclusive_publication_poll(&_exclusive_publication, async) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }
    while (NULL == _exclusive_publication)
    {
        aeron_archive_idle(aeron_archive);

        if (aeron_async_add_exclusive_publication_poll(&_exclusive_publication, async) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
    }

    aeron_publication_constants_t constants;
    aeron_exclusive_publication_constants(_exclusive_publication, &constants);

    char *recording_channel;
    size_t recording_channel_len = strlen(channel) + 50; // add enough space for an additional session id
    aeron_alloc((void **)&recording_channel, recording_channel_len);

    int rc = 0;

    if (aeron_archive_channel_with_session_id(
        recording_channel,
        recording_channel_len,
        channel,
        constants.session_id) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
        goto cleanup;
    }

    if (aeron_archive_start_recording(
        NULL,
        aeron_archive,
        recording_channel,
        stream_id,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
        goto cleanup;
    }

    *exclusive_publication_p = _exclusive_publication;

cleanup:
    aeron_free(recording_channel);
    return rc;
}

int aeron_archive_start_recording(
    int64_t *subscription_id_p,
    aeron_archive_t *aeron_archive,
    const char *recording_channel,
    int32_t recording_stream_id,
    aeron_archive_source_location_t source_location,
    bool auto_stop)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_start_recording(
        aeron_archive->archive_proxy,
        recording_channel,
        recording_stream_id,
        source_location == AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        auto_stop,
        correlation_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response(
            subscription_id_p,
            aeron_archive,
            "AeronArchive::startRecording",
            correlation_id);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_get_recording_position(
    int64_t *recording_position_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_get_recording_position(
        aeron_archive->archive_proxy,
        correlation_id,
        recording_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response(
            recording_position_p,
            aeron_archive,
            "AeronArchive::getRecordingPosition",
            correlation_id);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_get_start_position(
    int64_t *start_position_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_get_start_position(
        aeron_archive->archive_proxy,
        correlation_id,
        recording_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response(
            start_position_p,
            aeron_archive,
            "AeronArchive::getStartPosition",
            correlation_id);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_get_stop_position(
    int64_t *stop_position_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_get_stop_position(
        aeron_archive->archive_proxy,
        correlation_id,
        recording_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response(
            stop_position_p,
            aeron_archive,
            "AeronArchive::getStopPosition",
            correlation_id);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_get_max_recorded_position(
    int64_t *max_recorded_position_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_get_max_recorded_position(
        aeron_archive->archive_proxy,
        correlation_id,
        recording_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response(
            max_recorded_position_p,
            aeron_archive,
            "AeronArchive::getMaxRecordedPosition",
            correlation_id);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_stop_recording_subscription(
    aeron_archive_t *aeron_archive,
    int64_t subscription_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_stop_recording_subscription(
        aeron_archive->archive_proxy,
        correlation_id,
        subscription_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response(
            NULL,
            aeron_archive,
            "AeronArchive::stopRecordingSubscription",
            correlation_id);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_try_stop_recording_subscription(
    bool *stopped_p,
    aeron_archive_t *aeron_archive,
    int64_t subscription_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_stop_recording_subscription(
        aeron_archive->archive_proxy,
        correlation_id,
        subscription_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response_allowing_error(
            stopped_p,
            aeron_archive,
            "AeronArchive::tryStopRecordingSubscription",
            correlation_id,
            ARCHIVE_ERROR_CODE_UNKNOWN_SUBSCRIPTION);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_stop_recording_channel_and_stream(
    aeron_archive_t *aeron_archive,
    const char *channel,
    int32_t stream_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_stop_recording(
        aeron_archive->archive_proxy,
        correlation_id,
        channel,
        stream_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response(
            NULL,
            aeron_archive,
            "AeronArchive::stopRecording",
            correlation_id);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_try_stop_recording_channel_and_stream(
    bool *stopped_p,
    aeron_archive_t *aeron_archive,
    const char *channel,
    int32_t stream_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_stop_recording(
        aeron_archive->archive_proxy,
        correlation_id,
        channel,
        stream_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response_allowing_error(
            stopped_p,
            aeron_archive,
            "AeronArchive::tryStopRecordingChannelAndStream",
            correlation_id,
            ARCHIVE_ERROR_CODE_UNKNOWN_SUBSCRIPTION);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_try_stop_recording_by_identity(
    bool *stopped_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_stop_recording_by_identity(
        aeron_archive->archive_proxy,
        correlation_id,
        recording_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        int64_t relevant_id;
        rc = aeron_archive_poll_for_response(
            &relevant_id,
            aeron_archive,
            "AeronArchive::tryStopRecordingByIdentity",
            correlation_id);

        if (rc == 0)
        {
            *stopped_p = relevant_id != 0;
        }
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_stop_recording_publication_constants(
    aeron_archive_t *aeron_archive,
    aeron_publication_constants_t *constants)
{
    char *recording_channel;
    size_t recording_channel_len = strlen(constants->channel) + 50; // add enough space for an additional session id
    aeron_alloc((void **)&recording_channel, recording_channel_len);

    int rc;

    if (aeron_archive_channel_with_session_id(
        recording_channel,
        recording_channel_len,
        constants->channel,
        constants->session_id) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_stop_recording_channel_and_stream(
            aeron_archive,
            recording_channel,
            constants->stream_id);
    }

    aeron_free(recording_channel);
    return rc;
}

int aeron_archive_stop_recording_publication(
    aeron_archive_t *aeron_archive,
    aeron_publication_t *publication)
{
    aeron_publication_constants_t constants;

    aeron_publication_constants(publication, &constants);

    return aeron_archive_stop_recording_publication_constants(aeron_archive, &constants);
}

int aeron_archive_stop_recording_exclusive_publication(
    aeron_archive_t *aeron_archive,
    aeron_exclusive_publication_t *exclusive_publication)
{
    aeron_publication_constants_t constants;

    aeron_exclusive_publication_constants(exclusive_publication, &constants);

    return aeron_archive_stop_recording_publication_constants(aeron_archive, &constants);
}

int aeron_archive_find_last_matching_recording(
    int64_t *recording_id_p,
    aeron_archive_t *aeron_archive,
    int64_t min_recording_id,
    const char *channel_fragment,
    int32_t stream_id,
    int32_t session_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_find_last_matching_recording(
        aeron_archive->archive_proxy,
        correlation_id,
        min_recording_id,
        channel_fragment,
        stream_id,
        session_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response(
            recording_id_p,
            aeron_archive,
            "AeronArchive::findLastMatchingRecording",
            correlation_id);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_list_recording(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    aeron_archive_recording_descriptor_consumer_func_t recording_descriptor_consumer,
    void *recording_descriptor_consumer_clientd)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_list_recording(
        aeron_archive->archive_proxy,
        correlation_id,
        recording_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        aeron_archive->is_in_callback = true;

        rc = aeron_archive_poll_for_descriptors(
            count_p,
            aeron_archive,
            "AeronArchive::listRecording",
            correlation_id,
            1,
            recording_descriptor_consumer,
            recording_descriptor_consumer_clientd);

        aeron_archive->is_in_callback = false;
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_list_recordings(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t from_recording_id,
    int32_t record_count,
    aeron_archive_recording_descriptor_consumer_func_t recording_descriptor_consumer,
    void *recording_descriptor_consumer_clientd)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_list_recordings(
        aeron_archive->archive_proxy,
        correlation_id,
        from_recording_id,
        record_count))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        aeron_archive->is_in_callback = true;

        rc = aeron_archive_poll_for_descriptors(
            count_p,
            aeron_archive,
            "AeronArchive::listRecordings",
            correlation_id,
            record_count,
            recording_descriptor_consumer,
            recording_descriptor_consumer_clientd);

        aeron_archive->is_in_callback = false;
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_list_recordings_for_uri(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t from_recording_id,
    int32_t record_count,
    const char *channel_fragment,
    int32_t stream_id,
    aeron_archive_recording_descriptor_consumer_func_t recording_descriptor_consumer,
    void *recording_descriptor_consumer_clientd)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_list_recordings_for_uri(
        aeron_archive->archive_proxy,
        correlation_id,
        from_recording_id,
        record_count,
        channel_fragment,
        stream_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        aeron_archive->is_in_callback = true;

        rc = aeron_archive_poll_for_descriptors(
            count_p,
            aeron_archive,
            "AeronArchive::listRecordingsForUri",
            correlation_id,
            record_count,
            recording_descriptor_consumer,
            recording_descriptor_consumer_clientd);

        aeron_archive->is_in_callback = false;
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

// called by _start_replay with the lock already held
static int aeron_archive_start_replay_locked(
    int64_t *replay_session_id_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params)
{
    {
        aeron_uri_string_builder_t builder;

        if (aeron_uri_string_builder_init_on_string(&builder, replay_channel) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        const char *control_mode = aeron_uri_string_builder_get(&builder, AERON_UDP_CHANNEL_CONTROL_MODE_KEY);

        bool has_control_response_mode =
            NULL != control_mode &&
                strcmp(control_mode, AERON_UDP_CHANNEL_CONTROL_MODE_RESPONSE_VALUE) == 0;

        aeron_uri_string_builder_close(&builder);

        if (has_control_response_mode)
        {
            int rc = aeron_archive_start_replay_via_response_channel(
                replay_session_id_p,
                aeron_archive,
                recording_id,
                replay_channel,
                replay_stream_id,
                params);

            if (rc < 0)
            {
                AERON_APPEND_ERR("%s", "");
            }

            return rc;
        }
    }

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);

    if (!aeron_archive_proxy_replay(
        aeron_archive->archive_proxy,
        correlation_id,
        recording_id,
        replay_channel,
        replay_stream_id,
        params))
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    int rc = aeron_archive_poll_for_response(
        replay_session_id_p,
        aeron_archive,
        "AeronArchive::startReplay",
        correlation_id);

    return rc;
}

int aeron_archive_start_replay(
    int64_t *replay_session_id_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int rc = aeron_archive_start_replay_locked(replay_session_id_p, aeron_archive, recording_id, replay_channel, replay_stream_id, params);

    aeron_mutex_unlock(&aeron_archive->lock);

    return rc;
}

// called by _replay with the lock already held
static int aeron_archive_replay_locked(
    aeron_subscription_t **subscription_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params)
{
    {
        aeron_uri_string_builder_t builder;

        if (aeron_uri_string_builder_init_on_string(&builder, replay_channel) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        const char *control_mode = aeron_uri_string_builder_get(&builder, AERON_UDP_CHANNEL_CONTROL_MODE_KEY);

        bool has_control_response_mode =
            NULL != control_mode &&
            strcmp(control_mode, AERON_UDP_CHANNEL_CONTROL_MODE_RESPONSE_VALUE) == 0;

        aeron_uri_string_builder_close(&builder);

        if (has_control_response_mode)
        {
            int rc = aeron_archive_replay_via_response_channel(
                subscription_p,
                aeron_archive,
                recording_id,
                replay_channel,
                replay_stream_id,
                params);

            if (rc < 0)
            {
                AERON_APPEND_ERR("%s", "");
            }

            return rc;
        }
    }

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);

    if (!aeron_archive_proxy_replay(
        aeron_archive->archive_proxy,
        correlation_id,
        recording_id,
        replay_channel,
        replay_stream_id,
        params))
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    int64_t replay_session_id;

    if (aeron_archive_poll_for_response(
        &replay_session_id,
        aeron_archive,
        "AeronArchive::replay",
        correlation_id) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    char *replay_channel_with_sid;
    size_t replay_channel_with_sid_len = strlen(replay_channel) + 50; // add enough space for an additional session id
    aeron_alloc((void **)&replay_channel_with_sid, replay_channel_with_sid_len);

    int rc = 0;

    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_on_string(&builder, replay_channel);
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_SESSION_ID_KEY, (int32_t)replay_session_id);
        aeron_uri_string_builder_sprint(&builder, replay_channel_with_sid, replay_channel_with_sid_len);
        aeron_uri_string_builder_close(&builder);
    }

    aeron_async_add_subscription_t *async_add_subscription;
    aeron_subscription_t *subscription = NULL;

    if (aeron_async_add_subscription(
        &async_add_subscription,
        aeron_archive->ctx->aeron,
        replay_channel_with_sid,
        replay_stream_id,
        NULL,
        NULL,
        NULL,
        NULL) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
        goto cleanup;
    }

    if (aeron_async_add_subscription_poll(&subscription, async_add_subscription) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
        goto cleanup;
    }

    while (NULL == subscription)
    {
        aeron_archive_idle(aeron_archive);

        if (aeron_async_add_subscription_poll(&subscription, async_add_subscription) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            rc = -1;
            goto cleanup;
        }
    }

    *subscription_p = subscription;

cleanup:
    aeron_free(replay_channel_with_sid);
    return rc;
}

int aeron_archive_replay(
    aeron_subscription_t **subscription_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int rc = aeron_archive_replay_locked(subscription_p, aeron_archive, recording_id, replay_channel, replay_stream_id, params);

    aeron_mutex_unlock(&aeron_archive->lock);

    return rc;
}

int aeron_archive_truncate_recording(
    int64_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    int64_t position)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_truncate_recording(
        aeron_archive->archive_proxy,
        correlation_id,
        recording_id,
        position))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response(
            count_p,
            aeron_archive,
            "AeronArchive::truncateRecording",
            correlation_id);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_stop_replay(
    aeron_archive_t *aeron_archive,
    int64_t replay_session_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_stop_replay(
        aeron_archive->archive_proxy,
        correlation_id,
        replay_session_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response(
            NULL,
            aeron_archive,
            "AeronArchive::stopReplay",
            correlation_id);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_stop_all_replays(
    aeron_archive_t *aeron_archive,
    int64_t recording_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_stop_all_replays(
        aeron_archive->archive_proxy,
        correlation_id,
        recording_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response(
            NULL,
            aeron_archive,
            "AeronArchive::stopAllReplays",
            correlation_id);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_list_recording_subscriptions(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    int32_t pseudo_index,
    int32_t subscription_count,
    const char *channel_fragment,
    int32_t stream_id,
    bool apply_stream_id,
    aeron_archive_recording_subscription_descriptor_consumer_func_t recording_subscription_descriptor_consumer,
    void *recording_subscription_descriptor_consumer_clientd)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_list_recording_subscriptions(
        aeron_archive->archive_proxy,
        correlation_id,
        pseudo_index,
        subscription_count,
        channel_fragment,
        stream_id,
        apply_stream_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        aeron_archive->is_in_callback = true;

        rc = aeron_archive_poll_for_subscription_descriptors(
            count_p,
            aeron_archive,
            "AeronArchive::listRecordingSubscriptions",
            correlation_id,
            subscription_count,
            recording_subscription_descriptor_consumer,
            recording_subscription_descriptor_consumer_clientd);

        aeron_archive->is_in_callback = false;
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_purge_recording(
    int64_t *deleted_segments_count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_purge_recording(
        aeron_archive->archive_proxy,
        correlation_id,
        recording_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response(
            deleted_segments_count_p,
            aeron_archive,
            "AeronArchive::purgeRecording",
            correlation_id);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_extend_recording(
    int64_t *subscription_id_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    const char *recording_channel,
    int32_t recording_stream_id,
    aeron_archive_source_location_t source_location,
    bool auto_stop)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_extend_recording(
        aeron_archive->archive_proxy,
        recording_id,
        recording_channel,
        recording_stream_id,
        source_location == AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        auto_stop,
        correlation_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response(
            subscription_id_p,
            aeron_archive,
            "AeronArchive::extendRecording",
            correlation_id);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_replicate(
    int64_t *replication_id_p,
    aeron_archive_t *aeron_archive,
    int64_t src_recording_id,
    const char *src_control_channel,
    int32_t src_control_stream_id,
    aeron_archive_replication_params_t *params)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_replicate(
        aeron_archive->archive_proxy,
        correlation_id,
        src_recording_id,
        src_control_stream_id,
        src_control_channel,
        params))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response(
            replication_id_p,
            aeron_archive,
            "AeronArchive::replicate",
            correlation_id);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_stop_replication(
    aeron_archive_t *aeron_archive,
    int64_t replication_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_stop_replication(
        aeron_archive->archive_proxy,
        correlation_id,
        replication_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response(
            NULL,
            aeron_archive,
            "AeronArchive::stopReplication",
            correlation_id);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_try_stop_replication(
    bool *stopped_p,
    aeron_archive_t *aeron_archive,
    int64_t replication_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_stop_replication(
        aeron_archive->archive_proxy,
        correlation_id,
        replication_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response_allowing_error(
            stopped_p,
            aeron_archive,
            "AeronArchive::tryStopReplication",
            correlation_id,
            ARCHIVE_ERROR_CODE_UNKNOWN_SUBSCRIPTION);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_detach_segments(
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    int64_t new_start_position)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_detach_segments(
        aeron_archive->archive_proxy,
        correlation_id,
        recording_id,
        new_start_position))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response(
            NULL,
            aeron_archive,
            "AeronArchive::detachSegments",
            correlation_id);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_delete_detached_segments(
    int64_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_delete_detached_segments(
        aeron_archive->archive_proxy,
        correlation_id,
        recording_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response(
            count_p,
            aeron_archive,
            "AeronArchive::deleteDetachedSegments",
            correlation_id);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_purge_segments(
    int64_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    int64_t new_start_position)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_purge_segments(
        aeron_archive->archive_proxy,
        correlation_id,
        recording_id,
        new_start_position))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response(
            count_p,
            aeron_archive,
            "AeronArchive::purgeSegments",
            correlation_id);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_attach_segments(
    int64_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_attach_segments(
        aeron_archive->archive_proxy,
        correlation_id,
        recording_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response(
            count_p,
            aeron_archive,
            "AeronArchive::attachSegments",
            correlation_id);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int aeron_archive_migrate_segments(
    int64_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t src_recording_id,
    int64_t dst_recording_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);
    int rc;

    if (!aeron_archive_proxy_migrate_segments(
        aeron_archive->archive_proxy,
        correlation_id,
        src_recording_id,
        dst_recording_id))
    {
        AERON_APPEND_ERR("%s", "");
        rc = -1;
    }
    else
    {
        rc = aeron_archive_poll_for_response(
            count_p,
            aeron_archive,
            "AeronArchive::migrateSegments",
            correlation_id);
    }

    aeron_mutex_unlock(&aeron_archive->lock);
    return rc;
}

int64_t aeron_archive_segment_file_base_position(
    int64_t start_position,
    int64_t position,
    int32_t term_buffer_length,
    int32_t segment_file_length)
{
    int64_t start_term_base_position = start_position - (start_position & (term_buffer_length - 1));
    int64_t length_from_base_position = position - start_term_base_position;
    int64_t segments = (length_from_base_position - (length_from_base_position & (segment_file_length - 1)));

    return start_term_base_position + segments;
}

aeron_archive_context_t *aeron_archive_get_archive_context(aeron_archive_t *aeron_archive)
{
    return aeron_archive->ctx;
}

aeron_archive_context_t *aeron_archive_get_and_own_archive_context(aeron_archive_t *aeron_archive)
{
    aeron_archive->owns_ctx = false;

    return aeron_archive->ctx;
}

int64_t aeron_archive_get_archive_id(aeron_archive_t *aeron_archive)
{
    return aeron_archive->archive_id;
}

aeron_subscription_t *aeron_archive_get_control_response_subscription(aeron_archive_t *aeron_archive)
{
    return aeron_archive->subscription;
}

aeron_subscription_t *aeron_archive_get_and_own_control_response_subscription(aeron_archive_t *aeron_archive)
{
    aeron_archive->owns_control_response_subscription = false;

    return aeron_archive->subscription;
}

/* **************** */

int aeron_archive_poll_for_response(
    int64_t *relevant_id_p,
    aeron_archive_t *aeron_archive,
    const char *operation_name,
    int64_t correlation_id)
{
    aeron_archive_control_response_poller_t *poller = aeron_archive->control_response_poller;

    int64_t deadline_ns = aeron_nano_clock() + aeron_archive->ctx->message_timeout_ns;

    while (true)
    {
        if (aeron_archive_poll_next_response(
            aeron_archive,
            operation_name,
            correlation_id,
            deadline_ns) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        // make sure the session id matches
        if (poller->control_session_id != aeron_archive->control_session_id)
        {
            aeron_archive_context_invoke_aeron_client(aeron_archive->ctx);
            continue;
        }

        if (poller->is_code_error)
        {
            if (poller->correlation_id == correlation_id)
            {
                // got an error, and the correlation ids match
                AERON_SET_ERR(
                    -AERON_ERROR_CODE_GENERIC_ERROR,
                    "response for correlationId=%" PRIi64 ", errorCode=%" PRIi64 ", error: %s",
                    correlation_id,
                    poller->relevant_id,
                    poller->error_message);
                return -1;
            }
            else if (NULL != aeron_archive->ctx->error_handler)
            {
               aeron_archive_handle_control_response_with_error_handler(aeron_archive->ctx, poller);
            }
        }
        else if (poller->correlation_id == correlation_id)
        {
            if (!poller->is_code_ok)
            {
                AERON_SET_ERR(-AERON_ERROR_CODE_GENERIC_ERROR, "unexpected response code: %i", poller->code_value);
                return -1;
            }

            if (NULL != relevant_id_p)
            {
                *relevant_id_p = poller->relevant_id;
            }

            return 0;
        }
    }
}

int aeron_archive_poll_for_descriptors(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    const char *operation_name,
    int64_t correlation_id,
    int32_t record_count,
    aeron_archive_recording_descriptor_consumer_func_t recording_descriptor_consumer,
    void *recording_descriptor_consumer_clientd)
{
    aeron_archive_recording_descriptor_poller_t *poller = aeron_archive->recording_descriptor_poller;

    int32_t existing_remain_count = record_count;

    int64_t deadline_ns = aeron_nano_clock() + aeron_archive->ctx->message_timeout_ns;

    aeron_archive_recording_descriptor_poller_reset(
        poller,
        correlation_id,
        record_count,
        recording_descriptor_consumer,
        recording_descriptor_consumer_clientd);

    while (true)
    {
        const int fragments = aeron_archive_recording_descriptor_poller_poll(poller);

        if (fragments < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        const int32_t remaining_record_count = poller->remaining_record_count;

        if (poller->is_dispatch_complete)
        {
            *count_p = record_count - remaining_record_count;

            return 0;
        }

        if (remaining_record_count != existing_remain_count)
        {
            existing_remain_count = remaining_record_count;

            deadline_ns = aeron_nano_clock() + aeron_archive->ctx->message_timeout_ns;
        }

        aeron_archive_context_invoke_aeron_client(aeron_archive->ctx);

        if (fragments > 0)
        {
            continue;
        }

        if (!aeron_subscription_is_connected(aeron_archive->subscription))
        {
            AERON_SET_ERR(-AERON_ERROR_CODE_GENERIC_ERROR, "%s", "not connected");
            return -1;
        }

        if (aeron_nano_clock() > deadline_ns)
        {
            AERON_SET_ERR(ETIMEDOUT, "%s awaiting recording descriptors - correlationId=%" PRIi64, operation_name, correlation_id);
            return -1;
        }

        aeron_archive_idle(aeron_archive);
    }
}

int aeron_archive_poll_for_subscription_descriptors(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    const char *operation_name,
    int64_t correlation_id,
    int32_t subscription_count,
    aeron_archive_recording_subscription_descriptor_consumer_func_t recording_subscription_descriptor_consumer,
    void *recording_subscription_descriptor_consumer_clientd)
{
    aeron_archive_recording_subscription_descriptor_poller_t *poller = aeron_archive->recording_subscription_descriptor_poller;

    int32_t existing_remain_count = subscription_count;

    int64_t deadline_ns = aeron_nano_clock() + aeron_archive->ctx->message_timeout_ns;

    aeron_archive_recording_subscription_descriptor_poller_reset(
        poller,
        correlation_id,
        subscription_count,
        recording_subscription_descriptor_consumer,
        recording_subscription_descriptor_consumer_clientd);

    while (true)
    {
        const int fragments = aeron_archive_recording_subscription_descriptor_poller_poll(poller);

        if (fragments < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        const int32_t remaining_subscription_count = poller->remaining_subscription_count;

        if (poller->is_dispatch_complete)
        {
            *count_p = subscription_count - remaining_subscription_count;

            return 0;
        }

        if (remaining_subscription_count != existing_remain_count)
        {
            existing_remain_count = remaining_subscription_count;

            // we made progress, so update the deadline_ns
            deadline_ns = aeron_nano_clock() + aeron_archive->ctx->message_timeout_ns;
        }

        aeron_archive_context_invoke_aeron_client(aeron_archive->ctx);

        if (fragments > 0)
        {
            continue;
        }

        if (!aeron_subscription_is_connected(aeron_archive->subscription))
        {
            AERON_SET_ERR(-AERON_ERROR_CODE_GENERIC_ERROR, "%s", "subscription is not connected");
            return -1;
        }

        if (aeron_nano_clock() > deadline_ns)
        {
            AERON_SET_ERR(ETIMEDOUT, "%s awaiting recording descriptors - correlationId=%" PRIi64, operation_name, correlation_id);
            return -1;
        }

        aeron_archive_idle(aeron_archive);
    }
}

int aeron_archive_replay_via_response_channel(
    aeron_subscription_t **subscription_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params)
{
    int64_t deadline_ns = aeron_nano_clock() + aeron_archive->ctx->message_timeout_ns;

    aeron_async_add_subscription_t *async;
    if (aeron_async_add_subscription(
        &async,
        aeron_archive->ctx->aeron,
        replay_channel,
        replay_stream_id,
        NULL, NULL, NULL, NULL) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    int64_t subscription_id = aeron_async_add_subscription_get_registration_id(async);

    aeron_subscription_t *replay_subscription = NULL;
    if (aeron_async_add_subscription_poll(&replay_subscription, async) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    while (NULL == replay_subscription)
    {
        if (aeron_nano_clock() > deadline_ns)
        {
            AERON_SET_ERR(ETIMEDOUT, "%s", "waiting for subscription to be created");
            return -1;
        }

        aeron_archive_idle(aeron_archive);

        if (aeron_async_add_subscription_poll(&replay_subscription, async) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
    }

    if (aeron_archive_initiate_replay_via_response_channel(
        NULL,
        aeron_archive,
        recording_id,
        replay_channel,
        replay_stream_id,
        params,
        subscription_id,
        deadline_ns) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    while (!aeron_subscription_is_connected(replay_subscription))
    {
        if (aeron_nano_clock() > deadline_ns)
        {
            AERON_SET_ERR(ETIMEDOUT, "%s", "waiting for subscription to be connected");
            return -1;
        }

        aeron_archive_idle(aeron_archive);
    }

    *subscription_p = replay_subscription;

    return 0;
}

int aeron_archive_start_replay_via_response_channel(
    int64_t *replay_session_id_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params)
{
    if (AERON_NULL_VALUE == params->subscription_registration_id)
    {
        AERON_SET_ERR(-AERON_ERROR_CODE_GENERIC_ERROR, "%s", "must supply a valid subscription registration id");
        return -1;
    }

    int64_t deadline_ns = aeron_nano_clock() + aeron_archive->ctx->message_timeout_ns;

    if (aeron_archive_initiate_replay_via_response_channel(
        replay_session_id_p,
        aeron_archive,
        recording_id,
        replay_channel,
        replay_stream_id,
        params,
        params->subscription_registration_id,
        deadline_ns) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    return 0;
}

int aeron_archive_initiate_replay_via_response_channel(
    int64_t *replay_session_id_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params,
    int64_t subscription_id,
    int64_t deadline_ns)
{
    char *control_request_channel = NULL;
    int rc = -1;

    // acquire a replay token, and load it up into a copy of the params
    aeron_archive_replay_params_t response_channel_replay_params;
    aeron_archive_replay_params_copy(&response_channel_replay_params, params);

    int64_t correlation_id = aeron_archive_next_correlation_id(aeron_archive);

    if (!aeron_archive_request_replay_token(
        aeron_archive->archive_proxy,
        correlation_id,
        recording_id))
    {
        AERON_APPEND_ERR("%s", "");
        goto cleanup;
    }

    if (aeron_archive_poll_for_response(
        &response_channel_replay_params.replay_token,
        aeron_archive,
        "AeronArchive::replayToken",
        correlation_id) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto cleanup;
    }

    // update the control request channel uri with the subscription id of the response channel
    // add enough space for an additional k/v pairs:
    size_t control_request_channel_len = strlen(aeron_archive->ctx->control_request_channel) + 100;
    aeron_alloc((void **)&control_request_channel, control_request_channel_len);

    aeron_uri_string_builder_t builder;

    bool failure = aeron_uri_string_builder_init_on_string(&builder, aeron_archive->ctx->control_request_channel) < 0 ||
        aeron_uri_string_builder_put(&builder, AERON_URI_TERM_OFFSET_KEY, NULL) < 0 ||
        aeron_uri_string_builder_put(&builder, AERON_URI_TERM_ID_KEY, NULL) < 0 ||
        aeron_uri_string_builder_put(&builder, AERON_URI_INITIAL_TERM_ID_KEY, NULL) < 0 ||
        aeron_uri_string_builder_put_int64(&builder, AERON_URI_RESPONSE_CORRELATION_ID_KEY, subscription_id) < 0 ||
        aeron_uri_string_builder_put(&builder, AERON_URI_TERM_LENGTH_KEY, "64k") < 0 ||
        aeron_uri_string_builder_put(&builder, AERON_URI_SPIES_SIMULATE_CONNECTION_KEY, "false") < 0 ||
        aeron_uri_string_builder_sprint(&builder, control_request_channel, control_request_channel_len) < 0;

    aeron_uri_string_builder_close(&builder);

    if (failure)
    {
        AERON_APPEND_ERR("%s", "");
        goto cleanup;
    }

    // create an exclusive publication that is the 'request' channel that will trigger the creation of the 'response' channel
    aeron_async_add_exclusive_publication_t *async;
    if (aeron_async_add_exclusive_publication(
        &async,
        aeron_archive->ctx->aeron,
        control_request_channel,
        aeron_archive->ctx->control_request_stream_id) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto cleanup;
    }

    int64_t publication_id = aeron_async_add_exclusive_exclusive_publication_get_registration_id(async);

    aeron_exclusive_publication_t *exclusive_publication;
    if (aeron_async_add_exclusive_publication_poll(&exclusive_publication, async) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto cleanup;
    }

    while (NULL == exclusive_publication)
    {
        if (aeron_nano_clock() > deadline_ns)
        {
            AERON_SET_ERR(ETIMEDOUT, "%s", "waiting for publication to be created");
            goto cleanup;
        }

        aeron_archive_idle(aeron_archive);

        if (aeron_async_add_exclusive_publication_poll(&exclusive_publication, async) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto cleanup;
        }
    }

    // create a (short-lived) proxy based on the exclusive publication just created
    aeron_archive_proxy_t archive_proxy;

    // the exclusive_publication is now 'owned' by the proxy
    if (aeron_archive_proxy_init(
        &archive_proxy,
        aeron_archive->ctx,
        exclusive_publication,
        AERON_ARCHIVE_PROXY_RETRY_ATTEMPTS_DEFAULT) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        aeron_exclusive_publication_close(exclusive_publication, NULL, NULL);
        goto cleanup;
    }

    aeron_archive_proxy_set_control_esssion_id(&archive_proxy, aeron_archive->control_session_id);

    aeron_counters_reader_t *counters_reader = aeron_counters_reader(aeron_archive->ctx->aeron);
    int pub_limit_counter_id = aeron_counters_reader_find_by_type_id_and_registration_id(
        counters_reader,
        AERON_COUNTER_PUBLISHER_LIMIT_TYPE_ID,
        publication_id);

    while (!aeron_exclusive_publication_is_connected(exclusive_publication))
    {
        if (aeron_nano_clock() > deadline_ns)
        {
            AERON_SET_ERR(ETIMEDOUT, "%s", "waiting for publication to connect");
            goto cleanup_proxy;
        }

        aeron_archive_idle(aeron_archive);
    }

    int64_t *pub_limit_counter = aeron_counters_reader_addr(counters_reader, pub_limit_counter_id);

    while (0 == *pub_limit_counter)
    {
        if (aeron_nano_clock() > deadline_ns)
        {
            AERON_SET_ERR(ETIMEDOUT, "%s", "waiting for available publication limit");
            goto cleanup_proxy;
        }

        aeron_archive_idle(aeron_archive);
    }

    // using the newly created proxy (based on the newly created exclusive publisher), send the replay request
    correlation_id = aeron_archive_next_correlation_id(aeron_archive);

    if (!aeron_archive_proxy_replay(
        &archive_proxy,
        correlation_id,
        recording_id,
        replay_channel,
        replay_stream_id,
        &response_channel_replay_params))
    {
        AERON_APPEND_ERR("%s", "");
        goto cleanup_proxy;
    }

    if (aeron_archive_poll_for_response(
        replay_session_id_p,
        aeron_archive,
        "AeronArchive::replay",
        correlation_id) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto cleanup_proxy;
    }

    rc = 0;

cleanup_proxy:
    aeron_archive_proxy_close(&archive_proxy);

cleanup:
    if (NULL != control_request_channel)
    {
        aeron_free(control_request_channel);
    }
    return rc;
}

int aeron_archive_channel_with_session_id(char *out, size_t out_len, const char *in, int32_t session_id)
{
    int rc = 0;
    aeron_uri_string_builder_t builder;

    if (aeron_uri_string_builder_init_on_string(&builder, in) < 0 ||
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_SESSION_ID_KEY, session_id) < 0 ||
        aeron_uri_string_builder_sprint(&builder, out, out_len) < 0)
    {
        rc = -1;
    }

    aeron_uri_string_builder_close(&builder);

    return rc;
}
