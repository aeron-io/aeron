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

#include <stdio.h>
#include <inttypes.h>

#include "aeron_archive.h"
#include "aeron_archive_context.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "uri/aeron_uri_string_builder.h"
#include "aeron_archive_client.h"
#include "command/aeron_control_protocol.h"

#define REPLAY_MERGE_LIVE_ADD_MAX_WINDOW (32 * 1024 * 1024)
#define REPLAY_MERGE_REPLAY_REMOVE_THRESHOLD (0)
#define REPLAY_MERGE_INITIAL_GET_MAX_RECORDED_POSITION_BACKOFF_MS (8)
#define REPLAY_MERGE_GET_MAX_RECORDED_POSITION_BACKOFF_MAX_MS (500)
#define REPLAY_MERGE_ARCHIVE_POLL_INTERVAL_MS (100)

typedef enum aeron_archive_replay_merge_state_en
{
    RESOLVE_REPLAY_PORT = 0,
    GET_RECORDING_POSITION = 1,
    REPLAY = 2,
    CATCHUP = 3,
    ATTEMPT_LIVE_JOIN = 4,
    MERGED = 5,
    FAILED = 6,
    CLOSED = 7
}
aeron_archive_replay_merge_state_t;

struct aeron_archive_replay_merge_stct
{
    aeron_subscription_t *subscription;
    aeron_archive_t *aeron_archive;
    aeron_t *aeron;
    aeron_archive_proxy_t *archive_proxy;
    int64_t control_session_id;
    aeron_uri_string_builder_t replay_channel_builder;
    char *replay_destination;
    char *live_destination;
    char *replay_endpoint;
    size_t replay_endpoint_malloced_len;
    int64_t recording_id;
    int64_t start_position;
    long long epoch_clock;
    int64_t merge_progress_timeout_ms;
    long long time_of_last_progress_ms;
    long long time_of_next_get_max_recorded_position_ms;
    aeron_async_destination_t *async_destination;
    aeron_archive_replay_merge_state_t state;
    aeron_image_t *image;
    int64_t active_correlation_id;
    int64_t next_target_position;
    long long get_max_recorded_position_backoff_ms;
    long long time_of_last_scheduled_archive_poll_ms;
    bool is_replay_active;
    int64_t replay_session_id;
    int64_t position_of_last_progress;
    bool is_live_added;
};

static int aeron_archive_replay_merge_resolve_replay_port(int *work_count_p, aeron_archive_replay_merge_t *replay_merge, long long now_ms);
static int aeron_archive_replay_merge_get_recording_position(int *work_count_p, aeron_archive_replay_merge_t *replay_merge, long long now_ms);
static int aeron_archive_replay_merge_replay(int *work_count_p, aeron_archive_replay_merge_t *replay_merge, long long now_ms);
static int aeron_archive_replay_merge_catchup(int *work_count_p, aeron_archive_replay_merge_t *replay_merge, long long now_ms);
static int aeron_archive_replay_merge_attempt_live_join(int *work_count_p, aeron_archive_replay_merge_t *replay_merge, long long now_ms);

static void aeron_archive_replay_merge_set_state(aeron_archive_replay_merge_t *replay_merge, aeron_archive_replay_merge_state_t new_state);
static int aeron_archive_replay_merge_handle_async_destination(aeron_archive_replay_merge_t *replay_merge);

static bool aeron_archive_replay_merge_call_get_max_recorded_position(aeron_archive_replay_merge_t *replay_merge, long long now_ms);
static int aeron_archive_replay_merge_poll_for_response(bool *found_response_p, aeron_archive_replay_merge_t *replay_merge);
static bool aeron_archive_replay_merge_should_add_live_destination(aeron_archive_replay_merge_t *replay_merge, int64_t position);
static bool aeron_archive_replay_merge_should_stop_and_remove_replay(aeron_archive_replay_merge_t *replay_merge, int64_t position);

static void aeron_archive_replay_merge_stop_replay(aeron_archive_replay_merge_t *replay_merge);

/* ************* */

int aeron_archive_replay_merge_init(
    aeron_archive_replay_merge_t **replay_merge,
    aeron_subscription_t *subscription,
    aeron_archive_t *aeron_archive,
    const char *replay_channel,
    const char *replay_destination,
    const char *live_destination,
    int64_t recording_id,
    int64_t start_position,
    long long epoch_clock,
    int64_t merge_progress_timeout_ms)
{
    {
        aeron_subscription_constants_t constants;
        aeron_subscription_constants(subscription, &constants);

        if (strncmp(constants.channel, AERON_IPC_CHANNEL, AERON_IPC_CHANNEL_LEN) == 0 ||
            strncmp(replay_channel, AERON_IPC_CHANNEL, AERON_IPC_CHANNEL_LEN) == 0 ||
            strncmp(replay_destination, AERON_IPC_CHANNEL, AERON_IPC_CHANNEL_LEN) == 0 ||
            strncmp(live_destination, AERON_IPC_CHANNEL, AERON_IPC_CHANNEL_LEN) == 0)
        {
            AERON_SET_ERR(EINVAL, "%s", "IPC merging is not supported");
            return -1;
        }

        if (NULL == strstr(constants.channel, "control-mode=manual"))
        {
            AERON_SET_ERR(EINVAL, "Subscription URI must have 'control-mode=manual' uri=%s", constants.channel);
            return -1;
        }
    }

    aeron_archive_replay_merge_t *_replay_merge;

    if (aeron_alloc((void **)&_replay_merge, sizeof(aeron_archive_replay_merge_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_replay_merge_t");
        return -1;
    }

    _replay_merge->subscription = subscription;
    _replay_merge->aeron_archive = aeron_archive;
    _replay_merge->aeron = aeron_archive_context_get_aeron(aeron_archive_get_archive_context(aeron_archive));
    _replay_merge->archive_proxy = aeron_archive_proxy(aeron_archive);
    _replay_merge->control_session_id = aeron_archive_control_session_id(aeron_archive);

    aeron_uri_string_builder_init_on_string(&_replay_merge->replay_channel_builder, replay_channel);
    aeron_uri_string_builder_put(&_replay_merge->replay_channel_builder, AERON_URI_LINGER_TIMEOUT_KEY, "0");
    aeron_uri_string_builder_put(&_replay_merge->replay_channel_builder, AERON_URI_EOS_KEY, "false");

    size_t replay_endpoint_len;
    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_on_string(&builder, replay_destination);

        const char *replay_endpoint = aeron_uri_string_builder_get(&builder, AERON_UDP_CHANNEL_ENDPOINT_KEY);
        replay_endpoint_len = strlen(replay_endpoint);
        _replay_merge->replay_endpoint_malloced_len = replay_endpoint_len + 25; // +25 for terminator and possible resolved ip/port

        aeron_alloc((void **)&_replay_merge->replay_endpoint, _replay_merge->replay_endpoint_malloced_len);

        snprintf(
            _replay_merge->replay_endpoint,
            _replay_merge->replay_endpoint_malloced_len,
            "%s",
            replay_endpoint);

        aeron_uri_string_builder_close(&builder);
    }

    {
        if (strncmp(":0", &_replay_merge->replay_endpoint[replay_endpoint_len - 2], 2) == 0)
        {
            _replay_merge->state = RESOLVE_REPLAY_PORT;
        }
        else
        {
            aeron_uri_string_builder_put(
                &_replay_merge->replay_channel_builder,
                AERON_UDP_CHANNEL_ENDPOINT_KEY,
                _replay_merge->replay_endpoint);
            _replay_merge->state = GET_RECORDING_POSITION;
        }
    }

    size_t replay_destination_len = strlen(replay_destination);
    aeron_alloc((void **)&_replay_merge->replay_destination, replay_destination_len + 1);
    memcpy(_replay_merge->replay_destination, replay_destination, replay_destination_len);
    _replay_merge->replay_destination[replay_destination_len] = '\0';

    size_t live_destination_len = strlen(live_destination);
    aeron_alloc((void **)&_replay_merge->live_destination, live_destination_len + 1);
    memcpy(_replay_merge->live_destination, live_destination, live_destination_len);
    _replay_merge->live_destination[live_destination_len] = '\0';

    _replay_merge->recording_id = recording_id;
    _replay_merge->start_position = start_position;
    _replay_merge->epoch_clock = epoch_clock;
    _replay_merge->merge_progress_timeout_ms = merge_progress_timeout_ms;

    if (aeron_subscription_async_add_destination(
        &_replay_merge->async_destination,
        _replay_merge->aeron,
        _replay_merge->subscription,
        _replay_merge->replay_destination) < 0)
    {
        aeron_uri_string_builder_close(&_replay_merge->replay_channel_builder);
        aeron_free(_replay_merge->replay_destination);
        aeron_free(_replay_merge->live_destination);
        aeron_free(_replay_merge->replay_endpoint);
        aeron_free(_replay_merge);

        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    _replay_merge->time_of_last_progress_ms = epoch_clock;
    _replay_merge->time_of_next_get_max_recorded_position_ms = epoch_clock;

    _replay_merge->image = NULL;
    _replay_merge->active_correlation_id = AERON_NULL_VALUE;
    _replay_merge->next_target_position = AERON_NULL_VALUE;
    _replay_merge->get_max_recorded_position_backoff_ms = REPLAY_MERGE_INITIAL_GET_MAX_RECORDED_POSITION_BACKOFF_MS;
    _replay_merge->time_of_last_scheduled_archive_poll_ms = 0;
    _replay_merge->is_replay_active = false;
    _replay_merge->replay_session_id = AERON_NULL_VALUE;
    _replay_merge->position_of_last_progress = AERON_NULL_VALUE;
    _replay_merge->is_live_added = false;

    *replay_merge = _replay_merge;

    return 0;
}

int aeron_archive_replay_merge_close(aeron_archive_replay_merge_t *replay_merge)
{
    if (!aeron_is_closed(replay_merge->aeron))
    {
        if (aeron_archive_replay_merge_handle_async_destination(replay_merge) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        while (NULL != replay_merge->async_destination)
        {
            aeron_archive_idle(replay_merge->aeron_archive);

            aeron_archive_context_invoke_aeron_client(replay_merge->aeron_archive->ctx);

            if (aeron_archive_replay_merge_handle_async_destination(replay_merge) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                return -1;
            }
        }

        if (MERGED != replay_merge->state)
        {
            if (aeron_subscription_async_remove_destination(
                &replay_merge->async_destination,
                replay_merge->aeron,
                replay_merge->subscription,
                replay_merge->replay_destination) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                return -1;
            }

            if (aeron_archive_replay_merge_handle_async_destination(replay_merge) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                return -1;
            }

            while (NULL != replay_merge->async_destination)
            {
                aeron_archive_idle(replay_merge->aeron_archive);

                aeron_archive_context_invoke_aeron_client(replay_merge->aeron_archive->ctx);

                if (aeron_archive_replay_merge_handle_async_destination(replay_merge) < 0)
                {
                    AERON_APPEND_ERR("%s", "");
                    return -1;
                }
            }
        }

        if (replay_merge->is_replay_active)
        {
            aeron_archive_replay_merge_stop_replay(replay_merge);
        }
    }

    aeron_uri_string_builder_close(&replay_merge->replay_channel_builder);
    aeron_free(replay_merge->replay_destination);
    aeron_free(replay_merge->live_destination);
    aeron_free(replay_merge->replay_endpoint);
    aeron_free(replay_merge);

    return 0;
}

int aeron_archive_replay_merge_do_work(int *work_count_p, aeron_archive_replay_merge_t *replay_merge)
{
    if (aeron_archive_replay_merge_handle_async_destination(replay_merge) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        aeron_archive_replay_merge_set_state(replay_merge, FAILED);
        return -1;
    }

    int rc = 0;
    const long long now_ms = aeron_epoch_clock();
    bool check_progress = true;
    int work_count = 0;

    switch (replay_merge->state)
    {
        case RESOLVE_REPLAY_PORT:
            rc = aeron_archive_replay_merge_resolve_replay_port(&work_count, replay_merge, now_ms);
            break;

        case GET_RECORDING_POSITION:
            rc = aeron_archive_replay_merge_get_recording_position(&work_count, replay_merge, now_ms);
            break;

        case REPLAY:
            rc = aeron_archive_replay_merge_replay(&work_count, replay_merge, now_ms);
            break;

        case CATCHUP:
            rc = aeron_archive_replay_merge_catchup(&work_count, replay_merge, now_ms);
            break;

        case ATTEMPT_LIVE_JOIN:
            rc = aeron_archive_replay_merge_attempt_live_join(&work_count, replay_merge, now_ms);
            break;

        default:
            check_progress = false;
            break;
    }

    if (NULL != work_count_p)
    {
        *work_count_p += work_count;
    }

    if (-1 == rc)
    {
        AERON_APPEND_ERR("%s", "");
        aeron_archive_replay_merge_set_state(replay_merge, FAILED);
        return -1;
    }

    if (check_progress)
    {
        if (now_ms > (replay_merge->time_of_last_progress_ms + replay_merge->merge_progress_timeout_ms))
        {
            AERON_SET_ERR(ETIMEDOUT, "replay_merge no progress: state=%i", replay_merge->state);
            aeron_archive_replay_merge_set_state(replay_merge, FAILED);
            return -1;
        }

        if (AERON_NULL_VALUE == replay_merge->active_correlation_id &&
            (now_ms > (replay_merge->time_of_last_scheduled_archive_poll_ms + REPLAY_MERGE_ARCHIVE_POLL_INTERVAL_MS)))
        {
            replay_merge->time_of_last_scheduled_archive_poll_ms = now_ms;

            bool ignored;
            if (aeron_archive_replay_merge_poll_for_response(&ignored, replay_merge) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                aeron_archive_replay_merge_set_state(replay_merge, FAILED);
                return -1;
            }
        }
    }

    return 0;
}

int aeron_archive_replay_merge_poll(
    aeron_archive_replay_merge_t *replay_merge,
    aeron_fragment_handler_t handler,
    void *clientd,
    int fragment_limit)
{
    if (aeron_archive_replay_merge_do_work(NULL, replay_merge) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    return NULL == replay_merge->image ? 0 : aeron_image_poll(replay_merge->image, handler, clientd, fragment_limit);
}

aeron_image_t *aeron_archive_replay_merge_image(aeron_archive_replay_merge_t *replay_merge)
{
    return replay_merge->image;

}

bool aeron_archive_replay_merge_is_merged(aeron_archive_replay_merge_t *replay_merge)
{
    return MERGED == replay_merge->state;
}

bool aeron_archive_replay_merge_has_failed(aeron_archive_replay_merge_t *replay_merge)
{
    return FAILED == replay_merge->state;
}

bool aeron_archive_replay_merge_is_live_added(aeron_archive_replay_merge_t *replay_merge)
{
    return replay_merge->is_live_added;
}

/* ************* */

static int aeron_archive_replay_merge_resolve_replay_port(int *work_count_p, aeron_archive_replay_merge_t *replay_merge, long long now_ms)
{
    char resolved_endpoint[AERON_CLIENT_MAX_LOCAL_ADDRESS_STR_LEN] = { 0 };
    int rc = aeron_subscription_resolved_endpoint(replay_merge->subscription, resolved_endpoint, AERON_URI_MAX_LENGTH);

    if (rc < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (rc == 0) // not found
    {
        *work_count_p = 0;
        return 0;
    }

    if (strlen(resolved_endpoint) != 0)
    {
        char *p = strrchr(resolved_endpoint, ':');

        if (NULL == p)
        {
            AERON_SET_ERR(-1, "malformed endpoint missing semicolon: resolved_endpoint=%s", resolved_endpoint);
            return -1;
        }

        size_t dest_idx = strlen(replay_merge->replay_endpoint) - 2;
        char *dest = &replay_merge->replay_endpoint[dest_idx];

        if (dest_idx + strlen(p) >= replay_merge->replay_endpoint_malloced_len)
        {
            AERON_SET_ERR(
                -1,
                "resolved endpoint is too long: replay_endpoint=%s resolved_endpoint=%s",
                replay_merge->replay_endpoint,
                resolved_endpoint);
            return -1;
        }

        snprintf(dest, replay_merge->replay_endpoint_malloced_len - dest_idx, "%s", p);

        aeron_uri_string_builder_put(
            &replay_merge->replay_channel_builder,
            AERON_UDP_CHANNEL_ENDPOINT_KEY,
            replay_merge->replay_endpoint);

        replay_merge->time_of_last_progress_ms = now_ms;

        aeron_archive_replay_merge_set_state(replay_merge, GET_RECORDING_POSITION);

        *work_count_p = 1;
    }
    else
    {
        *work_count_p = 0;
    }

    return 0;
}

static int aeron_archive_replay_merge_get_recording_position(int *work_count_p, aeron_archive_replay_merge_t *replay_merge, long long now_ms)
{
    int work_count = 0;

    if (AERON_NULL_VALUE == replay_merge->active_correlation_id)
    {
        if (aeron_archive_replay_merge_call_get_max_recorded_position(replay_merge, now_ms))
        {
            replay_merge->time_of_last_progress_ms = now_ms;
            work_count += 1;
        }
    }
    else
    {
        bool found_response;

        if (aeron_archive_replay_merge_poll_for_response(&found_response, replay_merge) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        if (found_response)
        {
            replay_merge->next_target_position = aeron_archive_control_response_poller(replay_merge->aeron_archive)->relevant_id;
            replay_merge->active_correlation_id = AERON_NULL_VALUE;

            if (AERON_NULL_VALUE != replay_merge->next_target_position)
            {
                replay_merge->time_of_last_progress_ms = now_ms;
                aeron_archive_replay_merge_set_state(replay_merge, REPLAY);
            }

            work_count += 1;
        }
    }

    *work_count_p = work_count;

    return 0;
}

static int aeron_archive_replay_merge_replay(int *work_count_p, aeron_archive_replay_merge_t *replay_merge, long long now_ms)
{
    int work_count = 0;

    if (AERON_NULL_VALUE == replay_merge->active_correlation_id)
    {
        char replay_channel[AERON_URI_MAX_LENGTH];

        int64_t correlation_id = aeron_archive_next_correlation_id(replay_merge->aeron_archive);

        aeron_uri_string_builder_sprint(&replay_merge->replay_channel_builder, replay_channel, sizeof(replay_channel));

        aeron_archive_replay_params_t replay_params;
        aeron_archive_replay_params_init(&replay_params);

        replay_params.position = replay_merge->start_position;
        replay_params.length = INT64_MAX;

        aeron_subscription_constants_t subscription_constants;
        aeron_subscription_constants(replay_merge->subscription, &subscription_constants);

        if (aeron_archive_proxy_replay(
            replay_merge->archive_proxy,
            correlation_id,
            replay_merge->recording_id,
            replay_channel,
            subscription_constants.stream_id,
            &replay_params))
        {
            replay_merge->time_of_last_progress_ms = now_ms;
            replay_merge->active_correlation_id = correlation_id;
            work_count += 1;
        }
    }
    else
    {
        bool found_response;

        if (aeron_archive_replay_merge_poll_for_response(&found_response, replay_merge) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        if (found_response)
        {
            replay_merge->is_replay_active = true;
            replay_merge->replay_session_id = aeron_archive_control_response_poller(replay_merge->aeron_archive)->relevant_id;
            replay_merge->time_of_last_progress_ms = now_ms;
            replay_merge->active_correlation_id = AERON_NULL_VALUE;

            replay_merge->get_max_recorded_position_backoff_ms = REPLAY_MERGE_INITIAL_GET_MAX_RECORDED_POSITION_BACKOFF_MS;
            replay_merge->time_of_next_get_max_recorded_position_ms = now_ms;

            aeron_archive_replay_merge_set_state(replay_merge, CATCHUP);

            work_count += 1;
        }
    }

    *work_count_p = work_count;

    return 0;
}

static int aeron_archive_replay_merge_catchup(int *work_count_p, aeron_archive_replay_merge_t *replay_merge, long long now_ms)
{
    int work_count = 0;

    if (NULL == replay_merge->image && aeron_subscription_is_connected(replay_merge->subscription))
    {
        replay_merge->time_of_last_progress_ms = now_ms;
        replay_merge->image = aeron_subscription_image_by_session_id(replay_merge->subscription, (int32_t)replay_merge->replay_session_id);
        replay_merge->position_of_last_progress = NULL == replay_merge->image ? AERON_NULL_VALUE : aeron_image_position(replay_merge->image);
    }

    if (NULL != replay_merge->image)
    {
        int64_t position = aeron_image_position(replay_merge->image);

        if (position >= replay_merge->next_target_position)
        {
            replay_merge->time_of_last_progress_ms = now_ms;
            replay_merge->position_of_last_progress = position;
            replay_merge->active_correlation_id = AERON_NULL_VALUE;

            aeron_archive_replay_merge_set_state(replay_merge, ATTEMPT_LIVE_JOIN);

            work_count += 1;
        }
        else if (position > replay_merge->position_of_last_progress)
        {
            replay_merge->time_of_last_progress_ms = now_ms;
            replay_merge->position_of_last_progress = position;
        }
        else if (aeron_image_is_closed(replay_merge->image))
        {
            AERON_SET_ERR(ETIMEDOUT, "%s", "replay merge image closed unexpectedly");
            return -1;
        }
    }

    *work_count_p = work_count;

    return 0;
}

static int aeron_archive_replay_merge_attempt_live_join(int *work_count_p, aeron_archive_replay_merge_t *replay_merge, long long now_ms)
{
    /* if we're still waiting around for a previous async destination to be complete, just bail out early */
    if (NULL != replay_merge->async_destination)
    {
        *work_count_p = 0;

        return 0;
    }

    int work_count = 0;

    if (AERON_NULL_VALUE == replay_merge->active_correlation_id)
    {
        if (aeron_archive_replay_merge_call_get_max_recorded_position(replay_merge, now_ms))
        {
            replay_merge->time_of_last_progress_ms = now_ms;
            work_count += 1;
        }
    }
    else
    {
        bool found_response;

        if (aeron_archive_replay_merge_poll_for_response(&found_response, replay_merge) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        if (found_response)
        {
            replay_merge->next_target_position = aeron_archive_control_response_poller(replay_merge->aeron_archive)->relevant_id;
            replay_merge->active_correlation_id = AERON_NULL_VALUE;

            if (AERON_NULL_VALUE != replay_merge->next_target_position)
            {
                aeron_archive_replay_merge_state_t next_state = CATCHUP;

                if (NULL != replay_merge->image)
                {
                    int64_t position = aeron_image_position(replay_merge->image);

                    if (aeron_archive_replay_merge_should_add_live_destination(replay_merge, position))
                    {
                        if (aeron_subscription_async_add_destination(
                            &replay_merge->async_destination,
                            replay_merge->aeron,
                            replay_merge->subscription,
                            replay_merge->live_destination) < 0)
                        {
                            AERON_APPEND_ERR("%s", "");
                            return -1;
                        }

                        replay_merge->time_of_last_progress_ms = now_ms;
                        replay_merge->position_of_last_progress = position;
                        replay_merge->is_live_added = true;
                    }
                    else if (aeron_archive_replay_merge_should_stop_and_remove_replay(replay_merge, position))
                    {
                        if (aeron_subscription_async_remove_destination(
                            &replay_merge->async_destination,
                            replay_merge->aeron,
                            replay_merge->subscription,
                            replay_merge->replay_destination) < 0)
                        {
                            AERON_APPEND_ERR("%s", "");
                            return -1;
                        }

                        aeron_archive_replay_merge_stop_replay(replay_merge);
                        replay_merge->time_of_last_progress_ms = now_ms;
                        replay_merge->position_of_last_progress = position;
                        next_state = MERGED;
                    }
                }

                aeron_archive_replay_merge_set_state(replay_merge, next_state);
            }

            work_count += 1;
        }
    }

    *work_count_p = work_count;

    return 0;
}

static void aeron_archive_replay_merge_set_state(aeron_archive_replay_merge_t *replay_merge, aeron_archive_replay_merge_state_t new_state)
{
    replay_merge->state = new_state;
}

static int aeron_archive_replay_merge_handle_async_destination(aeron_archive_replay_merge_t *replay_merge)
{
    if (NULL != replay_merge->async_destination)
    {
        int rc = aeron_subscription_async_destination_poll(replay_merge->async_destination);

        if (rc == 1)
        {
            replay_merge->async_destination = NULL;
        }
        else if (rc < 0)
        {
            replay_merge->async_destination = NULL;
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
    }

    return 0;
}

static bool aeron_archive_replay_merge_call_get_max_recorded_position(aeron_archive_replay_merge_t *replay_merge, long long now_ms)
{
    if (now_ms < replay_merge->time_of_next_get_max_recorded_position_ms)
    {
        return false;
    }

    int64_t correlation_id = aeron_archive_next_correlation_id(replay_merge->aeron_archive);

    bool result = aeron_archive_proxy_get_max_recorded_position(
        replay_merge->archive_proxy,
        correlation_id,
        replay_merge->recording_id);

    if (result)
    {
        replay_merge->active_correlation_id = correlation_id;
    }

    replay_merge->get_max_recorded_position_backoff_ms *= 2;
    if (replay_merge->get_max_recorded_position_backoff_ms > REPLAY_MERGE_GET_MAX_RECORDED_POSITION_BACKOFF_MAX_MS)
    {
        replay_merge->get_max_recorded_position_backoff_ms = REPLAY_MERGE_GET_MAX_RECORDED_POSITION_BACKOFF_MAX_MS;
    }
    replay_merge->time_of_next_get_max_recorded_position_ms = now_ms + replay_merge->get_max_recorded_position_backoff_ms;

    return result;
}

static int aeron_archive_replay_merge_poll_for_response(bool *found_response_p, aeron_archive_replay_merge_t *replay_merge)
{
    aeron_archive_control_response_poller_t *poller = aeron_archive_control_response_poller(replay_merge->aeron_archive);

    const int poll_count = aeron_archive_control_response_poller_poll(poller);

    if (poll_count < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (poller->is_poll_complete &&
        poller->control_session_id == replay_merge->control_session_id)
    {
        if (poller->is_code_error)
        {
            AERON_SET_ERR(
                (int32_t)poller->relevant_id,
                "correlation_id=%" PRIi64 " %s",
                poller->correlation_id,
                poller->error_message);
            return -1;
        }

        *found_response_p = poller->correlation_id == replay_merge->active_correlation_id;
    }
    else
    {
        *found_response_p = false;

        if (poll_count == 0 && !aeron_subscription_is_connected(poller->subscription))
        {
            AERON_SET_ERR(-AERON_ERROR_CODE_GENERIC_ERROR, "%s", "archive is not connected");
            return -1;
        }
    }

    return 0;
}

static bool aeron_archive_replay_merge_should_add_live_destination(aeron_archive_replay_merge_t *replay_merge, int64_t position)
{
    aeron_image_constants_t image_constants;

    aeron_image_constants(replay_merge->image, &image_constants);
    size_t window_len = image_constants.term_buffer_length / 4;

    return !replay_merge->is_live_added &&
        (replay_merge->next_target_position - position) <=
            (int64_t)(window_len < REPLAY_MERGE_LIVE_ADD_MAX_WINDOW ? window_len : REPLAY_MERGE_LIVE_ADD_MAX_WINDOW);
}

static bool aeron_archive_replay_merge_should_stop_and_remove_replay(aeron_archive_replay_merge_t *replay_merge, int64_t position)
{
    return replay_merge->is_live_added &&
        (replay_merge->next_target_position - position) <= REPLAY_MERGE_REPLAY_REMOVE_THRESHOLD &&
        aeron_image_active_transport_count(replay_merge->image) >= 2;
}

static void aeron_archive_replay_merge_stop_replay(aeron_archive_replay_merge_t *replay_merge)
{
    int64_t correlation_id = aeron_archive_next_correlation_id(replay_merge->aeron_archive);

    if (aeron_archive_proxy_stop_replay(
        replay_merge->archive_proxy,
        correlation_id,
        replay_merge->replay_session_id))
    {
        replay_merge->is_replay_active = false;
    }
}
