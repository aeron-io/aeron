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

#ifndef AERON_AERON_ARCHIVE_ASYNC_CLIENT_H
#define AERON_AERON_ARCHIVE_ASYNC_CLIENT_H

#include "aeron_archive.h"
#include "aeron_archive_proxy.h"

typedef struct aeron_archive_async_client_stct aeron_archive_async_client_t;

typedef struct aeron_archive_async_client_listener_stct
{
    void *clientd;

    void (*on_connected)(void *clientd);
    void (*on_disconnected)(void *clientd);
    void (*on_control_response)(
        void *clientd,
        int64_t correlation_id,
        int64_t relevant_id,
        int32_t code,
        const char *error_message);
    void (*on_recording_descriptor)(void *clientd, aeron_archive_recording_descriptor_t *recording_descriptor);
    void (*on_error)(void *clientd, int errcode, const char *errmsg);
}
aeron_archive_async_client_listener_t;

int aeron_archive_async_client_create(
    aeron_archive_async_client_t **client,
    aeron_archive_context_t *context,
    aeron_archive_async_client_listener_t *listener);

void aeron_archive_async_client_close(aeron_archive_async_client_t *client);

void aeron_archive_async_client_destroy(aeron_archive_async_client_t *client);

int aeron_archive_async_client_poll(aeron_archive_async_client_t *client);

bool aeron_archive_async_client_is_connected(aeron_archive_async_client_t *client);

bool aeron_archive_async_client_is_closed(aeron_archive_async_client_t *client);

int64_t aeron_archive_async_client_get_control_session_id(aeron_archive_async_client_t *client);

bool aeron_archive_async_client_try_send_list_recording_request(
    aeron_archive_async_client_t *client,
    int64_t correlation_id,
    int64_t recording_id);

bool aeron_archive_async_client_try_send_max_recorded_position_request(
    aeron_archive_async_client_t *client,
    int64_t correlation_id,
    int64_t recording_id);

bool aeron_archive_async_client_try_send_replay_token_request(
    aeron_archive_async_client_t *client,
    int64_t correlation_id,
    int64_t recording_id);

bool aeron_archive_async_client_try_send_replay_request(
    aeron_archive_async_client_t *client,
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params);

bool aeron_archive_async_client_try_send_stop_replay_request(
    aeron_archive_async_client_t *client,
    int64_t correlation_id,
    int64_t replay_session_id);

#endif //AERON_AERON_ARCHIVE_ASYNC_CLIENT_H
