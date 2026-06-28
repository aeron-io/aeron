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

#ifndef AERON_ARCHIVE_CLIENT_CONTEXT_H
#define AERON_ARCHIVE_CLIENT_CONTEXT_H

#include "aeron_archive.h"
#include "aeron_archive_credentials_supplier.h"

#define AERON_ARCHIVE_MESSAGE_TIMEOUT_ENV_VAR "AERON_ARCHIVE_MESSAGE_TIMEOUT"
#define AERON_ARCHIVE_MESSAGE_TIMEOUT_NS_DEFAULT  (UINT64_C(10) * 1000 * 1000 * 1000) // 10 seconds
#define AERON_ARCHIVE_MESSAGE_RETRY_ATTEMPTS_ENV_VAR "AERON_ARCHIVE_MESSAGE_RETRY_ATTEMPTS"
#define AERON_ARCHIVE_MESSAGE_RETRY_ATTEMPTS_DEFAULT  UINT32_C(3)
#define AERON_ARCHIVE_CONTROL_CHANNEL_ENV_VAR "AERON_ARCHIVE_CONTROL_CHANNEL"
#define AERON_ARCHIVE_CONTROL_STREAM_ID_ENV_VAR "AERON_ARCHIVE_CONTROL_STREAM_ID"
#define AERON_ARCHIVE_CONTROL_STREAM_ID_DEFAULT (10)
#define AERON_ARCHIVE_CONTROL_TERM_BUFFER_LENGTH_ENV_VAR "AERON_ARCHIVE_CONTROL_TERM_BUFFER_LENGTH"
#define AERON_ARCHIVE_CONTROL_TERM_BUFFER_LENGTH_DEFAULT (64 * 1024)
#define AERON_ARCHIVE_CONTROL_TERM_BUFFER_SPARSE_ENV_VAR "AERON_ARCHIVE_CONTROL_TERM_BUFFER_SPARSE"
#define AERON_ARCHIVE_CONTROL_TERM_BUFFER_SPARSE_DEFAULT (true)
#define AERON_ARCHIVE_CONTROL_MTU_LENGTH_ENV_VAR "AERON_ARCHIVE_CONTROL_MTU_LENGTH"
#define AERON_ARCHIVE_CONTROL_RESPONSE_CHANNEL_ENV_VAR "AERON_ARCHIVE_CONTROL_RESPONSE_CHANNEL"
#define AERON_ARCHIVE_CONTROL_RESPONSE_STREAM_ID_ENV_VAR "AERON_ARCHIVE_CONTROL_RESPONSE_STREAM_ID"
#define AERON_ARCHIVE_CONTROL_RESPONSE_STREAM_ID_DEFAULT (20)
#define AERON_ARCHIVE_RECORDING_EVENTS_CHANNEL_ENV_VAR "AERON_ARCHIVE_RECORDING_EVENTS_CHANNEL"
#define AERON_ARCHIVE_RECORDING_EVENTS_STREAM_ID_ENV_VAR "AERON_ARCHIVE_RECORDING_EVENTS_STREAM_ID"
#define AERON_ARCHIVE_RECORDING_EVENTS_STREAM_ID_DEFAULT (30)
#define AERON_ARCHIVE_CLIENT_NAME_ENV_VAR "AERON_ARCHIVE_CLIENT_NAME"

struct aeron_archive_context_stct
{
    aeron_t *aeron;
    char aeron_directory_name[AERON_MAX_PATH];
    char client_name[AERON_COUNTER_MAX_CLIENT_NAME_LENGTH + 1];
    bool owns_aeron_client;

    char *control_request_channel;
    size_t control_request_channel_length;
    int32_t control_request_stream_id;

    char *control_response_channel;
    size_t control_response_channel_length;
    int32_t control_response_stream_id;

    char *recording_events_channel;
    size_t recording_events_channel_length;
    int32_t recording_events_stream_id;

    uint64_t message_timeout_ns;
    uint32_t message_retry_attempts;

    size_t control_term_buffer_length;
    size_t control_mtu_length;
    bool control_term_buffer_sparse;

    aeron_idle_strategy_func_t idle_strategy_func;
    void *idle_strategy_state;
    bool owns_idle_strategy;

    aeron_archive_credentials_supplier_t credentials_supplier;

    aeron_archive_recording_signal_consumer_func_t on_recording_signal;
    void *on_recording_signal_clientd;

    aeron_error_handler_t error_handler;
    void *error_handler_clientd;

    aeron_archive_delegating_invoker_func_t delegating_invoker_func;
    void *delegating_invoker_func_clientd;
};

int aeron_archive_context_duplicate(aeron_archive_context_t **dest_p, aeron_archive_context_t *src);

int aeron_archive_context_conclude(aeron_archive_context_t *ctx);

void aeron_archive_context_idle(aeron_archive_context_t *ctx);

void aeron_archive_context_invoke_aeron_client(aeron_archive_context_t *ctx);

void aeron_archive_context_invoke_error_handler(
    aeron_archive_context_t *ctx, int64_t correlation_id, int32_t error_code, const char *error_message);

#endif //AERON_ARCHIVE_CLIENT_CONTEXT_H
