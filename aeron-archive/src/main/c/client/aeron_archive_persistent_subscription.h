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
#include "aeron_archive_async_client.h"
#include "util/aeron_error.h"
#include "uri/aeron_uri_string_builder.h"

struct aeron_archive_persistent_subscription_context_stct
{
    aeron_t *aeron;
    aeron_context_t *aeron_ctx;
    bool owns_aeron_client;
    char *aeron_directory_name;
    aeron_archive_context_t *archive_context;
    int64_t recording_id;
    char *live_channel;
    int32_t live_stream_id;
    char *replay_channel;
    int32_t replay_stream_id;
    int64_t start_position;
    aeron_archive_persistent_subscription_listener_t listener;
    aeron_counter_t *state_counter;
    aeron_counter_t *join_difference_counter;
    aeron_counter_t *live_left_counter;
    aeron_counter_t *live_joined_counter;
};

typedef struct aeron_archive_persistent_subscription_async_archive_op_stct
{
    int64_t correlation_id;
    int64_t deadline_ns;

    int64_t relevant_id;
    int32_t code;
    char error_message[AERON_ERROR_MAX_TOTAL_LENGTH];

    bool response_received;
}
aeron_archive_persistent_subscription_async_archive_op_t;

enum aeron_archive_persistent_subscription_max_recorded_position_state
{
    REQUEST_MAX_POSITION,
    AWAIT_MAX_POSITION,
    RECHECK_REQUIRED,
};

typedef struct aeron_archive_persistent_subscription_max_recorded_position_stct
{
    aeron_archive_persistent_subscription_async_archive_op_t op;

    enum aeron_archive_persistent_subscription_max_recorded_position_state state;
    int64_t max_recorded_position;
    int32_t close_enough_threshold;
}
max_recorded_position_t;

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
    ADD_REQUEST_PUBLICATION,
    AWAIT_REQUEST_PUBLICATION,
    SEND_REPLAY_TOKEN_REQUEST,
    AWAIT_REPLAY_TOKEN,
    REPLAY,
    ATTEMPT_SWITCH,
    ADD_LIVE_SUBSCRIPTION,
    AWAIT_LIVE,
    LIVE,
    FAILED,
}
aeron_archive_persistent_subscription_state_t;

typedef enum aeron_archive_replay_channel_type_en
{
    REPLAY_CHANNEL_SESSION_SPECIFIC,
    REPLAY_CHANNEL_DYNAMIC_PORT,
    REPLAY_CHANNEL_RESPONSE_CHANNEL,
}
aeron_archive_replay_channel_type_t;

typedef struct aeron_archive_persistent_subscription_list_recording_request_stct
{
    aeron_archive_persistent_subscription_async_archive_op_t op;

    int remaining;

    int64_t recording_id;
    int64_t start_position;
    int64_t stop_position;
    int32_t term_buffer_length;
    int32_t stream_id;
}
aeron_archive_persistent_subscription_list_recording_request_t;

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
    bool replay_channel_is_ipc;
    aeron_archive_persistent_subscription_list_recording_request_t list_recording_request;
    max_recorded_position_t max_recorded_position;
    aeron_archive_persistent_subscription_async_archive_op_t replay_request;
    aeron_archive_persistent_subscription_async_archive_op_t replay_token_request;
    int64_t replay_session_id;
    int64_t replay_image_deadline_ns;
    int64_t join_difference;
    aeron_async_add_subscription_t *add_replay_subscription;
    aeron_subscription_t *replay_subscription;
    aeron_image_t *replay_image;
    int64_t live_image_deadline_ns;
    bool live_image_deadline_breached;
    aeron_async_add_subscription_t *add_live_subscription;
    aeron_subscription_t *live_subscription;
    aeron_image_t *live_image;
    aeron_image_controlled_fragment_assembler_t *assembler;
    aeron_image_fragment_assembler_t *uncontrolled_assembler;
    int64_t next_live_position;
    int64_t position;
    int64_t last_observed_live_position;
    aeron_async_add_exclusive_publication_t *add_request_publication;
    aeron_exclusive_publication_t *request_publication;
    int64_t replay_token;
    aeron_archive_proxy_t *response_channel_archive_proxy;
    int failure_errcode;
    char failure_message[AERON_ERROR_MAX_TOTAL_LENGTH];
    bool use_aeron_agent_invoker;
};

typedef struct aeron_archive_persistent_subscription_poll_ctx_stct
{
    bool controlled;
    union
    {
        aeron_controlled_fragment_handler_t controlled;
        aeron_fragment_handler_t uncontrolled;
    }
    handler;
    void *clientd;
    size_t fragment_limit;
}
aeron_archive_persistent_subscription_poll_ctx_t;

int aeron_archive_persistent_subscription_context_conclude(aeron_archive_persistent_subscription_context_t *context);

aeron_counter_t *aeron_archive_persistent_subscription_context_get_state_counter(
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Returns the join difference, i.e. the difference between the live position and the replay position
 * at the point the switch to live was attempted. A negative value means the replay was ahead of
 * the live stream. Zero means they were aligned. INT64_MIN means no join has been attempted yet.
 *
 * @param persistent_subscription to check.
 * @return the join difference.
 */
int64_t aeron_archive_persistent_subscription_join_difference(aeron_archive_persistent_subscription_t *persistent_subscription);

/**
 * Overrides the message timeout used for internal deadline calculations.
 * Intended for testing timeout code paths.
 *
 * @param persistent_subscription to modify.
 * @param message_timeout_ns new timeout in nanoseconds.
 */
void aeron_archive_persistent_subscription_set_message_timeout_ns_for_testing(
    aeron_archive_persistent_subscription_t *persistent_subscription, uint64_t message_timeout_ns);

#endif //AERON_AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_H
