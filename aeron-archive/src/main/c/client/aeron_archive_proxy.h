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

#ifndef AERON_ARCHIVE_PROXY_H
#define AERON_ARCHIVE_PROXY_H

#include "aeron_archive.h"
#include "aeronc.h"
#include "aeron_common.h"

#define AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH (8 * 1024)

typedef struct aeron_archive_proxy_stct
{
    aeron_archive_context_t *ctx;
    aeron_exclusive_publication_t *exclusive_publication;
    int retry_attempts;
    uint8_t buffer[AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH];
}
aeron_archive_proxy_t;

#define AERON_ARCHIVE_PROXY_RETRY_ATTEMPTS_DEFAULT (3)

int aeron_archive_proxy_create(
    aeron_archive_proxy_t **archive_proxy,
    aeron_archive_context_t *ctx,
    aeron_exclusive_publication_t *exclusive_publication,
    int retry_attempts);

int aeron_archive_proxy_init(
    aeron_archive_proxy_t *archive_proxy,
    aeron_archive_context_t *ctx,
    aeron_exclusive_publication_t *exclusive_publication,
    int retry_attempts);

int aeron_archive_proxy_close(aeron_archive_proxy_t *archive_proxy);

int aeron_archive_proxy_delete(aeron_archive_proxy_t *archive_proxy);

bool aeron_archive_proxy_try_connect(
    aeron_archive_proxy_t *archive_proxy,
    const char *control_response_channel,
    int32_t control_response_stream_id,
    aeron_archive_encoded_credentials_t *encoded_credentials,
    int64_t correlation_id);

bool aeron_archive_proxy_archive_id(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t control_session_id);

bool aeron_archive_proxy_challenge_response(
    aeron_archive_proxy_t *archive_proxy,
    aeron_archive_encoded_credentials_t *encoded_credentials,
    int64_t correlation_id,
    int64_t control_session_id);

bool aeron_archive_proxy_close_session(
    aeron_archive_proxy_t *archive_proxy,
    int64_t control_session_id);

bool aeron_archive_proxy_start_recording(
    aeron_archive_proxy_t *archive_proxy,
    const char *recording_channel,
    int32_t recording_stream_id,
    bool local_source,
    bool auto_stop,
    int64_t correlation_id,
    int64_t control_session_id);

bool aeron_archive_proxy_get_recording_position(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t control_session_id);

bool aeron_archive_proxy_get_start_position(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t control_session_id);

bool aeron_archive_proxy_get_stop_position(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t control_session_id);

bool aeron_archive_proxy_get_max_recorded_position(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t control_session_id);

bool aeron_archive_proxy_stop_recording(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    const char *channel,
    int32_t stream_id,
    int64_t control_session_id);

bool aeron_archive_proxy_stop_recording_subscription(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t subscription_id,
    int64_t control_session_id);

bool aeron_archive_proxy_stop_recording_by_identity(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t control_session_id);

bool aeron_archive_proxy_find_last_matching_recording(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t min_recording_id,
    const char *channel_fragment,
    int32_t stream_id,
    int32_t session_id,
    int64_t control_session_id);

bool aeron_archive_proxy_list_recording(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t control_session_id);

bool aeron_archive_proxy_list_recordings(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t from_recording_id,
    int32_t record_count,
    int64_t control_session_id);

bool aeron_archive_proxy_list_recordings_for_uri(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t from_recording_id,
    int32_t record_count,
    const char *channel_fragment,
    int32_t stream_id,
    int64_t control_session_id);

bool aeron_archive_proxy_replay(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params,
    int64_t control_session_id);

bool aeron_archive_proxy_truncate_recording(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t position,
    int64_t control_session_id);

bool aeron_archive_proxy_stop_replay(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t replay_session_id,
    int64_t control_session_id);

bool aeron_archive_proxy_stop_all_replays(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t control_session_id);

bool aeron_archive_proxy_list_recording_subscriptions(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int32_t pseudo_index,
    int32_t subscription_count,
    const char *channel_fragment,
    int32_t stream_id,
    bool apply_stream_id,
    int64_t control_session_id);

bool aeron_archive_proxy_purge_recording(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t control_session_id);

bool aeron_archive_proxy_extend_recording(
    aeron_archive_proxy_t *archive_proxy,
    int64_t recording_id,
    const char *recording_channel,
    int32_t recording_stream_id,
    bool local_source,
    bool auto_stop,
    int64_t correlation_id,
    int64_t control_session_id);

bool aeron_archive_proxy_replicate(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t src_recording_id,
    int32_t src_control_stream_id,
    const char *src_control_channel,
    aeron_archive_replication_params_t *params,
    int64_t control_session_id);

bool aeron_archive_proxy_stop_replication(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t replication_id,
    int64_t control_session_id);

bool aeron_archive_request_replay_token(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t control_session_id);

bool aeron_archive_proxy_detach_segments(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t new_start_position,
    int64_t control_session_id);

bool aeron_archive_proxy_delete_detached_segments(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t control_session_id);

bool aeron_archive_proxy_purge_segments(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t new_start_position,
    int64_t control_session_id);

bool aeron_archive_proxy_attach_segments(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t control_session_id);

bool aeron_archive_proxy_migrate_segments(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t src_recording_id,
    int64_t dst_recording_id,
    int64_t control_session_id);

#endif //AERON_ARCHIVE_PROXY_H
