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

#include "aeron_alloc.h"
#include "aeron_archive.h"
#include "aeron_archive_client_version.h"
#include "aeron_archive_context.h"
#include "aeron_archive_proxy.h"
#include "aeron_archive_configuration.h"
#include "aeron_archive_replay_params.h"
#include "util/aeron_error.h"

#include "c/aeron_archive_client/authConnectRequest.h"
#include "c/aeron_archive_client/archiveIdRequest.h"
#include "c/aeron_archive_client/challengeResponse.h"
#include "c/aeron_archive_client/closeSessionRequest.h"
#include "c/aeron_archive_client/startRecordingRequest2.h"
#include "c/aeron_archive_client/extendRecordingRequest2.h"
#include "c/aeron_archive_client/recordingPositionRequest.h"
#include "c/aeron_archive_client/startPositionRequest.h"
#include "c/aeron_archive_client/stopPositionRequest.h"
#include "c/aeron_archive_client/maxRecordedPositionRequest.h"
#include "c/aeron_archive_client/stopRecordingRequest.h"
#include "c/aeron_archive_client/stopRecordingSubscriptionRequest.h"
#include "c/aeron_archive_client/stopRecordingByIdentityRequest.h"
#include "c/aeron_archive_client/findLastMatchingRecordingRequest.h"
#include "c/aeron_archive_client/listRecordingRequest.h"
#include "c/aeron_archive_client/listRecordingsRequest.h"
#include "c/aeron_archive_client/listRecordingsForUriRequest.h"
#include "c/aeron_archive_client/boundedReplayRequest.h"
#include "c/aeron_archive_client/replayRequest.h"
#include "c/aeron_archive_client/truncateRecordingRequest.h"
#include "c/aeron_archive_client/stopReplayRequest.h"
#include "c/aeron_archive_client/stopAllReplaysRequest.h"
#include "c/aeron_archive_client/listRecordingSubscriptionsRequest.h"
#include "c/aeron_archive_client/purgeRecordingRequest.h"
#include "c/aeron_archive_client/replicateRequest2.h"
#include "c/aeron_archive_client/stopReplicationRequest.h"
#include "c/aeron_archive_client/replayTokenRequest.h"
#include "c/aeron_archive_client/detachSegmentsRequest.h"
#include "c/aeron_archive_client/deleteDetachedSegmentsRequest.h"
#include "c/aeron_archive_client/purgeSegmentsRequest.h"
#include "c/aeron_archive_client/attachSegmentsRequest.h"
#include "c/aeron_archive_client/migrateSegmentsRequest.h"
#include "c/aeron_archive_client/updateChannelRequest.h"

int64_t aeron_archive_proxy_offer_once(aeron_archive_proxy_t *archive_proxy, size_t length);

bool aeron_archive_proxy_offer(
    aeron_archive_proxy_t *archive_proxy,
    size_t length);

/* **************** */

int aeron_archive_proxy_create(
    aeron_archive_proxy_t **archive_proxy,
    aeron_archive_context_t *ctx,
    aeron_exclusive_publication_t *exclusive_publication,
    int retry_attempts)
{
    aeron_archive_proxy_t *_archive_proxy = NULL;

    if (aeron_alloc((void **)&_archive_proxy, sizeof(aeron_archive_proxy_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_proxy_t");
        return -1;
    }

    aeron_archive_proxy_init(_archive_proxy, ctx, exclusive_publication, retry_attempts);

    *archive_proxy = _archive_proxy;

    return 0;
}

int aeron_archive_proxy_init(
    aeron_archive_proxy_t *archive_proxy,
    aeron_archive_context_t *ctx,
    aeron_exclusive_publication_t *exclusive_publication,
    int retry_attempts)
{
    int total_length = snprintf(
        archive_proxy->client_info,
        sizeof(archive_proxy->client_info),
        "name=%s version=%s commit=%s",
        ctx->client_name,
        aeron_archive_client_version_text(),
        aeron_archive_client_version_git_sha());
    if (total_length < 0)
    {
        AERON_SET_ERR(errno, "%s", "Failed to format client_info");
        return -1;
    }
    archive_proxy->client_info[total_length] = '\0';

    archive_proxy->ctx = ctx;
    archive_proxy->exclusive_publication = exclusive_publication;
    archive_proxy->control_session_id = AERON_NULL_VALUE;
    archive_proxy->retry_attempts = retry_attempts;

    return 0;
}

int aeron_archive_proxy_set_control_esssion_id(
    aeron_archive_proxy_t *archive_proxy,
    int64_t control_session_id)
{
    archive_proxy->control_session_id = control_session_id;

    return 0;
}

int aeron_archive_proxy_close(aeron_archive_proxy_t *archive_proxy)
{
    aeron_exclusive_publication_close(archive_proxy->exclusive_publication, NULL, NULL);
    archive_proxy->exclusive_publication = NULL;

    return 0;
}

int aeron_archive_proxy_delete(aeron_archive_proxy_t *archive_proxy)
{
    aeron_archive_proxy_close(archive_proxy);

    aeron_free(archive_proxy);

    return 0;
}

bool aeron_archive_proxy_try_connect(
    aeron_archive_proxy_t *archive_proxy,
    const char *control_response_channel,
    int32_t control_response_stream_id,
    aeron_archive_encoded_credentials_t *encoded_credentials,
    int64_t correlation_id)
{
    struct aeron_archive_client_authConnectRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_authConnectRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_authConnectRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_authConnectRequest_set_responseStreamId(&codec, control_response_stream_id);
    aeron_archive_client_authConnectRequest_set_version(&codec, aeron_archive_semantic_version());
    aeron_archive_client_authConnectRequest_put_responseChannel(
        &codec,
        control_response_channel,
        strlen(control_response_channel));
    aeron_archive_client_authConnectRequest_put_encodedCredentials(
        &codec,
        NULL != encoded_credentials && NULL != encoded_credentials->data ? encoded_credentials->data : "",
        NULL != encoded_credentials && NULL != encoded_credentials->data ? encoded_credentials->length : 0);
    aeron_archive_client_authConnectRequest_put_clientInfo(
        &codec,
        archive_proxy->client_info,
        strlen(archive_proxy->client_info));

    return aeron_archive_proxy_offer_once(
        archive_proxy,
        aeron_archive_client_authConnectRequest_encoded_length(&codec)) > 0;
}

bool aeron_archive_proxy_archive_id(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id)
{
    struct aeron_archive_client_archiveIdRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_archiveIdRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_archiveIdRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_archiveIdRequest_set_correlationId(&codec, correlation_id);

    return aeron_archive_proxy_offer_once(
        archive_proxy,
        aeron_archive_client_archiveIdRequest_encoded_length(&codec)) > 0;
}

bool aeron_archive_proxy_challenge_response(
    aeron_archive_proxy_t *archive_proxy,
    aeron_archive_encoded_credentials_t *encoded_credentials,
    int64_t correlation_id)
{
    struct aeron_archive_client_challengeResponse codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_challengeResponse_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_challengeResponse_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_challengeResponse_set_correlationId(&codec, correlation_id);
    aeron_archive_client_challengeResponse_put_encodedCredentials(
        &codec,
        NULL == encoded_credentials ? "" : encoded_credentials->data,
        NULL == encoded_credentials ? 0 : encoded_credentials->length);

    return aeron_archive_proxy_offer_once(
        archive_proxy,
        aeron_archive_client_challengeResponse_encoded_length(&codec)) > 0;
}

bool aeron_archive_proxy_close_session(aeron_archive_proxy_t *archive_proxy)
{
    struct aeron_archive_client_closeSessionRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_closeSessionRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_closeSessionRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);

    return aeron_archive_proxy_offer_once(
        archive_proxy,
        aeron_archive_client_closeSessionRequest_encoded_length(&codec)) > 0;
}

bool aeron_archive_proxy_start_recording(
    aeron_archive_proxy_t *archive_proxy,
    const char *recording_channel,
    int32_t recording_stream_id,
    bool local_source,
    bool auto_stop,
    int64_t correlation_id)
{
    struct aeron_archive_client_startRecordingRequest2 codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_startRecordingRequest2_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_startRecordingRequest2_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_startRecordingRequest2_set_correlationId(&codec, correlation_id);
    aeron_archive_client_startRecordingRequest2_set_streamId(&codec, recording_stream_id);
    aeron_archive_client_startRecordingRequest2_set_sourceLocation(
        &codec,
        local_source ? aeron_archive_client_sourceLocation_LOCAL : aeron_archive_client_sourceLocation_REMOTE);
    aeron_archive_client_startRecordingRequest2_set_autoStop(
        &codec,
        auto_stop ? aeron_archive_client_booleanType_TRUE : aeron_archive_client_booleanType_FALSE);
    aeron_archive_client_startRecordingRequest2_put_channel(
        &codec,
        recording_channel,
        strlen(recording_channel));

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_startRecordingRequest2_encoded_length(&codec));
}

bool aeron_archive_proxy_get_recording_position(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id)
{
    struct aeron_archive_client_recordingPositionRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_recordingPositionRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_recordingPositionRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_recordingPositionRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_recordingPositionRequest_set_recordingId(&codec, recording_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_recordingPositionRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_get_start_position(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id)
{
    struct aeron_archive_client_startPositionRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_startPositionRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_startPositionRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_startPositionRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_startPositionRequest_set_recordingId(&codec, recording_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_startPositionRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_get_stop_position(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id)
{
    struct aeron_archive_client_stopPositionRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_stopPositionRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_stopPositionRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_stopPositionRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_stopPositionRequest_set_recordingId(&codec, recording_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_stopPositionRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_get_max_recorded_position(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id)
{
    struct aeron_archive_client_maxRecordedPositionRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_maxRecordedPositionRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_maxRecordedPositionRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_maxRecordedPositionRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_maxRecordedPositionRequest_set_recordingId(&codec, recording_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_maxRecordedPositionRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_stop_recording(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    const char *channel,
    int32_t stream_id)
{
    struct aeron_archive_client_stopRecordingRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_stopRecordingRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_stopRecordingRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_stopRecordingRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_stopRecordingRequest_set_streamId(&codec, stream_id);
    aeron_archive_client_stopRecordingRequest_put_channel(
        &codec,
        channel,
        strlen(channel));

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_stopRecordingRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_stop_recording_subscription(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t subscription_id)
{
    struct aeron_archive_client_stopRecordingSubscriptionRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_stopRecordingSubscriptionRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_stopRecordingSubscriptionRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_stopRecordingSubscriptionRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_stopRecordingSubscriptionRequest_set_subscriptionId(&codec, subscription_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_stopRecordingSubscriptionRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_stop_recording_by_identity(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id)
{
    struct aeron_archive_client_stopRecordingByIdentityRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_stopRecordingByIdentityRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_stopRecordingByIdentityRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_stopRecordingByIdentityRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_stopRecordingByIdentityRequest_set_recordingId(&codec, recording_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_stopRecordingByIdentityRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_find_last_matching_recording(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t min_recording_id,
    const char *channel_fragment,
    int32_t stream_id,
    int32_t session_id)
{
    struct aeron_archive_client_findLastMatchingRecordingRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_findLastMatchingRecordingRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_findLastMatchingRecordingRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_findLastMatchingRecordingRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_findLastMatchingRecordingRequest_set_minRecordingId(&codec, min_recording_id);
    aeron_archive_client_findLastMatchingRecordingRequest_set_sessionId(&codec, session_id);
    aeron_archive_client_findLastMatchingRecordingRequest_set_streamId(&codec, stream_id);
    aeron_archive_client_findLastMatchingRecordingRequest_put_channel(
        &codec,
        channel_fragment,
        strlen(channel_fragment));

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_findLastMatchingRecordingRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_list_recording(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id)
{
    struct aeron_archive_client_listRecordingRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_listRecordingRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_listRecordingRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_listRecordingRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_listRecordingRequest_set_recordingId(&codec, recording_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_listRecordingRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_list_recordings(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t from_recording_id,
    int32_t record_count)
{
    struct aeron_archive_client_listRecordingsRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_listRecordingsRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_listRecordingsRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_listRecordingsRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_listRecordingsRequest_set_fromRecordingId(&codec, from_recording_id);
    aeron_archive_client_listRecordingsRequest_set_recordCount(&codec, record_count);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_listRecordingsRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_list_recordings_for_uri(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t from_recording_id,
    int32_t record_count,
    const char *channel_fragment,
    int32_t stream_id)
{
    struct aeron_archive_client_listRecordingsForUriRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_listRecordingsForUriRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_listRecordingsForUriRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_listRecordingsForUriRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_listRecordingsForUriRequest_set_fromRecordingId(&codec, from_recording_id);
    aeron_archive_client_listRecordingsForUriRequest_set_recordCount(&codec, record_count);
    aeron_archive_client_listRecordingsForUriRequest_set_streamId(&codec, stream_id);
    aeron_archive_client_listRecordingsForUriRequest_put_channel(
        &codec,
        channel_fragment,
        strlen(channel_fragment));

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_listRecordingsForUriRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_replay(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params)
{
    size_t length;
    struct aeron_archive_client_messageHeader hdr;

    if (aeron_archive_replay_params_is_bounded(params))
    {
        struct aeron_archive_client_boundedReplayRequest codec;

        aeron_archive_client_boundedReplayRequest_wrap_and_apply_header(
            &codec,
            (char *)archive_proxy->buffer,
            0,
            AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
            &hdr);
        aeron_archive_client_boundedReplayRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
        aeron_archive_client_boundedReplayRequest_set_correlationId(&codec, correlation_id);
        aeron_archive_client_boundedReplayRequest_set_recordingId(&codec, recording_id);
        aeron_archive_client_boundedReplayRequest_set_position(&codec, params->position);
        aeron_archive_client_boundedReplayRequest_set_length(&codec, params->length);
        aeron_archive_client_boundedReplayRequest_set_limitCounterId(&codec, params->bounding_limit_counter_id);
        aeron_archive_client_boundedReplayRequest_set_replayStreamId(&codec, replay_stream_id);
        aeron_archive_client_boundedReplayRequest_set_fileIoMaxLength(&codec, params->file_io_max_length);
        aeron_archive_client_boundedReplayRequest_set_replayToken(&codec, params->replay_token);
        aeron_archive_client_boundedReplayRequest_put_replayChannel(
            &codec,
           replay_channel,
            strlen(replay_channel));

        length = aeron_archive_client_boundedReplayRequest_encoded_length(&codec);
    }
    else
    {
        struct aeron_archive_client_replayRequest codec;

        aeron_archive_client_replayRequest_wrap_and_apply_header(
            &codec,
            (char *)archive_proxy->buffer,
            0,
            AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
            &hdr);
        aeron_archive_client_replayRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
        aeron_archive_client_replayRequest_set_correlationId(&codec, correlation_id);
        aeron_archive_client_replayRequest_set_recordingId(&codec, recording_id);
        aeron_archive_client_replayRequest_set_position(&codec, params->position);
        aeron_archive_client_replayRequest_set_length(&codec, params->length);
        aeron_archive_client_replayRequest_set_replayStreamId(&codec, replay_stream_id);
        aeron_archive_client_replayRequest_set_fileIoMaxLength(&codec, params->file_io_max_length);
        aeron_archive_client_replayRequest_set_replayToken(&codec, params->replay_token);
        aeron_archive_client_replayRequest_put_replayChannel(
            &codec,
            replay_channel,
            strlen(replay_channel));

        length = aeron_archive_client_replayRequest_encoded_length(&codec);
    }

    return aeron_archive_proxy_offer(archive_proxy, length);
}

bool aeron_archive_proxy_truncate_recording(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t position)
{
    struct aeron_archive_client_truncateRecordingRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_truncateRecordingRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_truncateRecordingRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_truncateRecordingRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_truncateRecordingRequest_set_recordingId(&codec, recording_id);
    aeron_archive_client_truncateRecordingRequest_set_position(&codec, position);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_truncateRecordingRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_stop_replay(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t replay_session_id)
{
    struct aeron_archive_client_stopReplayRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_stopReplayRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_stopReplayRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_stopReplayRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_stopReplayRequest_set_replaySessionId(&codec, replay_session_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_stopReplayRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_stop_all_replays(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id)
{
    struct aeron_archive_client_stopAllReplaysRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_stopAllReplaysRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_stopAllReplaysRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_stopAllReplaysRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_stopAllReplaysRequest_set_recordingId(&codec, recording_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_stopAllReplaysRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_list_recording_subscriptions(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int32_t pseudo_index,
    int32_t subscription_count,
    const char *channel_fragment,
    int32_t stream_id,
    bool apply_stream_id)
{
    struct aeron_archive_client_listRecordingSubscriptionsRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_listRecordingSubscriptionsRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_listRecordingSubscriptionsRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_listRecordingSubscriptionsRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_listRecordingSubscriptionsRequest_set_pseudoIndex(&codec, pseudo_index);
    aeron_archive_client_listRecordingSubscriptionsRequest_set_subscriptionCount(&codec, subscription_count);
    aeron_archive_client_listRecordingSubscriptionsRequest_set_applyStreamId(
        &codec,
        apply_stream_id ? aeron_archive_client_booleanType_TRUE : aeron_archive_client_booleanType_FALSE);
    aeron_archive_client_listRecordingSubscriptionsRequest_set_streamId(&codec, stream_id);
    aeron_archive_client_listRecordingSubscriptionsRequest_put_channel(
        &codec,
        channel_fragment,
        strlen(channel_fragment));

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_listRecordingSubscriptionsRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_purge_recording(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id)
{
    struct aeron_archive_client_purgeRecordingRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_purgeRecordingRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_purgeRecordingRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_purgeRecordingRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_purgeRecordingRequest_set_recordingId(&codec, recording_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_purgeRecordingRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_extend_recording(
    aeron_archive_proxy_t *archive_proxy,
    int64_t recording_id,
    const char *recording_channel,
    int32_t recording_stream_id,
    bool local_source,
    bool auto_stop,
    int64_t correlation_id)
{
    struct aeron_archive_client_extendRecordingRequest2 codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_extendRecordingRequest2_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_extendRecordingRequest2_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_extendRecordingRequest2_set_correlationId(&codec, correlation_id);
    aeron_archive_client_extendRecordingRequest2_set_recordingId(&codec, recording_id);
    aeron_archive_client_extendRecordingRequest2_set_streamId(&codec, recording_stream_id);
    aeron_archive_client_extendRecordingRequest2_set_sourceLocation(
        &codec,
        local_source ? aeron_archive_client_sourceLocation_LOCAL : aeron_archive_client_sourceLocation_REMOTE);
    aeron_archive_client_extendRecordingRequest2_set_autoStop(
        &codec,
        auto_stop ? aeron_archive_client_booleanType_TRUE : aeron_archive_client_booleanType_FALSE);
    aeron_archive_client_extendRecordingRequest2_put_channel(
        &codec,
        recording_channel,
        strlen(recording_channel));

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_extendRecordingRequest2_encoded_length(&codec));
}

bool aeron_archive_proxy_replicate(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t src_recording_id,
    int32_t src_control_stream_id,
    const char *src_control_channel,
    aeron_archive_replication_params_t *params)
{
    struct aeron_archive_client_replicateRequest2 codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_replicateRequest2_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_replicateRequest2_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_replicateRequest2_set_correlationId(&codec, correlation_id);
    aeron_archive_client_replicateRequest2_set_srcRecordingId(&codec, src_recording_id);
    aeron_archive_client_replicateRequest2_set_dstRecordingId(&codec, params->dst_recording_id);
    aeron_archive_client_replicateRequest2_set_stopPosition(&codec, params->stop_position);
    aeron_archive_client_replicateRequest2_set_channelTagId(&codec, params->channel_tag_id);
    aeron_archive_client_replicateRequest2_set_subscriptionTagId(&codec, params->subscription_tag_id);
    aeron_archive_client_replicateRequest2_set_srcControlStreamId(&codec, src_control_stream_id);
    aeron_archive_client_replicateRequest2_set_fileIoMaxLength(&codec, params->file_io_max_length);
    aeron_archive_client_replicateRequest2_set_replicationSessionId(&codec, params->replication_session_id);

    aeron_archive_client_replicateRequest2_put_srcControlChannel(
        &codec,
        src_control_channel,
        strlen(src_control_channel));
    aeron_archive_client_replicateRequest2_put_liveDestination(
        &codec,
        params->live_destination,
        strlen(params->live_destination));
    aeron_archive_client_replicateRequest2_put_replicationChannel(
        &codec,
        params->replication_channel,
        strlen(params->replication_channel));
    aeron_archive_client_replicateRequest2_put_encodedCredentials(
        &codec,
        NULL == params->encoded_credentials ? "" : params->encoded_credentials->data,
        NULL == params->encoded_credentials ? 0 : params->encoded_credentials->length);
    aeron_archive_client_replicateRequest2_put_srcResponseChannel(
        &codec,
        params->src_response_channel,
        strlen(params->src_response_channel));

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_replicateRequest2_encoded_length(&codec));
}


bool aeron_archive_proxy_stop_replication(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t replication_id)
{
    struct aeron_archive_client_stopReplicationRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_stopReplicationRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_stopReplicationRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_stopReplicationRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_stopReplicationRequest_set_replicationId(&codec, replication_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_stopReplicationRequest_encoded_length(&codec));
}

bool aeron_archive_request_replay_token(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id)
{
    struct aeron_archive_client_replayTokenRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_replayTokenRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_replayTokenRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_replayTokenRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_replayTokenRequest_set_recordingId(&codec, recording_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_replayTokenRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_detach_segments(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t new_start_position)
{
    struct aeron_archive_client_detachSegmentsRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_detachSegmentsRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_detachSegmentsRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_detachSegmentsRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_detachSegmentsRequest_set_recordingId(&codec, recording_id);
    aeron_archive_client_detachSegmentsRequest_set_newStartPosition(&codec, new_start_position);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_detachSegmentsRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_delete_detached_segments(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id)
{
    struct aeron_archive_client_deleteDetachedSegmentsRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_deleteDetachedSegmentsRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_deleteDetachedSegmentsRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_deleteDetachedSegmentsRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_deleteDetachedSegmentsRequest_set_recordingId(&codec, recording_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_deleteDetachedSegmentsRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_purge_segments(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t new_start_position)
{
    struct aeron_archive_client_purgeSegmentsRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_purgeSegmentsRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_purgeSegmentsRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_purgeSegmentsRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_purgeSegmentsRequest_set_recordingId(&codec, recording_id);
    aeron_archive_client_purgeSegmentsRequest_set_newStartPosition(&codec, new_start_position);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_purgeSegmentsRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_attach_segments(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t recording_id)
{
    struct aeron_archive_client_attachSegmentsRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_attachSegmentsRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_attachSegmentsRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_attachSegmentsRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_attachSegmentsRequest_set_recordingId(&codec, recording_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_attachSegmentsRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_migrate_segments(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t src_recording_id,
    int64_t dst_recording_id)
{
    struct aeron_archive_client_migrateSegmentsRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_migrateSegmentsRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_migrateSegmentsRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_migrateSegmentsRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_migrateSegmentsRequest_set_srcRecordingId(&codec, src_recording_id);
    aeron_archive_client_migrateSegmentsRequest_set_dstRecordingId(&codec, dst_recording_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_migrateSegmentsRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_update_channel(
        aeron_archive_proxy_t *archive_proxy,
        int64_t correlation_id,
        int64_t recording_id,
        const char *new_channel)
{
    struct aeron_archive_client_updateChannelRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_updateChannelRequest_wrap_and_apply_header(
            &codec,
            (char *)archive_proxy->buffer,
            0,
            AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
            &hdr);
    aeron_archive_client_updateChannelRequest_set_controlSessionId(&codec, archive_proxy->control_session_id);
    aeron_archive_client_updateChannelRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_updateChannelRequest_set_recordingId(&codec, recording_id);
    aeron_archive_client_updateChannelRequest_put_channel(&codec, new_channel, strlen(new_channel));

    return aeron_archive_proxy_offer(
            archive_proxy, aeron_archive_client_updateChannelRequest_encoded_length(&codec));
}

/* ************* */

// The length here must NOT include the messageHeader encoded length
int64_t aeron_archive_proxy_offer_once(aeron_archive_proxy_t *archive_proxy, size_t length)
{
    return aeron_exclusive_publication_offer(
        archive_proxy->exclusive_publication,
        archive_proxy->buffer,
        aeron_archive_client_messageHeader_encoded_length() + length,
        NULL,
        NULL);
}

// The length here must NOT include the messageHeader encoded length
bool aeron_archive_proxy_offer(
    aeron_archive_proxy_t *archive_proxy,
    size_t length)
{
    int attempts = archive_proxy->retry_attempts;

    while (true)
    {
        int64_t result = aeron_archive_proxy_offer_once(archive_proxy, length);

        if (result > 0)
        {
            return true;
        }

        if (AERON_PUBLICATION_CLOSED == result ||
            AERON_PUBLICATION_NOT_CONNECTED == result ||
            AERON_PUBLICATION_MAX_POSITION_EXCEEDED == result)
        {
            AERON_APPEND_ERR("%s", "");
            return false;
        }

        if (--attempts <= 0)
        {
            AERON_SET_ERR(-1, "%s", "too many retries");
            return false;
        }

        aeron_archive_context_idle(archive_proxy->ctx);
    }
}
