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
package io.aeron.archive;

import io.aeron.archive.codecs.*;

class ControlRequestDecoders
{
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    final ConnectRequestDecoder connectRequest = new ConnectRequestDecoder();
    final CloseSessionRequestDecoder closeSessionRequest = new CloseSessionRequestDecoder();
    final StartRecordingRequestDecoder startRecordingRequest = new StartRecordingRequestDecoder();
    final StartRecordingRequest2Decoder startRecordingRequest2 = new StartRecordingRequest2Decoder();
    final StopRecordingRequestDecoder stopRecordingRequest = new StopRecordingRequestDecoder();
    final ReplayRequestDecoder replayRequest = new ReplayRequestDecoder();
    final StopReplayRequestDecoder stopReplayRequest = new StopReplayRequestDecoder();
    final ListRecordingsRequestDecoder listRecordingsRequest = new ListRecordingsRequestDecoder();
    final ListRecordingsForUriRequestDecoder listRecordingsForUriRequest = new ListRecordingsForUriRequestDecoder();
    final ListRecordingRequestDecoder listRecordingRequest = new ListRecordingRequestDecoder();
    final ExtendRecordingRequestDecoder extendRecordingRequest = new ExtendRecordingRequestDecoder();
    final ExtendRecordingRequest2Decoder extendRecordingRequest2 = new ExtendRecordingRequest2Decoder();
    final RecordingPositionRequestDecoder recordingPositionRequest = new RecordingPositionRequestDecoder();
    final TruncateRecordingRequestDecoder truncateRecordingRequest = new TruncateRecordingRequestDecoder();
    final PurgeRecordingRequestDecoder purgeRecordingRequest = new PurgeRecordingRequestDecoder();
    final StopRecordingSubscriptionRequestDecoder stopRecordingSubscriptionRequest =
        new StopRecordingSubscriptionRequestDecoder();
    final StopPositionRequestDecoder stopPositionRequest = new StopPositionRequestDecoder();
    final FindLastMatchingRecordingRequestDecoder findLastMatchingRecordingRequest =
        new FindLastMatchingRecordingRequestDecoder();
    final ListRecordingSubscriptionsRequestDecoder listRecordingSubscriptionsRequest =
        new ListRecordingSubscriptionsRequestDecoder();
    final StopRecordingByIdentityRequestDecoder stopRecordingByIdentityRequest =
        new StopRecordingByIdentityRequestDecoder();
    final BoundedReplayRequestDecoder boundedReplayRequest = new BoundedReplayRequestDecoder();
    final StopAllReplaysRequestDecoder stopAllReplaysRequest = new StopAllReplaysRequestDecoder();
    final ReplicateRequestDecoder replicateRequest = new ReplicateRequestDecoder();
    final ReplicateRequest2Decoder replicateRequest2 = new ReplicateRequest2Decoder();
    final StopReplicationRequestDecoder stopReplicationRequest = new StopReplicationRequestDecoder();
    final StartPositionRequestDecoder startPositionRequest = new StartPositionRequestDecoder();
    final DetachSegmentsRequestDecoder detachSegmentsRequest = new DetachSegmentsRequestDecoder();
    final DeleteDetachedSegmentsRequestDecoder deleteDetachedSegmentsRequest =
        new DeleteDetachedSegmentsRequestDecoder();
    final PurgeSegmentsRequestDecoder purgeSegmentsRequest = new PurgeSegmentsRequestDecoder();
    final MaxRecordedPositionRequestDecoder maxRecordedPositionRequest =
        new MaxRecordedPositionRequestDecoder();
    final AttachSegmentsRequestDecoder attachSegmentsRequest = new AttachSegmentsRequestDecoder();
    final MigrateSegmentsRequestDecoder migrateSegmentsRequest = new MigrateSegmentsRequestDecoder();
    final AuthConnectRequestDecoder authConnectRequest = new AuthConnectRequestDecoder();
    final ChallengeResponseDecoder challengeResponse = new ChallengeResponseDecoder();
    final KeepAliveRequestDecoder keepAliveRequest = new KeepAliveRequestDecoder();
    final TaggedReplicateRequestDecoder taggedReplicateRequest = new TaggedReplicateRequestDecoder();
    final ArchiveIdRequestDecoder archiveIdRequestDecoder = new ArchiveIdRequestDecoder();
    final ReplayTokenRequestDecoder replayTokenRequestDecoder = new ReplayTokenRequestDecoder();
    final UpdateChannelRequestDecoder updateChannelRequestDecoder = new UpdateChannelRequestDecoder();
}
