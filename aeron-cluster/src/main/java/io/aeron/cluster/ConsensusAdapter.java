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
package io.aeron.cluster;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.ArrayUtil;

import java.util.ArrayList;
import java.util.List;

class ConsensusAdapter implements FragmentHandler, AutoCloseable
{
    static final int FRAGMENT_LIMIT = 10;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final CanvassPositionDecoder canvassPositionDecoder = new CanvassPositionDecoder();
    private final RequestVoteDecoder requestVoteDecoder = new RequestVoteDecoder();
    private final VoteDecoder voteDecoder = new VoteDecoder();
    private final NewLeadershipTermDecoder newLeadershipTermDecoder = new NewLeadershipTermDecoder();
    private final AppendPositionDecoder appendPositionDecoder = new AppendPositionDecoder();
    private final CommitPositionDecoder commitPositionDecoder = new CommitPositionDecoder();
    private final CatchupPositionDecoder catchupPositionDecoder = new CatchupPositionDecoder();
    private final StopCatchupDecoder stopCatchupDecoder = new StopCatchupDecoder();

    private final TerminationPositionDecoder terminationPositionDecoder = new TerminationPositionDecoder();
    private final TerminationAckDecoder terminationAckDecoder = new TerminationAckDecoder();
    private final BackupQueryDecoder backupQueryDecoder = new BackupQueryDecoder();
    private final ChallengeResponseDecoder challengeResponseDecoder = new ChallengeResponseDecoder();
    private final HeartbeatRequestDecoder heartbeatRequestDecoder = new HeartbeatRequestDecoder();
    private final StandbySnapshotDecoder standbySnapshotDecoder = new StandbySnapshotDecoder();

    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this);
    private final Subscription subscription;
    private final ConsensusModuleAgent consensusModuleAgent;

    ConsensusAdapter(final Subscription subscription, final ConsensusModuleAgent consensusModuleAgent)
    {
        this.subscription = subscription;
        this.consensusModuleAgent = consensusModuleAgent;
    }

    public void close()
    {
        CloseHelper.close(subscription);
    }

    public int poll()
    {
        return subscription.poll(fragmentAssembler, FRAGMENT_LIMIT);
    }

    public int poll(final int limit)
    {
        return subscription.poll(fragmentAssembler, limit);
    }

    @SuppressWarnings("MethodLength")
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        switch (messageHeaderDecoder.templateId())
        {
            case CanvassPositionDecoder.TEMPLATE_ID:
                canvassPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onCanvassPosition(
                    canvassPositionDecoder.logLeadershipTermId(),
                    canvassPositionDecoder.logPosition(),
                    canvassPositionDecoder.leadershipTermId(),
                    canvassPositionDecoder.followerMemberId(),
                    canvassPositionDecoder.protocolVersion());
                break;

            case RequestVoteDecoder.TEMPLATE_ID:
                requestVoteDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onRequestVote(
                    requestVoteDecoder.logLeadershipTermId(),
                    requestVoteDecoder.logPosition(),
                    requestVoteDecoder.candidateTermId(),
                    requestVoteDecoder.candidateMemberId(),
                    requestVoteDecoder.protocolVersion());
                break;

            case VoteDecoder.TEMPLATE_ID:
                voteDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onVote(
                    voteDecoder.candidateTermId(),
                    voteDecoder.logLeadershipTermId(),
                    voteDecoder.logPosition(),
                    voteDecoder.candidateMemberId(),
                    voteDecoder.followerMemberId(),
                    voteDecoder.vote() == BooleanType.TRUE);
                break;

            case NewLeadershipTermDecoder.TEMPLATE_ID:
                newLeadershipTermDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long rawCommitPosition = newLeadershipTermDecoder.commitPosition();
                final long commitPosition =
                    NewLeadershipTermDecoder.commitPositionNullValue() == rawCommitPosition ?
                        AeronArchive.NULL_POSITION : rawCommitPosition;

                consensusModuleAgent.onNewLeadershipTerm(
                    newLeadershipTermDecoder.logLeadershipTermId(),
                    newLeadershipTermDecoder.nextLeadershipTermId(),
                    newLeadershipTermDecoder.nextTermBaseLogPosition(),
                    newLeadershipTermDecoder.nextLogPosition(),
                    newLeadershipTermDecoder.leadershipTermId(),
                    newLeadershipTermDecoder.termBaseLogPosition(),
                    newLeadershipTermDecoder.logPosition(),
                    commitPosition,
                    newLeadershipTermDecoder.leaderRecordingId(),
                    newLeadershipTermDecoder.timestamp(),
                    newLeadershipTermDecoder.leaderMemberId(),
                    newLeadershipTermDecoder.logSessionId(),
                    newLeadershipTermDecoder.appVersion(),
                    newLeadershipTermDecoder.isStartup() == BooleanType.TRUE);
                break;

            case AppendPositionDecoder.TEMPLATE_ID:
                appendPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final short flagsDecodedValue = appendPositionDecoder.flags();
                final short flags = AppendPositionDecoder.flagsNullValue() == flagsDecodedValue ?
                    ConsensusModuleAgent.APPEND_POSITION_FLAG_NONE : flagsDecodedValue;

                consensusModuleAgent.onAppendPosition(
                    appendPositionDecoder.leadershipTermId(),
                    appendPositionDecoder.logPosition(),
                    appendPositionDecoder.followerMemberId(),
                    flags);

                break;

            case CommitPositionDecoder.TEMPLATE_ID:
                commitPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onCommitPosition(
                    commitPositionDecoder.leadershipTermId(),
                    commitPositionDecoder.logPosition(),
                    commitPositionDecoder.leaderMemberId());
                break;

            case CatchupPositionDecoder.TEMPLATE_ID:
                catchupPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onCatchupPosition(
                    catchupPositionDecoder.leadershipTermId(),
                    catchupPositionDecoder.logPosition(),
                    catchupPositionDecoder.followerMemberId(),
                    catchupPositionDecoder.catchupEndpoint());
                break;

            case StopCatchupDecoder.TEMPLATE_ID:
                stopCatchupDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onStopCatchup(
                    stopCatchupDecoder.leadershipTermId(),
                    stopCatchupDecoder.followerMemberId());
                break;

            case TerminationPositionDecoder.TEMPLATE_ID:
                terminationPositionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onTerminationPosition(
                    terminationPositionDecoder.leadershipTermId(),
                    terminationPositionDecoder.logPosition());
                break;

            case TerminationAckDecoder.TEMPLATE_ID:
                terminationAckDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onTerminationAck(
                    terminationAckDecoder.leadershipTermId(),
                    terminationAckDecoder.logPosition(),
                    terminationAckDecoder.memberId());
                break;

            case BackupQueryDecoder.TEMPLATE_ID:
            {
                backupQueryDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final String responseChannel = backupQueryDecoder.responseChannel();
                final int credentialsLength = backupQueryDecoder.encodedCredentialsLength();
                final byte[] credentials;
                if (credentialsLength > 0)
                {
                    credentials = new byte[credentialsLength];
                    backupQueryDecoder.getEncodedCredentials(credentials, 0, credentials.length);
                }
                else
                {
                    credentials = ArrayUtil.EMPTY_BYTE_ARRAY;
                }

                consensusModuleAgent.onBackupQuery(
                    backupQueryDecoder.correlationId(),
                    backupQueryDecoder.responseStreamId(),
                    backupQueryDecoder.version(),
                    responseChannel,
                    credentials,
                    header);
                break;
            }

            case ChallengeResponseDecoder.TEMPLATE_ID:
            {
                challengeResponseDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final byte[] credentials = new byte[challengeResponseDecoder.encodedCredentialsLength()];
                challengeResponseDecoder.getEncodedCredentials(credentials, 0, credentials.length);

                consensusModuleAgent.onConsensusChallengeResponse(
                    challengeResponseDecoder.correlationId(),
                    challengeResponseDecoder.clusterSessionId(),
                    credentials);
                break;
            }

            case HeartbeatRequestDecoder.TEMPLATE_ID:
            {
                heartbeatRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final String responseChannel = heartbeatRequestDecoder.responseChannel();
                final int credentialsLength = heartbeatRequestDecoder.encodedCredentialsLength();
                final byte[] credentials;
                if (credentialsLength > 0)
                {
                    credentials = new byte[credentialsLength];
                    heartbeatRequestDecoder.getEncodedCredentials(credentials, 0, credentials.length);
                }
                else
                {
                    heartbeatRequestDecoder.skipEncodedCredentials();
                    credentials = ArrayUtil.EMPTY_BYTE_ARRAY;
                }

                consensusModuleAgent.onHeartbeatRequest(
                    heartbeatRequestDecoder.correlationId(),
                    heartbeatRequestDecoder.responseStreamId(),
                    responseChannel,
                    credentials,
                    header);

                break;
            }

            case StandbySnapshotDecoder.TEMPLATE_ID:
            {
                standbySnapshotDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long correlationId = standbySnapshotDecoder.correlationId();
                final int version = standbySnapshotDecoder.version();
                final int responseStreamId = standbySnapshotDecoder.responseStreamId();
                final List<StandbySnapshotEntry> standbySnapshotEntries = new ArrayList<>();

                for (final StandbySnapshotDecoder.SnapshotsDecoder standbySnapshot : standbySnapshotDecoder.snapshots())
                {
                    standbySnapshotEntries.add(new StandbySnapshotEntry(
                        standbySnapshot.recordingId(),
                        standbySnapshot.leadershipTermId(),
                        standbySnapshot.termBaseLogPosition(),
                        standbySnapshot.logPosition(),
                        standbySnapshot.timestamp(),
                        standbySnapshot.serviceId(),
                        standbySnapshot.archiveEndpoint()));
                }

                final String responseChannel = standbySnapshotDecoder.responseChannel();
                final byte[] encodedCredentials;
                if (0 == standbySnapshotDecoder.encodedCredentialsLength())
                {
                    standbySnapshotDecoder.skipEncodedCredentials();
                    encodedCredentials = ArrayUtil.EMPTY_BYTE_ARRAY;
                }
                else
                {
                    encodedCredentials = new byte[standbySnapshotDecoder.encodedCredentialsLength()];
                    standbySnapshotDecoder.getEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);
                }

                consensusModuleAgent.onStandbySnapshot(
                    correlationId,
                    version,
                    standbySnapshotEntries,
                    responseStreamId,
                    responseChannel,
                    encodedCredentials,
                    header);
            }
        }
    }
}
