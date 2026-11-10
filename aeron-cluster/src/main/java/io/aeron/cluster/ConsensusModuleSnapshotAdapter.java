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

import io.aeron.Image;
import io.aeron.ImageControlledFragmentAssembler;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.cluster.service.ClusterClock;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

import static io.aeron.cluster.ConsensusModule.Configuration.SNAPSHOT_TYPE_ID;

class ConsensusModuleSnapshotAdapter implements ControlledFragmentHandler
{
    static final int FRAGMENT_LIMIT = 10;

    private boolean inSnapshot = false;
    private boolean isDone = false;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SnapshotMarkerDecoder snapshotMarkerDecoder = new SnapshotMarkerDecoder();
    private final ClusterSessionDecoder clusterSessionDecoder = new ClusterSessionDecoder();
    private final SessionMessageHeaderDecoder sessionMessageHeaderDecoder = new SessionMessageHeaderDecoder();
    private final TimerDecoder timerDecoder = new TimerDecoder();
    private final ConsensusModuleDecoder consensusModuleDecoder = new ConsensusModuleDecoder();
    private final ClusterMembersDecoder clusterMembersDecoder = new ClusterMembersDecoder();
    private final PendingMessageTrackerDecoder pendingMessageTrackerDecoder = new PendingMessageTrackerDecoder();
    private final ImageControlledFragmentAssembler fragmentAssembler = new ImageControlledFragmentAssembler(this);
    private final Image image;
    private final ConsensusModuleSnapshotListener listener;

    ConsensusModuleSnapshotAdapter(final Image image, final ConsensusModuleSnapshotListener listener)
    {
        this.image = image;
        this.listener = listener;
    }

    boolean isDone()
    {
        return isDone;
    }

    int poll()
    {
        return image.controlledPoll(fragmentAssembler, FRAGMENT_LIMIT);
    }

    @Override
    @SuppressWarnings("MethodLength")
    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        if (MessageHeaderDecoder.SCHEMA_ID != schemaId)
        {
            throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        switch (messageHeaderDecoder.templateId())
        {
            case SessionMessageHeaderDecoder.TEMPLATE_ID:
                sessionMessageHeaderDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                listener.onLoadPendingMessage(sessionMessageHeaderDecoder.clusterSessionId(), buffer, offset, length);
                break;

            case SnapshotMarkerDecoder.TEMPLATE_ID:
                snapshotMarkerDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long typeId = snapshotMarkerDecoder.typeId();
                if (SNAPSHOT_TYPE_ID != typeId)
                {
                    throw new ClusterException("unexpected snapshot type: " + typeId);
                }

                switch (snapshotMarkerDecoder.mark())
                {
                    case BEGIN:
                        if (inSnapshot)
                        {
                            throw new ClusterException("already in snapshot");
                        }
                        inSnapshot = true;

                        listener.onLoadBeginSnapshot(
                            snapshotMarkerDecoder.appVersion(),
                            ClusterClock.map(snapshotMarkerDecoder.timeUnit()),
                            buffer,
                            offset,
                            length);
                        return Action.CONTINUE;

                    case END:
                        if (!inSnapshot)
                        {
                            throw new ClusterException("missing begin snapshot");
                        }
                        listener.onLoadEndSnapshot(buffer, offset, length);
                        isDone = true;
                        return Action.BREAK;

                    case SECTION:
                    case NULL_VAL:
                        break;
                }
                break;

            case ClusterSessionDecoder.TEMPLATE_ID:
                clusterSessionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                listener.onLoadClusterSession(
                    clusterSessionDecoder.clusterSessionId(),
                    clusterSessionDecoder.correlationId(),
                    clusterSessionDecoder.openedLogPosition(),
                    clusterSessionDecoder.timeOfLastActivity(),
                    clusterSessionDecoder.closeReason(),
                    clusterSessionDecoder.responseStreamId(),
                    clusterSessionDecoder.responseChannel(),
                    buffer,
                    offset,
                    length);
                break;

            case TimerDecoder.TEMPLATE_ID:
                timerDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                listener.onLoadTimer(timerDecoder.correlationId(), timerDecoder.deadline(), buffer, offset, length);
                break;

            case ConsensusModuleDecoder.TEMPLATE_ID:
                consensusModuleDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                listener.onLoadConsensusModuleState(
                    consensusModuleDecoder.nextSessionId(),
                    consensusModuleDecoder.nextServiceSessionId(),
                    consensusModuleDecoder.logServiceSessionId(),
                    consensusModuleDecoder.pendingMessageCapacity(),
                    buffer,
                    offset,
                    length);
                break;

            case ClusterMembersDecoder.TEMPLATE_ID:
                // Ignored
                break;

            case PendingMessageTrackerDecoder.TEMPLATE_ID:
                pendingMessageTrackerDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                listener.onLoadPendingMessageTracker(
                    pendingMessageTrackerDecoder.nextServiceSessionId(),
                    pendingMessageTrackerDecoder.logServiceSessionId(),
                    pendingMessageTrackerDecoder.pendingMessageCapacity(),
                    pendingMessageTrackerDecoder.serviceId(),
                    buffer,
                    offset,
                    length);
                break;
        }

        return Action.CONTINUE;
    }
}
