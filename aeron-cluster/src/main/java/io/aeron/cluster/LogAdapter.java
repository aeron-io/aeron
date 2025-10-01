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

import io.aeron.Aeron;
import io.aeron.BufferBuilder;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.cluster.codecs.*;
import io.aeron.cluster.service.ClusterClock;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;

import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;

final class LogAdapter implements ControlledFragmentHandler
{
    private final int fragmentLimit;
    private long logPosition;
    private Image image;
    private final ConsensusModuleAgent consensusModuleAgent;
    private final BufferBuilder builder = new BufferBuilder();
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionOpenEventDecoder sessionOpenEventDecoder = new SessionOpenEventDecoder();
    private final SessionCloseEventDecoder sessionCloseEventDecoder = new SessionCloseEventDecoder();
    private final SessionMessageHeaderDecoder sessionHeaderDecoder = new SessionMessageHeaderDecoder();
    private final TimerEventDecoder timerEventDecoder = new TimerEventDecoder();
    private final ClusterActionRequestDecoder clusterActionRequestDecoder = new ClusterActionRequestDecoder();
    private final NewLeadershipTermEventDecoder newLeadershipTermEventDecoder = new NewLeadershipTermEventDecoder();

    LogAdapter(final ConsensusModuleAgent consensusModuleAgent, final int fragmentLimit)
    {
        this.consensusModuleAgent = consensusModuleAgent;
        this.fragmentLimit = fragmentLimit;
    }

    long disconnect(final ErrorHandler errorHandler)
    {
        long registrationId = Aeron.NULL_VALUE;

        if (null != image)
        {
            logPosition = image.position();
            CloseHelper.close(errorHandler, image.subscription());
            registrationId = image.subscription().registrationId();
            image = null;
        }

        return registrationId;
    }

    void disconnect(final ErrorHandler errorHandler, final long maxLogPosition)
    {
        consensusModuleAgent.awaitLocalSocketsClosed(disconnect(errorHandler));
        logPosition = Math.min(logPosition, maxLogPosition);
    }

    Subscription subscription()
    {
        return null == image ? null : image.subscription();
    }

    ConsensusModuleAgent consensusModuleAgent()
    {
        return consensusModuleAgent;
    }

    long position()
    {
        if (null == image)
        {
            return logPosition;
        }

        return image.position();
    }

    int poll(final long boundPosition)
    {
        if (null == image)
        {
            return 0;
        }

        return image.boundedControlledPoll(this, boundPosition, fragmentLimit);
    }

    boolean isImageClosed()
    {
        return null == image || image.isClosed();
    }

    boolean isLogEndOfStream()
    {
        return null != image && image.isEndOfStream();
    }

    boolean isLogEndOfStreamAt(final long position)
    {
        return null != image && position == image.endOfStreamPosition();
    }

    Image image()
    {
        return image;
    }

    void image(final Image image)
    {
        if (null != this.image)
        {
            logPosition = this.image.position();
        }

        this.image = image;
    }

    void asyncRemoveDestination(final String destination)
    {
        if (null != image && !image.subscription().isClosed())
        {
            image.subscription().asyncRemoveDestination(destination);
        }
    }

    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        Action action = Action.CONTINUE;
        final byte flags = header.flags();

        if ((flags & UNFRAGMENTED) == UNFRAGMENTED)
        {
            action = onMessage(buffer, offset, length, header);
        }
        else if ((flags & BEGIN_FRAG_FLAG) == BEGIN_FRAG_FLAG)
        {
            builder.reset()
                .captureHeader(header)
                .append(buffer, offset, length)
                .nextTermOffset(BitUtil.align(offset + length + HEADER_LENGTH, FRAME_ALIGNMENT));
        }
        else if (offset == builder.nextTermOffset())
        {
            final int limit = builder.limit();

            builder.append(buffer, offset, length);

            if ((flags & END_FRAG_FLAG) == END_FRAG_FLAG)
            {
                action = onMessage(builder.buffer(), 0, builder.limit(), builder.completeHeader(header));

                if (Action.ABORT == action)
                {
                    builder.limit(limit);
                }
                else
                {
                    builder.reset();
                }
            }
            else
            {
                builder.nextTermOffset(BitUtil.align(offset + length + HEADER_LENGTH, FRAME_ALIGNMENT));
            }
        }
        else
        {
            builder.reset();
        }

        return action;
    }

    @SuppressWarnings("MethodLength")
    private Action onMessage(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        final int templateId = messageHeaderDecoder.templateId();
        final int actingVersion = messageHeaderDecoder.version();
        final int actingBlockLength = messageHeaderDecoder.blockLength();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            return consensusModuleAgent.onReplayExtensionMessage(
                actingBlockLength, templateId, schemaId, actingVersion, buffer, offset, length, header);
        }

        switch (templateId)
        {
            case SessionMessageHeaderDecoder.TEMPLATE_ID:
                sessionHeaderDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    actingVersion);

                consensusModuleAgent.onReplaySessionMessage(
                    sessionHeaderDecoder.clusterSessionId(),
                    sessionHeaderDecoder.timestamp());

                return Action.CONTINUE;

            case TimerEventDecoder.TEMPLATE_ID:
                timerEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    actingVersion);

                consensusModuleAgent.onReplayTimerEvent(
                    timerEventDecoder.correlationId());
                break;

            case SessionOpenEventDecoder.TEMPLATE_ID:
                sessionOpenEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    actingVersion);

                consensusModuleAgent.onReplaySessionOpen(
                    header.position(),
                    sessionOpenEventDecoder.correlationId(),
                    sessionOpenEventDecoder.clusterSessionId(),
                    sessionOpenEventDecoder.timestamp(),
                    sessionOpenEventDecoder.responseStreamId(),
                    sessionOpenEventDecoder.responseChannel());
                break;

            case SessionCloseEventDecoder.TEMPLATE_ID:
                sessionCloseEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    actingVersion);

                consensusModuleAgent.onReplaySessionClose(
                    sessionCloseEventDecoder.clusterSessionId(),
                    sessionCloseEventDecoder.closeReason());
                break;

            case ClusterActionRequestDecoder.TEMPLATE_ID:
                clusterActionRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    actingVersion);

                final int flags = ClusterActionRequestDecoder.flagsNullValue() != clusterActionRequestDecoder.flags() ?
                    clusterActionRequestDecoder.flags() : ConsensusModule.CLUSTER_ACTION_FLAGS_DEFAULT;

                consensusModuleAgent.onReplayClusterAction(
                    clusterActionRequestDecoder.leadershipTermId(),
                    clusterActionRequestDecoder.logPosition(),
                    clusterActionRequestDecoder.timestamp(),
                    clusterActionRequestDecoder.action(),
                    flags);
                return Action.BREAK;

            case NewLeadershipTermEventDecoder.TEMPLATE_ID:
                newLeadershipTermEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    actingVersion);

                consensusModuleAgent.onReplayNewLeadershipTermEvent(
                    newLeadershipTermEventDecoder.leadershipTermId(),
                    newLeadershipTermEventDecoder.logPosition(),
                    newLeadershipTermEventDecoder.timestamp(),
                    newLeadershipTermEventDecoder.termBaseLogPosition(),
                    ClusterClock.map(newLeadershipTermEventDecoder.timeUnit()),
                    newLeadershipTermEventDecoder.appVersion());
                break;
        }

        return Action.CONTINUE;
    }
}
