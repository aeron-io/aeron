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
package io.aeron.archive.client;

import io.aeron.ControlledFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.codecs.ControlResponseDecoder;
import io.aeron.archive.codecs.MessageHeaderDecoder;
import io.aeron.archive.codecs.MessageHeaderEncoder;
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;

import static io.aeron.archive.client.ControlResponsePoller.FRAGMENT_LIMIT;

final class AsyncAeronArchive implements AutoCloseable
{
    private final ControlledFragmentAssembler fragmentAssembler = new ControlledFragmentAssembler(this::onFragment);
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();

    private final AeronArchive.Context context;
    private final AsyncAeronArchiveListener listener;
    private State state = State.CONNECTING;
    private AeronArchive.AsyncConnect asyncConnect;
    private AeronArchive aeronArchive;
    private ArchiveProxy archiveProxy;
    private Subscription subscription;
    private long controlSessionId;

    AsyncAeronArchive(final AeronArchive.Context context, final AsyncAeronArchiveListener listener)
    {
        this.context = context;
        this.listener = listener;
    }

    boolean trySendListRecordingRequest(final long correlationId, final long recordingId)
    {
        if (state == State.CONNECTED)
        {
            try
            {
                return archiveProxy.listRecording(recordingId, correlationId, controlSessionId);
            }
            catch (final ArchiveException e)
            {
                state = State.DISCONNECTED;
            }
        }

        return false;
    }

    boolean trySendMaxRecordedPositionRequest(final long correlationId, final long recordingId)
    {
        if (state == State.CONNECTED)
        {
            try
            {
                return archiveProxy.getMaxRecordedPosition(recordingId, correlationId, controlSessionId);
            }
            catch (final ArchiveException e)
            {
                state = State.DISCONNECTED;
            }
        }

        return false;
    }

    boolean trySendReplayTokenRequest(final long correlationId, final long recordingId)
    {
        if (state == State.CONNECTED)
        {
            try
            {
                return archiveProxy.requestReplayToken(
                    correlationId,
                    controlSessionId,
                    recordingId
                );
            }
            catch (final ArchiveException e)
            {
                state = State.DISCONNECTED;
            }
        }
        return false;
    }

    boolean trySendReplayRequest(
        final long correlationId,
        final long recordingId,
        final int replayStreamId,
        final String replayChannel,
        final ReplayParams replayParams)
    {
        return trySendReplayRequest(
            archiveProxy,
            correlationId,
            recordingId,
            replayStreamId,
            replayChannel,
            replayParams);
    }

    boolean trySendReplayRequest(
        final ArchiveProxy archiveProxy,
        final long correlationId,
        final long recordingId,
        final int replayStreamId,
        final String replayChannel,
        final ReplayParams replayParams)
    {
        if (state == State.CONNECTED)
        {
            try
            {
                return archiveProxy.replay(
                    recordingId,
                    replayChannel,
                    replayStreamId,
                    replayParams,
                    correlationId,
                    controlSessionId
                );
            }
            catch (final ArchiveException e)
            {
                state = State.DISCONNECTED;
            }
        }

        return false;
    }

    boolean trySendStopReplayRequest(final long correlationId, final long replaySessionId)
    {
        if (state == State.CONNECTED)
        {
            try
            {
                return archiveProxy.stopReplay(replaySessionId, correlationId, controlSessionId);
            }
            catch (final ArchiveException e)
            {
                state = State.DISCONNECTED;
            }
        }

        return false;
    }

    int poll()
    {
        return switch (state)
        {
            case CONNECTING -> connecting();
            case CONNECTED -> connected();
            case DISCONNECTED -> disconnected();
            case CLOSED -> 0;
        };
    }

    private int connecting()
    {
        int workCount = 0;

        AeronArchive.AsyncConnect asyncConnect = this.asyncConnect;

        if (asyncConnect == null)
        {
            try
            {
                asyncConnect = AeronArchive.asyncConnect(context.clone());
            }
            catch (final Exception e)
            {
                state = State.CLOSED;
                listener.onError(e);
                return 1;
            }

            this.asyncConnect = asyncConnect;
            workCount++;
        }

        AeronArchive aeronArchive = null;
        try
        {
            final int stepBefore = asyncConnect.step();
            aeronArchive = asyncConnect.poll();
            workCount += asyncConnect.step() - stepBefore;
        }
        catch (final Exception e)
        {
            this.asyncConnect = null;
            CloseHelper.close(e::addSuppressed, asyncConnect);
            listener.onError(e);
            workCount++;
        }

        if (aeronArchive != null)
        {
            this.asyncConnect = null;
            this.aeronArchive = aeronArchive;
            archiveProxy = aeronArchive.archiveProxy();
            subscription = aeronArchive.controlResponsePoller().subscription();
            controlSessionId = aeronArchive.controlSessionId();
            state = State.CONNECTED;
            listener.onConnected();
        }

        return workCount;
    }

    private int connected()
    {
        if (!subscription.isConnected())
        {
            state = State.DISCONNECTED;
            return 1;
        }

        return subscription.controlledPoll(fragmentAssembler, FRAGMENT_LIMIT);
    }

    private int disconnected()
    {
        CloseHelper.quietClose(aeronArchive);

        subscription = null;
        archiveProxy = null;
        aeronArchive = null;

        try
        {
            listener.onDisconnected();
        }
        finally
        {
            state = State.CONNECTING;
        }

        return 1;
    }

    boolean isConnected()
    {
        return state == State.CONNECTED;
    }

    boolean isClosed()
    {
        return state == State.CLOSED;
    }

    public void close()
    {
        if (state != State.CLOSED)
        {
            state = State.CLOSED;

            CloseHelper.quietCloseAll(asyncConnect, aeronArchive);
        }
    }

    private ControlledFragmentHandler.Action onFragment(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new ArchiveException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        final int templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case ControlResponseDecoder.TEMPLATE_ID:
                controlResponseDecoder.wrap(
                    buffer,
                    offset + MessageHeaderEncoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                if (controlResponseDecoder.controlSessionId() == controlSessionId)
                {
                    listener.onControlResponse(
                        controlResponseDecoder.correlationId(),
                        controlResponseDecoder.relevantId(),
                        controlResponseDecoder.code(),
                        controlResponseDecoder.errorMessage());
                }
                break;

            case RecordingDescriptorDecoder.TEMPLATE_ID:
                recordingDescriptorDecoder.wrap(
                    buffer,
                    offset + MessageHeaderEncoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                if (recordingDescriptorDecoder.controlSessionId() == controlSessionId)
                {
                    listener.onRecordingDescriptor(
                        controlSessionId,
                        recordingDescriptorDecoder.correlationId(),
                        recordingDescriptorDecoder.recordingId(),
                        recordingDescriptorDecoder.startTimestamp(),
                        recordingDescriptorDecoder.stopTimestamp(),
                        recordingDescriptorDecoder.startPosition(),
                        recordingDescriptorDecoder.stopPosition(),
                        recordingDescriptorDecoder.initialTermId(),
                        recordingDescriptorDecoder.segmentFileLength(),
                        recordingDescriptorDecoder.termBufferLength(),
                        recordingDescriptorDecoder.mtuLength(),
                        recordingDescriptorDecoder.sessionId(),
                        recordingDescriptorDecoder.streamId(),
                        recordingDescriptorDecoder.strippedChannel(),
                        recordingDescriptorDecoder.originalChannel(),
                        recordingDescriptorDecoder.sourceIdentity());
                }
                break;
        }

        return ControlledFragmentHandler.Action.CONTINUE;
    }

    private enum State
    {
        CONNECTING,
        CONNECTED,
        DISCONNECTED,
        CLOSED,
    }
}
