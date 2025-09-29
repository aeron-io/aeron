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

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.*;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.security.AuthorisationService;
import io.aeron.security.NullCredentialsSupplier;
import org.agrona.DirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Long2ObjectHashMap;

import static io.aeron.CommonContext.RESPONSE_CORRELATION_ID_PARAM_NAME;

class ControlSessionAdapter implements FragmentHandler
{
    private static final int FRAGMENT_LIMIT = 10;
    private static final int FILE_IO_MAX_LENGTH_VERSION = 7;
    private static final int SESSION_ID_VERSION = 8;
    private static final int ENCODED_CREDENTIALS_VERSION = 8;
    private static final int REPLAY_TOKEN_VERSION = 10;

    private final ControlRequestDecoders decoders;
    private final AuthorisationService authorisationService;
    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this);
    private final Long2ObjectHashMap<SessionInfo> controlSessionByIdMap = new Long2ObjectHashMap<>();
    private final Subscription controlSubscription;
    private final Subscription localControlSubscription;
    private final ArchiveConductor conductor;

    ControlSessionAdapter(
        final ControlRequestDecoders decoders,
        final Subscription controlSubscription,
        final Subscription localControlSubscription,
        final ArchiveConductor conductor,
        final AuthorisationService authorisationService)
    {
        this.decoders = decoders;
        this.controlSubscription = controlSubscription;
        this.localControlSubscription = localControlSubscription;
        this.conductor = conductor;
        this.authorisationService = authorisationService;
    }

    public int poll()
    {
        int fragmentsRead = 0;

        if (null != controlSubscription)
        {
            fragmentsRead += controlSubscription.poll(fragmentAssembler, FRAGMENT_LIMIT);
        }

        fragmentsRead += localControlSubscription.poll(fragmentAssembler, FRAGMENT_LIMIT);

        return fragmentsRead;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("MethodLength")
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final MessageHeaderDecoder headerDecoder = decoders.header;
        headerDecoder.wrap(buffer, offset);

        final int schemaId = headerDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new ArchiveException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        final int templateId = headerDecoder.templateId();
        switch (templateId)
        {
            case ConnectRequestDecoder.TEMPLATE_ID:
            {
                final ConnectRequestDecoder decoder = decoders.connectRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final Image image = (Image)header.context();

                final ControlSession session = conductor.newControlSession(
                    image,
                    decoder.correlationId(),
                    decoder.responseStreamId(),
                    decoder.version(),
                    decoder.responseChannel(),
                    ArrayUtil.EMPTY_BYTE_ARRAY,
                    "",
                    this);
                controlSessionByIdMap.put(session.sessionId(), new SessionInfo(image, session));
                break;
            }

            case CloseSessionRequestDecoder.TEMPLATE_ID:
            {
                final CloseSessionRequestDecoder decoder = decoders.closeSessionRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final SessionInfo info = controlSessionByIdMap.get(controlSessionId);
                if (null != info)
                {
                    info.controlSession.abort(ControlSession.SESSION_CLOSED_MSG);
                }
                break;
            }

            case StartRecordingRequestDecoder.TEMPLATE_ID:
            {
                final StartRecordingRequestDecoder decoder = decoders.startRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStartRecording(
                        correlationId,
                        decoder.streamId(),
                        decoder.sourceLocation(),
                        false,
                        decoder.channel());
                }
                break;
            }

            case StopRecordingRequestDecoder.TEMPLATE_ID:
            {
                final StopRecordingRequestDecoder decoder = decoders.stopRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStopRecording(correlationId, decoder.streamId(), decoder.channel());
                }
                break;
            }

            case ReplayRequestDecoder.TEMPLATE_ID:
            {
                final ReplayRequestDecoder decoder = decoders.replayRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final int fileIoMaxLength = FILE_IO_MAX_LENGTH_VERSION <= headerDecoder.version() ?
                    decoder.fileIoMaxLength() : Aeron.NULL_VALUE;
                final long recordingId = decoder.recordingId();
                final long position = decoder.position();
                final long replayLength = decoder.length();
                final int replayStreamId = decoder.replayStreamId();
                final long replayToken = REPLAY_TOKEN_VERSION <= headerDecoder.version() ?
                    decoder.replayToken() : Aeron.NULL_VALUE;

                final String replayChannel = decoder.replayChannel();
                final ChannelUri channelUri = ChannelUri.parse(replayChannel);
                final Image image = (Image)header.context();
                final ControlSession controlSession = setupSessionAndChannelForReplay(
                    channelUri,
                    replayToken,
                    recordingId,
                    correlationId,
                    controlSessionId,
                    templateId,
                    image.correlationId());

                if (null != controlSession)
                {
                    controlSession.onStartReplay(
                        correlationId,
                        recordingId,
                        position,
                        replayLength,
                        fileIoMaxLength,
                        replayStreamId,
                        channelUri.toString());
                }
                break;
            }

            case StopReplayRequestDecoder.TEMPLATE_ID:
            {
                final StopReplayRequestDecoder decoder = decoders.stopReplayRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStopReplay(correlationId, decoder.replaySessionId());
                }
                break;
            }

            case ListRecordingsRequestDecoder.TEMPLATE_ID:
            {
                final ListRecordingsRequestDecoder decoder = decoders.listRecordingsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onListRecordings(correlationId, decoder.fromRecordingId(), decoder.recordCount());
                }
                break;
            }

            case ListRecordingsForUriRequestDecoder.TEMPLATE_ID:
            {
                final ListRecordingsForUriRequestDecoder decoder = decoders.listRecordingsForUriRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    final int channelLength = decoder.channelLength();
                    final byte[] bytes = 0 == channelLength ? ArrayUtil.EMPTY_BYTE_ARRAY : new byte[channelLength];
                    decoder.getChannel(bytes, 0, channelLength);

                    controlSession.onListRecordingsForUri(
                        correlationId,
                        decoder.fromRecordingId(),
                        decoder.recordCount(),
                        decoder.streamId(),
                        bytes);
                }
                break;
            }

            case ListRecordingRequestDecoder.TEMPLATE_ID:
            {
                final ListRecordingRequestDecoder decoder = decoders.listRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onListRecording(correlationId, decoder.recordingId());
                }
                break;
            }

            case ExtendRecordingRequestDecoder.TEMPLATE_ID:
            {
                final ExtendRecordingRequestDecoder decoder = decoders.extendRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onExtendRecording(
                        correlationId,
                        decoder.recordingId(),
                        decoder.streamId(),
                        decoder.sourceLocation(),
                        false,
                        decoder.channel());
                }
                break;
            }

            case RecordingPositionRequestDecoder.TEMPLATE_ID:
            {
                final RecordingPositionRequestDecoder decoder = decoders.recordingPositionRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onGetRecordingPosition(correlationId, decoder.recordingId());
                }
                break;
            }

            case TruncateRecordingRequestDecoder.TEMPLATE_ID:
            {
                final TruncateRecordingRequestDecoder decoder = decoders.truncateRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onTruncateRecording(correlationId, decoder.recordingId(), decoder.position());
                }
                break;
            }

            case StopRecordingSubscriptionRequestDecoder.TEMPLATE_ID:
            {
                final StopRecordingSubscriptionRequestDecoder decoder = decoders.stopRecordingSubscriptionRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStopRecordingSubscription(correlationId, decoder.subscriptionId());
                }
                break;
            }

            case StopPositionRequestDecoder.TEMPLATE_ID:
            {
                final StopPositionRequestDecoder decoder = decoders.stopPositionRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onGetStopPosition(correlationId, decoder.recordingId());
                }
                break;
            }

            case FindLastMatchingRecordingRequestDecoder.TEMPLATE_ID:
            {
                final FindLastMatchingRecordingRequestDecoder decoder = decoders.findLastMatchingRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    final int channelLength = decoder.channelLength();
                    final byte[] bytes = 0 == channelLength ? ArrayUtil.EMPTY_BYTE_ARRAY : new byte[channelLength];

                    decoder.getChannel(bytes, 0, channelLength);
                    controlSession.onFindLastMatchingRecording(
                        correlationId,
                        decoder.minRecordingId(),
                        decoder.sessionId(),
                        decoder.streamId(),
                        bytes);
                }
                break;
            }

            case ListRecordingSubscriptionsRequestDecoder.TEMPLATE_ID:
            {
                final ListRecordingSubscriptionsRequestDecoder decoder = decoders.listRecordingSubscriptionsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onListRecordingSubscriptions(
                        correlationId,
                        decoder.pseudoIndex(),
                        decoder.subscriptionCount(),
                        decoder.applyStreamId() == BooleanType.TRUE,
                        decoder.streamId(),
                        decoder.channel());
                }
                break;
            }

            case BoundedReplayRequestDecoder.TEMPLATE_ID:
            {
                final BoundedReplayRequestDecoder decoder = decoders.boundedReplayRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final long position = decoder.position();
                final long replayLength = decoder.length();
                final long recordingId = decoder.recordingId();
                final int limitCounterId = decoder.limitCounterId();
                final int replayStreamId = decoder.replayStreamId();
                final int fileIoMaxLength = FILE_IO_MAX_LENGTH_VERSION <= headerDecoder.version() ?
                    decoder.fileIoMaxLength() : Aeron.NULL_VALUE;
                final long replayToken = REPLAY_TOKEN_VERSION <= headerDecoder.version() ?
                    decoder.replayToken() : Aeron.NULL_VALUE;

                final String replayChannel = decoder.replayChannel();

                final Image image = (Image)header.context();

                final ChannelUri channelUri = ChannelUri.parse(replayChannel);
                final ControlSession controlSession = setupSessionAndChannelForReplay(
                    channelUri,
                    replayToken,
                    recordingId,
                    correlationId,
                    controlSessionId,
                    templateId,
                    image.correlationId());

                if (null != controlSession)
                {
                    controlSession.onStartBoundedReplay(
                        correlationId,
                        recordingId,
                        position,
                        replayLength,
                        limitCounterId,
                        fileIoMaxLength,
                        replayStreamId,
                        channelUri.toString());
                }
                break;
            }

            case StopAllReplaysRequestDecoder.TEMPLATE_ID:
            {
                final StopAllReplaysRequestDecoder decoder = decoders.stopAllReplaysRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStopAllReplays(correlationId, decoder.recordingId());
                }
                break;
            }

            case ReplicateRequestDecoder.TEMPLATE_ID:
            {
                final ReplicateRequestDecoder decoder = decoders.replicateRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onReplicate(
                        correlationId,
                        decoder.srcRecordingId(),
                        decoder.dstRecordingId(),
                        AeronArchive.NULL_POSITION,
                        Aeron.NULL_VALUE,
                        Aeron.NULL_VALUE,
                        decoder.srcControlStreamId(),
                        Aeron.NULL_VALUE,
                        Aeron.NULL_VALUE,
                        decoder.srcControlChannel(),
                        decoder.liveDestination(),
                        "",
                        NullCredentialsSupplier.NULL_CREDENTIAL,
                        "");
                }
                break;
            }

            case StopReplicationRequestDecoder.TEMPLATE_ID:
            {
                final StopReplicationRequestDecoder decoder = decoders.stopReplicationRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStopReplication(correlationId, decoder.replicationId());
                }
                break;
            }

            case StartPositionRequestDecoder.TEMPLATE_ID:
            {
                final StartPositionRequestDecoder decoder = decoders.startPositionRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onGetStartPosition(correlationId, decoder.recordingId());
                }
                break;
            }

            case DetachSegmentsRequestDecoder.TEMPLATE_ID:
            {
                final DetachSegmentsRequestDecoder decoder = decoders.detachSegmentsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onDetachSegments(correlationId, decoder.recordingId(), decoder.newStartPosition());
                }
                break;
            }

            case DeleteDetachedSegmentsRequestDecoder.TEMPLATE_ID:
            {
                final DeleteDetachedSegmentsRequestDecoder decoder = decoders.deleteDetachedSegmentsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onDeleteDetachedSegments(correlationId, decoder.recordingId());
                }
                break;
            }

            case PurgeSegmentsRequestDecoder.TEMPLATE_ID:
            {
                final PurgeSegmentsRequestDecoder decoder = decoders.purgeSegmentsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onPurgeSegments(correlationId, decoder.recordingId(), decoder.newStartPosition());
                }
                break;
            }

            case AttachSegmentsRequestDecoder.TEMPLATE_ID:
            {
                final AttachSegmentsRequestDecoder decoder = decoders.attachSegmentsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onAttachSegments(correlationId, decoder.recordingId());
                }
                break;
            }

            case MigrateSegmentsRequestDecoder.TEMPLATE_ID:
            {
                final MigrateSegmentsRequestDecoder decoder = decoders.migrateSegmentsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onMigrateSegments(correlationId, decoder.srcRecordingId(), decoder.dstRecordingId());
                }
                break;
            }

            case AuthConnectRequestDecoder.TEMPLATE_ID:
            {
                final AuthConnectRequestDecoder decoder = decoders.authConnectRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final String responseChannel = decoder.responseChannel();
                final int credentialsLength = decoder.encodedCredentialsLength();
                final byte[] credentials;

                if (credentialsLength > 0)
                {
                    credentials = new byte[credentialsLength];
                    decoder.getEncodedCredentials(credentials, 0, credentialsLength);
                }
                else
                {
                    credentials = ArrayUtil.EMPTY_BYTE_ARRAY;
                    decoder.skipEncodedCredentials();
                }
                final String clientInfo = decoder.clientInfo();

                final Image image = (Image)header.context();

                final ControlSession session = conductor.newControlSession(
                    image,
                    decoder.correlationId(),
                    decoder.responseStreamId(),
                    decoder.version(),
                    responseChannel,
                    credentials,
                    clientInfo,
                    this);
                controlSessionByIdMap.put(session.sessionId(), new SessionInfo(image, session));
                break;
            }

            case ChallengeResponseDecoder.TEMPLATE_ID:
            {
                final ChallengeResponseDecoder decoder = decoders.challengeResponse;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final SessionInfo info = controlSessionByIdMap.get(controlSessionId);
                if (null != info)
                {
                    final int credentialsLength = decoder.encodedCredentialsLength();
                    final byte[] credentials;

                    if (credentialsLength > 0)
                    {
                        credentials = new byte[credentialsLength];
                        decoder.getEncodedCredentials(credentials, 0, credentialsLength);
                    }
                    else
                    {
                        credentials = ArrayUtil.EMPTY_BYTE_ARRAY;
                    }

                    info.controlSession.onChallengeResponse(decoder.correlationId(), credentials);
                }
                break;
            }

            case KeepAliveRequestDecoder.TEMPLATE_ID:
            {
                final KeepAliveRequestDecoder decoder = decoders.keepAliveRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onKeepAlive(correlationId);
                }
                break;
            }

            case TaggedReplicateRequestDecoder.TEMPLATE_ID:
            {
                final TaggedReplicateRequestDecoder decoder = decoders.taggedReplicateRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onReplicate(
                        correlationId,
                        decoder.srcRecordingId(),
                        decoder.dstRecordingId(),
                        AeronArchive.NULL_POSITION,
                        decoder.channelTagId(),
                        decoder.subscriptionTagId(),
                        decoder.srcControlStreamId(),
                        Aeron.NULL_VALUE,
                        Aeron.NULL_VALUE,
                        decoder.srcControlChannel(),
                        decoder.liveDestination(),
                        "",
                        NullCredentialsSupplier.NULL_CREDENTIAL,
                        "");
                }
                break;
            }

            case StartRecordingRequest2Decoder.TEMPLATE_ID:
            {
                final StartRecordingRequest2Decoder decoder = decoders.startRecordingRequest2;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStartRecording(
                        correlationId,
                        decoder.streamId(),
                        decoder.sourceLocation(),
                        decoder.autoStop() == BooleanType.TRUE,
                        decoder.channel());
                }
                break;
            }

            case ExtendRecordingRequest2Decoder.TEMPLATE_ID:
            {
                final ExtendRecordingRequest2Decoder decoder = decoders.extendRecordingRequest2;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onExtendRecording(
                        correlationId,
                        decoder.recordingId(),
                        decoder.streamId(),
                        decoder.sourceLocation(),
                        decoder.autoStop() == BooleanType.TRUE,
                        decoder.channel());
                }
                break;
            }

            case StopRecordingByIdentityRequestDecoder.TEMPLATE_ID:
            {
                final StopRecordingByIdentityRequestDecoder decoder = decoders.stopRecordingByIdentityRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStopRecordingByIdentity(correlationId, decoder.recordingId());
                }
                break;
            }

            case ReplicateRequest2Decoder.TEMPLATE_ID:
            {
                final ReplicateRequest2Decoder decoder = decoders.replicateRequest2;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                final int fileIoMaxLength = FILE_IO_MAX_LENGTH_VERSION <= headerDecoder.version() ?
                    decoder.fileIoMaxLength() : Aeron.NULL_VALUE;

                final int sessionId = SESSION_ID_VERSION <= headerDecoder.version() ?
                    decoder.replicationSessionId() : Aeron.NULL_VALUE;

                final String srcControlChannel = decoder.srcControlChannel();
                final String liveDestination = decoder.liveDestination();
                final String replicationChannel = decoder.replicationChannel();
                final byte[] encodedCredentials;
                if (ENCODED_CREDENTIALS_VERSION <= headerDecoder.version())
                {
                    encodedCredentials = new byte[decoder.encodedCredentialsLength()];
                    decoder.getEncodedCredentials(encodedCredentials, 0, decoder.encodedCredentialsLength());
                }
                else
                {
                    encodedCredentials = NullCredentialsSupplier.NULL_CREDENTIAL;
                }
                final String srcResponseChannel = decoder.srcResponseChannel();

                if (null != controlSession)
                {
                    controlSession.onReplicate(
                        correlationId,
                        decoder.srcRecordingId(),
                        decoder.dstRecordingId(),
                        decoder.stopPosition(),
                        decoder.channelTagId(),
                        decoder.subscriptionTagId(),
                        decoder.srcControlStreamId(),
                        fileIoMaxLength,
                        sessionId,
                        srcControlChannel,
                        liveDestination,
                        replicationChannel,
                        encodedCredentials,
                        srcResponseChannel);
                }
                break;
            }

            case PurgeRecordingRequestDecoder.TEMPLATE_ID:
            {
                final PurgeRecordingRequestDecoder decoder = decoders.purgeRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onPurgeRecording(correlationId, decoder.recordingId());
                }
                break;
            }

            case MaxRecordedPositionRequestDecoder.TEMPLATE_ID:
            {
                final MaxRecordedPositionRequestDecoder decoder = decoders.maxRecordedPositionRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onGetMaxRecordedPosition(correlationId, decoder.recordingId());
                }
                break;
            }

            case ArchiveIdRequestDecoder.TEMPLATE_ID:
            {
                final ArchiveIdRequestDecoder decoder = decoders.archiveIdRequestDecoder;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onArchiveId(correlationId);
                }
                break;
            }

            case ReplayTokenRequestDecoder.TEMPLATE_ID:
            {
                final ReplayTokenRequestDecoder decoder = decoders.replayTokenRequestDecoder;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final long recordingId = decoder.recordingId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);
                if (null != controlSession)
                {
                    final long replayToken = conductor.generateReplayToken(controlSession, recordingId);
                    controlSession.sendOkResponse(correlationId, replayToken);
                }

                break;
            }

            case UpdateChannelRequestDecoder.TEMPLATE_ID:
            {
                final UpdateChannelRequestDecoder decoder = decoders.updateChannelRequestDecoder;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final long recordingId = decoder.recordingId();
                final String channel = decoder.channel();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);
                if (null != controlSession)
                {
                    conductor.updateChannel(correlationId, recordingId, channel, controlSession);
                }

                break;
            }
        }
    }

    void abortControlSessionByImage(final Image image)
    {
        for (final SessionInfo info : controlSessionByIdMap.values())
        {
            if (info.image == image && !info.controlSession.isDone())
            {
                final Subscription subscription = image.subscription();
                info.controlSession.abort(ControlSession.REQUEST_IMAGE_NOT_AVAILABLE_MSG +
                    " : controlSessionId=" + info.controlSession.sessionId() +
                    " image.correlationId=" + image.correlationId() +
                    " sessionId=" + image.sessionId() +
                    " streamId=" + subscription.streamId() +
                    " channel=" + subscription.channel());
                break;
            }
        }
    }

    void removeControlSession(final long controlSessionId, final boolean sessionAborted, final String abortReason)
    {
        final SessionInfo sessionInfo = controlSessionByIdMap.remove(controlSessionId);
        if (null != sessionInfo && sessionAborted)
        {
            sessionInfo.image.reject(abortReason);
        }
        conductor.removeReplayTokensForSession(controlSessionId);
        final Counter controlSessionsCounter = conductor.context().controlSessionsCounter();
        if (!controlSessionsCounter.isClosed())
        {
            controlSessionsCounter.decrementRelease();
        }
    }

    private ControlSession setupSessionAndChannelForReplay(
        final ChannelUri channelUri,
        final long replayToken,
        final long recordingId,
        final long correlationId,
        final long controlSessionId,
        final int templateId,
        final long imageCorrelationId)
    {
        final ControlSession controlSession;
        if (channelUri.hasControlModeResponse() && Aeron.NULL_VALUE != replayToken)
        {
            controlSession = conductor.getReplaySession(replayToken, recordingId);
            if (null == controlSession)
            {
                throw new ArchiveException("Unknown session or token timeout for replayToken=" + replayToken);
            }

            channelUri.put(RESPONSE_CORRELATION_ID_PARAM_NAME, Long.toString(imageCorrelationId));
        }
        else
        {
            controlSession = getControlSession(correlationId, controlSessionId, templateId);
        }
        return controlSession;
    }

    private ControlSession getControlSession(
        final long correlationId, final long controlSessionId, final int templateId)
    {
        final SessionInfo info = controlSessionByIdMap.get(controlSessionId);
        if (null != info)
        {
            final ControlSession controlSession = info.controlSession;
            final byte[] principal = controlSession.encodedPrincipal();
            if (!authorisationService.isAuthorised(MessageHeaderDecoder.SCHEMA_ID, templateId, null, principal))
            {
                conductor.logWarning("unauthorised archive action=" + templateId +
                    " controlSessionId=" + controlSessionId + " source=" + info.image.sourceIdentity());

                controlSession.sendErrorResponse(
                    correlationId, ArchiveException.UNAUTHORISED_ACTION, "unauthorised action");

                return null;
            }
            return controlSession;
        }
        else
        {
            conductor.logWarning("control request for unknown session:" +
                " controlSessionId=" + controlSessionId +
                " templateId=" + templateId);
            return null;
        }
    }

    private record SessionInfo(Image image, ControlSession controlSession)
    {
    }
}
