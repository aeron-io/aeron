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
package io.aeron.archive.client;

import io.aeron.*;
import io.aeron.archive.codecs.*;
import io.aeron.security.CredentialsSupplier;
import io.aeron.security.NullCredentialsSupplier;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.*;

import static io.aeron.archive.client.AeronArchive.Configuration.MESSAGE_RETRY_ATTEMPTS_DEFAULT;
import static io.aeron.archive.client.AeronArchive.Configuration.MESSAGE_TIMEOUT_DEFAULT_NS;

/**
 * Proxy class for encapsulating encoding and sending of control protocol messages to an archive.
 */
public final class ArchiveProxy
{
    /**
     * Default number of retry attempts to be made when offering requests.
     * @deprecated Use {@link AeronArchive.Configuration#MESSAGE_RETRY_ATTEMPTS_DEFAULT}.
     */
    @Deprecated(since = "1.48.6", forRemoval = true)
    public static final int DEFAULT_RETRY_ATTEMPTS = MESSAGE_RETRY_ATTEMPTS_DEFAULT;

    private final long connectTimeoutNs;
    private final int retryAttempts;
    private final IdleStrategy retryIdleStrategy;
    private final NanoClock nanoClock;
    private final CredentialsSupplier credentialsSupplier;

    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
    private final ExclusivePublication publication;
    private final MessageHeaderEncoder messageHeader = new MessageHeaderEncoder();
    private StartRecordingRequestEncoder startRecordingRequest;
    private StartRecordingRequest2Encoder startRecordingRequest2;
    private StopRecordingRequestEncoder stopRecordingRequest;
    private StopRecordingSubscriptionRequestEncoder stopRecordingSubscriptionRequest;
    private StopRecordingByIdentityRequestEncoder stopRecordingByIdentityRequest;
    private ReplayRequestEncoder replayRequest;
    private StopReplayRequestEncoder stopReplayRequest;
    private ListRecordingsRequestEncoder listRecordingsRequest;
    private ListRecordingsForUriRequestEncoder listRecordingsForUriRequest;
    private ListRecordingRequestEncoder listRecordingRequest;
    private ExtendRecordingRequestEncoder extendRecordingRequest;
    private ExtendRecordingRequest2Encoder extendRecordingRequest2;
    private RecordingPositionRequestEncoder recordingPositionRequest;
    private TruncateRecordingRequestEncoder truncateRecordingRequest;
    private PurgeRecordingRequestEncoder purgeRecordingRequest;
    private StopPositionRequestEncoder stopPositionRequest;
    private MaxRecordedPositionRequestEncoder maxRecordedPositionRequestEncoder;
    private FindLastMatchingRecordingRequestEncoder findLastMatchingRecordingRequest;
    private ListRecordingSubscriptionsRequestEncoder listRecordingSubscriptionsRequest;
    private BoundedReplayRequestEncoder boundedReplayRequest;
    private StopAllReplaysRequestEncoder stopAllReplaysRequest;
    private ReplicateRequest2Encoder replicateRequest;
    private StopReplicationRequestEncoder stopReplicationRequest;
    private StartPositionRequestEncoder startPositionRequest;
    private DetachSegmentsRequestEncoder detachSegmentsRequest;
    private DeleteDetachedSegmentsRequestEncoder deleteDetachedSegmentsRequest;
    private PurgeSegmentsRequestEncoder purgeSegmentsRequest;
    private AttachSegmentsRequestEncoder attachSegmentsRequest;
    private MigrateSegmentsRequestEncoder migrateSegmentsRequest;
    private ArchiveIdRequestEncoder archiveIdRequestEncoder;
    private ReplayTokenRequestEncoder replayTokenRequestEncoder;

    /**
     * Create a proxy with a {@link ExclusivePublication} for sending control message requests.
     * <p>
     * This provides a default {@link IdleStrategy} of a {@link YieldingIdleStrategy} when offers are back pressured
     * with a defaults of {@link AeronArchive.Configuration#MESSAGE_TIMEOUT_DEFAULT_NS} and
     * {@link AeronArchive.Configuration#MESSAGE_RETRY_ATTEMPTS_DEFAULT}.
     *
     * @param publication publication for sending control messages to an archive.
     * @throws ClassCastException if {@code publication} is not an instance of {@link ExclusivePublication}.
     * @deprecated Use another constructor with an {@link ExclusivePublication}.
     */
    @Deprecated(forRemoval = true, since = "1.47.0")
    public ArchiveProxy(final Publication publication)
    {
        this((ExclusivePublication)publication);
    }

    /**
     * Create a proxy with a {@link ExclusivePublication} for sending control message requests.
     * <p>
     * This provides a default {@link IdleStrategy} of a {@link YieldingIdleStrategy} when offers are back pressured
     * with a defaults of {@link AeronArchive.Configuration#MESSAGE_TIMEOUT_DEFAULT_NS} and
     * {@link AeronArchive.Configuration#MESSAGE_RETRY_ATTEMPTS_DEFAULT}.
     *
     * @param publication publication for sending control messages to an archive.
     * @throws ClassCastException if {@code publication} is not an instance of {@link ExclusivePublication}.
     */
    public ArchiveProxy(final ExclusivePublication publication)
    {
        this(
            publication,
            YieldingIdleStrategy.INSTANCE,
            SystemNanoClock.INSTANCE,
            MESSAGE_TIMEOUT_DEFAULT_NS,
            MESSAGE_RETRY_ATTEMPTS_DEFAULT,
            new NullCredentialsSupplier());
    }

    /**
     * Create a proxy with a {@link ExclusivePublication} for sending control message requests.
     *
     * @param publication         publication for sending control messages to an archive.
     * @param retryIdleStrategy   for what should happen between retry attempts at offering messages.
     * @param nanoClock           to be used for calculating checking deadlines.
     * @param connectTimeoutNs    for connection requests.
     * @param retryAttempts       for offering control messages before giving up.
     * @param credentialsSupplier for the AuthConnectRequest
     * @throws ClassCastException if {@code publication} is not an instance of {@link ExclusivePublication}.
     * @deprecated Use another constructor with an {@link ExclusivePublication}.
     */
    @Deprecated(forRemoval = true, since = "1.47.0")
    public ArchiveProxy(
        final Publication publication,
        final IdleStrategy retryIdleStrategy,
        final NanoClock nanoClock,
        final long connectTimeoutNs,
        final int retryAttempts,
        final CredentialsSupplier credentialsSupplier)
    {
        this(
            (ExclusivePublication)publication,
            retryIdleStrategy,
            nanoClock,
            connectTimeoutNs,
            retryAttempts,
            credentialsSupplier);
    }

    /**
     * Create a proxy with a {@link ExclusivePublication} for sending control message requests.
     *
     * @param publication         publication for sending control messages to an archive.
     * @param retryIdleStrategy   for what should happen between retry attempts at offering messages.
     * @param nanoClock           to be used for calculating checking deadlines.
     * @param connectTimeoutNs    for connection requests.
     * @param retryAttempts       for offering control messages before giving up.
     * @param credentialsSupplier for the AuthConnectRequest
     * @since 1.47.0
     */
    public ArchiveProxy(
        final ExclusivePublication publication,
        final IdleStrategy retryIdleStrategy,
        final NanoClock nanoClock,
        final long connectTimeoutNs,
        final int retryAttempts,
        final CredentialsSupplier credentialsSupplier)
    {
        this.publication = publication;
        this.retryIdleStrategy = retryIdleStrategy;
        this.nanoClock = nanoClock;
        this.connectTimeoutNs = connectTimeoutNs;
        this.retryAttempts = retryAttempts;
        this.credentialsSupplier = credentialsSupplier;
    }

    /**
     * Get the {@link Publication} used for sending control messages.
     *
     * @return the {@link Publication} used for sending control messages.
     */
    public Publication publication()
    {
        return publication;
    }

    /**
     * Connect to an archive on its control interface providing the response stream details.
     *
     * @param responseChannel  for the control message responses.
     * @param responseStreamId for the control message responses.
     * @param correlationId    for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean connect(final String responseChannel, final int responseStreamId, final long correlationId)
    {
        final byte[] encodedCredentials = credentialsSupplier.encodedCredentials();

        final AuthConnectRequestEncoder connectRequestEncoder = new AuthConnectRequestEncoder();
        connectRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .correlationId(correlationId)
            .responseStreamId(responseStreamId)
            .version(AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION)
            .responseChannel(responseChannel)
            .putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);

        return offerWithTimeout(connectRequestEncoder.encodedLength(), null);
    }

    /**
     * Try and connect to an archive on its control interface providing the response stream details. Only one attempt
     * will be made to offer the request.
     *
     * @param responseChannel  for the control message responses.
     * @param responseStreamId for the control message responses.
     * @param correlationId    for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean tryConnect(final String responseChannel, final int responseStreamId, final long correlationId)
    {
        final byte[] encodedCredentials = credentialsSupplier.encodedCredentials();

        final AuthConnectRequestEncoder connectRequestEncoder = new AuthConnectRequestEncoder();
        connectRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .correlationId(correlationId)
            .responseStreamId(responseStreamId)
            .version(AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION)
            .responseChannel(responseChannel)
            .putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + connectRequestEncoder.encodedLength();

        return publication.offer(buffer, 0, length) > 0;
    }

    /**
     * Connect to an archive on its control interface providing the response stream details.
     *
     * @param responseChannel    for the control message responses.
     * @param responseStreamId   for the control message responses.
     * @param correlationId      for this request.
     * @param aeronClientInvoker for aeron client conductor thread.
     * @return true if successfully offered otherwise false.
     */
    public boolean connect(
        final String responseChannel,
        final int responseStreamId,
        final long correlationId,
        final AgentInvoker aeronClientInvoker)
    {
        final byte[] encodedCredentials = credentialsSupplier.encodedCredentials();

        final AuthConnectRequestEncoder connectRequestEncoder = new AuthConnectRequestEncoder();
        connectRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .correlationId(correlationId)
            .responseStreamId(responseStreamId)
            .version(AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION)
            .responseChannel(responseChannel)
            .putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);

        return offerWithTimeout(connectRequestEncoder.encodedLength(), aeronClientInvoker);
    }

    /**
     * Keep this archive session alive by notifying the archive.
     *
     * @param controlSessionId with the archive.
     * @param correlationId    for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean keepAlive(final long controlSessionId, final long correlationId)
    {
        final KeepAliveRequestEncoder keepAliveRequestEncoder = new KeepAliveRequestEncoder();
        keepAliveRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId);

        return offer(keepAliveRequestEncoder.encodedLength());
    }

    /**
     * Close this control session with the archive.
     *
     * @param controlSessionId with the archive.
     * @return true if successfully offered otherwise false.
     */
    public boolean closeSession(final long controlSessionId)
    {
        final CloseSessionRequestEncoder closeSessionRequestEncoder = new CloseSessionRequestEncoder();
        closeSessionRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId);

        return offer(closeSessionRequestEncoder.encodedLength());
    }

    /**
     * Try and send a ChallengeResponse to an archive on its control interface providing the credentials. Only one
     * attempt will be made to offer the request.
     *
     * @param encodedCredentials to send.
     * @param correlationId      for this response.
     * @param controlSessionId   for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean tryChallengeResponse(
        final byte[] encodedCredentials, final long correlationId, final long controlSessionId)
    {
        final ChallengeResponseEncoder challengeResponseEncoder = new ChallengeResponseEncoder();
        challengeResponseEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + challengeResponseEncoder.encodedLength();

        return publication.offer(buffer, 0, length) > 0;
    }

    /**
     * Start recording streams for a given channel and stream id pairing.
     *
     * @param channel          to be recorded.
     * @param streamId         to be recorded.
     * @param sourceLocation   of the publication to be recorded.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean startRecording(
        final String channel,
        final int streamId,
        final SourceLocation sourceLocation,
        final long correlationId,
        final long controlSessionId)
    {
        if (null == startRecordingRequest)
        {
            startRecordingRequest = new StartRecordingRequestEncoder();
        }

        startRecordingRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .streamId(streamId)
            .sourceLocation(sourceLocation)
            .channel(channel);

        return offer(startRecordingRequest.encodedLength());
    }

    /**
     * Start recording streams for a given channel and stream id pairing.
     *
     * @param channel          to be recorded.
     * @param streamId         to be recorded.
     * @param sourceLocation   of the publication to be recorded.
     * @param autoStop         if the recording should be automatically stopped when complete.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean startRecording(
        final String channel,
        final int streamId,
        final SourceLocation sourceLocation,
        final boolean autoStop,
        final long correlationId,
        final long controlSessionId)
    {
        if (null == startRecordingRequest2)
        {
            startRecordingRequest2 = new StartRecordingRequest2Encoder();
        }

        startRecordingRequest2
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .streamId(streamId)
            .sourceLocation(sourceLocation)
            .autoStop(autoStop ? BooleanType.TRUE : BooleanType.FALSE)
            .channel(channel);

        return offer(startRecordingRequest2.encodedLength());
    }

    /**
     * Stop an active recording.
     *
     * @param channel          to be stopped.
     * @param streamId         to be stopped.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean stopRecording(
        final String channel, final int streamId, final long correlationId, final long controlSessionId)
    {
        if (null == stopRecordingRequest)
        {
            stopRecordingRequest = new StopRecordingRequestEncoder();
        }

        stopRecordingRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .streamId(streamId)
            .channel(channel);

        return offer(stopRecordingRequest.encodedLength());
    }

    /**
     * Stop a recording by the {@link Subscription#registrationId()} it was registered with.
     *
     * @param subscriptionId   that identifies the subscription in the archive doing the recording.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean stopRecording(final long subscriptionId, final long correlationId, final long controlSessionId)
    {
        if (null == stopRecordingSubscriptionRequest)
        {
            stopRecordingSubscriptionRequest = new StopRecordingSubscriptionRequestEncoder();
        }

        stopRecordingSubscriptionRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .subscriptionId(subscriptionId);

        return offer(stopRecordingSubscriptionRequest.encodedLength());
    }

    /**
     * Stop an active recording by the recording id. This is not the {@link Subscription#registrationId()}.
     *
     * @param recordingId      that identifies a recording in the archive.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean stopRecordingByIdentity(
        final long recordingId, final long correlationId, final long controlSessionId)
    {
        if (null == stopRecordingByIdentityRequest)
        {
            stopRecordingByIdentityRequest = new StopRecordingByIdentityRequestEncoder();
        }

        stopRecordingByIdentityRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId);

        return offer(stopRecordingByIdentityRequest.encodedLength());
    }

    /**
     * Replay a recording from a given position. Supports specifying {@link ReplayParams} to change the behaviour of the
     * replay. For example a bounded replay can be requested by specifying the boundingLimitCounterId. The ReplayParams
     * is free to be reused after this call completes.
     *
     * @param recordingId      to be replayed.
     * @param replayChannel    to which the replay should be sent.
     * @param replayStreamId   to which the replay should be sent.
     * @param replayParams     optional parameters change the behaviour of the replay.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     * @see ReplayParams
     */
    public boolean replay(
        final long recordingId,
        final String replayChannel,
        final int replayStreamId,
        final ReplayParams replayParams,
        final long correlationId,
        final long controlSessionId)
    {
        if (replayParams.isBounded())
        {
            return boundedReplay(
                recordingId,
                replayParams.position(),
                replayParams.length(),
                replayParams.boundingLimitCounterId(),
                replayChannel,
                replayStreamId,
                correlationId,
                controlSessionId,
                replayParams.fileIoMaxLength(),
                replayParams.replayToken());
        }
        else
        {
            return replay(
                recordingId,
                replayParams.position(),
                replayParams.length(),
                replayChannel,
                replayStreamId,
                correlationId,
                controlSessionId,
                replayParams.fileIoMaxLength(),
                replayParams.replayToken());
        }
    }

    /**
     * Replay a recording from a given position.
     *
     * @param recordingId      to be replayed.
     * @param position         from which the replay should be started.
     * @param length           of the stream to be replayed. Use {@link Long#MAX_VALUE} to follow a live stream.
     * @param replayChannel    to which the replay should be sent.
     * @param replayStreamId   to which the replay should be sent.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean replay(
        final long recordingId,
        final long position,
        final long length,
        final String replayChannel,
        final int replayStreamId,
        final long correlationId,
        final long controlSessionId)
    {
        return replay(
            recordingId,
            position,
            length,
            replayChannel,
            replayStreamId,
            correlationId,
            controlSessionId,
            Aeron.NULL_VALUE,
            Aeron.NULL_VALUE);
    }

    /**
     * Replay a recording from a given position bounded by a position counter.
     *
     * @param recordingId      to be replayed.
     * @param position         from which the replay should be started.
     * @param length           of the stream to be replayed. Use {@link Long#MAX_VALUE} to follow a live stream.
     * @param limitCounterId   to use as the replay bound.
     * @param replayChannel    to which the replay should be sent.
     * @param replayStreamId   to which the replay should be sent.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean boundedReplay(
        final long recordingId,
        final long position,
        final long length,
        final int limitCounterId,
        final String replayChannel,
        final int replayStreamId,
        final long correlationId,
        final long controlSessionId)
    {
        return boundedReplay(
            recordingId,
            position,
            length,
            limitCounterId,
            replayChannel,
            replayStreamId,
            correlationId,
            controlSessionId,
            Aeron.NULL_VALUE,
            Aeron.NULL_VALUE);
    }

    /**
     * Stop an existing replay session.
     *
     * @param replaySessionId  that should be stopped.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean stopReplay(final long replaySessionId, final long correlationId, final long controlSessionId)
    {
        if (null == stopReplayRequest)
        {
            stopReplayRequest = new StopReplayRequestEncoder();
        }

        stopReplayRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .replaySessionId(replaySessionId);

        return offer(stopReplayRequest.encodedLength());
    }

    /**
     * Stop any existing replay sessions for recording id or all replay sessions regardless of recording id.
     *
     * @param recordingId      that should be stopped.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean stopAllReplays(final long recordingId, final long correlationId, final long controlSessionId)
    {
        if (null == stopAllReplaysRequest)
        {
            stopAllReplaysRequest = new StopAllReplaysRequestEncoder();
        }

        stopAllReplaysRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId);

        return offer(stopAllReplaysRequest.encodedLength());
    }

    /**
     * List a range of recording descriptors.
     *
     * @param fromRecordingId  at which to begin listing.
     * @param recordCount      for the number of descriptors to be listed.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean listRecordings(
        final long fromRecordingId, final int recordCount, final long correlationId, final long controlSessionId)
    {
        if (null == listRecordingsRequest)
        {
            listRecordingsRequest = new ListRecordingsRequestEncoder();
        }

        listRecordingsRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .fromRecordingId(fromRecordingId)
            .recordCount(recordCount);

        return offer(listRecordingsRequest.encodedLength());
    }

    /**
     * List a range of recording descriptors which match a channel URI fragment and stream id.
     *
     * @param fromRecordingId  at which to begin listing.
     * @param recordCount      for the number of descriptors to be listed.
     * @param channelFragment  to match recordings on from the original channel URI in the archive descriptor.
     * @param streamId         to match recordings on.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean listRecordingsForUri(
        final long fromRecordingId,
        final int recordCount,
        final String channelFragment,
        final int streamId,
        final long correlationId,
        final long controlSessionId)
    {
        if (null == listRecordingsForUriRequest)
        {
            listRecordingsForUriRequest = new ListRecordingsForUriRequestEncoder();
        }

        listRecordingsForUriRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .fromRecordingId(fromRecordingId)
            .recordCount(recordCount)
            .streamId(streamId)
            .channel(channelFragment);

        return offer(listRecordingsForUriRequest.encodedLength());
    }

    /**
     * List a recording descriptor for a given recording id.
     *
     * @param recordingId      at which to begin listing.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean listRecording(final long recordingId, final long correlationId, final long controlSessionId)
    {
        if (null == listRecordingRequest)
        {
            listRecordingRequest = new ListRecordingRequestEncoder();
        }

        listRecordingRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId);

        return offer(listRecordingRequest.encodedLength());
    }

    /**
     * Extend an existing, non-active, recorded stream for the same channel and stream id.
     * <p>
     * The channel must be configured for the initial position from which it will be extended. This can be done
     * with {@link ChannelUriStringBuilder#initialPosition(long, int, int)}. The details required to initialise can
     * be found by calling {@link #listRecording(long, long, long)}.
     *
     * @param channel          to be recorded.
     * @param streamId         to be recorded.
     * @param sourceLocation   of the publication to be recorded.
     * @param recordingId      to be extended.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean extendRecording(
        final String channel,
        final int streamId,
        final SourceLocation sourceLocation,
        final long recordingId,
        final long correlationId,
        final long controlSessionId)
    {
        if (null == extendRecordingRequest)
        {
            extendRecordingRequest = new ExtendRecordingRequestEncoder();
        }

        extendRecordingRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId)
            .streamId(streamId)
            .sourceLocation(sourceLocation)
            .channel(channel);

        return offer(extendRecordingRequest.encodedLength());
    }

    /**
     * Extend an existing, non-active, recorded stream for the same channel and stream id.
     * <p>
     * The channel must be configured for the initial position from which it will be extended. This can be done
     * with {@link ChannelUriStringBuilder#initialPosition(long, int, int)}. The details required to initialise can
     * be found by calling {@link #listRecording(long, long, long)}.
     *
     * @param channel          to be recorded.
     * @param streamId         to be recorded.
     * @param sourceLocation   of the publication to be recorded.
     * @param autoStop         if the recording should be automatically stopped when complete.
     * @param recordingId      to be extended.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean extendRecording(
        final String channel,
        final int streamId,
        final SourceLocation sourceLocation,
        final boolean autoStop,
        final long recordingId,
        final long correlationId,
        final long controlSessionId)
    {
        if (null == extendRecordingRequest2)
        {
            extendRecordingRequest2 = new ExtendRecordingRequest2Encoder();
        }

        extendRecordingRequest2
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId)
            .streamId(streamId)
            .sourceLocation(sourceLocation)
            .autoStop(autoStop ? BooleanType.TRUE : BooleanType.FALSE)
            .channel(channel);

        return offer(extendRecordingRequest2.encodedLength());
    }

    /**
     * Get the recorded position of an active recording.
     *
     * @param recordingId      of the active recording that the position is being requested for.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean getRecordingPosition(final long recordingId, final long correlationId, final long controlSessionId)
    {
        if (null == recordingPositionRequest)
        {
            recordingPositionRequest = new RecordingPositionRequestEncoder();
        }

        recordingPositionRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId);

        return offer(recordingPositionRequest.encodedLength());
    }

    /**
     * Truncate a stopped recording to a given position that is less than the stopped position. The provided position
     * must be on a fragment boundary. Truncating a recording to the start position effectively deletes the recording.
     *
     * @param recordingId      of the stopped recording to be truncated.
     * @param position         to which the recording will be truncated.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean truncateRecording(
        final long recordingId, final long position, final long correlationId, final long controlSessionId)
    {
        if (null == truncateRecordingRequest)
        {
            truncateRecordingRequest = new TruncateRecordingRequestEncoder();
        }

        truncateRecordingRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId)
            .position(position);

        return offer(truncateRecordingRequest.encodedLength());
    }

    /**
     * Purge a stopped recording, i.e. mark recording as {@link io.aeron.archive.codecs.RecordingState#INVALID}
     * and delete the corresponding segment files. The space in the Catalog will be reclaimed upon compaction.
     *
     * @param recordingId      of the stopped recording to be purged.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean purgeRecording(
        final long recordingId, final long correlationId, final long controlSessionId)
    {
        if (null == purgeRecordingRequest)
        {
            purgeRecordingRequest = new PurgeRecordingRequestEncoder();
        }

        purgeRecordingRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId);

        return offer(purgeRecordingRequest.encodedLength());
    }

    /**
     * Get the start position of a recording.
     *
     * @param recordingId      of the recording that the position is being requested for.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean getStartPosition(final long recordingId, final long correlationId, final long controlSessionId)
    {
        if (null == startPositionRequest)
        {
            startPositionRequest = new StartPositionRequestEncoder();
        }

        startPositionRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId);

        return offer(startPositionRequest.encodedLength());
    }

    /**
     * Get the stop position of a recording.
     *
     * @param recordingId      of the recording that the stop position is being requested for.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean getStopPosition(final long recordingId, final long correlationId, final long controlSessionId)
    {
        if (null == stopPositionRequest)
        {
            stopPositionRequest = new StopPositionRequestEncoder();
        }

        stopPositionRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId);

        return offer(stopPositionRequest.encodedLength());
    }

    /**
     * Get the stop or active recorded position of a recording.
     *
     * @param recordingId      of the recording that the stop of active recording position is being requested for.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean getMaxRecordedPosition(
        final long recordingId, final long correlationId, final long controlSessionId)
    {
        if (null == maxRecordedPositionRequestEncoder)
        {
            maxRecordedPositionRequestEncoder = new MaxRecordedPositionRequestEncoder();
        }

        maxRecordedPositionRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId);

        return offer(maxRecordedPositionRequestEncoder.encodedLength());
    }

    /**
     * Get the id of the Archive.
     *
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean archiveId(final long correlationId, final long controlSessionId)
    {
        if (null == archiveIdRequestEncoder)
        {
            archiveIdRequestEncoder = new ArchiveIdRequestEncoder();
        }

        archiveIdRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId);

        return offer(archiveIdRequestEncoder.encodedLength());
    }

    /**
     * Find the last recording that matches the given criteria.
     *
     * @param minRecordingId   to search back to.
     * @param channelFragment  for a contains match on the original channel stored with the archive descriptor.
     * @param streamId         of the recording to match.
     * @param sessionId        of the recording to match.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean findLastMatchingRecording(
        final long minRecordingId,
        final String channelFragment,
        final int streamId,
        final int sessionId,
        final long correlationId,
        final long controlSessionId)
    {
        if (null == findLastMatchingRecordingRequest)
        {
            findLastMatchingRecordingRequest = new FindLastMatchingRecordingRequestEncoder();
        }

        findLastMatchingRecordingRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .minRecordingId(minRecordingId)
            .sessionId(sessionId)
            .streamId(streamId)
            .channel(channelFragment);

        return offer(findLastMatchingRecordingRequest.encodedLength());
    }

    /**
     * List registered subscriptions in the archive which have been used to record streams.
     *
     * @param pseudoIndex       in the list of active recording subscriptions.
     * @param subscriptionCount for the number of descriptors to be listed.
     * @param channelFragment   for a contains match on the stripped channel used with the registered subscription.
     * @param streamId          for the subscription.
     * @param applyStreamId     when matching.
     * @param correlationId     for this request.
     * @param controlSessionId  for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean listRecordingSubscriptions(
        final int pseudoIndex,
        final int subscriptionCount,
        final String channelFragment,
        final int streamId,
        final boolean applyStreamId,
        final long correlationId,
        final long controlSessionId)
    {
        if (null == listRecordingSubscriptionsRequest)
        {
            listRecordingSubscriptionsRequest = new ListRecordingSubscriptionsRequestEncoder();
        }

        listRecordingSubscriptionsRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .pseudoIndex(pseudoIndex)
            .subscriptionCount(subscriptionCount)
            .applyStreamId(applyStreamId ? BooleanType.TRUE : BooleanType.FALSE)
            .streamId(streamId)
            .channel(channelFragment);

        return offer(listRecordingSubscriptionsRequest.encodedLength());
    }

    /**
     * Replicate a recording from a source archive to a destination which can be considered a backup for a primary
     * archive. The source recording will be replayed via the provided replay channel and use the original stream id.
     * If the destination recording id is {@link io.aeron.Aeron#NULL_VALUE} then a new destination recording is created,
     * otherwise the provided destination recording id will be extended. The details of the source recording
     * descriptor will be replicated.
     * <p>
     * For a source recording that is still active the replay can merge with the live stream and then follow it
     * directly and no longer require the replay from the source. This would require a multicast live destination.
     * <p>
     * Errors will be reported asynchronously and can be checked for with {@link AeronArchive#pollForErrorResponse()}
     * or {@link AeronArchive#checkForErrorResponse()}.
     *
     * @param srcRecordingId     recording id which must exist in the source archive.
     * @param dstRecordingId     recording to extend in the destination, otherwise {@link io.aeron.Aeron#NULL_VALUE}.
     * @param srcControlChannel  remote control channel for the source archive to instruct the replay on.
     * @param srcControlStreamId remote control stream id for the source archive to instruct the replay on.
     * @param liveDestination    destination for the live stream if merge is required. Empty or null for no merge.
     * @param correlationId      for this request.
     * @param controlSessionId   for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean replicate(
        final long srcRecordingId,
        final long dstRecordingId,
        final int srcControlStreamId,
        final String srcControlChannel,
        final String liveDestination,
        final long correlationId,
        final long controlSessionId)
    {
        return replicate(
            srcRecordingId,
            dstRecordingId,
            AeronArchive.NULL_POSITION,
            Aeron.NULL_VALUE,
            Aeron.NULL_VALUE,
            srcControlStreamId,
            srcControlChannel,
            liveDestination,
            null,
            correlationId,
            controlSessionId,
            Aeron.NULL_VALUE,
            Aeron.NULL_VALUE,
            NullCredentialsSupplier.NULL_CREDENTIAL,
            null);
    }

    /**
     * Replicate a recording from a source archive to a destination which can be considered a backup for a primary
     * archive. The source recording will be replayed via the provided replay channel and use the original stream id.
     * If the destination recording id is {@link io.aeron.Aeron#NULL_VALUE} then a new destination recording is created,
     * otherwise the provided destination recording id will be extended. The details of the source recording
     * descriptor will be replicated.
     * <p>
     * For a source recording that is still active the replay can merge with the live stream and then follow it
     * directly and no longer require the replay from the source. This would require a multicast live destination.
     * <p>
     * Errors will be reported asynchronously and can be checked for with {@link AeronArchive#pollForErrorResponse()}
     * or {@link AeronArchive#checkForErrorResponse()}.
     *
     * @param srcRecordingId     recording id which must exist in the source archive.
     * @param dstRecordingId     recording to extend in the destination, otherwise {@link io.aeron.Aeron#NULL_VALUE}.
     * @param stopPosition       position to stop the replication. {@link AeronArchive#NULL_POSITION} to stop at end
     *                           of current recording.
     * @param srcControlStreamId remote control stream id for the source archive to instruct the replay on.
     * @param srcControlChannel  remote control channel for the source archive to instruct the replay on.
     * @param liveDestination    destination for the live stream if merge is required. Empty or null for no merge.
     * @param replicationChannel channel over which the replication will occur. Empty or null for default channel.
     * @param correlationId      for this request.
     * @param controlSessionId   for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean replicate(
        final long srcRecordingId,
        final long dstRecordingId,
        final long stopPosition,
        final int srcControlStreamId,
        final String srcControlChannel,
        final String liveDestination,
        final String replicationChannel,
        final long correlationId,
        final long controlSessionId)
    {
        return replicate(
            srcRecordingId,
            dstRecordingId,
            stopPosition,
            Aeron.NULL_VALUE,
            Aeron.NULL_VALUE,
            srcControlStreamId,
            srcControlChannel,
            liveDestination,
            replicationChannel,
            correlationId,
            controlSessionId,
            Aeron.NULL_VALUE,
            Aeron.NULL_VALUE,
            NullCredentialsSupplier.NULL_CREDENTIAL,
            null);
    }

    /**
     * Replicate a recording from a source archive to a destination which can be considered a backup for a primary
     * archive. The source recording will be replayed via the provided replay channel and use the original stream id.
     * If the destination recording id is {@link io.aeron.Aeron#NULL_VALUE} then a new destination recording is created,
     * otherwise the provided destination recording id will be extended. The details of the source recording
     * descriptor will be replicated. The subscription used in the archive will be tagged with the provided tags.
     * <p>
     * For a source recording that is still active the replay can merge with the live stream and then follow it
     * directly and no longer require the replay from the source. This would require a multicast live destination.
     * <p>
     * Errors will be reported asynchronously and can be checked for with {@link AeronArchive#pollForErrorResponse()}
     * or {@link AeronArchive#checkForErrorResponse()}.
     *
     * @param srcRecordingId     recording id which must exist in the source archive.
     * @param dstRecordingId     recording to extend in the destination, otherwise {@link io.aeron.Aeron#NULL_VALUE}.
     * @param channelTagId       used to tag the replication subscription.
     * @param subscriptionTagId  used to tag the replication subscription.
     * @param srcControlChannel  remote control channel for the source archive to instruct the replay on.
     * @param srcControlStreamId remote control stream id for the source archive to instruct the replay on.
     * @param liveDestination    destination for the live stream if merge is required. Empty or null for no merge.
     * @param correlationId      for this request.
     * @param controlSessionId   for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean taggedReplicate(
        final long srcRecordingId,
        final long dstRecordingId,
        final long channelTagId,
        final long subscriptionTagId,
        final int srcControlStreamId,
        final String srcControlChannel,
        final String liveDestination,
        final long correlationId,
        final long controlSessionId)
    {
        return replicate(
            srcRecordingId,
            dstRecordingId,
            AeronArchive.NULL_POSITION,
            channelTagId,
            subscriptionTagId,
            srcControlStreamId,
            srcControlChannel,
            liveDestination,
            null,
            correlationId,
            controlSessionId,
            Aeron.NULL_VALUE,
            Aeron.NULL_VALUE,
            NullCredentialsSupplier.NULL_CREDENTIAL,
            null);
    }

    /**
     * Replicate a recording from a source archive to a destination which can be considered a backup for a primary
     * archive. The source recording will be replayed via the provided replay channel and use the original stream id.
     * If the destination recording id is {@link io.aeron.Aeron#NULL_VALUE} then a new destination recording is created,
     * otherwise the provided destination recording id will be extended. The details of the source recording
     * descriptor will be replicated. The subscription used in the archive will be tagged with the provided tags.
     * <p>
     * For a source recording that is still active the replay can merge with the live stream and then follow it
     * directly and no longer require the replay from the source. This would require a multicast live destination.
     * <p>
     * Errors will be reported asynchronously and can be checked for with {@link AeronArchive#pollForErrorResponse()}
     * or {@link AeronArchive#checkForErrorResponse()}.
     *
     * @param srcRecordingId     recording id which must exist in the source archive.
     * @param dstRecordingId     recording to extend in the destination, otherwise {@link io.aeron.Aeron#NULL_VALUE}.
     * @param stopPosition       position to stop the replication. {@link AeronArchive#NULL_POSITION} to stop at end
     *                           of current recording.
     * @param channelTagId       used to tag the replication subscription.
     * @param subscriptionTagId  used to tag the replication subscription.
     * @param srcControlChannel  remote control channel for the source archive to instruct the replay on.
     * @param srcControlStreamId remote control stream id for the source archive to instruct the replay on.
     * @param liveDestination    destination for the live stream if merge is required. Empty or null for no merge.
     * @param replicationChannel channel over which the replication will occur. Empty or null for default channel.
     * @param correlationId      for this request.
     * @param controlSessionId   for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean taggedReplicate(
        final long srcRecordingId,
        final long dstRecordingId,
        final long stopPosition,
        final long channelTagId,
        final long subscriptionTagId,
        final int srcControlStreamId,
        final String srcControlChannel,
        final String liveDestination,
        final String replicationChannel,
        final long correlationId,
        final long controlSessionId)
    {
        return replicate(
            srcRecordingId,
            dstRecordingId,
            stopPosition,
            channelTagId,
            subscriptionTagId,
            srcControlStreamId,
            srcControlChannel,
            liveDestination,
            replicationChannel,
            correlationId,
            controlSessionId,
            Aeron.NULL_VALUE,
            Aeron.NULL_VALUE,
            NullCredentialsSupplier.NULL_CREDENTIAL,
            null);
    }

    /**
     * Replicate a recording from a source archive to a destination which can be considered a backup for a primary
     * archive. The behaviour of the replication is controlled through the {@link ReplicationParams}.
     * <p>
     * For a source recording that is still active the replay can merge with the live stream and then follow it
     * directly and no longer require the replay from the source. This would require a multicast live destination.
     * <p>
     * Errors will be reported asynchronously and can be checked for with {@link AeronArchive#pollForErrorResponse()}
     * or {@link AeronArchive#checkForErrorResponse()}.
     * <p>
     * The ReplicationParams is free to be reused when this call completes.
     *
     * @param srcRecordingId     recording id which must exist in the source archive.
     * @param srcControlChannel  remote control channel for the source archive to instruct the replay on.
     * @param srcControlStreamId remote control stream id for the source archive to instruct the replay on.
     * @param replicationParams  optional parameters to control the behaviour of the replication.
     * @param correlationId      for this request.
     * @param controlSessionId   for this request.
     * @return true if successfully offered otherwise false.
     * @see ReplicationParams
     */
    public boolean replicate(
        final long srcRecordingId,
        final int srcControlStreamId,
        final String srcControlChannel,
        final ReplicationParams replicationParams,
        final long correlationId,
        final long controlSessionId)
    {
        if (null != replicationParams.liveDestination() && Aeron.NULL_VALUE != replicationParams.replicationSessionId())
        {
            throw new IllegalArgumentException(
                "ReplicationParams.liveDestination and ReplicationParams.sessionId can not be specified together");
        }

        return replicate(
            srcRecordingId,
            replicationParams.dstRecordingId(),
            replicationParams.stopPosition(),
            replicationParams.channelTagId(),
            replicationParams.subscriptionTagId(),
            srcControlStreamId,
            srcControlChannel,
            replicationParams.liveDestination(),
            replicationParams.replicationChannel(),
            correlationId,
            controlSessionId,
            replicationParams.fileIoMaxLength(),
            replicationParams.replicationSessionId(),
            replicationParams.encodedCredentials(),
            replicationParams.srcResponseChannel());
    }

    /**
     * Stop an active replication by the registration id it was registered with.
     *
     * @param replicationId    that identifies the session in the archive doing the replication.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean stopReplication(final long replicationId, final long correlationId, final long controlSessionId)
    {
        if (null == stopReplicationRequest)
        {
            stopReplicationRequest = new StopReplicationRequestEncoder();
        }

        stopReplicationRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .replicationId(replicationId);

        return offer(stopReplicationRequest.encodedLength());
    }

    /**
     * Detach segments from the beginning of a recording up to the provided new start position.
     * <p>
     * The new start position must be first byte position of a segment after the existing start position.
     * <p>
     * It is not possible to detach segments which are active for recording or being replayed.
     *
     * @param recordingId      to which the operation applies.
     * @param newStartPosition for the recording after the segments are detached.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     * @see AeronArchive#segmentFileBasePosition(long, long, int, int)
     */
    public boolean detachSegments(
        final long recordingId, final long newStartPosition, final long correlationId, final long controlSessionId)
    {
        if (null == detachSegmentsRequest)
        {
            detachSegmentsRequest = new DetachSegmentsRequestEncoder();
        }

        detachSegmentsRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId)
            .newStartPosition(newStartPosition);

        return offer(detachSegmentsRequest.encodedLength());
    }

    /**
     * Delete segments which have been previously detached from a recording.
     *
     * @param recordingId      to which the operation applies.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     * @see #detachSegments(long, long, long, long)
     */
    public boolean deleteDetachedSegments(final long recordingId, final long correlationId, final long controlSessionId)
    {
        if (null == deleteDetachedSegmentsRequest)
        {
            deleteDetachedSegmentsRequest = new DeleteDetachedSegmentsRequestEncoder();
        }

        deleteDetachedSegmentsRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId);

        return offer(deleteDetachedSegmentsRequest.encodedLength());
    }

    /**
     * Purge (detach and delete) segments from the beginning of a recording up to the provided new start position.
     * <p>
     * The new start position must be first byte position of a segment after the existing start position.
     * <p>
     * It is not possible to purge segments which are active for recording or being replayed.
     *
     * @param recordingId      to which the operation applies.
     * @param newStartPosition for the recording after the segments are detached.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     * @see #detachSegments(long, long, long, long)
     * @see #deleteDetachedSegments(long, long, long)
     * @see AeronArchive#segmentFileBasePosition(long, long, int, int)
     */
    public boolean purgeSegments(
        final long recordingId, final long newStartPosition, final long correlationId, final long controlSessionId)
    {
        if (null == purgeSegmentsRequest)
        {
            purgeSegmentsRequest = new PurgeSegmentsRequestEncoder();
        }

        purgeSegmentsRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId)
            .newStartPosition(newStartPosition);

        return offer(purgeSegmentsRequest.encodedLength());
    }

    /**
     * Attach segments to the beginning of a recording to restore history that was previously detached.
     * <p>
     * Segment files must match the existing recording and join exactly to the start position of the recording
     * they are being attached to.
     *
     * @param recordingId      to which the operation applies.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     * @see #detachSegments(long, long, long, long)
     */
    public boolean attachSegments(final long recordingId, final long correlationId, final long controlSessionId)
    {
        if (null == attachSegmentsRequest)
        {
            attachSegmentsRequest = new AttachSegmentsRequestEncoder();
        }

        attachSegmentsRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId);

        return offer(attachSegmentsRequest.encodedLength());
    }

    /**
     * Migrate segments from a source recording and attach them to the beginning or end of a destination recording.
     * <p>
     * The source recording must match the destination recording for segment length, term length, mtu length, and
     * stream id. The source recording must join to the destination recording on a segment boundary and without gaps,
     * i.e., the stop position and term id of one must match the start position and term id of the other.
     * <p>
     * The source recording must be stopped. The destination recording must be stopped if migrating segments
     * to the end of the destination recording.
     * <p>
     * The source recording will be effectively truncated back to its start position after the migration.
     *
     * @param srcRecordingId   source recording from which the segments will be migrated.
     * @param dstRecordingId   destination recording to which the segments will be attached.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean migrateSegments(
        final long srcRecordingId, final long dstRecordingId, final long correlationId, final long controlSessionId)
    {
        if (null == migrateSegmentsRequest)
        {
            migrateSegmentsRequest = new MigrateSegmentsRequestEncoder();
        }

        migrateSegmentsRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .srcRecordingId(srcRecordingId)
            .dstRecordingId(dstRecordingId);

        return offer(migrateSegmentsRequest.encodedLength());
    }

    /**
     * Request a token for this session that will allow a replay to be initiated from another image without
     * re-authentication.
     *
     * @param lastCorrelationId for the request
     * @param controlSessionId  for the request
     * @param recordingId       that will be replayed.
     * @return true if successfully offered
     */
    public boolean requestReplayToken(final long lastCorrelationId, final long controlSessionId, final long recordingId)
    {
        if (null == replayTokenRequestEncoder)
        {
            replayTokenRequestEncoder = new ReplayTokenRequestEncoder();
        }

        replayTokenRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(lastCorrelationId)
            .recordingId(recordingId);

        return offer(replayTokenRequestEncoder.encodedLength());
    }


    private boolean offer(final int length)
    {
        retryIdleStrategy.reset();

        int attempts = retryAttempts;
        while (true)
        {
            final long position = this.publication.offer(buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + length);
            if (position > 0)
            {
                return true;
            }

            if (position == Publication.CLOSED)
            {
                throw new ArchiveException("connection to the archive has been closed");
            }

            if (position == Publication.NOT_CONNECTED)
            {
                throw new ArchiveException("connection to the archive is no longer available");
            }

            if (position == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new ArchiveException(
                    "offer failed due to max position being reached: term-length=" + publication.termBufferLength());
            }

            if (--attempts <= 0)
            {
                return false;
            }

            retryIdleStrategy.idle();
        }
    }

    private boolean offerWithTimeout(final int length, final AgentInvoker aeronClientInvoker)
    {
        retryIdleStrategy.reset();

        final long deadlineNs = nanoClock.nanoTime() + connectTimeoutNs;
        while (true)
        {
            final long position = publication.offer(buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + length);
            if (position > 0)
            {
                return true;
            }

            if (position == Publication.CLOSED)
            {
                throw new ArchiveException("connection to the archive has been closed");
            }

            if (position == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new ArchiveException(
                    "offer failed due to max position being reached: term-length=" + publication.termBufferLength());
            }

            if (deadlineNs - nanoClock.nanoTime() < 0)
            {
                return false;
            }

            if (null != aeronClientInvoker)
            {
                aeronClientInvoker.invoke();
            }

            retryIdleStrategy.idle();
        }
    }

    private boolean replay(
        final long recordingId,
        final long position,
        final long length,
        final String replayChannel,
        final int replayStreamId,
        final long correlationId,
        final long controlSessionId,
        final int fileIoMaxLength,
        final long replayToken)
    {
        if (null == replayRequest)
        {
            replayRequest = new ReplayRequestEncoder();
        }

        replayRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId)
            .position(position)
            .length(length)
            .replayStreamId(replayStreamId)
            .fileIoMaxLength(fileIoMaxLength)
            .replayToken(replayToken)
            .replayChannel(replayChannel);

        return offer(replayRequest.encodedLength());
    }

    private boolean boundedReplay(
        final long recordingId,
        final long position,
        final long length,
        final int limitCounterId,
        final String replayChannel,
        final int replayStreamId,
        final long correlationId,
        final long controlSessionId,
        final int fileIoMaxLength,
        final long replayToken)
    {
        if (null == boundedReplayRequest)
        {
            boundedReplayRequest = new BoundedReplayRequestEncoder();
        }

        boundedReplayRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId)
            .position(position)
            .length(length)
            .limitCounterId(limitCounterId)
            .replayStreamId(replayStreamId)
            .fileIoMaxLength(fileIoMaxLength)
            .replayToken(replayToken)
            .replayChannel(replayChannel);

        return offer(boundedReplayRequest.encodedLength());
    }

    private boolean replicate(
        final long srcRecordingId,
        final long dstRecordingId,
        final long stopPosition,
        final long channelTagId,
        final long subscriptionTagId,
        final int srcControlStreamId,
        final String srcControlChannel,
        final String liveDestination,
        final String replicationChannel,
        final long correlationId,
        final long controlSessionId,
        final int fileIoMaxLength,
        final int replicationSessionId,
        final byte[] encodedCredentials,
        final String srcResponseChannel)
    {
        if (null == replicateRequest)
        {
            replicateRequest = new ReplicateRequest2Encoder();
        }

        replicateRequest
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .srcRecordingId(srcRecordingId)
            .dstRecordingId(dstRecordingId)
            .stopPosition(stopPosition)
            .channelTagId(channelTagId)
            .subscriptionTagId(subscriptionTagId)
            .srcControlStreamId(srcControlStreamId)
            .fileIoMaxLength(fileIoMaxLength)
            .srcControlChannel(srcControlChannel)
            .liveDestination(liveDestination)
            .replicationChannel(replicationChannel)
            .replicationSessionId(replicationSessionId)
            .putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length)
            .srcResponseChannel(srcResponseChannel);

        return offer(replicateRequest.encodedLength());
    }
}
