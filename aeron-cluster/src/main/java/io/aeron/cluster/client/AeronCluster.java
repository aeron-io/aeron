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
package io.aeron.cluster.client;

import io.aeron.*;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.codecs.*;
import io.aeron.config.Config;
import io.aeron.config.DefaultType;
import io.aeron.exceptions.*;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.security.AuthenticationException;
import io.aeron.security.CredentialsSupplier;
import io.aeron.security.NullCredentialsSupplier;
import io.aeron.version.Versioned;
import org.agrona.*;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.*;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.REJOIN_PARAM_NAME;
import static org.agrona.SystemUtil.getDurationInNanos;

/**
 * Client for interacting with an Aeron Cluster.
 * <p>
 * A client will attempt to open a session and then offer ingress messages which are replicated to clustered services
 * for reliability. If the clustered service responds then response messages and events are sent via the egress stream.
 * <p>
 * <b>Note:</b> Instances of this class are not threadsafe.
 */
@Versioned
public final class AeronCluster implements AutoCloseable
{
    /**
     * Length of a session message header for cluster ingress or egress.
     */
    public static final int SESSION_HEADER_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + SessionMessageHeaderEncoder.BLOCK_LENGTH;

    private static final int SEND_ATTEMPTS = 3;
    private static final int FRAGMENT_LIMIT = 10;

    private final long clusterSessionId;
    private long leadershipTermId;
    private int leaderMemberId;
    private final Context ctx;
    private final Subscription subscription;
    private State state;
    private long stateDeadline;
    private Image egressImage;
    private Publication publication;
    private final NanoClock nanoClock;
    private final IdleStrategy idleStrategy;
    private final BufferClaim bufferClaim = new BufferClaim();
    private final UnsafeBuffer headerBuffer = new UnsafeBuffer(new byte[SESSION_HEADER_LENGTH]);
    private final DirectBufferVector headerVector = new DirectBufferVector(headerBuffer, 0, SESSION_HEADER_LENGTH);
    private final MessageHeaderEncoder messageHeaderEncoder;
    private final SessionMessageHeaderEncoder sessionMessageHeaderEncoder = new SessionMessageHeaderEncoder();
    private final SessionKeepAliveEncoder sessionKeepAliveEncoder = new SessionKeepAliveEncoder();
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionMessageHeaderDecoder sessionMessageHeaderDecoder = new SessionMessageHeaderDecoder();
    private final NewLeaderEventDecoder newLeaderEventDecoder = new NewLeaderEventDecoder();
    private final SessionEventDecoder sessionEventDecoder = new SessionEventDecoder();
    private final AdminRequestEncoder adminRequestEncoder = new AdminRequestEncoder();
    private final AdminResponseDecoder adminResponseDecoder = new AdminResponseDecoder();
    private final FragmentAssembler fragmentAssembler;
    private final EgressListener egressListener;
    private final ControlledFragmentAssembler controlledFragmentAssembler;
    private final ControlledEgressListener controlledEgressListener;
    private EgressListenerExtension egressListenerExtension;
    private ControlledEgressListenerExtension controlledEgressListenerExtension;
    private Int2ObjectHashMap<MemberIngress> endpointByIdMap;

    /**
     * Connect to the cluster using default configuration.
     *
     * @return allocated cluster client if the connection is successful.
     */
    public static AeronCluster connect()
    {
        return connect(new Context());
    }

    /**
     * Connect to the cluster providing {@link Context} for configuration.
     *
     * @param ctx for configuration.
     * @return allocated cluster client if the connection is successful.
     */
    public static AeronCluster connect(final AeronCluster.Context ctx)
    {
        AsyncConnect asyncConnect = null;
        try
        {
            ctx.conclude();

            final Aeron aeron = ctx.aeron();
            final long deadlineNs = aeron.context().nanoClock().nanoTime() + ctx.messageTimeoutNs();
            asyncConnect = new AsyncConnect(ctx, deadlineNs);
            final AgentInvoker aeronClientInvoker = aeron.conductorAgentInvoker();
            final AgentInvoker agentInvoker = ctx.agentInvoker();
            final IdleStrategy idleStrategy = ctx.idleStrategy();

            AeronCluster aeronCluster;
            AsyncConnect.State state = asyncConnect.state();
            while (null == (aeronCluster = asyncConnect.poll()))
            {
                if (null != aeronClientInvoker)
                {
                    aeronClientInvoker.invoke();
                }

                if (null != agentInvoker)
                {
                    agentInvoker.invoke();
                }

                if (state != asyncConnect.state())
                {
                    state = asyncConnect.state();
                    idleStrategy.reset();
                }
                else
                {
                    idleStrategy.idle();
                }
            }

            return aeronCluster;
        }
        catch (final ConcurrentConcludeException ex)
        {
            throw ex;
        }
        catch (final Exception ex)
        {
            if (!ctx.ownsAeronClient())
            {
                CloseHelper.quietCloseAll(asyncConnect);
            }

            CloseHelper.quietClose(ctx::close);

            throw ex;
        }
    }

    /**
     * Begin an attempt at creating a connection which can be completed by calling {@link AsyncConnect#poll()} until
     * it returns the client, before complete it will return null.
     *
     * @return the {@link AsyncConnect} that can be polled for completion.
     */
    public static AsyncConnect asyncConnect()
    {
        return asyncConnect(new Context());
    }

    /**
     * Begin an attempt at creating a connection which can be completed by calling {@link AsyncConnect#poll()} until
     * it returns the client, before complete it will return null.
     *
     * @param ctx for the cluster.
     * @return the {@link AsyncConnect} that can be polled for completion.
     */
    public static AsyncConnect asyncConnect(final Context ctx)
    {
        try
        {
            ctx.conclude();

            final long deadlineNs = ctx.aeron().context().nanoClock().nanoTime() + ctx.messageTimeoutNs();

            return new AsyncConnect(ctx, deadlineNs);
        }
        catch (final Exception ex)
        {
            ctx.close();

            throw ex;
        }
    }

    AeronCluster(
        final Context ctx,
        final MessageHeaderEncoder messageHeaderEncoder,
        final Publication publication,
        final Subscription subscription,
        final Image egressImage,
        final Int2ObjectHashMap<MemberIngress> endpointByIdMap,
        final long clusterSessionId,
        final long leadershipTermId,
        final int leaderMemberId)
    {
        this.ctx = ctx;
        this.messageHeaderEncoder = messageHeaderEncoder;
        this.subscription = subscription;
        this.egressImage = egressImage;
        this.endpointByIdMap = endpointByIdMap;
        this.clusterSessionId = clusterSessionId;
        this.leadershipTermId = leadershipTermId;
        this.leaderMemberId = leaderMemberId;
        this.publication = publication;

        this.nanoClock = ctx.aeron().context().nanoClock();
        this.idleStrategy = ctx.idleStrategy();
        this.egressListener = ctx.egressListener();
        this.fragmentAssembler = new FragmentAssembler(this::onFragment, 0, ctx.isDirectAssemblers());
        this.controlledEgressListener = ctx.controlledEgressListener();
        this.controlledFragmentAssembler = new ControlledFragmentAssembler(
            this::onControlledFragment, 0, ctx.isDirectAssemblers());

        sessionMessageHeaderEncoder
            .wrapAndApplyHeader(headerBuffer, 0, messageHeaderEncoder)
            .clusterSessionId(clusterSessionId)
            .leadershipTermId(leadershipTermId);

        state(State.CONNECTED, 0);
    }

    /**
     * An EgressListener for extension schemas.
     *
     * @param listenerExtension listener extension.
     */
    public void egressListenerExtension(final EgressListenerExtension listenerExtension)
    {
        this.egressListenerExtension = listenerExtension;
    }

    /**
     * A ControlledEgressListener for extension schemas.
     *
     * @param listenerExtension listener extension.
     */
    public void controlledEgressListenerExtension(final ControlledEgressListenerExtension listenerExtension)
    {
        this.controlledEgressListenerExtension = listenerExtension;
    }

    /**
     * Close session and release associated resources.
     */
    public void close()
    {
        if (State.CLOSED == state)
        {
            return;
        }

        if (null != publication && publication.isConnected() && State.CONNECTED == state)
        {
            closeSession();
        }

        if (!ctx.ownsAeronClient())
        {
            final ErrorHandler errorHandler = ctx.errorHandler();
            CloseHelper.close(errorHandler, subscription);
            CloseHelper.close(errorHandler, publication);
        }

        state(State.CLOSED, 0);
        ctx.close();
    }

    /**
     * Is the client closed? The client can be closed by calling {@link #close()}, the cluster sending an event,
     * or the client permanently losing a cluster connection.
     *
     * @return true if closed otherwise false.
     */
    public boolean isClosed()
    {
        return State.CLOSED == state;
    }

    /**
     * Get the context used to launch this cluster client.
     *
     * @return the context used to launch this cluster client.
     */
    public Context context()
    {
        return ctx;
    }

    /**
     * Cluster session id for the session that was opened as the result of a successful connect.
     *
     * @return session id for the session that was opened as the result of a successful connect.
     */
    public long clusterSessionId()
    {
        return clusterSessionId;
    }

    /**
     * Leadership term identity for the cluster. Advances with changing leadership.
     *
     * @return leadership term identity for the cluster.
     */
    public long leadershipTermId()
    {
        return leadershipTermId;
    }

    /**
     * Get the current leader member id for the cluster.
     *
     * @return the current leader member id for the cluster.
     */
    public int leaderMemberId()
    {
        return leaderMemberId;
    }

    /**
     * Get the raw {@link Publication} for sending to the cluster.
     * <p>
     * This can be wrapped with a {@link IngressSessionDecorator} for pre-pending the cluster session header to
     * messages.
     * {@link io.aeron.cluster.codecs.SessionMessageHeaderEncoder} should be used for raw access.
     * <p>
     * Results of offering to this publication should be passed to {@link #trackIngressPublicationResult(long)}.
     *
     * @return the raw {@link Publication} for connecting to the cluster.
     */
    public Publication ingressPublication()
    {
        return publication;
    }

    /**
     * Get the raw {@link Subscription} for receiving from the cluster.
     * <p>
     * This can be wrapped with a {@link EgressAdapter} for dispatching events from the cluster.
     * {@link io.aeron.cluster.codecs.SessionMessageHeaderDecoder} should be used for raw access.
     *
     * @return the raw {@link Subscription} for receiving from the cluster.
     */
    public Subscription egressSubscription()
    {
        return subscription;
    }

    /**
     * Try to claim a range in the publication log into which a message can be written with zero copy semantics.
     * Once the message has been written then {@link BufferClaim#commit()} should be called thus making it available.
     * <p>
     * On successful claim, the Cluster ingress header will be written to the start of the claimed buffer section.
     * Clients <b>MUST</b> write into the claimed buffer region at offset + {@link AeronCluster#SESSION_HEADER_LENGTH}.
     * <pre>{@code
     *     final DirectBuffer srcBuffer = acquireMessage();
     *
     *     if (aeronCluster.tryClaim(length, bufferClaim) > 0L)
     *     {
     *         try
     *         {
     *              final MutableDirectBuffer buffer = bufferClaim.buffer();
     *              final int offset = bufferClaim.offset();
     *              // ensure that data is written at the correct offset
     *              buffer.putBytes(offset + AeronCluster.SESSION_HEADER_LENGTH, srcBuffer, 0, length);
     *         }
     *         finally
     *         {
     *             bufferClaim.commit();
     *         }
     *     }
     * }</pre>
     *
     * @param length      of the range to claim in bytes. The additional bytes for the session header will be added.
     * @param bufferClaim to be populated if the claim succeeds.
     * @return The new stream position, otherwise a negative error value as specified in
     * {@link io.aeron.Publication#tryClaim(int, BufferClaim)}.
     * @throws IllegalArgumentException if the length is greater than {@link io.aeron.Publication#maxPayloadLength()}.
     * @see Publication#tryClaim(int, BufferClaim)
     * @see BufferClaim#commit()
     * @see BufferClaim#abort()
     */
    public long tryClaim(final int length, final BufferClaim bufferClaim)
    {
        final long offset = publication.tryClaim(length + SESSION_HEADER_LENGTH, bufferClaim);

        trackIngressPublicationResult(offset);

        if (offset > 0)
        {
            bufferClaim.putBytes(headerBuffer, 0, SESSION_HEADER_LENGTH);
        }

        return offset;
    }

    /**
     * Non-blocking publish of a partial buffer containing a message plus session header to a cluster.
     * <p>
     * This version of the method will set the timestamp value in the header to zero.
     *
     * @param buffer containing message.
     * @param offset offset in the buffer at which the encoded message begins.
     * @param length in bytes of the encoded message.
     * @return the same as {@link Publication#offer(DirectBuffer, int, int)}.
     */
    public long offer(final DirectBuffer buffer, final int offset, final int length)
    {
        final long result = publication.offer(headerBuffer, 0, SESSION_HEADER_LENGTH, buffer, offset, length, null);

        trackIngressPublicationResult(result);

        return result;
    }

    /**
     * Non-blocking publish by gathering buffer vectors into a message. The first vector will be replaced by the cluster
     * session message header so must be left unused.
     *
     * @param vectors which make up the message.
     * @return the same as {@link Publication#offer(DirectBufferVector[])}.
     * @see Publication#offer(DirectBufferVector[])
     */
    public long offer(final DirectBufferVector[] vectors)
    {
        vectors[0] = headerVector;

        final long result = publication.offer(vectors, null);

        trackIngressPublicationResult(result);

        return result;
    }

    /**
     * Send a keep alive message to the cluster to keep this session open.
     * <p>
     * <b>Note:</b> Sending keep-alive can fail during a leadership transition. The application should continue to call
     * {@link #pollEgress()} to ensure a connection to the new leader is established.
     *
     * @return true if successfully sent otherwise false if back pressured.
     */
    public boolean sendKeepAlive()
    {
        idleStrategy.reset();
        int attempts = SEND_ATTEMPTS;

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + SessionKeepAliveEncoder.BLOCK_LENGTH;

        while (true)
        {
            final long position = publication.tryClaim(length, bufferClaim);

            trackIngressPublicationResult(position);

            if (position > 0)
            {
                sessionKeepAliveEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .clusterSessionId(clusterSessionId);

                bufferClaim.commit();

                return true;
            }

            if (position == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new ClusterException("max position exceeded: term-length=" + publication.termBufferLength());
            }

            if (--attempts <= 0)
            {
                break;
            }

            idleStrategy.idle();
            invokeInvokers();
        }

        return false;
    }

    /**
     * Sends an admin request to initiate a snapshot action in the cluster. This request requires elevated privileges.
     *
     * @param correlationId for the request.
     * @return {@code true} if the request was sent or {@code false} otherwise.
     * @see EgressListener#onAdminResponse(long, long, AdminRequestType, AdminResponseCode, String, DirectBuffer, int, int)
     * @see ControlledEgressListener#onAdminResponse(long, long, AdminRequestType, AdminResponseCode, String, DirectBuffer, int, int)
     */
    public boolean sendAdminRequestToTakeASnapshot(final long correlationId)
    {
        idleStrategy.reset();
        int attempts = SEND_ATTEMPTS;

        final int length =
            MessageHeaderEncoder.ENCODED_LENGTH +
            AdminRequestEncoder.BLOCK_LENGTH +
            AdminRequestEncoder.payloadHeaderLength();

        while (true)
        {
            final long position = publication.tryClaim(length, bufferClaim);

            trackIngressPublicationResult(position);

            if (position > 0)
            {
                adminRequestEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .clusterSessionId(clusterSessionId)
                    .correlationId(correlationId)
                    .requestType(AdminRequestType.SNAPSHOT)
                    .putPayload(ArrayUtil.EMPTY_BYTE_ARRAY, 0, 0);

                bufferClaim.commit();

                return true;
            }

            if (position == Publication.CLOSED)
            {
                throw new ClusterException("ingress publication is closed");
            }

            if (position == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new ClusterException("max position exceeded: term-length=" + publication.termBufferLength());
            }

            if (--attempts <= 0)
            {
                break;
            }

            idleStrategy.idle();
            invokeInvokers();
        }

        return false;
    }

    /**
     * Poll the {@link #egressSubscription()} for session messages which are dispatched to
     * {@link Context#egressListener()}. Invoking this method, or {@link #controlledPollEgress()}, frequently is
     * important for detecting leadership changes in a cluster.
     * <p>
     * <b>Note:</b> if {@link Context#egressListener()} is not set then a {@link ConfigurationException} could result.
     *
     * @return 0 if no work was available, or a positive number if work has been done,
     * typically the number of fragments processed.
     * @see #controlledPollEgress()
     */
    public int pollEgress()
    {
        int workCount = subscription.poll(fragmentAssembler, FRAGMENT_LIMIT);

        if (egressImage.isClosed() && (State.CONNECTED == state || State.AWAIT_NEW_LEADER_CONNECTION == state))
        {
            onDisconnected();
            workCount++;
        }

        workCount += pollStateChanges();

        return workCount;
    }

    /**
     * Poll the {@link #egressSubscription()} for session messages which are dispatched to
     * {@link Context#controlledEgressListener()}. Invoking this method, or {@link #pollEgress()}, frequently is
     * important for detecting leadership changes in a cluster.
     * <p>
     * <b>Note:</b> if {@link Context#controlledEgressListener()} is not set then a {@link ConfigurationException}
     * could result.
     *
     * @return 0 if no work was available, a positive number if work has been done,
     * typically the number of fragments processed.
     * @see #pollEgress()
     */
    public int controlledPollEgress()
    {
        int workCount = subscription.controlledPoll(controlledFragmentAssembler, FRAGMENT_LIMIT);

        if (egressImage.isClosed() && (State.CONNECTED == state || State.AWAIT_NEW_LEADER_CONNECTION == state))
        {
            onDisconnected();
            workCount++;
        }

        workCount += pollStateChanges();

        return workCount;
    }

    /**
     * Polls for client state changes. Needs to be called explicitly only by applications which use
     * {@link #egressSubscription()} directly instead of calling {@link #pollEgress()} or
     * {@link #controlledPollEgress()}.
     *
     * @return 0 if state has not changed, a positive number otherwise.
     */
    public int pollStateChanges()
    {
        if (State.PENDING_CLOSE == state ||
            ((State.AWAIT_NEW_LEADER == state || State.AWAIT_NEW_LEADER_CONNECTION == state) &&
            0 <= nanoClock.nanoTime() - stateDeadline))
        {
            close();

            return 1;
        }

        return 0;
    }

    /**
     * To be called when a new leader event is delivered. This method needs to be called when using the
     * {@link EgressAdapter} or {@link EgressPoller} rather than {@link #pollEgress()} method.
     *
     * @param clusterSessionId which must match {@link #clusterSessionId()}.
     * @param leadershipTermId that identifies the term for which the new leader has been elected.
     * @param leaderMemberId   which has become the new leader.
     * @param ingressEndpoints comma separated list of cluster ingress endpoints to connect to with the leader first.
     */
    public void onNewLeader(
        final long clusterSessionId,
        final long leadershipTermId,
        final int leaderMemberId,
        final String ingressEndpoints)
    {
        if (clusterSessionId != this.clusterSessionId)
        {
            throw new ClusterException(
                "invalid clusterSessionId=" + clusterSessionId + " expected=" + this.clusterSessionId);
        }

        state(State.AWAIT_NEW_LEADER_CONNECTION, nanoClock.nanoTime() + ctx.messageTimeoutNs());

        this.leadershipTermId = leadershipTermId;
        this.leaderMemberId = leaderMemberId;
        sessionMessageHeaderEncoder.leadershipTermId(leadershipTermId);

        CloseHelper.close(publication);
        if (ctx.ingressEndpoints() != null)
        {
            ctx.ingressEndpoints(ingressEndpoints);
            updateMemberEndpoints(ingressEndpoints, leaderMemberId);
        }
        else
        {
            publication = addIngressPublication(ctx, ctx.ingressChannel(), ctx.ingressStreamId());
        }

        fragmentAssembler.clear();
        controlledFragmentAssembler.clear();
        egressListener.onNewLeader(clusterSessionId, leadershipTermId, leaderMemberId, ingressEndpoints);
        controlledEgressListener.onNewLeader(clusterSessionId, leadershipTermId, leaderMemberId, ingressEndpoints);
    }

    /**
     * Updates the state of this client based on ingress publication result. Should be called with every {@code offer}
     * and {@code tryClaim} result when {@link #ingressPublication()} is used directly. Methods of this class which send
     * ingress messages call it automatically.
     *
     * @param result the result returned by the ingress publication.
     */
    public void trackIngressPublicationResult(final long result)
    {
        if (State.CONNECTED == state)
        {
            if (Publication.NOT_CONNECTED == result || Publication.CLOSED == result)
            {
                onDisconnected();
            }
            else if (Publication.MAX_POSITION_EXCEEDED == result)
            {
                publication.close();
                state(State.PENDING_CLOSE, 0);
            }
        }
        else if (State.AWAIT_NEW_LEADER_CONNECTION == state)
        {
            if (0 < result)
            {
                state(State.CONNECTED, 0);
            }
        }
    }

    static Int2ObjectHashMap<MemberIngress> parseIngressEndpoints(final Context ctx, final String endpoints)
    {
        final Int2ObjectHashMap<MemberIngress> endpointByIdMap = new Int2ObjectHashMap<>();

        if (null != endpoints)
        {
            for (final String endpoint : endpoints.split(","))
            {
                final int i = endpoint.indexOf('=');
                if (-1 == i)
                {
                    throw new ConfigurationException("endpoint missing '=' separator: " + endpoints);
                }

                final int memberId = AsciiEncoding.parseIntAscii(endpoint, 0, i);
                endpointByIdMap.put(memberId, new MemberIngress(ctx, memberId, endpoint.substring(i + 1)));
            }
        }

        return endpointByIdMap;
    }

    static Publication addIngressPublication(final Context ctx, final String channel, final int streamId)
    {
        if (ctx.isIngressExclusive())
        {
            return ctx.aeron().addExclusivePublication(channel, streamId);
        }
        else
        {
            return ctx.aeron().addPublication(channel, streamId);
        }
    }

    static long asyncAddIngressPublication(final Context ctx, final String channel, final int streamId)
    {
        if (ctx.isIngressExclusive())
        {
            return ctx.aeron().asyncAddExclusivePublication(channel, streamId);
        }
        else
        {
            return ctx.aeron().asyncAddPublication(channel, streamId);
        }
    }

    static Publication getIngressPublication(final Context ctx, final long registrationId)
    {
        if (ctx.isIngressExclusive())
        {
            return ctx.aeron().getExclusivePublication(registrationId);
        }
        else
        {
            return ctx.aeron().getPublication(registrationId);
        }
    }

    private void updateMemberEndpoints(final String ingressEndpoints, final int leaderMemberId)
    {
        CloseHelper.closeAll(endpointByIdMap.values());

        final Int2ObjectHashMap<MemberIngress> map = parseIngressEndpoints(ctx, ingressEndpoints);
        final MemberIngress newLeader = map.get(leaderMemberId);
        final ChannelUri channelUri = ChannelUri.parse(ctx.ingressChannel());

        if (channelUri.isUdp())
        {
            channelUri.put(CommonContext.ENDPOINT_PARAM_NAME, newLeader.endpoint);
        }

        publication = addIngressPublication(ctx, channelUri.toString(), ctx.ingressStreamId());
        newLeader.publication = publication;
        endpointByIdMap = map;
    }

    private void onDisconnected()
    {
        publication.close();
        state(State.AWAIT_NEW_LEADER, nanoClock.nanoTime() + ctx.newLeaderTimeoutNs());
    }

    @SuppressWarnings("MethodLength")
    private void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        final int templateId = messageHeaderDecoder.templateId();

        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            if (egressListenerExtension != null)
            {
                egressListenerExtension.onExtensionMessage(
                    messageHeaderDecoder.blockLength(),
                    templateId,
                    schemaId,
                    messageHeaderDecoder.version(),
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    length - MessageHeaderDecoder.ENCODED_LENGTH);
            }
            else
            {
                throw new ClusterException(
                    "expected cluster egress schemaId=" + MessageHeaderDecoder.SCHEMA_ID + " actual=" + schemaId);
            }
        }

        switch (templateId)
        {
            case SessionMessageHeaderDecoder.TEMPLATE_ID:
            {
                sessionMessageHeaderDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long sessionId = sessionMessageHeaderDecoder.clusterSessionId();
                if (sessionId == clusterSessionId)
                {
                    egressListener.onMessage(
                        sessionId,
                        sessionMessageHeaderDecoder.timestamp(),
                        buffer,
                        offset + SESSION_HEADER_LENGTH,
                        length - SESSION_HEADER_LENGTH,
                        header);
                }
                break;
            }

            case SessionEventDecoder.TEMPLATE_ID:
            {
                sessionEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long sessionId = sessionEventDecoder.clusterSessionId();
                if (sessionId == clusterSessionId)
                {
                    final EventCode code = sessionEventDecoder.code();
                    if (EventCode.CLOSED == code)
                    {
                        state(State.PENDING_CLOSE, 0);
                    }

                    egressListener.onSessionEvent(
                        sessionEventDecoder.correlationId(),
                        sessionId,
                        sessionEventDecoder.leadershipTermId(),
                        sessionEventDecoder.leaderMemberId(),
                        code,
                        sessionEventDecoder.detail());
                }
                break;
            }

            case NewLeaderEventDecoder.TEMPLATE_ID:
            {
                newLeaderEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long sessionId = newLeaderEventDecoder.clusterSessionId();
                if (sessionId == clusterSessionId)
                {
                    egressImage = (Image)header.context();
                    onNewLeader(
                        sessionId,
                        newLeaderEventDecoder.leadershipTermId(),
                        newLeaderEventDecoder.leaderMemberId(),
                        newLeaderEventDecoder.ingressEndpoints());
                }
                break;
            }

            case AdminResponseDecoder.TEMPLATE_ID:
            {
                adminResponseDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long sessionId = adminResponseDecoder.clusterSessionId();
                if (sessionId == clusterSessionId)
                {
                    final long correlationId = adminResponseDecoder.correlationId();
                    final AdminRequestType requestType = adminResponseDecoder.requestType();
                    final AdminResponseCode responseCode = adminResponseDecoder.responseCode();
                    final String message = adminResponseDecoder.message();
                    final int payloadOffset = adminResponseDecoder.offset() +
                        AdminResponseDecoder.BLOCK_LENGTH +
                        AdminResponseDecoder.messageHeaderLength() +
                        message.length() +
                        AdminResponseDecoder.payloadHeaderLength();
                    final int payloadLength = adminResponseDecoder.payloadLength();

                    egressListener.onAdminResponse(
                        sessionId,
                        correlationId,
                        requestType,
                        responseCode,
                        message,
                        buffer,
                        payloadOffset,
                        payloadLength);
                }
                break;
            }

            default:
                break;
        }
    }

    @SuppressWarnings("MethodLength")
    private ControlledFragmentHandler.Action onControlledFragment(
        final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        final int templateId = messageHeaderDecoder.templateId();

        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            if (controlledEgressListenerExtension != null)
            {
                return controlledEgressListenerExtension.onExtensionMessage(
                    messageHeaderDecoder.blockLength(),
                    templateId,
                    schemaId,
                    messageHeaderDecoder.version(),
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    length - MessageHeaderDecoder.ENCODED_LENGTH);
            }
            else
            {
                throw new ClusterException(
                    "expected cluster egress schemaId=" + MessageHeaderDecoder.SCHEMA_ID + " actual=" + schemaId);
            }
        }

        switch (templateId)
        {
            case SessionMessageHeaderDecoder.TEMPLATE_ID:
            {
                sessionMessageHeaderDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long sessionId = sessionMessageHeaderDecoder.clusterSessionId();
                if (sessionId == clusterSessionId)
                {
                    return controlledEgressListener.onMessage(
                        sessionId,
                        sessionMessageHeaderDecoder.timestamp(),
                        buffer,
                        offset + SESSION_HEADER_LENGTH,
                        length - SESSION_HEADER_LENGTH,
                        header);
                }
                break;
            }

            case SessionEventDecoder.TEMPLATE_ID:
            {
                sessionEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long sessionId = sessionEventDecoder.clusterSessionId();
                if (sessionId == clusterSessionId)
                {
                    final EventCode code = sessionEventDecoder.code();
                    if (EventCode.CLOSED == code)
                    {
                        state(State.PENDING_CLOSE, 0);
                    }

                    controlledEgressListener.onSessionEvent(
                        sessionEventDecoder.correlationId(),
                        sessionId,
                        sessionEventDecoder.leadershipTermId(),
                        sessionEventDecoder.leaderMemberId(),
                        code,
                        sessionEventDecoder.detail());
                }
                break;
            }

            case NewLeaderEventDecoder.TEMPLATE_ID:
            {
                newLeaderEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long sessionId = newLeaderEventDecoder.clusterSessionId();
                if (sessionId == clusterSessionId)
                {
                    egressImage = (Image)header.context();
                    onNewLeader(
                        sessionId,
                        newLeaderEventDecoder.leadershipTermId(),
                        newLeaderEventDecoder.leaderMemberId(),
                        newLeaderEventDecoder.ingressEndpoints());

                    return ControlledFragmentHandler.Action.COMMIT;
                }
                break;
            }

            case AdminResponseDecoder.TEMPLATE_ID:
            {
                adminResponseDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long sessionId = adminResponseDecoder.clusterSessionId();
                if (sessionId == clusterSessionId)
                {
                    final long correlationId = adminResponseDecoder.correlationId();
                    final AdminRequestType requestType = adminResponseDecoder.requestType();
                    final AdminResponseCode responseCode = adminResponseDecoder.responseCode();
                    final String message = adminResponseDecoder.message();
                    final int payloadOffset = adminResponseDecoder.offset() +
                        AdminResponseDecoder.BLOCK_LENGTH +
                        AdminResponseDecoder.messageHeaderLength() +
                        message.length() +
                        AdminResponseDecoder.payloadHeaderLength();
                    final int payloadLength = adminResponseDecoder.payloadLength();

                    controlledEgressListener.onAdminResponse(
                        sessionId,
                        correlationId,
                        requestType,
                        responseCode,
                        message,
                        buffer,
                        payloadOffset,
                        payloadLength);
                }
                break;
            }

            default:
                break;
        }

        return ControlledFragmentHandler.Action.CONTINUE;
    }

    private void closeSession()
    {
        idleStrategy.reset();
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + SessionCloseRequestEncoder.BLOCK_LENGTH;
        final SessionCloseRequestEncoder sessionCloseRequestEncoder = new SessionCloseRequestEncoder();
        int attempts = SEND_ATTEMPTS;

        while (true)
        {
            final long position = publication.tryClaim(length, bufferClaim);

            trackIngressPublicationResult(position);

            if (position > 0)
            {
                sessionCloseRequestEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .clusterSessionId(clusterSessionId);

                bufferClaim.commit();
                break;
            }

            if (--attempts <= 0)
            {
                break;
            }

            idleStrategy.idle();
            invokeInvokers();
        }
    }

    private void invokeInvokers()
    {
        if (null != ctx.aeron().conductorAgentInvoker())
        {
            ctx.aeron().conductorAgentInvoker().invoke();
        }

        if (null != ctx.agentInvoker())
        {
            ctx.agentInvoker().invoke();
        }
    }

    private void state(final State newState, final long newStateDeadline)
    {
        //System.out.println(
        //    Instant.now() + " AeronCluster " + state + " -> " + newState + " (" + newStateDeadline + ")");
        state = newState;
        stateDeadline = newStateDeadline;
    }

    private enum State
    {
        /**
         * The session is connected to a leader.
         */
        CONNECTED,
        /**
         * The session got disconnected from a leader, waiting for a new one.
         */
        AWAIT_NEW_LEADER,
        /**
         * The session got notified of a new leader, i.e. egress connected, waiting for ingress to connect.
         */
        AWAIT_NEW_LEADER_CONNECTION,
        /**
         * The session got notified it's closed or the client decided it can't continue.
         * The client is about to close, possibly during the next poll.
         */
        PENDING_CLOSE,
        /**
         * The session and client are closed. Terminal state.
         */
        CLOSED,
    }

    /**
     * Configuration options for cluster client.
     */
    @Config(existsInC = false)
    public static final class Configuration
    {
        /**
         * Major version of the network protocol from client to consensus module. If these don't match then client
         * and consensus module are not compatible.
         */
        public static final int PROTOCOL_MAJOR_VERSION = 0;

        /**
         * Minor version of the network protocol from client to consensus module. If these don't match then some
         * features may not be available.
         */
        public static final int PROTOCOL_MINOR_VERSION = 3;

        /**
         * Patch version of the network protocol from client to consensus module. If these don't match then bug fixes
         * may not have been applied.
         */
        public static final int PROTOCOL_PATCH_VERSION = 0;

        /**
         * Combined semantic version for the client to consensus module protocol.
         *
         * @see SemanticVersion
         */
        public static final int PROTOCOL_SEMANTIC_VERSION = SemanticVersion.compose(
            PROTOCOL_MAJOR_VERSION, PROTOCOL_MINOR_VERSION, PROTOCOL_PATCH_VERSION);

        /**
         * Timeout when waiting on a message to be sent or received.
         */
        @Config
        public static final String MESSAGE_TIMEOUT_PROP_NAME = "aeron.cluster.message.timeout";

        /**
         * Default timeout when waiting on a message to be sent or received.
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 5L * 1000 * 1000 * 1000)
        public static final long MESSAGE_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(5);

        /**
         * Property name for the comma separated map of cluster memberId to ingress endpoint for use with unicast. This
         * is the endpoint values which get substituted into the {@link #INGRESS_CHANNEL_PROP_NAME} when using UDP
         * unicast.
         * <p>
         * {@code "0=endpoint,1=endpoint,2=endpoint"}
         * <p>
         * Each member of the list will be substituted for the endpoint in the {@link #INGRESS_CHANNEL_PROP_NAME} value.
         */
        @Config
        public static final String INGRESS_ENDPOINTS_PROP_NAME = "aeron.cluster.ingress.endpoints";

        /**
         * Default comma separated list of cluster ingress endpoints.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String INGRESS_ENDPOINTS_DEFAULT = null;

        /**
         * Channel for sending messages to a cluster. Ideally this will be a multicast address otherwise unicast will
         * be required and the {@link #INGRESS_ENDPOINTS_PROP_NAME} is used to substitute the endpoints from the
         * {@link #INGRESS_ENDPOINTS_PROP_NAME} list.
         */
        @Config
        public static final String INGRESS_CHANNEL_PROP_NAME = "aeron.cluster.ingress.channel";

        /**
         * Channel for sending messages to a cluster.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String INGRESS_CHANNEL_DEFAULT = null;

        /**
         * Stream id within a channel for sending messages to a cluster.
         */
        @Config
        public static final String INGRESS_STREAM_ID_PROP_NAME = "aeron.cluster.ingress.stream.id";

        /**
         * Default stream id within a channel for sending messages to a cluster.
         */
        @Config
        public static final int INGRESS_STREAM_ID_DEFAULT = 101;

        /**
         * Channel for receiving response messages from a cluster.
         * <p>
         * Channel's <em>endpoint</em> can be specified explicitly (i.e. by providing address and port pair) or
         * by using zero as a port number. Here is an example of valid response channels:
         * <ul>
         *     <li>{@code aeron:udp?endpoint=localhost:9020} - listen on port {@code 9020} on localhost.</li>
         *     <li>{@code aeron:udp?endpoint=192.168.10.10:9020} - listen on port {@code 9020} on
         *     {@code 192.168.10.10}.</li>
         *     <li>{@code aeron:udp?endpoint=localhost:0} - in this case the port is unspecified and the OS
         *     will assign a free port from the
         *     <a href="https://en.wikipedia.org/wiki/Ephemeral_port">ephemeral port range</a>.</li>
         * </ul>
         */
        @Config
        public static final String EGRESS_CHANNEL_PROP_NAME = "aeron.cluster.egress.channel";

        /**
         * Channel for receiving response messages from a cluster.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String EGRESS_CHANNEL_DEFAULT = null;

        /**
         * Stream id within a channel for receiving messages from a cluster.
         */
        @Config
        public static final String EGRESS_STREAM_ID_PROP_NAME = "aeron.cluster.egress.stream.id";

        /**
         * Default stream id within a channel for receiving messages from a cluster.
         */
        @Config
        public static final int EGRESS_STREAM_ID_DEFAULT = 102;

        /**
         * The timeout in nanoseconds to wait for a message.
         *
         * @return timeout in nanoseconds to wait for a message.
         * @see #MESSAGE_TIMEOUT_PROP_NAME
         */
        public static long messageTimeoutNs()
        {
            return getDurationInNanos(MESSAGE_TIMEOUT_PROP_NAME, MESSAGE_TIMEOUT_DEFAULT_NS);
        }

        /**
         * The value {@link #INGRESS_ENDPOINTS_DEFAULT} or system property {@link #INGRESS_ENDPOINTS_PROP_NAME} if set.
         *
         * @return {@link #INGRESS_ENDPOINTS_DEFAULT} or system property {@link #INGRESS_ENDPOINTS_PROP_NAME} if set.
         */
        public static String ingressEndpoints()
        {
            return System.getProperty(INGRESS_ENDPOINTS_PROP_NAME, INGRESS_ENDPOINTS_DEFAULT);
        }

        /**
         * The value {@link #INGRESS_CHANNEL_DEFAULT} or system property {@link #INGRESS_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #INGRESS_CHANNEL_DEFAULT} or system property {@link #INGRESS_CHANNEL_PROP_NAME} if set.
         */
        public static String ingressChannel()
        {
            return System.getProperty(INGRESS_CHANNEL_PROP_NAME, INGRESS_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #INGRESS_STREAM_ID_DEFAULT} or system property {@link #INGRESS_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #INGRESS_STREAM_ID_DEFAULT} or system property {@link #INGRESS_STREAM_ID_PROP_NAME} if set.
         */
        public static int ingressStreamId()
        {
            return Integer.getInteger(INGRESS_STREAM_ID_PROP_NAME, INGRESS_STREAM_ID_DEFAULT);
        }

        /**
         * The value {@link #EGRESS_CHANNEL_DEFAULT} or system property {@link #EGRESS_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #EGRESS_CHANNEL_DEFAULT} or system property {@link #EGRESS_CHANNEL_PROP_NAME} if set.
         */
        public static String egressChannel()
        {
            return System.getProperty(EGRESS_CHANNEL_PROP_NAME, EGRESS_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #EGRESS_STREAM_ID_DEFAULT} or system property {@link #EGRESS_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #EGRESS_STREAM_ID_DEFAULT} or system property {@link #EGRESS_STREAM_ID_PROP_NAME} if set.
         */
        public static int egressStreamId()
        {
            return Integer.getInteger(EGRESS_STREAM_ID_PROP_NAME, EGRESS_STREAM_ID_DEFAULT);
        }
    }

    /**
     * Context for cluster session and connection.
     */
    public static final class Context implements Cloneable
    {
        private static final VarHandle IS_CONCLUDED_VH;
        static
        {
            try
            {
                IS_CONCLUDED_VH = MethodHandles.lookup().findVarHandle(Context.class, "isConcluded", boolean.class);
            }
            catch (final ReflectiveOperationException ex)
            {
                throw new ExceptionInInitializerError(ex);
            }
        }

        private volatile boolean isConcluded;
        private long messageTimeoutNs = Configuration.messageTimeoutNs();
        private long newLeaderTimeoutNs = NULL_VALUE;
        private String ingressEndpoints = Configuration.ingressEndpoints();
        private String ingressChannel = Configuration.ingressChannel();
        private int ingressStreamId = Configuration.ingressStreamId();
        private String egressChannel = Configuration.egressChannel();
        private int egressStreamId = Configuration.egressStreamId();
        private IdleStrategy idleStrategy;
        private String aeronDirectoryName = CommonContext.getAeronDirectoryName();
        private Aeron aeron;
        private CredentialsSupplier credentialsSupplier;
        private boolean ownsAeronClient = false;
        private boolean isIngressExclusive = true;
        private ErrorHandler errorHandler = Aeron.Configuration.DEFAULT_ERROR_HANDLER;
        private boolean isDirectAssemblers = false;
        private EgressListener egressListener;
        private ControlledEgressListener controlledEgressListener;
        private AgentInvoker agentInvoker;

        /**
         * Perform a shallow copy of the object.
         *
         * @return a shallow copy of the object.
         */
        public Context clone()
        {
            try
            {
                return (Context)super.clone();
            }
            catch (final CloneNotSupportedException ex)
            {
                throw new RuntimeException(ex);
            }
        }

        /**
         * Conclude configuration by setting up defaults when specifics are not provided.
         */
        public void conclude()
        {
            if ((boolean)IS_CONCLUDED_VH.getAndSet(this, true))
            {
                throw new ConcurrentConcludeException();
            }

            if (Strings.isEmpty(ingressChannel))
            {
                throw new ConfigurationException("ingressChannel must be specified");
            }

            if (ingressChannel.startsWith(CommonContext.IPC_CHANNEL))
            {
                if (null != ingressEndpoints)
                {
                    throw new ConfigurationException(
                        "AeronCluster.Context ingressEndpoints must be null when using IPC ingress");
                }
            }

            if (Strings.isEmpty(egressChannel))
            {
                throw new ConfigurationException("egressChannel must be specified");
            }

            final ChannelUri egressChannelUri = ChannelUri.parse(egressChannel);
            if (egressChannelUri.isUdp())
            {
                egressChannelUri.put(REJOIN_PARAM_NAME, "false");
                egressChannel = egressChannelUri.toString();
            }

            if (null == aeron)
            {
                aeron = Aeron.connect(
                    new Aeron.Context()
                        .aeronDirectoryName(aeronDirectoryName)
                        .errorHandler(errorHandler)
                        .clientName("cluster-client"));

                ownsAeronClient = true;
            }

            if (null == idleStrategy)
            {
                idleStrategy = new BackoffIdleStrategy(1, 10, 1000, 1000);
            }

            if (null == credentialsSupplier)
            {
                credentialsSupplier = new NullCredentialsSupplier();
            }

            if (null == egressListener)
            {
                egressListener =
                    (clusterSessionId, timestamp, buffer, offset, length, header) ->
                    {
                        throw new ConfigurationException(
                            "egressListener must be specified on AeronCluster.Context");
                    };
            }

            if (null == controlledEgressListener)
            {
                controlledEgressListener =
                    (clusterSessionId, timestamp, buffer, offset, length, header) ->
                    {
                        throw new ConfigurationException(
                            "controlledEgressListener must be specified on AeronCluster.Context");
                    };
            }
        }

        /**
         * Has the context had the {@link #conclude()} method called.
         *
         * @return true of the {@link #conclude()} method has been called.
         */
        public boolean isConcluded()
        {
            return isConcluded;
        }

        /**
         * Set the message timeout in nanoseconds to wait for sending or receiving a message.
         *
         * @param messageTimeoutNs to wait for sending or receiving a message.
         * @return this for a fluent API.
         * @see Configuration#MESSAGE_TIMEOUT_PROP_NAME
         */
        public Context messageTimeoutNs(final long messageTimeoutNs)
        {
            this.messageTimeoutNs = messageTimeoutNs;
            return this;
        }

        /**
         * The message timeout in nanoseconds to wait for sending or receiving a message.
         *
         * @return the message timeout in nanoseconds to wait for sending or receiving a message.
         * @see Configuration#MESSAGE_TIMEOUT_PROP_NAME
         */
        @Config
        public long messageTimeoutNs()
        {
            return CommonContext.checkDebugTimeout(messageTimeoutNs, TimeUnit.NANOSECONDS);
        }

        /**
         * The timeout to wait for a new leader after noticing a disconnection from the previous one. Upon timeout the
         * cluster will be considered lost and the client will close. If set to {@link Aeron#NULL_VALUE}, a reasonable
         * default will be used.
         *
         * @param newLeaderTimeoutNs the new leader timeout in nanoseconds or {@link Aeron#NULL_VALUE}.
         * @return this for a fluent API.
         */
        public Context newLeaderTimeoutNs(final long newLeaderTimeoutNs)
        {
            if (!(0 < newLeaderTimeoutNs || NULL_VALUE == newLeaderTimeoutNs))
            {
                throw new IllegalArgumentException(
                    "newLeaderTimeoutNs must be positive or -1, but was " + newLeaderTimeoutNs);
            }
            this.newLeaderTimeoutNs = newLeaderTimeoutNs;
            return this;
        }

        /**
         * The timeout to wait for a new leader after noticing a disconnection from the previous one. Upon timeout the
         * cluster will be considered lost and the client will close.
         *
         * @return the new leader timeout in nanoseconds.
         */
        public long newLeaderTimeoutNs()
        {
            return CommonContext.checkDebugTimeout(newLeaderTimeoutNs, TimeUnit.NANOSECONDS);
        }

        /**
         * The endpoints representing members for use with unicast to be substituted into the {@link #ingressChannel()}
         * for endpoints. A null value can be used when multicast where the {@link #ingressChannel()} contains the
         * multicast endpoint.
         *
         * @param clusterMembers which are all candidates to be leader.
         * @return this for a fluent API.
         * @see Configuration#INGRESS_ENDPOINTS_PROP_NAME
         */
        public Context ingressEndpoints(final String clusterMembers)
        {
            this.ingressEndpoints = clusterMembers;
            return this;
        }

        /**
         * The endpoints representing members for use with unicast to be substituted into the {@link #ingressChannel()}
         * for endpoints. A null value can be used when multicast where the {@link #ingressChannel()} contains the
         * multicast endpoint.
         *
         * @return member endpoints of the cluster which are all candidates to be leader.
         * @see Configuration#INGRESS_ENDPOINTS_PROP_NAME
         */
        @Config
        public String ingressEndpoints()
        {
            return ingressEndpoints;
        }

        /**
         * Set the channel parameter for the ingress channel.
         * <p>
         * The endpoints representing members for use with unicast are substituted from {@link #ingressEndpoints()}
         * for endpoints. If this channel contains a multicast endpoint, then {@link #ingressEndpoints()} should
         * be set to null.
         *
         * @param channel parameter for the ingress channel.
         * @return this for a fluent API.
         * @see Configuration#INGRESS_CHANNEL_PROP_NAME
         */
        public Context ingressChannel(final String channel)
        {
            ingressChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for the ingress channel.
         * <p>
         * The endpoints representing members for use with unicast are substituted from {@link #ingressEndpoints()}
         * for endpoints. A null value can be used when multicast where this contains the multicast endpoint.
         *
         * @return the channel parameter for the ingress channel.
         * @see Configuration#INGRESS_CHANNEL_PROP_NAME
         */
        @Config
        public String ingressChannel()
        {
            return ingressChannel;
        }

        /**
         * Set the stream id for the ingress channel.
         *
         * @param streamId for the ingress channel.
         * @return this for a fluent API
         * @see Configuration#INGRESS_STREAM_ID_PROP_NAME
         */
        public Context ingressStreamId(final int streamId)
        {
            ingressStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for the ingress channel.
         *
         * @return the stream id for the ingress channel.
         * @see Configuration#INGRESS_STREAM_ID_PROP_NAME
         */
        @Config
        public int ingressStreamId()
        {
            return ingressStreamId;
        }

        /**
         * Set the channel parameter for the egress channel.
         *
         * @param channel parameter for the egress channel.
         * @return this for a fluent API.
         * @see Configuration#EGRESS_CHANNEL_PROP_NAME
         */
        public Context egressChannel(final String channel)
        {
            egressChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for the egress channel.
         *
         * @return the channel parameter for the egress channel.
         * @see Configuration#EGRESS_CHANNEL_PROP_NAME
         */
        @Config
        public String egressChannel()
        {
            return egressChannel;
        }

        /**
         * Set the stream id for the egress channel.
         *
         * @param streamId for the egress channel.
         * @return this for a fluent API
         * @see Configuration#EGRESS_STREAM_ID_PROP_NAME
         */
        public Context egressStreamId(final int streamId)
        {
            egressStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for the egress channel.
         *
         * @return the stream id for the egress channel.
         * @see Configuration#EGRESS_STREAM_ID_PROP_NAME
         */
        @Config
        public int egressStreamId()
        {
            return egressStreamId;
        }

        /**
         * Set the {@link IdleStrategy} used when waiting for responses.
         *
         * @param idleStrategy used when waiting for responses.
         * @return this for a fluent API.
         */
        public Context idleStrategy(final IdleStrategy idleStrategy)
        {
            this.idleStrategy = idleStrategy;
            return this;
        }

        /**
         * Get the {@link IdleStrategy} used when waiting for responses.
         *
         * @return the {@link IdleStrategy} used when waiting for responses.
         */
        public IdleStrategy idleStrategy()
        {
            return idleStrategy;
        }

        /**
         * Set the top level Aeron directory used for communication between the Aeron client and Media Driver.
         *
         * @param aeronDirectoryName the top level Aeron directory.
         * @return this for a fluent API.
         */
        public Context aeronDirectoryName(final String aeronDirectoryName)
        {
            this.aeronDirectoryName = aeronDirectoryName;
            return this;
        }

        /**
         * Get the top level Aeron directory used for communication between the Aeron client and Media Driver.
         *
         * @return The top level Aeron directory.
         */
        public String aeronDirectoryName()
        {
            return aeronDirectoryName;
        }

        /**
         * {@link Aeron} client for communicating with the local Media Driver.
         * <p>
         * This client will be closed when the {@link AeronCluster#close()} or {@link #close()} methods are called if
         * {@link #ownsAeronClient()} is true.
         *
         * @param aeron client for communicating with the local Media Driver.
         * @return this for a fluent API.
         * @see Aeron#connect()
         */
        public Context aeron(final Aeron aeron)
        {
            this.aeron = aeron;
            return this;
        }

        /**
         * {@link Aeron} client for communicating with the local Media Driver.
         * <p>
         * If not provided then a default will be established during {@link #conclude()} by calling
         * {@link Aeron#connect()}.
         *
         * @return client for communicating with the local Media Driver.
         */
        public Aeron aeron()
        {
            return aeron;
        }

        /**
         * Does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         *
         * @param ownsAeronClient does this context own the {@link #aeron()} client.
         * @return this for a fluent API.
         */
        public Context ownsAeronClient(final boolean ownsAeronClient)
        {
            this.ownsAeronClient = ownsAeronClient;
            return this;
        }

        /**
         * Does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         *
         * @return does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         */
        public boolean ownsAeronClient()
        {
            return ownsAeronClient;
        }

        /**
         * Is ingress to the cluster exclusively from a single thread to this client? The client should not be used
         * from another thread, e.g. a separate thread calling {@link AeronCluster#sendKeepAlive()} - which is awful
         * design by the way!
         *
         * @param isIngressExclusive true if ingress to the cluster is exclusively from a single thread for this client?
         * @return this for a fluent API.
         */
        public Context isIngressExclusive(final boolean isIngressExclusive)
        {
            this.isIngressExclusive = isIngressExclusive;
            return this;
        }

        /**
         * Is ingress the {@link Publication} to the cluster used exclusively from a single thread to this client?
         *
         * @return true if the ingress {@link Publication} is to be used exclusively from a single thread?
         */
        public boolean isIngressExclusive()
        {
            return isIngressExclusive;
        }

        /**
         * Set the {@link CredentialsSupplier} to be used for authentication with the cluster.
         *
         * @param credentialsSupplier to be used for authentication with the cluster.
         * @return this for fluent API.
         */
        public Context credentialsSupplier(final CredentialsSupplier credentialsSupplier)
        {
            this.credentialsSupplier = credentialsSupplier;
            return this;
        }

        /**
         * Get the {@link CredentialsSupplier} to be used for authentication with the cluster.
         *
         * @return the {@link CredentialsSupplier} to be used for authentication with the cluster.
         */
        public CredentialsSupplier credentialsSupplier()
        {
            return credentialsSupplier;
        }

        /**
         * Set the {@link ErrorHandler} to be used for handling any exceptions.
         *
         * @param errorHandler Method to handle objects of type Throwable.
         * @return this for fluent API.
         */
        public Context errorHandler(final ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        /**
         * Get the {@link ErrorHandler} to be used for handling any exceptions.
         *
         * @return The {@link ErrorHandler} to be used for handling any exceptions.
         */
        public ErrorHandler errorHandler()
        {
            return errorHandler;
        }

        /**
         * Are direct buffers used for fragment assembly on egress?
         *
         * @param isDirectAssemblers true if direct buffers used for fragment assembly on egress.
         * @return this for a fluent API.
         */
        public Context isDirectAssemblers(final boolean isDirectAssemblers)
        {
            this.isDirectAssemblers = isDirectAssemblers;
            return this;
        }

        /**
         * Are direct buffers used for fragment assembly on egress?
         *
         * @return true if direct buffers used for fragment assembly on egress.
         */
        public boolean isDirectAssemblers()
        {
            return isDirectAssemblers;
        }

        /**
         * Set the {@link EgressListener} function that will be called when polling for egress via
         * {@link AeronCluster#pollEgress()}.
         * <p>
         * Only {@link EgressListener#onMessage(long, long, DirectBuffer, int, int, Header)} will be dispatched
         * when using {@link AeronCluster#pollEgress()}.
         *
         * @param listener function that will be called when polling for egress via {@link AeronCluster#pollEgress()}.
         * @return this for a fluent API.
         */
        public Context egressListener(final EgressListener listener)
        {
            this.egressListener = listener;
            return this;
        }

        /**
         * Get the {@link EgressListener} function that will be called when polling for egress via
         * {@link AeronCluster#pollEgress()}.
         *
         * @return the {@link EgressListener} function that will be called when polling for egress via
         * {@link AeronCluster#pollEgress()}.
         */
        public EgressListener egressListener()
        {
            return egressListener;
        }

        /**
         * Set the {@link ControlledEgressListener} function that will be called when polling for egress via
         * {@link AeronCluster#controlledPollEgress()}.
         * <p>
         * Only {@link ControlledEgressListener#onMessage(long, long, DirectBuffer, int, int, Header)} will be
         * dispatched when using {@link AeronCluster#controlledPollEgress()}.
         *
         * @param listener function that will be called when polling for egress via
         *                 {@link AeronCluster#controlledPollEgress()}.
         * @return this for a fluent API.
         */
        public Context controlledEgressListener(final ControlledEgressListener listener)
        {
            this.controlledEgressListener = listener;
            return this;
        }

        /**
         * Get the {@link ControlledEgressListener} function that will be called when polling for egress via
         * {@link AeronCluster#controlledPollEgress()}.
         *
         * @return the {@link ControlledEgressListener} function that will be called when polling for egress via
         * {@link AeronCluster#controlledPollEgress()}.
         */
        public ControlledEgressListener controlledEgressListener()
        {
            return controlledEgressListener;
        }

        /**
         * Set the {@link AgentInvoker} to be invoked in addition to any invoker used by the {@link #aeron()} instance.
         * <p>
         * Useful for when running on a low thread count scenario.
         *
         * @param agentInvoker to be invoked while awaiting a response in the client or when awaiting completion.
         * @return this for a fluent API.
         */
        public Context agentInvoker(final AgentInvoker agentInvoker)
        {
            this.agentInvoker = agentInvoker;
            return this;
        }

        /**
         * Get the {@link AgentInvoker} to be invoked in addition to any invoker used by the {@link #aeron()} instance.
         *
         * @return the {@link AgentInvoker} that is used.
         */
        public AgentInvoker agentInvoker()
        {
            return agentInvoker;
        }

        /**
         * Close the context and free applicable resources.
         * <p>
         * If {@link #ownsAeronClient()} is true then the {@link #aeron()} client will be closed.
         */
        public void close()
        {
            if (ownsAeronClient)
            {
                CloseHelper.close(aeron);
            }
        }

        /**
         * {@inheritDoc}
         */
        public String toString()
        {
            return "AeronCluster.Context" +
                "\n{" +
                "\n    isConcluded=" + isConcluded() +
                "\n    ownsAeronClient=" + ownsAeronClient +
                "\n    aeronDirectoryName='" + aeronDirectoryName + '\'' +
                "\n    aeron=" + aeron +
                "\n    messageTimeoutNs=" + messageTimeoutNs +
                "\n    newLeaderTimeoutNs=" + newLeaderTimeoutNs +
                "\n    ingressEndpoints='" + ingressEndpoints + '\'' +
                "\n    ingressChannel='" + ingressChannel + '\'' +
                "\n    ingressStreamId=" + ingressStreamId +
                "\n    egressChannel='" + egressChannel + '\'' +
                "\n    egressStreamId=" + egressStreamId +
                "\n    idleStrategy=" + idleStrategy +
                "\n    credentialsSupplier=" + credentialsSupplier +
                "\n    isIngressExclusive=" + isIngressExclusive +
                "\n    errorHandler=" + errorHandler +
                "\n    isDirectAssemblers=" + isDirectAssemblers +
                "\n    egressListener=" + egressListener +
                "\n    controlledEgressListener=" + controlledEgressListener +
                "\n}";
        }
    }

    /**
     * Allows for the async establishment of a cluster session. {@link #poll()} should be called repeatedly until
     * it returns a non-null value with the new {@link AeronCluster} client. On error {@link #close()} should be called
     * to clean up allocated resources.
     */
    public static final class AsyncConnect implements AutoCloseable
    {
        /**
         * Represents connection state.
         */
        public enum State
        {
            /**
             * Create egress subscription.
             */
            CREATE_EGRESS_SUBSCRIPTION(-1),
            /**
             * Create ingress publication.
             */
            CREATE_INGRESS_PUBLICATIONS(0),
            /**
             * Await ingress publication connected.
             */
            AWAIT_PUBLICATION_CONNECTED(1),
            /**
             * Send message to Cluster.
             */
            SEND_MESSAGE(2),
            /**
             * Poll for Cluster response.
             */
            POLL_RESPONSE(3),
            /**
             * Initialize internal state.
             */
            CONCLUDE_CONNECT(4),
            /**
             * Connection established.
             */
            DONE(5);

            private static final State[] STATES = values();

            final int step;

            State(final int step)
            {
                this.step = step;
            }

            static State fromStep(final int step)
            {
                if (step < CREATE_EGRESS_SUBSCRIPTION.step || step > DONE.step)
                {
                    return null;
                }
                return STATES[step + 1];
            }
        }

        private Image egressImage;
        private final long deadlineNs;
        private long leaderHeartbeatTimeoutNs;
        private long correlationId = NULL_VALUE;
        private long clusterSessionId;
        private long leadershipTermId;
        private int leaderMemberId;
        private State state = State.CREATE_EGRESS_SUBSCRIPTION;
        private int messageLength = 0;

        private final Context ctx;
        private final NanoClock nanoClock;
        private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();

        private Subscription egressSubscription;
        private EgressPoller egressPoller;
        private long egressRegistrationId = NULL_VALUE;
        private Int2ObjectHashMap<MemberIngress> memberByIdMap;
        private long ingressRegistrationId = NULL_VALUE;
        private Publication ingressPublication;

        AsyncConnect(final Context ctx, final long deadlineNs)
        {
            this.ctx = ctx;

            memberByIdMap = parseIngressEndpoints(ctx, ctx.ingressEndpoints());
            nanoClock = ctx.aeron().context().nanoClock();
            this.deadlineNs = deadlineNs;
        }

        /**
         * Close allocated resources. Must be called on error. On success this is a no op.
         */
        public void close()
        {
            if (State.DONE != state)
            {
                final ErrorHandler errorHandler = ctx.errorHandler();
                if (null != ingressPublication)
                {
                    CloseHelper.close(errorHandler, ingressPublication);
                }
                else if (NULL_VALUE != ingressRegistrationId)
                {
                    ctx.aeron().asyncRemovePublication(ingressRegistrationId);
                }

                if (null != egressSubscription)
                {
                    CloseHelper.close(errorHandler, egressSubscription);
                }
                else if (NULL_VALUE != egressRegistrationId)
                {
                    ctx.aeron().asyncRemoveSubscription(egressRegistrationId);
                }

                CloseHelper.closeAll(errorHandler, memberByIdMap.values());
                ctx.close();
            }
        }

        /**
         * Indicates which step in the connect process has been reached.
         *
         * @return which step in the connect process has reached.
         */
        public int step()
        {
            return state.step;
        }


        /**
         * Get the current connection state.
         *
         * @return current state.
         */
        public State state()
        {
            return state;
        }

        private void state(final State newState)
        {
//            System.out.println("AeronCluster.AsyncConnect " + state + " -> " + stepName(newState));
            state = newState;
        }

        /**
         * Get the String representation of a step in the connect process.
         *
         * @param step to get the string representation for.
         * @return the String representation of a step in the connect process.
         * @see #step()
         */
        public static String stepName(final int step)
        {
            final State state = State.fromStep(step);
            return null != state ? state.name() : "<unknown>";
        }

        /**
         * Poll to advance steps in the connection until complete or error.
         *
         * @return null if not yet complete then {@link AeronCluster} when complete.
         */
        public AeronCluster poll()
        {
            checkDeadline();

            switch (state)
            {
                case CREATE_EGRESS_SUBSCRIPTION:
                    createEgressSubscription();
                    break;

                case CREATE_INGRESS_PUBLICATIONS:
                    createIngressPublications();
                    break;

                case AWAIT_PUBLICATION_CONNECTED:
                    awaitPublicationConnected();
                    break;

                case SEND_MESSAGE:
                    sendMessage();
                    break;

                case POLL_RESPONSE:
                    pollResponse();
                    break;

                case CONCLUDE_CONNECT:
                    return concludeConnect();

                default:
                    break;
            }

            return null;
        }

        private void checkDeadline()
        {
            if (deadlineNs - nanoClock.nanoTime() < 0)
            {
                final boolean isConnected = null != egressSubscription && egressSubscription.isConnected();
                final String endpointPort = null != egressSubscription ?
                    egressSubscription.tryResolveChannelEndpointPort() : "<unknown>";

                final StringBuffer errorMessage = new StringBuffer("cluster connect timeout: ");
                try
                {
                    if (state == State.AWAIT_PUBLICATION_CONNECTED)
                    {
                        if (null == ingressPublication)
                        {
                            errorMessage.append("couldn't connect to any of the cluster endpoints! ");
                        }
                        else
                        {
                            final String ingressChannel = ingressPublication.channel();
                            final String[] channelParts = ingressChannel.split("\\?");
                            final String[] ingressEndpointParts = channelParts[1].split("=");

                            errorMessage.append("couldn't connect to ingress at " + ingressEndpointParts[1]);
                        }
                    }
                    else if (state == State.POLL_RESPONSE)
                    {
                        final ChannelUri egressChannelUri = ChannelUri.parse(ctx.egressChannel);
                        if (egressChannelUri.isUdp())
                        {
                            final String endpoint = egressChannelUri.get(CommonContext.ENDPOINT_PARAM_NAME);
                            final String egressHost = endpoint.split(":")[0];
                            final String ingressChannel = ingressPublication.channel();
                            if (null != ingressChannel)
                            {
                                final String[] channelParts = ingressChannel.split("\\?");
                                final String[] ingressEndpointParts = channelParts[1].split("=");
                                final String ingressEndpoint = ingressEndpointParts[1].split(":")[0];

                                errorMessage.append(" - Connected to cluster at " + ingressEndpoint +
                                    ", but the cluster node cannot connect to you at " + egressHost);
                                if (endpoint.startsWith("localhost") || endpoint.startsWith("127."))
                                {
                                    errorMessage.append(".  Cannot bind to localhost if cluster nodes are remote.");
                                }
                            }
                        }
                        else
                        {
                            errorMessage.append(".  ");  // log something special for ipc?
                        }
                    }
                    errorMessage.append(
                        " state=" + state.name() +
                        " messageTimeout=" + ctx.messageTimeoutNs() + "ns" +
                        " ingressChannel=" + ctx.ingressChannel() +
                        " ingressEndpoints=" + ctx.ingressEndpoints() +
                        " ingressPublication=" + ingressPublication +
                        " egress.isConnected=" + isConnected +
                        " responseChannel=" + endpointPort);

                    final TimeoutException ex = new TimeoutException(
                        errorMessage.toString());

                    for (final MemberIngress member : memberByIdMap.values())
                    {
                        if (null != member.publicationException)
                        {
                            ex.addSuppressed(member.publicationException);
                        }
                    }
                    throw ex;
                }
                catch (final Exception ex)
                {
                    System.out.println("exception logging exception: " + ex.getMessage());
                    ex.printStackTrace();
                }
            }

            if (Thread.currentThread().isInterrupted())
            {
                throw new AeronException("unexpected interrupt");
            }
        }

        private void createEgressSubscription()
        {
            if (NULL_VALUE == egressRegistrationId)
            {
                egressRegistrationId = ctx.aeron().asyncAddSubscription(ctx.egressChannel(), ctx.egressStreamId());
            }

            egressSubscription = ctx.aeron().getSubscription(egressRegistrationId);
            if (null != egressSubscription)
            {
                egressPoller = new EgressPoller(egressSubscription, FRAGMENT_LIMIT);
                egressRegistrationId = NULL_VALUE;
                state(State.CREATE_INGRESS_PUBLICATIONS);
            }
        }

        private void createIngressPublications()
        {
            if (null == ctx.ingressEndpoints())
            {
                if (NULL_VALUE == ingressRegistrationId)
                {
                    ingressRegistrationId = asyncAddIngressPublication(
                        ctx, ctx.ingressChannel(), ctx.ingressStreamId());
                }

                if (null == ingressPublication)
                {
                    ingressPublication = getIngressPublication(ctx, ingressRegistrationId);
                }

                if (null != ingressPublication)
                {
                    ingressRegistrationId = NULL_VALUE;
                    state(State.AWAIT_PUBLICATION_CONNECTED);
                }
            }
            else
            {
                int publicationCount = 0;
                int failureCount = 0;
                final ChannelUri channelUri = ChannelUri.parse(ctx.ingressChannel());

                for (final MemberIngress member : memberByIdMap.values())
                {
                    try
                    {
                        if (null != member.publicationException)
                        {
                            failureCount++;
                            continue;
                        }

                        if (null == member.publication)
                        {
                            if (NULL_VALUE == member.registrationId)
                            {
                                if (channelUri.isUdp())
                                {
                                    channelUri.put(CommonContext.ENDPOINT_PARAM_NAME, member.endpoint);
                                }
                                member.registrationId = asyncAddIngressPublication(
                                    ctx, channelUri.toString(), ctx.ingressStreamId());
                            }
                            member.publication = getIngressPublication(ctx, member.registrationId);
                        }

                        if (null != member.publication)
                        {
                            member.registrationId = NULL_VALUE;
                            publicationCount++;
                        }
                    }
                    catch (final RegistrationException ex)
                    {
                        member.publicationException = ex;
                    }
                }

                if (publicationCount + failureCount == memberByIdMap.size())
                {
                    if (0 == publicationCount)
                    {
                        throw memberByIdMap.values().iterator().next().publicationException;
                    }

                    state(State.AWAIT_PUBLICATION_CONNECTED);
                }
            }
        }

        private void awaitPublicationConnected()
        {
            final String responseChannel = egressSubscription.tryResolveChannelEndpointPort();
            if (null != responseChannel)
            {
                if (null == ingressPublication)
                {
                    for (final MemberIngress member : memberByIdMap.values())
                    {
                        if (null != member.publication && member.publication.isConnected())
                        {
                            ingressPublication = member.publication;
                            prepareConnectRequest(responseChannel);
                            break;
                        }
                    }
                }
                else if (ingressPublication.isConnected())
                {
                    prepareConnectRequest(responseChannel);
                }
            }
        }

        private void prepareConnectRequest(final String responseChannel)
        {
            correlationId = ctx.aeron().nextCorrelationId();
            final byte[] encodedCredentials = ctx.credentialsSupplier().encodedCredentials();

            final SessionConnectRequestEncoder encoder = new SessionConnectRequestEncoder()
                .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .correlationId(correlationId)
                .responseStreamId(ctx.egressStreamId())
                .version(Configuration.PROTOCOL_SEMANTIC_VERSION)
                .responseChannel(responseChannel)
                .putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);

            messageLength = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
            state(State.SEND_MESSAGE);
        }

        private void sendMessage()
        {
            final long position = ingressPublication.offer(buffer, 0, messageLength);
            if (position > 0)
            {
                state(State.POLL_RESPONSE);
            }
            else if (Publication.CLOSED == position || Publication.NOT_CONNECTED == position)
            {
                throw new ClusterException("unexpected loss of connection to cluster");
            }
        }

        private void pollResponse()
        {
            if (egressPoller.poll() > 0 &&
                egressPoller.isPollComplete() &&
                egressPoller.correlationId() == correlationId)
            {
                if (egressPoller.isChallenged())
                {
                    correlationId = NULL_VALUE;
                    clusterSessionId = egressPoller.clusterSessionId();
                    prepareChallengeResponse(ctx.credentialsSupplier().onChallenge(egressPoller.encodedChallenge()));
                    return;
                }

                switch (egressPoller.eventCode())
                {
                    case OK:
                        leadershipTermId = egressPoller.leadershipTermId();
                        leaderMemberId = egressPoller.leaderMemberId();
                        clusterSessionId = egressPoller.clusterSessionId();
                        leaderHeartbeatTimeoutNs = egressPoller.leaderHeartbeatTimeoutNs();
                        egressImage = egressPoller.egressImage();
                        state(State.CONCLUDE_CONNECT);
                        break;

                    case ERROR:
                        throw new ClusterException(egressPoller.detail());

                    case REDIRECT:
                        updateMembers();
                        break;

                    case AUTHENTICATION_REJECTED:
                        throw new AuthenticationException(egressPoller.detail());

                    case CLOSED:
                    case NULL_VAL:
                        break;
                }
            }
        }

        private void prepareChallengeResponse(final byte[] encodedCredentials)
        {
            correlationId = ctx.aeron().nextCorrelationId();

            final ChallengeResponseEncoder encoder = new ChallengeResponseEncoder()
                .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .correlationId(correlationId)
                .clusterSessionId(clusterSessionId)
                .putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);

            messageLength = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();

            state(State.SEND_MESSAGE);
        }

        private void updateMembers()
        {
            leaderMemberId = egressPoller.leaderMemberId();
            final MemberIngress leader = memberByIdMap.get(leaderMemberId);
            if (null != leader)
            {
                ingressPublication = leader.publication;
                leader.publication = null;
            }

            CloseHelper.closeAll(memberByIdMap.values());
            memberByIdMap = parseIngressEndpoints(ctx, egressPoller.detail());

            if (null == ingressPublication)
            {
                final MemberIngress member = memberByIdMap.get(leaderMemberId);
                final ChannelUri channelUri = ChannelUri.parse(ctx.ingressChannel());

                if (channelUri.isUdp())
                {
                    channelUri.put(CommonContext.ENDPOINT_PARAM_NAME, member.endpoint);
                }

                ingressPublication = addIngressPublication(ctx, channelUri.toString(), ctx.ingressStreamId());
            }

            state(State.AWAIT_PUBLICATION_CONNECTED);
        }

        private AeronCluster concludeConnect()
        {
            if (ctx.newLeaderTimeoutNs() == NULL_VALUE)
            {
                ctx.newLeaderTimeoutNs(2 * (leaderHeartbeatTimeoutNs != NULL_VALUE ?
                    leaderHeartbeatTimeoutNs : ConsensusModule.Configuration.LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS));
            }

            final AeronCluster aeronCluster = new AeronCluster(
                ctx,
                messageHeaderEncoder,
                ingressPublication,
                egressSubscription,
                egressImage,
                memberByIdMap,
                clusterSessionId,
                leadershipTermId,
                leaderMemberId);

            ingressPublication = null;
            memberByIdMap.remove(leaderMemberId);
            CloseHelper.closeAll(memberByIdMap.values());

            state(State.DONE);

            return aeronCluster;
        }
    }

    static final class MemberIngress implements AutoCloseable
    {
        private final Context ctx;
        final int memberId;
        final String endpoint;
        long registrationId = NULL_VALUE;
        Publication publication;
        RegistrationException publicationException;

        MemberIngress(final Context ctx, final int memberId, final String endpoint)
        {
            this.ctx = ctx;
            this.memberId = memberId;
            this.endpoint = endpoint;
        }

        public void close()
        {
            if (null != publication)
            {
                CloseHelper.close(publication);
            }
            else if (NULL_VALUE != registrationId)
            {
                ctx.aeron().asyncRemovePublication(registrationId);
            }
            registrationId = NULL_VALUE;
            publication = null;
        }

        public String toString()
        {
            return "MemberIngress{" +
                "memberId=" + memberId +
                ", endpoint='" + endpoint + '\'' +
                ", publication=" + publication +
                '}';
        }
    }
}
