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
package io.aeron.cluster.service;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.api.InternalApi;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SnapshotMark;
import io.aeron.cluster.codecs.SnapshotMarkerEncoder;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.IdleStrategy;

import java.util.concurrent.TimeUnit;

/**
 * Base class of common functions required to take a snapshot of cluster state.
 */
@InternalApi
public class SnapshotTaker
{
    /**
     * Reusable {@link BufferClaim} to avoid allocation.
     */
    protected final BufferClaim bufferClaim = new BufferClaim();

    /**
     * Reusable {@link MessageHeaderEncoder} to avoid allocation.
     */
    protected final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();

    /**
     * {@link Publication} to which the snapshot will be written.
     */
    protected final ExclusivePublication publication;

    /**
     * {@link IdleStrategy} to be called when back pressure is propagated from the {@link #publication}.
     */
    protected final IdleStrategy idleStrategy;

    private static final int ENCODED_MARKER_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + SnapshotMarkerEncoder.BLOCK_LENGTH;
    private final AgentInvoker aeronAgentInvoker;
    private final SnapshotMarkerEncoder snapshotMarkerEncoder = new SnapshotMarkerEncoder();

    /**
     * Construct a {@link SnapshotTaker} which will encode the snapshot to a publication.
     *
     * @param publication       into which the snapshot will be encoded.
     * @param idleStrategy      to call when the publication is back pressured.
     * @param aeronAgentInvoker to call when idling so it stays active.
     */
    public SnapshotTaker(
        final ExclusivePublication publication, final IdleStrategy idleStrategy, final AgentInvoker aeronAgentInvoker)
    {
        this.publication = publication;
        this.idleStrategy = idleStrategy;
        this.aeronAgentInvoker = aeronAgentInvoker;
    }

    /**
     * Mark the beginning of the encoded snapshot.
     *
     * @param snapshotTypeId      type to identify snapshot within a cluster.
     * @param leadershipTermId    at which the snapshot was taken.
     * @param termBaseLogPosition at which {@code leadershipTermId} started.
     * @param logPosition         at which the snapshot was taken.
     * @param snapshotIndex       so the snapshot can be sectioned.
     * @param timeUnit            of the cluster timestamps stored in the snapshot.
     * @param appVersion          associated with the snapshot from {@link ClusteredServiceContainer.Context#appVersion()}.
     */
    public void markBegin(
        final long snapshotTypeId,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final int snapshotIndex,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        markSnapshot(
            snapshotTypeId,
            leadershipTermId,
            termBaseLogPosition,
            logPosition,
            snapshotIndex,
            SnapshotMark.BEGIN,
            timeUnit,
            appVersion);
    }

    /**
     * Mark the end of the encoded snapshot.
     *
     * @param snapshotTypeId      type to identify snapshot within a cluster.
     * @param leadershipTermId    at which the snapshot was taken.
     * @param termBaseLogPosition at which {@code leadershipTermId} started.
     * @param logPosition         at which the snapshot was taken.
     * @param snapshotIndex       so the snapshot can be sectioned.
     * @param timeUnit            of the cluster timestamps stored in the snapshot.
     * @param appVersion          associated with the snapshot from {@link ClusteredServiceContainer.Context#appVersion()}.
     */
    public void markEnd(
        final long snapshotTypeId,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final int snapshotIndex,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        markSnapshot(
            snapshotTypeId,
            leadershipTermId,
            termBaseLogPosition,
            logPosition,
            snapshotIndex,
            SnapshotMark.END,
            timeUnit,
            appVersion);
    }

    /**
     * Generically {@link SnapshotMark} a snapshot.
     *
     * @param snapshotTypeId      type to identify snapshot within a cluster.
     * @param leadershipTermId    at which the snapshot was taken.
     * @param termBaseLogPosition at which {@code leadershipTermId} started.
     * @param logPosition         at which the snapshot was taken.
     * @param snapshotIndex       so the snapshot can be sectioned.
     * @param snapshotMark        which specifies the type of snapshot mark.
     * @param timeUnit            of the cluster timestamps stored in the snapshot.
     * @param appVersion          associated with the snapshot from {@link ClusteredServiceContainer.Context#appVersion()}.
     */
    public void markSnapshot(
        final long snapshotTypeId,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final int snapshotIndex,
        final SnapshotMark snapshotMark,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        idleStrategy.reset();
        while (true)
        {
            final long result = publication.tryClaim(ENCODED_MARKER_LENGTH, bufferClaim);
            if (result > 0)
            {
                snapshotMarkerEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .typeId(snapshotTypeId)
                    .logPosition(logPosition)
                    .leadershipTermId(leadershipTermId)
                    .index(snapshotIndex)
                    .mark(snapshotMark)
                    .timeUnit(ClusterClock.map(timeUnit))
                    .appVersion(appVersion)
                    .termBaseLogPosition(termBaseLogPosition);

                bufferClaim.commit();
                break;
            }

            checkResultAndIdle(result);
        }
    }

    /**
     * Check for thread interrupt and throw an {@link AgentTerminationException} if interrupted.
     */
    protected static void checkInterruptStatus()
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new AgentTerminationException("interrupted");
        }
    }

    /**
     * Check the result of offering to a publication when writing a snapshot.
     *
     * @param position    of an offer or try claim to a publication.
     * @param publication on which the offer or try claim was attempted.
     */
    protected static void checkResult(final long position, final Publication publication)
    {
        if (Publication.NOT_CONNECTED == position)
        {
            throw new ClusterException("publication is not connected");
        }

        if (Publication.CLOSED == position)
        {
            throw new ClusterException("publication is closed");
        }

        if (Publication.MAX_POSITION_EXCEEDED == position)
        {
            throw new ClusterException("publication at max position: term-length=" + publication.termBufferLength());
        }
    }

    /**
     * Check the result of offering to a publication when writing a snapshot and then idle after invoking the client
     * agent if necessary.
     *
     * @param position of an offer or try claim to a publication.
     */
    protected void checkResultAndIdle(final long position)
    {
        checkResult(position, publication);
        checkInterruptStatus();
        invokeAgentClient();
        idleStrategy.idle();
    }

    /**
     * Invoke the Aeron client agent if necessary.
     */
    protected void invokeAgentClient()
    {
        if (null != aeronAgentInvoker)
        {
            aeronAgentInvoker.invoke();
        }
    }

    /**
     * Helper method to offer a message into the snapshot publication.
     *
     * @param buffer containing the message.
     * @param offset at which the message begins.
     * @param length of the message.
     */
    protected final void offer(final DirectBuffer buffer, final int offset, final int length)
    {
        idleStrategy.reset();
        while (true)
        {
            final long result = publication.offer(buffer, offset, length);
            if (result > 0)
            {
                break;
            }

            checkResultAndIdle(result);
        }
    }
}
