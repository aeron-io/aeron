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
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

import java.util.concurrent.TimeUnit;

/**
 * Interface which a service must implement to be contained in the cluster.
 * <p>
 * The {@code cluster} object should only be used to send messages to the cluster or schedule timers in
 * response to other messages and timers. Sending messages and timers should not happen from cluster lifecycle
 * methods like {@link #onStart(Cluster, Image)}, {@link #onRoleChange(Cluster.Role)} or
 * {@link #onTakeSnapshot(ExclusivePublication)}, or {@link #onTerminate(Cluster)}, except the session lifecycle
 * methods.
 */
public interface ClusteredService
{
    /**
     * Start event where the service can perform any initialisation required and load snapshot state.
     * The snapshot image can be null if no previous snapshot exists.
     * <p>
     * <b>Note:</b> As this is a potentially long-running operation the implementation should use
     * {@link Cluster#idleStrategy()} and then occasionally call {@link org.agrona.concurrent.IdleStrategy#idle()} or
     * {@link org.agrona.concurrent.IdleStrategy#idle(int)}, especially when polling the {@link Image} returns 0.
     * <p>
     * If a {@link RuntimeException} is thrown from this method, e.g. {@link IllegalStateException}, then this will
     * cause the ClusteredServiceAgent and the ConsensusModuleAgent to shut down with an
     * {@link org.agrona.concurrent.AgentTerminationException}.
     *
     * @param cluster       with which the service can interact.
     * @param snapshotImage from which the service can load its archived state which can be null when no snapshot.
     */
    void onStart(Cluster cluster, Image snapshotImage);

    /**
     * A session has been opened for a client to the cluster.
     *
     * @param session   for the client which have been opened.
     * @param timestamp at which the session was opened.
     */
    void onSessionOpen(ClientSession session, long timestamp);

    /**
     * A session has been closed for a client to the cluster.
     *
     * @param session     that has been closed.
     * @param timestamp   at which the session was closed.
     * @param closeReason the session was closed.
     */
    void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason);

    /**
     * A message has been received to be processed by a clustered service.
     *
     * @param session   for the client which sent the message. This can be null if the client was a service.
     * @param timestamp for when the message was received.
     * @param buffer    containing the message.
     * @param offset    in the buffer at which the message is encoded.
     * @param length    of the encoded message.
     * @param header    aeron header for the incoming message.
     */
    void onSessionMessage(
        ClientSession session,
        long timestamp,
        DirectBuffer buffer,
        int offset,
        int length,
        Header header);

    /**
     * A scheduled timer has expired.
     *
     * @param correlationId for the expired timer.
     * @param timestamp     at which the timer expired.
     */
    void onTimerEvent(long correlationId, long timestamp);

    /**
     * The service should take a snapshot and store its state to the provided archive {@link ExclusivePublication}.
     * <p>
     * <b>Note:</b> As this is a potentially long-running operation the implementation should use
     * {@link Cluster#idleStrategy()} and then occasionally call {@link org.agrona.concurrent.IdleStrategy#idle()} or
     * {@link org.agrona.concurrent.IdleStrategy#idle(int)},
     * especially when the snapshot {@link ExclusivePublication} returns {@link Publication#BACK_PRESSURED}.
     *
     * @param snapshotPublication to which the state should be recorded.
     */
    void onTakeSnapshot(ExclusivePublication snapshotPublication);

    /**
     * Notify that the cluster node has changed role.
     *
     * @param newRole that the node has assumed.
     */
    void onRoleChange(Cluster.Role newRole);

    /**
     * Called when the container is going to terminate but only after a successful start.
     *
     * @param cluster with which the service can interact.
     */
    void onTerminate(Cluster cluster);

    /**
     * An election has been successful and a leader has entered a new term.
     *
     * @param leadershipTermId    identity for the new leadership term.
     * @param logPosition         position the log has reached as the result of this message.
     * @param timestamp           for the new leadership term.
     * @param termBaseLogPosition position at the beginning of the leadership term.
     * @param leaderMemberId      who won the election.
     * @param logSessionId        session id for the publication of the log.
     * @param timeUnit            for the timestamps in the coming leadership term.
     * @param appVersion          for the application configured in the consensus module.
     */
    default void onNewLeadershipTermEvent(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition,
        final int leaderMemberId,
        final int logSessionId,
        final TimeUnit timeUnit,
        final int appVersion)
    {
    }

    /**
     * Implement this method to perform background tasks that are not related to the deterministic state machine
     * model, such as keeping external connections alive to the cluster. This method must <b>not</b> be used to
     * directly, or indirectly, update the service state. This method cannot be used for making calls on
     * {@link Cluster} which could update the log such as {@link Cluster#scheduleTimer(long, long)} or
     * {@link Cluster#offer(DirectBuffer, int, int)}.
     * <p>
     * This method is not for long-running operations. Time taken can impact latency and should only be used for
     * short constant time operations.
     *
     * @param nowNs which can be used for measuring elapsed time and be used in the same way as
     *              {@link System#nanoTime()}. This is <b>not</b> {@link Cluster#time()}.
     * @return 0 if no work is done otherwise a positive number.
     * @since 1.40.0
     */
    default int doBackgroundWork(final long nowNs)
    {
        return 0;
    }
}
