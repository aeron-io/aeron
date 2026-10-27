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
import io.aeron.archive.client.AeronArchive;
import io.aeron.security.AuthorisationService;

import org.agrona.concurrent.IdleStrategy;

import java.util.concurrent.TimeUnit;

/**
 * Control interface for performing operations on the consensus module from a {@link ConsensusModuleExtension}.
 */
public interface ConsensusModuleControl
{
    /**
     * The unique id for the hosting member of the cluster.
     *
     * @return unique id for the hosting member of the cluster.
     */
    int memberId();

    /**
     * Cluster time as {@link #timeUnit()}s since 1 Jan 1970 UTC.
     *
     * @return time as {@link #timeUnit()}s since 1 Jan 1970 UTC.
     * @see #timeUnit()
     */
    long time();

    /**
     * The unit of time applied when timestamping and invoking {@link #time()} operations.
     *
     * @return the unit of time applied when timestamping and invoking {@link #time()} operations.
     * @see #time()
     */
    TimeUnit timeUnit();

    /**
     * {@link IdleStrategy} which should be used by the extension when it experiences back-pressure or is undertaking
     * any long-running actions.
     *
     * @return the {@link IdleStrategy} which should be used by the extension when it experiences back-pressure or is
     * undertaking any long-running actions.
     */
    IdleStrategy idleStrategy();

    /**
     * The {@link ConsensusModule.Context} under which the extension is running.
     *
     * @return the {@link ConsensusModule.Context} under which the extension is running.
     */
    ConsensusModule.Context context();

    /**
     * The {@link Aeron} client to be used by the extension.
     *
     * @return the {@link Aeron} client to be used by the extension.
     */
    Aeron aeron();

    /**
     * The {@link AeronArchive} client to be used by the extension.
     *
     * @return the {@link AeronArchive} client to be used by the extension.
     */
    AeronArchive archive();

    /**
     * The {@link AuthorisationService} used by the consensus module.
     *
     * @return the {@link AuthorisationService} used by the consensus module.
     */
    AuthorisationService authorisationService();

    /**
     * Lookup a {@link ClusterClientSession} for a given id.
     *
     * @param clusterSessionId for the session to lookup.
     * @return a {@link ClusterClientSession} for a given id, otherwise {@code null} if not found.
     */
    ClusterClientSession getClientSession(long clusterSessionId);

    /**
     * Close a cluster session as an administrative function.
     *
     * @param clusterSessionId to be closed.
     */
    void closeClusterSession(long clusterSessionId);

    /**
     * Numeric id for the commit position counter.
     *
     * @return commit position counter id.
     */
    int commitPositionCounterId();

    /**
     * Numeric id for the cluster (used when running multiple clusters on the same media driver).
     *
     * @return numeric id for the cluster.
     * @see ConsensusModule.Context#clusterId(int)
     */
    int clusterId();

    /**
     * The current cluster member for this node.
     *
     * @return cluster member for this node.
     */
    ClusterMember clusterMember();

    /**
     * Is this node still the leader, confirmed by a quorum of members that recognise the current leadership term
     * and have echoed a confirmation counter strictly beyond {@code confirmationToken}? {@code false} if this node
     * is not the leader or fewer than a quorum have done so.
     *
     * <p>Capture the token from {@link #triggerQuorumConfirmation()} after the read point, then poll this method
     * from {@link ConsensusModuleExtension#consensusWork(long)}. Discard it on any leadership change; do not
     * persist it, transfer it to another control instance, or reuse it in a later leadership epoch.
     * Compact confirmation uses full-width generations and checks the full leadership term on the wire.
     * A quorum, including the leader, must support compact confirmation. Members without Compact support
     * still participate in log replication but cannot supply a confirmation acknowledgement. Tokens captured before
     * election completion are invalidated, and round exhaustion fails closed instead of wrapping.
     *
     * <p>Call on the consensus agent thread.
     *
     * @param confirmationToken captured in the current leadership epoch.
     * @return {@code true} if leadership is confirmed by a fresh quorum, otherwise {@code false}.
     */
    boolean isLeadershipConfirmedSince(long confirmationToken);

    /**
     * Request a coalesced leader confirmation round so followers acknowledge promptly, letting
     * {@link #isLeadershipConfirmedSince(long)} confirm leadership in ~1 RTT rather than waiting for the periodic
     * keep-alive. Multiple calls within a duty cycle share a single round.
     *
     * <p>Call on the consensus agent thread.
     *
     * @return an opaque, short-lived confirmation token to pass to {@link #isLeadershipConfirmedSince(long)}, or
     *         {@link io.aeron.Aeron#NULL_VALUE} if this node is not the leader or an election is in progress.
     */
    long triggerQuorumConfirmation();
}
