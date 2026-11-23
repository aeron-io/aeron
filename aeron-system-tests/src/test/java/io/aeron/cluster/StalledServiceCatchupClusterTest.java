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

import io.aeron.archive.client.AeronArchive;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import net.bytebuddy.asm.Advice;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.test.cluster.TestCluster.aCluster;
import static io.aeron.test.cluster.TestCluster.awaitElectionClosed;
import static io.aeron.test.cluster.TestCluster.awaitElectionState;

/**
 * A follower whose service stands still while it catches up, its consensus module reading on: the service must
 * still join the log where the module does once the catch-up is cut short, by the module giving it up for want of
 * progress, by the leader going, or by the leader's archive revoking the catch-up replay.
 * <p>
 * The service stands still on the test service's pause message, which only the follower's service acts on, sent
 * while the follower is down: the follower meets it in its catch-up. The messages sent after it are more than the
 * log's receiver window (128 KiB with the test log channel's 512 KiB terms), so the module can read a window ahead
 * of its service.
 */
@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class StalledServiceCatchupClusterTest
{
    private static final int MESSAGES_BEFORE = 10;
    private static final int MESSAGES_AFTER = 5_000;
    private static final long STALL_NS = TimeUnit.SECONDS.toNanos(15);
    /** More than a receiver window of the test's 4 byte messages, 96 bytes each in the log. */
    private static final int MESSAGES_WINDOWS = 3_000;
    /** Longer than the follower's leader heartbeat timeout (2 s), for its module to give its catch-up up. */
    private static final long FIRST_STALL_NS = TimeUnit.SECONDS.toNanos(4);
    private static final long SECOND_STALL_NS = TimeUnit.SECONDS.toNanos(5);

    private static ClusterInstrumentor holdInstrumentor;
    private static ClusterInstrumentor endpointInstrumentor;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @BeforeAll
    static void beforeAll()
    {
        // Loaded before the instrumentation, which then retransforms them: ByteBuddy passes over a class loaded
        // while it transforms another, as ClusterMember is while ConsensusModuleAgent is.
        ConsensusModuleAgent.class.getName();
        ClusterMember.class.getName();

        holdInstrumentor = new ClusterInstrumentor(
            HoldCatchupReplayStop.class, "ConsensusModuleAgent", "stopExistingCatchupReplay");
        endpointInstrumentor = new ClusterInstrumentor(
            FixedCatchupEndpoint.class, "ClusterMember", "catchupEndpoint");
    }

    @AfterAll
    static void afterAll()
    {
        holdInstrumentor.reset();
        endpointInstrumentor.reset();
    }

    @AfterEach
    void afterEach()
    {
        HoldCatchupReplayStop.hold = false;
        HoldCatchupReplayStop.heldReplaySessionId = NULL_VALUE;
        FixedCatchupEndpoint.fixed = false;
    }

    /**
     * While {@link #fixed} is set, each member catches up on a port of its own, the same for every catch-up, as
     * members configured with a catch-up port do, rather than on a port taken afresh for each one.
     */
    public static class FixedCatchupEndpoint
    {
        static volatile boolean fixed = false;

        @SuppressWarnings("checkstyle:FinalParameters") // the advice rewrites the endpoint returned
        @Advice.OnMethodExit
        static void catchupEndpoint(
            @Advice.FieldValue("id") final int id, @Advice.Return(readOnly = false) String endpoint)
        {
            if (fixed && null != endpoint && endpoint.endsWith(":0"))
            {
                endpoint = endpoint.substring(0, endpoint.length() - 1) + (20690 + id);
            }
        }
    }

    /**
     * While {@link #hold} is set, the first catch-up replay the leader stops, when a follower canvasses again, is
     * left running, as when its archive is too busy to act on the stop in time. The leader forgets the replay as it
     * does once it has sent the stop, so it can start another for the follower's next catch-up; the replay's session
     * is kept for the test to stop.
     */
    public static class HoldCatchupReplayStop
    {
        static volatile boolean hold = false;
        static volatile long heldReplaySessionId = NULL_VALUE;

        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        static boolean stopExistingCatchupReplay(@Advice.Argument(0) final ClusterMember follower)
        {
            if (hold && NULL_VALUE == heldReplaySessionId && NULL_VALUE != follower.catchupReplaySessionId())
            {
                heldReplaySessionId = follower.catchupReplaySessionId();
                follower.catchupReplaySessionId(NULL_VALUE);
                follower.catchupReplayCorrelationId(NULL_VALUE);
                return true;
            }

            return false;
        }
    }

    @Test
    @InterruptAfter(90)
    void shouldJoinTheLogWhenTheServiceStallsDuringCatchup()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        final TestNode follower = stallDuringCatchup(cluster);
        final int total = MESSAGES_BEFORE + 1 + MESSAGES_AFTER;

        cluster.awaitServiceMessageCount(follower, total);
        cluster.awaitServicesMessageCount(total);
    }

    @Test
    @InterruptAfter(90)
    void shouldJoinTheLogWhenTheServiceStallsDuringCatchupAndTheLeaderStops()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final TestNode follower = stallDuringCatchup(cluster);
        final int total = MESSAGES_BEFORE + 1 + MESSAGES_AFTER;

        cluster.stopNode(leader);
        cluster.awaitLeader(leader.index());

        cluster.awaitServiceMessageCount(follower, total);
        cluster.awaitServicesMessageCount(total);
    }

    /**
     * As a three-member cluster under random faults had it on Aeron 1.54.0: the follower's first catch-up stalls and
     * it canvasses again; the leader's archive is slow to stop the first catch-up replay, so the follower's second
     * catch-up subscription, on the same endpoint for the same session, reads on from it, the leader's new replay to
     * that endpoint clashing with it; then the replay is stopped, which revokes its publication, and the follower's
     * driver drops the image while the follower's service, behind its module, has not read all of it.
     */
    @Test
    @InterruptAfter(60)
    void shouldJoinTheLogWhenAStaleCatchupReplayIsRevokedBeforeTheServiceReadsIt()
    {
        revokeAStaleCatchupReplay(true);
    }

    /**
     * The same, each catch-up on a port taken afresh, as {@code TestCluster}'s members do: the stale replay sends to
     * a port no longer subscribed and the follower's next catch-up reads the leader's new replay.
     */
    @Test
    @InterruptAfter(60)
    void shouldJoinTheLogWhenAStaleCatchupReplayIsRevokedOnAPortNoLongerUsed()
    {
        revokeAStaleCatchupReplay(false);
    }

    private void revokeAStaleCatchupReplay(final boolean fixedCatchupEndpoint)
    {
        FixedCatchupEndpoint.fixed = fixedCatchupEndpoint;
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        cluster.connectClient();
        cluster.sendAndAwaitMessages(MESSAGES_BEFORE);

        final TestNode stopped = cluster.followers().get(0);
        awaitElectionClosed(stopped);
        cluster.stopNode(stopped);

        // The first catch-up stalls on the first pause, long enough for the module to give it up.
        cluster.sendMessageToSlowDownService(stopped.index(), FIRST_STALL_NS);
        cluster.sendMessages(MESSAGES_WINDOWS);
        // The second catch-up, from the stale replay, stalls on the second.
        cluster.sendMessageToSlowDownService(stopped.index(), SECOND_STALL_NS);
        cluster.sendMessages(MESSAGES_WINDOWS);
        final int total = MESSAGES_BEFORE + 1 + MESSAGES_WINDOWS + 1 + MESSAGES_WINDOWS;
        cluster.awaitResponseMessageCount(total);

        HoldCatchupReplayStop.heldReplaySessionId = NULL_VALUE;
        HoldCatchupReplayStop.hold = true;
        final TestNode follower = cluster.startStaticNode(stopped.index(), false);

        final int beforeSecondPause = MESSAGES_BEFORE + 1 + MESSAGES_WINDOWS;
        Tests.await(() -> follower.service().messageCount() >= beforeSecondPause);
        // The service on the second pause, the module a window ahead of it on the stale replay's image.
        Tests.sleep(500);

        final long replaySessionId = HoldCatchupReplayStop.heldReplaySessionId;
        HoldCatchupReplayStop.hold = false;
        if (NULL_VALUE == replaySessionId)
        {
            throw new IllegalStateException("the follower did not canvass again during its first catch-up");
        }

        try (AeronArchive leaderArchive = AeronArchive.connect(new AeronArchive.Context()
            .clientName("test")
            .aeronDirectoryName(cluster.startClientMediaDriver().aeronDirectoryName())
            .controlRequestChannel(leader.archive().context().controlChannel())
            .controlRequestStreamId(leader.archive().context().controlStreamId())
            .controlResponseChannel("aeron:udp?endpoint=localhost:0")))
        {
            leaderArchive.stopReplay(replaySessionId);
        }

        cluster.awaitServiceMessageCount(follower, total);
        cluster.awaitServicesMessageCount(total);
    }

    /**
     * A follower stopped, the pause message for it and {@link #MESSAGES_AFTER} more sent, the follower started
     * again: returned once it catches up and its service stands still on the pause message.
     *
     * @param cluster with a leader.
     * @return the follower, started again.
     */
    private static TestNode stallDuringCatchup(final TestCluster cluster)
    {
        cluster.connectClient();
        cluster.sendAndAwaitMessages(MESSAGES_BEFORE);

        final TestNode stopped = cluster.followers().get(0);
        awaitElectionClosed(stopped);
        cluster.stopNode(stopped);

        cluster.sendMessageToSlowDownService(stopped.index(), STALL_NS);
        cluster.sendMessages(MESSAGES_AFTER);
        cluster.awaitResponseMessageCount(MESSAGES_BEFORE + 1 + MESSAGES_AFTER);

        final TestNode follower = cluster.startStaticNode(stopped.index(), false);
        awaitElectionState(follower, ElectionState.FOLLOWER_CATCHUP);
        // The service stands on the pause message, the module a window ahead of it.
        Tests.sleep(1_000);

        return follower;
    }
}
