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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.ExclusivePublication;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import net.bytebuddy.asm.Advice;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.Supplier;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static io.aeron.test.driver.TestMediaDriver.shouldRunJavaMediaDriver;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class LeadershipConfirmationSystemTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @TempDir
    Path directory;

    @Test
    @InterruptAfter(60)
    void shouldWaitForDelayedEchoesAndConfirmAfterLeaderChange()
    {
        assumeTrue(shouldRunJavaMediaDriver());
        for (int memberId = 0; memberId < 3; memberId++)
        {
            DelayConfirmationAck.DELAYED_ROUNDS.set(memberId, -1);
        }
        final ClusterInstrumentor instrumentor = new ClusterInstrumentor(
            DelayConfirmationAck.class, "ConsensusPublisher", "compactLeadershipConfirmAck");
        try
        {
            final TestCluster cluster = aCluster().withStaticNodes(3)
                .withClusterBaseDir(directory.resolve("cluster").toString())
                .withAeronBaseDir(directory.resolve("aeron").toString())
                .withExtensionSuppler(ConfirmationExtension::new)
                .withServiceSupplier(index -> new TestNode.TestService[0])
                .start();
            systemTestWatcher.cluster(cluster);
            final TestNode leader = cluster.awaitLeader();
            final ConfirmationExtension extension = extension(leader);
            appendAndAwaitCommit(leader);

            // Only confirmation ACKs are held; ordinary replication and leader-health messages still flow.
            DelayConfirmationAck.blockedMembers = 0b111;
            final long token = extension.requestRound();
            assertNotEquals(Aeron.NULL_VALUE, token);
            awaitDelayedEchoes(cluster, leader, token);
            assertFalse(extension.isConfirmed(token));
            appendAndAwaitCommit(leader);
            assertFalse(extension.isConfirmed(token));

            final TestNode follower = cluster.followers().get(0);
            DelayConfirmationAck.blockedMembers &= ~(1 << follower.memberId());
            Tests.await(() -> extension.isConfirmed(token));

            // Leave another read pending when its leader stops, then elect a replacement in the same cluster.
            DelayConfirmationAck.blockedMembers = 0b111;
            final long pendingToken = extension.requestRound();
            awaitDelayedEchoes(cluster, leader, pendingToken);
            assertFalse(extension.isConfirmed(pendingToken));
            cluster.stopNode(leader);
            final TestNode replacement = cluster.awaitLeader(leader.memberId());
            assertNotEquals(leader.memberId(), replacement.memberId());
            final ConfirmationExtension replacementExtension = extension(replacement);
            // Tokens belong to one control instance and are not transferred to the replacement.

            DelayConfirmationAck.blockedMembers = 0;
            final long freshToken = replacementExtension.requestRound();
            assertNotEquals(Aeron.NULL_VALUE, freshToken);
            Tests.await(() -> replacementExtension.isConfirmed(freshToken));
            appendAndAwaitCommit(replacement);
            assertTrue(replacementExtension.isConfirmed(freshToken));
        }
        finally
        {
            DelayConfirmationAck.blockedMembers = 0;
            instrumentor.reset();
        }
    }

    @Test
    @InterruptAfter(90)
    void shouldContinueReadsAfterSequentialFollowerRestartsInTheSameTerm()
    {
        assumeTrue(shouldRunJavaMediaDriver());
        final TestCluster cluster = aCluster().withStaticNodes(3)
            .withClusterBaseDir(directory.resolve("cluster").toString())
            .withAeronBaseDir(directory.resolve("aeron").toString())
            .withExtensionSuppler(ConfirmationExtension::new)
            .withServiceSupplier(index -> new TestNode.TestService[0]).start();
        systemTestWatcher.cluster(cluster);
        final TestNode leader = cluster.awaitLeader();
        final ConfirmationExtension extension = extension(leader);
        appendAndAwaitCommit(leader);
        final long term = extension.leadershipTerm();
        final long initial = extension.requestRound();
        final List<TestNode> followers = cluster.followers();
        for (final TestNode follower : followers)
        {
            Tests.await(() -> extension.peerConfirmed(follower.memberId(), initial));
        }
        for (final TestNode follower : followers)
        {
            cluster.stopNode(follower);
            final TestNode restarted = cluster.startStaticNode(follower.memberId(), false);
            TestCluster.awaitElectionClosed(restarted);
            appendAndAwaitCommit(leader);
            assertEquals(term, extension.leadershipTerm());
            final long token = extension.requestRound();
            Tests.await(() -> extension.peerConfirmed(follower.memberId(), token));
            assertTrue(extension.isConfirmed(token));
        }
        final long last = extension.requestRound();
        Tests.await(() -> extension.isConfirmed(last));
        appendAndAwaitCommit(leader);
        assertEquals(term, extension.leadershipTerm());
    }

    @Test
    @InterruptAfter(90)
    void shouldRecoverRecreatedConsensusImagesWithoutReplacingPublicationsOrChangingTerm()
    {
        assumeTrue(shouldRunJavaMediaDriver());
        final TestCluster cluster = aCluster().withStaticNodes(3)
            .withClusterBaseDir(directory.resolve("cluster").toString())
            .withAeronBaseDir(directory.resolve("aeron").toString())
            .withExtensionSuppler(ConfirmationExtension::new)
            .withServiceSupplier(index -> new TestNode.TestService[0]).start();
        systemTestWatcher.cluster(cluster);
        final TestNode leader = cluster.awaitLeader();
        final ConfirmationExtension extension = extension(leader);
        final List<TestNode> followers = cluster.followers();
        appendAndAwaitCommit(leader);
        final long term = extension.leadershipTerm();
        final long initial = extension.requestRound();
        for (final TestNode follower : followers)
        {
            Tests.await(() -> extension.peerConfirmed(follower.memberId(), initial));
        }

        // Recreate each receiver, including the leader's, while every outgoing Publication survives.
        final TestNode[] nodes = { followers.get(0), followers.get(1), leader };
        for (final TestNode receiver : nodes)
        {
            final ConfirmationExtension receiverExtension = extension(receiver);
            final int senderId = receiver == leader ? followers.get(0).memberId() : leader.memberId();
            final Image oldImage = receiverExtension.peerImage(senderId);
            final ExclusivePublication[][] publications = new ExclusivePublication[nodes.length][];
            for (int i = 0; i < nodes.length; i++)
            {
                publications[i] = extension(nodes[i]).consensusPublications();
            }
            receiverExtension.recreateConsensusSubscription();
            assertTrue(oldImage.isClosed());
            final long token = extension.requestRound();
            for (final TestNode follower : followers)
            {
                Tests.await(() -> extension.peerConfirmed(follower.memberId(), token));
            }
            assertTrue(extension.isConfirmed(token));
            assertNotSame(oldImage, receiverExtension.peerImage(senderId));
            for (int i = 0; i < nodes.length; i++)
            {
                final ExclusivePublication[] after = extension(nodes[i]).consensusPublications();
                for (int j = 0; j < after.length; j++)
                {
                    assertSame(publications[i][j], after[j]);
                }
            }
            assertEquals(term, extension.leadershipTerm());
            appendAndAwaitCommit(leader);
        }
    }

    private static void appendAndAwaitCommit(final TestNode leader)
    {
        long position;
        while ((position = extension(leader).append()) <= 0)
        {
            Tests.yield();
        }
        final long targetPosition = position;
        Tests.await(() -> leader.commitPosition() >= targetPosition);
    }

    private static ConfirmationExtension extension(final TestNode node)
    {
        return (ConfirmationExtension)node.consensusModule().context().consensusModuleExtension();
    }

    private static void awaitDelayedEchoes(final TestCluster cluster, final TestNode leader, final long token)
    {
        for (final TestNode follower : cluster.followers())
        {
            assertNotEquals(leader.memberId(), follower.memberId());
            Tests.await(() -> DelayConfirmationAck.DELAYED_ROUNDS.get(follower.memberId()) > token);
        }
    }

    public static class DelayConfirmationAck
    {
        public static volatile int blockedMembers;
        public static final AtomicLongArray DELAYED_ROUNDS = new AtomicLongArray(
            new long[]{ -1, -1, -1 });

        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        static boolean delay(@Advice.Argument(2) final int memberId, @Advice.Argument(3) final long round)
        {
            if (0 != (blockedMembers & (1 << memberId)))
            {
                DELAYED_ROUNDS.set(memberId, round);
                return true;
            }
            return false;
        }
    }

    private static final class ConfirmationExtension extends TestNode.TestConsensusModuleExtension
    {
        private final ConcurrentLinkedQueue<Runnable> commands = new ConcurrentLinkedQueue<>();
        private ConsensusModuleControl control;
        private ExclusivePublication logPublication;
        private final UnsafeBuffer message = new UnsafeBuffer(new byte[MessageHeaderEncoder.ENCODED_LENGTH]);

        private ConfirmationExtension()
        {
            new MessageHeaderEncoder().wrap(message, 0)
                .schemaId(TestCluster.EXTENSION_SCHEMA_ID)
                .templateId(TestCluster.EXTENSION_TEMPLATE_ID)
                .version(TestCluster.EXTENSION_VERSION)
                .blockLength(0);
        }

        public int supportedSchemaId()
        {
            return TestCluster.EXTENSION_SCHEMA_ID;
        }

        public void onElectionComplete(final ConsensusControlState state)
        {
            super.onElectionComplete(state);
            logPublication = state.logPublication();
        }

        public void onPrepareForNewLeadership()
        {
            logPublication = null;
        }

        private long leadershipTerm()
        {
            return onAgentThread(() -> Tests.<Long>getField(control, "leadershipTermId"));
        }

        private boolean peerConfirmed(final int memberId, final long token)
        {
            return onAgentThread(() ->
            {
                final ClusterMember[] members = Tests.getField(control, "activeMembers");
                final long term = Tests.getField(control, "leadershipTermId");
                for (final ClusterMember member : members)
                {
                    if (member.id() == memberId)
                    {
                        return member.compactConfirmation.confirmed(term, token);
                    }
                }
                return false;
            });
        }

        private Image peerImage(final int memberId)
        {
            return onAgentThread(() ->
            {
                final ClusterMember[] members = Tests.getField(control, "activeMembers");
                return Tests.getField(ClusterMember.findMember(members, memberId).compactConfirmation, "image");
            });
        }

        private ExclusivePublication[] consensusPublications()
        {
            return onAgentThread(() ->
            {
                final ClusterMember[] members = Tests.getField(control, "activeMembers");
                final ExclusivePublication[] publications = new ExclusivePublication[members.length];
                for (int i = 0; i < members.length; i++)
                {
                    publications[i] = members[i].publication();
                }
                return publications;
            });
        }

        private void recreateConsensusSubscription()
        {
            onAgentThread(() ->
            {
                final ConsensusAdapter adapter = Tests.getField(control, "consensusAdapter");
                final Subscription previous = Tests.getField(adapter, "subscription");
                final Aeron aeron = Tests.getField(control, "aeron");
                previous.close();
                Tests.setField(adapter, "subscription", aeron.addSubscription(previous.channel(), previous.streamId()));
                return true;
            });
        }

        private long append()
        {
            return onAgentThread(() -> null == logPublication ? Aeron.NULL_VALUE :
                logPublication.offer(message, 0, message.capacity()));
        }

        public void onStart(final ConsensusModuleControl control, final Image snapshotImage)
        {
            this.control = control;
        }

        public int doWork(final long nowNs)
        {
            int workCount = 0;
            Runnable command;
            while (null != (command = commands.poll()))
            {
                command.run();
                ++workCount;
            }
            return workCount;
        }

        private long requestRound()
        {
            return onAgentThread(() -> control.triggerQuorumConfirmation());
        }

        private boolean isConfirmed(final long token)
        {
            return onAgentThread(() -> control.isLeadershipConfirmedSince(token));
        }

        private <T> T onAgentThread(final Supplier<T> action)
        {
            final CompletableFuture<T> result = new CompletableFuture<>();
            commands.add(() -> result.complete(action.get()));
            Tests.await(result::isDone);
            return result.join();
        }
    }
}
