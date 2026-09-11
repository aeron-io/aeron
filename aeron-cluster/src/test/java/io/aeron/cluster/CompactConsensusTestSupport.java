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
import io.aeron.ConcurrentPublication;
import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.UnavailableImageHandler;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.driver.DutyCycleTracker;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.security.AuthorisationService;
import io.aeron.security.DefaultAuthenticatorSupplier;
import io.aeron.status.ReadableCounter;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestClusterClock;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class CompactConsensusTestSupport
{
    static ConsensusModule.Context newContext()
    {
        final Aeron aeron = mock(Aeron.class);
        final ConcurrentPublication response = mock(ConcurrentPublication.class);
        when(aeron.conductorAgentInvoker()).thenReturn(mock(AgentInvoker.class));
        when(aeron.addPublication(anyString(), anyInt())).thenReturn(response);
        when(aeron.getPublication(anyLong())).thenReturn(response);
        when(aeron.addExclusivePublication(anyString(), anyInt())).thenReturn(mock(ExclusivePublication.class));
        when(aeron.addSubscription(anyString(), anyInt())).thenReturn(mock(Subscription.class));
        when(aeron.addSubscription(anyString(), anyInt(), eq(null), any(UnavailableImageHandler.class)))
            .thenReturn(mock(Subscription.class));
        when(response.isConnected()).thenReturn(true);
        when(response.availableWindow()).thenReturn(Long.MAX_VALUE);
        return TestContexts.localhostConsensusModule()
            .errorHandler(Tests::onError)
            .errorCounter(mock(AtomicCounter.class))
            .countedErrorHandler(mock(CountedErrorHandler.class))
            .moduleStateCounter(mock(Counter.class))
            .commitPositionCounter(mock(Counter.class))
            .controlToggleCounter(mock(Counter.class))
            .nodeControlToggleCounter(mock(Counter.class))
            .clusterNodeRoleCounter(mock(Counter.class))
            .electionCounter(mock(Counter.class))
            .leadershipTermIdCounter(mock(Counter.class))
            .timedOutClientCounter(mock(Counter.class))
            .clusterTimeConsumerSupplier(context -> time -> {})
            .idleStrategySupplier(NoOpIdleStrategy::new)
            .timerServiceSupplier((clock, handler) -> mock(TimerService.class))
            .aeron(aeron)
            .clusterMemberId(0)
            .authenticatorSupplier(new DefaultAuthenticatorSupplier())
            .authorisationServiceSupplier(() -> AuthorisationService.DENY_ALL)
            .clusterMarkFile(mock(ClusterMarkFile.class))
            .archiveContext(new AeronArchive.Context())
            .logPublisher(mock(LogPublisher.class))
            .egressPublisher(mock(EgressPublisher.class))
            .dutyCycleTracker(new DutyCycleTracker());
    }

    static final class Pair
    {
        final Node leader = new Node(0, Cluster.Role.LEADER);
        final Node follower = new Node(1, Cluster.Role.FOLLOWER);
        final Link toFollower = new Link(follower);
        final Link toLeader = new Link(leader);


        Pair()
        {
            this(true);
        }

        Pair(final boolean connect)
        {
            leader.members[1].publication(toFollower.publication);
            follower.members[0].publication(toLeader.publication);
            if (connect)
            {
                echo();
                broadcast();
                echo();
                broadcast();
                echo();
            }
        }

        void broadcast()
        {
            leader.nowNs += leader.context.leaderHeartbeatIntervalNs();
            tickLeader(leader.nowNs);
            toFollower.deliver();
        }

        int tickLeader(final long nowNs)
        {
            leader.nowNs = nowNs;
            return leader.agent.updateLeaderPosition(nowNs, leader.recordedPosition, leader.recordedPosition);
        }

        void echo()
        {
            follower.agent.updateFollowerPosition(++follower.nowNs);
            toLeader.deliver();
        }
    }

    static final class Node
    {
        final ConsensusModuleAgent agent;
        final ConsensusModule.Context context;
        final ClusterMember[] members;
        long recordedPosition;
        long nowNs;
        long committedPosition;

        Node(final int id, final Cluster.Role role)
        {
            final TestClusterClock clock = new TestClusterClock(TimeUnit.MILLISECONDS);
            context = newContext().clusterMemberId(id)
                .clusterMembers(id + ",localhost:20000,localhost:20001,localhost:20002,localhost:0,localhost:8010")
                .epochClock(clock.asEpochClock()).clusterClock(clock)
                .recordingLog(mock(RecordingLog.class)).ingressChannel("aeron:udp");
            when(context.commitPositionCounter().getPlain()).thenAnswer(invocation -> committedPosition);
            when(context.commitPositionCounter().proposeMaxRelease(anyLong())).thenAnswer(invocation ->
            {
                final long position = invocation.getArgument(0);
                final boolean advanced = position > committedPosition;
                committedPosition = Math.max(committedPosition, position);
                return advanced;
            });
            agent = new ConsensusModuleAgent(context);
            agent.state(ConsensusModule.State.ACTIVE, "");
            agent.role(role);
            members = setActiveMembers(agent, 0, 1, 2);
            Tests.setField(agent, "consensusPublisher", new ConsensusPublisher());
            final ReadableCounter appendPosition = mock(ReadableCounter.class);
            when(appendPosition.get()).thenAnswer(invocation -> recordedPosition);
            Tests.setField(agent, "appendPosition", appendPosition);
            completeElection(agent, 42, members[0], 0);
        }
    }

    static final class Link
    {
        Image image = mock(Image.class);
        final ExclusivePublication publication = mock(ExclusivePublication.class);
        final List<UnsafeBuffer> messages = new ArrayList<>();
        final ConsensusAdapter adapter;
        int credit = 1000;
        int delivered;

        Link(final Node target)
        {
            adapter = new ConsensusAdapter(mock(Subscription.class), target.agent);
            when(publication.tryClaim(anyInt(), any(BufferClaim.class))).thenAnswer(invocation ->
            {
                if (0 == credit)
                {
                    return Publication.BACK_PRESSURED;
                }
                --credit;
                final int length = invocation.getArgument(0);
                assertTrue(DataHeaderFlyweight.HEADER_LENGTH + length <= 64);
                final UnsafeBuffer buffer = new UnsafeBuffer(new byte[64]);
                final BufferClaim claim = invocation.getArgument(1);
                claim.wrap(buffer, 0, DataHeaderFlyweight.HEADER_LENGTH + length);
                messages.add(buffer);
                return messages.size() * 64L;
            });
        }

        void replaceImage()
        {
            when(image.isClosed()).thenReturn(true);
            image = mock(Image.class);
            delivered = messages.size();
        }

        int template(final int index)
        {
            return new MessageHeaderDecoder().wrap(messages.get(index), DataHeaderFlyweight.HEADER_LENGTH).templateId();
        }

        void deliver()
        {
            while (delivered < messages.size())
            {
                final UnsafeBuffer buffer = messages.get(delivered++);
                final MessageHeaderDecoder header =
                    new MessageHeaderDecoder().wrap(buffer, DataHeaderFlyweight.HEADER_LENGTH);
                adapter.onFragment(buffer, DataHeaderFlyweight.HEADER_LENGTH,
                    MessageHeaderEncoder.ENCODED_LENGTH + header.blockLength(), new Header(0, 0, image));
            }
        }
    }

    static ClusterMember[] setActiveMembers(final ConsensusModuleAgent agent, final int... ids)
    {
        final ClusterMember[] members = new ClusterMember[ids.length];
        for (int i = 0; i < ids.length; i++)
        {
            members[i] = new ClusterMember(ids[i], "", "", "", "", "", "");
        }

        final org.agrona.collections.Int2ObjectHashMap<ClusterMember> map =
            new org.agrona.collections.Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(members, map);
        Tests.setField(agent, "activeMembers", members);
        Tests.setField(agent, "rankedPositions", new long[ClusterMember.quorumThreshold(members.length)]);
        Tests.setField(agent, "clusterMemberByIdMap", map);

        return members;
    }

    static void completeElection(
        final ConsensusModuleAgent agent, final long term, final ClusterMember leader, final long nowNs)
    {
        final Election election = mock(Election.class);
        when(election.leadershipTermId()).thenReturn(term);
        when(election.leader()).thenReturn(leader);
        Tests.setField(agent, "election", election);
        agent.electionComplete(nowNs);
    }

}
