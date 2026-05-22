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
import io.aeron.Counter;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.cluster.client.ClusterEvent;
import io.aeron.test.cluster.TestClusterClock;
import org.agrona.ErrorHandler;
import org.agrona.collections.Int2ObjectHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static io.aeron.Aeron.NULL_VALUE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;

public class ElectionTruncateLogPositionTest
{
    private static final long RECORDING_ID = 1L;
    private static final long INITIAL_LOG_POSITION = 10695424L;
    private static final long TRUNCATED_LOG_POSITION = 10695296L;
    private static final long LEADERSHIP_TERM_ID = 2L;

    private ConsensusModule.Context ctx;
    private ConsensusModuleAgent consensusModuleAgent;
    private ClusterMember thisMember;
    private ClusterMember[] clusterMembers;
    private ConsensusPublisher consensusPublisher;

    @BeforeEach
    void setUp()
    {
        consensusModuleAgent = mock(ConsensusModuleAgent.class);
        consensusPublisher = mock(ConsensusPublisher.class);
        final Aeron aeron = mock(Aeron.class);
        final Subscription subscription = mock(Subscription.class);
        final Image logImage = mock(Image.class);
        final Counter electionStateCounter = mock(Counter.class);
        final Counter electionCounter = mock(Counter.class);
        final ErrorHandler errorHandler = mock(ErrorHandler.class);

        ctx = new ConsensusModule.Context()
            .aeron(aeron)
            .clusterClock(new TestClusterClock(NANOSECONDS))
            .random(new Random())
            .errorHandler(errorHandler)
            .electionStateCounter(electionStateCounter)
            .electionCounter(electionCounter);

        thisMember = ClusterMember.parseEndpoints(
            -1, "localhost:20000,localhost:20001,localhost:20002,localhost:20003,localhost:20004");
        thisMember.id(0);
        clusterMembers = new ClusterMember[]{thisMember};

        when(aeron.addCounter(anyInt(), anyString())).thenReturn(electionStateCounter);
        when(aeron.addSubscription(anyString(), anyInt())).thenReturn(subscription);
        when(consensusModuleAgent.logRecordingId()).thenReturn(RECORDING_ID);
        when(logImage.joinPosition()).thenReturn(TRUNCATED_LOG_POSITION);
        when(subscription.imageBySessionId(anyInt())).thenReturn(logImage);
    }

    @Test
    void shouldUpdateLogPositionOnTruncateLogEntry()
    {
        final Election election = createElection(INITIAL_LOG_POSITION);

        assertEquals(INITIAL_LOG_POSITION, election.logPosition(),
            "Initial logPosition should be " + INITIAL_LOG_POSITION);

        try
        {
            election.onTruncateLogEntry(
                thisMember.id(),
                ElectionState.CANVASS,
                LEADERSHIP_TERM_ID,
                LEADERSHIP_TERM_ID,
                LEADERSHIP_TERM_ID + 1,
                INITIAL_LOG_POSITION,  // commitPosition
                INITIAL_LOG_POSITION,  // logPosition (current)
                INITIAL_LOG_POSITION,  // appendPosition
                INITIAL_LOG_POSITION,  // oldPosition
                TRUNCATED_LOG_POSITION // newPosition (target)
            );
            fail("Should throw ClusterEvent");
        }
        catch (final ClusterEvent e)
        {
            assertTrue(e.getMessage().contains("Truncating Cluster Log"));
        }

        assertEquals(TRUNCATED_LOG_POSITION, election.logPosition(),
            "logPosition should be updated to truncated position " + TRUNCATED_LOG_POSITION);

        verify(consensusModuleAgent).truncateLogEntry(LEADERSHIP_TERM_ID, TRUNCATED_LOG_POSITION);
    }

    @Test
    void shouldAllowFollowerJoinAfterTruncation()
    {
        final Election election = createElection(INITIAL_LOG_POSITION);

        try
        {
            election.onTruncateLogEntry(
                thisMember.id(),
                ElectionState.CANVASS,
                LEADERSHIP_TERM_ID,
                LEADERSHIP_TERM_ID,
                LEADERSHIP_TERM_ID + 1,
                INITIAL_LOG_POSITION,
                INITIAL_LOG_POSITION,
                INITIAL_LOG_POSITION,
                INITIAL_LOG_POSITION,
                TRUNCATED_LOG_POSITION
            );
        }
        catch (final ClusterEvent e)
        {
            // Expected
        }

        assertDoesNotThrow(
            () -> election.verifyLogJoinPosition("followerLogAwait", TRUNCATED_LOG_POSITION),
            "verifyLogJoinPosition should not throw when positions match after truncation");
    }

    @Test
    void shouldThrowWhenJoinPositionMismatchesStaleLogPosition()
    {
        final Election election = createElection(INITIAL_LOG_POSITION);

        final ClusterEvent exception = assertThrows(
            ClusterEvent.class,
            () -> election.verifyLogJoinPosition("followerLogAwait", TRUNCATED_LOG_POSITION),
            "Should throw ClusterEvent when joinPosition doesn't match logPosition");

        assertTrue(exception.getMessage().contains("followerLogAwait"));
        assertTrue(exception.getMessage().contains("joinPosition=" + TRUNCATED_LOG_POSITION));
        assertTrue(exception.getMessage().contains("logPosition=" + INITIAL_LOG_POSITION));
        assertTrue(exception.getMessage().contains("less than"),
            "Should indicate joinPosition is less than logPosition");
    }

    private Election createElection(final long logPosition)
    {
        return new Election(
            true,                    // isNodeStartup
            NULL_VALUE,              // gracefulClosedLeaderId
            LEADERSHIP_TERM_ID,      // logLeadershipTermId
            logPosition,             // logPosition
            logPosition,             // appendPosition
            logPosition,             // termBaseLogPosition
            clusterMembers,          // clusterMembers
            new Int2ObjectHashMap<>(), // clusterMemberByIdMap
            thisMember,              // thisMember
            consensusPublisher,      // consensusPublisher
            ctx,                     // ctx
            consensusModuleAgent     // consensusModuleAgent
        );
    }
}
