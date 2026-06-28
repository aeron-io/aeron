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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.ClusterMember.compareLog;
import static io.aeron.cluster.ClusterMember.hasQuorumAtPosition;
import static io.aeron.cluster.ClusterMember.isQuorumCandidate;
import static io.aeron.cluster.ClusterMember.isQuorumLeader;
import static io.aeron.cluster.ClusterMember.isUnanimousCandidate;
import static io.aeron.cluster.ClusterMember.isUnanimousLeader;
import static io.aeron.cluster.ClusterMember.quorumPosition;
import static io.aeron.cluster.ClusterMember.quorumThreshold;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterMemberTest
{
    private final ClusterMember[] members = ClusterMember.parse(
        "0,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|" +
        "1,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|" +
        "2,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|");

    private final ClusterMember[] membersWithArchiveResponse = ClusterMember.parse(
        "0,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint,archiveResponseEndpoint|" +
        "1,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint,archiveResponseEndpoint|" +
        "2,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint,archiveResponseEndpoint|");

    private final ClusterMember[] membersWithEgressResponse = ClusterMember.parse(
        "0,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint,archiveResponseEndpoint," +
        "egressResponseEndpoint|" +
        "1,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint,archiveResponseEndpoint," +
        "egressResponseEndpoint|" +
        "2,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint,archiveResponseEndpoint," +
        "egressResponseEndpoint|");

    private final long[] rankedPositions = new long[quorumThreshold(members.length)];

    @Test
    void shouldParseCorrectly()
    {
        for (final ClusterMember member : members)
        {
            assertEquals("ingressEndpoint", member.ingressEndpoint());
            assertEquals("consensusEndpoint", member.consensusEndpoint());
            assertEquals("logEndpoint", member.logEndpoint());
            assertEquals("catchupEndpoint", member.catchupEndpoint());
            assertEquals("archiveEndpoint", member.archiveEndpoint());
            assertNull(member.archiveResponseEndpoint());
            assertNull(member.egressResponseEndpoint());
            assertEquals(
                "ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint", member.endpoints());
        }
    }

    @Test
    void shouldParseCorrectlyOptionalArchiveResponse()
    {
        for (final ClusterMember member : membersWithArchiveResponse)
        {
            assertEquals("ingressEndpoint", member.ingressEndpoint());
            assertEquals("consensusEndpoint", member.consensusEndpoint());
            assertEquals("logEndpoint", member.logEndpoint());
            assertEquals("catchupEndpoint", member.catchupEndpoint());
            assertEquals("archiveEndpoint", member.archiveEndpoint());
            assertEquals("archiveResponseEndpoint", member.archiveResponseEndpoint());
            assertNull(member.egressResponseEndpoint());
            assertEquals(
                "ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint,archiveResponseEndpoint",
                member.endpoints());
        }
    }

    @Test
    void shouldParseCorrectlyOptionalEgressResponseEntry()
    {
        for (final ClusterMember member : membersWithEgressResponse)
        {
            assertEquals("ingressEndpoint", member.ingressEndpoint());
            assertEquals("consensusEndpoint", member.consensusEndpoint());
            assertEquals("logEndpoint", member.logEndpoint());
            assertEquals("catchupEndpoint", member.catchupEndpoint());
            assertEquals("archiveEndpoint", member.archiveEndpoint());
            assertEquals("archiveResponseEndpoint", member.archiveResponseEndpoint());
            assertEquals("egressResponseEndpoint", member.egressResponseEndpoint());
            assertEquals(
                "ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint," +
                "archiveResponseEndpoint,egressResponseEndpoint",
                member.endpoints());
        }
    }

    @ParameterizedTest
    @CsvSource({
        "1,1",
        "2,2",
        "3,2",
        "4,3",
        "5,3",
        "6,4",
        "7,4" })
    void shouldDetermineQuorumSize(final int clusterSize, final int expectedQuorumSize)
    {
        assertEquals(expectedQuorumSize, ClusterMember.quorumThreshold(clusterSize));
    }

    @ParameterizedTest
    @CsvSource({
        "0,0,0,false",
        "0,0,1,true",
        "0,1,1,false",
        "0,2,1,false",
        "1,2,1,false",
        "1,2,5,true",
        "1,5,5,true",
        "1,8,5,false",
        "10,8,1,true",
        "10,10,1,true",
        "10,12,1,false"
    })
    void shouldCheckMemberIsActive(
        final long timeOfLastAppendPositionNs,
        final long nowNs,
        final long timeoutNs,
        final boolean expected)
    {
        final ClusterMember member = newMember(0).timeOfLastAppendPositionNs(timeOfLastAppendPositionNs);
        assertEquals(expected, member.isActive(nowNs, timeoutNs));
    }

    @Test
    void shouldRankClusterStart()
    {
        assertThat(quorumPosition(members, rankedPositions, 0, 10), is(0L));
    }

    @ParameterizedTest
    @CsvSource({
        "0,0,0,0",
        "123,0,0,0",
        "123,123,0,123",
        "123,123,123,123",
        "0,123,123,123",
        "0,0,123,0",
        "0,123,200,123",
        "5,3,1,3",
        "5,1,3,3",
        "1,3,5,3",
        "1,5,3,3",
        "3,1,5,3",
        "3,5,1,3"
    })
    void shouldDetermineQuorumPosition(
        final long member0LogPosition,
        final long member1LogPosition,
        final long member2LogPosition,
        final long expectedQuorumPosition)
    {
        members[0].logPosition(member0LogPosition);
        members[1].logPosition(member1LogPosition);
        members[2].logPosition(member2LogPosition);

        final long quorumPosition = quorumPosition(members, rankedPositions, 0, 10);
        assertEquals(expectedQuorumPosition, quorumPosition);
    }

    @ParameterizedTest
    @CsvSource({
        "0,0,0,0,0,0,0,0,0,0,0,10,0",
        "1,0,2,0,3,0,4,0,5,0,5,10,3",
        "1,0,2,0,3,0,2,0,5,0,5,10,2",
        "1,1,2,0,3,1,2,0,5,1,5,5,1",
        "5,1,2,0,3,1,2,0,1,1,5,5,1",
        "5,0,2,1,3,0,2,1,1,0,5,5,0",
        "5,0,2,1,3,0,2,1,1,1,5,5,1",
        "5,0,2,1,3,1,2,1,1,0,5,5,2",
        "5,0,2,2,3,3,2,1,1,5,8,7,1",
        "5,0,2,0,3,0,2,0,1,0,8,7,0",
        "5,5,2,0,3,0,2,0,1,0,8,7,0",
        "5,5,2,5,3,0,2,0,1,0,8,7,0",
        "5,5,2,5,3,5,2,0,1,0,8,7,2",
        "4,5,2,5,4,5,2,0,1,0,8,7,2",
        "4,5,2,5,4,5,2,0,3,3,8,7,3",
    })
    void shouldOnlyConsiderActiveNodesWhenDeterminingQuorumPosition(
        final long member0LogPosition,
        final long member0Timestamp,
        final long member1LogPosition,
        final long member1Timestamp,
        final long member2LogPosition,
        final long member2Timestamp,
        final long member3LogPosition,
        final long member3Timestamp,
        final long member4LogPosition,
        final long member4Timestamp,
        final long nowNs,
        final long timeoutNs,
        final long expectedQuorumPosition)
    {
        final ClusterMember[] clusterMembers = new ClusterMember[]
        {
            newMember(0, 0, member0LogPosition, member0Timestamp),
            newMember(1, 0, member1LogPosition, member1Timestamp),
            newMember(2, 0, member2LogPosition, member2Timestamp),
            newMember(3, 0, member3LogPosition, member3Timestamp),
            newMember(4, 0, member4LogPosition, member4Timestamp)
        };
        final long[] positions = new long[quorumThreshold(clusterMembers.length)];

        final long quorumPosition = quorumPosition(clusterMembers, positions, nowNs, timeoutNs);
        assertEquals(expectedQuorumPosition, quorumPosition);
    }

    @Test
    void isUnanimousCandidateReturnFalseIfThereIsAMemberWithoutLogPosition()
    {
        final int gracefulClosedLeaderId = NULL_VALUE;
        final ClusterMember candidate = newMember(4, 100, 1000, 0);
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(1, 2, 100, 0),
            newMember(2, 8, NULL_POSITION, 0),
            newMember(3, 1, 1, 0)
        };

        assertFalse(isUnanimousCandidate(members, candidate, gracefulClosedLeaderId));
    }

    @Test
    void isUnanimousCandidateReturnFalseIfThereIsAMemberWithMoreUpToDateLog()
    {
        final int gracefulClosedLeaderId = NULL_VALUE;
        final ClusterMember candidate = newMember(4, 10, 800, 0);
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(1, 2, 100, 0),
            newMember(2, 8, 6, 0),
            newMember(3, 11, 1000, 0)
        };

        assertFalse(isUnanimousCandidate(members, candidate, gracefulClosedLeaderId));
    }

    @Test
    void isUnanimousCandidateReturnFalseIfLeaderClosesGracefully()
    {
        final int gracefulClosedLeaderId = 1;
        final ClusterMember candidate = newMember(2, 2, 100, 0);
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(1, 2, 100, 0),
            newMember(2, 2, 100, 0),
        };

        assertFalse(isUnanimousCandidate(members, candidate, gracefulClosedLeaderId));
    }

    @Test
    void isUnanimousCandidateReturnTrueIfTheCandidateHasTheMostUpToDateLog()
    {
        final int gracefulClosedLeaderId = NULL_VALUE;
        final ClusterMember candidate = newMember(2, 10, 800, 0);
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(10, 2, 100, 0),
            newMember(20, 8, 6, 0),
            newMember(30, 10, 800, 0)
        };

        assertTrue(isUnanimousCandidate(members, candidate, gracefulClosedLeaderId));
    }

    @Test
    void isQuorumCandidateReturnFalseWhenQuorumIsNotReached()
    {
        final ClusterMember candidate = newMember(2, 10, 800, 0);
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(10, 2, 100, 0),
            newMember(20, 18, 600, 0),
            newMember(30, 10, 800, 0),
            newMember(40, 19, 800, 0),
            newMember(50, 10, 1000, 0),
        };

        assertFalse(isQuorumCandidate(members, candidate));
    }

    @Test
    void isQuorumCandidateReturnTrueWhenQuorumIsReached()
    {
        final ClusterMember candidate = newMember(2, 10, 800, 0);
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(10, 2, 100, 0),
            newMember(20, 18, 600, 0),
            newMember(30, 10, 800, 0),
            newMember(40, 9, 800, 0),
            newMember(50, 10, 700, 0)
        };

        assertTrue(isQuorumCandidate(members, candidate));
    }

    @Test
    void isQuorumLeaderReturnsTrueWhenQuorumIsReached()
    {
        final int candidateTermId = -5;
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(1).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(2).candidateTermId(candidateTermId * 2).vote(Boolean.FALSE),
            newMember(3).candidateTermId(candidateTermId).vote(null),
            newMember(4).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(5).candidateTermId(candidateTermId).vote(Boolean.TRUE)
        };

        assertTrue(isQuorumLeader(members, candidateTermId));
    }

    @Test
    void isQuorumLeaderReturnsFalseIfAtLeastOneNegativeVoteIsDetected()
    {
        final int candidateTermId = 8;
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(1).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(2).candidateTermId(candidateTermId).vote(Boolean.FALSE),
            newMember(3).candidateTermId(candidateTermId).vote(Boolean.TRUE)
        };

        assertFalse(isQuorumLeader(members, candidateTermId));
    }

    @Test
    void isQuorumLeaderReturnsFalseWhenQuorumIsNotReached()
    {
        final int candidateTermId = 2;
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(1).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(2).candidateTermId(candidateTermId).vote(null),
            newMember(3).candidateTermId(candidateTermId + 5).vote(Boolean.TRUE)
        };

        assertFalse(isQuorumLeader(members, candidateTermId));
    }

    @Test
    void isUnanimousLeaderReturnsFalseIfThereIsAtLeastOneNegativeVoteForAGivenCandidateTerm()
    {
        final int candidateTermId = 42;
        final int gracefulClosedLeaderId = NULL_VALUE;
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(1).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(2).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(3).candidateTermId(candidateTermId).vote(Boolean.FALSE)
        };

        assertFalse(isUnanimousLeader(members, candidateTermId, gracefulClosedLeaderId));
    }

    @Test
    void isUnanimousLeaderReturnsFalseIfLeaderClosesGracefully()
    {
        final int candidateTermId = 7;
        final int gracefulClosedLeaderId = 1;
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(1).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(2).candidateTermId(candidateTermId).vote(Boolean.TRUE),
        };

        assertFalse(isUnanimousLeader(members, candidateTermId, gracefulClosedLeaderId));
    }

    @Test
    void isUnanimousLeaderReturnsFalseIfNotAllNodesVotedPositively()
    {
        final int candidateTermId = 2;
        final int gracefulClosedLeaderId = NULL_VALUE;
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(1).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(2).candidateTermId(candidateTermId).vote(null),
            newMember(3).candidateTermId(candidateTermId).vote(Boolean.TRUE)
        };

        assertFalse(isUnanimousLeader(members, candidateTermId, gracefulClosedLeaderId));
    }

    @Test
    void isUnanimousLeaderReturnsFalseIfNotAllNodesHadTheExpectedCandidateTermId()
    {
        final int candidateTermId = 2;
        final int gracefulClosedLeaderId = NULL_VALUE;
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(1).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(2).candidateTermId(candidateTermId + 1).vote(Boolean.TRUE),
            newMember(3).candidateTermId(candidateTermId).vote(Boolean.TRUE)
        };

        assertFalse(isUnanimousLeader(members, candidateTermId, gracefulClosedLeaderId));
    }

    @Test
    void isUnanimousLeaderReturnsTrueIfAllNodesVotedWithTrue()
    {
        final int candidateTermId = 42;
        final int gracefulClosedLeaderId = NULL_VALUE;
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(1).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(2).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(3).candidateTermId(candidateTermId).vote(Boolean.TRUE)
        };

        assertTrue(isUnanimousLeader(members, candidateTermId, gracefulClosedLeaderId));
    }

    @ParameterizedTest
    @CsvSource({
        "5,1000,3,999999,1",
        "-100,99999,4,0,-1",
        "42,371239192371239,42,1001,1",
        "3,-777,3,273291846723894,-1",
        "1,1024,1,1024,0",
    })
    void compareLogReturnsResult(
        final long lhsLogLeadershipTermId,
        final long lhsLogPosition,
        final long rhsLogLeadershipTermId,
        final long rhsLogPosition,
        final int expectedResult)
    {
        assertEquals(
            expectedResult,
            compareLog(lhsLogLeadershipTermId, lhsLogPosition, rhsLogLeadershipTermId, rhsLogPosition));
        assertEquals(expectedResult, compareLog(
            newMember(5, lhsLogLeadershipTermId, lhsLogPosition, 0),
            newMember(100, rhsLogLeadershipTermId, rhsLogPosition, 0)));
    }

    @Test
    void shouldNotVoteIfHasNoPosition()
    {
        final ClusterMember member = newMember(1, 0, NULL_POSITION, 0);
        final ClusterMember candidate = newMember(2, 0, 100, 0);
        assertFalse(member.willVoteFor(candidate));
    }

    @Test
    void shouldNotVoteIfHasMoreLog()
    {
        final ClusterMember member = newMember(1, 0, 500, 0);
        final ClusterMember candidate = newMember(2, 0, 100, 0);
        assertFalse(member.willVoteFor(candidate));
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, 900, 1024 })
    void shouldVoteIfHasLessOrTheSameAmountOfLog(final long logPosition)
    {
        final ClusterMember member = newMember(1, 0, logPosition, 0);
        final ClusterMember candidate = newMember(2, 0, 1024, 0);
        assertTrue(member.willVoteFor(candidate));
    }

    @Test
    void shouldReturnFalseIfNotActiveWhenDoingPositionChecks()
    {
        final long leadershipTermId = 42;
        final long logPosition = 500;
        final ClusterMember member = newMember(1, leadershipTermId, logPosition, 0);
        assertFalse(member.hasReachedPosition(leadershipTermId, logPosition, 5, 3));
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, 10 })
    void shouldReturnFalseIfLeadershipTermIdDoesNotMatch(final long leadershipTermId)
    {
        final long logPosition = 500;
        final ClusterMember member = newMember(1, 6, logPosition, 0);
        assertFalse(member.hasReachedPosition(leadershipTermId, logPosition, 5, 10));
    }

    @Test
    void shouldReturnFalseIfLogPositionIsLessThan()
    {
        final long leadershipTermId = 6;
        final long logPosition = 500;
        final ClusterMember member = newMember(1, leadershipTermId, 100, 0);
        assertFalse(member.hasReachedPosition(leadershipTermId, logPosition, 5, 10));
    }

    @ParameterizedTest
    @ValueSource(longs = { 100, Long.MAX_VALUE })
    void shouldReturnTrueIfLogPositionIsEqualOrGreaterThan(final long logPosition)
    {
        final long leadershipTermId = 42;
        final ClusterMember member = newMember(1, leadershipTermId, logPosition, 10);
        assertTrue(member.hasReachedPosition(leadershipTermId, 100, 12, 5));
    }

    @Test
    void hasQuorumAtPositionReturnFalseIfNotAQuorum()
    {
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(10, 2, 100, 0),
            newMember(20, 18, 600, 0),
            newMember(30, 10, 800, 0),
            newMember(40, 19, 800, 0),
            newMember(50, 10, 1000, 0),
        };

        assertFalse(hasQuorumAtPosition(members, 10, 800, 0, 10));
    }

    @Test
    void hasQuorumAtPositionReturnTrueIfQuorumIsAtPosition()
    {
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(10, 2, 100, 0),
            newMember(20, 10, 600, 0),
            newMember(30, 10, 800, 0),
            newMember(40, 19, 800, 0),
            newMember(50, 10, 1000, 0),
        };

        assertTrue(hasQuorumAtPosition(members, 10, 600, 5, 10));
    }

    private static ClusterMember newMember(
        final int id, final long leadershipTermId, final long logPosition, final long timeOfLastAppendPositionNs)
    {
        return newMember(id)
            .leadershipTermId(leadershipTermId)
            .logPosition(logPosition)
            .timeOfLastAppendPositionNs(timeOfLastAppendPositionNs);
    }

    private static ClusterMember newMember(final int id)
    {
        return new ClusterMember(id, null, null, null, null, null, null);
    }
}
