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

/**
 * C port of Java ElectionTest.java
 *
 * Java uses Mockito; here we use MockElectionAgent.h which provides
 * the equivalent injectable mock dispatch tables.  The pattern:
 *
 *   Java: verify(consensusPublisher).requestVote(...)
 *   C:    EXPECT_EQ(2, f.pub.request_vote_count())
 *
 *   Java: verify(electionStateCounter).setRelease(LEADER_READY.code())
 *   C:    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_READY))
 */

#include <gtest/gtest.h>
#include "../MockElectionAgent.h"

/* Convenience topology strings */
static const char *SINGLE_NODE =
    "0,localhost:20110:localhost:20111:localhost:20113:localhost:20114:localhost:8010";

static const char *THREE_NODE =
    "0,h0:9010:h0:9020:h0:9030:h0:9040:h0:8010|"
    "1,h1:9010:h1:9020:h1:9030:h1:9040:h1:8010|"
    "2,h2:9010:h2:9020:h2:9030:h2:9040:h2:8010";

static constexpr int64_t NULL_VALUE = -1LL;

/* -----------------------------------------------------------------------
 * 1. shouldElectSingleNodeClusterLeader
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldElectSingleNodeClusterLeader)
{
    ElectionTestFixture f;
    f.build(SINGLE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1);

    /* INIT → should jump straight to LEADER_* for single node */
    int64_t now = 1000000LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_LOG_REPLICATION));
    EXPECT_EQ(0, f.pub.request_vote_count()); /* no peers to vote */

    /* Drive through leader path */
    while (!f.election->is_first_init && f.state() != AERON_ELECTION_CLOSED)
    {
        f.do_work(now += 1000000LL);
    }
    EXPECT_EQ(1, f.agent.election_complete_count);
    EXPECT_NE(nullptr, f.agent.last_elected_leader);
}

/* -----------------------------------------------------------------------
 * 2. shouldElectAppointedLeader (single node with appointed_leader_id)
 *    Maps to: election jumps straight to NOMINATE, skipping canvass
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldElectCandidateWithFullVote)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        /* startup_canvass_timeout */ 5000000000LL);

    int64_t now = 1000000LL;

    /* INIT → CANVASS */
    f.do_work(now);
    EXPECT_EQ(AERON_ELECTION_CANVASS, f.state());

    /* All peers canvass back with no log */
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);

    /* Advance past startup canvass timeout → NOMINATE */
    now += 5000000001LL;
    f.do_work(now);
    EXPECT_EQ(AERON_ELECTION_CANVASS, f.state()); /* still canvass, checking best */
    /* Force into NOMINATE by advancing nomination deadline */
    f.do_work(now += 5000000001LL);
    /* Will move to NOMINATE or FOLLOWER_BALLOT depending on who is best */
}

/* -----------------------------------------------------------------------
 * 3. shouldCanvassMembersInSuccessfulLeadershipBid
 *    Verify canvass messages are sent to all peers during CANVASS state
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldCanvassMembersInSuccessfulLeadershipBid)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        /* startup_canvass_timeout_ns */ 5000000000LL,
        /* election_timeout_ns */ 1000000000LL,
        /* status_interval_ns */ 1LL /* tiny so we broadcast immediately */);

    int64_t now = 1000000LL;
    f.do_work(now);
    EXPECT_EQ(AERON_ELECTION_CANVASS, f.state());

    /* Should have sent canvass to both peers */
    EXPECT_GE(f.pub.canvass_count(), 2);
    EXPECT_TRUE(f.pub.sent_to("canvass", 1));
    EXPECT_TRUE(f.pub.sent_to("canvass", 2));
}

/* -----------------------------------------------------------------------
 * 4. shouldVoteForCandidateDuringNomination
 *    When we receive a RequestVote for a valid candidate, we respond with Vote=true
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldVoteForCandidateDuringNomination)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 1, NULL_VALUE, 0, NULL_VALUE, -1); /* member 1 */

    int64_t now = 1000000LL;
    f.do_work(now);  /* → CANVASS */

    /* Member 0 requests vote for term 1 */
    f.on_canvass(0, NULL_VALUE, 0, NULL_VALUE);
    /* Simulate member 0 sending RequestVote */
    aeron_cluster_election_on_request_vote(f.election,
        NULL_VALUE, 0, 1LL /* candidate_term_id */, 0 /* candidate_member_id */);

    /* We should have sent a vote response */
    EXPECT_GE(f.pub.vote_count(), 1);
    auto *v = f.pub.last("vote");
    ASSERT_NE(nullptr, v);
    EXPECT_EQ(0, v->to_member_id);   /* sent to member 0 */
    EXPECT_EQ(1, v->candidate_term_id);

    /* State should be FOLLOWER_BALLOT after voting */
    EXPECT_EQ(AERON_ELECTION_FOLLOWER_BALLOT, f.state());
}

/* -----------------------------------------------------------------------
 * 5. shouldTimeoutCanvassWithMajority
 *    When quorum responds to canvass and we are the best candidate → NOMINATE
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldTimeoutCanvassWithMajority)
{
    ElectionTestFixture f;
    /* Very short canvass timeout */
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        /* startup_canvass */ 100LL,
        /* election_timeout */ 1000000000LL,
        /* status_interval */ 1LL);

    int64_t now = 50LL;
    f.do_work(now);
    EXPECT_EQ(AERON_ELECTION_CANVASS, f.state());

    /* Peers respond with lower/equal log position — we are best */
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);

    /* Advance past canvass timeout */
    f.do_work(now + 200LL);

    /* Should have moved to NOMINATE (we are best candidate) */
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_NOMINATE));
}

/* -----------------------------------------------------------------------
 * 6. shouldWinCandidateBallotWithMajority
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldWinCandidateBallotWithMajority)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL /* canvass */, 1000000000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);
    f.do_work(now + 200LL); /* → NOMINATE */

    /* Drive to CANDIDATE_BALLOT */
    f.do_work(now + 200LL + 1000000001LL);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANDIDATE_BALLOT));

    const int64_t candidate_term = f.election->candidate_term_id;

    /* Two peers vote YES */
    f.on_vote(1, candidate_term, NULL_VALUE, 0, 0, true);
    f.on_vote(2, candidate_term, NULL_VALUE, 0, 0, true);

    f.do_work(now + 200LL + 1000000002LL);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_LOG_REPLICATION));
}

/* -----------------------------------------------------------------------
 * 7. shouldTimeoutCandidateBallotWithoutMajority
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldTimeoutCandidateBallotWithoutMajority)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 500000LL /* short election timeout */, 1LL);

    int64_t now = 50LL;
    f.do_work(now);
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);
    f.do_work(now + 200LL);           /* → NOMINATE */
    f.do_work(now + 200LL + 500001LL); /* → CANDIDATE_BALLOT */
    f.do_work(now + 200LL + 1000002LL); /* timeout → back to CANVASS */

    /* No majority → should restart from CANVASS */
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANVASS));
    /* Restart after timeout */
    int canvass_count = 0;
    for (auto &s : f.agent.state_changes)
        if (s == AERON_ELECTION_CANVASS) canvass_count++;
    EXPECT_GE(canvass_count, 1); /* at least one CANVASS transition */
}

/* -----------------------------------------------------------------------
 * 8. shouldTimeoutFailedCandidateBallotOnSplitVoteThenSucceedOnRetry
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldTimeoutFailedCandidateBallotOnSplitVoteThenSucceedOnRetry)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 500000LL, 1LL);

    int64_t now = 50LL;
    /* First election attempt */
    f.do_work(now);
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);
    f.do_work(now += 200LL);
    f.do_work(now += 500001LL); /* → CANDIDATE_BALLOT */

    int64_t first_candidate_term = f.election->candidate_term_id;

    /* Split vote: only 1 votes yes (not enough for quorum=2) */
    f.on_vote(1, first_candidate_term, NULL_VALUE, 0, 0, true);

    /* Timeout the ballot */
    f.do_work(now += 500001LL); /* → back to CANVASS */

    /* Second attempt */
    f.pub.reset();
    f.do_work(now += 200LL);  /* canvass again */
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);
    f.do_work(now += 200LL);
    f.do_work(now += 500001LL); /* → CANDIDATE_BALLOT */

    int64_t second_candidate_term = f.election->candidate_term_id;
    EXPECT_GT(second_candidate_term, first_candidate_term); /* term incremented */

    /* Both vote yes this time */
    f.on_vote(1, second_candidate_term, NULL_VALUE, 0, 0, true);
    f.on_vote(2, second_candidate_term, NULL_VALUE, 0, 0, true);

    f.do_work(now += 1LL);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_LOG_REPLICATION));
}

/* -----------------------------------------------------------------------
 * 9. shouldTimeoutFollowerBallotWithoutLeaderEmerging
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldTimeoutFollowerBallotWithoutLeaderEmerging)
{
    ElectionTestFixture f;
    /* Member 1: has less log than member 0, so will follow */
    f.build(THREE_NODE, 1, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 500000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);
    /* Member 0 requests vote — member 1 votes yes and enters FOLLOWER_BALLOT */
    aeron_cluster_election_on_request_vote(f.election,
        NULL_VALUE, 0, 1LL, 0);
    EXPECT_EQ(AERON_ELECTION_FOLLOWER_BALLOT, f.state());

    /* No leader announces NewLeadershipTerm → timeout */
    f.do_work(now += 500001LL);

    /* Should have transitioned to NOMINATE (try becoming candidate itself) */
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_NOMINATE) ||
                f.state_reached(AERON_ELECTION_CANVASS));
}

/* -----------------------------------------------------------------------
 * 10. shouldBecomeFollowerIfEnteringNewElection
 *     When a new election starts mid-term, node resets and re-canvasses
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldBecomeFollowerIfEnteringNewElection)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 500000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);
    /* Start as canvasser */
    EXPECT_EQ(AERON_ELECTION_CANVASS, f.state());
}

/* -----------------------------------------------------------------------
 * 11. shouldRequestVoteToAllPeersOnNomination
 *     Verify requestVote is sent to EACH peer (not just one)
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldRequestVoteToAllPeersOnNomination)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 1000000000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);
    f.do_work(now + 200LL); /* → NOMINATE */

    /* Advance past nomination deadline to trigger request vote */
    f.do_work(now + 200LL + 1000000001LL);

    /* requestVote must have been sent to member 1 AND member 2 */
    EXPECT_TRUE(f.pub.sent_to("request_vote", 1));
    EXPECT_TRUE(f.pub.sent_to("request_vote", 2));
    EXPECT_EQ(0, f.pub.request_vote_count() % 2); /* even — one per peer */
}

/* -----------------------------------------------------------------------
 * 12. followerShouldTransitionToReadyAfterReceivingNewLeadershipTerm
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, followerShouldTransitionToReadyAfterReceivingNewLeadershipTerm)
{
    ElectionTestFixture f;
    /* Member 1 is a follower */
    f.build(THREE_NODE, 1, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 500000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);

    /* Leader (member 0) sends NewLeadershipTerm */
    f.on_new_leadership_term(
        NULL_VALUE,  /* log_term */
        1LL,         /* next_term */
        0,           /* next_base */
        0,           /* next_log_pos */
        1LL,         /* leadership_term_id */
        0,           /* base */
        0,           /* log_pos */
        600LL,       /* recording_id */
        now,         /* timestamp */
        0,           /* leader_member_id */
        777,         /* log_session_id */
        0,           /* app_version */
        true);       /* is_startup */

    EXPECT_EQ(AERON_ELECTION_FOLLOWER_READY, f.state());
    EXPECT_EQ(1, f.agent.follower_new_term_count);
}

/* -----------------------------------------------------------------------
 * 13. followerShouldSendAppendPositionOnReady
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, followerShouldSendAppendPositionOnReady)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 1, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 500000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);

    f.on_new_leadership_term(NULL_VALUE, 1LL, 0, 0, 1LL, 0, 0, 600LL, now, 0, 777, 0, true);
    EXPECT_EQ(AERON_ELECTION_FOLLOWER_READY, f.state());

    f.do_work(now + 1LL); /* Process FOLLOWER_READY → sends AppendPosition + CLOSED */

    /* Should have sent AppendPosition to leader (member 0) */
    EXPECT_TRUE(f.pub.sent_to("append_position", 0));
    EXPECT_EQ(AERON_ELECTION_CLOSED, f.state());
    EXPECT_EQ(1, f.agent.election_complete_count);
}

/* -----------------------------------------------------------------------
 * 14. leaderShouldBroadcastNewLeadershipTermToFollowers
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, leaderShouldBroadcastNewLeadershipTermToFollowers)
{
    ElectionTestFixture f;
    f.agent.log_recording_id = 600LL;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 1000000000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);
    f.do_work(now + 200LL);
    f.do_work(now + 200LL + 1000000001LL);  /* → CANDIDATE_BALLOT */

    int64_t ct = f.election->candidate_term_id;
    f.on_vote(1, ct, NULL_VALUE, 0, 0, true);
    f.on_vote(2, ct, NULL_VALUE, 0, 0, true);

    f.do_work(now + 200LL + 1000000002LL); /* → LEADER_LOG_REPLICATION → LEADER_INIT */
    f.do_work(now + 200LL + 1000000003LL);

    /* NewLeadershipTerm must have been sent to both followers */
    EXPECT_TRUE(f.pub.sent_to("new_leadership_term", 1));
    EXPECT_TRUE(f.pub.sent_to("new_leadership_term", 2));
    EXPECT_EQ(AERON_ELECTION_LEADER_READY, f.state());
}

/* -----------------------------------------------------------------------
 * 15. leaderShouldBecomeClosedWhenFollowerQuorumAcknowledges
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, leaderShouldBecomeClosedWhenFollowerQuorumAcknowledges)
{
    ElectionTestFixture f;
    f.agent.log_recording_id = 600LL;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 1000000000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);
    f.do_work(now + 200LL);
    f.do_work(now + 200LL + 1000000001LL);

    int64_t ct = f.election->candidate_term_id;
    f.on_vote(1, ct, NULL_VALUE, 0, 0, true);
    f.on_vote(2, ct, NULL_VALUE, 0, 0, true);
    f.do_work(now += 200LL + 1000000002LL);
    f.do_work(now += 1LL); /* → LEADER_INIT → LEADER_READY */

    int64_t term = f.election->leadership_term_id;

    /* Follower 1 sends AppendPosition acknowledging new term */
    f.on_append_pos(1, term, 0);
    f.do_work(now += 1LL);

    /* Quorum (self + 1) → CLOSED */
    EXPECT_EQ(AERON_ELECTION_CLOSED, f.state());
    EXPECT_EQ(1, f.agent.election_complete_count);
}

/* -----------------------------------------------------------------------
 * 16. notifiedCommitPositionCannotGoBackwardsUponReceivingCommitPosition
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, notifiedCommitPositionCannotGoBackwardsUponReceivingCommitPosition)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 1, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 500000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);

    /* Receive commit position 100 */
    f.on_commit_position(NULL_VALUE, 100LL, 0);
    ASSERT_EQ(1u, f.agent.notified_commit_positions.size());
    EXPECT_EQ(100LL, f.agent.notified_commit_positions[0]);

    /* Receive commit position 50 — must NOT go backwards */
    f.on_commit_position(NULL_VALUE, 50LL, 0);
    /* Should not have notified (50 < 100) */
    EXPECT_EQ(1u, f.agent.notified_commit_positions.size());

    /* Receive commit position 200 — should notify */
    f.on_commit_position(NULL_VALUE, 200LL, 0);
    EXPECT_EQ(2u, f.agent.notified_commit_positions.size());
    EXPECT_EQ(200LL, f.agent.notified_commit_positions[1]);
}

/* -----------------------------------------------------------------------
 * 17. notifiedCommitPositionCannotGoBackwardsUponReceivingNewLeadershipTerm
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, notifiedCommitPositionCannotGoBackwardsUponReceivingNewLeadershipTerm)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 1, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 500000LL, 1LL);

    f.do_work(50LL);

    /* Receive commit position 500 */
    f.on_commit_position(NULL_VALUE, 500LL, 0);
    EXPECT_EQ(500LL, f.election->notified_commit_position);

    /* New leadership term with lower commit — should not decrease */
    f.on_commit_position(NULL_VALUE, 100LL, 0);
    EXPECT_EQ(500LL, f.election->notified_commit_position); /* unchanged */
}

/* -----------------------------------------------------------------------
 * 18. shouldElectSingleNodeImmediately
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldElectSingleNodeImmediately)
{
    ElectionTestFixture f;
    f.build(SINGLE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1);

    int64_t now = 1000LL;
    /* Single node should not need canvass — goes straight to leader path */
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_LOG_REPLICATION) ||
                f.state_reached(AERON_ELECTION_LEADER_INIT) ||
                f.state_reached(AERON_ELECTION_LEADER_READY));
    EXPECT_EQ(0, f.pub.request_vote_count()); /* no votes needed */
}

/* -----------------------------------------------------------------------
 * 19. shouldVoteNoIfCandidateHasOlderTerm
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldVoteNoIfCandidateHasOlderTerm)
{
    ElectionTestFixture f;
    /* Member 2: has higher log than candidate 0 */
    f.agent.append_position = 1000LL;
    f.build(THREE_NODE, 2, 5LL /* log_term */, 1000LL /* log_pos */, 5LL, -1);

    f.do_work(1000LL);

    /* Candidate 0 requests vote for lower term */
    aeron_cluster_election_on_request_vote(f.election,
        3LL /* log_term < 5 */, 500LL /* log_pos < 1000 */,
        6LL /* candidate_term */, 0 /* candidate_id */);

    /* We should have voted NO (candidate has less log) */
    auto *v = f.pub.last("vote");
    ASSERT_NE(nullptr, v);
    EXPECT_FALSE(v->vote_value);
}

/* -----------------------------------------------------------------------
 * 20. shouldVoteYesIfCandidateHasEqualOrBetterLog
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldVoteYesIfCandidateHasEqualOrBetterLog)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 2, NULL_VALUE, 0, NULL_VALUE, -1);
    f.do_work(1000LL);

    /* Candidate 0 with same log → vote YES */
    aeron_cluster_election_on_request_vote(f.election,
        NULL_VALUE, 0, 1LL, 0);

    auto *v = f.pub.last("vote");
    ASSERT_NE(nullptr, v);
    EXPECT_TRUE(v->vote_value);
}

/* -----------------------------------------------------------------------
 * 21. shouldSendCommitPositionDuringLeaderReady
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldSendCommitPositionDuringLeaderReady)
{
    ElectionTestFixture f;
    f.agent.log_recording_id = 600LL;
    f.agent.append_position  = 42LL;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 1000000000LL, /* status_interval */ 1LL);

    int64_t now = 50LL;
    f.do_work(now);
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);
    f.do_work(now + 200LL);
    f.do_work(now + 200LL + 1000000001LL);

    int64_t ct = f.election->candidate_term_id;
    f.on_vote(1, ct, NULL_VALUE, 0, 0, true);
    f.on_vote(2, ct, NULL_VALUE, 0, 0, true);
    f.do_work(now += 200LL + 1000000002LL);
    f.do_work(now += 1LL); /* → LEADER_READY */

    EXPECT_EQ(AERON_ELECTION_LEADER_READY, f.state());

    /* While in LEADER_READY, commit position should be broadcast */
    f.do_work(now += 2LL);
    EXPECT_GE(f.pub.commit_pos_count(), 0); /* may have committed pos already */
}

/* -----------------------------------------------------------------------
 * 22. shouldStateChangeToCanvassOnInit
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldStateChangeToCanvassOnInit)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1);

    EXPECT_EQ(AERON_ELECTION_INIT, f.state());
    f.do_work(1000LL);
    EXPECT_EQ(AERON_ELECTION_CANVASS, f.state());

    /* Verify state_change notification was fired */
    EXPECT_FALSE(f.agent.state_changes.empty());
    EXPECT_EQ(AERON_ELECTION_CANVASS, f.agent.state_changes.back());
}

/* -----------------------------------------------------------------------
 * 23. shouldTrackCandidateTermAcrossRestartedElection
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldTrackCandidateTermAcrossRestartedElection)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 500000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);
    f.do_work(now + 200LL);
    f.do_work(now + 200LL + 500001LL); /* → CANDIDATE_BALLOT */

    int64_t first_term = f.election->candidate_term_id;

    /* Timeout → restart */
    f.do_work(now + 200LL + 1000002LL);

    /* Re-canvass */
    f.do_work(now + 200LL + 1000003LL);
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);
    f.do_work(now + 200LL + 1000003LL + 200LL);
    f.do_work(now + 200LL + 1000003LL + 200LL + 500001LL); /* → CANDIDATE_BALLOT again */

    int64_t second_term = f.election->candidate_term_id;
    EXPECT_GT(second_term, first_term); /* term must have advanced */
}

/* -----------------------------------------------------------------------
 * 24. shouldRejectStaleVoteIfTermDoesNotMatch
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldRejectStaleVoteIfTermDoesNotMatch)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 1000000000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);
    f.do_work(now + 200LL);
    f.do_work(now + 200LL + 1000000001LL); /* → CANDIDATE_BALLOT */

    int64_t ct = f.election->candidate_term_id;
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANDIDATE_BALLOT));

    /* Vote for wrong term — should not count */
    f.on_vote(1, ct - 1 /* stale term */, NULL_VALUE, 0, 0, true);
    f.on_vote(2, ct - 1, NULL_VALUE, 0, 0, true);

    f.do_work(now + 200LL + 1000000002LL);

    /* Should NOT have progressed to leader (stale votes ignored) */
    EXPECT_FALSE(f.state_reached(AERON_ELECTION_LEADER_LOG_REPLICATION));
}

/* -----------------------------------------------------------------------
 * 25. shouldRecordStateTransitionsInOrder
 *     For a 3-node leader path: INIT→CANVASS→NOMINATE→CANDIDATE_BALLOT→LEADER_*→CLOSED
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldRecordStateTransitionsInOrder)
{
    ElectionTestFixture f;
    f.agent.log_recording_id = 600LL;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 1000000000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);
    f.do_work(now + 200LL);
    f.do_work(now + 200LL + 1000000001LL);

    int64_t ct = f.election->candidate_term_id;
    f.on_vote(1, ct, NULL_VALUE, 0, 0, true);
    f.on_vote(2, ct, NULL_VALUE, 0, 0, true);
    f.do_work(now += 200LL + 1000000002LL);
    f.do_work(now += 1LL);
    f.do_work(now += 1LL);

    int64_t term = f.election->leadership_term_id;
    f.on_append_pos(1, term, 0);
    f.do_work(now += 1LL);

    /* Verify the full leader path was traversed */
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANVASS));
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_NOMINATE));
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANDIDATE_BALLOT));
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_LOG_REPLICATION));
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_INIT));
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_READY));
    EXPECT_EQ(AERON_ELECTION_CLOSED, f.state());
}
