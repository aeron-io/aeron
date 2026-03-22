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

#include <gtest/gtest.h>
#include <cstring>
#include <cstdlib>

#include "aeron_cluster_recording_log.h"
#include "aeron_cluster_member.h"
#include "aeron_cluster_timer_service.h"
#include "aeron_cluster_pending_message_tracker.h"
#include "aeron_consensus_module_configuration.h"

/* -----------------------------------------------------------------------
 * RecordingLog unit tests
 * ----------------------------------------------------------------------- */
class RecordingLogTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        m_dir = "/tmp/aeron_cluster_test_recording_log";
        std::system(("rm -rf " + m_dir + " && mkdir -p " + m_dir).c_str());
    }

    void TearDown() override
    {
        std::system(("rm -rf " + m_dir).c_str());
    }

    std::string m_dir;
};

TEST_F(RecordingLogTest, shouldCreateAndReloadTermEntry)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(log, 42, 0, 0, 1000));
    ASSERT_EQ(1, log->entry_count);

    aeron_cluster_recording_log_entry_t *e = aeron_cluster_recording_log_find_last_term(log);
    ASSERT_NE(nullptr, e);
    EXPECT_EQ(42, e->recording_id);
    EXPECT_EQ(0,  e->leadership_term_id);
    EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM, e->entry_type);

    aeron_cluster_recording_log_close(log);

    /* Reload */
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
    EXPECT_EQ(1, log->entry_count);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldCommitLogPosition)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(log, 1, 0, 0, 1000));

    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(log, 0, 12345));

    aeron_cluster_recording_log_entry_t *e = aeron_cluster_recording_log_find_last_term(log);
    ASSERT_NE(nullptr, e);
    EXPECT_EQ(12345, e->log_position);

    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldAppendAndFindSnapshot)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(log, 1, 0, 0, 1000));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(log, 99, 0, 0, 500, 2000, -1));

    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(log, -1);
    ASSERT_NE(nullptr, snap);
    EXPECT_EQ(99, snap->recording_id);
    EXPECT_EQ(500, snap->log_position);
    EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT, snap->entry_type);

    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldCreateRecoveryPlan)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(log, 10, 0, 0, 1000));
    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(log, 0, 8000));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(log, 20, 0, 0, 7000, 1500, -1));

    aeron_cluster_recovery_plan_t *plan = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_create_recovery_plan(log, &plan, 1));
    ASSERT_NE(nullptr, plan);

    EXPECT_EQ(0,    plan->last_leadership_term_id);
    EXPECT_EQ(8000, plan->last_append_position);
    EXPECT_EQ(10,   plan->last_term_recording_id);
    EXPECT_EQ(1,    plan->snapshot_count);
    EXPECT_EQ(20,   plan->snapshots[0].recording_id);

    aeron_cluster_recovery_plan_free(plan);
    aeron_cluster_recording_log_close(log);
}

/* -----------------------------------------------------------------------
 * ClusterMember unit tests
 * ----------------------------------------------------------------------- */
TEST(ClusterMemberTest, shouldParseSingleMember)
{
    aeron_cluster_member_t *members = nullptr;
    int count = 0;

    ASSERT_EQ(0, aeron_cluster_members_parse(
        "0,localhost:20110:localhost:20111:localhost:20113:localhost:20114:localhost:8010",
        &members, &count));

    EXPECT_EQ(1, count);
    EXPECT_EQ(0, members[0].id);
    EXPECT_STREQ("localhost:20110", members[0].ingress_endpoint);
    EXPECT_STREQ("localhost:20111", members[0].consensus_endpoint);

    aeron_cluster_members_free(members, count);
}

TEST(ClusterMemberTest, shouldParseThreeMembers)
{
    aeron_cluster_member_t *members = nullptr;
    int count = 0;
    const char *topology =
        "0,h0:9010:h0:9020:h0:9030:h0:9040:h0:8010|"
        "1,h1:9010:h1:9020:h1:9030:h1:9040:h1:8010|"
        "2,h2:9010:h2:9020:h2:9030:h2:9040:h2:8010";

    ASSERT_EQ(0, aeron_cluster_members_parse(topology, &members, &count));
    EXPECT_EQ(3, count);
    EXPECT_EQ(0, members[0].id);
    EXPECT_EQ(1, members[1].id);
    EXPECT_EQ(2, members[2].id);

    aeron_cluster_members_free(members, count);
}

TEST(ClusterMemberTest, shouldComputeQuorumThreshold)
{
    EXPECT_EQ(2, aeron_cluster_member_quorum_threshold(3));
    EXPECT_EQ(3, aeron_cluster_member_quorum_threshold(5));
    EXPECT_EQ(4, aeron_cluster_member_quorum_threshold(7));
    EXPECT_EQ(1, aeron_cluster_member_quorum_threshold(1));
}

/* -----------------------------------------------------------------------
 * TimerService unit tests
 * ----------------------------------------------------------------------- */
static int64_t g_fired_id = -1;
static void timer_fired(void *clientd, int64_t correlation_id)
{
    g_fired_id = correlation_id;
}

TEST(TimerServiceTest, shouldScheduleAndFireTimer)
{
    aeron_cluster_timer_service_t *svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&svc, timer_fired, nullptr));

    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 42, 1000));
    EXPECT_EQ(0, aeron_cluster_timer_service_poll(svc, 999));
    g_fired_id = -1;
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(svc, 1001));
    EXPECT_EQ(42, g_fired_id);
    EXPECT_EQ(0, aeron_cluster_timer_service_timer_count(svc));

    aeron_cluster_timer_service_close(svc);
}

TEST(TimerServiceTest, shouldCancelTimer)
{
    aeron_cluster_timer_service_t *svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&svc, timer_fired, nullptr));

    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 10, 500));
    EXPECT_TRUE(aeron_cluster_timer_service_cancel(svc, 10));
    g_fired_id = -1;
    EXPECT_EQ(0, aeron_cluster_timer_service_poll(svc, 1000));
    EXPECT_EQ(-1, g_fired_id);

    aeron_cluster_timer_service_close(svc);
}

TEST(TimerServiceTest, shouldUpdateExistingTimer)
{
    aeron_cluster_timer_service_t *svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&svc, timer_fired, nullptr));

    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 7, 100));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 7, 2000));  /* update */
    g_fired_id = -1;
    EXPECT_EQ(0, aeron_cluster_timer_service_poll(svc, 500));
    EXPECT_EQ(-1, g_fired_id);
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(svc, 2001));
    EXPECT_EQ(7, g_fired_id);

    aeron_cluster_timer_service_close(svc);
}

/* -----------------------------------------------------------------------
 * PendingMessageTracker unit tests
 * ----------------------------------------------------------------------- */
TEST(PendingMessageTrackerTest, shouldAppendOnlyUncommittedMessages)
{
    aeron_cluster_pending_message_tracker_t tracker;
    aeron_cluster_pending_message_tracker_init(&tracker, 0, 1, 5, 4096);

    /* Session IDs <= 5 are already committed */
    EXPECT_FALSE(aeron_cluster_pending_message_tracker_should_append(&tracker, 3));
    EXPECT_FALSE(aeron_cluster_pending_message_tracker_should_append(&tracker, 5));
    EXPECT_TRUE (aeron_cluster_pending_message_tracker_should_append(&tracker, 6));
}

TEST(PendingMessageTrackerTest, shouldAdvanceOnAppend)
{
    aeron_cluster_pending_message_tracker_t tracker;
    aeron_cluster_pending_message_tracker_init(&tracker, 0, 1, 0, 4096);

    EXPECT_EQ(1, tracker.next_service_session_id);
    aeron_cluster_pending_message_tracker_on_appended(&tracker, 3);
    EXPECT_EQ(4, tracker.next_service_session_id);
}

/* -----------------------------------------------------------------------
 * ElectionState values match Java enum ordinals
 * ----------------------------------------------------------------------- */
TEST(ElectionStateTest, shouldMatchJavaOrdinals)
{
    EXPECT_EQ(0,  (int)AERON_ELECTION_INIT);
    EXPECT_EQ(1,  (int)AERON_ELECTION_CANVASS);
    EXPECT_EQ(2,  (int)AERON_ELECTION_NOMINATE);
    EXPECT_EQ(3,  (int)AERON_ELECTION_CANDIDATE_BALLOT);
    EXPECT_EQ(4,  (int)AERON_ELECTION_FOLLOWER_BALLOT);
    EXPECT_EQ(8,  (int)AERON_ELECTION_LEADER_READY);
    EXPECT_EQ(16, (int)AERON_ELECTION_FOLLOWER_READY);
    EXPECT_EQ(17, (int)AERON_ELECTION_CLOSED);
}
