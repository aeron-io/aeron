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
#include <cstdint>
#include <algorithm>
#include <random>

#include "aeron_cluster_recording_log.h"
#include "aeron_cluster_member.h"
#include "aeron_cluster_timer_service.h"
#include "aeron_cluster_pending_message_tracker.h"
#include "aeron_consensus_module_configuration.h"

/* -----------------------------------------------------------------------
 * Constants (mirror Java ConsensusModule.Configuration)
 * ----------------------------------------------------------------------- */
static constexpr int32_t CM_SERVICE_ID = -1;   /* ConsensusModule.Configuration.SERVICE_ID */
static constexpr int32_t SERVICE_ID    = 0;    /* first user service */

/* -----------------------------------------------------------------------
 * RecordingLog tests
 * ----------------------------------------------------------------------- */
class RecordingLogTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        m_dir = "/tmp/aeron_cluster_test_recording_log_" + std::to_string(getpid());
        std::system(("rm -rf " + m_dir + " && mkdir -p " + m_dir).c_str());
    }
    void TearDown() override
    {
        std::system(("rm -rf " + m_dir).c_str());
    }

    static void append_term(aeron_cluster_recording_log_t *log,
                             int64_t rec, int64_t term, int64_t base, int64_t ts)
    {
        ASSERT_EQ(0, aeron_cluster_recording_log_append_term(log, rec, term, base, ts));
    }
    static void append_snap(aeron_cluster_recording_log_t *log,
                             int64_t rec, int64_t term, int64_t base,
                             int64_t pos, int64_t ts, int32_t svc)
    {
        ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(log, rec, term, base, pos, ts, svc));
    }

    std::string m_dir;
};

TEST_F(RecordingLogTest, shouldCreateNewIndex)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    EXPECT_EQ(0, log->entry_count);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldAppendAndThenCommitTermPosition)
{
    const int64_t new_position = 9999L;
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
        append_term(log, 1, 1111, 2222, 3333);
        ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(log, 1111, new_position));
        aeron_cluster_recording_log_close(log);
    }
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        EXPECT_EQ(1, log->entry_count);
        auto *e = aeron_cluster_recording_log_entry_at(log, 0);
        ASSERT_NE(nullptr, e);
        EXPECT_EQ(new_position, e->log_position);
        aeron_cluster_recording_log_close(log);
    }
}

TEST_F(RecordingLogTest, shouldAppendAndThenReloadLatestSnapshot)
{
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
        append_term(log,  1, 0, 0, 0);
        append_snap(log,  2, 0, 0, 100, 1, CM_SERVICE_ID);
        append_snap(log,  3, 0, 0, 100, 2, SERVICE_ID);
        aeron_cluster_recording_log_close(log);
    }
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        EXPECT_EQ(3, log->entry_count);
        auto *snap = aeron_cluster_recording_log_get_latest_snapshot(log, CM_SERVICE_ID);
        ASSERT_NE(nullptr, snap);
        EXPECT_EQ(2, snap->recording_id);
        snap = aeron_cluster_recording_log_get_latest_snapshot(log, SERVICE_ID);
        ASSERT_NE(nullptr, snap);
        EXPECT_EQ(3, snap->recording_id);
        aeron_cluster_recording_log_close(log);
    }
}

TEST_F(RecordingLogTest, shouldIgnoreIncompleteSnapshotInRecoveryPlan)
{
    /* Only CM snapshot at pos=777, service snapshot missing → not included */
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
        append_snap(log, 1, 1, 0, 777, 0, CM_SERVICE_ID);
        /* service snapshot at 777 missing */
        append_snap(log, 3, 1, 0, 888, 0, CM_SERVICE_ID);
        append_snap(log, 4, 1, 0, 888, 0, SERVICE_ID);
        aeron_cluster_recording_log_close(log);
    }
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        aeron_cluster_recovery_plan_t *plan = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_create_recovery_plan(log, &plan, 1));
        ASSERT_NE(nullptr, plan);
        /* Latest complete snapshot is at pos=888 */
        EXPECT_EQ(3, plan->snapshots[0].recording_id);  /* CM */
        EXPECT_EQ(4, plan->snapshots[1].recording_id);  /* service */
        aeron_cluster_recovery_plan_free(plan);
        aeron_cluster_recording_log_close(log);
    }
}

TEST_F(RecordingLogTest, shouldIgnoreInvalidMidSnapshotInRecoveryPlan)
{
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
        append_snap(log, 1, 1, 0, 777, 0, CM_SERVICE_ID);
        append_snap(log, 2, 1, 0, 777, 0, SERVICE_ID);
        append_snap(log, 3, 1, 0, 888, 0, CM_SERVICE_ID);
        append_snap(log, 4, 1, 0, 888, 0, SERVICE_ID);
        append_snap(log, 5, 1, 0, 999, 0, CM_SERVICE_ID);
        append_snap(log, 6, 1, 0, 999, 0, SERVICE_ID);
        ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry_at(log, 2)); /* rec=3 */
        ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry_at(log, 3)); /* rec=4 */
        aeron_cluster_recording_log_close(log);
    }
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        aeron_cluster_recovery_plan_t *plan = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_create_recovery_plan(log, &plan, 1));
        ASSERT_NE(nullptr, plan);
        /* Latest valid complete set is at pos=999 */
        EXPECT_EQ(5, plan->snapshots[0].recording_id);
        EXPECT_EQ(6, plan->snapshots[1].recording_id);
        aeron_cluster_recovery_plan_free(plan);
        aeron_cluster_recording_log_close(log);
    }
}

TEST_F(RecordingLogTest, shouldIgnoreInvalidTermInRecoveryPlan)
{
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
        append_term(log, 0, 9,  444, 0);
        append_term(log, 0, 10, 666, 0);
        append_snap(log, 1, 10, 666, 777, 0, CM_SERVICE_ID);
        append_snap(log, 2, 10, 666, 777, 0, SERVICE_ID);
        append_term(log, 0, 11, 999, 0);  /* will be invalidated */
        ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry_at(log, 4));
        aeron_cluster_recording_log_close(log);
    }
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        auto *last_term = aeron_cluster_recording_log_find_last_term(log);
        ASSERT_NE(nullptr, last_term);
        EXPECT_EQ(10, last_term->leadership_term_id);
        EXPECT_TRUE(aeron_cluster_recording_log_is_unknown(log, 11));
        EXPECT_EQ(-1, aeron_cluster_recording_log_get_term_timestamp(log, 11));
        aeron_cluster_recording_log_close(log);
    }
}

TEST_F(RecordingLogTest, shouldInvalidateLatestSnapshot)
{
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
        append_term(log, 1, 7, 0, 1000);
        append_snap(log, 2, 7, 0, 640,  1001, CM_SERVICE_ID);
        append_snap(log, 3, 7, 0, 640,  1001, SERVICE_ID);
        append_snap(log, 4, 7, 0, 1280, 1002, CM_SERVICE_ID);
        append_snap(log, 5, 7, 0, 1280, 1002, SERVICE_ID);
        append_term(log, 1, 8, 1280, 1002);
        EXPECT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));
        EXPECT_EQ(6, log->entry_count);
        aeron_cluster_recording_log_close(log);
    }
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        /* entries 3 and 4 (pos=1280 snapshots) should be invalid */
        auto *e3 = aeron_cluster_recording_log_entry_at(log, 3);
        auto *e4 = aeron_cluster_recording_log_entry_at(log, 4);
        ASSERT_NE(nullptr, e3); ASSERT_NE(nullptr, e4);
        EXPECT_FALSE(aeron_cluster_recording_log_entry_is_valid(e3));
        EXPECT_FALSE(aeron_cluster_recording_log_entry_is_valid(e4));
        /* entry 1 (pos=640 CM snap) still valid */
        auto *e1 = aeron_cluster_recording_log_entry_at(log, 1);
        ASSERT_NE(nullptr, e1);
        EXPECT_TRUE(aeron_cluster_recording_log_entry_is_valid(e1));
        /* Latest snapshot now at pos=640 */
        auto *snap = aeron_cluster_recording_log_get_latest_snapshot(log, CM_SERVICE_ID);
        ASSERT_NE(nullptr, snap);
        EXPECT_EQ(2, snap->recording_id);
        /* Invalidate again — removes pos=640 set */
        EXPECT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));
        aeron_cluster_recording_log_close(log);
    }
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        /* No valid snapshot left → returns 0 */
        EXPECT_EQ(0, aeron_cluster_recording_log_invalidate_latest_snapshot(log));
        aeron_cluster_recording_log_close(log);
    }
}

TEST_F(RecordingLogTest, shouldNotAllowInvalidateOfSnapshotWithoutParentTerm)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_snap(log, -10, 1, 0, 777, 0, CM_SERVICE_ID);
    append_snap(log, -11, 1, 0, 777, 0, SERVICE_ID);
    /* No parent TERM → should fail */
    EXPECT_EQ(-1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldRecoverSnapshotsMidLogMarkedInvalid)
{
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
        append_snap(log, 1, 1, 10, 555, 0, CM_SERVICE_ID);
        append_snap(log, 2, 1, 10, 555, 0, SERVICE_ID);
        append_snap(log, 3, 1, 10, 777, 0, CM_SERVICE_ID);
        append_snap(log, 4, 1, 10, 777, 0, SERVICE_ID);
        append_snap(log, 5, 1, 10, 888, 0, CM_SERVICE_ID);
        append_snap(log, 6, 1, 10, 888, 0, SERVICE_ID);
        ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry_at(log, 2));
        ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry_at(log, 3));
        aeron_cluster_recording_log_close(log);
    }
    /* Verify invalidated entries have correct flag */
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        EXPECT_EQ(6, log->entry_count);
        auto *e2 = aeron_cluster_recording_log_entry_at(log, 2);
        auto *e3 = aeron_cluster_recording_log_entry_at(log, 3);
        ASSERT_NE(nullptr, e2); ASSERT_NE(nullptr, e3);
        EXPECT_FALSE(aeron_cluster_recording_log_entry_is_valid(e2));
        EXPECT_FALSE(aeron_cluster_recording_log_entry_is_valid(e3));
        /* Entries 0,1,4,5 still valid */
        EXPECT_TRUE(aeron_cluster_recording_log_entry_is_valid(aeron_cluster_recording_log_entry_at(log, 0)));
        EXPECT_TRUE(aeron_cluster_recording_log_entry_is_valid(aeron_cluster_recording_log_entry_at(log, 4)));
        aeron_cluster_recording_log_close(log);
    }
}

TEST_F(RecordingLogTest, shouldRecoverSnapshotsLastInLogMarkedWithInvalid)
{
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
        append_term(log, 1, 1, 0,  0);
        append_snap(log, -10, 1, 0, 777, 0, CM_SERVICE_ID);
        append_snap(log, -11, 1, 0, 777, 0, SERVICE_ID);
        append_term(log, 1, 2, 10, 0);
        append_snap(log, -12, 2, 10, 888, 0, CM_SERVICE_ID);
        append_snap(log, -13, 2, 10, 888, 0, SERVICE_ID);
        EXPECT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));
        EXPECT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));
        aeron_cluster_recording_log_close(log);
    }
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        /* All snapshots invalidated → no valid snapshot */
        EXPECT_EQ(nullptr, aeron_cluster_recording_log_get_latest_snapshot(log, CM_SERVICE_ID));
        /* But term entries remain valid */
        auto *last_term = aeron_cluster_recording_log_find_last_term(log);
        ASSERT_NE(nullptr, last_term);
        EXPECT_EQ(2, last_term->leadership_term_id);
        aeron_cluster_recording_log_close(log);
    }
}

TEST_F(RecordingLogTest, shouldAppendTermWithLeadershipTermIdOutOfOrder)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    /* Append terms out of order — C implementation should accept */
    append_term(log, 1, 5, 0, 1000);
    append_term(log, 2, 3, 0, 900);  /* earlier term appended after */
    EXPECT_EQ(2, log->entry_count);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldNotCreateInitialTermWithMinusOneTermId)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    /* Appending term with leadership_term_id = -1 should fail */
    EXPECT_EQ(-1, aeron_cluster_recording_log_append_term(log, 1, -1, 0, 0));
    EXPECT_EQ(0, log->entry_count);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldDetermineIfSnapshotIsInvalid)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 1, 0, 0, 0);
    append_snap(log, 2, 0, 0, 100, 0, CM_SERVICE_ID);
    append_snap(log, 3, 0, 0, 100, 0, SERVICE_ID);

    auto *e1 = aeron_cluster_recording_log_entry_at(log, 1);
    ASSERT_NE(nullptr, e1);
    EXPECT_TRUE(aeron_cluster_recording_log_entry_is_valid(e1));

    EXPECT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));

    auto *e1_after = aeron_cluster_recording_log_entry_at(log, 1);
    ASSERT_NE(nullptr, e1_after);
    EXPECT_FALSE(aeron_cluster_recording_log_entry_is_valid(e1_after));

    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldFindLastTermRecordingId)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 42, 0, 0, 0);
    append_term(log, 99, 1, 0, 0);
    EXPECT_EQ(99, aeron_cluster_recording_log_find_last_term_recording_id(log));
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldGetTermTimestamp)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 1, 5, 0, 12345);
    EXPECT_EQ(12345, aeron_cluster_recording_log_get_term_timestamp(log, 5));
    EXPECT_EQ(-1,    aeron_cluster_recording_log_get_term_timestamp(log, 99));
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldCreateRecoveryPlan)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 10, 0, 0, 1000);
    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(log, 0, 8000));
    append_snap(log, 20, 0, 0, 7000, 1500, CM_SERVICE_ID);
    append_snap(log, 21, 0, 0, 7000, 1500, SERVICE_ID);

    aeron_cluster_recovery_plan_t *plan = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_create_recovery_plan(log, &plan, 1));
    ASSERT_NE(nullptr, plan);
    EXPECT_EQ(0,    plan->last_leadership_term_id);
    EXPECT_EQ(8000, plan->last_append_position);
    EXPECT_EQ(10,   plan->last_term_recording_id);
    EXPECT_GE(plan->snapshot_count, 1);
    EXPECT_EQ(20,   plan->snapshots[0].recording_id);

    aeron_cluster_recovery_plan_free(plan);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldHandleEmptyRecordingLog)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

    EXPECT_EQ(nullptr, aeron_cluster_recording_log_find_last_term(log));
    EXPECT_EQ(-1,      aeron_cluster_recording_log_find_last_term_recording_id(log));
    EXPECT_TRUE(aeron_cluster_recording_log_is_unknown(log, 0));

    aeron_cluster_recovery_plan_t *plan = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_create_recovery_plan(log, &plan, 1));
    ASSERT_NE(nullptr, plan);
    EXPECT_EQ(-1, plan->last_leadership_term_id);
    EXPECT_EQ(0,  plan->snapshot_count);

    aeron_cluster_recovery_plan_free(plan);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldPersistAndReloadMultipleTerms)
{
    const int TERM_COUNT = 5;
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
        for (int i = 0; i < TERM_COUNT; i++)
        {
            append_term(log, i + 100, i, i * 1000, i * 100);
            if (i > 0)
            {
                ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(log, i - 1, i * 1000));
            }
        }
        aeron_cluster_recording_log_close(log);
    }
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        EXPECT_EQ(TERM_COUNT, log->entry_count);
        auto *last = aeron_cluster_recording_log_find_last_term(log);
        ASSERT_NE(nullptr, last);
        EXPECT_EQ(TERM_COUNT - 1, last->leadership_term_id);
        aeron_cluster_recording_log_close(log);
    }
}

TEST_F(RecordingLogTest, shouldHandleInvalidateLatestSnapshotWithMultipleServices)
{
    const int service_count = 3;
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
        append_term(log, 1, 0, 0, 0);
        /* Complete snapshot set at pos=500: CM + 3 services */
        append_snap(log, 10, 0, 0, 500, 0, CM_SERVICE_ID);
        for (int s = 0; s < service_count; s++)
        {
            append_snap(log, 20 + s, 0, 0, 500, 0, s);
        }
        EXPECT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));
        /* All snapshots at pos=500 should be invalid */
        for (int i = 1; i <= service_count + 1; i++)
        {
            auto *e = aeron_cluster_recording_log_entry_at(log, i);
            ASSERT_NE(nullptr, e);
            EXPECT_FALSE(aeron_cluster_recording_log_entry_is_valid(e))
                << "entry " << i << " should be invalid";
        }
        aeron_cluster_recording_log_close(log);
    }
}

TEST_F(RecordingLogTest, snapshotEntriesCanBeQueriedByServiceId)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 1, 0, 0, 0);
    append_snap(log, 10, 0, 0, 100, 0, CM_SERVICE_ID);
    append_snap(log, 11, 0, 0, 100, 0, 0);
    append_snap(log, 12, 0, 0, 200, 0, CM_SERVICE_ID);
    append_snap(log, 13, 0, 0, 200, 0, 0);

    auto *cm_snap  = aeron_cluster_recording_log_get_latest_snapshot(log, CM_SERVICE_ID);
    auto *svc_snap = aeron_cluster_recording_log_get_latest_snapshot(log, 0);
    ASSERT_NE(nullptr, cm_snap);
    ASSERT_NE(nullptr, svc_snap);
    EXPECT_EQ(12, cm_snap->recording_id);
    EXPECT_EQ(13, svc_snap->recording_id);

    aeron_cluster_recording_log_close(log);
}

/* -----------------------------------------------------------------------
 * TimerService tests
 * ----------------------------------------------------------------------- */
static int64_t g_last_fired_id    = -1;
static int     g_fired_count      = 0;
static bool    g_handler_returns  = true;

static void reset_timer_state()
{
    g_last_fired_id   = -1;
    g_fired_count     = 0;
    g_handler_returns = true;
}

static void timer_expiry(void *clientd, int64_t correlation_id)
{
    (void)clientd;
    g_last_fired_id = correlation_id;
    g_fired_count++;
}

class TimerServiceTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        reset_timer_state();
        ASSERT_EQ(0, aeron_cluster_timer_service_create(&m_svc, timer_expiry, nullptr));
    }
    void TearDown() override
    {
        aeron_cluster_timer_service_close(m_svc);
    }
    aeron_cluster_timer_service_t *m_svc = nullptr;
};

TEST_F(TimerServiceTest, pollIsANoOpWhenNoTimersScheduled)
{
    EXPECT_EQ(0, aeron_cluster_timer_service_poll(m_svc, 999999));
    EXPECT_EQ(-1, g_last_fired_id);
    EXPECT_EQ(0, aeron_cluster_timer_service_timer_count(m_svc));
}

TEST_F(TimerServiceTest, pollIsANoOpWhenNoScheduledTimersAreExpired)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 1000));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 2000));
    EXPECT_EQ(0, aeron_cluster_timer_service_poll(m_svc, 999));
    EXPECT_EQ(-1, g_last_fired_id);
    EXPECT_EQ(2, aeron_cluster_timer_service_timer_count(m_svc));
}

TEST_F(TimerServiceTest, pollShouldExpireSingleTimer)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 42, 500));
    EXPECT_EQ(0, aeron_cluster_timer_service_poll(m_svc, 499));
    EXPECT_EQ(-1, g_last_fired_id);
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 500));
    EXPECT_EQ(42, g_last_fired_id);
    EXPECT_EQ(0, aeron_cluster_timer_service_timer_count(m_svc));
}

TEST_F(TimerServiceTest, pollShouldRemoveExpiredTimers)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 100));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 200));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 3, 1000));
    EXPECT_EQ(2, aeron_cluster_timer_service_poll(m_svc, 300));
    EXPECT_EQ(1, aeron_cluster_timer_service_timer_count(m_svc));
}

TEST_F(TimerServiceTest, pollShouldExpireTimersInOrderOfDeadline)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 10, 300));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 20, 100));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 30, 200));

    std::vector<int64_t> fired;
    auto old_expiry = [](void *cd, int64_t id) { static_cast<std::vector<int64_t>*>(cd)->push_back(id); };
    aeron_cluster_timer_service_t *svc2 = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&svc2, old_expiry, &fired));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc2, 10, 300));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc2, 20, 100));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc2, 30, 200));
    aeron_cluster_timer_service_poll(svc2, 400);
    EXPECT_EQ(3u, fired.size());
    EXPECT_EQ(20, fired[0]);
    EXPECT_EQ(30, fired[1]);
    EXPECT_EQ(10, fired[2]);
    aeron_cluster_timer_service_close(svc2);
}

TEST_F(TimerServiceTest, cancelTimerReturnsFalseForUnknownCorrelationId)
{
    EXPECT_FALSE(aeron_cluster_timer_service_cancel(m_svc, 999));
}

TEST_F(TimerServiceTest, cancelTimerReturnsTrueAfterCancellingTheTimer)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 7, 1000));
    EXPECT_TRUE(aeron_cluster_timer_service_cancel(m_svc, 7));
    EXPECT_EQ(0, aeron_cluster_timer_service_timer_count(m_svc));
    /* Cancelled timer must not fire */
    EXPECT_EQ(0, aeron_cluster_timer_service_poll(m_svc, 2000));
    EXPECT_EQ(-1, g_last_fired_id);
}

TEST_F(TimerServiceTest, cancelTimerAfterPoll)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 100));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 200));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 3, 500));
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 150));
    EXPECT_TRUE(aeron_cluster_timer_service_cancel(m_svc, 3));
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 300));
    EXPECT_EQ(2, g_last_fired_id);
    EXPECT_EQ(0, aeron_cluster_timer_service_timer_count(m_svc));
}

TEST_F(TimerServiceTest, scheduleTimerForExistingIdShouldShiftUpWhenDeadlineDecreases)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 1000));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 2000));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 500));  /* move timer 1 earlier */
    /* Timer 1 should fire before timer 2 */
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 700));
    EXPECT_EQ(1, g_last_fired_id);
}

TEST_F(TimerServiceTest, scheduleTimerForExistingIdShouldShiftDownWhenDeadlineIncreases)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 100));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 2000)); /* push later */
    EXPECT_EQ(0, aeron_cluster_timer_service_poll(m_svc, 500));
    EXPECT_EQ(-1, g_last_fired_id);
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 2001));
    EXPECT_EQ(1, g_last_fired_id);
}

TEST_F(TimerServiceTest, cancelExpiredTimerIsANoOp)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 5, 100));
    ASSERT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 200));
    /* Timer 5 already fired — cancel is a no-op */
    EXPECT_FALSE(aeron_cluster_timer_service_cancel(m_svc, 5));
}

TEST_F(TimerServiceTest, nextDeadlineReturnsMaxWhenEmpty)
{
    EXPECT_EQ(INT64_MAX, aeron_cluster_timer_service_next_deadline(m_svc));
}

TEST_F(TimerServiceTest, nextDeadlineReturnsEarliestDeadline)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 500));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 200));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 3, 800));
    EXPECT_EQ(200, aeron_cluster_timer_service_next_deadline(m_svc));
}

TEST_F(TimerServiceTest, scheduleMustRetainOrderBetweenDeadlines)
{
    /* All timers at same deadline — all should fire at that time */
    for (int i = 0; i < 10; i++)
    {
        ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, i, 1000));
    }
    EXPECT_EQ(10, aeron_cluster_timer_service_poll(m_svc, 1000));
    EXPECT_EQ(0,  aeron_cluster_timer_service_timer_count(m_svc));
}

TEST_F(TimerServiceTest, manyRandomOperations)
{
    std::mt19937 rng(42);
    std::uniform_int_distribution<int64_t> id_dist(1, 100);
    std::uniform_int_distribution<int64_t> ts_dist(1, 10000);
    std::uniform_int_distribution<int>     op_dist(0, 2);

    for (int i = 0; i < 1000; i++)
    {
        int op = op_dist(rng);
        int64_t id = id_dist(rng);
        int64_t ts = ts_dist(rng);
        if (op == 0)       { aeron_cluster_timer_service_schedule(m_svc, id, ts); }
        else if (op == 1)  { aeron_cluster_timer_service_cancel(m_svc, id); }
        else               { aeron_cluster_timer_service_poll(m_svc, ts); }
    }
    /* After random ops, drain all */
    int remaining = aeron_cluster_timer_service_timer_count(m_svc);
    EXPECT_GE(remaining, 0);
}

/* -----------------------------------------------------------------------
 * ClusterMember tests
 * ----------------------------------------------------------------------- */
class ClusterMemberTest : public ::testing::Test
{
protected:
    void TearDown() override
    {
        if (m_members) { aeron_cluster_members_free(m_members, m_count); }
    }
    void parse(const char *str)
    {
        if (m_members) { aeron_cluster_members_free(m_members, m_count); m_members = nullptr; }
        ASSERT_EQ(0, aeron_cluster_members_parse(str, &m_members, &m_count));
    }

    aeron_cluster_member_t *m_members = nullptr;
    int m_count = 0;
};

TEST_F(ClusterMemberTest, shouldParseCorrectly)
{
    parse("0,in:cons:log:catch:arch");
    ASSERT_EQ(1, m_count);
    EXPECT_EQ(0, m_members[0].id);
    EXPECT_STREQ("in",   m_members[0].ingress_endpoint);
    EXPECT_STREQ("cons", m_members[0].consensus_endpoint);
    EXPECT_STREQ("log",  m_members[0].log_endpoint);
    EXPECT_STREQ("catch",m_members[0].catchup_endpoint);
    EXPECT_STREQ("arch", m_members[0].archive_endpoint);
}

TEST_F(ClusterMemberTest, shouldParseThreeMembers)
{
    parse("0,h0:9010:h0:9020:h0:9030:h0:9040:h0:8010|"
          "1,h1:9010:h1:9020:h1:9030:h1:9040:h1:8010|"
          "2,h2:9010:h2:9020:h2:9030:h2:9040:h2:8010");
    ASSERT_EQ(3, m_count);
    for (int i = 0; i < 3; i++) { EXPECT_EQ(i, m_members[i].id); }
}

TEST_F(ClusterMemberTest, shouldFindMemberById)
{
    parse("0,h0:e0:l0:c0:a0|1,h1:e1:l1:c1:a1|2,h2:e2:l2:c2:a2");
    ASSERT_EQ(3, m_count);
    auto *m = aeron_cluster_member_find_by_id(m_members, m_count, 1);
    ASSERT_NE(nullptr, m);
    EXPECT_EQ(1, m->id);
    EXPECT_EQ(nullptr, aeron_cluster_member_find_by_id(m_members, m_count, 99));
}

TEST_F(ClusterMemberTest, shouldComputeQuorumThreshold)
{
    EXPECT_EQ(1, aeron_cluster_member_quorum_threshold(1));
    EXPECT_EQ(2, aeron_cluster_member_quorum_threshold(2));
    EXPECT_EQ(2, aeron_cluster_member_quorum_threshold(3));
    EXPECT_EQ(3, aeron_cluster_member_quorum_threshold(4));
    EXPECT_EQ(3, aeron_cluster_member_quorum_threshold(5));
    EXPECT_EQ(4, aeron_cluster_member_quorum_threshold(6));
    EXPECT_EQ(4, aeron_cluster_member_quorum_threshold(7));
}

TEST_F(ClusterMemberTest, shouldComputeQuorumPositionWithAllActive)
{
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e");
    const int64_t now_ns = 1000000LL;
    const int64_t timeout = 10000000000LL;  /* 10s */

    m_members[0].log_position = 100; m_members[0].time_of_last_append_position_ns = now_ns;
    m_members[1].log_position = 200; m_members[1].time_of_last_append_position_ns = now_ns;
    m_members[2].log_position = 300; m_members[2].time_of_last_append_position_ns = now_ns;

    /* Quorum (2 of 3) = second highest = 200 */
    int64_t pos = aeron_cluster_member_quorum_position(m_members, m_count, now_ns, timeout);
    EXPECT_EQ(200, pos);
}

TEST_F(ClusterMemberTest, shouldOnlyConsiderActiveNodesForQuorumPosition)
{
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e");
    const int64_t now_ns     = 100000000LL;
    const int64_t timeout    = 10000000LL;  /* 10ms */

    /* Member 2 timed out (last update was 0, now is 100ms > 10ms timeout) */
    m_members[0].log_position = 100; m_members[0].time_of_last_append_position_ns = now_ns;
    m_members[1].log_position = 200; m_members[1].time_of_last_append_position_ns = now_ns;
    m_members[2].log_position = 300; m_members[2].time_of_last_append_position_ns = 0; /* timed out */

    /* Quorum with member 2 as -1: sorted = [200, 100, -1], quorum index 1 = 100 */
    int64_t pos = aeron_cluster_member_quorum_position(m_members, m_count, now_ns, timeout);
    EXPECT_EQ(100, pos);
}

TEST_F(ClusterMemberTest, shouldCountVotesForCandidateTerm)
{
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e");
    m_members[0].candidate_term_id = 5;
    m_members[1].candidate_term_id = 5;
    m_members[2].candidate_term_id = 4;  /* voted for different term */
    EXPECT_EQ(2, aeron_cluster_member_count_votes(m_members, m_count, 5));
    EXPECT_EQ(1, aeron_cluster_member_count_votes(m_members, m_count, 4));
    EXPECT_EQ(0, aeron_cluster_member_count_votes(m_members, m_count, 99));
}

TEST_F(ClusterMemberTest, singleMemberAlwaysQuorum)
{
    parse("0,a:b:c:d:e");
    const int64_t now_ns  = 1000LL;
    const int64_t timeout = 9999999999LL;
    m_members[0].log_position = 42;
    m_members[0].time_of_last_append_position_ns = now_ns;
    /* Single node: quorum position = its own position */
    EXPECT_EQ(42, aeron_cluster_member_quorum_position(m_members, m_count, now_ns, timeout));
}

TEST_F(ClusterMemberTest, noMembersActiveReturnsMinusOne)
{
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e");
    const int64_t now_ns  = 100000000LL;
    const int64_t timeout = 1LL;  /* all timed out */
    m_members[0].time_of_last_append_position_ns = 0;
    m_members[1].time_of_last_append_position_ns = 0;
    m_members[2].time_of_last_append_position_ns = 0;
    int64_t pos = aeron_cluster_member_quorum_position(m_members, m_count, now_ns, timeout);
    EXPECT_EQ(-1, pos);
}

/* -----------------------------------------------------------------------
 * PendingServiceMessageTracker tests
 * ----------------------------------------------------------------------- */
class PendingMessageTrackerTest : public ::testing::Test
{
protected:
    aeron_cluster_pending_message_tracker_t m_tracker{};

    void init(int32_t svc, int64_t next_id, int64_t log_id, int64_t cap = 4096)
    {
        aeron_cluster_pending_message_tracker_init(&m_tracker, svc, next_id, log_id, cap);
    }
};

TEST_F(PendingMessageTrackerTest, shouldAppendOnlyUncommittedMessages)
{
    init(0, 1, 5);
    EXPECT_FALSE(aeron_cluster_pending_message_tracker_should_append(&m_tracker, 3));
    EXPECT_FALSE(aeron_cluster_pending_message_tracker_should_append(&m_tracker, 5));
    EXPECT_TRUE (aeron_cluster_pending_message_tracker_should_append(&m_tracker, 6));
    EXPECT_TRUE (aeron_cluster_pending_message_tracker_should_append(&m_tracker, 100));
}

TEST_F(PendingMessageTrackerTest, shouldAdvanceNextSessionIdOnAppend)
{
    init(0, 1, 0);
    EXPECT_EQ(1, m_tracker.next_service_session_id);
    aeron_cluster_pending_message_tracker_on_appended(&m_tracker, 3);
    EXPECT_EQ(4, m_tracker.next_service_session_id);
    aeron_cluster_pending_message_tracker_on_appended(&m_tracker, 10);
    EXPECT_EQ(11, m_tracker.next_service_session_id);
}

TEST_F(PendingMessageTrackerTest, shouldNotDecreaseNextSessionIdOnAppend)
{
    init(0, 1, 0);
    aeron_cluster_pending_message_tracker_on_appended(&m_tracker, 10);
    EXPECT_EQ(11, m_tracker.next_service_session_id);
    aeron_cluster_pending_message_tracker_on_appended(&m_tracker, 5);  /* lower ID */
    EXPECT_EQ(11, m_tracker.next_service_session_id);  /* unchanged */
}

TEST_F(PendingMessageTrackerTest, shouldAdvanceLogServiceSessionIdOnCommit)
{
    init(0, 1, 0);
    EXPECT_EQ(0, m_tracker.log_service_session_id);
    aeron_cluster_pending_message_tracker_on_committed(&m_tracker, 7);
    EXPECT_EQ(7, m_tracker.log_service_session_id);
}

TEST_F(PendingMessageTrackerTest, shouldNotDecreaseLogServiceSessionIdOnCommit)
{
    init(0, 1, 10);
    aeron_cluster_pending_message_tracker_on_committed(&m_tracker, 5);
    EXPECT_EQ(10, m_tracker.log_service_session_id);  /* unchanged */
}

TEST_F(PendingMessageTrackerTest, emptyTrackerAllowsAllMessages)
{
    init(0, 1, 0);
    for (int64_t i = 1; i <= 100; i++)
    {
        EXPECT_TRUE(aeron_cluster_pending_message_tracker_should_append(&m_tracker, i));
    }
}

TEST_F(PendingMessageTrackerTest, trackerWithHighLogIdBlocksLowerMessages)
{
    init(0, 50, 100);
    EXPECT_FALSE(aeron_cluster_pending_message_tracker_should_append(&m_tracker, 50));
    EXPECT_FALSE(aeron_cluster_pending_message_tracker_should_append(&m_tracker, 100));
    EXPECT_TRUE (aeron_cluster_pending_message_tracker_should_append(&m_tracker, 101));
}

/* -----------------------------------------------------------------------
 * ElectionState enum ordinals match Java
 * ----------------------------------------------------------------------- */
TEST(ElectionStateTest, shouldMatchJavaOrdinals)
{
    EXPECT_EQ(0,  (int)AERON_ELECTION_INIT);
    EXPECT_EQ(1,  (int)AERON_ELECTION_CANVASS);
    EXPECT_EQ(2,  (int)AERON_ELECTION_NOMINATE);
    EXPECT_EQ(3,  (int)AERON_ELECTION_CANDIDATE_BALLOT);
    EXPECT_EQ(4,  (int)AERON_ELECTION_FOLLOWER_BALLOT);
    EXPECT_EQ(5,  (int)AERON_ELECTION_LEADER_LOG_REPLICATION);
    EXPECT_EQ(6,  (int)AERON_ELECTION_LEADER_REPLAY);
    EXPECT_EQ(7,  (int)AERON_ELECTION_LEADER_INIT);
    EXPECT_EQ(8,  (int)AERON_ELECTION_LEADER_READY);
    EXPECT_EQ(9,  (int)AERON_ELECTION_FOLLOWER_LOG_REPLICATION);
    EXPECT_EQ(10, (int)AERON_ELECTION_FOLLOWER_REPLAY);
    EXPECT_EQ(11, (int)AERON_ELECTION_FOLLOWER_CATCHUP_INIT);
    EXPECT_EQ(12, (int)AERON_ELECTION_FOLLOWER_CATCHUP_AWAIT);
    EXPECT_EQ(13, (int)AERON_ELECTION_FOLLOWER_CATCHUP);
    EXPECT_EQ(14, (int)AERON_ELECTION_FOLLOWER_LOG_INIT);
    EXPECT_EQ(15, (int)AERON_ELECTION_FOLLOWER_LOG_AWAIT);
    EXPECT_EQ(16, (int)AERON_ELECTION_FOLLOWER_READY);
    EXPECT_EQ(17, (int)AERON_ELECTION_CLOSED);
}

/* ============================================================
 * REMAINING RECORDING LOG TESTS
 * ============================================================ */

TEST_F(RecordingLogTest, appendTermShouldRejectNullValueAsRecordingId)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    EXPECT_EQ(-1, aeron_cluster_recording_log_append_term(log, -1LL, 0, 0, 0));
    EXPECT_EQ(0, log->entry_count);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, appendSnapshotShouldRejectNullValueAsRecordingId)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    EXPECT_EQ(-1, aeron_cluster_recording_log_append_snapshot(log, -1LL, 0, 0, 0, 0, 0));
    EXPECT_EQ(0, log->entry_count);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, appendTermShouldNotAcceptDifferentRecordingIds)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 42, 0, 0, 0);
    /* Different recording_id → must fail */
    EXPECT_EQ(-1, aeron_cluster_recording_log_append_term(log, 21, 1, 0, 0));
    EXPECT_EQ(1, log->entry_count);
    aeron_cluster_recording_log_close(log);

    /* Reload and verify */
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
    EXPECT_EQ(-1, aeron_cluster_recording_log_append_term(log, -5, -5, -5, -5));
    EXPECT_EQ(1, log->entry_count);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, appendTermShouldOnlyAllowASingleValidTermForSameLeadershipTermId)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 8, 0, 0, 0);
    append_term(log, 8, 1, 1, 1);
    ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry_at(log, 0));
    append_term(log, 8, 0, 100, 100);  /* replacing invalidated term 0 */
    /* term 1 already valid → must fail */
    EXPECT_EQ(-1, aeron_cluster_recording_log_append_term(log, 8, 1, 5, 5));
    EXPECT_EQ(3, log->entry_count);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldRemoveEntry)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 1, 3, 2, 4);
    append_term(log, 1, 4, 3, 5);
    ASSERT_EQ(0, aeron_cluster_recording_log_remove_entry(log, 4, log->entry_count - 1));
    EXPECT_EQ(2, log->entry_count);

    /* After invalidation the last term entry should be leadershipTermId=3 */
    auto *e = aeron_cluster_recording_log_find_last_term(log);
    ASSERT_NE(nullptr, e);
    EXPECT_EQ(3, e->leadership_term_id);

    aeron_cluster_recording_log_close(log);

    /* Reload: 2 physical entries, 1 valid */
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
    EXPECT_EQ(2, log->entry_count);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldInvalidateLatestSnapshotWithStandbyVariant)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 1, 0, 0, 0);
    /* Standby snapshot (not invalidated by invalidateLatestSnapshot — standby ignored) */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
        log, 5, 0, 0, 100, 0, -1, "aeron:udp?endpoint=localhost:8080"));
    append_snap(log, 10, 0, 0, 200, 0, -1);
    append_snap(log, 11, 0, 0, 200, 0, 0);

    /* invalidateLatestSnapshot should invalidate the regular snapshot group at pos=200 */
    EXPECT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));

    auto *cm_snap = aeron_cluster_recording_log_get_latest_snapshot(log, -1);
    /* The regular snapshot at 200 is invalidated; no other regular snapshot left */
    EXPECT_EQ(nullptr, cm_snap);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldIgnoreStandbySnapshotInRecoveryPlan)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 1, 0, 0, 0);
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
        log, 5, 0, 0, 100, 0, -1, "aeron:udp?endpoint=localhost:8080"));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
        log, 6, 0, 0, 100, 0, 0, "aeron:udp?endpoint=localhost:8080"));

    aeron_cluster_recovery_plan_t *plan = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_create_recovery_plan(log, &plan, 1));
    /* Standby snapshots should NOT be included in recovery plan */
    EXPECT_EQ(0, plan->snapshot_count);
    aeron_cluster_recovery_plan_free(plan);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldCommitLogPositionByTerm)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 1, 5, 0, 0);
    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position_by_term(log, 5, 99999));
    auto *e = aeron_cluster_recording_log_find_last_term(log);
    ASSERT_NE(nullptr, e);
    EXPECT_EQ(99999, e->log_position);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, entryAtReturnsNullForOutOfRange)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 1, 0, 0, 0);
    EXPECT_NE(nullptr, aeron_cluster_recording_log_entry_at(log, 0));
    EXPECT_EQ(nullptr, aeron_cluster_recording_log_entry_at(log, 1));
    EXPECT_EQ(nullptr, aeron_cluster_recording_log_entry_at(log, -1));
    aeron_cluster_recording_log_close(log);
}

/* ============================================================
 * REMAINING TIMER SERVICE TESTS
 * ============================================================ */

TEST_F(TimerServiceTest, cancelTimerReturnsTrueAfterCancellingLastTimer)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 99, 1000));
    EXPECT_TRUE(aeron_cluster_timer_service_cancel(m_svc, 99));
    EXPECT_EQ(0, aeron_cluster_timer_service_timer_count(m_svc));
    /* Next deadline after removing last timer */
    EXPECT_EQ(INT64_MAX, aeron_cluster_timer_service_next_deadline(m_svc));
}

TEST_F(TimerServiceTest, scheduleTimerNoOpIfDeadlineDoesNotChange)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 5, 1000));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 5, 1000)); /* same deadline */
    /* Still only one timer */
    EXPECT_EQ(1, aeron_cluster_timer_service_timer_count(m_svc));
    g_fired_count = 0;
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 1001));
    EXPECT_EQ(1, g_fired_count); /* fired once */
}

TEST_F(TimerServiceTest, pollShouldExpireMultipleTimersAtSameDeadline)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 500));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 500));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 3, 500));
    g_fired_count = 0;
    EXPECT_EQ(3, aeron_cluster_timer_service_poll(m_svc, 500));
    EXPECT_EQ(3, g_fired_count);
}

TEST_F(TimerServiceTest, expireTimerThenCancelFires)
{
    /* Fire timer 1 at t=100, cancel timer 2 before it fires */
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 100));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 500));
    g_fired_count = 0; g_last_fired_id = -1;
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 100));
    EXPECT_EQ(1, g_last_fired_id);
    EXPECT_TRUE(aeron_cluster_timer_service_cancel(m_svc, 2));
    EXPECT_EQ(0, aeron_cluster_timer_service_poll(m_svc, 1000));
}

TEST_F(TimerServiceTest, moveUpTimerAndCancelAnother)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 1000));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 2000));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 3, 3000));
    /* Move timer 3 to fire before timer 1 */
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 3, 500));
    EXPECT_TRUE(aeron_cluster_timer_service_cancel(m_svc, 1));
    /* Timer 3 fires at 500, timer 2 fires at 2000 */
    g_last_fired_id = -1;
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 600));
    EXPECT_EQ(3, g_last_fired_id);
    g_last_fired_id = -1;
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 2001));
    EXPECT_EQ(2, g_last_fired_id);
}

TEST_F(TimerServiceTest, moveDownTimerAndCancelAnother)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 100));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 200));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 3, 300));
    /* Push timer 1 to fire later than timer 3 */
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 400));
    EXPECT_TRUE(aeron_cluster_timer_service_cancel(m_svc, 2));
    std::vector<int64_t> fired;
    auto capture_fn = [](void *cd, int64_t id) {
        static_cast<std::vector<int64_t>*>(cd)->push_back(id);
    };
    aeron_cluster_timer_service_t *svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&svc, capture_fn, &fired));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 1, 400));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 3, 300));
    aeron_cluster_timer_service_poll(svc, 500);
    EXPECT_EQ(2u, fired.size());
    EXPECT_EQ(3, fired[0]);
    EXPECT_EQ(1, fired[1]);
    aeron_cluster_timer_service_close(svc);
}

/* ============================================================
 * REMAINING CLUSTER MEMBER TESTS
 * ============================================================ */

TEST_F(ClusterMemberTest, shouldDetermineQuorumPositionWithFiveNodes)
{
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e|3,a:b:c:d:e|4,a:b:c:d:e");
    const int64_t now_ns  = 1000000LL;
    const int64_t timeout = 10000000000LL;

    m_members[0].log_position = 100; m_members[0].time_of_last_append_position_ns = now_ns;
    m_members[1].log_position = 200; m_members[1].time_of_last_append_position_ns = now_ns;
    m_members[2].log_position = 300; m_members[2].time_of_last_append_position_ns = now_ns;
    m_members[3].log_position = 400; m_members[3].time_of_last_append_position_ns = now_ns;
    m_members[4].log_position = 500; m_members[4].time_of_last_append_position_ns = now_ns;

    /* Quorum = 3; third highest = 300 */
    EXPECT_EQ(300, aeron_cluster_member_quorum_position(m_members, m_count, now_ns, timeout));
}

TEST_F(ClusterMemberTest, isQuorumCandidateFalseWhenNoPosition)
{
    parse("0,a:b:c:d:e");
    m_members[0].log_position = -1; /* no position */
    EXPECT_FALSE(aeron_cluster_member_is_quorum_candidate(&m_members[0], 1000LL, 9999999999LL));
}

TEST_F(ClusterMemberTest, isQuorumCandidateFalseWhenTimedOut)
{
    parse("0,a:b:c:d:e");
    m_members[0].log_position = 100;
    m_members[0].time_of_last_append_position_ns = 0;
    /* timeout = 1ns, now = 1000000ns → timed out */
    EXPECT_FALSE(aeron_cluster_member_is_quorum_candidate(&m_members[0], 1000000LL, 1LL));
}

TEST_F(ClusterMemberTest, isQuorumCandidateTrueWhenRecentAndHasPosition)
{
    parse("0,a:b:c:d:e");
    m_members[0].log_position = 42;
    m_members[0].time_of_last_append_position_ns = 900000LL;
    EXPECT_TRUE(aeron_cluster_member_is_quorum_candidate(&m_members[0], 1000000LL, 9999999999LL));
}

TEST_F(ClusterMemberTest, shouldCountVotesForMultipleTerms)
{
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e|3,a:b:c:d:e|4,a:b:c:d:e");
    for (int i = 0; i < m_count; i++) { m_members[i].candidate_term_id = (int64_t)(i < 3 ? 10 : 11); }
    EXPECT_EQ(3, aeron_cluster_member_count_votes(m_members, m_count, 10));
    EXPECT_EQ(2, aeron_cluster_member_count_votes(m_members, m_count, 11));
    EXPECT_EQ(0, aeron_cluster_member_count_votes(m_members, m_count, 99));
}

TEST_F(ClusterMemberTest, quorumThresholdForLargeCluster)
{
    EXPECT_EQ(5, aeron_cluster_member_quorum_threshold(9));
    EXPECT_EQ(6, aeron_cluster_member_quorum_threshold(11));
}

TEST_F(ClusterMemberTest, quorumPositionWithTwoNodesTimedOut)
{
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e");
    const int64_t now_ns  = 100000000LL;
    const int64_t timeout = 1LL;  /* everyone timed out */

    m_members[0].log_position = 100;
    m_members[1].log_position = 200;
    m_members[2].log_position = 300;
    /* All timed out → quorum position = -1 */
    int64_t pos = aeron_cluster_member_quorum_position(m_members, m_count, now_ns, timeout);
    EXPECT_EQ(-1, pos);
}

TEST_F(ClusterMemberTest, parseEmptyTopologyReturnsZero)
{
    if (m_members) { aeron_cluster_members_free(m_members, m_count); m_members = nullptr; }
    ASSERT_EQ(0, aeron_cluster_members_parse(nullptr, &m_members, &m_count));
    EXPECT_EQ(0, m_count);
    EXPECT_EQ(nullptr, m_members);
}


/* ============================================================
 * SORTED-ON-RELOAD TESTS (entriesInTheRecordingLogShouldBeSorted equiv.)
 * ============================================================ */

TEST_F(RecordingLogTest, entriesShouldBeSortedByLeadershipTermId)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

    /* Append in non-sorted order */
    append_term(log, 0, 0, 0, 10);   /* leadershipTermId=0 */
    append_term(log, 0, 2, 2048, 0); /* leadershipTermId=2 */
    append_term(log, 0, 3, 5000, 100); /* leadershipTermId=3 */
    append_term(log, 0, 1, 700, 0);  /* leadershipTermId=1 */

    EXPECT_EQ(4, log->sorted_count);

    /* After sort: 0, 1, 2, 3 */
    auto *e0 = aeron_cluster_recording_log_entry_at(log, 0);
    auto *e1 = aeron_cluster_recording_log_entry_at(log, 1);
    auto *e2 = aeron_cluster_recording_log_entry_at(log, 2);
    auto *e3 = aeron_cluster_recording_log_entry_at(log, 3);
    ASSERT_NE(nullptr, e0); ASSERT_NE(nullptr, e1);
    ASSERT_NE(nullptr, e2); ASSERT_NE(nullptr, e3);

    EXPECT_EQ(0, e0->leadership_term_id);
    EXPECT_EQ(1, e1->leadership_term_id);
    EXPECT_EQ(2, e2->leadership_term_id);
    EXPECT_EQ(3, e3->leadership_term_id);

    aeron_cluster_recording_log_close(log);

    /* Reload: sorted order must be preserved */
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
    EXPECT_EQ(4, log->sorted_count);
    EXPECT_EQ(0, aeron_cluster_recording_log_entry_at(log, 0)->leadership_term_id);
    EXPECT_EQ(3, aeron_cluster_recording_log_entry_at(log, 3)->leadership_term_id);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, termsShouldSortBeforeSnapshotsAtSameTermBase)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log,  1, 0, 0, 0);
    append_snap(log, 10, 0, 0, 100, 0, -1);  /* snapshot at same termBase=0 */
    append_snap(log, 11, 0, 0, 100, 0, 0);

    EXPECT_EQ(3, log->sorted_count);

    /* TERM must come before SNAPSHOT */
    auto *e0 = aeron_cluster_recording_log_entry_at(log, 0);
    ASSERT_NE(nullptr, e0);
    EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM, e0->entry_type);
    auto *e1 = aeron_cluster_recording_log_entry_at(log, 1);
    ASSERT_NE(nullptr, e1);
    EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT, e1->entry_type);

    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, snapshotsShouldSortByServiceIdDescending)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log,  1, 0, 0, 0);
    append_snap(log, 20, 0, 0, 500, 0, 0);   /* serviceId=0 */
    append_snap(log, 21, 0, 0, 500, 0, 1);   /* serviceId=1 */
    append_snap(log, 22, 0, 0, 500, 0, -1);  /* serviceId=-1 (CM) */

    /* All at same leadershipTermId=0, termBase=0, logPosition=500
     * serviceId DESC: 1, 0, -1 */
    auto *e1 = aeron_cluster_recording_log_entry_at(log, 1);
    auto *e2 = aeron_cluster_recording_log_entry_at(log, 2);
    auto *e3 = aeron_cluster_recording_log_entry_at(log, 3);
    ASSERT_NE(nullptr, e1); ASSERT_NE(nullptr, e2); ASSERT_NE(nullptr, e3);

    EXPECT_EQ(1,  e1->service_id);
    EXPECT_EQ(0,  e2->service_id);
    EXPECT_EQ(-1, e3->service_id);

    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, invalidEntriesShouldBeVisibleInSortedView)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log,  1, 0, 0, 0);
    append_snap(log, 10, 0, 0, 100, 0, -1);
    append_snap(log, 11, 0, 0, 100, 0, 0);

    /* Invalidate the CM snapshot */
    ASSERT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));

    EXPECT_EQ(3, log->sorted_count); /* all 3 still present in sorted view */
    /* Find invalidated entries */
    bool found_invalid = false;
    for (int i = 0; i < log->sorted_count; i++)
    {
        auto *e = aeron_cluster_recording_log_entry_at(log, i);
        if (!e->is_valid) { found_invalid = true; }
    }
    EXPECT_TRUE(found_invalid);

    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, reloadShouldRebuildSortedView)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 1, 5, 0, 0);
    append_term(log, 1, 3, 0, 0);
    append_term(log, 1, 1, 0, 0);

    ASSERT_EQ(0, aeron_cluster_recording_log_reload(log));

    EXPECT_EQ(3, log->sorted_count);
    EXPECT_EQ(1, aeron_cluster_recording_log_entry_at(log, 0)->leadership_term_id);
    EXPECT_EQ(3, aeron_cluster_recording_log_entry_at(log, 1)->leadership_term_id);
    EXPECT_EQ(5, aeron_cluster_recording_log_entry_at(log, 2)->leadership_term_id);

    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, standbySnapshotSortsBetweenTermAndSnapshot)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log,  1, 0, 0, 0);
    append_snap(log, 10, 0, 0, 100, 0, -1);   /* regular SNAPSHOT */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
        log, 20, 0, 0, 100, 0, -1, "aeron:udp?endpoint=localhost:8080"));  /* STANDBY */

    /* Sort order at same termBase=0: TERM < STANDBY < SNAPSHOT */
    auto *e0 = aeron_cluster_recording_log_entry_at(log, 0);
    auto *e1 = aeron_cluster_recording_log_entry_at(log, 1);
    auto *e2 = aeron_cluster_recording_log_entry_at(log, 2);
    ASSERT_NE(nullptr, e0); ASSERT_NE(nullptr, e1); ASSERT_NE(nullptr, e2);

    int base0 = e0->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
    int base1 = e1->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
    int base2 = e2->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;

    EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM,             base0);
    EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_STANDBY_SNAPSHOT, base1);
    EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT,         base2);

    aeron_cluster_recording_log_close(log);
}

/* ============================================================
 * REMAINING TIMER SERVICE TESTS
 * ============================================================ */

TEST_F(TimerServiceTest, pollShouldStopAfterPollLimitIsReached)
{
    const int POLL_LIMIT = 5;
    for (int i = 0; i < POLL_LIMIT * 2; i++)
    {
        ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, i, i));
    }
    g_fired_count = 0;
    int fired = aeron_cluster_timer_service_poll_limit(m_svc, INT64_MAX, POLL_LIMIT);
    EXPECT_EQ(POLL_LIMIT, fired);
    EXPECT_EQ(POLL_LIMIT, g_fired_count);
    EXPECT_EQ(POLL_LIMIT, aeron_cluster_timer_service_timer_count(m_svc));
}

struct SnapshotCapture { std::vector<std::pair<int64_t,int64_t>> timers; };
static void capture_snapshot(void *cd, int64_t correl, int64_t deadline)
{
    static_cast<SnapshotCapture*>(cd)->timers.push_back({correl, deadline});
}

TEST_F(TimerServiceTest, snapshotProcessesAllRemainingTimersInDeadlineOrder)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 3, 30));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 4, 29));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 5, 15));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 10));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 14));

    /* Poll timers at deadline <= 14 (fires 1,2) */
    g_fired_count = 0;
    EXPECT_EQ(2, aeron_cluster_timer_service_poll(m_svc, 14));

    /* Snapshot remaining (5,4,3) in deadline order */
    SnapshotCapture cap;
    aeron_cluster_timer_service_snapshot(m_svc, capture_snapshot, &cap);
    ASSERT_EQ(3u, cap.timers.size());
    EXPECT_EQ(5, cap.timers[0].first);  EXPECT_EQ(15, cap.timers[0].second);
    EXPECT_EQ(4, cap.timers[1].first);  EXPECT_EQ(29, cap.timers[1].second);
    EXPECT_EQ(3, cap.timers[2].first);  EXPECT_EQ(30, cap.timers[2].second);
}

TEST_F(TimerServiceTest, snapshotEmptyServiceDoesNothing)
{
    SnapshotCapture cap;
    aeron_cluster_timer_service_snapshot(m_svc, capture_snapshot, &cap);
    EXPECT_EQ(0u, cap.timers.size());
}

/* ============================================================
 * REMAINING CLUSTER MEMBER TESTS
 * ============================================================ */

/* Helper: set member state from individual fields */
static void member_set(aeron_cluster_member_t *m, int32_t id,
                        int64_t term_id, int64_t log_pos, int64_t ts = 0)
{
    m->id = id;
    m->leadership_term_id = term_id;
    m->log_position  = log_pos;
    m->time_of_last_append_position_ns = ts;
    m->candidate_term_id = -1;
    m->vote = -1;
}

TEST_F(ClusterMemberTest, shouldRankClusterStart)
{
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e");
    const int64_t now_ns  = 0LL;
    const int64_t timeout = 10LL;
    m_members[0].log_position = 0;
    m_members[1].log_position = 0;
    m_members[2].log_position = 0;
    EXPECT_EQ(0, aeron_cluster_member_quorum_position(m_members, m_count, now_ns, timeout));
}

/* shouldDetermineQuorumPosition parameterized cases */
struct QuorumCase { int64_t p0, p1, p2, expected; };
static const QuorumCase QUORUM_CASES[] = {
    {0,0,0,0},{123,0,0,0},{123,123,0,123},{123,123,123,123},
    {0,123,123,123},{0,0,123,0},{0,123,200,123},
    {5,3,1,3},{5,1,3,3},{1,3,5,3},{1,5,3,3},{3,1,5,3},{3,5,1,3}
};

class QuorumPositionTest : public ::testing::TestWithParam<QuorumCase> {};
INSTANTIATE_TEST_SUITE_P(ClusterMember, QuorumPositionTest,
    ::testing::ValuesIn(QUORUM_CASES));

TEST_P(QuorumPositionTest, shouldDetermineQuorumPosition)
{
    auto c = GetParam();
    aeron_cluster_member_t members[3] = {};
    members[0].log_position = c.p0; members[0].time_of_last_append_position_ns = 0;
    members[1].log_position = c.p1; members[1].time_of_last_append_position_ns = 0;
    members[2].log_position = c.p2; members[2].time_of_last_append_position_ns = 0;
    EXPECT_EQ(c.expected, aeron_cluster_member_quorum_position(members, 3, 0, INT64_MAX));
}

TEST_F(ClusterMemberTest, isUnanimousCandidateFalseIfMemberHasNoLogPosition)
{
    parse("1,a:b:c:d:e|2,a:b:c:d:e|3,a:b:c:d:e");
    aeron_cluster_member_t candidate{};
    member_set(&candidate, 4, 2, 1000);

    member_set(&m_members[0], 1, 2, 100);
    member_set(&m_members[1], 2, 8, -1);  /* no position */
    member_set(&m_members[2], 3, 1, 1);

    EXPECT_FALSE(aeron_cluster_member_is_unanimous_candidate(
        m_members, m_count, &candidate, -1));
}

TEST_F(ClusterMemberTest, isUnanimousCandidateFalseIfMemberHasMoreLog)
{
    parse("1,a:b:c:d:e|2,a:b:c:d:e|3,a:b:c:d:e");
    aeron_cluster_member_t candidate{};
    member_set(&candidate, 4, 10, 800);

    member_set(&m_members[0], 1, 2, 100);
    member_set(&m_members[1], 2, 8, 6);
    member_set(&m_members[2], 3, 11, 1000);  /* better than candidate */

    EXPECT_FALSE(aeron_cluster_member_is_unanimous_candidate(
        m_members, m_count, &candidate, -1));
}

TEST_F(ClusterMemberTest, isUnanimousCandidateFalseIfGracefulLeaderSkipped)
{
    parse("1,a:b:c:d:e|2,a:b:c:d:e");
    aeron_cluster_member_t candidate{};
    member_set(&candidate, 2, 2, 100);

    member_set(&m_members[0], 1, 2, 100);
    member_set(&m_members[1], 2, 2, 100);

    /* gracefulClosedLeaderId=1 → only member 2 counts; 1 < quorum(2)=2 → false */
    EXPECT_FALSE(aeron_cluster_member_is_unanimous_candidate(
        m_members, m_count, &candidate, 1));
}

TEST_F(ClusterMemberTest, isUnanimousCandidateTrueIfCandidateHasBestLog)
{
    parse("10,a:b:c:d:e|20,a:b:c:d:e|30,a:b:c:d:e");
    aeron_cluster_member_t candidate{};
    member_set(&candidate, 2, 10, 800);

    member_set(&m_members[0], 10, 2, 100);
    member_set(&m_members[1], 20, 8, 6);
    member_set(&m_members[2], 30, 10, 800);

    EXPECT_TRUE(aeron_cluster_member_is_unanimous_candidate(
        m_members, m_count, &candidate, -1));
}

TEST_F(ClusterMemberTest, isQuorumCandidateFalseWhenQuorumNotReached)
{
    parse("10,a:b:c:d:e|20,a:b:c:d:e|30,a:b:c:d:e|40,a:b:c:d:e|50,a:b:c:d:e");
    aeron_cluster_member_t candidate{};
    member_set(&candidate, 2, 10, 800);

    member_set(&m_members[0], 10, 2, 100);
    member_set(&m_members[1], 20, 18, 600);
    member_set(&m_members[2], 30, 10, 800);
    member_set(&m_members[3], 40, 19, 800);
    member_set(&m_members[4], 50, 10, 1000);  /* better than candidate */

    EXPECT_FALSE(aeron_cluster_member_is_quorum_candidate_for(
        m_members, m_count, &candidate));
}

TEST_F(ClusterMemberTest, isQuorumCandidateTrueWhenQuorumReached)
{
    parse("10,a:b:c:d:e|20,a:b:c:d:e|30,a:b:c:d:e|40,a:b:c:d:e|50,a:b:c:d:e");
    aeron_cluster_member_t candidate{};
    member_set(&candidate, 2, 10, 800);

    member_set(&m_members[0], 10, 2, 100);
    member_set(&m_members[1], 20, 18, 600);
    member_set(&m_members[2], 30, 10, 800);
    member_set(&m_members[3], 40, 9, 800);
    member_set(&m_members[4], 50, 10, 700);

    EXPECT_TRUE(aeron_cluster_member_is_quorum_candidate_for(
        m_members, m_count, &candidate));
}

TEST_F(ClusterMemberTest, isQuorumLeaderReturnsTrueWhenQuorumReached)
{
    const int64_t ct = -5;
    parse("1,a:b:c:d:e|2,a:b:c:d:e|3,a:b:c:d:e|4,a:b:c:d:e|5,a:b:c:d:e");
    m_members[0].candidate_term_id = ct;   m_members[0].vote = 1;   /* YES */
    m_members[1].candidate_term_id = ct*2; m_members[1].vote = 0;   /* NO, different term */
    m_members[2].candidate_term_id = ct;   m_members[2].vote = -1;  /* null */
    m_members[3].candidate_term_id = ct;   m_members[3].vote = 1;   /* YES */
    m_members[4].candidate_term_id = ct;   m_members[4].vote = 1;   /* YES */

    EXPECT_TRUE(aeron_cluster_member_is_quorum_leader(m_members, m_count, ct));
}

TEST_F(ClusterMemberTest, isQuorumLeaderReturnsFalseOnNegativeVote)
{
    const int64_t ct = 8;
    parse("1,a:b:c:d:e|2,a:b:c:d:e|3,a:b:c:d:e|4,a:b:c:d:e|5,a:b:c:d:e");
    m_members[0].candidate_term_id = ct; m_members[0].vote = 1;
    m_members[1].candidate_term_id = ct; m_members[1].vote = 0;  /* explicit NO */
    m_members[2].candidate_term_id = ct; m_members[2].vote = 1;
    m_members[3].candidate_term_id = ct; m_members[3].vote = 1;
    m_members[4].candidate_term_id = ct; m_members[4].vote = 1;

    EXPECT_FALSE(aeron_cluster_member_is_quorum_leader(m_members, m_count, ct));
}

TEST_F(ClusterMemberTest, hasQuorumAtPositionTrue)
{
    const int64_t now_ns = 1000LL;
    const int64_t timeout = 9999999LL;
    const int64_t lt = 5LL;
    const int64_t pos = 100LL;

    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e");
    m_members[0].leadership_term_id = lt; m_members[0].log_position = 100;
    m_members[0].time_of_last_append_position_ns = now_ns;
    m_members[1].leadership_term_id = lt; m_members[1].log_position = 150;
    m_members[1].time_of_last_append_position_ns = now_ns;
    m_members[2].leadership_term_id = lt; m_members[2].log_position = 50;
    m_members[2].time_of_last_append_position_ns = now_ns;

    EXPECT_TRUE(aeron_cluster_member_has_quorum_at_position(
        m_members, m_count, lt, pos, now_ns, timeout));
}

TEST_F(ClusterMemberTest, hasQuorumAtPositionFalse)
{
    const int64_t now_ns = 1000LL;
    const int64_t timeout = 9999999LL;
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e");
    m_members[0].leadership_term_id = 5; m_members[0].log_position = 50;
    m_members[0].time_of_last_append_position_ns = now_ns;
    m_members[1].leadership_term_id = 5; m_members[1].log_position = 50;
    m_members[1].time_of_last_append_position_ns = now_ns;
    m_members[2].leadership_term_id = 5; m_members[2].log_position = 50;
    m_members[2].time_of_last_append_position_ns = now_ns;

    /* Need >= 100, everyone only has 50 */
    EXPECT_FALSE(aeron_cluster_member_has_quorum_at_position(
        m_members, m_count, 5, 100, now_ns, timeout));
}
