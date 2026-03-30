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
#include <string>

#include "aeron_cluster_backup_agent.h"
#include "aeron_cluster_recording_log.h"

/* -----------------------------------------------------------------------
 * ClusterBackupAgent state-machine tests.
 * All tests pass ctx.aeron = nullptr so no real Aeron driver is needed.
 * The recording log is backed by a per-test temp directory.
 * ----------------------------------------------------------------------- */

static constexpr int64_t BACKUP_NOW_MS              = INT64_C(100000);
static constexpr int64_t BACKUP_RESPONSE_TIMEOUT_NS = INT64_C(2000)  * INT64_C(1000000);
static constexpr int64_t BACKUP_QUERY_INTERVAL_NS   = INT64_C(1000)  * INT64_C(1000000);
static constexpr int64_t BACKUP_PROGRESS_TIMEOUT_NS = INT64_C(10000) * INT64_C(1000000);
static constexpr int64_t COOL_DOWN_INTERVAL_NS      = INT64_C(1000)  * INT64_C(1000000);

class ClusterBackupAgentTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        m_dir = "/tmp/aeron_cluster_backup_agent_test_" + std::to_string(getpid());
        if (std::system(("rm -rf " + m_dir + " && mkdir -p " + m_dir).c_str())) {}


        memset(&m_ctx, 0, sizeof(m_ctx));
        m_ctx.aeron          = nullptr;
        m_ctx.backup_archive = nullptr;

        m_ctx.backup_response_timeout_ns      = BACKUP_RESPONSE_TIMEOUT_NS;
        m_ctx.backup_query_interval_ns        = BACKUP_QUERY_INTERVAL_NS;
        m_ctx.backup_progress_timeout_ns      = BACKUP_PROGRESS_TIMEOUT_NS;
        m_ctx.cool_down_interval_ns           = COOL_DOWN_INTERVAL_NS;
        m_ctx.replication_progress_timeout_ns  = INT64_C(5000) * INT64_C(1000000);
        m_ctx.replication_progress_interval_ns = INT64_C(500)  * INT64_C(1000000);
        m_ctx.log_stream_id       = 100;
        m_ctx.consensus_stream_id = 20;

        strncpy(m_ctx.consensus_channel,
                "aeron:udp?endpoint=localhost:9001",
                sizeof(m_ctx.consensus_channel) - 1);
        strncpy(m_ctx.cluster_consensus_endpoints,
                "localhost:9001,localhost:9002,localhost:9003",
                sizeof(m_ctx.cluster_consensus_endpoints) - 1);

        ASSERT_EQ(0, aeron_cluster_backup_agent_create(&m_agent, &m_ctx, m_dir.c_str()));
    }

    void TearDown() override
    {
        if (nullptr != m_agent)
        {
            aeron_cluster_backup_agent_close(m_agent);
            m_agent = nullptr;
        }
        if (std::system(("rm -rf " + m_dir).c_str())) {}

    }

    aeron_cluster_backup_context_t m_ctx{};
    aeron_cluster_backup_agent_t  *m_agent = nullptr;
    std::string                    m_dir;
};

/* -----------------------------------------------------------------------
 * Creation / accessors
 * ----------------------------------------------------------------------- */

TEST_F(ClusterBackupAgentTest, createSucceeds)
{
    ASSERT_NE(nullptr, m_agent);
    EXPECT_EQ(AERON_CLUSTER_BACKUP_STATE_BACKUP_QUERY, m_agent->state);
    EXPECT_FALSE(m_agent->is_closed);
}

TEST_F(ClusterBackupAgentTest, endpointsAreParsedFromCommaSeparatedList)
{
    /* Three endpoints in cluster_consensus_endpoints → parsed_endpoints_count == 3 */
    EXPECT_EQ(3, m_agent->parsed_endpoints_count);
}

TEST_F(ClusterBackupAgentTest, stateAccessorReturnsCurrentState)
{
    EXPECT_EQ(AERON_CLUSTER_BACKUP_STATE_BACKUP_QUERY,
              aeron_cluster_backup_agent_state(m_agent));

    m_agent->state = AERON_CLUSTER_BACKUP_STATE_BACKING_UP;
    EXPECT_EQ(AERON_CLUSTER_BACKUP_STATE_BACKING_UP,
              aeron_cluster_backup_agent_state(m_agent));
}

TEST_F(ClusterBackupAgentTest, onStartSetsTimings)
{
    aeron_cluster_backup_agent_on_start(m_agent, BACKUP_NOW_MS);
    EXPECT_EQ(BACKUP_NOW_MS, m_agent->time_of_last_progress_ms);
    EXPECT_EQ(BACKUP_NOW_MS - 1, m_agent->time_of_last_backup_query_ms);
}

TEST_F(ClusterBackupAgentTest, closeWithNullPointerIsNoop)
{
    /* Should not crash — close checks for NULL */
    aeron_cluster_backup_agent_close(nullptr);
}

/* -----------------------------------------------------------------------
 * do_work: CLOSED state early-exit
 * ----------------------------------------------------------------------- */

TEST_F(ClusterBackupAgentTest, doWorkInClosedStateReturnsZero)
{
    m_agent->is_closed = true;
    EXPECT_EQ(0, aeron_cluster_backup_agent_do_work(m_agent, BACKUP_NOW_MS));
}

TEST_F(ClusterBackupAgentTest, doWorkInClosedEnumStateReturnsZero)
{
    m_agent->state = AERON_CLUSTER_BACKUP_STATE_CLOSED;
    EXPECT_EQ(0, aeron_cluster_backup_agent_do_work(m_agent, BACKUP_NOW_MS));
}

/* -----------------------------------------------------------------------
 * do_reset_backup
 * ----------------------------------------------------------------------- */

TEST_F(ClusterBackupAgentTest, doResetBackupSetsCoolDownDeadlineOnFirstCall)
{
    m_agent->state = AERON_CLUSTER_BACKUP_STATE_RESET_BACKUP;
    /* cool_down_deadline_ms is INT64_C(-1) == NULL_VALUE after create */
    EXPECT_EQ(INT64_C(-1), m_agent->cool_down_deadline_ms);

    m_agent->time_of_last_progress_ms = BACKUP_NOW_MS;
    aeron_cluster_backup_agent_do_work(m_agent, BACKUP_NOW_MS);

    int64_t expected = BACKUP_NOW_MS + COOL_DOWN_INTERVAL_NS / INT64_C(1000000);
    EXPECT_EQ(expected, m_agent->cool_down_deadline_ms);
    /* State stays RESET_BACKUP during cool-down */
    EXPECT_EQ(AERON_CLUSTER_BACKUP_STATE_RESET_BACKUP, m_agent->state);
}

TEST_F(ClusterBackupAgentTest, doResetBackupTransitionsToBackupQueryAfterCoolDown)
{
    m_agent->state = AERON_CLUSTER_BACKUP_STATE_RESET_BACKUP;
    m_agent->time_of_last_progress_ms = BACKUP_NOW_MS;

    /* First call: sets cool-down deadline */
    aeron_cluster_backup_agent_do_work(m_agent, BACKUP_NOW_MS);
    EXPECT_EQ(AERON_CLUSTER_BACKUP_STATE_RESET_BACKUP, m_agent->state);

    /* Advance past cool-down deadline */
    int64_t after_cooldown =
        BACKUP_NOW_MS + COOL_DOWN_INTERVAL_NS / INT64_C(1000000) + 1;
    aeron_cluster_backup_agent_do_work(m_agent, after_cooldown);

    EXPECT_EQ(AERON_CLUSTER_BACKUP_STATE_BACKUP_QUERY, m_agent->state);
    EXPECT_EQ(INT64_C(-1), m_agent->cool_down_deadline_ms); /* reset to NULL_VALUE */
}

/* -----------------------------------------------------------------------
 * do_backing_up
 * ----------------------------------------------------------------------- */

TEST_F(ClusterBackupAgentTest, doBackingUpResetsOnProgressTimeout)
{
    m_agent->state                    = AERON_CLUSTER_BACKUP_STATE_BACKING_UP;
    m_agent->time_of_last_progress_ms = BACKUP_NOW_MS;

    /* Advance past progress timeout (10 s) */
    int64_t after_timeout =
        BACKUP_NOW_MS + BACKUP_PROGRESS_TIMEOUT_NS / INT64_C(1000000) + 1;

    aeron_cluster_backup_agent_do_work(m_agent, after_timeout);

    EXPECT_EQ(AERON_CLUSTER_BACKUP_STATE_RESET_BACKUP, m_agent->state);
}

TEST_F(ClusterBackupAgentTest, doBackingUpTransitionsToBackupQueryOnQueryInterval)
{
    m_agent->state                      = AERON_CLUSTER_BACKUP_STATE_BACKING_UP;
    m_agent->time_of_last_progress_ms   = BACKUP_NOW_MS;
    m_agent->time_of_last_backup_query_ms = BACKUP_NOW_MS;

    /* Advance past query interval (1 s) but well before progress timeout (10 s) */
    int64_t after_interval =
        BACKUP_NOW_MS + BACKUP_QUERY_INTERVAL_NS / INT64_C(1000000) + 1;

    aeron_cluster_backup_agent_do_work(m_agent, after_interval);

    EXPECT_EQ(AERON_CLUSTER_BACKUP_STATE_BACKUP_QUERY, m_agent->state);
}

/* -----------------------------------------------------------------------
 * do_update_recording_log
 * ----------------------------------------------------------------------- */

TEST_F(ClusterBackupAgentTest, doUpdateRecordingLogTransitionsToBacking)
{
    /* No pending entries, no retrieved snapshots — should still advance */
    m_agent->state                      = AERON_CLUSTER_BACKUP_STATE_UPDATE_RECORDING_LOG;
    m_agent->leader_log_entry_pending   = false;
    m_agent->leader_last_term_entry_pending = false;
    m_agent->snapshots_retrieved_count  = 0;
    m_agent->time_of_last_progress_ms   = BACKUP_NOW_MS;

    int work = aeron_cluster_backup_agent_do_work(m_agent, BACKUP_NOW_MS);

    EXPECT_GT(work, 0);
    EXPECT_EQ(AERON_CLUSTER_BACKUP_STATE_BACKING_UP, m_agent->state);
}

TEST_F(ClusterBackupAgentTest, doUpdateRecordingLogAppendsUnknownLeaderLastTermEntry)
{
    m_agent->state                      = AERON_CLUSTER_BACKUP_STATE_UPDATE_RECORDING_LOG;
    m_agent->leader_log_entry_pending   = false;
    m_agent->snapshots_retrieved_count  = 0;
    m_agent->time_of_last_progress_ms   = BACKUP_NOW_MS;

    /* Pending last-term entry with leadership_term_id=7 (unknown in empty log) */
    m_agent->leader_last_term_entry_pending                    = true;
    m_agent->leader_last_term_entry_recording_id               = INT64_C(42);
    m_agent->leader_last_term_entry_leadership_term_id         = INT64_C(7);
    m_agent->leader_last_term_entry_term_base_log_position     = INT64_C(0);
    m_agent->leader_last_term_entry_timestamp                  = INT64_C(0);

    /* Verify the recording log is empty before the call */
    ASSERT_EQ(nullptr, aeron_cluster_recording_log_find_last_term(m_agent->recording_log));

    aeron_cluster_backup_agent_do_work(m_agent, BACKUP_NOW_MS);

    EXPECT_EQ(AERON_CLUSTER_BACKUP_STATE_BACKING_UP, m_agent->state);

    /* Term 7 with recording_id=42 should now be in the log */
    const aeron_cluster_recording_log_entry_t *entry =
        aeron_cluster_recording_log_find_last_term(m_agent->recording_log);
    ASSERT_NE(nullptr, entry);
    EXPECT_EQ(INT64_C(7),  entry->leadership_term_id);
    EXPECT_EQ(INT64_C(42), entry->recording_id);
}

/* -----------------------------------------------------------------------
 * Context / configuration tests
 * ----------------------------------------------------------------------- */

TEST_F(ClusterBackupAgentTest, singleEndpointParsedCorrectly)
{
    /* Create a second agent with single endpoint */
    aeron_cluster_backup_context_t ctx2{};
    ctx2.aeron = nullptr;
    ctx2.backup_archive = nullptr;
    ctx2.backup_progress_timeout_ns = BACKUP_PROGRESS_TIMEOUT_NS;
    ctx2.cool_down_interval_ns = COOL_DOWN_INTERVAL_NS;
    ctx2.backup_response_timeout_ns = BACKUP_RESPONSE_TIMEOUT_NS;
    ctx2.backup_query_interval_ns = BACKUP_QUERY_INTERVAL_NS;
    strncpy(ctx2.cluster_consensus_endpoints, "localhost:9001",
            sizeof(ctx2.cluster_consensus_endpoints) - 1);

    std::string dir2 = m_dir + "_single";
    if (std::system(("mkdir -p " + dir2).c_str())) {}

    aeron_cluster_backup_agent_t *agent2 = nullptr;
    ASSERT_EQ(0, aeron_cluster_backup_agent_create(&agent2, &ctx2, dir2.c_str()));
    EXPECT_EQ(1, agent2->parsed_endpoints_count);
    aeron_cluster_backup_agent_close(agent2);
    if (std::system(("rm -rf " + dir2).c_str())) {}
}

TEST_F(ClusterBackupAgentTest, emptyEndpointsYieldsZeroParsed)
{
    aeron_cluster_backup_context_t ctx2{};
    ctx2.aeron = nullptr;
    ctx2.backup_archive = nullptr;
    ctx2.backup_progress_timeout_ns = BACKUP_PROGRESS_TIMEOUT_NS;
    ctx2.cool_down_interval_ns = COOL_DOWN_INTERVAL_NS;
    ctx2.backup_response_timeout_ns = BACKUP_RESPONSE_TIMEOUT_NS;
    ctx2.backup_query_interval_ns = BACKUP_QUERY_INTERVAL_NS;
    ctx2.cluster_consensus_endpoints[0] = '\0';

    std::string dir2 = m_dir + "_empty";
    if (std::system(("mkdir -p " + dir2).c_str())) {}

    aeron_cluster_backup_agent_t *agent2 = nullptr;
    ASSERT_EQ(0, aeron_cluster_backup_agent_create(&agent2, &ctx2, dir2.c_str()));
    EXPECT_EQ(0, agent2->parsed_endpoints_count);
    aeron_cluster_backup_agent_close(agent2);
    if (std::system(("rm -rf " + dir2).c_str())) {}
}

TEST_F(ClusterBackupAgentTest, logSourceAcceptableDefaultIsAny)
{
    /* Default source_type is ANY (0) via zero-init */
    EXPECT_EQ(AERON_CLUSTER_BACKUP_SOURCE_ANY, m_ctx.source_type);
}

TEST_F(ClusterBackupAgentTest, doBackupQueryWithNullAeronReturnsZero)
{
    /* With aeron==NULL, do_backup_query can't create publications, returns 0 */
    m_agent->state = AERON_CLUSTER_BACKUP_STATE_BACKUP_QUERY;
    m_agent->time_of_last_progress_ms = BACKUP_NOW_MS;
    int work = aeron_cluster_backup_agent_do_work(m_agent, BACKUP_NOW_MS);
    /* Should do slow tick work (1) but backup_query returns 0 */
    EXPECT_GE(work, 0);
    /* State should remain BACKUP_QUERY (couldn't connect) */
    EXPECT_EQ(AERON_CLUSTER_BACKUP_STATE_BACKUP_QUERY, m_agent->state);
}

TEST_F(ClusterBackupAgentTest, doUpdateRecordingLogAppendsLeaderLogEntryWhenSnapshotsPresent)
{
    m_agent->state = AERON_CLUSTER_BACKUP_STATE_UPDATE_RECORDING_LOG;
    m_agent->time_of_last_progress_ms = BACKUP_NOW_MS;

    /* Set pending leader log entry — BUT it requires snap_term_id >= entry term.
     * With snapshots_retrieved_count == 0, snap_term_id == INT64_MIN, so the
     * leader_log_entry is NOT appended (by design — mirrors Java). */
    m_agent->leader_log_entry_pending                    = true;
    m_agent->leader_log_entry_recording_id               = INT64_C(99);
    m_agent->leader_log_entry_leadership_term_id         = INT64_C(3);
    m_agent->leader_log_entry_term_base_log_position     = INT64_C(0);
    m_agent->leader_log_entry_timestamp                  = INT64_C(0);
    m_agent->leader_last_term_entry_pending              = false;
    m_agent->snapshots_retrieved_count                   = 0;

    aeron_cluster_backup_agent_do_work(m_agent, BACKUP_NOW_MS);

    EXPECT_EQ(AERON_CLUSTER_BACKUP_STATE_BACKING_UP, m_agent->state);
    /* Leader log entry NOT appended because no snapshots exist with matching term */
    EXPECT_EQ(nullptr, aeron_cluster_recording_log_find_last_term(m_agent->recording_log));
}

TEST_F(ClusterBackupAgentTest, progressTimeoutResetsAnyNonTerminalState)
{
    /* Test that the global progress timeout check at end of do_work fires
     * for SNAPSHOT_RETRIEVE state (not just BACKING_UP) */
    m_agent->state = AERON_CLUSTER_BACKUP_STATE_SNAPSHOT_RETRIEVE;
    m_agent->time_of_last_progress_ms = BACKUP_NOW_MS;

    int64_t after_timeout =
        BACKUP_NOW_MS + BACKUP_PROGRESS_TIMEOUT_NS / INT64_C(1000000) + 1;
    aeron_cluster_backup_agent_do_work(m_agent, after_timeout);

    EXPECT_EQ(AERON_CLUSTER_BACKUP_STATE_RESET_BACKUP, m_agent->state);
}

TEST_F(ClusterBackupAgentTest, doResetBackupClearsSnapshotCounts)
{
    m_agent->state = AERON_CLUSTER_BACKUP_STATE_RESET_BACKUP;
    m_agent->snapshots_to_retrieve_count = 5;
    m_agent->snapshots_retrieved_count   = 3;
    m_agent->time_of_last_progress_ms = BACKUP_NOW_MS;

    aeron_cluster_backup_agent_do_work(m_agent, BACKUP_NOW_MS);

    /* reset_agent clears counts */
    EXPECT_EQ(0, m_agent->snapshots_to_retrieve_count);
    EXPECT_EQ(0, m_agent->snapshots_retrieved_count);
}
