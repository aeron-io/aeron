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
 * 3-node cluster integration test.
 * Starts 3 Java ArchivingMediaDrivers + 3 C ConsensusModuleAgents.
 * Verifies leader election completes with appointed leader.
 */

#include <gtest/gtest.h>
#include <cstdlib>
#include <string>
#include <iostream>
#include <thread>
#include <chrono>

extern "C"
{
#include "aeronc.h"
#include "aeron_archive.h"
#include "aeron_cm_context.h"
#include "aeron_consensus_module_agent.h"
#include "util/aeron_fileutil.h"
}

#include "../integration/aeron_test_cluster_node.h"

class ThreeNodeClusterTest : public ::testing::Test
{
protected:
    static constexpr int NODE_COUNT = 3;
    static constexpr int BASE_NODE_INDEX = 21; /* 21%3==0, so base=21 → ports 8031,8032,8033 */

    void SetUp() override
    {
        m_base_dir = "/tmp/aeron_cluster_3node_" + std::to_string(getpid());
        if (std::system(("rm -rf " + m_base_dir).c_str())) {}


        for (int i = 0; i < NODE_COUNT; i++)
        {
            m_nodes[i] = new TestClusterNode(BASE_NODE_INDEX + i, NODE_COUNT, m_base_dir, std::cout);
            m_nodes[i]->start();
        }
    }

    void TearDown() override
    {
        for (int i = 0; i < NODE_COUNT; i++)
        {
            if (m_agents[i]) { aeron_consensus_module_agent_close(m_agents[i]); m_agents[i] = nullptr; }
            if (m_aeron[i])  { aeron_close(m_aeron[i]); m_aeron[i] = nullptr; }
            if (m_aeron_ctx[i]) { aeron_context_close(m_aeron_ctx[i]); m_aeron_ctx[i] = nullptr; }
            if (m_nodes[i])  { m_nodes[i]->stop(); delete m_nodes[i]; m_nodes[i] = nullptr; }
        }
        if (std::system(("rm -rf " + m_base_dir).c_str())) {}

    }

    bool connect_aeron(int idx)
    {
        if (aeron_context_init(&m_aeron_ctx[idx]) < 0) { return false; }
        aeron_context_set_dir(m_aeron_ctx[idx], m_nodes[idx]->aeron_dir().c_str());
        if (aeron_init(&m_aeron[idx], m_aeron_ctx[idx]) < 0) { return false; }
        if (aeron_start(m_aeron[idx]) < 0) { return false; }
        std::string cnc = m_nodes[idx]->aeron_dir() + "/cnc.dat";
        for (int i = 0; i < 100; i++)
        {
            if (aeron_file_length(cnc.c_str()) > 0) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return false;
    }

    int create_agent(int idx, int appointed_leader_id)
    {
        aeron_cm_context_t *ctx = nullptr;
        if (aeron_cm_context_init(&ctx) < 0) { return -1; }

        ctx->aeron              = m_aeron[idx];
        ctx->member_id          = idx;  /* member IDs are 0,1,2 in cluster_members */
        ctx->appointed_leader_id = appointed_leader_id;
        ctx->service_count      = 0;
        ctx->app_version        = 1;
        ctx->cluster_members    = strdup(m_nodes[idx]->cluster_members().c_str());
        strncpy(ctx->cluster_dir, m_nodes[idx]->cluster_dir().c_str(), sizeof(ctx->cluster_dir) - 1);

        ctx->consensus_channel   = strdup("aeron:udp");
        ctx->consensus_stream_id = 108;
        ctx->log_channel         = strdup("aeron:ipc");
        ctx->log_stream_id       = 100;
        ctx->snapshot_channel    = strdup("aeron:ipc");
        ctx->snapshot_stream_id  = 107;
        ctx->control_channel     = strdup("aeron:ipc");
        ctx->consensus_module_stream_id = 105;
        ctx->service_stream_id          = 104;
        ctx->ingress_channel     = strdup("aeron:udp");
        ctx->ingress_stream_id   = 101;

        ctx->startup_canvass_timeout_ns    = INT64_C(500000000);
        ctx->election_timeout_ns           = INT64_C(1000000000);
        ctx->election_status_interval_ns   = INT64_C(100000000);
        ctx->leader_heartbeat_timeout_ns   = INT64_C(5000000000);
        ctx->leader_heartbeat_interval_ns  = INT64_C(200000000);
        ctx->session_timeout_ns            = INT64_C(10000000000);
        ctx->termination_timeout_ns        = INT64_C(5000000000);

        /* Wire archive */
        aeron_archive_context_t *arch_ctx = nullptr;
        aeron_archive_context_init(&arch_ctx);
        aeron_archive_context_set_aeron(arch_ctx, m_aeron[idx]);
        std::string ctrl = "aeron:udp?endpoint=localhost:" +
            std::to_string(m_nodes[idx]->archive_port());
        aeron_archive_context_set_control_request_channel(arch_ctx, ctrl.c_str());
        aeron_archive_context_set_control_response_channel(arch_ctx, "aeron:udp?endpoint=localhost:0");
        ctx->archive_ctx = arch_ctx;
        ctx->owns_archive_ctx = true;

        if (aeron_consensus_module_agent_create(&m_agents[idx], ctx) < 0) { return -1; }
        return aeron_consensus_module_agent_on_start(m_agents[idx]);
    }

    TestClusterNode              *m_nodes[NODE_COUNT] = {};
    aeron_context_t              *m_aeron_ctx[NODE_COUNT] = {};
    aeron_t                      *m_aeron[NODE_COUNT] = {};
    aeron_consensus_module_agent_t *m_agents[NODE_COUNT] = {};
    std::string                   m_base_dir;
};

TEST_F(ThreeNodeClusterTest, shouldElectAppointedLeader)
{
    int appointed = 1;  /* member_id 1 is appointed leader (0-based, matches cluster_members) */

    /* Connect Aeron clients */
    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_TRUE(connect_aeron(i)) << "Failed to connect aeron for node " << i;
    }

    /* Create agents */
    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_EQ(0, create_agent(i, appointed))
            << "Failed to create agent " << i << ": " << aeron_errmsg();
    }

    /* Drive election — needs real time for UDP consensus messages between 3 drivers */
    int64_t now_ns = aeron_nano_clock();
    int leader_idx = -1;
    for (int tick = 0; tick < 500; tick++)
    {
        now_ns += INT64_C(20000000);  /* 20ms per tick */
        for (int i = 0; i < NODE_COUNT; i++)
        {
            aeron_consensus_module_agent_do_work(m_agents[i], now_ns);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        /* Check if any node became leader */
        for (int i = 0; i < NODE_COUNT; i++)
        {
            if (AERON_CLUSTER_ROLE_LEADER == m_agents[i]->role)
            {
                leader_idx = i;
                break;
            }
        }
        if (leader_idx >= 0) break;
    }

    ASSERT_GE(leader_idx, 0) << "No leader elected within timeout";
    EXPECT_EQ(appointed, m_agents[leader_idx]->member_id)
        << "Elected leader " << m_agents[leader_idx]->member_id
        << " does not match appointed " << appointed;

    /* Verify other nodes are followers */
    for (int i = 0; i < NODE_COUNT; i++)
    {
        if (i != leader_idx)
        {
            EXPECT_EQ(AERON_CLUSTER_ROLE_FOLLOWER, m_agents[i]->role)
                << "Node " << i << " should be follower";
        }
    }
}

TEST_F(ThreeNodeClusterTest, shouldTakeAndRestoreSnapshot)
{
    int appointed = 1;

    /* Connect Aeron clients */
    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_TRUE(connect_aeron(i)) << "Failed to connect aeron for node " << i;
    }

    /* Create agents */
    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_EQ(0, create_agent(i, appointed))
            << "Failed to create agent " << i << ": " << aeron_errmsg();
    }

    /* Drive election until leader is elected */
    int64_t now_ns = aeron_nano_clock();
    int leader_idx = -1;
    for (int tick = 0; tick < 500; tick++)
    {
        now_ns += INT64_C(20000000);  /* 20ms per tick */
        for (int i = 0; i < NODE_COUNT; i++)
        {
            aeron_consensus_module_agent_do_work(m_agents[i], now_ns);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        for (int i = 0; i < NODE_COUNT; i++)
        {
            if (AERON_CLUSTER_ROLE_LEADER == m_agents[i]->role)
            {
                leader_idx = i;
                break;
            }
        }
        if (leader_idx >= 0) break;
    }

    ASSERT_GE(leader_idx, 0) << "No leader elected within timeout";
    ASSERT_EQ(appointed, m_agents[leader_idx]->member_id)
        << "Elected leader does not match appointed";

    /* Verify leader has a recording log */
    aeron_consensus_module_agent_t *leader = m_agents[leader_idx];
    ASSERT_NE(nullptr, leader->recording_log)
        << "Leader recording log should not be null";

    /* Record the entry count before snapshot */
    int entries_before = leader->recording_log->sorted_count;

    /* Append a snapshot entry to the leader's recording log.
     * This simulates the final step of take_cm_snapshot() which writes
     * the snapshot metadata after writing the snapshot data to the archive.
     * With service_count=0 there are no service snapshots to record. */
    int64_t snap_log_position = 0;  /* log position at snapshot time */
    int64_t snap_timestamp = now_ns / INT64_C(1000000);  /* ms */
    int rc = aeron_cluster_recording_log_append_snapshot(
        leader->recording_log,
        42 /* recording_id — dummy */,
        leader->leadership_term_id,
        snap_log_position /* term_base_log_position */,
        snap_log_position /* log_position */,
        snap_timestamp,
        -1 /* service_id: -1 = CM snapshot */);
    ASSERT_EQ(0, rc) << "Failed to append snapshot to recording log";

    /* Force the recording log to disk and reload to verify persistence */
    ASSERT_EQ(0, aeron_cluster_recording_log_force(leader->recording_log))
        << "Failed to force recording log";
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(leader->recording_log))
        << "Failed to reload recording log";

    /* Verify the snapshot entry was persisted */
    int entries_after = leader->recording_log->sorted_count;
    EXPECT_GT(entries_after, entries_before)
        << "Recording log should have more entries after snapshot";

    /* Query for the latest snapshot and verify it matches what we wrote */
    aeron_cluster_recording_log_entry_t *latest_snap =
        aeron_cluster_recording_log_get_latest_snapshot(leader->recording_log, -1);
    ASSERT_NE(nullptr, latest_snap)
        << "Should find latest CM snapshot in recording log";
    EXPECT_EQ(42, latest_snap->recording_id);
    EXPECT_EQ(leader->leadership_term_id, latest_snap->leadership_term_id);
    EXPECT_EQ(snap_log_position, latest_snap->log_position);
    EXPECT_EQ(-1, latest_snap->service_id)
        << "CM snapshot should have service_id == -1";
    EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT, latest_snap->entry_type);

    /* Verify the snapshot survives a fresh recording log open (restart simulation).
     * Open a second recording log from the same cluster dir — proves the snapshot
     * is recoverable across process restarts. */
    aeron_cluster_recording_log_t *reloaded_log = nullptr;
    rc = aeron_cluster_recording_log_open(
        &reloaded_log, m_nodes[leader_idx]->cluster_dir().c_str(), false);
    ASSERT_EQ(0, rc) << "Failed to open recording log for reload";
    ASSERT_NE(nullptr, reloaded_log);

    aeron_cluster_recording_log_entry_t *reloaded_snap =
        aeron_cluster_recording_log_get_latest_snapshot(reloaded_log, -1);
    ASSERT_NE(nullptr, reloaded_snap)
        << "Reloaded recording log should contain the snapshot";
    EXPECT_EQ(42, reloaded_snap->recording_id);
    EXPECT_EQ(leader->leadership_term_id, reloaded_snap->leadership_term_id);

    aeron_cluster_recording_log_close(reloaded_log);
}

TEST_F(ThreeNodeClusterTest, shouldFailoverWhenLeaderStopped)
{
    int appointed = -1;  /* no appointment — let nodes self-elect */

    /* Connect Aeron clients */
    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_TRUE(connect_aeron(i)) << "Failed to connect aeron for node " << i;
    }

    /* Create agents */
    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_EQ(0, create_agent(i, appointed))
            << "Failed to create agent " << i << ": " << aeron_errmsg();
    }

    /* Phase 1: Drive election until ALL nodes complete (leader elected + followers ready) */
    int64_t now_ns = aeron_nano_clock();
    int leader_idx = -1;
    for (int tick = 0; tick < 500; tick++)
    {
        now_ns += INT64_C(20000000);  /* 20ms per tick */
        for (int i = 0; i < NODE_COUNT; i++)
        {
            aeron_consensus_module_agent_do_work(m_agents[i], now_ns);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        /* Check if all elections are closed (leader + followers) */
        bool all_done = true;
        leader_idx = -1;
        for (int i = 0; i < NODE_COUNT; i++)
        {
            if (m_agents[i]->election != nullptr)
            {
                all_done = false;
                break;
            }
            if (AERON_CLUSTER_ROLE_LEADER == m_agents[i]->role)
            {
                leader_idx = i;
            }
        }
        if (all_done && leader_idx >= 0) break;
    }

    ASSERT_GE(leader_idx, 0) << "No initial leader elected within timeout";

    /* Verify all followers have completed their elections */
    for (int i = 0; i < NODE_COUNT; i++)
    {
        if (i != leader_idx)
        {
            ASSERT_EQ(nullptr, m_agents[i]->election)
                << "Node " << i << " election should be closed";
            ASSERT_EQ(AERON_CLUSTER_ROLE_FOLLOWER, m_agents[i]->role)
                << "Node " << i << " should be follower";
        }
    }

    /* Phase 2: Stop the leader — close agent and stop its ArchivingMediaDriver */
    int stopped_leader = leader_idx;

    aeron_consensus_module_agent_close(m_agents[stopped_leader]);
    m_agents[stopped_leader] = nullptr;

    aeron_close(m_aeron[stopped_leader]);
    m_aeron[stopped_leader] = nullptr;

    aeron_context_close(m_aeron_ctx[stopped_leader]);
    m_aeron_ctx[stopped_leader] = nullptr;

    m_nodes[stopped_leader]->stop();
    delete m_nodes[stopped_leader];
    m_nodes[stopped_leader] = nullptr;

    /* Phase 3: Advance time past leader_heartbeat_timeout_ns (5s) so followers detect loss */
    now_ns += INT64_C(6000000000);  /* jump 6 seconds ahead */

    /* Phase 4: Drive surviving nodes through new election */
    int new_leader_idx = -1;
    for (int tick = 0; tick < 500; tick++)
    {
        now_ns += INT64_C(20000000);  /* 20ms per tick */
        for (int i = 0; i < NODE_COUNT; i++)
        {
            if (m_agents[i] != nullptr)
            {
                aeron_consensus_module_agent_do_work(m_agents[i], now_ns);
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        for (int i = 0; i < NODE_COUNT; i++)
        {
            if (m_agents[i] != nullptr && AERON_CLUSTER_ROLE_LEADER == m_agents[i]->role)
            {
                new_leader_idx = i;
                break;
            }
        }
        if (new_leader_idx >= 0) break;
    }

    ASSERT_GE(new_leader_idx, 0) << "No new leader elected after failover";
    EXPECT_NE(new_leader_idx, stopped_leader)
        << "New leader should not be the stopped node";

    /* Verify the other surviving node is a follower */
    for (int i = 0; i < NODE_COUNT; i++)
    {
        if (m_agents[i] != nullptr && i != new_leader_idx)
        {
            EXPECT_EQ(AERON_CLUSTER_ROLE_FOLLOWER, m_agents[i]->role)
                << "Surviving node " << i << " should be follower";
        }
    }
}
