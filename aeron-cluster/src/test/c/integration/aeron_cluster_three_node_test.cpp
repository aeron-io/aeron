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
 * 3-node cluster integration test -- matches Java ClusterNodeTest pattern.
 * Each node has its own TestClusterNode (driver+archive).
 * Each ConsensusModuleAgent creates its own Aeron client from the node's aeron_dir.
 * NO shared m_aeron array.
 */

#ifdef _MSC_VER
#ifndef _WINSOCKAPI_
#define _WINSOCKAPI_
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#endif

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

#ifdef _MSC_VER
static std::string make_test_dir(const char *prefix)
{
    char tmp[MAX_PATH];
    GetTempPathA(MAX_PATH, tmp);
    return std::string(tmp) + prefix + std::to_string(GetCurrentProcessId());
}
#else
static std::string make_test_dir(const char *prefix)
{
    return std::string("/tmp/") + prefix + std::to_string(getpid());
}
#endif

class ThreeNodeClusterTest : public ::testing::Test
{
protected:
    static constexpr int NODE_COUNT = 3;
    static constexpr int BASE_NODE_INDEX = 21;

    void SetUp() override
    {
        m_base_dir = make_test_dir("aeron_cluster_3node_");
#ifdef _MSC_VER
        if (std::system(("rmdir /s /q \"" + m_base_dir + "\" 2>nul").c_str())) {}
#else
        if (std::system(("rm -rf " + m_base_dir).c_str())) {}
#endif

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
            if (m_agents[i])
            {
                aeron_consensus_module_agent_close(m_agents[i]);
                m_agents[i] = nullptr;
            }
            if (m_cm_ctx[i])
            {
                aeron_cm_context_close(m_cm_ctx[i]);
                m_cm_ctx[i] = nullptr;
            }
            if (m_nodes[i])
            {
                m_nodes[i]->stop();
                delete m_nodes[i];
                m_nodes[i] = nullptr;
            }
        }
#ifdef _MSC_VER
        if (std::system(("rmdir /s /q \"" + m_base_dir + "\"").c_str())) {}
#else
        if (std::system(("rm -rf " + m_base_dir).c_str())) {}
#endif
    }

    /**
     * Create a CM agent for node idx. The CM context auto-creates its own
     * Aeron client from the node's aeron_dir via on_start auto-creation.
     */
    int create_agent(int idx, int appointed_leader_id)
    {
        aeron_cm_context_t *ctx = nullptr;
        if (aeron_cm_context_init(&ctx) < 0) { return -1; }

        /* Set aeron_directory_name -- on_start will auto-create Aeron client */
        snprintf(ctx->aeron_directory_name, sizeof(ctx->aeron_directory_name),
                 "%s", m_nodes[idx]->aeron_dir().c_str());
        ctx->member_id           = idx;
        ctx->appointed_leader_id = appointed_leader_id;
        ctx->service_count       = 0;
        ctx->app_version         = 1;

        if (ctx->cluster_members) { free(ctx->cluster_members); }
        ctx->cluster_members = strdup(m_nodes[idx]->cluster_members().c_str());
        strncpy(ctx->cluster_dir, m_nodes[idx]->cluster_dir().c_str(), sizeof(ctx->cluster_dir) - 1);

        if (ctx->consensus_channel) { free(ctx->consensus_channel); }
        ctx->consensus_channel   = strdup("aeron:udp");
        ctx->consensus_stream_id = 108;
        if (ctx->log_channel) { free(ctx->log_channel); }
        ctx->log_channel         = strdup("aeron:ipc");
        ctx->log_stream_id       = 100;
        if (ctx->snapshot_channel) { free(ctx->snapshot_channel); }
        ctx->snapshot_channel    = strdup("aeron:ipc");
        ctx->snapshot_stream_id  = 107;
        if (ctx->control_channel) { free(ctx->control_channel); }
        ctx->control_channel     = strdup("aeron:ipc");
        ctx->consensus_module_stream_id = 105;
        ctx->service_stream_id          = 104;
        if (ctx->ingress_channel) { free(ctx->ingress_channel); }
        ctx->ingress_channel     = strdup("aeron:udp");
        ctx->ingress_stream_id   = 101;

        ctx->startup_canvass_timeout_ns    = INT64_C(500000000);
        ctx->election_timeout_ns           = INT64_C(1000000000);
        ctx->election_status_interval_ns   = INT64_C(100000000);
        ctx->leader_heartbeat_timeout_ns   = INT64_C(5000000000);
        ctx->leader_heartbeat_interval_ns  = INT64_C(200000000);
        ctx->session_timeout_ns            = INT64_C(10000000000);
        ctx->termination_timeout_ns        = INT64_C(5000000000);

        /* Archive context -- auto-creates its own Aeron client from aeron_directory_name */
        aeron_archive_context_t *arch_ctx = nullptr;
        aeron_archive_context_init(&arch_ctx);
        aeron_archive_context_set_aeron_directory_name(arch_ctx, m_nodes[idx]->aeron_dir().c_str());
        aeron_archive_context_set_control_request_channel(arch_ctx, "aeron:ipc");
        aeron_archive_context_set_control_response_channel(arch_ctx, "aeron:ipc");
        ctx->archive_ctx = arch_ctx;
        ctx->owns_archive_ctx = true;

        m_cm_ctx[idx] = ctx;

        if (aeron_consensus_module_agent_create(&m_agents[idx], ctx) < 0) { return -1; }
        return aeron_consensus_module_agent_on_start(m_agents[idx]);
    }

    TestClusterNode                *m_nodes[NODE_COUNT] = {};
    aeron_cm_context_t             *m_cm_ctx[NODE_COUNT] = {};
    aeron_consensus_module_agent_t *m_agents[NODE_COUNT] = {};
    std::string                     m_base_dir;
};

/* DISABLED: The in-process C archive server has a double-free bug in
 * aeron_archive_control_session_do_work -> aeron_async_resource_poll
 * when an archive client connects via IPC. Re-enable once fixed. */
TEST_F(ThreeNodeClusterTest, DISABLED_shouldElectAppointedLeader)
{
    int appointed = 1;

    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_EQ(0, create_agent(i, appointed))
            << "Failed to create agent " << i << ": " << aeron_errmsg();
    }

    /* Drive election */
    int64_t now_ns = aeron_nano_clock();
    int leader_idx = -1;
    for (int tick = 0; tick < 500; tick++)
    {
        now_ns += INT64_C(20000000);
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
    EXPECT_EQ(appointed, m_agents[leader_idx]->member_id)
        << "Elected leader " << m_agents[leader_idx]->member_id
        << " does not match appointed " << appointed;

    for (int i = 0; i < NODE_COUNT; i++)
    {
        if (i != leader_idx)
        {
            EXPECT_EQ(AERON_CLUSTER_ROLE_FOLLOWER, m_agents[i]->role)
                << "Node " << i << " should be follower";
        }
    }
}

/* DISABLED: Same C archive server IPC double-free bug. */
TEST_F(ThreeNodeClusterTest, DISABLED_shouldTakeAndRestoreSnapshot)
{
    int appointed = 1;

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
        now_ns += INT64_C(20000000);
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

    aeron_consensus_module_agent_t *leader = m_agents[leader_idx];
    ASSERT_NE(nullptr, leader->recording_log)
        << "Leader recording log should not be null";

    int entries_before = leader->recording_log->sorted_count;

    int64_t snap_log_position = 0;
    int64_t snap_timestamp = now_ns / INT64_C(1000000);
    int rc = aeron_cluster_recording_log_append_snapshot(
        leader->recording_log,
        42,
        leader->leadership_term_id,
        snap_log_position,
        snap_log_position,
        snap_timestamp,
        -1);
    ASSERT_EQ(0, rc) << "Failed to append snapshot to recording log";

    ASSERT_EQ(0, aeron_cluster_recording_log_force(leader->recording_log))
        << "Failed to force recording log";
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(leader->recording_log))
        << "Failed to reload recording log";

    int entries_after = leader->recording_log->sorted_count;
    EXPECT_GT(entries_after, entries_before)
        << "Recording log should have more entries after snapshot";

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

/* DISABLED: Same C archive server IPC double-free bug. */
TEST_F(ThreeNodeClusterTest, DISABLED_shouldFailoverWhenLeaderStopped)
{
    int appointed = -1;

    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_EQ(0, create_agent(i, appointed))
            << "Failed to create agent " << i << ": " << aeron_errmsg();
    }

    /* Phase 1: Drive election until ALL nodes complete */
    int64_t now_ns = aeron_nano_clock();
    int leader_idx = -1;
    for (int tick = 0; tick < 500; tick++)
    {
        now_ns += INT64_C(20000000);
        for (int i = 0; i < NODE_COUNT; i++)
        {
            aeron_consensus_module_agent_do_work(m_agents[i], now_ns);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

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

    /* Phase 2: Stop the leader -- close agent, context, and driver */
    int stopped_leader = leader_idx;

    aeron_consensus_module_agent_close(m_agents[stopped_leader]);
    m_agents[stopped_leader] = nullptr;

    /* CM context close also closes the CM's auto-created Aeron client */
    aeron_cm_context_close(m_cm_ctx[stopped_leader]);
    m_cm_ctx[stopped_leader] = nullptr;

    m_nodes[stopped_leader]->stop();
    delete m_nodes[stopped_leader];
    m_nodes[stopped_leader] = nullptr;

    /* Phase 3: Advance time past leader_heartbeat_timeout_ns (5s) */
    now_ns += INT64_C(6000000000);

    /* Phase 4: Drive surviving nodes through new election */
    int new_leader_idx = -1;
    for (int tick = 0; tick < 500; tick++)
    {
        now_ns += INT64_C(20000000);
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

    for (int i = 0; i < NODE_COUNT; i++)
    {
        if (m_agents[i] != nullptr && i != new_leader_idx)
        {
            EXPECT_EQ(AERON_CLUSTER_ROLE_FOLLOWER, m_agents[i]->role)
                << "Surviving node " << i << " should be follower";
        }
    }
}
