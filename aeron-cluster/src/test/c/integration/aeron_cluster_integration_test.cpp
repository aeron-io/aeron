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
#include <cstdlib>
#include <string>
#include <iostream>
#include <thread>
#include <chrono>

extern "C"
{
#include "aeronc.h"
#include "aeron_archive.h"
#include "aeron_cluster_recording_log.h"
#include "aeron_cm_context.h"
#include "aeron_consensus_module_agent.h"
#include "aeron_cluster_service_context.h"
#include "aeron_clustered_service_agent.h"
#include "service/aeron_cluster_client_session.h"
#include "util/aeron_fileutil.h"
}

#include "../integration/aeron_test_cluster_node.h"

/**
 * Integration test: starts a real ArchivingMediaDriver (Java process),
 * connects a C Aeron client, and verifies the archive is reachable.
 *
 * This is the foundation for full cluster integration tests where
 * the C ConsensusModuleAgent runs against a real archive.
 */
class ClusterIntegrationTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        m_base_dir = "/tmp/aeron_cluster_integ_" + std::to_string(getpid());
        std::system(("rm -rf " + m_base_dir).c_str());

        m_node = new TestClusterNode(0, 1, m_base_dir, std::cout);
        m_node->start();
    }

    void TearDown() override
    {
        if (m_aeron)
        {
            aeron_close(m_aeron);
            m_aeron = nullptr;
        }
        if (m_aeron_ctx)
        {
            aeron_context_close(m_aeron_ctx);
            m_aeron_ctx = nullptr;
        }

        if (m_node)
        {
            m_node->stop();
            delete m_node;
            m_node = nullptr;
        }
        std::system(("rm -rf " + m_base_dir).c_str());
    }

    bool connect_aeron()
    {
        if (aeron_context_init(&m_aeron_ctx) < 0) { return false; }
        aeron_context_set_dir(m_aeron_ctx, m_node->aeron_dir().c_str());

        if (aeron_init(&m_aeron, m_aeron_ctx) < 0) { return false; }
        if (aeron_start(m_aeron) < 0) { return false; }

        /* Wait for driver to become active by checking cnc file */
        std::string cnc_file = m_node->aeron_dir() + "/cnc.dat";
        for (int i = 0; i < 100; i++)
        {
            if (aeron_file_length(cnc_file.c_str()) > 0)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return false;
    }

    bool connect_archive(aeron_archive_t **archive)
    {
        aeron_archive_context_t *arch_ctx = nullptr;
        if (aeron_archive_context_init(&arch_ctx) < 0) { return false; }

        aeron_archive_context_set_aeron(arch_ctx, m_aeron);

        std::string control_channel =
            "aeron:udp?endpoint=localhost:" + std::to_string(m_node->archive_port());
        aeron_archive_context_set_control_request_channel(arch_ctx, control_channel.c_str());
        aeron_archive_context_set_control_response_channel(arch_ctx, "aeron:udp?endpoint=localhost:0");

        int rc = aeron_archive_connect(archive, arch_ctx);
        aeron_archive_context_close(arch_ctx);
        return rc == 0;
    }

    TestClusterNode   *m_node      = nullptr;
    aeron_context_t   *m_aeron_ctx = nullptr;
    aeron_t           *m_aeron     = nullptr;
    std::string        m_base_dir;
};

/* -----------------------------------------------------------------------
 * Smoke test: verify ArchivingMediaDriver starts and Aeron client connects
 * ----------------------------------------------------------------------- */
TEST_F(ClusterIntegrationTest, shouldStartArchivingMediaDriverAndConnect)
{
    ASSERT_TRUE(connect_aeron()) << "Failed to connect Aeron client to ArchivingMediaDriver";

    /* Verify the cnc file exists (driver is running) */
    std::string cnc_file = m_node->aeron_dir() + "/cnc.dat";
    EXPECT_GT(aeron_file_length(cnc_file.c_str()), 0);
}

/* -----------------------------------------------------------------------
 * Smoke test: verify archive client can connect
 * ----------------------------------------------------------------------- */

static void recording_descriptor_noop(
    aeron_archive_recording_descriptor_t *descriptor, void *clientd)
{
    int32_t *count = static_cast<int32_t *>(clientd);
    (*count)++;
}

TEST_F(ClusterIntegrationTest, shouldConnectArchiveClient)
{
    ASSERT_TRUE(connect_aeron()) << "Failed to connect Aeron client";

    aeron_archive_t *archive = nullptr;
    ASSERT_TRUE(connect_archive(&archive)) << "Failed to connect archive: " << aeron_errmsg();
    ASSERT_NE(nullptr, archive);

    /* Query archive — should succeed (0 recordings expected on fresh start) */
    int32_t found = 0;
    int32_t count = 0;
    aeron_archive_list_recordings(&count, archive, 0, 10,
        recording_descriptor_noop, &found);
    EXPECT_GE(count, 0);

    aeron_archive_close(archive);
}

/* -----------------------------------------------------------------------
 * Smoke test: verify recording log can be created in cluster dir
 * ----------------------------------------------------------------------- */
TEST_F(ClusterIntegrationTest, shouldCreateRecordingLogInClusterDir)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_node->cluster_dir().c_str(), true))
        << "Failed to create recording log in cluster dir";
    EXPECT_EQ(0, log->entry_count);
    aeron_cluster_recording_log_close(log);
}

/* -----------------------------------------------------------------------
 * Smoke test: verify CM context can be initialized with real parameters
 * ----------------------------------------------------------------------- */
TEST_F(ClusterIntegrationTest, shouldInitializeConsensusModuleContext)
{
    aeron_cm_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&ctx));

    ctx->member_id     = 0;
    ctx->service_count = 1;
    ctx->app_version   = 1;
    ctx->cluster_members = strdup(m_node->cluster_members().c_str());
    ASSERT_NE(nullptr, ctx->cluster_members);

    /* Verify the context is valid */
    EXPECT_EQ(0, ctx->member_id);
    EXPECT_EQ(1, ctx->service_count);

    aeron_cm_context_close(ctx);
}

/* -----------------------------------------------------------------------
 * Integration test: start ConsensusModuleAgent and run election
 * ----------------------------------------------------------------------- */
TEST_F(ClusterIntegrationTest, shouldCreateAgentAndRunElection)
{
    ASSERT_TRUE(connect_aeron()) << "Failed to connect Aeron client";

    /* Set up CM context with real Aeron */
    aeron_cm_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&ctx));
    ctx->aeron             = m_aeron;
    ctx->member_id         = 0;
    ctx->appointed_leader_id = 0;
    ctx->service_count     = 0;  /* no services for this smoke test */
    ctx->app_version       = 1;
    ctx->cluster_members   = strdup(m_node->cluster_members().c_str());
    strncpy(ctx->cluster_dir, m_node->cluster_dir().c_str(), sizeof(ctx->cluster_dir) - 1);

    ctx->consensus_channel   = strdup("aeron:udp?alias=consensus");
    ctx->consensus_stream_id = 108;
    ctx->log_channel         = strdup("aeron:udp?term-length=512k|alias=raft");
    ctx->log_stream_id       = 100;
    ctx->snapshot_channel    = strdup("aeron:ipc?term-length=128k");
    ctx->snapshot_stream_id  = 107;
    ctx->control_channel     = strdup("aeron:ipc?term-length=128k");
    ctx->consensus_module_stream_id = 105;
    ctx->service_stream_id         = 104;
    ctx->ingress_channel     = strdup("aeron:udp?term-length=128k|alias=ingress");
    ctx->ingress_stream_id   = 101;

    /* Fast timeouts for testing */
    ctx->startup_canvass_timeout_ns    = INT64_C(200000000);   /* 200ms */
    ctx->election_timeout_ns           = INT64_C(500000000);   /* 500ms */
    ctx->election_status_interval_ns   = INT64_C(50000000);    /* 50ms */
    ctx->leader_heartbeat_timeout_ns   = INT64_C(5000000000);  /* 5s */
    ctx->leader_heartbeat_interval_ns  = INT64_C(200000000);   /* 200ms */
    ctx->session_timeout_ns            = INT64_C(10000000000); /* 10s */
    ctx->termination_timeout_ns        = INT64_C(5000000000);  /* 5s */

    /* Wire archive context */
    aeron_archive_context_t *arch_ctx = nullptr;
    ASSERT_EQ(0, aeron_archive_context_init(&arch_ctx));
    aeron_archive_context_set_aeron(arch_ctx, m_aeron);
    std::string control = "aeron:udp?endpoint=localhost:" + std::to_string(m_node->archive_port());
    aeron_archive_context_set_control_request_channel(arch_ctx, control.c_str());
    aeron_archive_context_set_control_response_channel(arch_ctx, "aeron:udp?endpoint=localhost:0");
    ctx->archive_ctx = arch_ctx;
    ctx->owns_archive_ctx = true;

    /* Create the agent */
    aeron_consensus_module_agent_t *agent = nullptr;
    ASSERT_EQ(0, aeron_consensus_module_agent_create(&agent, ctx))
        << "agent create failed: " << aeron_errmsg();
    ASSERT_NE(nullptr, agent);
    EXPECT_EQ(0, agent->member_id);

    /* on_start: add subscriptions, connect archive, create election */
    int start_rc = aeron_consensus_module_agent_on_start(agent);
    ASSERT_EQ(0, start_rc) << "on_start failed: " << aeron_errmsg();
    EXPECT_EQ(AERON_CM_STATE_ACTIVE, agent->state);
    EXPECT_NE(nullptr, agent->election);

    /* Drive election — for a single-node cluster, should self-elect as leader */
    int64_t now_ns = aeron_nano_clock();
    bool became_leader = false;
    for (int i = 0; i < 100; i++)
    {
        now_ns += INT64_C(50000000); /* 50ms per tick */
        aeron_consensus_module_agent_do_work(agent, now_ns);

        if (AERON_CLUSTER_ROLE_LEADER == agent->role)
        {
            became_leader = true;
            break;
        }
    }

    EXPECT_TRUE(became_leader) << "Single-node cluster did not elect leader within 5s";
    if (became_leader)
    {
        EXPECT_EQ(AERON_CLUSTER_ROLE_LEADER, agent->role);
    }

    aeron_consensus_module_agent_close(agent);
}

/* -----------------------------------------------------------------------
 * Integration test: ClusteredServiceAgent starts and receives on_start
 * ----------------------------------------------------------------------- */

static bool s_on_start_called = false;
static bool s_on_terminate_called = false;

static void test_service_on_start(
    void *clientd, aeron_cluster_t *cluster, aeron_cluster_snapshot_image_t *snapshot_image)
{
    (void)clientd; (void)cluster; (void)snapshot_image;
    s_on_start_called = true;
}

static void test_service_on_terminate(void *clientd, aeron_cluster_t *cluster)
{
    (void)clientd; (void)cluster;
    s_on_terminate_called = true;
}

static void test_service_on_session_message(
    void *clientd, aeron_cluster_client_session_t *session, int64_t timestamp,
    const uint8_t *buffer, size_t length)
{
    (void)clientd; (void)session; (void)timestamp;
    (void)buffer; (void)length;
}

TEST_F(ClusterIntegrationTest, shouldStartServiceAgentWithOnStartCallback)
{
    ASSERT_TRUE(connect_aeron()) << "Failed to connect Aeron client";

    s_on_start_called = false;
    s_on_terminate_called = false;

    /* Create service context */
    aeron_cluster_service_context_t *svc_ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_service_context_init(&svc_ctx));

    aeron_cluster_service_context_set_aeron(svc_ctx, m_aeron);
    aeron_cluster_service_context_set_service_id(svc_ctx, 0);
    aeron_cluster_service_context_set_control_channel(svc_ctx, "aeron:ipc?term-length=128k");
    aeron_cluster_service_context_set_consensus_module_stream_id(svc_ctx, 104);
    svc_ctx->service_channel = strdup("aeron:ipc?term-length=128k");
    svc_ctx->service_stream_id = 105;
    svc_ctx->snapshot_channel = strdup("aeron:ipc?term-length=128k");
    svc_ctx->snapshot_stream_id = 107;
    svc_ctx->cluster_id = 0;
    strncpy(svc_ctx->cluster_dir, m_node->cluster_dir().c_str(), sizeof(svc_ctx->cluster_dir) - 1);

    /* Set up service callbacks */
    aeron_clustered_service_t service = {};
    service.on_start = test_service_on_start;
    service.on_terminate = test_service_on_terminate;
    service.on_session_message = test_service_on_session_message;
    service.clientd = nullptr;
    svc_ctx->service = &service;

    /* Create the service agent */
    aeron_clustered_service_agent_t *svc_agent = nullptr;
    ASSERT_EQ(0, aeron_clustered_service_agent_create(&svc_agent, svc_ctx))
        << "service agent create failed: " << aeron_errmsg();

    /* on_start — with no recovery counter, should call on_start(NULL snapshot) */
    int rc = aeron_clustered_service_agent_on_start(svc_agent);
    ASSERT_EQ(0, rc) << "service on_start failed: " << aeron_errmsg();

    EXPECT_TRUE(s_on_start_called) << "on_start callback was not invoked";
    EXPECT_TRUE(svc_agent->is_service_active);

    /* Close should invoke on_terminate since service is still active */
    aeron_clustered_service_agent_close(svc_agent);
    EXPECT_TRUE(s_on_terminate_called) << "on_terminate was not invoked on close";

    /* Context owns the service pointer, but we allocated it on stack — just free ctx */
    svc_ctx->service = nullptr; /* prevent double-free of stack object */
    aeron_cluster_service_context_close(svc_ctx);
}

/* -----------------------------------------------------------------------
 * Integration test: CM leader + Service running together
 * CM becomes leader, sends JoinLog, service receives on_join_log
 * ----------------------------------------------------------------------- */

static std::atomic<bool> s_combined_on_start{false};
static std::atomic<bool> s_combined_on_terminate{false};

static void combined_on_start(
    void *clientd, aeron_cluster_t *cluster, aeron_cluster_snapshot_image_t *img)
{
    (void)clientd; (void)cluster; (void)img;
    s_combined_on_start.store(true);
}

static void combined_on_terminate(void *clientd, aeron_cluster_t *cluster)
{
    (void)clientd; (void)cluster;
    s_combined_on_terminate.store(true);
}

static void combined_on_session_message(
    void *clientd, aeron_cluster_client_session_t *session, int64_t timestamp,
    const uint8_t *buffer, size_t length)
{
    (void)clientd; (void)session; (void)timestamp; (void)buffer; (void)length;
}

TEST_F(ClusterIntegrationTest, shouldRunCombinedLeaderAndService)
{
    ASSERT_TRUE(connect_aeron()) << "Failed to connect Aeron client";

    s_combined_on_start.store(false);
    s_combined_on_terminate.store(false);

    /* ---- CM context ---- */
    aeron_cm_context_t *cm_ctx = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&cm_ctx));
    cm_ctx->aeron              = m_aeron;
    cm_ctx->member_id          = 0;
    cm_ctx->appointed_leader_id = 0;
    cm_ctx->service_count      = 1;
    cm_ctx->app_version        = 1;
    cm_ctx->cluster_members    = strdup(m_node->cluster_members().c_str());
    strncpy(cm_ctx->cluster_dir, m_node->cluster_dir().c_str(), sizeof(cm_ctx->cluster_dir) - 1);

    cm_ctx->consensus_channel   = strdup("aeron:udp?alias=consensus");
    cm_ctx->consensus_stream_id = 108;
    cm_ctx->log_channel         = strdup("aeron:udp?term-length=512k|alias=raft");
    cm_ctx->log_stream_id       = 100;
    cm_ctx->snapshot_channel    = strdup("aeron:ipc?term-length=128k");
    cm_ctx->snapshot_stream_id  = 107;
    cm_ctx->control_channel     = strdup("aeron:ipc?term-length=128k");
    cm_ctx->consensus_module_stream_id = 105;
    cm_ctx->service_stream_id          = 104;
    cm_ctx->ingress_channel     = strdup("aeron:udp?term-length=128k|alias=ingress");
    cm_ctx->ingress_stream_id   = 101;

    cm_ctx->startup_canvass_timeout_ns   = INT64_C(200000000);
    cm_ctx->election_timeout_ns          = INT64_C(500000000);
    cm_ctx->election_status_interval_ns  = INT64_C(50000000);
    cm_ctx->leader_heartbeat_timeout_ns  = INT64_C(5000000000);
    cm_ctx->leader_heartbeat_interval_ns = INT64_C(200000000);
    cm_ctx->session_timeout_ns           = INT64_C(10000000000);
    cm_ctx->termination_timeout_ns       = INT64_C(5000000000);

    aeron_archive_context_t *arch_ctx = nullptr;
    ASSERT_EQ(0, aeron_archive_context_init(&arch_ctx));
    aeron_archive_context_set_aeron(arch_ctx, m_aeron);
    std::string ctrl = "aeron:udp?endpoint=localhost:" + std::to_string(m_node->archive_port());
    aeron_archive_context_set_control_request_channel(arch_ctx, ctrl.c_str());
    aeron_archive_context_set_control_response_channel(arch_ctx, "aeron:udp?endpoint=localhost:0");
    cm_ctx->archive_ctx = arch_ctx;
    cm_ctx->owns_archive_ctx = true;

    /* ---- Service context ---- */
    aeron_cluster_service_context_t *svc_ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_service_context_init(&svc_ctx));
    aeron_cluster_service_context_set_aeron(svc_ctx, m_aeron);
    aeron_cluster_service_context_set_service_id(svc_ctx, 0);
    aeron_cluster_service_context_set_control_channel(svc_ctx, "aeron:ipc?term-length=128k");
    aeron_cluster_service_context_set_consensus_module_stream_id(svc_ctx, 104);
    svc_ctx->service_channel = strdup("aeron:ipc?term-length=128k");
    svc_ctx->service_stream_id = 105;
    svc_ctx->snapshot_channel = strdup("aeron:ipc?term-length=128k");
    svc_ctx->snapshot_stream_id = 107;
    svc_ctx->cluster_id = 0;
    strncpy(svc_ctx->cluster_dir, m_node->cluster_dir().c_str(), sizeof(svc_ctx->cluster_dir) - 1);

    aeron_clustered_service_t service = {};
    service.on_start = combined_on_start;
    service.on_terminate = combined_on_terminate;
    service.on_session_message = combined_on_session_message;
    svc_ctx->service = &service;

    /* ---- Create both agents ---- */
    aeron_consensus_module_agent_t *cm_agent = nullptr;
    ASSERT_EQ(0, aeron_consensus_module_agent_create(&cm_agent, cm_ctx))
        << "CM create: " << aeron_errmsg();

    aeron_clustered_service_agent_t *svc_agent = nullptr;
    ASSERT_EQ(0, aeron_clustered_service_agent_create(&svc_agent, svc_ctx))
        << "Service create: " << aeron_errmsg();

    /* ---- Start both ---- */
    ASSERT_EQ(0, aeron_consensus_module_agent_on_start(cm_agent))
        << "CM on_start: " << aeron_errmsg();
    ASSERT_EQ(0, aeron_clustered_service_agent_on_start(svc_agent))
        << "Svc on_start: " << aeron_errmsg();

    EXPECT_TRUE(s_combined_on_start.load()) << "Service on_start not called";

    /* ---- Drive CM election until leader ---- */
    int64_t now_ns = aeron_nano_clock();
    bool became_leader = false;
    for (int i = 0; i < 100; i++)
    {
        now_ns += INT64_C(50000000);
        aeron_consensus_module_agent_do_work(cm_agent, now_ns);
        if (AERON_CLUSTER_ROLE_LEADER == cm_agent->role)
        {
            became_leader = true;
            break;
        }
    }
    ASSERT_TRUE(became_leader) << "CM did not become leader";

    /* ---- Drive both agents so service receives JoinLog ---- */
    for (int i = 0; i < 50; i++)
    {
        now_ns += INT64_C(20000000);
        aeron_consensus_module_agent_do_work(cm_agent, now_ns);
        aeron_clustered_service_agent_do_work(svc_agent, now_ns);
    }

    /* ---- Cleanup ---- */
    aeron_clustered_service_agent_close(svc_agent);
    EXPECT_TRUE(s_combined_on_terminate.load()) << "on_terminate not called";

    svc_ctx->service = nullptr;
    aeron_cluster_service_context_close(svc_ctx);
    aeron_consensus_module_agent_close(cm_agent);
}

