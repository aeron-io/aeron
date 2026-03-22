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
 * C port of Java ConsensusModuleContextTest.
 *
 * Tests verify default values, env-var overrides, setter/getter round-trips,
 * and conclude() validation — no Aeron driver required.
 *
 * Status per Java test:
 *   ✅ Implemented below
 *   🔶 Requires ClusterMarkFile (not yet in C — tracked as TODO)
 *   🔶 Requires counter infrastructure (not yet in C)
 */

#include <gtest/gtest.h>
#include <cstring>
#include <cstdlib>
#include <string>

extern "C"
{
#include "aeron_cm_context.h"
#include "aeron_consensus_module_configuration.h"
#include "aeron_alloc.h"
#include "aeron_cluster_service_context.h"
}

class ConsensusModuleContextTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        ASSERT_EQ(0, aeron_cm_context_init(&m_ctx));
    }
    void TearDown() override
    {
        aeron_cm_context_close(m_ctx);
        m_ctx = nullptr;
    }

    aeron_cm_context_t *m_ctx = nullptr;
};

/* -----------------------------------------------------------------------
 * Default value tests
 * ----------------------------------------------------------------------- */

TEST_F(ConsensusModuleContextTest, shouldInitializeWithDefaultMemberId)
{
    EXPECT_EQ(AERON_CM_MEMBER_ID_DEFAULT, m_ctx->member_id);
}

TEST_F(ConsensusModuleContextTest, shouldInitializeWithDefaultServiceCount)
{
    EXPECT_EQ(AERON_CM_SERVICE_COUNT_DEFAULT, m_ctx->service_count);
}

TEST_F(ConsensusModuleContextTest, shouldInitializeWithDefaultSessionTimeout)
{
    EXPECT_EQ(AERON_CM_SESSION_TIMEOUT_NS_DEFAULT, m_ctx->session_timeout_ns);
}

TEST_F(ConsensusModuleContextTest, shouldInitializeWithDefaultLeaderHeartbeatTimeout)
{
    EXPECT_EQ(AERON_CM_LEADER_HEARTBEAT_TIMEOUT_NS_DEFAULT, m_ctx->leader_heartbeat_timeout_ns);
}

TEST_F(ConsensusModuleContextTest, shouldInitializeWithDefaultLeaderHeartbeatInterval)
{
    EXPECT_EQ(AERON_CM_LEADER_HEARTBEAT_INTERVAL_NS_DEFAULT, m_ctx->leader_heartbeat_interval_ns);
}

TEST_F(ConsensusModuleContextTest, shouldInitializeWithDefaultElectionTimeout)
{
    EXPECT_EQ(AERON_CM_ELECTION_TIMEOUT_NS_DEFAULT, m_ctx->election_timeout_ns);
}

TEST_F(ConsensusModuleContextTest, shouldInitializeWithDefaultStartupCanvassTimeout)
{
    EXPECT_EQ(AERON_CM_STARTUP_CANVASS_TIMEOUT_NS_DEFAULT, m_ctx->startup_canvass_timeout_ns);
}

TEST_F(ConsensusModuleContextTest, shouldInitializeWithDefaultLogChannel)
{
    ASSERT_NE(nullptr, m_ctx->log_channel);
    EXPECT_STREQ(AERON_CM_LOG_CHANNEL_DEFAULT, m_ctx->log_channel);
}

TEST_F(ConsensusModuleContextTest, shouldInitializeWithDefaultIngressChannel)
{
    ASSERT_NE(nullptr, m_ctx->ingress_channel);
    EXPECT_STREQ(AERON_CM_INGRESS_CHANNEL_DEFAULT, m_ctx->ingress_channel);
}

TEST_F(ConsensusModuleContextTest, shouldInitializeWithDefaultConsensusChannel)
{
    ASSERT_NE(nullptr, m_ctx->consensus_channel);
    EXPECT_STREQ(AERON_CM_CONSENSUS_CHANNEL_DEFAULT, m_ctx->consensus_channel);
}

TEST_F(ConsensusModuleContextTest, shouldInitializeWithDefaultControlChannel)
{
    ASSERT_NE(nullptr, m_ctx->control_channel);
    EXPECT_STREQ(AERON_CM_CONTROL_CHANNEL_DEFAULT, m_ctx->control_channel);
}

TEST_F(ConsensusModuleContextTest, shouldInitializeWithDefaultStreamIds)
{
    EXPECT_EQ(AERON_CM_LOG_STREAM_ID_DEFAULT,               m_ctx->log_stream_id);
    EXPECT_EQ(AERON_CM_INGRESS_STREAM_ID_DEFAULT,            m_ctx->ingress_stream_id);
    EXPECT_EQ(AERON_CM_CONSENSUS_STREAM_ID_DEFAULT,          m_ctx->consensus_stream_id);
    EXPECT_EQ(AERON_CM_CONSENSUS_MODULE_STREAM_ID_DEFAULT,   m_ctx->consensus_module_stream_id);
    EXPECT_EQ(AERON_CM_SERVICE_STREAM_ID_DEFAULT,            m_ctx->service_stream_id);
    EXPECT_EQ(AERON_CM_SNAPSHOT_STREAM_ID_DEFAULT,           m_ctx->snapshot_stream_id);
}

TEST_F(ConsensusModuleContextTest, shouldInitializeWithNullAeron)
{
    EXPECT_EQ(nullptr, m_ctx->aeron);
    EXPECT_FALSE(m_ctx->owns_aeron_client);
}

TEST_F(ConsensusModuleContextTest, shouldInitializeWithNullClusterMembers)
{
    EXPECT_EQ(nullptr, m_ctx->cluster_members);
}

TEST_F(ConsensusModuleContextTest, shouldInitializeWithDefaultAppVersion)
{
    EXPECT_EQ(0, m_ctx->app_version);
}

/* -----------------------------------------------------------------------
 * Setter / getter round-trips
 * ----------------------------------------------------------------------- */

TEST_F(ConsensusModuleContextTest, shouldSetAndGetMemberId)
{
    m_ctx->member_id = 3;
    EXPECT_EQ(3, m_ctx->member_id);
}

TEST_F(ConsensusModuleContextTest, shouldSetAndGetServiceCount)
{
    m_ctx->service_count = 4;
    EXPECT_EQ(4, m_ctx->service_count);
}

TEST_F(ConsensusModuleContextTest, shouldSetAndGetClusterDir)
{
    const char *dir = "/tmp/my_cluster";
    ASSERT_EQ(0, snprintf(m_ctx->cluster_dir, sizeof(m_ctx->cluster_dir), "%s", dir));
    EXPECT_STREQ(dir, m_ctx->cluster_dir);
}

TEST_F(ConsensusModuleContextTest, shouldSetAndGetSessionTimeoutNs)
{
    m_ctx->session_timeout_ns = 30000000000LL;
    EXPECT_EQ(30000000000LL, m_ctx->session_timeout_ns);
}

TEST_F(ConsensusModuleContextTest, shouldSetAndGetElectionTimeoutNs)
{
    m_ctx->election_timeout_ns = 2000000000LL;
    EXPECT_EQ(2000000000LL, m_ctx->election_timeout_ns);
}

TEST_F(ConsensusModuleContextTest, shouldSetAndGetLeaderHeartbeatTimeoutNs)
{
    m_ctx->leader_heartbeat_timeout_ns = 5000000000LL;
    EXPECT_EQ(5000000000LL, m_ctx->leader_heartbeat_timeout_ns);
}

TEST_F(ConsensusModuleContextTest, shouldSetAndGetLeaderHeartbeatIntervalNs)
{
    m_ctx->leader_heartbeat_interval_ns = 500000000LL;
    EXPECT_EQ(500000000LL, m_ctx->leader_heartbeat_interval_ns);
}

TEST_F(ConsensusModuleContextTest, shouldSetAndGetClusterMembers)
{
    const char *members =
        "0,h0:9010:h0:9020:h0:9030:h0:9040:h0:8010|"
        "1,h1:9010:h1:9020:h1:9030:h1:9040:h1:8010";
    aeron_free(m_ctx->cluster_members);
    m_ctx->cluster_members = nullptr;
    size_t n = strlen(members) + 1;
    aeron_alloc((void **)&m_ctx->cluster_members, n);
    memcpy(m_ctx->cluster_members, members, n);
    EXPECT_STREQ(members, m_ctx->cluster_members);
}

TEST_F(ConsensusModuleContextTest, shouldSetAndGetAppVersion)
{
    m_ctx->app_version = 0x010203;
    EXPECT_EQ(0x010203, m_ctx->app_version);
}

TEST_F(ConsensusModuleContextTest, shouldSetAndGetAppointedLeaderId)
{
    m_ctx->appointed_leader_id = 2;
    EXPECT_EQ(2, m_ctx->appointed_leader_id);
}

/* -----------------------------------------------------------------------
 * conclude() validation tests
 * ----------------------------------------------------------------------- */

TEST_F(ConsensusModuleContextTest, concludeFailsIfClusterMembersIsNull)
{
    /* cluster_members = NULL (default) → conclude must fail */
    EXPECT_EQ(-1, aeron_cm_context_conclude(m_ctx));
}

TEST_F(ConsensusModuleContextTest, concludeFailsIfClusterMembersIsEmpty)
{
    aeron_free(m_ctx->cluster_members);
    size_t n = 1;
    aeron_alloc((void **)&m_ctx->cluster_members, n);
    m_ctx->cluster_members[0] = '\0';
    EXPECT_EQ(-1, aeron_cm_context_conclude(m_ctx));
}

/* -----------------------------------------------------------------------
 * Env-var override tests (mirrors shouldInitializeContextWithValuesSpecifiedViaEnvironment)
 * ----------------------------------------------------------------------- */

TEST_F(ConsensusModuleContextTest, shouldApplyEnvVarForMemberId)
{
    setenv(AERON_CM_MEMBER_ID_ENV_VAR, "7", 1);
    aeron_cm_context_close(m_ctx);
    m_ctx = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&m_ctx));
    unsetenv(AERON_CM_MEMBER_ID_ENV_VAR);

    EXPECT_EQ(7, m_ctx->member_id);
}

TEST_F(ConsensusModuleContextTest, shouldApplyEnvVarForConsensusStreamId)
{
    setenv(AERON_CM_CONSENSUS_STREAM_ID_ENV_VAR, "999", 1);
    aeron_cm_context_close(m_ctx);
    m_ctx = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&m_ctx));
    unsetenv(AERON_CM_CONSENSUS_STREAM_ID_ENV_VAR);

    EXPECT_EQ(999, m_ctx->consensus_stream_id);
}

TEST_F(ConsensusModuleContextTest, shouldApplyEnvVarForServiceCount)
{
    setenv(AERON_CM_SERVICE_COUNT_ENV_VAR, "3", 1);
    aeron_cm_context_close(m_ctx);
    m_ctx = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&m_ctx));
    unsetenv(AERON_CM_SERVICE_COUNT_ENV_VAR);

    EXPECT_EQ(3, m_ctx->service_count);
}

TEST_F(ConsensusModuleContextTest, shouldApplyEnvVarForClusterMembers)
{
    const char *members = "0,h:p:h:p:h:p:h:p:h:p";
    setenv(AERON_CM_CLUSTER_MEMBERS_ENV_VAR, members, 1);
    aeron_cm_context_close(m_ctx);
    m_ctx = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&m_ctx));
    unsetenv(AERON_CM_CLUSTER_MEMBERS_ENV_VAR);

    ASSERT_NE(nullptr, m_ctx->cluster_members);
    EXPECT_STREQ(members, m_ctx->cluster_members);
}

TEST_F(ConsensusModuleContextTest, shouldApplyEnvVarForLogChannel)
{
    const char *ch = "aeron:udp?term-length=128m";
    setenv(AERON_CM_LOG_CHANNEL_ENV_VAR, ch, 1);
    aeron_cm_context_close(m_ctx);
    m_ctx = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&m_ctx));
    unsetenv(AERON_CM_LOG_CHANNEL_ENV_VAR);

    ASSERT_NE(nullptr, m_ctx->log_channel);
    EXPECT_STREQ(ch, m_ctx->log_channel);
}

TEST_F(ConsensusModuleContextTest, shouldApplyEnvVarForElectionTimeout)
{
    setenv(AERON_CM_ELECTION_TIMEOUT_ENV_VAR, "2000000000ns", 1);
    aeron_cm_context_close(m_ctx);
    m_ctx = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&m_ctx));
    unsetenv(AERON_CM_ELECTION_TIMEOUT_ENV_VAR);

    EXPECT_EQ(2000000000LL, m_ctx->election_timeout_ns);
}

TEST_F(ConsensusModuleContextTest, shouldApplyEnvVarForSessionTimeout)
{
    setenv(AERON_CM_SESSION_TIMEOUT_ENV_VAR, "20000000000ns", 1);
    aeron_cm_context_close(m_ctx);
    m_ctx = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&m_ctx));
    unsetenv(AERON_CM_SESSION_TIMEOUT_ENV_VAR);

    EXPECT_EQ(20000000000LL, m_ctx->session_timeout_ns);
}

TEST_F(ConsensusModuleContextTest, shouldApplyEnvVarForClusterDir)
{
    setenv(AERON_CM_CLUSTER_DIR_ENV_VAR, "/tmp/test_cluster_dir", 1);
    aeron_cm_context_close(m_ctx);
    m_ctx = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&m_ctx));
    unsetenv(AERON_CM_CLUSTER_DIR_ENV_VAR);

    EXPECT_STREQ("/tmp/test_cluster_dir", m_ctx->cluster_dir);
}

/* -----------------------------------------------------------------------
 * Election state enum codes match Java ordinals (sanity check)
 * ----------------------------------------------------------------------- */

TEST(ConsensusModuleConfigTest, electionStateCodes)
{
    EXPECT_EQ(0,  (int)AERON_ELECTION_INIT);
    EXPECT_EQ(1,  (int)AERON_ELECTION_CANVASS);
    EXPECT_EQ(17, (int)AERON_ELECTION_CLOSED);
}

TEST(ConsensusModuleConfigTest, cmStateCodes)
{
    EXPECT_EQ(0, (int)AERON_CM_STATE_INIT);
    EXPECT_EQ(1, (int)AERON_CM_STATE_ACTIVE);
    EXPECT_EQ(6, (int)AERON_CM_STATE_CLOSED);
}

TEST(ConsensusModuleConfigTest, snapshotTypeIds)
{
    EXPECT_EQ(1LL, AERON_CM_SNAPSHOT_TYPE_ID);
    EXPECT_EQ(2LL, AERON_SVC_SNAPSHOT_TYPE_ID);
}

/* -----------------------------------------------------------------------
 * Service context tests — mirrors ClusteredServiceContainerContextTest
 * ----------------------------------------------------------------------- */


class ServiceContextTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        ASSERT_EQ(0, aeron_cluster_service_context_init(&m_ctx));
    }
    void TearDown() override
    {
        aeron_cluster_service_context_close(m_ctx);
        m_ctx = nullptr;
    }
    aeron_cluster_service_context_t *m_ctx = nullptr;
};

TEST_F(ServiceContextTest, shouldInitializeWithDefaultServiceId)
{
    EXPECT_EQ(AERON_CLUSTER_SERVICE_ID_DEFAULT, m_ctx->service_id);
}

TEST_F(ServiceContextTest, shouldInitializeWithDefaultStreamIds)
{
    EXPECT_EQ(AERON_CLUSTER_CONSENSUS_MODULE_STREAM_ID_DEFAULT, m_ctx->consensus_module_stream_id);
    EXPECT_EQ(AERON_CLUSTER_SERVICE_STREAM_ID_DEFAULT, m_ctx->service_stream_id);
    EXPECT_EQ(AERON_CLUSTER_SNAPSHOT_STREAM_ID_DEFAULT, m_ctx->snapshot_stream_id);
}

TEST_F(ServiceContextTest, shouldInitializeWithDefaultChannels)
{
    ASSERT_NE(nullptr, m_ctx->control_channel);
    ASSERT_NE(nullptr, m_ctx->service_channel);
    ASSERT_NE(nullptr, m_ctx->snapshot_channel);
}

TEST_F(ServiceContextTest, concludeFailsIfServiceCallbackIsNull)
{
    /* service is NULL → conclude must fail */
    EXPECT_EQ(-1, aeron_cluster_service_context_conclude(m_ctx));
}

TEST_F(ServiceContextTest, shouldSetAndGetServiceId)
{
    ASSERT_EQ(0, aeron_cluster_service_context_set_service_id(m_ctx, 2));
    EXPECT_EQ(2, aeron_cluster_service_context_get_service_id(m_ctx));
}

TEST_F(ServiceContextTest, shouldSetAndGetControlChannel)
{
    ASSERT_EQ(0, aeron_cluster_service_context_set_control_channel(m_ctx, "aeron:ipc"));
    EXPECT_STREQ("aeron:ipc", aeron_cluster_service_context_get_control_channel(m_ctx));
}

TEST_F(ServiceContextTest, shouldSetAndGetClusterDir)
{
    ASSERT_EQ(0, aeron_cluster_service_context_set_cluster_dir(m_ctx, "/tmp/svc_cluster"));
    EXPECT_STREQ("/tmp/svc_cluster", aeron_cluster_service_context_get_cluster_dir(m_ctx));
}

TEST_F(ServiceContextTest, shouldApplyEnvVarForServiceId)
{
    setenv(AERON_CLUSTER_SERVICE_ID_ENV_VAR, "5", 1);
    aeron_cluster_service_context_close(m_ctx);
    m_ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_service_context_init(&m_ctx));
    unsetenv(AERON_CLUSTER_SERVICE_ID_ENV_VAR);

    EXPECT_EQ(5, m_ctx->service_id);
}

TEST_F(ServiceContextTest, shouldApplyEnvVarForSnapshotStreamId)
{
    setenv(AERON_CLUSTER_SNAPSHOT_STREAM_ID_ENV_VAR, "777", 1);
    aeron_cluster_service_context_close(m_ctx);
    m_ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_service_context_init(&m_ctx));
    unsetenv(AERON_CLUSTER_SNAPSHOT_STREAM_ID_ENV_VAR);

    EXPECT_EQ(777, m_ctx->snapshot_stream_id);
}

TEST_F(ServiceContextTest, shouldInitializeWithNullAeron)
{
    EXPECT_EQ(nullptr, m_ctx->aeron);
    EXPECT_FALSE(m_ctx->owns_aeron_client);
}
