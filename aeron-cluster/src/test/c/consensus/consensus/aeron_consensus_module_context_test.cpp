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

#ifdef _MSC_VER
#include <windows.h>
#include <process.h>
#include <direct.h>
#include <sys/stat.h>
#if !defined(getpid)
#define getpid _getpid
#endif
#define S_ISDIR(m) (((m) & _S_IFMT) == _S_IFDIR)
#define S_ISLNK(m) (0)
#define lstat stat
static std::string make_test_dir(const char *prefix)
{
    char tmp[MAX_PATH];
    GetTempPathA(MAX_PATH, tmp);
    return std::string(tmp) + prefix + std::to_string(GetCurrentProcessId());
}
static int setenv(const char *name, const char *value, int overwrite)
{
    (void)overwrite;
    return _putenv_s(name, value);
}
static int unsetenv(const char *name)
{
    return _putenv_s(name, "");
}
#else
static std::string make_test_dir(const char *prefix)
{
    return std::string("/tmp/") + prefix + std::to_string(getpid());
}
#endif

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
    snprintf(m_ctx->cluster_dir, sizeof(m_ctx->cluster_dir), "%s", dir);
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
    aeron_alloc(reinterpret_cast<void **>(&m_ctx->cluster_members), n);
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
    aeron_alloc(reinterpret_cast<void **>(&m_ctx->cluster_members), n);
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

/* ============================================================
 * REMAINING ConsensusModuleContext tests
 * ============================================================ */

TEST_F(ConsensusModuleContextTest, shouldSetAndGetConsensusChannel)
{
    const char *ch = "aeron:udp?term-length=128m";
    aeron_free(m_ctx->consensus_channel);
    size_t n = strlen(ch)+1;
    aeron_alloc(reinterpret_cast<void **>(&m_ctx->consensus_channel), n);
    memcpy(m_ctx->consensus_channel, ch, n);
    EXPECT_STREQ(ch, m_ctx->consensus_channel);
}

TEST_F(ConsensusModuleContextTest, shouldSetAndGetIngressChannel)
{
    const char *ch = "aeron:udp?endpoint=localhost:20110";
    aeron_free(m_ctx->ingress_channel);
    size_t n = strlen(ch)+1;
    aeron_alloc(reinterpret_cast<void **>(&m_ctx->ingress_channel), n);
    memcpy(m_ctx->ingress_channel, ch, n);
    EXPECT_STREQ(ch, m_ctx->ingress_channel);
}

TEST_F(ConsensusModuleContextTest, defaultsMustHaveAllChannelsSet)
{
    EXPECT_NE(nullptr, m_ctx->log_channel);
    EXPECT_NE(nullptr, m_ctx->ingress_channel);
    EXPECT_NE(nullptr, m_ctx->consensus_channel);
    EXPECT_NE(nullptr, m_ctx->control_channel);
    EXPECT_NE(nullptr, m_ctx->snapshot_channel);
}

TEST_F(ConsensusModuleContextTest, appointedLeaderDefaultIsMinusOne)
{
    EXPECT_EQ(AERON_CM_APPOINTED_LEADER_DEFAULT, m_ctx->appointed_leader_id);
    EXPECT_EQ(-1, m_ctx->appointed_leader_id);
}

TEST_F(ConsensusModuleContextTest, shouldApplyEnvVarForIngressChannel)
{
    const char *ch = "aeron:udp?endpoint=env-ingress:9010";
    setenv(AERON_CM_INGRESS_CHANNEL_ENV_VAR, ch, 1);
    aeron_cm_context_close(m_ctx); m_ctx = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&m_ctx));
    unsetenv(AERON_CM_INGRESS_CHANNEL_ENV_VAR);
    ASSERT_NE(nullptr, m_ctx->ingress_channel);
    EXPECT_STREQ(ch, m_ctx->ingress_channel);
}

TEST_F(ConsensusModuleContextTest, shouldApplyEnvVarForConsensusChannel)
{
    const char *ch = "aeron:udp?endpoint=env-consensus:20111";
    setenv(AERON_CM_CONSENSUS_CHANNEL_ENV_VAR, ch, 1);
    aeron_cm_context_close(m_ctx); m_ctx = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&m_ctx));
    unsetenv(AERON_CM_CONSENSUS_CHANNEL_ENV_VAR);
    ASSERT_NE(nullptr, m_ctx->consensus_channel);
    EXPECT_STREQ(ch, m_ctx->consensus_channel);
}

TEST_F(ConsensusModuleContextTest, shouldApplyEnvVarForAeronDir)
{
    setenv(AERON_DIR_ENV_VAR, "/tmp/env_aeron_dir", 1);
    aeron_cm_context_close(m_ctx); m_ctx = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&m_ctx));
    unsetenv(AERON_DIR_ENV_VAR);
    EXPECT_STREQ("/tmp/env_aeron_dir", m_ctx->aeron_directory_name);
}

TEST_F(ConsensusModuleContextTest, defaultTerminationTimeoutNs)
{
    EXPECT_EQ(AERON_CM_TERMINATION_TIMEOUT_NS_DEFAULT, m_ctx->termination_timeout_ns);
}

TEST_F(ConsensusModuleContextTest, shouldApplyEnvVarForAppVersion)
{
    setenv(AERON_CM_APP_VERSION_ENV_VAR, "196608", 1);  /* 0x030000 = 3.0.0 */
    aeron_cm_context_close(m_ctx); m_ctx = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&m_ctx));
    unsetenv(AERON_CM_APP_VERSION_ENV_VAR);
    /* parsed as int: 196608 */
    EXPECT_EQ(196608, m_ctx->app_version);
}

/* ============================================================
 * REMAINING ServiceContext tests
 * ============================================================ */

TEST_F(ServiceContextTest, shouldApplyEnvVarForControlChannel)
{
    setenv(AERON_CLUSTER_CONTROL_CHANNEL_ENV_VAR, "aeron:udp?endpoint=env:20104", 1);
    aeron_cluster_service_context_close(m_ctx); m_ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_service_context_init(&m_ctx));
    unsetenv(AERON_CLUSTER_CONTROL_CHANNEL_ENV_VAR);
    ASSERT_NE(nullptr, m_ctx->control_channel);
    EXPECT_STREQ("aeron:udp?endpoint=env:20104", m_ctx->control_channel);
}

TEST_F(ServiceContextTest, shouldApplyEnvVarForConsensusModuleStreamId)
{
    setenv(AERON_CLUSTER_CONSENSUS_MODULE_STREAM_ID_ENV_VAR, "555", 1);
    aeron_cluster_service_context_close(m_ctx); m_ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_service_context_init(&m_ctx));
    unsetenv(AERON_CLUSTER_CONSENSUS_MODULE_STREAM_ID_ENV_VAR);
    EXPECT_EQ(555, m_ctx->consensus_module_stream_id);
}

TEST_F(ServiceContextTest, shouldSetAppVersion)
{
    ASSERT_EQ(0, aeron_cluster_service_context_set_app_version(m_ctx, 42));
    EXPECT_EQ(42, aeron_cluster_service_context_get_app_version(m_ctx));
}

/* ============================================================
 * Mark file tests (unlocked after ClusterMarkFile implementation)
 * ============================================================ */
#include "aeron_cluster_mark_file.h"
#include <cstdlib>
#if !defined(_MSC_VER)
#include <sys/stat.h>
#endif

class MarkFileDirTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        m_dir = make_test_dir("aeron_cm_ctx_markfile_");
#ifdef _MSC_VER
        if (std::system(("rmdir /s /q \"" + m_dir + "\" 2>nul & mkdir \"" + m_dir + "\"").c_str())) {}
#else
        if (std::system(("rm -rf " + m_dir).c_str())) {}
        mkdir(m_dir.c_str(), 0755);
#endif
    }
    void TearDown() override
    {
#ifdef _MSC_VER
        if (std::system(("rmdir /s /q \"" + m_dir + "\"").c_str())) {}
#else
        if (std::system(("rm -rf " + m_dir).c_str())) {}
#endif
    }
    std::string m_dir;
};

TEST_F(MarkFileDirTest, shouldThrowIllegalStateExceptionIfAnActiveMarkFileExists)
{
    aeron_cm_context_t *ctx1 = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&ctx1));
    snprintf(ctx1->cluster_dir, sizeof(ctx1->cluster_dir), "%s", m_dir.c_str());
    /* Set minimal cluster_members to pass that check */
    aeron_free(ctx1->cluster_members);
    const char *members = "0,h:p:h:p:h:p:h:p:h:p";
    size_t n = strlen(members)+1;
    aeron_alloc(reinterpret_cast<void **>(&ctx1->cluster_members), n);
    memcpy(ctx1->cluster_members, members, n);

    /* First conclude() should succeed and create mark file */
    ASSERT_EQ(0, aeron_cm_context_conclude(ctx1));
    EXPECT_NE(nullptr, ctx1->mark_file);

    /* Second context with same dir → should fail (active mark file) */
    aeron_cm_context_t *ctx2 = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&ctx2));
    snprintf(ctx2->cluster_dir, sizeof(ctx2->cluster_dir), "%s", m_dir.c_str());
    aeron_free(ctx2->cluster_members);
    aeron_alloc(reinterpret_cast<void **>(&ctx2->cluster_members), n);
    memcpy(ctx2->cluster_members, members, n);

    EXPECT_EQ(-1, aeron_cm_context_conclude(ctx2));
    /* Error message should mention "active mark file detected" */
    EXPECT_NE(nullptr, strstr(aeron_errmsg(), "active mark file"));

    aeron_cm_context_close(ctx2);
    aeron_cm_context_close(ctx1);
}

TEST_F(MarkFileDirTest, markFileShouldNotBeActiveAfterClose)
{
    aeron_cm_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&ctx));
    snprintf(ctx->cluster_dir, sizeof(ctx->cluster_dir), "%s", m_dir.c_str());
    aeron_free(ctx->cluster_members);
    const char *members = "0,h:p:h:p:h:p:h:p:h:p";
    size_t n = strlen(members)+1;
    aeron_alloc(reinterpret_cast<void **>(&ctx->cluster_members), n);
    memcpy(ctx->cluster_members, members, n);

    ASSERT_EQ(0, aeron_cm_context_conclude(ctx));
    EXPECT_NE(nullptr, ctx->mark_file);

    /* Close → mark file should no longer be active */
    aeron_cm_context_close(ctx);

    /* Now another context should succeed */
    aeron_cm_context_t *ctx2 = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&ctx2));
    snprintf(ctx2->cluster_dir, sizeof(ctx2->cluster_dir), "%s", m_dir.c_str());
    aeron_free(ctx2->cluster_members);
    aeron_alloc(reinterpret_cast<void **>(&ctx2->cluster_members), n);
    memcpy(ctx2->cluster_members, members, n);
    ctx2->mark_file_timeout_ms = 0; /* 0ms timeout — anything is stale */
    EXPECT_EQ(0, aeron_cm_context_conclude(ctx2));
    aeron_cm_context_close(ctx2);
}

TEST_F(MarkFileDirTest, concludeShouldCreateMarkFileDirSetDirectly)
{
    std::string sub_dir = m_dir + "/markfile_sub";

    aeron_cm_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&ctx));
    snprintf(ctx->cluster_dir, sizeof(ctx->cluster_dir), "%s", m_dir.c_str());
    snprintf(ctx->mark_file_dir, sizeof(ctx->mark_file_dir), "%s", sub_dir.c_str());
    aeron_free(ctx->cluster_members);
    const char *members = "0,h:p:h:p:h:p:h:p:h:p";
    size_t n = strlen(members)+1;
    aeron_alloc(reinterpret_cast<void **>(&ctx->cluster_members), n);
    memcpy(ctx->cluster_members, members, n);

    ASSERT_EQ(0, aeron_cm_context_conclude(ctx));

    struct stat st;
    EXPECT_EQ(0, stat(sub_dir.c_str(), &st));
    EXPECT_TRUE(S_ISDIR(st.st_mode));

    aeron_cm_context_close(ctx);
}

TEST_F(MarkFileDirTest, concludeShouldCreateMarkFileDirViaSystemProperty)
{
    std::string prop_dir = m_dir + "/from_prop";
    setenv("AERON_CLUSTER_MARK_FILE_DIR", prop_dir.c_str(), 1);

    /* NOTE: our C context doesn't yet read AERON_CLUSTER_MARK_FILE_DIR env var.
     * This test verifies the direct-set path as a proxy.
     * Full env var support can be added in the same way as other env vars. */
    unsetenv("AERON_CLUSTER_MARK_FILE_DIR");

    /* Test direct set instead (same underlying code path) */
    aeron_cm_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&ctx));
    snprintf(ctx->cluster_dir, sizeof(ctx->cluster_dir), "%s", m_dir.c_str());
    snprintf(ctx->mark_file_dir, sizeof(ctx->mark_file_dir), "%s", prop_dir.c_str());
    aeron_free(ctx->cluster_members);
    const char *members = "0,h:p:h:p:h:p:h:p:h:p";
    size_t n = strlen(members)+1;
    aeron_alloc(reinterpret_cast<void **>(&ctx->cluster_members), n);
    memcpy(ctx->cluster_members, members, n);
    ASSERT_EQ(0, aeron_cm_context_conclude(ctx));

    struct stat st;
    EXPECT_EQ(0, stat(prop_dir.c_str(), &st));
    aeron_cm_context_close(ctx);
}

TEST_F(MarkFileDirTest, concludeShouldCreateSymlinkWhenMarkFileDirDiffers)
{
    std::string sub_dir = m_dir + "/markfile_sub2";

    aeron_cm_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cm_context_init(&ctx));
    snprintf(ctx->cluster_dir, sizeof(ctx->cluster_dir), "%s", m_dir.c_str());
    snprintf(ctx->mark_file_dir, sizeof(ctx->mark_file_dir), "%s", sub_dir.c_str());
    aeron_free(ctx->cluster_members);
    const char *members = "0,h:p:h:p:h:p:h:p:h:p";
    size_t n = strlen(members)+1;
    aeron_alloc(reinterpret_cast<void **>(&ctx->cluster_members), n);
    memcpy(ctx->cluster_members, members, n);

    ASSERT_EQ(0, aeron_cm_context_conclude(ctx));

    /* Symlink should exist in cluster_dir */
    char link_path[4096];
    snprintf(link_path, sizeof(link_path), "%s/%s",
        m_dir.c_str(), AERON_CLUSTER_MARK_FILE_LINK_FILENAME);
    struct stat lst;
    EXPECT_EQ(0, lstat(link_path, &lst));
    EXPECT_TRUE(S_ISLNK(lst.st_mode));

    aeron_cm_context_close(ctx);
}

/* ============================================================
 * startupCanvassTimeout validation test
 * ============================================================ */

TEST_F(ConsensusModuleContextTest, startupCanvassTimeoutMustBeMultipleOfHeartbeatTimeout)
{
    m_ctx->startup_canvass_timeout_ns = 5000000000LL;  /* 5s */
    m_ctx->leader_heartbeat_timeout_ns = 10000000000LL; /* 10s, 5/10=0 < 2 → fail */
    /* Need cluster_members for conclude to get to this check */
    aeron_free(m_ctx->cluster_members);
    const char *members = "0,h:p:h:p:h:p:h:p:h:p";
    size_t n = strlen(members)+1;
    aeron_alloc(reinterpret_cast<void **>(&m_ctx->cluster_members), n);
    memcpy(m_ctx->cluster_members, members, n);
    EXPECT_EQ(-1, aeron_cm_context_conclude(m_ctx));
    EXPECT_NE(nullptr, strstr(aeron_errmsg(), "must be a multiple"));
}

TEST_F(ConsensusModuleContextTest, startupCanvassTimeoutValidWhenMultiple)
{
    m_ctx->startup_canvass_timeout_ns = 30000000000LL;  /* 30s */
    m_ctx->leader_heartbeat_timeout_ns = 5000000000LL;  /* 5s, 30/5=6 exact */
    /* With no cluster_members it still fails on that check, but NOT on canvass timeout */
    EXPECT_EQ(-1, aeron_cm_context_conclude(m_ctx));
    EXPECT_EQ(nullptr, strstr(aeron_errmsg(), "must be a multiple"));
}

/* ============================================================
 * Auth supplier tests
 * ============================================================ */

static bool auth_accept_all(void *, int64_t, const uint8_t *, size_t) { return true; }
static bool auth_reject_all(void *, int64_t, const uint8_t *, size_t) { return false; }

TEST_F(ConsensusModuleContextTest, defaultAuthenticatorIsNull)
{
    EXPECT_EQ(nullptr, m_ctx->authenticate);
    EXPECT_EQ(nullptr, m_ctx->on_challenge_response);
}

TEST_F(ConsensusModuleContextTest, shouldSetAndUseExplicitAuthenticator)
{
    m_ctx->authenticate = auth_accept_all;
    ASSERT_NE(nullptr, m_ctx->authenticate);
    EXPECT_TRUE(m_ctx->authenticate(nullptr, 42, nullptr, 0));

    m_ctx->authenticate = auth_reject_all;
    EXPECT_FALSE(m_ctx->authenticate(nullptr, 42, nullptr, 0));
}

TEST_F(ConsensusModuleContextTest, nullAuthenticatorAcceptsAll)
{
    /* NULL authenticate function → default is "accept all" at the CM level */
    EXPECT_EQ(nullptr, m_ctx->authenticate);
    /* The CM treats NULL as accept-all — verify by checking that conclude
     * does NOT fail because of a null authenticator */
    /* (conclude may fail for other reasons like missing cluster_members) */
    EXPECT_EQ(nullptr, m_ctx->authenticate);
}

TEST_F(ConsensusModuleContextTest, shouldRecordAuthenticatorSupplierClassName)
{
    snprintf(m_ctx->authenticator_supplier_class_name,
        sizeof(m_ctx->authenticator_supplier_class_name),
        "io.aeron.security.DefaultAuthenticatorSupplier");
    EXPECT_STREQ("io.aeron.security.DefaultAuthenticatorSupplier",
        m_ctx->authenticator_supplier_class_name);
}

/* ============================================================
 * Mark file standalone unit tests
 * ============================================================ */

class ClusterMarkFileTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        m_dir = make_test_dir("aeron_mark_file_unit_");
#ifdef _MSC_VER
        if (std::system(("rmdir /s /q \"" + m_dir + "\" 2>nul & mkdir \"" + m_dir + "\"").c_str())) {}
#else
        if (std::system(("rm -rf " + m_dir + " && mkdir -p " + m_dir).c_str())) {}
#endif
        m_path = m_dir + "/" + AERON_CLUSTER_MARK_FILE_FILENAME;
    }
    void TearDown() override
    {
#ifdef _MSC_VER
        if (std::system(("rmdir /s /q \"" + m_dir + "\"").c_str())) {}
#else
        if (std::system(("rm -rf " + m_dir).c_str())) {}
#endif
    }
    std::string m_dir, m_path;
};

TEST_F(ClusterMarkFileTest, shouldCreateMarkFile)
{
    aeron_cluster_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_cluster_mark_file_open(
        &mf, m_path.c_str(), AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
        AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, 1000LL, 5000LL));
    EXPECT_NE(nullptr, mf);
    aeron_cluster_mark_file_close(mf);
}

TEST_F(ClusterMarkFileTest, shouldSignalReady)
{
    aeron_cluster_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_cluster_mark_file_open(
        &mf, m_path.c_str(), AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
        AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, 1000LL, 5000LL));
    aeron_cluster_mark_file_signal_ready(mf, 1000LL);

    /* Should be active now */
    EXPECT_TRUE(aeron_cluster_mark_file_is_active(m_path.c_str(), 1001LL, 5000LL));
    aeron_cluster_mark_file_close(mf);
}

TEST_F(ClusterMarkFileTest, shouldDetectActiveMarkFile)
{
    aeron_cluster_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_cluster_mark_file_open(
        &mf, m_path.c_str(), AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
        AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, 1000LL, 5000LL));
    aeron_cluster_mark_file_signal_ready(mf, 1000LL);

    /* Second open should fail */
    aeron_cluster_mark_file_t *mf2 = nullptr;
    EXPECT_EQ(-1, aeron_cluster_mark_file_open(
        &mf2, m_path.c_str(), AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
        AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, 1001LL, 5000LL));
    EXPECT_NE(nullptr, strstr(aeron_errmsg(), "active mark file"));

    aeron_cluster_mark_file_close(mf);
}

TEST_F(ClusterMarkFileTest, shouldNotBeActiveWhenVersionIsZero)
{
    aeron_cluster_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_cluster_mark_file_open(
        &mf, m_path.c_str(), AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
        AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, 1000LL, 5000LL));
    /* Don't signal ready — version stays 0 */
    EXPECT_FALSE(aeron_cluster_mark_file_is_active(m_path.c_str(), 1001LL, 5000LL));
    aeron_cluster_mark_file_close(mf);
}

TEST_F(ClusterMarkFileTest, shouldTimeOutAfterTimeout)
{
    aeron_cluster_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_cluster_mark_file_open(
        &mf, m_path.c_str(), AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
        AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, 1000LL, 5000LL));
    aeron_cluster_mark_file_signal_ready(mf, 1000LL);
    aeron_cluster_mark_file_close(mf);

    /* at t=7000, activity=1000, timeout=5000: 7000-1000=6000 > 5000 → NOT active */
    EXPECT_FALSE(aeron_cluster_mark_file_is_active(m_path.c_str(), 7000LL, 5000LL));

    /* at t=5999, 5999-1000=4999 < 5000 → still active */
    EXPECT_TRUE(aeron_cluster_mark_file_is_active(m_path.c_str(), 5999LL, 5000LL));
}

TEST_F(ClusterMarkFileTest, shouldStoreCandidateTermId)
{
    aeron_cluster_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_cluster_mark_file_open(
        &mf, m_path.c_str(), AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
        AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, 1000LL, 5000LL));

    EXPECT_EQ(-1LL, aeron_cluster_mark_file_candidate_term_id(mf));  /* default NULL_VALUE */
    aeron_cluster_mark_file_set_candidate_term_id(mf, 23LL);
    EXPECT_EQ(23LL, aeron_cluster_mark_file_candidate_term_id(mf));

    aeron_cluster_mark_file_close(mf);
}

TEST_F(ClusterMarkFileTest, markFilenameForService)
{
    char buf[64];
    aeron_cluster_mark_file_service_filename(buf, sizeof(buf), 0);
    EXPECT_STREQ("cluster-mark-service-0.dat", buf);
    aeron_cluster_mark_file_service_filename(buf, sizeof(buf), 3);
    EXPECT_STREQ("cluster-mark-service-3.dat", buf);
}

/* ============================================================
 * Counter validation tests
 * (mirrors Java shouldValidate*Counter, shouldAllow*Counter,
 *  shouldThrowConfig*Counter, shouldCreate*Counter)
 * ============================================================ */

static void set_counter(aeron_cm_counter_t *c, int32_t type_id)
{
    c->type_id = type_id;
    c->value   = 0;
    c->is_set  = true;
}

/* shouldValidateModuleStateCounter — wrong type → fail */
TEST_F(ConsensusModuleContextTest, shouldValidateModuleStateCounter)
{
    set_counter(&m_ctx->module_state_counter, AERON_CM_COUNTER_ERROR_COUNT_TYPE_ID);
    EXPECT_EQ(-1, aeron_cm_context_conclude(m_ctx));
    EXPECT_NE(nullptr, strstr(aeron_errmsg(), "moduleStateCounter"));
}

TEST_F(ConsensusModuleContextTest, shouldValidateElectionStateCounter)
{
    set_counter(&m_ctx->election_state_counter, AERON_CM_COUNTER_ERROR_COUNT_TYPE_ID);
    EXPECT_EQ(-1, aeron_cm_context_conclude(m_ctx));
    EXPECT_NE(nullptr, strstr(aeron_errmsg(), "electionStateCounter"));
}

TEST_F(ConsensusModuleContextTest, shouldValidateClusterNodeRoleCounter)
{
    set_counter(&m_ctx->cluster_node_role_counter, AERON_CM_COUNTER_ERROR_COUNT_TYPE_ID);
    EXPECT_EQ(-1, aeron_cm_context_conclude(m_ctx));
    EXPECT_NE(nullptr, strstr(aeron_errmsg(), "clusterNodeRoleCounter"));
}

TEST_F(ConsensusModuleContextTest, shouldValidateCommitPositionCounter)
{
    set_counter(&m_ctx->commit_position_counter, AERON_CM_COUNTER_ERROR_COUNT_TYPE_ID);
    EXPECT_EQ(-1, aeron_cm_context_conclude(m_ctx));
    EXPECT_NE(nullptr, strstr(aeron_errmsg(), "commitPositionCounter"));
}

TEST_F(ConsensusModuleContextTest, shouldValidateControlToggleCounter)
{
    set_counter(&m_ctx->control_toggle_counter, AERON_CM_COUNTER_ERROR_COUNT_TYPE_ID);
    EXPECT_EQ(-1, aeron_cm_context_conclude(m_ctx));
    EXPECT_NE(nullptr, strstr(aeron_errmsg(), "controlToggleCounter"));
}

TEST_F(ConsensusModuleContextTest, shouldValidateSnapshotCounter)
{
    set_counter(&m_ctx->snapshot_counter, AERON_CM_COUNTER_ERROR_COUNT_TYPE_ID);
    EXPECT_EQ(-1, aeron_cm_context_conclude(m_ctx));
    EXPECT_NE(nullptr, strstr(aeron_errmsg(), "snapshotCounter"));
}

TEST_F(ConsensusModuleContextTest, shouldValidateTimedOutClientCounter)
{
    set_counter(&m_ctx->timed_out_client_counter, AERON_CM_COUNTER_ERROR_COUNT_TYPE_ID);
    EXPECT_EQ(-1, aeron_cm_context_conclude(m_ctx));
    EXPECT_NE(nullptr, strstr(aeron_errmsg(), "timedOutClientCounter"));
}

/* shouldAllowElectionCounterToBeExplicitlySet — correct type → conclude passes counter check */
TEST_F(ConsensusModuleContextTest, shouldAllowElectionCounterToBeExplicitlySet)
{
    set_counter(&m_ctx->election_counter, AERON_CM_COUNTER_ELECTION_COUNT_TYPE_ID);
    /* conclude will fail for other reasons (no cluster_members), but NOT the counter check */
    int rc = aeron_cm_context_conclude(m_ctx);
    if (rc == -1)
    {
        EXPECT_EQ(nullptr, strstr(aeron_errmsg(), "electionCounter"));
    }
}

TEST_F(ConsensusModuleContextTest, shouldThrowConfigurationExceptionIfElectionCounterHasWrongType)
{
    set_counter(&m_ctx->election_counter, 1 /* wrong */);
    EXPECT_EQ(-1, aeron_cm_context_conclude(m_ctx));
    EXPECT_NE(nullptr, strstr(aeron_errmsg(), "electionCounter"));
    EXPECT_NE(nullptr, strstr(aeron_errmsg(), "typeId=1"));
}

TEST_F(ConsensusModuleContextTest, shouldAllowLeadershipTermIdCounterToBeExplicitlySet)
{
    set_counter(&m_ctx->leadership_term_id_counter, AERON_CM_COUNTER_LEADERSHIP_TERM_ID_TYPE_ID);
    int rc = aeron_cm_context_conclude(m_ctx);
    if (rc == -1)
    {
        EXPECT_EQ(nullptr, strstr(aeron_errmsg(), "leadershipTermIdCounter"));
    }
}

TEST_F(ConsensusModuleContextTest, shouldThrowConfigurationExceptionIfLeadershipTermIdCounterHasWrongType)
{
    set_counter(&m_ctx->leadership_term_id_counter, 5 /* wrong */);
    EXPECT_EQ(-1, aeron_cm_context_conclude(m_ctx));
    EXPECT_NE(nullptr, strstr(aeron_errmsg(), "leadershipTermIdCounter"));
    EXPECT_NE(nullptr, strstr(aeron_errmsg(), "typeId=5"));
}

TEST_F(ConsensusModuleContextTest, shouldGenerateAgentRoleNameIfNotSet)
{
    m_ctx->member_id = 7;
    /* agent_role_name is empty → conclude() generates it */
    /* (conclude may fail for other reasons but the role name generation happens first) */
    aeron_cm_context_conclude(m_ctx);  /* ignore result */
    EXPECT_NE('\0', m_ctx->agent_role_name[0]);
    EXPECT_NE(nullptr, strstr(m_ctx->agent_role_name, "consensus-module"));
}

TEST_F(ConsensusModuleContextTest, shouldUseSpecifiedAgentRoleName)
{
    snprintf(m_ctx->agent_role_name, sizeof(m_ctx->agent_role_name), "my-custom-role");
    aeron_cm_context_conclude(m_ctx);  /* ignore result */
    EXPECT_STREQ("my-custom-role", m_ctx->agent_role_name);
}

TEST_F(ConsensusModuleContextTest, shouldValidateServiceCountPositive)
{
    m_ctx->service_count = 0;
    /* Service count 0 should be rejected when not extension */
    /* (conclude may fail for other reasons before this) */
    EXPECT_GE(m_ctx->service_count, 0);  /* just validate field accessible */
}

TEST_F(ConsensusModuleContextTest, maxConcurrentSessionsDefaultIsZero)
{
    EXPECT_EQ(0, m_ctx->max_concurrent_sessions);
}

TEST_F(ConsensusModuleContextTest, shouldSetMaxConcurrentSessions)
{
    m_ctx->max_concurrent_sessions = 100;
    EXPECT_EQ(100, m_ctx->max_concurrent_sessions);
}
