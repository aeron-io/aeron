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
 * Tests for the ClusteredServiceContainer C API and service context.
 * Exercises conclude validation, context getters/setters, and callback wiring.
 * Does not require a running cluster.
 */

#include <gtest/gtest.h>
#include <cstring>

#include "service/aeron_cluster_service_context.h"

extern "C" int aeron_cluster_service_container_conclude(aeron_cluster_service_context_t *ctx);

/* --------------------------------------------------------------------------
 * Tests: conclude validation
 * -------------------------------------------------------------------------- */

TEST(ServiceContextTest, shouldRejectNullContext)
{
    EXPECT_EQ(-1, aeron_cluster_service_container_conclude(nullptr));
}

TEST(ServiceContextTest, shouldRejectNullAeron)
{
    aeron_cluster_service_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_service_context_init(&ctx));

    aeron_clustered_service_t svc{};
    aeron_cluster_service_context_set_service(ctx, &svc);
    EXPECT_EQ(-1, aeron_cluster_service_container_conclude(ctx));

    aeron_cluster_service_context_close(ctx);
}

TEST(ServiceContextTest, shouldRejectNullService)
{
    aeron_cluster_service_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_service_context_init(&ctx));

    ctx->aeron = reinterpret_cast<aeron_t *>(0x1);
    EXPECT_EQ(-1, aeron_cluster_service_container_conclude(ctx));

    ctx->aeron = nullptr;
    aeron_cluster_service_context_close(ctx);
}

TEST(ServiceContextTest, shouldRejectNegativeServiceId)
{
    aeron_cluster_service_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_service_context_init(&ctx));

    aeron_clustered_service_t svc{};
    aeron_cluster_service_context_set_service(ctx, &svc);
    ctx->aeron = reinterpret_cast<aeron_t *>(0x1);
    aeron_cluster_service_context_set_service_id(ctx, -1);

    EXPECT_EQ(-1, aeron_cluster_service_container_conclude(ctx));

    ctx->aeron = nullptr;
    aeron_cluster_service_context_close(ctx);
}

TEST(ServiceContextTest, shouldRejectServiceIdAboveMax)
{
    aeron_cluster_service_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_service_context_init(&ctx));

    aeron_clustered_service_t svc{};
    aeron_cluster_service_context_set_service(ctx, &svc);
    ctx->aeron = reinterpret_cast<aeron_t *>(0x1);
    aeron_cluster_service_context_set_service_id(ctx, 200);

    EXPECT_EQ(-1, aeron_cluster_service_container_conclude(ctx));

    ctx->aeron = nullptr;
    aeron_cluster_service_context_close(ctx);
}

TEST(ServiceContextTest, shouldConcludeWithValidMinimalConfig)
{
    aeron_cluster_service_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_service_context_init(&ctx));

    aeron_clustered_service_t svc{};
    aeron_cluster_service_context_set_service(ctx, &svc);
    ctx->aeron = reinterpret_cast<aeron_t *>(0x1);
    aeron_cluster_service_context_set_service_id(ctx, 0);

    EXPECT_EQ(0, aeron_cluster_service_container_conclude(ctx));

    EXPECT_NE(nullptr, ctx->control_channel);
    EXPECT_NE(nullptr, ctx->snapshot_channel);
    EXPECT_EQ(104, ctx->consensus_module_stream_id);
    EXPECT_EQ(105, ctx->service_stream_id);
    EXPECT_EQ(107, ctx->snapshot_stream_id);

    ctx->aeron = nullptr;
    aeron_cluster_service_context_close(ctx);
}

TEST(ServiceContextTest, shouldPreserveUserSetChannels)
{
    aeron_cluster_service_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_service_context_init(&ctx));

    aeron_clustered_service_t svc{};
    aeron_cluster_service_context_set_service(ctx, &svc);
    ctx->aeron = reinterpret_cast<aeron_t *>(0x1);

    aeron_cluster_service_context_set_control_channel(ctx, "aeron:udp?endpoint=localhost:9999");
    aeron_cluster_service_context_set_snapshot_channel(ctx, "aeron:udp?endpoint=localhost:8888");

    EXPECT_EQ(0, aeron_cluster_service_container_conclude(ctx));

    EXPECT_STREQ("aeron:udp?endpoint=localhost:9999", ctx->control_channel);
    EXPECT_STREQ("aeron:udp?endpoint=localhost:8888", ctx->snapshot_channel);

    ctx->aeron = nullptr;
    aeron_cluster_service_context_close(ctx);
}

/* --------------------------------------------------------------------------
 * Tests: context getters/setters
 * -------------------------------------------------------------------------- */

TEST(ServiceContextTest, shouldGetAndSetServiceId)
{
    aeron_cluster_service_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_service_context_init(&ctx));

    aeron_cluster_service_context_set_service_id(ctx, 3);
    EXPECT_EQ(3, aeron_cluster_service_context_get_service_id(ctx));

    aeron_cluster_service_context_close(ctx);
}

TEST(ServiceContextTest, shouldGetAndSetClusterId)
{
    aeron_cluster_service_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_service_context_init(&ctx));

    aeron_cluster_service_context_set_cluster_id(ctx, 42);
    EXPECT_EQ(42, aeron_cluster_service_context_get_cluster_id(ctx));

    aeron_cluster_service_context_close(ctx);
}

TEST(ServiceContextTest, shouldGetAndSetAppVersion)
{
    aeron_cluster_service_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_service_context_init(&ctx));

    aeron_cluster_service_context_set_app_version(ctx, 0x00010002);
    EXPECT_EQ(0x00010002, aeron_cluster_service_context_get_app_version(ctx));

    aeron_cluster_service_context_close(ctx);
}

TEST(ServiceContextTest, shouldGetAndSetStreamIds)
{
    aeron_cluster_service_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_service_context_init(&ctx));

    aeron_cluster_service_context_set_consensus_module_stream_id(ctx, 200);
    aeron_cluster_service_context_set_service_stream_id(ctx, 201);
    aeron_cluster_service_context_set_snapshot_stream_id(ctx, 202);

    EXPECT_EQ(200, aeron_cluster_service_context_get_consensus_module_stream_id(ctx));
    EXPECT_EQ(201, aeron_cluster_service_context_get_service_stream_id(ctx));
    EXPECT_EQ(202, aeron_cluster_service_context_get_snapshot_stream_id(ctx));

    aeron_cluster_service_context_close(ctx);
}

TEST(ServiceContextTest, shouldGetAndSetClusterDir)
{
    aeron_cluster_service_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_service_context_init(&ctx));

    aeron_cluster_service_context_set_cluster_dir(ctx, "/tmp/my-cluster");
    EXPECT_STREQ("/tmp/my-cluster", aeron_cluster_service_context_get_cluster_dir(ctx));

    aeron_cluster_service_context_close(ctx);
}

TEST(ServiceContextTest, shouldGetAndSetControlChannel)
{
    aeron_cluster_service_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_service_context_init(&ctx));

    aeron_cluster_service_context_set_control_channel(ctx, "aeron:ipc");
    EXPECT_STREQ("aeron:ipc", aeron_cluster_service_context_get_control_channel(ctx));

    aeron_cluster_service_context_close(ctx);
}

/* --------------------------------------------------------------------------
 * Tests: C callback function pointer wiring
 * -------------------------------------------------------------------------- */

static int g_callback_count = 0;

static void test_on_role_change(void *cd, aeron_cluster_role_t role)
{
    g_callback_count++;
    EXPECT_EQ(AERON_CLUSTER_ROLE_LEADER, role);
}

TEST(ServiceCallbackTest, shouldWireOnRoleChangeCallback)
{
    aeron_clustered_service_t svc{};
    svc.clientd = nullptr;
    svc.on_role_change = test_on_role_change;

    g_callback_count = 0;
    svc.on_role_change(svc.clientd, AERON_CLUSTER_ROLE_LEADER);
    EXPECT_EQ(1, g_callback_count);
}

static int test_bg_work(void *cd, int64_t now_ns)
{
    g_callback_count++;
    return 1;
}

TEST(ServiceCallbackTest, shouldWireDoBackgroundWork)
{
    aeron_clustered_service_t svc{};
    svc.do_background_work = test_bg_work;

    g_callback_count = 0;
    int result = svc.do_background_work(svc.clientd, 12345LL);
    EXPECT_EQ(1, result);
    EXPECT_EQ(1, g_callback_count);
}

static void test_on_timer(void *cd, int64_t correl_id, int64_t ts)
{
    g_callback_count++;
    EXPECT_EQ(77LL, correl_id);
}

TEST(ServiceCallbackTest, shouldWireOnTimerEvent)
{
    aeron_clustered_service_t svc{};
    svc.on_timer_event = test_on_timer;

    g_callback_count = 0;
    svc.on_timer_event(svc.clientd, 77LL, 999LL);
    EXPECT_EQ(1, g_callback_count);
}

static void test_on_session_message(void *cd,
    aeron_cluster_client_session_t *s, int64_t ts,
    const uint8_t *buf, size_t len)
{
    g_callback_count++;
    EXPECT_EQ(5U, len);
    EXPECT_EQ(0, memcmp(buf, "hello", 5));
}

TEST(ServiceCallbackTest, shouldWireOnSessionMessage)
{
    aeron_clustered_service_t svc{};
    svc.on_session_message = test_on_session_message;

    g_callback_count = 0;
    const uint8_t msg[] = "hello";
    svc.on_session_message(svc.clientd, nullptr, 0, msg, 5);
    EXPECT_EQ(1, g_callback_count);
}
