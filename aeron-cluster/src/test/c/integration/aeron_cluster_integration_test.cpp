/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * ...
 */

/**
 * Integration tests matching the Java ClusterNodeTest pattern exactly:
 *
 *   before(): ClusteredMediaDriver.launch(driverCtx, archiveCtx, cmCtx)
 *   test:     container = launchEchoService();
 *             aeronCluster = connectToCluster(listener);
 *             offerMessage(msgBuffer, msg);
 *             awaitResponse(messageCount);
 *   after():  CloseHelper.closeAll(aeronCluster, container, clusteredMediaDriver)
 *
 * @InterruptAfter(10) maps to TEST timeout of 10 seconds.
 */

#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>

#ifdef _MSC_VER
#include <windows.h>
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

extern "C"
{
#include "aeronc.h"
#include "client/aeron_cluster.h"
#include "client/aeron_cluster_context.h"
#include "client/aeron_cluster_client.h"
#include "client/aeron_cluster_async_connect.h"
#include "util/aeron_fileutil.h"
}

#include "../integration/aeron_test_clustered_media_driver.h"
#include "../integration/aeron_cluster_server_helper.h"

/* -----------------------------------------------------------------------
 * Egress state -- collects messages received by the cluster client
 * ----------------------------------------------------------------------- */
struct EgressState
{
    std::atomic<int> received_count{0};
    std::vector<std::string> messages;
};

static void egress_on_message(
    void *clientd,
    int64_t cluster_session_id, int64_t leadership_term_id, int64_t timestamp,
    const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    (void)cluster_session_id; (void)leadership_term_id; (void)timestamp; (void)header;
    EgressState *s = static_cast<EgressState *>(clientd);
    s->messages.emplace_back(reinterpret_cast<const char *>(buffer), length);
    s->received_count.fetch_add(1);
}

/* -----------------------------------------------------------------------
 * Fixture -- matches Java ClusterNodeTest exactly
 * ----------------------------------------------------------------------- */
class ClusterIntegrationTest : public ::testing::Test
{
protected:
    /* Matches Java @BeforeEach: ClusteredMediaDriver.launch() -- no wait for leader */
    void SetUp() override
    {
        m_base_dir = make_test_dir("aeron_cluster_integ_");
#ifdef _MSC_VER
        if (std::system(("rmdir /s /q \"" + m_base_dir + "\" 2>nul").c_str())) {}
#else
        if (std::system(("rm -rf " + m_base_dir).c_str())) {}
#endif

        m_cmd = new TestClusteredMediaDriver(0, 1, m_base_dir, std::cout);
        ASSERT_EQ(0, m_cmd->launch()) << "ClusteredMediaDriver launch failed";
    }

    /* Matches Java @AfterEach: CloseHelper.closeAll(aeronCluster, consensusModule, container, clusteredMediaDriver) */
    void TearDown() override
    {
        if (m_cluster_client != nullptr)
        {
            aeron_cluster_close(m_cluster_client);
            m_cluster_client = nullptr;
        }
        if (m_cluster_client_ctx != nullptr)
        {
            aeron_cluster_context_close(m_cluster_client_ctx);
            m_cluster_client_ctx = nullptr;
        }
        if (m_server != nullptr)
        {
            cluster_server_stop(m_server);
            m_server = nullptr;
        }
        if (m_cmd != nullptr)
        {
            m_cmd->close();
            delete m_cmd;
            m_cmd = nullptr;
        }
#ifdef _MSC_VER
        if (std::system(("rmdir /s /q \"" + m_base_dir + "\"").c_str())) {}
#else
        if (std::system(("rm -rf " + m_base_dir).c_str())) {}
#endif
    }

    /* Matches Java launchEchoService() */
    cluster_server_handle_t *launchEchoService()
    {
        m_server = cluster_service_start(
            m_cmd->aeron_dir().c_str(),
            m_cmd->cluster_dir().c_str());
        if (m_server != nullptr)
        {
            cluster_server_start_background(m_server);
        }
        return m_server;
    }

    /* Matches Java connectToCluster(listener) -- synchronous, blocks until connected */
    aeron_cluster_t *connectToCluster(EgressState &egress_state)
    {
        aeron_cluster_context_init(&m_cluster_client_ctx);
        aeron_cluster_context_set_aeron_directory_name(m_cluster_client_ctx, m_cmd->aeron_dir().c_str());
        aeron_cluster_context_set_ingress_channel(m_cluster_client_ctx, "aeron:ipc");
        aeron_cluster_context_set_ingress_stream_id(m_cluster_client_ctx, 101);
        aeron_cluster_context_set_egress_channel(m_cluster_client_ctx, "aeron:ipc");
        aeron_cluster_context_set_egress_stream_id(m_cluster_client_ctx, 102);
        aeron_cluster_context_set_ingress_endpoints(m_cluster_client_ctx,
            m_cmd->ingress_endpoints().c_str());
        aeron_cluster_context_set_on_message(m_cluster_client_ctx, egress_on_message, &egress_state);
        aeron_cluster_context_set_message_timeout_ns(m_cluster_client_ctx, INT64_C(10000000000));

        aeron_cluster_async_connect_t *async_conn = nullptr;
        if (aeron_cluster_async_connect(&async_conn, m_cluster_client_ctx) < 0)
        {
            return nullptr;
        }

        /* Spin-poll until connected (matches Java AeronCluster.connect()) */
        aeron_cluster_t *cluster = nullptr;
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (cluster == nullptr && std::chrono::steady_clock::now() < deadline)
        {
            int rc = aeron_cluster_async_connect_poll(&cluster, async_conn);
            if (rc < 0) { return nullptr; }
            if (cluster == nullptr)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }

        m_cluster_client = cluster;
        return cluster;
    }

    /* Matches Java offerMessage() -- while (offer < 0) { yield(); } */
    void offerMessage(aeron_cluster_t *cluster, const uint8_t *buffer, size_t length)
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline)
        {
            int64_t result = aeron_cluster_offer(cluster, buffer, length);
            if (result > 0) { return; }
            aeron_cluster_poll_egress(cluster);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        FAIL() << "offerMessage timed out";
    }

    /* Matches Java awaitResponse() -- while (count == 0) { pollEgress(); yield(); } */
    void awaitResponse(aeron_cluster_t *cluster, EgressState &egress_state)
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (egress_state.received_count.load() == 0 && std::chrono::steady_clock::now() < deadline)
        {
            if (aeron_cluster_poll_egress(cluster) <= 0)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
    }

    TestClusteredMediaDriver  *m_cmd               = nullptr;
    cluster_server_handle_t   *m_server            = nullptr;
    aeron_cluster_t           *m_cluster_client     = nullptr;
    aeron_cluster_context_t   *m_cluster_client_ctx = nullptr;
    std::string                m_base_dir;
};

/* -----------------------------------------------------------------------
 * Matches Java: shouldConnectAndSendKeepAlive
 * @InterruptAfter(10)
 * ----------------------------------------------------------------------- */
TEST_F(ClusterIntegrationTest, shouldConnectAndSendKeepAlive)
{
    cluster_server_handle_t *server = launchEchoService();
    ASSERT_NE(nullptr, server);

    EgressState egress_state;
    aeron_cluster_t *cluster = connectToCluster(egress_state);
    ASSERT_NE(nullptr, cluster) << "Failed to connect: " << aeron_errmsg();

    EXPECT_TRUE(aeron_cluster_send_keep_alive(cluster));
}

/* -----------------------------------------------------------------------
 * Matches Java: shouldEchoMessageViaServiceUsingDirectOffer
 * @InterruptAfter(10)
 * ----------------------------------------------------------------------- */
TEST_F(ClusterIntegrationTest, shouldEchoMessageViaService)
{
    static const char MSG[] = "Hello World!";

    cluster_server_handle_t *server = launchEchoService();
    ASSERT_NE(nullptr, server);

    EgressState egress_state;
    aeron_cluster_t *cluster = connectToCluster(egress_state);
    ASSERT_NE(nullptr, cluster) << "Failed to connect: " << aeron_errmsg();

    offerMessage(cluster, reinterpret_cast<const uint8_t *>(MSG), strlen(MSG));
    awaitResponse(cluster, egress_state);

    EXPECT_GT(egress_state.received_count.load(), 0)
        << "No echo received -- message did not travel through service";
    if (!egress_state.messages.empty())
    {
        EXPECT_EQ(std::string(MSG), egress_state.messages[0]);
    }
}
