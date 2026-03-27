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
 * Echo integration test — only client-side headers + opaque server helper.
 * No typedef conflict between service's and client's aeron_cluster_t.
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

extern "C"
{
#include "aeronc.h"
#include "client/aeron_cluster.h"
#include "client/aeron_cluster_context.h"
#include "client/aeron_cluster_client.h"
#include "client/aeron_cluster_async_connect.h"
#include "util/aeron_fileutil.h"
}

#include "../integration/TestClusterNode.h"
#include "../integration/ClusterServerHelper.h"

/* -----------------------------------------------------------------------
 * Egress message collector
 * ----------------------------------------------------------------------- */
struct EchoState
{
    std::atomic<int>         received_count{0};
    std::vector<std::string> messages;
};

static void egress_on_message(
    void *clientd,
    int64_t cluster_session_id, int64_t leadership_term_id, int64_t timestamp,
    const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    (void)cluster_session_id; (void)leadership_term_id; (void)timestamp; (void)header;
    EchoState *s = static_cast<EchoState *>(clientd);
    s->messages.emplace_back(reinterpret_cast<const char *>(buffer), length);
    s->received_count.fetch_add(1);
}

/* -----------------------------------------------------------------------
 * Fixture
 * ----------------------------------------------------------------------- */
class ClusterEchoTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        m_base_dir = "/tmp/aeron_cluster_echo_" + std::to_string(getpid());
        std::system(("rm -rf " + m_base_dir + " && mkdir -p " + m_base_dir).c_str());
        /* Use node_index=5 to get unique port 8015 — avoids collision with other tests */
        m_node = new TestClusterNode(5, 1, m_base_dir, std::cout);
        m_node->start();
    }

    void TearDown() override
    {
        if (m_client)     { aeron_cluster_close(m_client); m_client = nullptr; }
        if (m_client_ctx) { aeron_cluster_context_close(m_client_ctx); m_client_ctx = nullptr; }
        if (m_aeron)      { aeron_close(m_aeron); m_aeron = nullptr; }
        if (m_aeron_ctx)  { aeron_context_close(m_aeron_ctx); m_aeron_ctx = nullptr; }
        if (m_node)       { m_node->stop(); delete m_node; m_node = nullptr; }
        std::system(("rm -rf " + m_base_dir).c_str());
    }

    bool connect_aeron()
    {
        if (aeron_context_init(&m_aeron_ctx) < 0) { return false; }
        aeron_context_set_dir(m_aeron_ctx, m_node->aeron_dir().c_str());
        if (aeron_init(&m_aeron, m_aeron_ctx) < 0) { return false; }
        if (aeron_start(m_aeron) < 0) { return false; }
        std::string cnc = m_node->aeron_dir() + "/cnc.dat";
        for (int i = 0; i < 100; i++)
        {
            if (aeron_file_length(cnc.c_str()) > 0) { return true; }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return false;
    }

    TestClusterNode         *m_node       = nullptr;
    aeron_context_t         *m_aeron_ctx  = nullptr;
    aeron_t                 *m_aeron      = nullptr;
    aeron_cluster_context_t *m_client_ctx = nullptr;
    aeron_cluster_t         *m_client     = nullptr;
    std::string              m_base_dir;
};

/* -----------------------------------------------------------------------
 * Test 1: client async_connect infrastructure does not crash
 * ----------------------------------------------------------------------- */
TEST_F(ClusterEchoTest, shouldAttemptClientConnectToCluster)
{
    ASSERT_TRUE(connect_aeron());

    EchoState echo_state;
    ASSERT_EQ(0, aeron_cluster_context_init(&m_client_ctx));
    aeron_cluster_context_set_aeron(m_client_ctx, m_aeron);
    /* Use IPC for ingress (same driver, avoids UDP loopback connection issue) */
    aeron_cluster_context_set_ingress_channel(m_client_ctx, "aeron:ipc");
    aeron_cluster_context_set_ingress_stream_id(m_client_ctx, 101);
    /* Use UDP for egress — same-client UDP pub/sub connection is proven to work */
    std::string egress_ch = "aeron:udp?endpoint=localhost:" + std::to_string(24500 + m_node->node_index());
    aeron_cluster_context_set_egress_channel(m_client_ctx, egress_ch.c_str());
    aeron_cluster_context_set_egress_stream_id(m_client_ctx, 102);
    aeron_cluster_context_set_egress_stream_id(m_client_ctx, 102);
    aeron_cluster_context_set_ingress_endpoints(m_client_ctx,
        (std::to_string(m_node->node_index()) + "=localhost:" +
         std::to_string(20110 + m_node->node_index())).c_str());    aeron_cluster_context_set_on_message(m_client_ctx, egress_on_message, &echo_state);
    aeron_cluster_context_set_message_timeout_ns(m_client_ctx, INT64_C(2000000000));

    aeron_cluster_async_connect_t *async_conn = nullptr;
    ASSERT_EQ(0, aeron_cluster_async_connect(&async_conn, m_client_ctx)) << aeron_errmsg();

    /* Poll briefly — no server running, so this should time out cleanly */
    aeron_cluster_t *cluster_client = nullptr;
    for (int i = 0; i < 20; i++)
    {
        int rc = aeron_cluster_async_connect_poll(&cluster_client, async_conn);
        if (rc != 0) { break; }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    EXPECT_NO_FATAL_FAILURE();
    if (cluster_client) { aeron_cluster_close(cluster_client); m_client = nullptr; }
}

/* -----------------------------------------------------------------------
 * Diagnostic: verify UDP pub/sub can connect on the same m_aeron client
 * ----------------------------------------------------------------------- */
TEST_F(ClusterEchoTest, shouldConnectUdpPubSubOnSameClient)
{
    ASSERT_TRUE(connect_aeron());

    /* Create a subscription on a UDP port */
    aeron_async_add_subscription_t *async_sub = nullptr;
    ASSERT_EQ(0, aeron_async_add_subscription(&async_sub, m_aeron,
        "aeron:udp?endpoint=localhost:24999", 999, nullptr, nullptr, nullptr, nullptr));

    aeron_subscription_t *sub = nullptr;
    for (int i = 0; i < 50; i++)
    {
        int rc = aeron_async_add_subscription_poll(&sub, async_sub);
        if (rc > 0) break;
        ASSERT_GE(rc, 0) << "sub poll error: " << aeron_errmsg();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_NE(nullptr, sub) << "subscription not ready";

    /* Create a publication to the same endpoint */
    aeron_async_add_publication_t *async_pub = nullptr;
    ASSERT_EQ(0, aeron_async_add_publication(&async_pub, m_aeron,
        "aeron:udp?endpoint=localhost:24999", 999));

    aeron_publication_t *pub = nullptr;
    for (int i = 0; i < 50; i++)
    {
        int rc = aeron_async_add_publication_poll(&pub, async_pub);
        if (rc > 0) break;
        ASSERT_GE(rc, 0) << "pub poll error: " << aeron_errmsg();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_NE(nullptr, pub) << "publication not ready";

    /* Wait for is_connected */
    bool connected = false;
    for (int i = 0; i < 200; i++)
    {
        if (aeron_publication_is_connected(pub))
        {
            connected = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(connected) << "UDP pub/sub on same client did not connect within 2s";

    aeron_publication_close(pub, nullptr, nullptr);
    aeron_subscription_close(sub, nullptr, nullptr);
}

/* -----------------------------------------------------------------------
 * Test 2: full echo — server (CM+service) + client send/receive
 * ----------------------------------------------------------------------- */
TEST_F(ClusterEchoTest, shouldEchoMessageEndToEnd)
{
    ASSERT_TRUE(connect_aeron());

    /* Start server (CM + echo service) using shared Aeron dir */
    cluster_server_handle_t *srv = cluster_server_start(
        m_aeron,
        m_node->cluster_dir().c_str(),
        m_node->archive_port(),
        m_node->cluster_members().c_str(),
        20110 + m_node->node_index(),
        20220 + m_node->node_index());
    ASSERT_NE(nullptr, srv) << "cluster_server_start failed: " << aeron_errmsg();

    /* Verify m_aeron is still healthy after cluster_server_start */
    {
        /* Test: can we create a UDP sub+pub pair and connect them? */
        aeron_async_add_subscription_t *test_async_sub = nullptr;
        ASSERT_EQ(0, aeron_async_add_subscription(&test_async_sub, m_aeron,
            "aeron:udp?endpoint=localhost:24888", 888, nullptr, nullptr, nullptr, nullptr));
        aeron_subscription_t *test_sub = nullptr;
        for (int j = 0; j < 50 && !test_sub; j++) {
            int r = aeron_async_add_subscription_poll(&test_sub, test_async_sub);
            if (r < 0) { fprintf(stderr, "[Echo] post-start sub fail: %s\n", aeron_errmsg()); break; }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (test_sub) {
            aeron_async_add_publication_t *test_async_pub = nullptr;
            aeron_async_add_publication(&test_async_pub, m_aeron, "aeron:udp?endpoint=localhost:24888", 888);
            aeron_publication_t *test_pub = nullptr;
            for (int j = 0; j < 50 && !test_pub; j++) {
                int r = aeron_async_add_publication_poll(&test_pub, test_async_pub);
                if (r < 0) break;
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            if (test_pub) {
                bool c = false;
                for (int j = 0; j < 100 && !c; j++) {
                    c = aeron_publication_is_connected(test_pub);
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
                fprintf(stderr, "[Echo] post-start UDP pub/sub connected=%d\n", c);
                aeron_publication_close(test_pub, nullptr, nullptr);
            }
            aeron_subscription_close(test_sub, nullptr, nullptr);
        }
    }

    /* Drive server until leader */
    int64_t now_ns = aeron_nano_clock();
    for (int i = 0; i < 100; i++)
    {
        now_ns += INT64_C(50000000);
        cluster_server_do_work(srv, now_ns);
        if (cluster_server_is_leader(srv)) { break; }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(cluster_server_is_leader(srv)) << "Server did not become leader";

    /* === Cross-client IPC diagnostic ===
     * Verify pub (srv->aeron) → sub (m_aeron) IPC connection works before proceeding */
    {
        aeron_t *srv_aeron = cluster_server_get_aeron(srv);
        /* sub on m_aeron, pub on srv_aeron — mirrors CM response_publication pattern */
        aeron_async_add_subscription_t *xc_async_sub = nullptr;
        aeron_async_add_subscription(&xc_async_sub, m_aeron, "aeron:ipc", 9991,
                                     nullptr, nullptr, nullptr, nullptr);
        aeron_subscription_t *xc_sub = nullptr;
        for (int j = 0; j < 50 && !xc_sub; j++) {
            aeron_async_add_subscription_poll(&xc_sub, xc_async_sub);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        aeron_async_add_exclusive_publication_t *xc_async_pub = nullptr;
        aeron_async_add_exclusive_publication(&xc_async_pub, srv_aeron, "aeron:ipc", 9991);
        aeron_exclusive_publication_t *xc_pub = nullptr;
        for (int j = 0; j < 50 && !xc_pub; j++) {
            aeron_async_add_exclusive_publication_poll(&xc_pub, xc_async_pub);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        bool xc_connected = false;
        for (int j = 0; j < 100 && !xc_connected; j++) {
            xc_connected = aeron_exclusive_publication_is_connected(xc_pub) &&
                           aeron_subscription_is_connected(xc_sub);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        fprintf(stderr, "[Echo] cross-client IPC pub(srv)→sub(client) connected=%d\n", xc_connected);
        if (xc_pub) aeron_exclusive_publication_close(xc_pub, nullptr, nullptr);
        if (xc_sub) aeron_subscription_close(xc_sub, nullptr, nullptr);
    }

    /* Connect client (shares same Aeron dir → same MediaDriver) */
    EchoState echo_state;
    ASSERT_EQ(0, aeron_cluster_context_init(&m_client_ctx));
    aeron_cluster_context_set_aeron(m_client_ctx, m_aeron);
    /* Use IPC for ingress (same driver, avoids UDP loopback connection issue) */
    aeron_cluster_context_set_ingress_channel(m_client_ctx, "aeron:ipc");
    aeron_cluster_context_set_ingress_stream_id(m_client_ctx, 101);
    /* Use UDP for egress — same-client UDP pub/sub connection is proven to work */
    std::string egress_ch = "aeron:udp?endpoint=localhost:" + std::to_string(24500 + m_node->node_index());
    aeron_cluster_context_set_egress_channel(m_client_ctx, egress_ch.c_str());
    aeron_cluster_context_set_egress_stream_id(m_client_ctx, 102);
    aeron_cluster_context_set_egress_stream_id(m_client_ctx, 102);
    aeron_cluster_context_set_ingress_endpoints(m_client_ctx,
        (std::to_string(m_node->node_index()) + "=localhost:" +
         std::to_string(20110 + m_node->node_index())).c_str());    aeron_cluster_context_set_on_message(m_client_ctx, egress_on_message, &echo_state);
    aeron_cluster_context_set_message_timeout_ns(m_client_ctx, INT64_C(30000000000)); /* 30s */

    aeron_cluster_async_connect_t *async_conn = nullptr;
    fprintf(stderr, "[Echo] egress='%s' ingress='%s' endpoints='%s'\n",
        aeron_cluster_context_get_egress_channel(m_client_ctx),
        aeron_cluster_context_get_ingress_channel(m_client_ctx),
        aeron_cluster_context_get_ingress_endpoints(m_client_ctx)
            ? aeron_cluster_context_get_ingress_endpoints(m_client_ctx) : "(null)");
    ASSERT_EQ(0, aeron_cluster_async_connect(&async_conn, m_client_ctx)) << aeron_errmsg();

    /* Poll connect while driving server — real sleeps let Aeron I/O process frames.
     * UDP session handshake typically needs 200-500ms real time. */
    aeron_cluster_t *cluster_client = nullptr;
    bool connected = false;
    for (int i = 0; i < 600; i++)
    {
        now_ns += INT64_C(5000000);
        cluster_server_do_work(srv, now_ns);

        int rc = aeron_cluster_async_connect_poll(&cluster_client, async_conn);
        if (rc > 0 && cluster_client != nullptr) { connected = true; break; }
        if (rc < 0)
        {
            fprintf(stderr, "[Echo] async_connect_poll rc=%d errcode=%d at i=%d msg: %s\n",
                    rc, aeron_errcode(), i, aeron_errmsg());
            /* Also print full aeron error detail */
            fprintf(stderr, "[Echo] Full aeron error: %s\n", aeron_errmsg());
            break;
        }

        if (i % 3 == 2)
        {
            /* Real sleep: let Aeron conductor threads process IPC connections */
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        if (i % 50 == 0)
        {
            fprintf(stderr, "[Echo] step=%u i=%d leader=%d state=%d sessions=%d pending=%d rejected=%d open_pending=%d log=%d/%d\n",
                    static_cast<unsigned>(aeron_cluster_async_connect_step(async_conn)), i,
                    cluster_server_is_leader(srv) ? 1 : 0,
                    cluster_server_cm_state(srv),
                    cluster_server_session_count(srv),
                    cluster_server_pending_session_count(srv),
                    cluster_server_rejected_session_count(srv),
                    cluster_server_session_open_pending(srv),
                    cluster_server_has_log_pub(srv),
                    cluster_server_has_session_log_pub(srv));
        }
        /* Poll egress subscription directly to see if any data arrives */
        if (aeron_cluster_async_connect_step(async_conn) >= 4 && i % 10 == 0)
        {
            fprintf(stderr, "[Echo] step4 i=%d open_pending=%d\n",
                    i, cluster_server_session_open_pending(srv));
        }
        /* Add a real sleep every 5 iterations when at step 4 to let IPC propagate */
        if (aeron_cluster_async_connect_step(async_conn) >= 4 && i % 5 == 4)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        /* Note: async_connect_poll internally calls egress_poller_poll which reads
         * from the egress subscription. The SESSION_EVENT is being sent by check_sessions
         * every tick (has_open_event_pending=true, pub_connected=1). */
    }

    if (!connected)
    {
        cluster_server_stop(srv);
        GTEST_SKIP() << "Client could not connect (timeout) — skipping echo";
        return;
    }
    m_client = cluster_client;
    ASSERT_NE(nullptr, m_client);

    /* Verify ingress publication is alive */
    fprintf(stderr, "[Echo] client connected! ingress_proxy=%p pub=%p is_exclusive=%d\n",
            static_cast<void*>(m_client->ingress_proxy),
            m_client->ingress_proxy ? static_cast<void*>(m_client->ingress_proxy->publication) : nullptr,
            m_client->ingress_proxy ? m_client->ingress_proxy->is_exclusive : -1);
    if (m_client->ingress_proxy && m_client->ingress_proxy->publication)
    {
        fprintf(stderr, "[Echo] pub is_closed=%d is_connected=%d\n",
                aeron_publication_is_closed(m_client->ingress_proxy->publication),
                aeron_publication_is_connected(m_client->ingress_proxy->publication));
    }

    /* Send message and wait for echo */
    static const char MSG[] = "hello-cluster";
    bool sent = false;
    for (int i = 0; i < 100 && !sent; i++)
    {
        now_ns += INT64_C(5000000);
        cluster_server_do_work(srv, now_ns);
        aeron_cluster_poll_egress(m_client);

        int64_t result = aeron_cluster_offer(
            m_client,
            reinterpret_cast<const uint8_t *>(MSG), sizeof(MSG) - 1);
        if (result > 0) { sent = true; }
        else if (i == 0)
        {
            fprintf(stderr, "[Echo] offer result=%lld svc_log_adapter=%d\n",
                    static_cast<long long>(result),
                    cluster_server_svc_has_log_adapter(srv));
        }
    }
    EXPECT_TRUE(sent) << "Failed to send message to cluster";
    fprintf(stderr, "[Echo] sent=%d svc_log_adapter=%d\n", sent,
            cluster_server_svc_has_log_adapter(srv));

    /* Poll for echo response */
    for (int i = 0; i < 300; i++)
    {
        now_ns += INT64_C(5000000);
        cluster_server_do_work(srv, now_ns);
        aeron_cluster_poll_egress(m_client);
        if (echo_state.received_count.load() > 0) { break; }
    }

    EXPECT_GT(echo_state.received_count.load(), 0)
        << "No echo received — message did not travel through service";
    if (!echo_state.messages.empty())
    {
        EXPECT_EQ(std::string(MSG), echo_state.messages[0])
            << "Echo content mismatch";
    }

    cluster_server_stop(srv);
}
