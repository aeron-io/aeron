/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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

#include <thread>
#include <atomic>

#include <gtest/gtest.h>

extern "C"
{
#include "aeronc.h"
#include "aeron_common.h"
#include "aeronmd.h"
}

class EmbeddedDriver
{
public:
    ~EmbeddedDriver()
    {
        m_running = false;
        if (m_thread.joinable())
        {
            m_thread.join();
        }
        aeron_driver_close(m_driver);
        aeron_driver_context_close(m_context);
    }

    void start(const std::string &aeron_dir)
    {
        aeron_driver_context_init(&m_context);
        aeron_driver_context_set_dir(m_context, aeron_dir.c_str());
        aeron_driver_context_set_dir_delete_on_start(m_context, true);
        aeron_driver_context_set_dir_delete_on_shutdown(m_context, true);
        aeron_driver_context_set_threading_mode(m_context, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_shared_idle_strategy(m_context, "sleep-ns");
        aeron_driver_context_set_term_buffer_sparse_file(m_context, true);
        aeron_driver_context_set_term_buffer_length(m_context, 64 * 1024);

        aeron_driver_init(&m_driver, m_context);
        aeron_driver_start(m_driver, true);
        m_thread = std::thread([this]()
        {
            while (m_running)
            {
                aeron_driver_main_idle_strategy(m_driver, aeron_driver_main_do_work(m_driver));
            }
        });
    }

private:
    aeron_driver_context_t *m_context = nullptr;
    aeron_driver_t *m_driver = nullptr;
    std::atomic<bool> m_running{true};
    std::thread m_thread;
};

class AeronAsyncCmdCloseTest : public testing::Test
{
protected:
    void SetUp() override
    {
        char dir[AERON_MAX_PATH];
        aeron_default_path(dir, sizeof(dir));
        m_aeron_dir = dir;
        m_driver.start(m_aeron_dir);
    }

    struct Client
    {
        aeron_context_t *ctx;
        aeron_t *aeron;

        void close()
        {
            aeron_close(aeron);
            aeron_context_close(ctx);
        }
    };

    Client createClient()
    {
        Client c = {};
        aeron_context_init(&c.ctx);
        aeron_context_set_dir(c.ctx, m_aeron_dir.c_str());
        aeron_init(&c.aeron, c.ctx);
        aeron_start(c.aeron);
        return c;
    }

    std::string m_aeron_dir;
    EmbeddedDriver m_driver;
};

// This test will "pass"; it must be run with a memory leak checker to verify no leaks.
TEST_F(AeronAsyncCmdCloseTest, shouldNotLeakWhenClosingWithPendingAsyncOperations)
{
    for (int i = 0; i < 10; i++)
    {
        Client client = createClient();

        // Start async operations but don't poll them to completion
        aeron_async_add_subscription_t *async_sub = nullptr;
        ASSERT_EQ(0, aeron_async_add_subscription(
            &async_sub, client.aeron, "aeron:udp?endpoint=localhost:0", 1001,
            nullptr, nullptr, nullptr, nullptr)) << aeron_errmsg();

        aeron_async_add_exclusive_publication_t *async_pub = nullptr;
        ASSERT_EQ(0, aeron_async_add_exclusive_publication(
            &async_pub, client.aeron, "aeron:udp?endpoint=localhost:0", 1002)) << aeron_errmsg();

        // Poll a few times — some operations may partially complete
        for (int j = 0; j < i; j++)
        {
            aeron_main_do_work(client.aeron);
            std::this_thread::yield();
        }

        // Also cancel one to exercise the remove command path
        if (async_sub != nullptr)
        {
            aeron_async_add_subscription_cancel(client.aeron, async_sub);
        }

        // Close without completing all operations. Before the fix, this
        // leaked the pending async commands and registering resources.
        client.close();
    }
}

// This test will "pass"; it must be run with a memory leak checker to verify no leaks.
TEST_F(AeronAsyncCmdCloseTest, shouldNotLeakWhenClosingAfterCancellingAsyncOperations)
{
    Client client = createClient();

    // Create several async operations
    for (int i = 0; i < 5; i++)
    {
        aeron_async_add_subscription_t *async_sub = nullptr;
        ASSERT_EQ(0, aeron_async_add_subscription(
            &async_sub, client.aeron, "aeron:udp?endpoint=localhost:0", 2000 + i,
            nullptr, nullptr, nullptr, nullptr)) << aeron_errmsg();

        aeron_async_add_exclusive_publication_t *async_pub = nullptr;
        ASSERT_EQ(0, aeron_async_add_exclusive_publication(
            &async_pub, client.aeron, "aeron:udp?endpoint=localhost:0", 3000 + i)) << aeron_errmsg();

        // Cancel immediately — enqueues remove commands
        aeron_async_add_subscription_cancel(client.aeron, async_sub);
        aeron_async_add_exclusive_publication_cancel(client.aeron, async_pub);
    }

    // Close immediately — the remove commands may still be in the queue
    client.close();
}
