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

#include <atomic>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "aeron_test_base.h"

#define SPY_RACE_PUB_URI   "aeron:udp?endpoint=localhost:24370"
#define SPY_RACE_SPY_URI   "aeron-spy:aeron:udp?endpoint=localhost:24370"
#define SPY_RACE_STREAM_ID (1234)
#define SPY_RACE_NUM_SPIES (32)

class SpySubscribableRaceTest : public CSystemTestBase, public testing::Test
{
public:
    SpySubscribableRaceTest() : CSystemTestBase(
        {},
        [](aeron_driver_context_t *ctx)
        {
            aeron_driver_context_set_threading_mode(ctx, AERON_THREADING_MODE_DEDICATED);
            aeron_driver_context_set_spies_simulate_connection(ctx, true);

            aeron_driver_context_set_sender_idle_strategy(ctx, "noop");
            aeron_driver_context_set_conductor_idle_strategy(ctx, "noop");
            aeron_driver_context_set_receiver_idle_strategy(ctx, "noop");

            aeron_driver_context_set_term_buffer_length(ctx, 64 * 1024);
        })
    {}
};

TEST_F(SpySubscribableRaceTest, shouldNotRaceWhenAddingSpiesWhileSending)
{
    ASSERT_TRUE(connect()) << aeron_errmsg();

    aeron_async_add_publication_t *pub_async = nullptr;
    ASSERT_EQ(aeron_async_add_publication(
        &pub_async, m_aeron, SPY_RACE_PUB_URI, SPY_RACE_STREAM_ID), 0);
    aeron_publication_t *publication = awaitPublicationOrError(pub_async);
    ASSERT_TRUE(publication) << aeron_errmsg();

    std::atomic<bool> stop_sender{ false };
    std::thread sender_thread(
        [&]()
        {
            static const uint8_t payload[32] = {};
            while (!stop_sender.load(std::memory_order_relaxed))
            {
                aeron_publication_offer(publication, payload, sizeof(payload), nullptr, nullptr);
            }
        });

    std::vector<aeron_async_add_subscription_t *> sub_asyncs(SPY_RACE_NUM_SPIES, nullptr);
    std::vector<aeron_subscription_t *>           subs(SPY_RACE_NUM_SPIES, nullptr);

    for (int i = 0; i < SPY_RACE_NUM_SPIES; i++)
    {
        ASSERT_EQ(aeron_async_add_subscription(
            &sub_asyncs[i], m_aeron, SPY_RACE_SPY_URI, SPY_RACE_STREAM_ID,
            nullptr, nullptr, nullptr, nullptr), 0)
            << "spy " << i << ": " << aeron_errmsg();
    }

    for (int i = 0; i < SPY_RACE_NUM_SPIES; i++)
    {
        subs[i] = awaitSubscriptionOrError(sub_asyncs[i]);
        ASSERT_TRUE(subs[i]) << "spy " << i << " registration failed: " << aeron_errmsg();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::atomic<int> closes_remaining{ SPY_RACE_NUM_SPIES };
    auto on_close = [](void *clientd)
    {
        static_cast<std::atomic<int> *>(clientd)->fetch_sub(1, std::memory_order_release);
    };

    for (auto *sub : subs)
    {
        aeron_subscription_close(sub, on_close, &closes_remaining);
    }

    while (closes_remaining.load(std::memory_order_acquire) > 0)
    {
        std::this_thread::yield();
    }

    stop_sender.store(true);
    sender_thread.join();

    aeron_publication_close(publication, nullptr, nullptr);
}
