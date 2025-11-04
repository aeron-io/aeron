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

#include <functional>

#include <gtest/gtest.h>

#include "EmbeddedMediaDriver.h"
#include "Aeron.h"
#include "TestUtil.h"

extern "C"
{
#include "aeron_image.h"
}

using namespace aeron;

class SystemTest : public testing::Test
{
public:
    SystemTest()
    {
        m_driver.start();
    }

    ~SystemTest() override
    {
        m_driver.stop();
    }

    static std::int32_t typeId(CountersReader &reader, std::int32_t counterId)
    {
        const index_t offset = aeron::concurrent::CountersReader::metadataOffset(counterId);
        return reader.metaDataBuffer().getInt32(offset + CountersReader::TYPE_ID_OFFSET);
    }

protected:
    EmbeddedMediaDriver m_driver;
};

// TODO: We need a way to clean up unresolved aeron_client_registering_resource_t* commands
TEST_F(SystemTest, DISABLED_shouldReclaimSubscriptionWhenOutOfScopeAndNotFound)
{
    std::shared_ptr<Aeron> aeron = Aeron::connect();

    aeron->addSubscription("aeron:udp?endpoint=localhost:24325", 10);
    const auto pub_reg_id = aeron->addPublication("aeron:udp?endpoint=localhost:24325", 10);

    auto pub = aeron->findPublication(pub_reg_id);
    while (!pub)
    {
        std::this_thread::yield();
        pub = aeron->findPublication(pub_reg_id);
    }
}

TEST_F(SystemTest, shouldGetDefaultPath)
{
    const std::string defaultPath = Context::defaultAeronPath();
    EXPECT_GT(defaultPath.length(), 0U);
}

TEST_F(SystemTest, shouldAddRemoveAvailableCounterHandlers)
{
    const int counterTypeId = 1001;
    int staticAvailable = 0;
    int staticUnavailable = 0;
    int dynamicAvailable = 0;
    int dynamicUnavailable = 0;
    std::uint64_t key1 = 982374234;
    std::uint64_t key2 = key1 + 1;
    std::uint8_t key[8];

    on_available_counter_t staticAvailableHandler =
        [&](CountersReader &countersReader, std::int64_t registrationId, std::int32_t counterId)
        {
            if (counterTypeId == typeId(countersReader, counterId))
            {
                staticAvailable++;
            }
        };

    on_available_counter_t staticUnavailableHandler =
        [&](CountersReader &countersReader, std::int64_t registrationId, std::int32_t counterId)
        {
            if (counterTypeId == typeId(countersReader, counterId))
            {
                staticUnavailable++;
            }
        };

    on_available_counter_t dynamicAvailableHandler =
        [&](CountersReader &countersReader, std::int64_t registrationId, std::int32_t counterId)
        {
            if (counterTypeId == typeId(countersReader, counterId))
            {
                dynamicAvailable++;
            }
        };

    on_available_counter_t dynamicUnavailableHandler =
        [&](CountersReader &countersReader, std::int64_t registrationId, std::int32_t counterId)
        {
            if (counterTypeId == typeId(countersReader, counterId))
            {
                dynamicUnavailable++;
            }
        };

    Context ctx;
    ctx.availableCounterHandler(staticAvailableHandler);
    ctx.unavailableCounterHandler(staticUnavailableHandler);
    ctx.useConductorAgentInvoker(true);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    invoker.start();

    std::int64_t availableRegId = aeron->addAvailableCounterHandler(dynamicAvailableHandler);
    std::int64_t unavailableRegId = aeron->addUnavailableCounterHandler(dynamicUnavailableHandler);
    invoker.invoke();

    ::memcpy(key, &key1, sizeof(key));
    const std::int64_t regId1 = aeron->addCounter(counterTypeId, key, sizeof(key), "my label");

    POLL_FOR(1 == staticAvailable, invoker);
    ASSERT_EQ(1, dynamicAvailable);

    {
        auto counter = aeron->findCounter(regId1);
    }

    POLL_FOR(1 == staticUnavailable, invoker);
    ASSERT_EQ(1, dynamicUnavailable);

    aeron->removeAvailableCounterHandler(availableRegId);
    aeron->removeUnavailableCounterHandler(unavailableRegId);
    invoker.invoke();

    ::memcpy(key, &key2, sizeof(key));
    const std::int64_t regId2 = aeron->addCounter(counterTypeId, key, sizeof(key), "my label");

    POLL_FOR(2 == staticAvailable, invoker);
    ASSERT_EQ(1, dynamicAvailable);

    {
        auto counter = aeron->findCounter(regId2);
    }

    POLL_FOR(2 == staticUnavailable, invoker);
    ASSERT_EQ(1, dynamicUnavailable);
}

TEST_F(SystemTest, shouldAddRemoveCloseHandler)
{
    int closeCount1 = 0;
    int closeCount2 = 0;

    Context ctx;
    ctx.useConductorAgentInvoker(true);
    auto handler =
        [&]()
        {
            closeCount1++;
        };

    {
        std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
        AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
        invoker.start();

        aeron->addCloseClientHandler(handler);
        invoker.invoke();
        std::int64_t regId2 = aeron->addCloseClientHandler(
            [&]()
            {
                closeCount2++;
            });
        invoker.invoke();

        aeron->removeCloseClientHandler(regId2);
    }

    EXPECT_EQ(1, closeCount1);
    EXPECT_EQ(0, closeCount2);
}

//
// These tests will fail with the sanitizer if not implemented correctly.
//

TEST_F(SystemTest, shouldFreeSubscriptionDataCorrectly)
{
    {
        Context ctx;
        ctx.useConductorAgentInvoker(false);

        std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
        int64_t i = aeron->addSubscription("aeron:ipc", 1000);
        std::shared_ptr<Subscription> subscription;
        do
        {
            subscription = aeron->findSubscription(i);
        }
        while (nullptr == subscription);
    }
}

TEST_F(SystemTest, shouldFreeSubscriptionDataCorrectlyWithInvoker)
{
    {
        Context ctx;
        ctx.useConductorAgentInvoker(true);
        std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
        AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
        invoker.start();

        int64_t i = aeron->addSubscription("aeron:ipc", 1000);
        std::shared_ptr<Subscription> subscription;
        do
        {
            invoker.invoke();
            subscription = aeron->findSubscription(i);
        }
        while (nullptr == subscription);
    }
}

class SystemTestParameterized : public testing::TestWithParam<std::string>
{
public:
    SystemTestParameterized()
    {
        const auto aeronDir = Context::defaultAeronPath().append("-").append(std::to_string(aeron_randomised_int32()));
        m_driver.aeronDir(aeronDir);
        m_driver.start();
    }

    ~SystemTestParameterized() override
    {
        m_driver.stop();
    }

protected:
    EmbeddedMediaDriver m_driver;
};

INSTANTIATE_TEST_SUITE_P(
    SystemTestParameterized,
    SystemTestParameterized,
    testing::Values("aeron:ipc?alias=test|term-length=64k", "aeron:udp?alias=test|endpoint=localhost:8092|term-length=64k"));

TEST_P(SystemTestParameterized, shouldFreeUnavailableImage)
{
    std::string channel = GetParam();
    const int stream_id = 1000;
    Context ctx;
    ctx
    .useConductorAgentInvoker(true)
    .resourceLingerTimeout(10)
    .idleSleepDuration(10)
    .aeronDir(m_driver.aeronDir());

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);

    const int64_t pub_registration_id = aeron->addExclusivePublication(channel, stream_id);
    std::shared_ptr<ExclusivePublication> publication;
    do
    {
        aeron->conductorAgentInvoker().invoke();
        publication = aeron->findExclusivePublication(pub_registration_id);
    }
    while (nullptr == publication);

    std::atomic<int64_t> image_correlation_id;
    image_correlation_id = -1;
    std::atomic<bool> image_unavailable;
    image_unavailable = false;
    const int64_t sub_registration_id = aeron->addSubscription(
        channel,
        stream_id,
        [&image_correlation_id](Image &image)
        {
            image_correlation_id = image.correlationId();
        },
        [&image_unavailable](Image &image)
        {
            image_unavailable = true;
        });
    std::shared_ptr<Subscription> subscription;
    do
    {
        aeron->conductorAgentInvoker().invoke();
        subscription = aeron->findSubscription(sub_registration_id);
    }
    while (nullptr == subscription);

    aeron_subscription_t *raw_subscription = subscription->subscription();
    aeron_image_t *raw_image = nullptr;
    {
        std::shared_ptr<Image> image;
        do
        {
            aeron->conductorAgentInvoker().invoke();
            image = subscription->imageBySessionId(publication->sessionId());
        }
        while (nullptr == image);

        // ref_cnt == 2 - because shared_ptr<Image>

        while (-1 == image_correlation_id)
        {
            aeron->conductorAgentInvoker().invoke();
        }
        ASSERT_EQ(image_correlation_id, image->correlationId());

        auto image_by_index = subscription->imageByIndex(0);
        EXPECT_NE(image, image_by_index);
        EXPECT_EQ(image_correlation_id, image_by_index->correlationId());

        // ref_cnt == 3 - because image_by_index

        raw_image =
            aeron_subscription_image_by_session_id(raw_subscription, publication->sessionId());
        EXPECT_EQ(4, aeron_image_refcnt_acquire(raw_image));

        EXPECT_EQ(0, aeron_subscription_image_release(raw_subscription, raw_image));
        ASSERT_EQ(3, aeron_image_refcnt_acquire(raw_image));
    }

    EXPECT_EQ(1, aeron_image_refcnt_acquire(raw_image));
    EXPECT_EQ(1, subscription->imageCount());
    EXPECT_EQ(1, aeron_subscription_image_count(raw_subscription));

    char log_buffer_file[AERON_MAX_PATH];
    EXPECT_GT(
        aeron_network_publication_location(log_buffer_file, sizeof(log_buffer_file), aeron->context().aeronDir().c_str(), pub_registration_id),
        0);
    EXPECT_GE(aeron_file_length(log_buffer_file), 0);
    const auto pub_log_file = std::string().append(log_buffer_file);

    EXPECT_GT(
        aeron_publication_image_location(log_buffer_file, sizeof(log_buffer_file), aeron->context().aeronDir().c_str(), image_correlation_id),
        0);
    const auto image_log_file = -1 == aeron_file_length(log_buffer_file) ? pub_log_file : std::string().append(log_buffer_file);

    publication.reset();

    while (!image_unavailable)
    {
        aeron->conductorAgentInvoker().invoke();
    }
    ASSERT_TRUE(image_unavailable);

    while (0 != subscription->imageCount())
    {
        aeron->conductorAgentInvoker().invoke();
    }

    auto deadline_ns = std::chrono::nanoseconds(m_driver.livenessTimeoutNs());
    auto zero_ns = std::chrono::nanoseconds(0);
    auto sleep_ms = std::chrono::milliseconds(10);
    while (deadline_ns > zero_ns)
    {
      if (-1 == aeron_file_length(image_log_file.c_str()))
      {
          break;
      }
      deadline_ns -= sleep_ms;
      std::this_thread::sleep_for(sleep_ms);
      aeron->conductorAgentInvoker().invoke();
    }
    EXPECT_GT(deadline_ns, zero_ns);

    EXPECT_EQ(-1, aeron_file_length(image_log_file.c_str())) << image_log_file << " not deleted";
    EXPECT_EQ(-1, aeron_file_length(pub_log_file.c_str())) << pub_log_file << " not deleted";
}
