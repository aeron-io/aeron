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
#include "aeron_socket.h"

using namespace aeron;

class RejectImageTestBase
{
public:
    RejectImageTestBase()
    {
        m_driver.start();
    }

    ~RejectImageTestBase()
    {
        m_driver.stop();
    }

    static std::int32_t typeId(CountersReader &reader, std::int32_t counterId)
    {
        const index_t offset = aeron::concurrent::CountersReader::metadataOffset(counterId);
        return reader.metaDataBuffer().getInt32(offset + CountersReader::TYPE_ID_OFFSET);
    }

    static std::string addressAsString(int16_t addressType, uint8_t *address)
    {
        if (AERON_RESPONSE_ADDRESS_TYPE_IPV4 == addressType)
        {
            char buffer[INET_ADDRSTRLEN] = {};
            inet_ntop(AF_INET, address, buffer, sizeof(buffer));
            return std::string{buffer};
        }
        else if (AERON_RESPONSE_ADDRESS_TYPE_IPV6 == addressType)
        {
            char buffer[INET6_ADDRSTRLEN] = {};
            inet_ntop(AF_INET6, address, buffer, sizeof(buffer));
            return "[" + std::string{buffer} + "]";
        }

        return "";
    }

    static std::shared_ptr<Aeron> connectClient(std::atomic<std::int32_t > &counter)
    {
        Context ctx;
        ctx.useConductorAgentInvoker(true);

        on_publication_error_frame_t errorFrameHandler =
            [&](aeron::status::PublicationErrorFrame &errorFrame)
            {
                std::atomic_fetch_add(&counter, 1);
            };

        ctx.errorFrameHandler(errorFrameHandler);
        return Aeron::connect(ctx);
    }

protected:
    EmbeddedMediaDriver m_driver;
};

class RejectImageTestSimple : public RejectImageTestBase, public testing::Test
{
};

class RejectImageTest : public RejectImageTestBase, public testing::TestWithParam<std::string>
{
};

INSTANTIATE_TEST_SUITE_P(
    RejectImageTestWithParam, RejectImageTest, testing::Values("127.0.0.1", "[::1]"));

TEST_P(RejectImageTest, shouldRejectImage)
{
    Context ctx;
    ctx.useConductorAgentInvoker(true);
    std::string address = GetParam();
    std::uint16_t port = 10000;
    std::string control = address + ":" + std::to_string(10001);
    std::string endpoint = address + ":" + std::to_string(port);

    aeron::status::PublicationErrorFrame error{ nullptr };

    on_publication_error_frame_t errorFrameHandler =
        [&](aeron::status::PublicationErrorFrame &errorFrame)
        {
            error = errorFrame;
            return;
        };

    ctx.errorFrameHandler(errorFrameHandler);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    invoker.start();

    std::int64_t groupTag = 99999;
    std::string mdc = "aeron:udp?control-mode=dynamic|control=" + control + "|fc=tagged,g:" + std::to_string(groupTag);
    std::string channel = "aeron:udp?endpoint=" + endpoint + "|control=" + control + "|gtag=" + std::to_string(groupTag);

    std::int64_t pubId = aeron->addPublication(mdc, 10000);
    std::int64_t subId = aeron->addSubscription(channel, 10000);
    invoker.invoke();

    POLL_FOR_NON_NULL(pub, aeron->findPublication(pubId), invoker);
    POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
    POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);

    std::string message = "Hello World!";

    auto *data = reinterpret_cast<const uint8_t *>(message.c_str());
    POLL_FOR(0 < pub->offer(data, message.length()), invoker);
    POLL_FOR(0 < sub->poll(
        [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            EXPECT_EQ(message, buffer.getStringWithoutLength(offset, length));
        },
        1), invoker);

    POLL_FOR(1 == sub->imageCount(), invoker);

    const std::shared_ptr<Image> image = sub->imageByIndex(0);
    image->reject("No Longer Valid");

    POLL_FOR(error.isValid(), invoker);
    ASSERT_EQ(pubId, error.registrationId());
    ASSERT_EQ(pub->sessionId(), error.sessionId());
    ASSERT_EQ(pub->streamId(), error.streamId());
    ASSERT_EQ(groupTag, error.groupTag());
    ASSERT_EQ(port, error.sourcePort());
    ASSERT_EQ(address, addressAsString(error.sourceAddressType(), error.sourceAddress()));
}

TEST_P(RejectImageTest, shouldRejectImageForExclusive)
{
    std::string address = GetParam();

    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::atomic<std::int32_t> errorFrameCount{0};

    on_publication_error_frame_t errorFrameHandler =
        [&](aeron::status::PublicationErrorFrame &errorFrame)
        {
            std::atomic_fetch_add(&errorFrameCount, 1);
            return;
        };

    ctx.errorFrameHandler(errorFrameHandler);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    invoker.start();

    std::int64_t pubId = aeron->addExclusivePublication("aeron:udp?endpoint=" + address + ":10000", 10000);
    std::int64_t subId = aeron->addSubscription("aeron:udp?endpoint=" + address + ":10000", 10000);
    invoker.invoke();

    POLL_FOR_NON_NULL(pub, aeron->findExclusivePublication(pubId), invoker);
    POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
    POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);

    std::string message = "Hello World!";

    auto *data = reinterpret_cast<const uint8_t *>(message.c_str());
    POLL_FOR(0 < pub->offer(data, message.length()), invoker);
    POLL_FOR(0 < sub->poll(
        [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            EXPECT_EQ(message, buffer.getStringWithoutLength(offset, length));
        },
        1), invoker);

    POLL_FOR(1 == sub->imageCount(), invoker);

    const std::shared_ptr<Image> image = sub->imageByIndex(0);
    image->reject("No Longer Valid");

    POLL_FOR(0 < errorFrameCount, invoker);
}

TEST_P(RejectImageTest, shouldOnlySeePublicationErrorFramesForPublicationsAddedToTheClient)
{
    const std::string address = GetParam();
    const std::string channel = "aeron:udp?endpoint=" + address + ":10000";
    const int streamId = 10000;

    std::atomic<std::int32_t> errorFrameCount0{0};
    std::shared_ptr<Aeron> aeron0 = connectClient(errorFrameCount0);
    AgentInvoker<ClientConductor> &invoker0 = aeron0->conductorAgentInvoker();

    std::atomic<std::int32_t> errorFrameCount1{0};
    std::shared_ptr<Aeron> aeron1 = connectClient(errorFrameCount1);
    AgentInvoker<ClientConductor> &invoker1 = aeron1->conductorAgentInvoker();

    std::atomic<std::int32_t> errorFrameCount2{0};
    std::shared_ptr<Aeron> aeron2 = connectClient(errorFrameCount2);
    AgentInvoker<ClientConductor> &invoker2 = aeron2->conductorAgentInvoker();

    invoker0.start();
    invoker1.start();
    invoker2.start();

    std::int64_t pub0Id = aeron0->addPublication(channel, streamId);
    std::int64_t subId = aeron0->addSubscription(channel, streamId);
    invoker0.invoke();

    POLL_FOR_NON_NULL(pub0, aeron0->findPublication(pub0Id), invoker0);
    POLL_FOR_NON_NULL(sub, aeron0->findSubscription(subId), invoker0);
    POLL_FOR(pub0->isConnected() && sub->isConnected(), invoker0);
    
    std::int64_t pub1Id = aeron1->addPublication(channel, streamId);
    invoker1.invoke();
    POLL_FOR_NON_NULL(pub1, aeron1->findPublication(pub1Id), invoker1);

    std::string message = "Hello World!";

    auto *data = reinterpret_cast<const uint8_t *>(message.c_str());
    POLL_FOR(0 < pub0->offer(data, message.length()), invoker0);
    POLL_FOR(0 < sub->poll(
        [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            EXPECT_EQ(message, buffer.getStringWithoutLength(offset, length));
        },
        1), invoker0);

    POLL_FOR(1 == sub->imageCount(), invoker0);

    const std::shared_ptr<Image> image = sub->imageByIndex(0);
    image->reject("No Longer Valid");

    POLL_FOR(0 < errorFrameCount0, invoker0);
    POLL_FOR(0 < errorFrameCount1, invoker1);

    int64_t timeout_ms = aeron_epoch_clock() + 500;
    while (aeron_epoch_clock() < timeout_ms)
    {
        invoker2.invoke();
        ASSERT_EQ(0, errorFrameCount2);
        std::this_thread::sleep_for(std::chrono::duration<long, std::milli>(1));
    }
}

TEST_F(RejectImageTestSimple, basic_ipc_test)
{
    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::atomic<std::int32_t> errorFrameCount{0};

    on_publication_error_frame_t errorFrameHandler =
        [&](aeron::status::PublicationErrorFrame &errorFrame)
        {
            std::atomic_fetch_add(&errorFrameCount, 1);
            return;
        };

    ctx.errorFrameHandler(errorFrameHandler);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    invoker.start();

    std::int64_t pubId = aeron->addPublication("aeron:ipc", 10000);
    std::int64_t subId = aeron->addSubscription("aeron:ipc", 10000);
    invoker.invoke();

    POLL_FOR_NON_NULL(pub, aeron->findPublication(pubId), invoker);
    POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
    POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);

    const std::shared_ptr<Image> image = sub->imageByIndex(0);
    image->reject("No Longer Valid");

    invoker.invoke();
}
