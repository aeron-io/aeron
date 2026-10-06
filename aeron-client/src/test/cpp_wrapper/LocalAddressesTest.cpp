
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
#include <gmock/gmock.h>

#include "EmbeddedMediaDriver.h"
#include "Aeron.h"
#include "ChannelUriStringBuilder.h"
#include "TestUtil.h"

using namespace aeron;
using namespace aeron::concurrent::status;
using testing::MockFunction;
using testing::_;

class LocalAddressesTest : public testing::Test
{
public:
    LocalAddressesTest()
    {
        m_driver.start();
    }

    ~LocalAddressesTest() override
    {
        m_driver.stop();
    }

    static std::string join(std::vector<std::string> &strings)
    {
        std::stringstream ss;
        ss << "{";
        std::for_each(
            strings.begin(),
            strings.end(),
            [&](const std::string &s)
            {
                ss << s;
                ss << ", ";
            });
        ss.seekp(-2, std::ios_base::end);
        ss << "}";
        return ss.str();
    }

protected:
    EmbeddedMediaDriver m_driver;
};

TEST_F(LocalAddressesTest, shouldGetLocalAddresses)
{
    std::int32_t streamId = 10001;
    std::string channel = "aeron:udp?endpoint=127.0.0.1:23456|control=127.0.0.1:23457";

    Context ctx;
    ctx.useConductorAgentInvoker(true);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);

    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    std::int64_t subId = aeron->addSubscription(channel, streamId);
    std::int64_t pubId = aeron->addPublication(channel, streamId);

    {
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        auto subAddresses = sub->localSocketAddresses();
        ASSERT_EQ(1U, subAddresses.size()) << join(subAddresses);
        EXPECT_NE(std::string::npos, channel.find(subAddresses[0]));
        ASSERT_EQ(channel, sub->tryResolveChannelEndpointPort());

        POLL_FOR_NON_NULL(pub, aeron->findPublication(pubId), invoker);
        auto pubAddresses = pub->localSocketAddresses();
        ASSERT_EQ(1U, pubAddresses.size());
        EXPECT_NE(std::string::npos, channel.find(pubAddresses[0]));
    }

    invoker.invoke();
}

TEST_F(LocalAddressesTest, shouldGetLocalAddressesForIpc)
{
    std::int32_t streamId = 10001;
    std::string channel = "aeron:ipc";

    Context ctx;
    ctx.useConductorAgentInvoker(true);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);

    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    std::int64_t subId = aeron->addSubscription(channel, streamId);
    std::int64_t pubId = aeron->addPublication(channel, streamId);
    std::int64_t exclusivePubId = aeron->addExclusivePublication(channel, streamId);

    {
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        auto subAddresses = sub->localSocketAddresses();
        ASSERT_EQ(0U, subAddresses.size()) << join(subAddresses);
        ASSERT_EQ(channel, sub->tryResolveChannelEndpointPort());

        POLL_FOR_NON_NULL(pub, aeron->findPublication(pubId), invoker);
        auto pubAddresses = pub->localSocketAddresses();
        ASSERT_EQ(0U, pubAddresses.size()) << join(pubAddresses);

        POLL_FOR_NON_NULL(exclusivePub, aeron->findExclusivePublication(exclusivePubId), invoker);
        auto exclusivePubAddresses = exclusivePub->localSocketAddresses();
        ASSERT_EQ(0U, exclusivePubAddresses.size()) << join(exclusivePubAddresses);
    }

    invoker.invoke();
}

TEST_F(LocalAddressesTest, shouldGetLocalAddressesForMds)
{
    std::int32_t streamId = 10001;
    std::string channel = "aeron:udp?control-mode=manual";

    Context ctx;
    ctx.useConductorAgentInvoker(true);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);

    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    std::int64_t subId = aeron->addSubscription(channel, streamId);
    {
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);

        int numDestinations = 32;
        for (int i = 0; i < numDestinations; i++)
        {
            int64_t destination = sub->addDestination("aeron:udp?endpoint=127.0.0.1:" + std::to_string(9000 + i));
            POLL_FOR(sub->findDestinationResponse(destination), invoker);
        }
        
        auto subAddresses = sub->localSocketAddresses();
        ASSERT_EQ(numDestinations, (int)subAddresses.size());

        for (int i = 0; i < numDestinations; i++)
        {
            std::string expectedAddress = "127.0.0.1:" + std::to_string(9000 + i);
            ASSERT_EQ(expectedAddress, subAddresses[i]);
        }
    }

    invoker.invoke();
}

TEST_F(LocalAddressesTest, shouldGetChannelStatusForPublications)
{
    std::int32_t streamId = 10001;
    std::string channel = "aeron:udp?endpoint=127.0.0.1:23456";

    Context ctx;
    ctx.useConductorAgentInvoker(true);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);

    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    std::int64_t pubId = aeron->addPublication(channel, streamId);
    std::int64_t exclusivePubId = aeron->addExclusivePublication(channel, streamId);

    {
        POLL_FOR_NON_NULL(pub, aeron->findPublication(pubId), invoker);
        POLL_FOR_NON_NULL(exclusivePub, aeron->findExclusivePublication(exclusivePubId), invoker);

        EXPECT_EQ(ChannelEndpointStatus::CHANNEL_ENDPOINT_ACTIVE, pub->channelStatus());
        EXPECT_EQ(ChannelEndpointStatus::CHANNEL_ENDPOINT_ACTIVE, exclusivePub->channelStatus());
    }

    invoker.invoke();
}
