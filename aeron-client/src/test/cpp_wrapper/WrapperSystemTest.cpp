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

using namespace aeron;

class WrapperSystemTest : public testing::Test
{
public:
    WrapperSystemTest()
    {
        m_driver.start();
    }

    ~WrapperSystemTest() override
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

TEST_F(WrapperSystemTest, shouldSendReceiveDataWithRawPointer)
{
    Context ctx;
    ctx.useConductorAgentInvoker(true);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    invoker.start();

    std::int64_t pubId = aeron->addPublication("aeron:ipc", 10000);
    std::int64_t subId = aeron->addSubscription("aeron:ipc", 10000);
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
}

TEST_F(WrapperSystemTest, shouldSendReceiveDataWithRawPointerExclusive)
{
    Context ctx;
    ctx.useConductorAgentInvoker(true);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    invoker.start();

    std::int64_t pubId = aeron->addExclusivePublication("aeron:ipc", 10000);
    std::int64_t subId = aeron->addSubscription("aeron:ipc", 10000);
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
}

TEST_F(WrapperSystemTest, shouldRejectClientNameThatIsTooLong)
{
    std::string name =
        "this is a very long value that we are hoping with be reject when the value gets "
        "set on the the context without causing issues will labels";

    try
    {
        Context ctx;
        ctx.useConductorAgentInvoker(true);
        ctx.clientName(name);

        std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
        FAIL();
    }
    catch (IllegalArgumentException &ex)
    {
        const char *string = strstr(ex.what(), "client_name length must <= 100");
        ASSERT_NE(nullptr, string) << ex.what();
    }
}

TEST_F(WrapperSystemTest, shouldRemovePendingAsyncPublicationUponError)
{
    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    invoker.start();

    auto channel = "aeron:udp?endpoint=localhost:99999";
    int stream_id = 1000;
    int64_t registration_id = aeron->addPublication(channel, stream_id);

    try
    {
        POLL_FOR_NON_NULL(publication2, aeron->findPublication(registration_id), invoker);
        FAIL();
    }
    catch( const AeronException& e )
    {
        auto errorMsg = std::string(e.what());
        EXPECT_NE(std::string::npos, errorMsg.find("port out of range: '99999'", 0)) << errorMsg;
    }

    try
    {
        POLL_FOR_NON_NULL(publication2, aeron->findPublication(registration_id), invoker);
        FAIL();
    }
    catch( const IllegalArgumentException& e )
    {
        auto errorMsg = std::string(e.what());
        EXPECT_NE(std::string::npos, errorMsg.find(std::string("Unknown registration id: ").append(std::to_string(registration_id)), 0));
    }
}

TEST_F(WrapperSystemTest, shouldRemovePendingAsyncPublicationUponSuccess)
{
    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    invoker.start();
    auto channel = "aeron:udp?endpoint=localhost:5555";
    int stream_id = 1000;
    int64_t registration_id = aeron->addPublication(channel, stream_id);
    POLL_FOR_NON_NULL(publication, aeron->findPublication(registration_id), invoker);

    try
    {
        POLL_FOR_NON_NULL(publication2, aeron->findPublication(registration_id), invoker);
        FAIL();
    }
    catch( const IllegalArgumentException& e )
    {
        auto errorMsg = std::string(e.what());
        EXPECT_NE(std::string::npos, errorMsg.find(std::string("Unknown registration id: ").append(std::to_string(registration_id)), 0));
    }
}

TEST_F(WrapperSystemTest, asyncPublicationIsAutomaticallyFreedWhenAeronIsClosed)
{
    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    invoker.start();

    auto channel = "aeron:udp?endpoint=localhost:5555";
    int stream_id = 1000;
    aeron->addPublicationAsync(channel, stream_id);
}

TEST_F(WrapperSystemTest, asyncPublicationIsAutomaticallyFreedWhenAeronIsClosedConductor)
{
    Context ctx;
    ctx.useConductorAgentInvoker(false);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);

    auto channel = "aeron:udp?endpoint=localhost:5555";
    int stream_id = 1000;
    auto async = aeron->addPublicationAsync(channel, stream_id);
    EXPECT_NE(nullptr, async);

    // wait for addPublicationAsync to complete
    int64_t counterId = aeron->addCounter(1000, nullptr, 0, "test");
    WAIT_FOR_NON_NULL(counter, aeron->findCounter(counterId));
}

TEST_F(WrapperSystemTest, shouldRemovePendingAsyncExclusivePublicationUponError)
{
    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    invoker.start();

    auto channel = "aeron:ipc?session-id=42";
    int stream_id = 1000;
    int64_t registration_id = aeron->addExclusivePublication(channel, stream_id);
    POLL_FOR_NON_NULL(publication, aeron->findExclusivePublication(registration_id), invoker);

    int64_t registration_id2 = aeron->addExclusivePublication(channel, stream_id);
    try
    {
        POLL_FOR_NON_NULL(publication2, aeron->findExclusivePublication(registration_id2), invoker);
        FAIL();
    }
    catch( const AeronException& e )
    {
        auto errorMsg = std::string(e.what());
        EXPECT_NE(std::string::npos, errorMsg.find("session-id is already in exclusive use for channel", 0));
    }

    try
    {
        POLL_FOR_NON_NULL(publication2, aeron->findExclusivePublication(registration_id2), invoker);
        FAIL();
    }
    catch( const IllegalArgumentException& e )
    {
        auto errorMsg = std::string(e.what());
        EXPECT_NE(std::string::npos, errorMsg.find(std::string("Unknown registration id: ").append(std::to_string(registration_id2)), 0));
    }
}

TEST_F(WrapperSystemTest, shouldRemovePendingAsyncSubscriptionUponError)
{
    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    invoker.start();

    auto channel = "aeron:udp?endpoint=localhost:99999";
    int stream_id = 1000;
    int64_t registration_id = aeron->addSubscription(channel, stream_id);

    try
    {
        POLL_FOR_NON_NULL(subscription, aeron->findSubscription(registration_id), invoker);
        FAIL();
    }
    catch( const AeronException& e )
    {
        auto errorMsg = std::string(e.what());
        EXPECT_NE(std::string::npos, errorMsg.find("port out of range: '99999'", 0)) << errorMsg;
    }

    try
    {
        POLL_FOR_NON_NULL(subscription, aeron->findSubscription(registration_id), invoker);
        FAIL();
    }
    catch( const IllegalArgumentException& e )
    {
        auto errorMsg = std::string(e.what());
        EXPECT_NE(std::string::npos, errorMsg.find(std::string("Unknown registration id: ").append(std::to_string(registration_id)), 0));
    }
}

TEST_F(WrapperSystemTest, shouldRemovePendingAsyncCounterUponError)
{
    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    invoker.start();

    int32_t typeId = 1000;
    int64_t registration_id = aeron->addCounter(typeId, nullptr, 0, "test");
    POLL_FOR_NON_NULL(counter, aeron->findCounter(registration_id), invoker);

    int64_t registration_id2 = aeron->addStaticCounter(typeId, nullptr, 0, "my static counter", registration_id);
    try
    {
        POLL_FOR_NON_NULL(staticCounter, aeron->findCounter(registration_id2), invoker);
        FAIL();
    }
    catch( const AeronException& e )
    {
        auto errorMsg = std::string(e.what());
        EXPECT_NE(std::string::npos, errorMsg.find("cannot add static counter, because a non-static counter exists", 0));
    }

    try
    {
        POLL_FOR_NON_NULL(staticCounter, aeron->findCounter(registration_id2), invoker);
        FAIL();
    }
    catch( const IllegalArgumentException& e )
    {
        auto errorMsg = std::string(e.what());
        EXPECT_NE(std::string::npos, errorMsg.find(std::string("Unknown registration id: ").append(std::to_string(registration_id2)), 0));
    }
}

TEST_F(WrapperSystemTest, asyncSubscriptionMustBeManuallyFreedAfterUsage)
{
    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    invoker.start();

    auto channel = "aeron:udp?endpoint=localhost:99999";
    int stream_id = 1000;
    auto async = aeron->addSubscriptionAsync(channel, stream_id);

    try
    {
        POLL_FOR_NON_NULL(subscription, aeron->findSubscription(async), invoker);
        FAIL();
    }
    catch( const AeronException& e )
    {
        auto errorMsg = std::string(e.what());
        EXPECT_NE(std::string::npos, errorMsg.find("port out of range: '99999'", 0)) << errorMsg;
    }

    delete async;
}

TEST_F(WrapperSystemTest, asyncSubscriptionMustBeManuallyFreedAfterUsageConductor)
{
    Context ctx;
    ctx.useConductorAgentInvoker(false);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);

    auto channel = "aeron:udp?endpoint=localhost:99999";
    int stream_id = 1000;
    auto async = aeron->addSubscriptionAsync(channel, stream_id);

    try
    {
        WAIT_FOR_NON_NULL(subscription, aeron->findSubscription(async));
        FAIL();
    }
    catch( const AeronException& e )
    {
        auto errorMsg = std::string(e.what());
        EXPECT_NE(std::string::npos, errorMsg.find("port out of range: '99999'", 0)) << errorMsg;
    }

    delete async;
}

TEST_F(WrapperSystemTest, nonPolledAsyncSubscriptionMustBeManuallyFreedAfterUsage)
{
    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    invoker.start();

    auto channel = "aeron:udp?endpoint=localhost:99999";
    int stream_id = 1000;
    auto async = aeron->addSubscriptionAsync(channel, stream_id);
    delete async;
}

TEST_F(WrapperSystemTest, polledSubscriptionShouldCloseAllAllocatedResourcesInvoker)
{
    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    invoker.start();

    auto channel = "aeron:udp?endpoint=localhost:10000";
    int stream_id = 1000;
    int64_t registration_id = aeron->addSubscription(channel, stream_id);

    POLL_FOR_NON_NULL(subscription, aeron->findSubscription(registration_id), invoker);
}

TEST_F(WrapperSystemTest, polledSubscriptionShouldCloseAllAllocatedResourcesConductor)
{
    Context ctx;
    ctx.useConductorAgentInvoker(false);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);

    auto channel = "aeron:udp?endpoint=localhost:10000";
    int stream_id = 1000;

    int64_t registration_id = aeron->addSubscription(channel, stream_id);
    WAIT_FOR_NON_NULL(subscription, aeron->findSubscription(registration_id));

    int64_t registration_id2 = aeron->addSubscription(channel, stream_id);
    WAIT_FOR_NON_NULL(subscription2, aeron->findSubscription(registration_id2));
}

TEST_F(WrapperSystemTest, polledSubscriptionShouldCloseAllAllocatedResourcesWhenConductorQueueIsFull)
{
    Context ctx;
    ctx.useConductorAgentInvoker(false).idleSleepDuration(200);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);

    auto channel = "aeron:udp?endpoint=localhost:10000";
    int stream_id = 1000;
    int64_t registration_id = aeron->addSubscription(channel, stream_id);

    WAIT_FOR_NON_NULL(subscription, aeron->findSubscription(registration_id));

    // overflow client command queue
    while (true)
    {
        try
        {
            aeron->addCounter(1000, nullptr, 0, "test");
        }
        catch (...)
        {
            break;
        }
    }

    subscription.reset();
    aeron.reset();
}

TEST_F(WrapperSystemTest, shouldDeleteAeronInstanceLastEvenIfManuallyReleased)
{
    Context ctx;
    ctx.useConductorAgentInvoker(false);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);

    auto channel = "aeron:udp?endpoint=localhost:10000";
    int stream_id = 1000;

    int64_t sub_registration_id = aeron->addSubscription(channel, stream_id);
    WAIT_FOR_NON_NULL(subscription, aeron->findSubscription(sub_registration_id));

    int64_t pub_registration_id = aeron->addPublication(channel, stream_id);
    WAIT_FOR_NON_NULL(publication, aeron->findPublication(pub_registration_id));

    int64_t exclusive_pub_registration_id = aeron->addExclusivePublication(channel, stream_id + 1);
    WAIT_FOR_NON_NULL(
        exclusivePublication, aeron->findExclusivePublication(exclusive_pub_registration_id));

    int64_t counter_registration_id = aeron->addCounter(1000, nullptr, 0, "test");
    WAIT_FOR_NON_NULL(counter, aeron->findCounter(counter_registration_id));

    aeron.reset();

    EXPECT_EQ(sub_registration_id, subscription->registrationId());
    EXPECT_EQ(pub_registration_id, publication->registrationId());
    EXPECT_EQ(exclusive_pub_registration_id, exclusivePublication->registrationId());
    EXPECT_EQ(counter_registration_id, counter->registrationId());
}

TEST_F(WrapperSystemTest, polledSubscriptionShouldCloseAllAllocatedResourcesAfterDriverWasStopped)
{
    Context ctx;
    ctx
        .useConductorAgentInvoker(false)
        .mediaDriverTimeout(350)
        .idleSleepDuration(1)
        .errorHandler(
            [](const std::exception& ignored)
            {
                // Deliberately ignored.
            });

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);

    auto channel = "aeron:udp?endpoint=localhost:10000";
    int stream_id = 1000;
    int64_t registration_id = aeron->addSubscription(channel, stream_id);

    WAIT_FOR_NON_NULL(subscription, aeron->findSubscription(registration_id));

    m_driver.stop();
    m_driver.closeDriver();

    // wait for driver being stopped detected by the underlying `aeron_t` instance
    std::this_thread::sleep_for(std::chrono::milliseconds(ctx.mediaDriverTimeout() * 2));

    EXPECT_EQ(registration_id, subscription->registrationId());
}

TEST_F(WrapperSystemTest, nonPolledPendingAsyncDestinationsAreAutomaticallyFreedSubscription)
{
    Context ctx;
    ctx.useConductorAgentInvoker(false);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);

    auto channel = "aeron:udp?control-mode=manual";
    auto dest1Uri = "aeron:udp?endpoint=localhost:4554";
    auto dest2Uri = "aeron:udp?endpoint=localhost:7777";
    auto dest3Uri = "aeron:udp?endpoint=localhost:10000";
    int stream_id = 1000;

    int64_t registration_id = aeron->addSubscription(channel, stream_id);
    WAIT_FOR_NON_NULL(subscription, aeron->findSubscription(registration_id));

    int64_t addDest1RegistrationId = subscription->addDestination(dest1Uri);
    subscription->addDestinationAsync(dest3Uri);
    int64_t addDest2RegistrationId = subscription->addDestination(dest2Uri);

    subscription->removeDestinationAsync(dest1Uri);
    int64_t removeDest2RegistrationId = subscription->removeDestination(dest2Uri);
    int64_t removeDest3RegistrationId = subscription->removeDestination(dest3Uri);

    EXPECT_GT(removeDest2RegistrationId, addDest1RegistrationId);
    EXPECT_GT(removeDest3RegistrationId, removeDest2RegistrationId);

    WAIT_FOR(subscription->findDestinationResponse(addDest2RegistrationId));
    WAIT_FOR(subscription->findDestinationResponse(removeDest3RegistrationId));
}

TEST_F(WrapperSystemTest, nonPolledPendingAsyncDestinationsAreAutomaticallyFreedPublication)
{
    Context ctx;
    ctx.useConductorAgentInvoker(false);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);

    auto channel = "aeron:udp?control-mode=manual";
    auto dest1Uri = "aeron:udp?endpoint=localhost:4554";
    auto dest2Uri = "aeron:udp?endpoint=localhost:7777";
    auto dest3Uri = "aeron:udp?endpoint=localhost:10000";
    int stream_id = 1000;

    int64_t registration_id = aeron->addPublication(channel, stream_id);
    WAIT_FOR_NON_NULL(publication, aeron->findPublication(registration_id));

    int64_t addDest1RegistrationId = publication->addDestination(dest1Uri);
    publication->addDestinationAsync(dest3Uri);
    int64_t addDest2RegistrationId = publication->addDestination(dest2Uri);

    publication->removeDestinationAsync(dest1Uri);
    int64_t removeDest2RegistrationId = publication->removeDestination(dest2Uri);
    int64_t removeDest3RegistrationId = publication->removeDestination(dest3Uri);

    EXPECT_GT(removeDest2RegistrationId, addDest1RegistrationId);
    EXPECT_GT(removeDest3RegistrationId, removeDest2RegistrationId);

    WAIT_FOR(publication->findDestinationResponse(addDest2RegistrationId));
    WAIT_FOR(publication->findDestinationResponse(removeDest3RegistrationId));
}

TEST_F(WrapperSystemTest, nonPolledPendingAsyncDestinationsAreAutomaticallyFreedExclusivePublication)
{
    Context ctx;
    ctx.useConductorAgentInvoker(false);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);

    auto channel = "aeron:udp?control-mode=manual";
    auto dest1Uri = "aeron:udp?endpoint=localhost:4554";
    auto dest2Uri = "aeron:udp?endpoint=localhost:7777";
    auto dest3Uri = "aeron:udp?endpoint=localhost:10000";
    int stream_id = 1000;

    int64_t registration_id = aeron->addExclusivePublication(channel, stream_id);
    WAIT_FOR_NON_NULL(publication, aeron->findExclusivePublication(registration_id));

    int64_t addDest1RegistrationId = publication->addDestination(dest1Uri);
    publication->addDestinationAsync(dest3Uri);
    int64_t addDest2RegistrationId = publication->addDestination(dest2Uri);

    publication->removeDestinationAsync(dest1Uri);
    int64_t removeDest2RegistrationId = publication->removeDestination(dest2Uri);
    int64_t removeDest3RegistrationId = publication->removeDestination(dest3Uri);

    EXPECT_GT(removeDest2RegistrationId, addDest1RegistrationId);
    EXPECT_GT(removeDest3RegistrationId, removeDest2RegistrationId);

    WAIT_FOR(publication->findDestinationResponse(addDest2RegistrationId));
    WAIT_FOR(publication->findDestinationResponse(removeDest3RegistrationId));
}
