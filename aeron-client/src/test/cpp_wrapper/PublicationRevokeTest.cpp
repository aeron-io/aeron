
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
#include "AeronCounters.h"
#include "concurrent/CountersReader.h"
#include "TestUtil.h"

#include "aeron_system_counters.h"

using namespace aeron;
using testing::MockFunction;
using testing::_;

#define POLL_FOR_ONE_MESSAGE \
do \
{ \
    POLL_FOR(1 == sub->poll( \
        [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header) \
        { \
            EXPECT_EQ(message, buffer.getString(offset)); \
            EXPECT_EQ(streamId, header.streamId()); \
            EXPECT_EQ(length, header.frameLength() - dataHeaderLength); \
        }, \
        1), invoker); \
} \
while (0)

class PublicationRevokeTest : public testing::TestWithParam<std::tuple<const char *, const char *, const std::uint64_t>>
{
public:
    PublicationRevokeTest()
    {
        m_driver.start();
    }

    ~PublicationRevokeTest() override
    {
        m_driver.stop();
    }

protected:
    EmbeddedMediaDriver m_driver;
};

static const int streamId = 1001;

typedef std::array<std::uint8_t, 1024> buffer_t;
static const int dataHeaderLength = 32;

INSTANTIATE_TEST_SUITE_P(
    PublicationRevokeTest,
    PublicationRevokeTest,
    testing::Values(
        std::make_tuple("aeron:udp?endpoint=localhost:24325", "aeron:udp?endpoint=localhost:24325", 1),
        std::make_tuple("aeron:ipc", "aeron:ipc", 0),
        std::make_tuple("aeron-spy:aeron:udp?endpoint=localhost:24325", "aeron:udp?endpoint=localhost:24325|ssc=true", 0)
    ));

TEST_P(PublicationRevokeTest, revokeTestSimple)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    std::int32_t imageUnavailable = 0;

    Context ctx;
    ctx.useConductorAgentInvoker(true);

    MockFunction<void(Image &image)> mockOnUnavailableImage;
    EXPECT_CALL(mockOnUnavailableImage, Call(_)).WillOnce(
        [&](Image &image)
        {
            ASSERT_TRUE(image.isPublicationRevoked());
            aeron::concurrent::atomic::getAndAddInt32(&imageUnavailable, 1);
        });;
    ctx.unavailableImageHandler(mockOnUnavailableImage.AsStdFunction());

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> invoker = aeron->conductorAgentInvoker();

    const char *subscriptionChannel = std::get<0>(GetParam());
    const char *publicationChannel = std::get<1>(GetParam());
    std::uint64_t  expectedPublicationImagesRevoked = std::get<2>(GetParam());

    std::int64_t subId = aeron->addSubscription(subscriptionChannel, streamId);
    std::int64_t pubId = aeron->addExclusivePublication(publicationChannel, streamId);

    POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
    POLL_FOR_NON_NULL(pub, aeron->findExclusivePublication(pubId), invoker);
    POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);

    std::string message = "hello world!";
    std::int32_t length = buffer.putString(0, message);

    POLL_FOR(0 < pub->offer(buffer, 0, length), invoker);

    POLL_FOR_ONE_MESSAGE;

    POLL_FOR(0 < pub->offer(buffer, 0, length), invoker);

    pub->revokeOnClose();
    pub->close();

    ASSERT_EQ(AERON_PUBLICATION_CLOSED, pub->offer(buffer, 0, length));

    POLL_FOR(1 == imageUnavailable, invoker);

    ASSERT_EQ(0, sub->imageCount());

    ASSERT_EQ(1, aeron->countersReader().getCounterValue(AERON_SYSTEM_COUNTER_PUBLICATIONS_REVOKED));
    ASSERT_EQ(expectedPublicationImagesRevoked, aeron->countersReader().getCounterValue(AERON_SYSTEM_COUNTER_PUBLICATION_IMAGES_REVOKED));
}

TEST_P(PublicationRevokeTest, revokeTestExclusive)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    std::int32_t imageUnavailable = 0;

    Context ctx;
    ctx.useConductorAgentInvoker(true);

    MockFunction<void(Image &image)> mockOnAvailableImage;
    EXPECT_CALL(mockOnAvailableImage, Call(_)).WillRepeatedly(
        [&](Image &image)
        {
        });;

    bool publicationShouldBeRevoked = true;
    MockFunction<void(Image &image)> mockOnUnavailableImage;
    EXPECT_CALL(mockOnUnavailableImage, Call(_)).WillRepeatedly(
        [&](Image &image)
        {
            ASSERT_EQ(publicationShouldBeRevoked, image.isPublicationRevoked());
            aeron::concurrent::atomic::getAndAddInt32(&imageUnavailable, 1);
        });;

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> invoker = aeron->conductorAgentInvoker();

    const char *subscriptionChannel = std::get<0>(GetParam());
    const char *publicationChannel = std::get<1>(GetParam());
    std::uint64_t  expectedPublicationImagesRevoked = std::get<2>(GetParam());

    std::int64_t subId = aeron->addSubscription(
        subscriptionChannel, streamId, mockOnAvailableImage.AsStdFunction(), mockOnUnavailableImage.AsStdFunction());
    std::int64_t pubId = aeron->addExclusivePublication(publicationChannel, streamId);
    std::int64_t pub2Id = aeron->addExclusivePublication(publicationChannel, streamId);

    POLL_FOR_NON_NULL(pub, aeron->findExclusivePublication(pubId), invoker);
    POLL_FOR_NON_NULL(pub2, aeron->findExclusivePublication(pub2Id), invoker);
    {
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        POLL_FOR(sub->isConnected() && pub->isConnected() && pub2->isConnected(), invoker);

        std::string message = "hello world!";
        std::int32_t length = buffer.putString(0, message);

        POLL_FOR(0 < pub->offer(buffer, 0, length), invoker);
        POLL_FOR(0 < pub2->offer(buffer, 0, length), invoker);

        POLL_FOR_ONE_MESSAGE;
        POLL_FOR_ONE_MESSAGE;

        POLL_FOR(0 < pub->offer(buffer, 0, length), invoker);

        ASSERT_EQ(2, sub->imageCount());

        pub->revoke();

        ASSERT_EQ(AERON_PUBLICATION_CLOSED, pub->offer(buffer, 0, length));

        POLL_FOR(1 == imageUnavailable, invoker);

        ASSERT_EQ(1, sub->imageCount());

        POLL_FOR(0 < pub2->offer(buffer, 0, length), invoker);

        POLL_FOR_ONE_MESSAGE;

        publicationShouldBeRevoked = false;
    }

    POLL_FOR(2 == imageUnavailable, invoker);

    pub2->close();

    ASSERT_EQ(1, aeron->countersReader().getCounterValue(AERON_SYSTEM_COUNTER_PUBLICATIONS_REVOKED));
    ASSERT_EQ(expectedPublicationImagesRevoked, aeron->countersReader().getCounterValue(AERON_SYSTEM_COUNTER_PUBLICATION_IMAGES_REVOKED));
}
