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

extern "C"
{
#include "aeron_image.h"
}

using namespace aeron;
using testing::MockFunction;
using testing::Return;
using testing::_;

class ImageTest : public testing::TestWithParam<std::tuple<const char*, const char*>>
{
public:
    ImageTest()
    {
        m_driver.start();
    }

    ~ImageTest() override
    {
        m_driver.stop();
    }

protected:
    EmbeddedMediaDriver m_driver;
};

typedef std::array<std::uint8_t, 1024> buffer_t;

INSTANTIATE_TEST_SUITE_P(
    ImageTest,
    ImageTest,
    testing::Values(
        std::make_tuple("udp", "localhost:24325"),
        std::make_tuple("ipc", nullptr)
    ));

TEST_P(ImageTest, shouldGetMultipleImages)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    std::int32_t streamId = 982375;
    ChannelUriStringBuilder uriBuilder;
    const std::string channel = setParameters(std::get<0>(GetParam()), std::get<1>(GetParam()), uriBuilder)
        .build();

    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    std::int64_t subId = aeron->addSubscription(channel, streamId);
    std::int64_t pubId1 = aeron->addExclusivePublication(channel, streamId);
    std::int64_t pubId2 = aeron->addExclusivePublication(channel, streamId);
    AgentInvoker<ClientConductor>& invoker = aeron->conductorAgentInvoker();

    MockFunction<void(concurrent::AtomicBuffer&, util::index_t, util::index_t, Header&)> mockHandler;

    EXPECT_CALL(mockHandler, Call(_, _, _, _)).Times(2);

    {
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        POLL_FOR_NON_NULL(pub1, aeron->findExclusivePublication(pubId1), invoker);
        POLL_FOR_NON_NULL(pub2, aeron->findExclusivePublication(pubId2), invoker);
        POLL_FOR(pub1->isConnected() && pub2->isConnected() && sub->isConnected(), invoker);

        std::string message = "hello world!";
        buffer.putString(0, message);
        POLL_FOR(0 < pub1->offer(buffer), invoker);
        POLL_FOR(0 < pub2->offer(buffer), invoker);

        POLL_FOR(2 == sub->imageCount(), invoker);

        EXPECT_NE(nullptr, sub->imageBySessionId(pub1->sessionId()));
        EXPECT_NE(nullptr, sub->imageBySessionId(pub2->sessionId()));
        EXPECT_NE(nullptr, sub->imageByIndex(0));
        EXPECT_NE(nullptr, sub->imageByIndex(1));
        const std::shared_ptr<std::vector<std::shared_ptr<Image>>> images = sub->copyOfImageList();
        EXPECT_EQ(2U, images->size());

        POLL_FOR(0 < (*images)[0]->poll(mockHandler.AsStdFunction(), 1), invoker);
        POLL_FOR(0 < (*images)[1]->poll(mockHandler.AsStdFunction(), 1), invoker);
    }

    invoker.invoke();
}

TEST_P(ImageTest, shouldBoundedPoll)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    std::int32_t streamId = 982375;
    ChannelUriStringBuilder uriBuilder;
    const std::string channel = setParameters(std::get<0>(GetParam()), std::get<1>(GetParam()), uriBuilder)
        .build();

    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    std::int64_t subId = aeron->addSubscription(channel, streamId);
    std::int64_t pubId1 = aeron->addExclusivePublication(channel, streamId);
    std::int64_t pubId2 = aeron->addExclusivePublication(channel, streamId);
    AgentInvoker<ClientConductor>& invoker = aeron->conductorAgentInvoker();

    MockFunction<void(concurrent::AtomicBuffer&, util::index_t, util::index_t, Header&)> mockHandler;

    EXPECT_CALL(mockHandler, Call(_, _, _, _)).Times(2);

    {
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        POLL_FOR_NON_NULL(pub1, aeron->findExclusivePublication(pubId1), invoker);
        POLL_FOR_NON_NULL(pub2, aeron->findExclusivePublication(pubId2), invoker);
        POLL_FOR(pub1->isConnected() && pub2->isConnected() && sub->isConnected(), invoker);

        std::string message = "hello world!";
        buffer.putString(0, message);
        POLL_FOR(0 < pub1->offer(buffer), invoker);
        POLL_FOR(0 < pub2->offer(buffer), invoker);
        POLL_FOR(2 == sub->imageCount(), invoker);

        const std::shared_ptr<std::vector<std::shared_ptr<Image>>> images = sub->copyOfImageList();
        EXPECT_EQ(2U, images->size());

        POLL_FOR(0 < (*images)[0]->boundedPoll(mockHandler.AsStdFunction(), 10000, 1), invoker);
        POLL_FOR(0 < (*images)[1]->boundedPoll(mockHandler.AsStdFunction(), 10000, 1), invoker);
    }

    invoker.invoke();
}

TEST_P(ImageTest, shouldControlledPoll)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    std::int32_t streamId = 982375;
    ChannelUriStringBuilder uriBuilder;
    const std::string channel = setParameters(std::get<0>(GetParam()), std::get<1>(GetParam()), uriBuilder)
        .build();

    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    std::int64_t subId = aeron->addSubscription(channel, streamId);
    std::int64_t pubId1 = aeron->addExclusivePublication(channel, streamId);
    std::int64_t pubId2 = aeron->addExclusivePublication(channel, streamId);
    AgentInvoker<ClientConductor>& invoker = aeron->conductorAgentInvoker();

    MockFunction<ControlledPollAction(
        concurrent::AtomicBuffer &buffer,
        util::index_t offset,
        util::index_t length,
        concurrent::logbuffer::Header &header)> mockHandler;

    EXPECT_CALL(mockHandler, Call(_, _, _, _)).Times(2).WillRepeatedly(Return(ControlledPollAction::CONTINUE));

    {
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        POLL_FOR_NON_NULL(pub1, aeron->findExclusivePublication(pubId1), invoker);
        POLL_FOR_NON_NULL(pub2, aeron->findExclusivePublication(pubId2), invoker);
        POLL_FOR(pub1->isConnected() && pub2->isConnected() && sub->isConnected(), invoker);

        std::string message = "hello world!";
        buffer.putString(0, message);
        POLL_FOR(0 < pub1->offer(buffer), invoker);
        POLL_FOR(0 < pub2->offer(buffer), invoker);
        POLL_FOR(2 == sub->imageCount(), invoker);

        const std::shared_ptr<std::vector<std::shared_ptr<Image>>> images = sub->copyOfImageList();
        EXPECT_EQ(2U, images->size());

        POLL_FOR(0 < (*images)[0]->controlledPoll(mockHandler.AsStdFunction(), 1), invoker);
        POLL_FOR(0 < (*images)[1]->controlledPoll(mockHandler.AsStdFunction(), 1), invoker);
    }

    invoker.invoke();
}

TEST_P(ImageTest, shouldBoundedControlledPoll)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    std::int32_t streamId = 982375;
    ChannelUriStringBuilder uriBuilder;
    const std::string channel = setParameters(std::get<0>(GetParam()), std::get<1>(GetParam()), uriBuilder)
        .build();

    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    std::int64_t subId = aeron->addSubscription(channel, streamId);
    std::int64_t pubId1 = aeron->addExclusivePublication(channel, streamId);
    std::int64_t pubId2 = aeron->addExclusivePublication(channel, streamId);
    AgentInvoker<ClientConductor>& invoker = aeron->conductorAgentInvoker();

    MockFunction<ControlledPollAction(
        concurrent::AtomicBuffer &buffer,
        util::index_t offset,
        util::index_t length,
        concurrent::logbuffer::Header &header)> mockHandler;

    EXPECT_CALL(mockHandler, Call(_, _, _, _)).Times(2).WillRepeatedly(Return(ControlledPollAction::CONTINUE));

    {
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        POLL_FOR_NON_NULL(pub1, aeron->findExclusivePublication(pubId1), invoker);
        POLL_FOR_NON_NULL(pub2, aeron->findExclusivePublication(pubId2), invoker);
        POLL_FOR(pub1->isConnected() && pub2->isConnected() && sub->isConnected(), invoker);

        std::string message = "hello world!";
        buffer.putString(0, message);
        POLL_FOR(0 < pub1->offer(buffer), invoker);
        POLL_FOR(0 < pub2->offer(buffer), invoker);
        POLL_FOR(2 == sub->imageCount(), invoker);

        const std::shared_ptr<std::vector<std::shared_ptr<Image>>> images = sub->copyOfImageList();
        EXPECT_EQ(2U, images->size());

        POLL_FOR(0 < (*images)[0]->boundedControlledPoll(mockHandler.AsStdFunction(), 100000, 1), invoker);
        POLL_FOR(0 < (*images)[1]->boundedControlledPoll(mockHandler.AsStdFunction(), 100000, 1), invoker);
    }

    invoker.invoke();
}

TEST_P(ImageTest, shouldBlockPollImage)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    std::int32_t streamId = 982375;
    ChannelUriStringBuilder uriBuilder;
    const std::string channel = setParameters(std::get<0>(GetParam()), std::get<1>(GetParam()), uriBuilder)
        .build();

    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    std::int64_t subId = aeron->addSubscription(channel, streamId);
    std::int64_t pubId1 = aeron->addExclusivePublication(channel, streamId);
    std::int64_t pubId2 = aeron->addExclusivePublication(channel, streamId);
    AgentInvoker<ClientConductor>& invoker = aeron->conductorAgentInvoker();
    std::int64_t bytesConsumed = 0;

    auto blockHandler = [&](
        concurrent::AtomicBuffer& buffer,
        util::index_t offset,
        util::index_t length,
        std::int32_t sessionId,
        std::int32_t termId)
    {
        bytesConsumed += length;
    };

    {
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        POLL_FOR_NON_NULL(pub1, aeron->findExclusivePublication(pubId1), invoker);
        POLL_FOR_NON_NULL(pub2, aeron->findExclusivePublication(pubId2), invoker);
        POLL_FOR(pub1->isConnected() && pub2->isConnected() && sub->isConnected(), invoker);

        std::string message = "hello world!";
        buffer.putString(0, message);
        for (int i = 0; i < 3; i++)
        {
            POLL_FOR(0 < pub1->offer(buffer), invoker);
            POLL_FOR(0 < pub2->offer(buffer), invoker);
        }

        POLL_FOR(2 == sub->imageCount(), invoker);

        const std::shared_ptr<std::vector<std::shared_ptr<Image>>> ptr = sub->copyOfImageList();
        EXPECT_EQ(2U, ptr->size());

        const std::shared_ptr<Image> image1 = sub->imageBySessionId(pub1->sessionId());
        const std::shared_ptr<Image> image2 = sub->imageBySessionId(pub2->sessionId());
        std::int64_t read1 = 0;
        std::int64_t read2 = 0;
        POLL_FOR(pub1->position() <= (read1 += image1->blockPoll(blockHandler, 1000000)), invoker);
        POLL_FOR(pub2->position() <= (read2 += image2->blockPoll(blockHandler, 1000000)), invoker);

        EXPECT_EQ(pub1->position() + pub2->position(), bytesConsumed);
        EXPECT_EQ(pub1->position() + pub2->position(), read1 + read2);

        EXPECT_EQ(pub1->position(), image1->position());
        EXPECT_EQ(pub2->position(), image2->position());
    }

    invoker.invoke();
}

TEST_F(ImageTest, shouldHandleImageRefCounts)
{
    std::string channel = "aeron:ipc";
    std::int32_t streamId = 4210812;
    std::vector<std::unique_ptr<Image>> imageList {};

    Context ctx;
    ctx.useConductorAgentInvoker(true).idleSleepDuration(1);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor>& invoker = aeron->conductorAgentInvoker();

    std::int64_t subId = aeron->addSubscription(
        channel,
        streamId,
        [&imageList] (Image& image)
        {
            auto imageFromCopyConstructor = std::unique_ptr<Image>{new Image(image)};
            auto imageFromCopyAssignment = std::unique_ptr<Image>{new Image()};
            *imageFromCopyAssignment = image;

            imageList.push_back(std::move(imageFromCopyConstructor));
            imageList.push_back(std::move(imageFromCopyAssignment));
        },
        [&imageList] (Image& image)
        {
            Image imageCopy {image};

            auto imageFromMoveOperator = std::unique_ptr<Image>{new Image(std::move(image))};
            auto imageFromMoveAssigment = std::unique_ptr<Image>{new Image()};
            *imageFromMoveAssigment = std::move(imageCopy);

            imageList.push_back(std::move(imageFromMoveOperator));
            imageList.push_back(std::move(imageFromMoveAssigment));
        });
    POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);

    {
        std::int64_t pubId = aeron->addExclusivePublication(channel, streamId);
        POLL_FOR_NON_NULL(pub, aeron->findExclusivePublication(pubId), invoker);
        POLL_FOR(1 == sub->imageCount(), invoker);
    }

    aeron_image_t *image = aeron_subscription_image_at_index(sub->subscription(), 0);
    aeron_subscription_image_release(sub->subscription(), image);
    ASSERT_EQ(3, aeron_image_refcnt_acquire(image));

    POLL_FOR(0 == sub->imageCount(), invoker);
    ASSERT_EQ(5, aeron_image_refcnt_acquire(image));

    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    invoker.invoke();
    ASSERT_EQ(4, aeron_image_refcnt_acquire(image));

    imageList.clear();
    ASSERT_EQ(0, aeron_image_refcnt_acquire(image));
}

TEST_F(ImageTest, shouldSafelyHandleNullSubscriptionAndImage)
{
    Image image{};

    ASSERT_EQ(0, image.termBufferLength());
    ASSERT_EQ(0, image.positionBitsToShift());
    ASSERT_EQ(0, image.sessionId());
    ASSERT_EQ(0, image.correlationId());
    ASSERT_THROW(image.subscriptionRegistrationId(), util::IllegalArgumentException);
    ASSERT_EQ(0, image.joinPosition());
    ASSERT_EQ(0, image.initialTermId());
    ASSERT_EQ("", image.sourceIdentity());
    ASSERT_TRUE(image.isClosed());
    ASSERT_FALSE(image.isPublicationRevoked());
    ASSERT_THROW(image.position(), util::IllegalArgumentException);
    ASSERT_EQ(0, image.subscriberPositionId());
    ASSERT_FALSE(image.isEndOfStream());
    ASSERT_THROW(image.endOfStreamPosition(), util::IllegalArgumentException);
    ASSERT_THROW(image.reject("because"), util::IllegalArgumentException);
    ASSERT_THROW(image.activeTransportCount(), util::IllegalArgumentException);

    auto fragmentHandler = [](AtomicBuffer &, index_t, index_t, Header &) {};
    auto controlledFragmentHandler = [](AtomicBuffer &, index_t, index_t, Header &) { return ControlledPollAction::CONTINUE; };
    auto blockHandler = [](AtomicBuffer&, index_t, index_t, std::int32_t, std::int32_t) {};

    ASSERT_THROW(image.poll(fragmentHandler, 1), util::IllegalArgumentException);
    ASSERT_THROW(image.boundedPoll(fragmentHandler, 1, 1), util::IllegalArgumentException);
    ASSERT_THROW(image.controlledPoll(controlledFragmentHandler, 1), util::IllegalArgumentException);
    ASSERT_THROW(image.boundedControlledPoll(controlledFragmentHandler, 1, 1), util::IllegalArgumentException);
    ASSERT_THROW(image.blockPoll(blockHandler, 1), util::IllegalArgumentException);
}