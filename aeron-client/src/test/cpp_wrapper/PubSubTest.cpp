
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
#include <random>
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "EmbeddedMediaDriver.h"
#include "Aeron.h"
#include "ChannelUriStringBuilder.h"
#include "TestUtil.h"
#include "FragmentAssembler.h"

using namespace aeron;
using testing::MockFunction;
using testing::_;

class PubSubTest : public testing::TestWithParam<std::tuple<const char *, const char *>>
{
public:
    PubSubTest()
    {
        m_driver.start();
    }

    ~PubSubTest() override
    {
        m_driver.stop();
    }

protected:
    EmbeddedMediaDriver m_driver;
};

typedef std::array<std::uint8_t, 1024> buffer_t;
static const int dataHeaderLength = 32;

INSTANTIATE_TEST_SUITE_P(
    PubSubTest,
    PubSubTest,
    testing::Values(
        std::make_tuple("udp", "localhost:24325"),
        std::make_tuple("udp", "224.20.30.39:24326"),
        std::make_tuple("ipc", nullptr)
    ));

TEST_P(PubSubTest, shouldSubscribePublishAndReceiveContextCallbacks)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    std::int32_t streamId = 982375;
    std::int32_t sessionId = 908712342;
    std::int32_t imageUnavailable = 0;

    ChannelUriStringBuilder uriBuilder;
    const std::string channel = setParameters(std::get<0>(GetParam()), std::get<1>(GetParam()), uriBuilder)
        .sessionId(sessionId)
        .networkInterface("localhost")
        .build();

    std::int64_t reservedValue = INT64_C(78923648723465);
    Context ctx;
    ctx.useConductorAgentInvoker(true);

    MockFunction<void(
        const std::string &channel,
        std::int32_t streamId,
        std::int32_t sessionId,
        std::int64_t correlationId)> mockOnNewPublication;

    MockFunction<void(
        const std::string &channel,
        std::int32_t streamId,
        std::int64_t correlationId)> mockOnNewSubscription;

    MockFunction<void(Image &image)> mockOnAvailableImage;
    MockFunction<void(Image &image)> mockOnUnavailableImage;
    MockFunction<void()> mockClientClose;

    EXPECT_CALL(mockOnNewPublication, Call(channel, streamId, sessionId, _));
    EXPECT_CALL(mockOnNewSubscription, Call(channel, streamId, _));
    EXPECT_CALL(mockOnAvailableImage, Call(_));
    EXPECT_CALL(mockOnUnavailableImage, Call(_)).WillOnce(
        [&](Image &image)
        {
            aeron::concurrent::atomic::putInt32Volatile(&imageUnavailable, 1);
        });;
    EXPECT_CALL(mockClientClose, Call());

    ctx.newPublicationHandler(mockOnNewPublication.AsStdFunction());
    ctx.newSubscriptionHandler(mockOnNewSubscription.AsStdFunction());
    ctx.availableImageHandler(mockOnAvailableImage.AsStdFunction());
    ctx.unavailableImageHandler(mockOnUnavailableImage.AsStdFunction());
    ctx.closeClientHandler(mockClientClose.AsStdFunction());

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    std::int64_t subId = aeron->addSubscription(channel, streamId);
    std::int64_t pubId = aeron->addPublication(channel, streamId);

    {
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        {
            POLL_FOR_NON_NULL(pub, aeron->findPublication(pubId), invoker);
            POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);

            on_reserved_value_supplier_t reservedValueSupplier =
                [=](AtomicBuffer &termBuffer, util::index_t termOffset, util::index_t length)
                {
                    return reservedValue;
                };

            std::string message = "hello world!";
            std::int32_t length = buffer.putString(0, message);
            const std::int64_t expectedPosition = util::BitUtil::align(
                dataHeaderLength + length, FrameDescriptor::FRAME_ALIGNMENT);

            POLL_FOR(0 < pub->offer(buffer, 0, length, reservedValueSupplier), invoker);
            POLL_FOR(0 < sub->poll(
                [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
                {
                    EXPECT_EQ(message, buffer.getString(offset));
                    EXPECT_EQ(reservedValue, header.reservedValue());
                    EXPECT_EQ(sessionId, header.sessionId());
                    EXPECT_EQ(streamId, header.streamId());
                    EXPECT_EQ(length, header.frameLength() - dataHeaderLength);
                    EXPECT_EQ(expectedPosition, header.position());
                },
                1), invoker);
        }

        POLL_FOR(1 == aeron::concurrent::atomic::getInt32Volatile(&imageUnavailable), invoker);
    }

    invoker.invoke();
}

TEST_P(PubSubTest, shouldSubscribePublishAndReceiveSubscriptionCallbacks)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    std::int32_t streamId = 982375;
    ChannelUriStringBuilder uriBuilder;
    const std::string channel = setParameters(std::get<0>(GetParam()), std::get<1>(GetParam()), uriBuilder)
        .networkInterface("localhost")
        .build();
    std::int32_t imageUnavailable = 0;
    Context ctx;

    MockFunction<void(Image &image)> mockOnAvailableImage;
    MockFunction<void(Image &image)> mockOnUnavailableImage;

    EXPECT_CALL(mockOnAvailableImage, Call(_));
    EXPECT_CALL(mockOnUnavailableImage, Call(_)).WillOnce(
        [&](Image &image)
        {
            aeron::concurrent::atomic::putInt32Volatile(&imageUnavailable, 1);
        });

    ctx.useConductorAgentInvoker(true);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    std::int64_t subId = aeron->addSubscription(
        channel, streamId, mockOnAvailableImage.AsStdFunction(), mockOnUnavailableImage.AsStdFunction());
    std::int64_t pubId = aeron->addPublication(channel, streamId);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();

    {
        // Nest to trigger subscription cleanup
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        {
            // Nest to trigger images becoming unavailable
            POLL_FOR_NON_NULL(pub, aeron->findPublication(pubId), invoker);
            POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);

            std::string message = "hello world!";
            buffer.putString(0, message);
            POLL_FOR(0 < pub->offer(buffer), invoker);

            POLL_FOR(
                0 < sub->poll(
                [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
                {
                    EXPECT_EQ(message, buffer.getString(offset));
                },
                1),
                invoker);
        }

        POLL_FOR(1 == aeron::concurrent::atomic::getInt32Volatile(&imageUnavailable), invoker);
    }

    // Allow callbacks to fire to complete cleanup and prevent sanitizer errors.
    invoker.invoke();
}

TEST_P(PubSubTest, shouldSubscribeExclusivePublish)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    std::int32_t streamId = 982375;
    std::int32_t termId = 23;
    std::int32_t initialTermId = 3;
    std::int32_t termOffset = 1024;
    ChannelUriStringBuilder uriBuilder;
    const std::string channel = setParameters(std::get<0>(GetParam()), std::get<1>(GetParam()), uriBuilder)
        .termId(termId)
        .termOffset(termOffset)
        .initialTermId(initialTermId)
        .networkInterface("localhost")
        .build();

    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    std::int64_t subId = aeron->addSubscription(channel, streamId);
    std::int64_t pubId = aeron->addExclusivePublication(channel, streamId);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();

    {
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        POLL_FOR_NON_NULL(pub, aeron->findExclusivePublication(pubId), invoker);
        POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);

        std::string message = "hello world!";
        buffer.putString(0, message);
        POLL_FOR(0 < pub->offer(buffer), invoker);

        POLL_FOR(0 < sub->poll(
            [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
            {
                EXPECT_EQ(message, buffer.getString(offset));
                EXPECT_EQ(termId, header.termId());
                EXPECT_EQ(termOffset, header.termOffset());
                EXPECT_EQ(initialTermId, header.initialTermId());
            },
            1), invoker);
    }

    invoker.invoke();
}

TEST_P(PubSubTest, shouldBlockPollSubscription)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    std::int32_t streamId = 982375;
    ChannelUriStringBuilder uriBuilder;
    const std::string channel = setParameters(std::get<0>(GetParam()), std::get<1>(GetParam()), uriBuilder)
        .networkInterface("localhost")
        .build();

    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    std::int64_t subId = aeron->addSubscription(channel, streamId);
    std::int64_t pubId = aeron->addPublication(channel, streamId);

    {
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        POLL_FOR_NON_NULL(pub, aeron->findPublication(pubId), invoker);
        POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);

        std::string message = "hello world!";
        buffer.putString(0, message);
        AtomicBuffer buffers[]{buffer, buffer, buffer};
        POLL_FOR(0 < pub->offer(buffers, 3), invoker);

        std::int64_t bytesReceived = 0;
        std::int64_t bytesConsumed = 0;
        POLL_FOR(pub->position() <= (
            bytesConsumed += sub->blockPoll(
                [&](concurrent::AtomicBuffer &buffer,
                    util::index_t offset,
                    util::index_t length,
                    std::int32_t sessionId,
                    std::int32_t termId)
                {
                    bytesReceived += length;
                    EXPECT_EQ(pub->sessionId(), sessionId);
                },
                100000)), invoker);

        EXPECT_EQ(pub->position(), bytesConsumed);
        EXPECT_EQ(pub->position(), bytesReceived);
    }

    invoker.invoke();
}

TEST_P(PubSubTest, shouldTryClaimAndControlledPollSubscription)
{
    std::int32_t streamId = 982375;
    ChannelUriStringBuilder uriBuilder;
    const std::string channel = setParameters(std::get<0>(GetParam()), std::get<1>(GetParam()), uriBuilder)
        .networkInterface("localhost")
        .build();

    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    std::int64_t subId = aeron->addSubscription(channel, streamId);
    std::int64_t pubId = aeron->addPublication(channel, streamId);

    {
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        POLL_FOR_NON_NULL(pub, aeron->findPublication(pubId), invoker);
        POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);

        std::string message = "hello world!";

        BufferClaim claim;
        POLL_FOR(0 < pub->tryClaim(16, claim), invoker);
        claim.buffer().putString(claim.offset(), message);
        claim.commit();
        bool seen = false;

        POLL_FOR(
            0 < sub->controlledPoll(
                [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
                {
                    seen = true;
                    return message == buffer.getString(offset) ?
                        ControlledPollAction::COMMIT : ControlledPollAction::ABORT;
                },
                1),
            invoker);

        EXPECT_TRUE(seen);
    }

    invoker.invoke();
}


TEST_P(PubSubTest, shouldExclusivePublicationTryClaimAndControlledPollSubscription)
{
    std::int32_t streamId = 982375;
    ChannelUriStringBuilder uriBuilder;
    const std::string channel = setParameters(std::get<0>(GetParam()), std::get<1>(GetParam()), uriBuilder)
        .networkInterface("localhost")
        .build();

    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    std::int64_t subId = aeron->addSubscription(channel, streamId);
    std::int64_t pubId = aeron->addExclusivePublication(channel, streamId);

    {
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        POLL_FOR_NON_NULL(pub, aeron->findExclusivePublication(pubId), invoker);
        POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);

        std::string message = "hello world!";

        BufferClaim claim;
        POLL_FOR(0 < pub->tryClaim(16, claim), invoker);
        claim.buffer().putString(claim.offset(), message);
        claim.commit();
        bool seen = false;

        POLL_FOR(
            0 < sub->controlledPoll(
                [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
                {
                    seen = true;
                    return message == buffer.getString(offset) ?
                    ControlledPollAction::COMMIT : ControlledPollAction::ABORT;
                },
                1),
            invoker);

        EXPECT_TRUE(seen);
    }

    invoker.invoke();
}

TEST_P(PubSubTest, shouldHandleEosPosition)
{
    buffer_t buf;
    AtomicBuffer message(buf);
    message.putString(0, "hello world!");

    std::int32_t streamId = 982375;
    ChannelUriStringBuilder uriBuilder;
    const std::string channel = setParameters(std::get<0>(GetParam()), std::get<1>(GetParam()), uriBuilder)
        .networkInterface("localhost")
        .build();
    Context ctx;

    ctx.useConductorAgentInvoker(true);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    std::int64_t subId = aeron->addSubscription(channel, streamId);
    std::int64_t pubId = aeron->addPublication(channel, streamId);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
    const int numMessages = 10;
    int messagesRemaining = numMessages;

    {
        // Nest to trigger subscription cleanup
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        {
            // Nest to trigger images becoming unavailable
            POLL_FOR_NON_NULL(pub, aeron->findPublication(pubId), invoker);
            POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);
            std::shared_ptr<Image> image = sub->imageByIndex(0);

            ASSERT_FALSE(image->isEndOfStream());
            ASSERT_EQ(INT64_MAX, image->endOfStreamPosition());

            for (int i = 0; i < numMessages; i++)
            {
                POLL_FOR(0 < pub->offer(message), invoker);
            }

            std::int64_t tStart = aeron_epoch_clock();
            while (0 < messagesRemaining)
            {
                int pollCount = sub->poll(
                    [&](
                        concurrent::AtomicBuffer &buffer,
                        util::index_t offset,
                        util::index_t length,
                        Header &header)
                    {}, 1);

                if (0 == pollCount)
                {
                    invoker.invoke();
                    ASSERT_LT(aeron_epoch_clock() - tStart, AERON_TEST_TIMEOUT) << "Failed waiting";
                    std::this_thread::yield();
                }

                messagesRemaining -= pollCount;
            }

            ASSERT_FALSE(image->isEndOfStream());
            ASSERT_EQ(INT64_MAX, image->endOfStreamPosition());

            for (int i = 0; i < numMessages; i++)
            {
                POLL_FOR(0 < pub->offer(message), invoker);
            }

            tStart = aeron_epoch_clock();
            messagesRemaining = numMessages;
            while (5 < messagesRemaining)
            {
                int pollCount = sub->poll(
                    [&](
                        concurrent::AtomicBuffer &buffer,
                        util::index_t offset,
                        util::index_t length,
                        Header &header)
                    {}, 1);

                if (0 == pollCount)
                {
                    invoker.invoke();
                    ASSERT_LT(aeron_epoch_clock() - tStart, AERON_TEST_TIMEOUT) << "Failed waiting";
                    std::this_thread::yield();
                }

                messagesRemaining -= pollCount;
            }

            ASSERT_FALSE(image->isEndOfStream());
            ASSERT_EQ(INT64_MAX, image->endOfStreamPosition());
        } // Close the publication by having it go out of scope.

        std::shared_ptr<Image> image = sub->imageByIndex(0);
        ASSERT_FALSE(image->isEndOfStream());

        std::int64_t tStart = aeron_epoch_clock();
        while (0 < messagesRemaining)
        {
            int pollCount = sub->poll([&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header) {}, 1);

            if (0 == pollCount)
            {
                invoker.invoke();
                ASSERT_LT(aeron_epoch_clock() - tStart, AERON_TEST_TIMEOUT) << "Failed waiting";
                std::this_thread::yield();
            }

            messagesRemaining -= pollCount;
        }

        POLL_FOR(image->isEndOfStream(), invoker);
        POLL_FOR(INT64_MAX != image->endOfStreamPosition(), invoker);
        std::int64_t endOfStreamPosition = image->endOfStreamPosition();
        ASSERT_EQ(endOfStreamPosition, image->position());
    }

    // Allow callbacks to fire to complete cleanup and prevent sanitizer errors.
    invoker.invoke();
}


// Useful to look at error stack when manually testing error conditions.
TEST_F(PubSubTest, DISABLED_shouldError)
{
    buffer_t buf;
    AtomicBuffer buffer(buf);
    std::int32_t streamId = 982375;
    std::int32_t termId = 23;
    std::int32_t initialTermId = 3;
    std::int32_t termOffset = 1024;
    ChannelUriStringBuilder uriBuilder;
    const std::string channel = setParameters("udp", "224.0.1.1:20000", uriBuilder)
        .termId(termId)
        .termOffset(termOffset)
        .initialTermId(initialTermId)
        .networkInterface("localhost")
        .build();

    Context ctx;
    ctx.useConductorAgentInvoker(true);

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    std::int64_t subId = aeron->addSubscription(channel, streamId);
    std::int64_t pubId = aeron->addExclusivePublication(channel, streamId);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();

    {
        POLL_FOR_NON_NULL(sub, aeron->findSubscription(subId), invoker);
        POLL_FOR_NON_NULL(pub, aeron->findExclusivePublication(pubId), invoker);
        POLL_FOR(pub->isConnected() && sub->isConnected(), invoker);

        std::string message = "hello world!";
        buffer.putString(0, message);
        POLL_FOR(0 < pub->offer(buffer), invoker);

        POLL_FOR(0 < sub->poll(
            [&](concurrent::AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
            {
                EXPECT_EQ(message, buffer.getString(offset));
                EXPECT_EQ(termId, header.termId());
                EXPECT_EQ(termOffset, header.termOffset());
                EXPECT_EQ(initialTermId, header.initialTermId());
            },
            1), invoker);
    }

    invoker.invoke();
}

class Exchanger
{
    using generator_t = std::independent_bits_engine<std::default_random_engine, CHAR_BIT, unsigned short>;

public:
    explicit Exchanger(
        const std::shared_ptr<Subscription> &subscription,
        const std::shared_ptr<ExclusivePublication> &publication,
        AgentInvoker<ClientConductor> &invoker) :
        m_subscription(subscription),
        m_publication(publication),
        m_assembler(
             [&](AtomicBuffer &buffer, index_t offset, index_t length, Header &header)
             {
                 m_innerHandler(buffer, offset, length, header);
             }),
        m_outerHandler(m_assembler.handler()),
        m_generator(generator_t(m_rd())),
        m_invoker(invoker)
    {
    }

    void exchange(int messageSize)
    {
        std::vector<uint8_t> vec(messageSize);
        std::generate(std::begin(vec), std::end(vec), [&] () { return static_cast<uint8_t>(m_generator()); } );

        AtomicBuffer buffer(vec.data(), messageSize);
        int64_t result;
        while((result = m_publication->offer(buffer)) < 0)
        {
            if (AERON_PUBLICATION_BACK_PRESSURED != result && AERON_PUBLICATION_ADMIN_ACTION != result)
            {
                FAIL() << "offer failed: " << result;
            }
            std::this_thread::yield();
        }

        int count = 0;
        m_innerHandler = [&](AtomicBuffer &buffer, index_t offset, index_t length, Header &header)
        {
            count++;
            ASSERT_EQ(messageSize, length);
            ASSERT_EQ(0, memcmp(buffer.buffer() + offset, vec.data(), length));
        };

        std::int64_t t0 = aeron_epoch_clock();
        while (count == 0)
        {
            m_invoker.invoke();
            m_subscription->poll(m_outerHandler, 10);
            ASSERT_LT(aeron_epoch_clock() - t0, AERON_TEST_TIMEOUT) << "Failed waiting for: count > 0";
            std::this_thread::yield();
        }

        ASSERT_EQ(1, count);
    }

private:
    std::shared_ptr<Subscription> m_subscription;
    std::shared_ptr<ExclusivePublication> m_publication;
    FragmentAssembler m_assembler;
    fragment_handler_t m_outerHandler;
    fragment_handler_t m_innerHandler;
    std::random_device m_rd;
    generator_t m_generator;
    AgentInvoker<ClientConductor> &m_invoker;
};

TEST_F(PubSubTest, shouldFragmentAndReassembleMessagesIfNeeded)
{
    const int32_t streamId = 1000;

    Context ctx;
    ctx.useConductorAgentInvoker(true);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    int64_t subscriptionId = aeron->addSubscription(IPC_CHANNEL, streamId);
    int64_t publicationId = aeron->addExclusivePublication(IPC_CHANNEL, streamId);
    AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();

    {
        POLL_FOR_NON_NULL(subscription, aeron->findSubscription(subscriptionId), invoker);
        POLL_FOR_NON_NULL(publication, aeron->findExclusivePublication(publicationId), invoker);
        POLL_FOR(publication->isConnected(), invoker);

        Exchanger exchanger(subscription, publication, invoker);
        exchanger.exchange(publication->maxPayloadLength() + 1);
        exchanger.exchange(publication->maxPayloadLength() * 3);
        exchanger.exchange(32);
    }

    invoker.invoke();
}

class PubSubTestInvokerTest : public testing::TestWithParam<bool>
{
public:
    PubSubTestInvokerTest()
    {
        m_driver.start();
    }

    ~PubSubTestInvokerTest() override
    {
        m_driver.stop();
    }

protected:
    EmbeddedMediaDriver m_driver;
};

INSTANTIATE_TEST_SUITE_P(
    PubSubTestInvokerTest,
    PubSubTestInvokerTest,
    testing::Values(false, true));

TEST_P(PubSubTestInvokerTest, shouldCreatePublicationUsingAsyncApi)
{
    const int32_t streamId = 1000;

    Context ctx;
    bool use_invoker = GetParam();
    ctx.useConductorAgentInvoker(use_invoker);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);

    auto asyncPublication = aeron->addPublicationAsync(IPC_CHANNEL, streamId);
    ASSERT_NE(nullptr, asyncPublication);
    const int64_t registrationId = aeron->addPublicationAsyncGetRegistrationId(asyncPublication);

    if (use_invoker)
    {
        AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
        POLL_FOR_NON_NULL(publication, aeron->findPublication(asyncPublication), invoker);
        ASSERT_EQ(registrationId, publication->registrationId());
    }
    else
    {
        WAIT_FOR_NON_NULL(publication, aeron->findPublication(asyncPublication));
        ASSERT_EQ(registrationId, publication->registrationId());
    }
}

TEST_P(PubSubTestInvokerTest, shouldCreateExclusivePublicationUsingAsyncApi)
{
    const int32_t streamId = 231;

    Context ctx;
    bool use_invoker = GetParam();
    ctx.useConductorAgentInvoker(use_invoker);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);

    auto asyncPublication = aeron->addExclusivePublicationAsync(IPC_CHANNEL, streamId);
    ASSERT_NE(nullptr, asyncPublication);
    const int64_t registrationId = aeron->addExclusivePublicationAsyncGetRegistrationId(asyncPublication);

    if (use_invoker)
    {
        AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
        POLL_FOR_NON_NULL(publication, aeron->findExclusivePublication(asyncPublication), invoker);
        ASSERT_EQ(registrationId, publication->registrationId());
    }
    else
    {
        WAIT_FOR_NON_NULL(publication, aeron->findExclusivePublication(asyncPublication));
        ASSERT_EQ(registrationId, publication->registrationId());
    }
}

TEST_P(PubSubTestInvokerTest, shouldCreateSubscriptionUsingAsyncApi)
{
    const int32_t streamId = 231;

    Context ctx;
    bool use_invoker = GetParam();
    ctx.useConductorAgentInvoker(use_invoker);
    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);

    auto asyncSubscription = aeron->addSubscriptionAsync(IPC_CHANNEL, streamId);
    ASSERT_NE(nullptr, asyncSubscription);
    const int64_t registrationId = aeron->addSubscriptionAsyncGetRegistrationId(asyncSubscription);

    if (use_invoker)
    {
        AgentInvoker<ClientConductor> &invoker = aeron->conductorAgentInvoker();
        POLL_FOR_NON_NULL(subscription, aeron->findSubscription(asyncSubscription), invoker);
        ASSERT_EQ(registrationId, subscription->registrationId());
    }
    else
    {
        WAIT_FOR_NON_NULL(subscription, aeron->findSubscription(asyncSubscription));
        ASSERT_EQ(registrationId, subscription->registrationId());
    }
}
