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

#include "aeron_test_base.h"

extern "C"
{
#include "concurrent/aeron_atomic.h"
#include "agent/aeron_driver_agent.h"
#include "aeron_driver_context.h"
}

#define URI "aeron:udp?endpoint=localhost:24325"
#define STREAM_ID (117)

struct message_t
{
    int64_t timestamp_1;
    int64_t timestamp_2;
    char text[16];
};

class TimestampsTest : public CSystemTestBase, public testing::Test
{
public:
    TimestampsTest() : CSystemTestBase(std::vector<std::pair<std::string, std::string>>{
            { "AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS", "loss" },
            { "AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_LOSS_ARGS", "rate=0" }
        })
    {}
};

int64_t null_reserved_value(void *clientd, uint8_t *buffer, size_t frame_length)
{
    return AERON_NULL_VALUE;
}

TEST_F(TimestampsTest, shouldPutTimestampInMessagesReservedValue)
{
#if !defined(__linux__)
    GTEST_SKIP();
#endif

    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_add_subscription_t *async_sub = nullptr;
    std::string uri = std::string(URI);
    const char *uri_s = uri.append("|media-rcv-ts-offset=reserved").c_str();

    struct message_t message = {};
    message.timestamp_1 = AERON_NULL_VALUE;
    message.timestamp_2 = AERON_NULL_VALUE;
    strcpy(message.text, "hello");

    ASSERT_TRUE(connect());
    ASSERT_EQ(aeron_async_add_publication(&async_pub, m_aeron, uri_s, STREAM_ID), 0);

    aeron_publication_t *publication = awaitPublicationOrError(async_pub);
    ASSERT_TRUE(publication) << aeron_errmsg();

    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, uri_s, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);

    aeron_subscription_t *subscription = awaitSubscriptionOrError(async_sub);
    ASSERT_TRUE(subscription) << aeron_errmsg();
    awaitConnected(subscription);

    while (aeron_publication_offer(
        publication, (const uint8_t *)&message, sizeof(message), null_reserved_value, nullptr) < 0)
    {
        std::this_thread::yield();
    }

    int poll_result;
    bool called = false;
    poll_handler_t handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        aeron_header_values_t header_values;
        aeron_header_values(header, &header_values);
        auto *incoming = (message_t *)buffer;
        EXPECT_NE(AERON_NULL_VALUE, header_values.frame.reserved_value);
        EXPECT_EQ(AERON_NULL_VALUE, incoming->timestamp_1);
        EXPECT_EQ(AERON_NULL_VALUE, incoming->timestamp_2);
        EXPECT_STREQ(incoming->text, message.text);
        called = true;
    };

    while ((poll_result = poll(subscription, handler, 1)) == 0)
    {
        std::this_thread::yield();
    }
    EXPECT_EQ(poll_result, 1) << aeron_errmsg();
    EXPECT_TRUE(called);

    EXPECT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);
    EXPECT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
}

TEST_F(TimestampsTest, shouldPutTimestampInMessagesAtOffset)
{
#if !defined(__linux__)
    GTEST_SKIP();
#endif

    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_add_subscription_t *async_sub = nullptr;
    std::stringstream uriStream;
    uriStream << URI << "|media-rcv-ts-offset=" << offsetof(message_t, timestamp_2) << '\0';
    std::string uri = uriStream.str();
    const char *uri_s = uri.c_str();

    struct message_t message = {};
    message.timestamp_1 = AERON_NULL_VALUE;
    message.timestamp_2 = AERON_NULL_VALUE;
    strcpy(message.text, "hello");

    ASSERT_TRUE(connect());
    ASSERT_EQ(aeron_async_add_publication(&async_pub, m_aeron, uri_s, STREAM_ID), 0);

    aeron_publication_t *publication = awaitPublicationOrError(async_pub);
    ASSERT_TRUE(publication) << aeron_errmsg();

    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, uri_s, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);

    aeron_subscription_t *subscription = awaitSubscriptionOrError(async_sub);
    ASSERT_TRUE(subscription) << aeron_errmsg();
    awaitConnected(subscription);

    while (aeron_publication_offer(
        publication, (const uint8_t *)&message, sizeof(message), null_reserved_value, nullptr) < 0)
    {
        std::this_thread::yield();
    }

    int poll_result;
    bool called = false;
    poll_handler_t handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        aeron_header_values_t header_values;
        aeron_header_values(header, &header_values);
        auto *incoming = (message_t *)buffer;
        EXPECT_EQ(AERON_NULL_VALUE, header_values.frame.reserved_value);
        EXPECT_EQ(AERON_NULL_VALUE, incoming->timestamp_1);
        EXPECT_NE(AERON_NULL_VALUE, incoming->timestamp_2);
        EXPECT_STREQ(incoming->text, message.text);
        called = true;
    };

    while ((poll_result = poll(subscription, handler, 1)) == 0)
    {
        std::this_thread::yield();
    }
    EXPECT_EQ(poll_result, 1) << aeron_errmsg();
    EXPECT_TRUE(called);

    EXPECT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);
    EXPECT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
}

TEST_F(TimestampsTest, shouldNotPutTimestampInMessagesAtIfOffsetExceedsMessage)
{
#if !defined(__linux__)
    GTEST_SKIP();
#endif

    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_add_subscription_t *async_sub = nullptr;
    std::stringstream uriStream;
    uriStream << URI << "|media-rcv-ts-offset=" << sizeof(message_t) - 4 << '\0';
    std::string uri = uriStream.str();
    const char *uri_s = uri.c_str();

    struct message_t message = {};

    ASSERT_TRUE(connect());
    ASSERT_EQ(aeron_async_add_publication(&async_pub, m_aeron, uri_s, STREAM_ID), 0);

    aeron_publication_t *publication = awaitPublicationOrError(async_pub);
    ASSERT_TRUE(publication) << aeron_errmsg();

    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, uri_s, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);

    aeron_subscription_t *subscription = awaitSubscriptionOrError(async_sub);
    ASSERT_TRUE(subscription) << aeron_errmsg();
    awaitConnected(subscription);

    while (aeron_publication_offer(
        publication, (const uint8_t *)&message, sizeof(message), null_reserved_value, nullptr) < 0)
    {
        std::this_thread::yield();
    }

    int poll_result;
    poll_handler_t handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        aeron_header_values_t header_values;
        aeron_header_values(header, &header_values);
        auto *incoming = (message_t *)buffer;
        EXPECT_EQ(AERON_NULL_VALUE, header_values.frame.reserved_value);
        EXPECT_EQ(0, incoming->timestamp_1);
        EXPECT_EQ(0, incoming->timestamp_2);
        EXPECT_EQ(0, memcmp(incoming->text, message.text, sizeof(incoming->text)));
    };

    while ((poll_result = poll(subscription, handler, 1)) == 0)
    {
        std::this_thread::yield();
    }
    EXPECT_EQ(poll_result, 1) << aeron_errmsg();

    EXPECT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);
    EXPECT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
}

TEST_F(TimestampsTest, shouldErrorIfMediaReceiveTimestampConfigurationClashes)
{
    aeron_async_add_subscription_t *async_sub = nullptr;

    std::string uriOriginal = std::string(URI);
    const char *uriOriginal_s = uriOriginal.append("|media-rcv-ts-offset=8").c_str();

    const char *uriNotSpecified_s = URI;

    std::string uriDifferentOffset = std::string(URI);
    const char *uriDifferentOffset_s = uriDifferentOffset.append("|media-rcv-ts-offset=reserved").c_str();

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, uriOriginal_s, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    aeron_subscription_t *subscription = awaitSubscriptionOrError(async_sub);
    ASSERT_TRUE(subscription) << aeron_errmsg();

    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, uriNotSpecified_s, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    ASSERT_FALSE(awaitSubscriptionOrError(async_sub));

    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, uriDifferentOffset_s, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    ASSERT_FALSE(awaitSubscriptionOrError(async_sub));
}

TEST_F(TimestampsTest, shouldErrorIfChannelReceiveTimestampConfigurationClashes)
{
    aeron_async_add_subscription_t *async_sub = nullptr;

    std::string uriOriginal = std::string(URI);
    const char *uriOriginal_s = uriOriginal.append("|channel-rcv-ts-offset=8").c_str();

    const char *uriNotSpecified_s = URI;

    std::string uriDifferentOffset = std::string(URI);
    const char *uriDifferentOffset_s = uriDifferentOffset.append("|channel-rcv-ts-offset=reserved").c_str();

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, uriOriginal_s, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    aeron_subscription_t *subscription = awaitSubscriptionOrError(async_sub);
    ASSERT_TRUE(subscription) << aeron_errmsg();

    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, uriNotSpecified_s, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    ASSERT_FALSE(awaitSubscriptionOrError(async_sub));

    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, uriDifferentOffset_s, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    ASSERT_FALSE(awaitSubscriptionOrError(async_sub));
}

TEST_F(TimestampsTest, shouldErrorIfChannelSendTimestampConfigurationClashes)
{
    aeron_async_add_subscription_t *async_pub = nullptr;

    std::string uriOriginal = std::string(URI);
    const char *uriOriginal_s = uriOriginal.append("|channel-snd-ts-offset=8").c_str();

    const char *uriNotSpecified_s = URI;

    std::string uriDifferentOffset = std::string(URI);
    const char *uriDifferentOffset_s = uriDifferentOffset.append("|channel-snd-ts-offset=reserved").c_str();

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_publication(&async_pub, m_aeron, uriOriginal_s, STREAM_ID), 0);
    ASSERT_TRUE(awaitPublicationOrError(async_pub)) << aeron_errmsg();

    ASSERT_EQ(aeron_async_add_publication(&async_pub, m_aeron, uriNotSpecified_s, STREAM_ID), 0);
    ASSERT_FALSE(awaitPublicationOrError(async_pub));

    ASSERT_EQ(aeron_async_add_publication(&async_pub, m_aeron, uriDifferentOffset_s, STREAM_ID), 0);
    ASSERT_FALSE(awaitPublicationOrError(async_pub));
}

TEST_F(TimestampsTest, shouldPutTimestampInMessagesReservedValueWithMergedMds)
{
#if !defined(__linux__)
    GTEST_SKIP();
#endif

    aeron_async_add_exclusive_publication_t *asyncPubA = nullptr;
    aeron_async_add_exclusive_publication_t *asyncPubB = nullptr;
    aeron_async_add_subscription_t *asyncSub = nullptr;
    aeron_async_destination_t *asyncDestA = nullptr;
    aeron_async_destination_t *asyncDestB = nullptr;
    std::string destinationA = std::string("aeron:udp?endpoint=localhost:24325");
    std::string destinationB = std::string("aeron:udp?endpoint=localhost:24326");
    std::string mdsUri = std::string("aeron:udp?control-mode=manual|media-rcv-ts-offset=reserved");

    struct message_t message = {};
    message.timestamp_1 = AERON_NULL_VALUE;
    message.timestamp_2 = AERON_NULL_VALUE;
    strcpy(message.text, "hello");

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_subscription(
        &asyncSub, m_aeron, mdsUri.c_str(), STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);

    aeron_subscription_t *subscription = awaitSubscriptionOrError(asyncSub);
    ASSERT_TRUE(subscription) << aeron_errmsg();

    ASSERT_EQ(0, aeron_subscription_async_add_destination(&asyncDestA, m_aeron, subscription, destinationA.c_str()));
    ASSERT_TRUE(awaitDestinationOrError(asyncDestA));

    ASSERT_EQ(0, aeron_subscription_async_add_destination(&asyncDestB, m_aeron, subscription, destinationB.c_str()));
    ASSERT_TRUE(awaitDestinationOrError(asyncDestB));

    ASSERT_EQ(aeron_async_add_exclusive_publication(&asyncPubA, m_aeron, destinationA.c_str(), STREAM_ID), 0);
    aeron_exclusive_publication_t *publicationA = awaitExclusivePublicationOrError(asyncPubA);
    ASSERT_TRUE(publicationA) << aeron_errmsg();

    aeron_publication_constants_t pubAConstants;
    aeron_exclusive_publication_constants(publicationA, &pubAConstants);
    int64_t pubAPosition = aeron_exclusive_publication_position(publicationA);

    int32_t termId = aeron_logbuffer_compute_term_id_from_position(
        pubAPosition,
        pubAConstants.position_bits_to_shift,
        pubAConstants.initial_term_id);
    auto termOffset = (int32_t)(pubAPosition & (pubAConstants.term_buffer_length - 1));

    std::stringstream publicationBStream;
    publicationBStream << destinationB;
    publicationBStream << "|session-id=" << pubAConstants.session_id;
    publicationBStream << "|init-term-id=" << pubAConstants.initial_term_id;
    publicationBStream << "|term-id=" << termId;
    publicationBStream << "|term-offset=" << termOffset;
    std::string destB = publicationBStream.str();

    ASSERT_EQ(aeron_async_add_exclusive_publication(&asyncPubB, m_aeron, destB.c_str(), STREAM_ID), 0);
    aeron_exclusive_publication_t *publicationB = awaitExclusivePublicationOrError(asyncPubB);
    ASSERT_TRUE(publicationB) << aeron_errmsg();

    awaitConnected(subscription);

    int poll_result;
    int called = 0;
    poll_handler_t handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        aeron_header_values_t header_values;
        aeron_header_values(header, &header_values);
        auto *incoming = (message_t *)buffer;
        EXPECT_NE(AERON_NULL_VALUE, header_values.frame.reserved_value);
        EXPECT_EQ(AERON_NULL_VALUE, incoming->timestamp_1);
        EXPECT_EQ(AERON_NULL_VALUE, incoming->timestamp_2);
        EXPECT_STREQ(incoming->text, message.text);
        called++;
    };

    while (aeron_exclusive_publication_offer(
        publicationA, (const uint8_t *)&message, sizeof(message), null_reserved_value, nullptr) < 0)
    {
        std::this_thread::yield();
    }

    while ((poll_result = poll(subscription, handler, 1)) == 0)
    {
        std::this_thread::yield();
    }
    EXPECT_EQ(poll_result, 1) << aeron_errmsg();
    EXPECT_EQ(1, called);

    while (aeron_exclusive_publication_offer(
        publicationB, (const uint8_t *)&message, sizeof(message), null_reserved_value, nullptr) < 0)
    {
        std::this_thread::yield();
    }

    // Check that publicationB's first message is merged (i.e. not visible to the subscription).
    for (int i = 0; i < 500; i++)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        ASSERT_EQ(0, poll(subscription, handler, 1));
    }

    while (aeron_exclusive_publication_offer(
        publicationB, (const uint8_t *)&message, sizeof(message), null_reserved_value, nullptr) < 0)
    {
        std::this_thread::yield();
    }

    while ((poll_result = poll(subscription, handler, 1)) == 0)
    {
        std::this_thread::yield();
    }
    EXPECT_EQ(poll_result, 1) << aeron_errmsg();
    EXPECT_EQ(2, called);

    EXPECT_EQ(aeron_exclusive_publication_close(publicationA, nullptr, nullptr), 0);
    EXPECT_EQ(aeron_exclusive_publication_close(publicationB, nullptr, nullptr), 0);
    EXPECT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
}

TEST_F(TimestampsTest, shouldPutTimestampInMessagesReservedValueWithNonMergedMds)
{
#if !defined(__linux__)
    GTEST_SKIP();
#endif

    aeron_async_add_publication_t *asyncPubA = nullptr;
    aeron_async_add_publication_t *asyncPubB = nullptr;
    aeron_async_add_subscription_t *asyncSub = nullptr;
    aeron_async_destination_t *asyncDestA = nullptr;
    aeron_async_destination_t *asyncDestB = nullptr;
    std::string destinationA = std::string("aeron:udp?endpoint=localhost:24325");
    std::string destinationB = std::string("aeron:udp?endpoint=localhost:24326");
    std::string mdsUri = std::string("aeron:udp?control-mode=manual|media-rcv-ts-offset=reserved");

    struct message_t message = {};
    message.timestamp_1 = AERON_NULL_VALUE;
    message.timestamp_2 = AERON_NULL_VALUE;
    strcpy(message.text, "hello");

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_subscription(
        &asyncSub, m_aeron, mdsUri.c_str(), STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);

    aeron_subscription_t *subscription = awaitSubscriptionOrError(asyncSub);
    ASSERT_TRUE(subscription) << aeron_errmsg();

    ASSERT_EQ(0, aeron_subscription_async_add_destination(&asyncDestA, m_aeron, subscription, destinationA.c_str()));
    ASSERT_TRUE(awaitDestinationOrError(asyncDestA));

    ASSERT_EQ(0, aeron_subscription_async_add_destination(&asyncDestB, m_aeron, subscription, destinationB.c_str()));
    ASSERT_TRUE(awaitDestinationOrError(asyncDestB));

    ASSERT_EQ(aeron_async_add_publication(&asyncPubA, m_aeron, destinationA.c_str(), STREAM_ID), 0);
    aeron_publication_t *publicationA = awaitPublicationOrError(asyncPubA);
    ASSERT_TRUE(publicationA) << aeron_errmsg();

    ASSERT_EQ(aeron_async_add_publication(&asyncPubB, m_aeron, destinationB.c_str(), STREAM_ID), 0);
    aeron_publication_t *publicationB = awaitPublicationOrError(asyncPubB);
    ASSERT_TRUE(publicationB) << aeron_errmsg();

    awaitConnected(subscription);

    while (aeron_publication_offer(
        publicationA, (const uint8_t *)&message, sizeof(message), null_reserved_value, nullptr) < 0)
    {
        std::this_thread::yield();
    }

    while (aeron_publication_offer(
        publicationB, (const uint8_t *)&message, sizeof(message), null_reserved_value, nullptr) < 0)
    {
        std::this_thread::yield();
    }

    int poll_result;
    int called = 0;
    poll_handler_t handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        aeron_header_values_t header_values;
        aeron_header_values(header, &header_values);
        auto *incoming = (message_t *)buffer;
        EXPECT_NE(AERON_NULL_VALUE, header_values.frame.reserved_value);
        EXPECT_EQ(AERON_NULL_VALUE, incoming->timestamp_1);
        EXPECT_EQ(AERON_NULL_VALUE, incoming->timestamp_2);
        EXPECT_STREQ(incoming->text, message.text);
        called++;
    };

    while ((poll_result = poll(subscription, handler, 1)) == 0)
    {
        std::this_thread::yield();
    }
    EXPECT_EQ(poll_result, 1) << aeron_errmsg();
    EXPECT_EQ(1, called);

    while ((poll_result = poll(subscription, handler, 1)) == 0)
    {
        std::this_thread::yield();
    }
    EXPECT_EQ(poll_result, 1) << aeron_errmsg();
    EXPECT_EQ(2, called);

    EXPECT_EQ(aeron_publication_close(publicationA, nullptr, nullptr), 0);
    EXPECT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
}

TEST_F(TimestampsTest, shouldPutChannelSendAndReceivesTimestampsInMessagesAtOffset)
{
#if !defined(__linux__)
    GTEST_SKIP();
#endif

    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_add_subscription_t *async_sub = nullptr;
    std::stringstream uriStream;
    uriStream << URI <<
        "|media-rcv-ts-offset=reserved" <<
        "|channel-snd-ts-offset=" << offsetof(message_t, timestamp_2) <<
        "|channel-rcv-ts-offset=" << offsetof(message_t, timestamp_1) << '\0';

    std::string uri = uriStream.str();
    const char *uri_s = uri.c_str();

    struct message_t message = {};
    message.timestamp_1 = AERON_NULL_VALUE;
    message.timestamp_2 = AERON_NULL_VALUE;
    strcpy(message.text, "hello");

    ASSERT_TRUE(connect());
    ASSERT_EQ(aeron_async_add_publication(&async_pub, m_aeron, uri_s, STREAM_ID), 0);

    aeron_publication_t *publication = awaitPublicationOrError(async_pub);
    ASSERT_TRUE(publication) << aeron_errmsg();

    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, uri_s, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);

    aeron_subscription_t *subscription = awaitSubscriptionOrError(async_sub);
    ASSERT_TRUE(subscription) << aeron_errmsg();
    awaitConnected(subscription);

    while (aeron_publication_offer(
        publication, (const uint8_t *)&message, sizeof(message), null_reserved_value, nullptr) < 0)
    {
        std::this_thread::yield();
    }

    int poll_result;
    bool called = false;
    poll_handler_t handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        aeron_header_values_t header_values;
        aeron_header_values(header, &header_values);
        auto *incoming = (message_t*)buffer;
        EXPECT_NE(AERON_NULL_VALUE, header_values.frame.reserved_value);
        EXPECT_NE(AERON_NULL_VALUE, incoming->timestamp_1);
        EXPECT_NE(AERON_NULL_VALUE, incoming->timestamp_2);
        EXPECT_STREQ(incoming->text, message.text);
        called = true;
    };

    while ((poll_result = poll(subscription, handler, 1)) == 0)
    {
        std::this_thread::yield();
    }
    EXPECT_EQ(poll_result, 1) << aeron_errmsg();
    EXPECT_TRUE(called);

    EXPECT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);
    EXPECT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
}

TEST_F(TimestampsTest, shouldSendTimestampAllMessagesInReservedValueWithMdc)
{
#if !defined(__linux__)
    GTEST_SKIP();
#endif

    aeron_async_add_publication_t *asyncPub = nullptr;
    aeron_async_add_subscription_t *asyncSubA = nullptr;
    aeron_async_add_subscription_t *asyncSubB = nullptr;
    aeron_async_destination_t *asyncDestA = nullptr;
    aeron_async_destination_t *asyncDestB = nullptr;
    std::string destinationA = std::string("aeron:udp?endpoint=localhost:24325");
    std::string destinationB = std::string("aeron:udp?endpoint=localhost:24326");
    std::string mdcUri = std::string("aeron:udp?control-mode=manual|channel-snd-ts-offset=reserved");

    struct message_t message = {};
    message.timestamp_1 = AERON_NULL_VALUE;
    message.timestamp_2 = AERON_NULL_VALUE;
    strcpy(message.text, "hello");

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_subscription(
        &asyncSubA, m_aeron, destinationA.c_str(), STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);

    aeron_subscription_t *subA = awaitSubscriptionOrError(asyncSubA);
    ASSERT_TRUE(subA) << aeron_errmsg();

    ASSERT_EQ(aeron_async_add_subscription(
        &asyncSubB, m_aeron, destinationB.c_str(), STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);

    aeron_subscription_t *subB = awaitSubscriptionOrError(asyncSubB);
    ASSERT_TRUE(subB) << aeron_errmsg();

    ASSERT_EQ(aeron_async_add_publication(&asyncPub, m_aeron, mdcUri.c_str(), STREAM_ID), 0);
    aeron_publication_t *pub = awaitPublicationOrError(asyncPub);
    ASSERT_TRUE(pub) << aeron_errmsg();

    ASSERT_EQ(0, aeron_publication_async_add_destination(&asyncDestA, m_aeron, pub, destinationA.c_str()));
    ASSERT_TRUE(awaitDestinationOrError(asyncDestA)) << aeron_errmsg();

    ASSERT_EQ(0, aeron_publication_async_add_destination(&asyncDestB, m_aeron, pub, destinationB.c_str()));
    ASSERT_TRUE(awaitDestinationOrError(asyncDestB));

    awaitConnected(subA);
    awaitConnected(subB);

    while (aeron_publication_offer(
        pub, (const uint8_t *)&message, sizeof(message), null_reserved_value, nullptr) < 0)
    {
        std::this_thread::yield();
    }

    int poll_result;
    int called = 0;
    poll_handler_t handler = [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        aeron_header_values_t header_values;
        aeron_header_values(header, &header_values);
        auto *incoming = (message_t*)buffer;
        EXPECT_NE(AERON_NULL_VALUE, header_values.frame.reserved_value);
        EXPECT_STREQ(incoming->text, message.text);
        called++;
    };

    while ((poll_result = poll(subA, handler, 1)) == 0)
    {
        std::this_thread::yield();
    }
    EXPECT_EQ(poll_result, 1) << aeron_errmsg();
    EXPECT_EQ(1, called);

    while ((poll_result = poll(subB, handler, 1)) == 0)
    {
        std::this_thread::yield();
    }
    EXPECT_EQ(poll_result, 1) << aeron_errmsg();
    EXPECT_EQ(2, called);

    EXPECT_EQ(aeron_publication_close(pub, nullptr, nullptr), 0);
    EXPECT_EQ(aeron_subscription_close(subA, nullptr, nullptr), 0);
    EXPECT_EQ(aeron_subscription_close(subB, nullptr, nullptr), 0);
}
