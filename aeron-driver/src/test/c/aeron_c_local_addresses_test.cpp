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

#include "aeron_test_base.h"

extern "C"
{
#include "concurrent/aeron_atomic.h"
}

#if defined(GTEST_USES_POSIX_RE)
#define RESOLVED_ADDRESS_PATTERN "^127\\.0\\.0\\.1:[1-9][0-9]*$"
#define RESOLVED_IPV6_ADDRESS_PATTERN "^\\[::1\\]:[1-9][0-9]*$"
#elif defined(GTEST_USES_SIMPLE_RE)
#define RESOLVED_ADDRESS_PATTERN "^127\\.0\\.0\\.1:\\d*$"
#define RESOLVED_IPV6_ADDRESS_PATTERN "^\\[::1\\]:\\d*$"
#endif
#define PUB_URI_ENDPOINT "127.0.0.1"
#define PUB_URI_CONTROL "127.0.0.1:24326"
#define URI_RESERVED "aeron:udp?endpoint=" PUB_URI_ENDPOINT ":0|control=" PUB_URI_CONTROL
#define URI_PUB "aeron:udp?endpoint=" PUB_URI_ENDPOINT ":1024|control=" PUB_URI_CONTROL
#define PUB_URI_IPV6 "aeron:udp?endpoint=[::1]:0"
#define STREAM_ID (117)

#define NUM_BUFFERS (4)

using namespace aeron;

class CLocalAddressesTest : public CSystemTestBase, public testing::Test
{
public:
    CLocalAddressesTest()
    {
        for (int i = 0; i < NUM_BUFFERS; i++)
        {
            m_addrs[i].iov_base = m_buffers[i];
            m_addrs[i].iov_len = sizeof(m_buffers[i]);
        }
    }

    static int awaitSubscriptionDestinationOrError(aeron_async_destination_t *async)
    {
        while (true)
        {
            std::this_thread::yield();
            int result = aeron_subscription_async_destination_poll(async);
            if (result != 0)
            {
                return result;
            }
        }
    }

protected:
    uint8_t m_buffers[NUM_BUFFERS][AERON_CLIENT_MAX_LOCAL_ADDRESS_STR_LEN] = {};
    aeron_iovec_t m_addrs[NUM_BUFFERS] = {};
};

TEST_F(CLocalAddressesTest, shouldGetAddressForPublication)
{
    std::atomic<bool> publicationClosedFlag(false);
    aeron_async_add_publication_t *async;
    aeron_publication_t *publication;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_publication(&async, m_aeron, URI_PUB, STREAM_ID), 0);
    ASSERT_TRUE((publication = awaitPublicationOrError(async))) << aeron_errmsg();

    ASSERT_EQ(1, aeron_publication_local_sockaddrs(publication, m_addrs, NUM_BUFFERS));
    ASSERT_STREQ(PUB_URI_CONTROL, reinterpret_cast<char *>(m_addrs[0].iov_base));

    aeron_publication_close(publication, setFlagOnClose, &publicationClosedFlag);

    while (!publicationClosedFlag)
    {
        std::this_thread::yield();
    }
}

TEST_F(CLocalAddressesTest, shouldGetAddressForExclusivePublication)
{
    std::atomic<bool> publicationClosedFlag(false);
    aeron_async_add_exclusive_publication_t *async = nullptr;
    aeron_exclusive_publication_t *publication;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_exclusive_publication(&async, m_aeron, URI_PUB, STREAM_ID), 0);
    ASSERT_TRUE((publication = awaitExclusivePublicationOrError(async))) << aeron_errmsg();

    ASSERT_EQ(1, aeron_exclusive_publication_local_sockaddrs(publication, m_addrs, NUM_BUFFERS));
    ASSERT_STREQ(PUB_URI_CONTROL, reinterpret_cast<char *>(m_addrs[0].iov_base));

    aeron_exclusive_publication_close(publication, setFlagOnClose, &publicationClosedFlag);

    while (!publicationClosedFlag)
    {
        std::this_thread::yield();
    }
}


TEST_F(CLocalAddressesTest, shouldGetAddressForSubscription)
{
    std::atomic<bool> subscriptionClosedFlag(false);
    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_subscription(
        &async, m_aeron, URI_RESERVED, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    ASSERT_TRUE((subscription = awaitSubscriptionOrError(async))) << aeron_errmsg();

    ASSERT_EQ(1, aeron_subscription_local_sockaddrs(subscription, m_addrs, NUM_BUFFERS));
    ASSERT_THAT(reinterpret_cast<char *>(m_addrs[0].iov_base), testing::ContainsRegex(RESOLVED_ADDRESS_PATTERN));

    ASSERT_EQ(1, aeron_subscription_resolved_endpoint(subscription, reinterpret_cast<char *>(m_buffers[0]), 1024));
    ASSERT_THAT(reinterpret_cast<char *>(m_buffers[0]), testing::ContainsRegex(RESOLVED_ADDRESS_PATTERN));

    std::string resolvedEndpointParam = "endpoint=" + std::string(reinterpret_cast<char *>(m_addrs[0].iov_base));
    char uriWithResolvedEndpoint[1024] = { 0 };
    aeron_subscription_try_resolve_channel_endpoint_port(
        subscription, uriWithResolvedEndpoint, sizeof(uriWithResolvedEndpoint));

    ASSERT_THAT(uriWithResolvedEndpoint, testing::HasSubstr(resolvedEndpointParam));

    aeron_subscription_close(subscription, setFlagOnClose, &subscriptionClosedFlag);

    while (!subscriptionClosedFlag)
    {
        std::this_thread::yield();
    }
}

TEST_F(CLocalAddressesTest, shouldGetIPv6AddressForSubscription)
{
    std::atomic<bool> subscriptionClosedFlag(false);
    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_subscription(
        &async, m_aeron, PUB_URI_IPV6, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    ASSERT_TRUE((subscription = awaitSubscriptionOrError(async))) << aeron_errmsg();

    ASSERT_EQ(1, aeron_subscription_local_sockaddrs(subscription, m_addrs, NUM_BUFFERS));
    ASSERT_THAT(reinterpret_cast<char *>(m_addrs[0].iov_base), testing::ContainsRegex(RESOLVED_IPV6_ADDRESS_PATTERN));

    ASSERT_EQ(1, aeron_subscription_resolved_endpoint(subscription, reinterpret_cast<char *>(m_buffers[0]), 1024));
    ASSERT_THAT(reinterpret_cast<char *>(m_buffers[0]), testing::ContainsRegex(RESOLVED_IPV6_ADDRESS_PATTERN));

    std::string resolvedEndpointParam = "endpoint=" + std::string(reinterpret_cast<char *>(m_addrs[0].iov_base));
    char uriWithResolvedEndpoint[1024] = { 0 };
    aeron_subscription_try_resolve_channel_endpoint_port(
        subscription, uriWithResolvedEndpoint, sizeof(uriWithResolvedEndpoint));

    ASSERT_THAT(uriWithResolvedEndpoint, testing::HasSubstr(resolvedEndpointParam));

    aeron_subscription_close(subscription, setFlagOnClose, &subscriptionClosedFlag);

    while (!subscriptionClosedFlag)
    {
        std::this_thread::yield();
    }
}

TEST_F(CLocalAddressesTest, shouldGetOriginalAddressWhenNoWildcardSpecified)
{
    std::atomic<bool> subscriptionClosedFlag(false);
    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription;
    const char *uri = "aeron:udp?endpoint=[::1]:12345";

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_subscription(
        &async, m_aeron, uri, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    ASSERT_TRUE((subscription = awaitSubscriptionOrError(async))) << aeron_errmsg();

    char uriWithResolvedEndpoint[1024] = { 0 };

    ASSERT_LT(0, aeron_subscription_try_resolve_channel_endpoint_port(
        subscription, uriWithResolvedEndpoint, sizeof(uriWithResolvedEndpoint)));
    ASSERT_STREQ(uri, uriWithResolvedEndpoint);

    aeron_subscription_close(subscription, setFlagOnClose, &subscriptionClosedFlag);

    while (!subscriptionClosedFlag)
    {
        std::this_thread::yield();
    }
}

TEST_F(CLocalAddressesTest, shouldGetAddressesForMultiDestinationSubscription)
{
    std::atomic<bool> subscriptionClosedFlag(false);
    aeron_async_add_subscription_t *async_sub = nullptr;
    aeron_async_destination_t *async_dest = nullptr;
    aeron_subscription_t *subscription;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, "aeron:udp?control-mode=manual", STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    ASSERT_TRUE((subscription = awaitSubscriptionOrError(async_sub))) << aeron_errmsg();

    ASSERT_EQ(0, aeron_subscription_async_add_destination(
        &async_dest, m_aeron, subscription, "aeron:udp?endpoint=127.0.0.1:9090"));
    ASSERT_EQ(1, awaitSubscriptionDestinationOrError(async_dest)) << aeron_errmsg();

    ASSERT_EQ(0, aeron_subscription_async_add_destination(
        &async_dest, m_aeron, subscription, "aeron:udp?endpoint=127.0.0.1:9091"));
    ASSERT_EQ(1, awaitSubscriptionDestinationOrError(async_dest)) << aeron_errmsg();

    ASSERT_EQ(0, aeron_subscription_async_add_destination(
        &async_dest, m_aeron, subscription, "aeron:udp?endpoint=127.0.0.1:9093"));
    ASSERT_EQ(1, awaitSubscriptionDestinationOrError(async_dest)) << aeron_errmsg();

    ASSERT_EQ(3, aeron_subscription_local_sockaddrs(subscription, m_addrs, NUM_BUFFERS));
    ASSERT_STREQ("127.0.0.1:9090", reinterpret_cast<char *>(m_addrs[0].iov_base));
    ASSERT_STREQ("127.0.0.1:9091", reinterpret_cast<char *>(m_addrs[1].iov_base));
    ASSERT_STREQ("127.0.0.1:9093", reinterpret_cast<char *>(m_addrs[2].iov_base));

    ASSERT_EQ(3, aeron_subscription_local_sockaddrs(subscription, m_addrs, 2));

    aeron_subscription_close(subscription, setFlagOnClose, &subscriptionClosedFlag);

    while (!subscriptionClosedFlag)
    {
        std::this_thread::yield();
    }
}

class CLocalAddressesTestParameterized : public CSystemTestBase, public testing::TestWithParam<std::string>
{
public:
    CLocalAddressesTestParameterized()
    {
        for (int i = 0; i < NUM_BUFFERS; i++)
        {
            m_addrs[i].iov_base = m_buffers[i];
            m_addrs[i].iov_len = sizeof(m_buffers[i]);
        }
    }

protected:
    uint8_t m_buffers[NUM_BUFFERS][AERON_CLIENT_MAX_LOCAL_ADDRESS_STR_LEN] = {};
    aeron_iovec_t m_addrs[NUM_BUFFERS] = {};
};

INSTANTIATE_TEST_SUITE_P(
    CLocalAddressesTestParameterized,
    CLocalAddressesTestParameterized,
    testing::Values("aeron:ipc?so-rcvbuf=32k|alias=test|term-length=64k|mtu=2048", "aeron:udp?endpoint=localhost:8092|alias=test", "aeron:udp?mtu=8|rcv-wnd=64k|control-mode=response", "aeron:udp?control-mode=manual|endpoint=localhost:0"));

TEST_P(CLocalAddressesTestParameterized, shouldGetOriginalChannelWhenNoWildcardSubstitutionToBePerformed)
{
    const std::string& channel = GetParam();
    std::atomic<bool> subscriptionClosedFlag(false);
    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription;

    ASSERT_TRUE(connect());

    ASSERT_EQ(aeron_async_add_subscription(
        &async, m_aeron, channel.c_str(), STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    ASSERT_TRUE((subscription = awaitSubscriptionOrError(async))) << aeron_errmsg();

    char uriWithResolvedEndpoint[1024] = { 0 };

    ASSERT_LT(0, aeron_subscription_try_resolve_channel_endpoint_port(
        subscription, uriWithResolvedEndpoint, sizeof(uriWithResolvedEndpoint)));
    ASSERT_STREQ(channel.c_str(), uriWithResolvedEndpoint);

    aeron_subscription_close(subscription, setFlagOnClose, &subscriptionClosedFlag);

    while (!subscriptionClosedFlag)
    {
        std::this_thread::yield();
    }
}
