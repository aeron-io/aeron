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

extern "C"
{
#include "uri/aeron_uri.h"
#include "util/aeron_netutil.h"
#include "util/aeron_strutil.h"
#include "aeron_driver_context.h"
#include "aeron_driver_conductor.h"
#include "aeron_name_resolver.h"
}

class DriverUriTest : public testing::Test
{
public:
    DriverUriTest()
    {
        if (aeron_driver_context_init(&m_context) < 0)
        {
            throw std::runtime_error("could not init context: " + std::string(aeron_errmsg()));
        }

        m_conductor.context = m_context;
    }

    ~DriverUriTest() override
    {
        aeron_uri_close(&m_uri);
        aeron_driver_context_close(m_context);
    }

protected:
    aeron_uri_t m_uri = {};
    aeron_driver_context_t *m_context = nullptr;
    aeron_driver_conductor_t m_conductor = {};
};

#define AERON_URI_PARSE(uri_str, uri) aeron_uri_parse(strlen(uri_str), uri_str, uri)

TEST_F(DriverUriTest, shouldParseCongestionControlParam)
{
    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|cc=static", &m_uri), 0);
    ASSERT_EQ(m_uri.type, AERON_URI_UDP);
    EXPECT_EQ(std::string(m_uri.params.udp.endpoint), "224.10.9.8");
    EXPECT_EQ(m_uri.params.udp.additional_params.length, 1u);
    EXPECT_EQ(std::string(m_uri.params.udp.additional_params.array[0].key), AERON_URI_CC_KEY);
    EXPECT_EQ(std::string(m_uri.params.udp.additional_params.array[0].value),
        AERON_STATICWINDOWCONGESTIONCONTROL_CC_PARAM_VALUE);
}

TEST_F(DriverUriTest, shouldParseNoPublicationParams)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), 0);

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), 0);
}

TEST_F(DriverUriTest, shouldParsePublicationParamLingerTimeout)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|linger=7777", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), 0);
    EXPECT_EQ(params.linger_timeout_ns, 7777u);
}

TEST_F(DriverUriTest, shouldParsePublicationParamUdpTermLength)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|term-length=131072", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), 0);
    EXPECT_EQ(params.term_length, 131072u);
}

TEST_F(DriverUriTest, shouldParsePublicationParamIpcTermLength)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?term-length=262144", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), 0);
    EXPECT_EQ(params.term_length, 262144u);
}

TEST_F(DriverUriTest, shouldParsePublicationParamIpcTermLengthOddValue)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?term-length=262143", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), -1);
}

TEST_F(DriverUriTest, shouldParsePublicationParamIpcTermLength32K)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?term-length=32768", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), -1);
}

TEST_F(DriverUriTest, shouldParsePublicationParamIpcTermLength2T)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?term-length=2147483648", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), -1);
}

TEST_F(DriverUriTest, shouldParsePublicationParamUdpEndpointAndMtuLength)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|mtu=18432", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), 0);
    EXPECT_EQ(params.mtu_length, 18432u);
}

TEST_F(DriverUriTest, shouldParsePublicationParamIpcMtuLength32K)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?mtu=32768", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), 0);
    EXPECT_EQ(params.mtu_length, 32768u);
}

TEST_F(DriverUriTest, shouldParsePublicationParamIpcMtuLengthNonPowerOf2)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?mtu=66560", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), -1);
}

TEST_F(DriverUriTest, shouldParsePublicationParamIpcMtuLengthLessThanHeaderLength)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?mtu=10", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), -1);
}

TEST_F(DriverUriTest, shouldParsePublicationParamIpcMtuLengthNotMultipleOfFrameAlignment)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?mtu=255", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), -1);
}

TEST_F(DriverUriTest, shouldParsePublicationParamIpcSparse)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?sparse=true", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), 0);
    EXPECT_EQ(params.is_sparse, true);
}

TEST_F(DriverUriTest, shouldParsePublicationParamsForReplayUdp)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|init-term-id=120|term-id=127|term-offset=64", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, true), 0) << aeron_errmsg();
    EXPECT_EQ(params.has_position, true);
    EXPECT_EQ(params.initial_term_id, 120l);
    EXPECT_EQ(params.term_id, 127l);
    EXPECT_EQ(params.term_offset, 64u);
}

TEST_F(DriverUriTest, shouldParsePublicationParamsForReplayIpc)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?init-term-id=250|term-id=257|term-offset=128", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, true), 0) << aeron_errmsg();
    EXPECT_EQ(params.has_position, true);
    EXPECT_EQ(params.initial_term_id, 250l);
    EXPECT_EQ(params.term_id, 257l);
    EXPECT_EQ(params.term_offset, 128u);
}

TEST_F(DriverUriTest, shouldParsePublicationParamsForReplayIpcNegativeTermIds)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?init-term-id=-257|term-id=-250|term-offset=128", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, true), 0) << aeron_errmsg();
    EXPECT_EQ(params.initial_term_id, -257l);
    EXPECT_EQ(params.term_id, -250l);
}

TEST_F(DriverUriTest, shouldParsePublicationParamsForReplayIpcOddTermOffset)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:ipc?init-term-id=-257|term-id=-250|term-offset=127", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, true), -1);
}

TEST_F(DriverUriTest, shouldParsePublicationParamsForReplayIpcTermOffsetBeyondTermLength)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE(
        "aeron:ipc?term-length=65536|init-term-id=-257|term-id=-250|term-offset=65537", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, true), -1);
}

TEST_F(DriverUriTest, shouldErrorParsingTooLargeTermIdRange)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE(
        "aeron:ipc?term-length=65536|init-term-id=-2147483648|term-id=0|term-offset=0", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, true), -1);
    EXPECT_THAT(std::string(aeron_errmsg()), ::testing::HasSubstr("Param difference greater than 2^31 - 1"));
}

TEST_F(DriverUriTest, shouldParsePublicationSessionId)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|session-id=1001", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), 0);
    EXPECT_EQ(params.has_session_id, true);
    EXPECT_EQ(params.session_id, 1001);
}

TEST_F(DriverUriTest, shouldErrorParsingNonNumericSessionId)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|session-id=foobar", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), -1);
}

TEST_F(DriverUriTest, shouldErrorParsingOutOfRangeSessionId)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|session-id=2147483648", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), -1);
}

TEST_F(DriverUriTest, shouldParseSubscriptionParamReliable)
{
    aeron_driver_uri_subscription_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|reliable=false", &m_uri), 0);
    EXPECT_EQ(aeron_driver_uri_subscription_params(&m_uri, &params, &m_conductor), 0);
    EXPECT_EQ(params.is_reliable, false);
}

TEST_F(DriverUriTest, shouldParseSubscriptionParamReliableDefault)
{
    aeron_driver_uri_subscription_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8", &m_uri), 0);
    EXPECT_EQ(aeron_driver_uri_subscription_params(&m_uri, &params, &m_conductor), 0);
    EXPECT_EQ(params.is_reliable, true);
}

TEST_F(DriverUriTest, shouldParseSubscriptionSessionId)
{
    aeron_driver_uri_subscription_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|session-id=1001", &m_uri), 0);
    EXPECT_EQ(aeron_driver_uri_subscription_params(&m_uri, &params, &m_conductor), 0);
    EXPECT_EQ(params.has_session_id, true);
    EXPECT_EQ(params.session_id, 1001);
}

TEST_F(DriverUriTest, shouldGetMediaReceiveTimestampOffset)
{
    aeron_driver_uri_subscription_params_t params = {};

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|session-id=1001|media-rcv-ts-offset=reserved", &m_uri), 0);
    EXPECT_EQ(aeron_driver_uri_subscription_params(&m_uri, &params, &m_conductor), 0);

    int32_t offset = 0;
    ASSERT_NE(-1, aeron_driver_uri_get_timestamp_offset(&m_uri, "media-rcv-ts-offset", &offset));
    EXPECT_EQ(AERON_UDP_CHANNEL_RESERVED_VALUE_OFFSET, offset);
}

TEST_F(DriverUriTest, shouldDefaultMediaReceiveTimestampOffsetToAeronNullValue)
{
    aeron_driver_uri_subscription_params_t params = {};

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|session-id=1001", &m_uri), 0);
    EXPECT_EQ(aeron_driver_uri_subscription_params(&m_uri, &params, &m_conductor), 0);

    int32_t offset = 0;
    ASSERT_NE(-1, aeron_driver_uri_get_timestamp_offset(&m_uri, "media-rcv-ts-offset", &offset));
    EXPECT_EQ(AERON_NULL_VALUE, offset);
}

TEST_F(DriverUriTest, shouldParseAndDefaultResponseCorrelationId)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), 0);
    EXPECT_EQ(INT64_C(-1), params.response_correlation_id);
}

TEST_F(DriverUriTest, shouldNotHaveMaxRetransmits)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), 0);
    EXPECT_FALSE(params.has_max_resend);
}

TEST_F(DriverUriTest, shouldHaveMaxRetransmits)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|max-resend=100", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), 0);
    EXPECT_TRUE(params.has_max_resend);
    EXPECT_EQ(INT64_C(100), params.max_resend);
}

TEST_F(DriverUriTest, shouldFailWithNegativeMaxRetransmits)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|max-resend=-1234", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), -1);
    EXPECT_THAT(std::string(aeron_errmsg()), ::testing::HasSubstr("could not parse max-resend"));
}

TEST_F(DriverUriTest, shouldFailWithZeroMaxRetransmits)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|max-resend=0", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), -1);
    EXPECT_THAT(std::string(aeron_errmsg()), ::testing::HasSubstr("must be > 0"));
}

TEST_F(DriverUriTest, shouldFailWithTooBigMaxRetransmits)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|max-resend=10000", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), -1);
    EXPECT_THAT(std::string(aeron_errmsg()), ::testing::HasSubstr("and <="));
}

TEST_F(DriverUriTest, shouldFailWithInvalidMaxRetransmits)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|max-resend=notanumber", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), -1);
    EXPECT_THAT(std::string(aeron_errmsg()), ::testing::HasSubstr("could not parse max-resend"));
}

TEST_F(DriverUriTest, shouldFailWithPublicationWindowLessThanMtu)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|pub-wnd=2048|mtu=4096", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), -1);
    EXPECT_THAT(std::string(aeron_errmsg()), ::testing::HasSubstr("pub-wnd=2048 cannot be less than the mtu=4096"));
}

TEST_F(DriverUriTest, shouldFailWithPublicationWindowMoreThanHalfTermLength)
{
    aeron_driver_uri_publication_params_t params;

    EXPECT_EQ(AERON_URI_PARSE("aeron:udp?endpoint=224.10.9.8|pub-wnd=262144|term-length=65536", &m_uri), 0);
    EXPECT_EQ(aeron_diver_uri_publication_params(&m_uri, &params, &m_conductor, false), -1);
    EXPECT_THAT(std::string(aeron_errmsg()), ::testing::HasSubstr("pub-wnd=262144 must not exceed half the term-length=65536"));
}

class UriResolverTest : public testing::Test
{
public:
    UriResolverTest() :
        addr_in((struct sockaddr_in *)&m_addr),
        addr_in6((struct sockaddr_in6 *)&m_addr)
    {
        aeron_default_name_resolver_supplier(&m_resolver, nullptr, nullptr);
    }

    static bool ipv4_match(const char *addr1_str, const char *addr2_str, size_t prefixlen)
    {
        struct sockaddr_in addr1{}, addr2{};

        if (inet_pton(AF_INET, addr1_str, &addr1.sin_addr) != 1 || inet_pton(AF_INET, addr2_str, &addr2.sin_addr) != 1)
        {
            throw std::runtime_error("could not convert address");
        }

        return aeron_ipv4_does_prefix_match(&addr1.sin_addr, &addr2.sin_addr, prefixlen);
    }

    static bool ipv6_match(const char *addr1_str, const char *addr2_str, size_t prefixlen)
    {
        struct sockaddr_in6 addr1{}, addr2{};

        if (inet_pton(AF_INET6, addr1_str, &addr1.sin6_addr) != 1 ||
            inet_pton(AF_INET6, addr2_str, &addr2.sin6_addr) != 1)
        {
            throw std::runtime_error("could not convert address");
        }

        return aeron_ipv6_does_prefix_match(&addr1.sin6_addr, &addr2.sin6_addr, prefixlen);
    }

    static size_t ipv6_prefixlen(const char *aadr_str)
    {
        struct sockaddr_in6 addr{};

        if (inet_pton(AF_INET6, aadr_str, &addr.sin6_addr) != 1)
        {
            throw std::runtime_error("could not convert address");
        }

        return aeron_ipv6_netmask_to_prefixlen(&addr.sin6_addr);
    }

    static size_t ipv4_prefixlen(const char *addr_str)
    {
        struct sockaddr_in addr{};

        if (inet_pton(AF_INET, addr_str, &addr.sin_addr) != 1)
        {
            throw std::runtime_error("could not convert address");
        }

        return aeron_ipv4_netmask_to_prefixlen(&addr.sin_addr);
    }

    int resolve_host_and_port(const char *address_str, struct sockaddr_storage *address)
    {
        return aeron_name_resolver_resolve_host_and_port(&m_resolver, address_str, "endpoint", false, address);
    }

protected:
    aeron_uri_t m_uri = {};
    struct sockaddr_storage m_addr = {};
    struct sockaddr_in *addr_in = nullptr;
    struct sockaddr_in6 *addr_in6 = nullptr;
    size_t m_prefixlen = 0;
    aeron_name_resolver_t m_resolver = {};
};

TEST_F(UriResolverTest, shouldResolveIpv4DottedDecimalAndPort)
{
    char buffer[AERON_URI_MAX_LENGTH];

    ASSERT_EQ(resolve_host_and_port("192.168.1.20:55", &m_addr), 0) << aeron_errmsg();
    EXPECT_EQ(m_addr.ss_family, AF_INET);
    EXPECT_EQ(addr_in->sin_family, AF_INET);
    EXPECT_STREQ(inet_ntop(AF_INET, &addr_in->sin_addr, buffer, sizeof(buffer)), "192.168.1.20");
    EXPECT_EQ(addr_in->sin_port, htons(55));
}

TEST_F(UriResolverTest, shouldResolveIpv4MaxPort)
{
    char buffer[AERON_URI_MAX_LENGTH];

    const std::string uri = std::string("127.0.0.1:") + std::to_string(UINT16_MAX);
    ASSERT_EQ(resolve_host_and_port(uri.c_str(), &m_addr), 0) << aeron_errmsg();
    EXPECT_EQ(m_addr.ss_family, AF_INET);
    EXPECT_EQ(addr_in->sin_family, AF_INET);
    EXPECT_STREQ(inet_ntop(AF_INET, &addr_in->sin_addr, buffer, sizeof(buffer)), "127.0.0.1");
    EXPECT_EQ(addr_in->sin_port, htons(UINT16_MAX));
}

TEST_F(UriResolverTest, shouldResolveIpv4MulticastDottedDecimalAndPort)
{
    ASSERT_EQ(resolve_host_and_port("223.255.255.255:1234", &m_addr), 0) << aeron_errmsg();
    ASSERT_EQ(resolve_host_and_port("224.0.0.0:1234", &m_addr), 0) << aeron_errmsg();
    ASSERT_EQ(resolve_host_and_port("239.255.255.255:1234", &m_addr), 0) << aeron_errmsg();
    ASSERT_EQ(resolve_host_and_port("240.0.0.0:1234", &m_addr), 0) << aeron_errmsg();
}

TEST_F(UriResolverTest, shouldResolveIpv6AndPort)
{
    char buffer[AERON_URI_MAX_LENGTH];

    ASSERT_EQ(resolve_host_and_port("[::1]:1234", &m_addr), 0) << aeron_errmsg();
    EXPECT_EQ(m_addr.ss_family, AF_INET6);
    EXPECT_EQ(addr_in6->sin6_family, AF_INET6);
    EXPECT_STREQ(inet_ntop(AF_INET6, &addr_in6->sin6_addr, buffer, sizeof(buffer)), "::1");
    EXPECT_EQ(addr_in->sin_port, htons(1234));

    ASSERT_EQ(resolve_host_and_port("[::1%eth0]:1234", &m_addr), 0) << aeron_errmsg();
    EXPECT_EQ(m_addr.ss_family, AF_INET6);
    EXPECT_EQ(addr_in6->sin6_family, AF_INET6);
    EXPECT_STREQ(inet_ntop(AF_INET6, &addr_in6->sin6_addr, buffer, sizeof(buffer)), "::1");
    EXPECT_EQ(addr_in->sin_port, htons(1234));

    ASSERT_EQ(resolve_host_and_port("[::1%12~_.-34]:1234", &m_addr), 0) << aeron_errmsg();
}

TEST_F(UriResolverTest, shouldResolveIpv6MulticastAndPort)
{
    ASSERT_EQ(resolve_host_and_port(
        "[FEFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF]:1234", &m_addr), 0) << aeron_errmsg();
    ASSERT_EQ(resolve_host_and_port("[FF00::]:1234", &m_addr), 0) << aeron_errmsg();
    ASSERT_EQ(resolve_host_and_port(
        "[FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF]:1234", &m_addr), 0) << aeron_errmsg();
}

TEST_F(UriResolverTest, shouldResolveLocalhost)
{
    char buffer[AERON_URI_MAX_LENGTH];

    ASSERT_EQ(resolve_host_and_port("localhost:1234", &m_addr), 0) << aeron_errmsg();
    EXPECT_EQ(m_addr.ss_family, AF_INET);
    EXPECT_EQ(addr_in->sin_family, AF_INET);
    EXPECT_STREQ(inet_ntop(AF_INET, &addr_in->sin_addr, buffer, sizeof(buffer)), "127.0.0.1");
    EXPECT_EQ(addr_in->sin_port, htons(1234));
}

TEST_F(UriResolverTest, shouldNotResolveInvalidPort)
{
    EXPECT_EQ(resolve_host_and_port("192.168.1.20:aa", &m_addr), -1);

    // Regex is ? for port so it's not mandatory
    EXPECT_EQ(resolve_host_and_port("192.168.1.20", &m_addr), -1);

    EXPECT_EQ(resolve_host_and_port("192.168.1.20:", &m_addr), -1);
    EXPECT_EQ(resolve_host_and_port("[::1]:aa", &m_addr), -1);

    // Regex is ? for port so it's not mandatory
    EXPECT_EQ(resolve_host_and_port("[::1]", &m_addr), -1);

    EXPECT_EQ(resolve_host_and_port("[::1]:", &m_addr), -1);
}

TEST_F(UriResolverTest, shouldNotResolvePortBeyoundMax)
{
    const std::string uri = std::string("127.0.0.1:") + std::to_string(UINT16_MAX + 1);
    EXPECT_EQ(resolve_host_and_port(uri.c_str(), &m_addr), -1);
}

TEST_F(UriResolverTest, shouldResolveIpv4Interface)
{
    char buffer[AERON_URI_MAX_LENGTH];

    ASSERT_EQ(aeron_interface_parse_and_resolve("192.168.1.20", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 32u);
    EXPECT_EQ(m_addr.ss_family, AF_INET);
    EXPECT_EQ(addr_in->sin_family, AF_INET);
    EXPECT_STREQ(inet_ntop(AF_INET, &addr_in->sin_addr, buffer, sizeof(buffer)), "192.168.1.20");

    ASSERT_EQ(aeron_interface_parse_and_resolve("192.168.1.20/24", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 24u);

    ASSERT_EQ(aeron_interface_parse_and_resolve("192.168.1.20:1234", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 32u);
    EXPECT_EQ(addr_in->sin_port, htons(1234));

    ASSERT_EQ(aeron_interface_parse_and_resolve("192.168.1.20:1234/24", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 24u);
    EXPECT_EQ(addr_in->sin_port, htons(1234));

    ASSERT_EQ(aeron_interface_parse_and_resolve("0.0.0.0/0", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 0u);
    EXPECT_EQ(m_addr.ss_family, AF_INET);
    EXPECT_EQ(addr_in->sin_family, AF_INET);
    EXPECT_STREQ(inet_ntop(AF_INET, &addr_in->sin_addr, buffer, sizeof(buffer)), "0.0.0.0");

    ASSERT_EQ(aeron_interface_parse_and_resolve("127.0.0.1", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 32u);
    EXPECT_EQ(m_addr.ss_family, AF_INET);
    EXPECT_EQ(addr_in->sin_family, AF_INET);
    EXPECT_STREQ(inet_ntop(AF_INET, &addr_in->sin_addr, buffer, sizeof(buffer)), "127.0.0.1");

    ASSERT_EQ(aeron_interface_parse_and_resolve("127.0.0.3/8", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 8u);
    EXPECT_EQ(m_addr.ss_family, AF_INET);
    EXPECT_EQ(addr_in->sin_family, AF_INET);
    EXPECT_STREQ(inet_ntop(AF_INET, &addr_in->sin_addr, buffer, sizeof(buffer)), "127.0.0.3");
}

TEST_F(UriResolverTest, shouldResolveHostNameInTheInterfaceSpecification)
{
    char buffer[AERON_URI_MAX_LENGTH];

    ASSERT_EQ(aeron_interface_parse_and_resolve("localhost:5551", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 32u);
    EXPECT_EQ(m_addr.ss_family, AF_INET);
    EXPECT_EQ(addr_in->sin_family, AF_INET);
    EXPECT_EQ(addr_in->sin_port, htons(5551));
    EXPECT_STREQ(inet_ntop(AF_INET, &addr_in->sin_addr, buffer, sizeof(buffer)), "127.0.0.1");

    ASSERT_EQ(aeron_interface_parse_and_resolve("localhost:0/24", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 24u);
    EXPECT_EQ(m_addr.ss_family, AF_INET);
    EXPECT_EQ(addr_in->sin_family, AF_INET);
    EXPECT_EQ(addr_in->sin_port, htons(0));
    EXPECT_STREQ(inet_ntop(AF_INET, &addr_in->sin_addr, buffer, sizeof(buffer)), "127.0.0.1");

    EXPECT_EQ(0, gethostname(buffer, sizeof(buffer)));
    std::string host_name = std::string(buffer).append(":8989/16");
    ASSERT_EQ(aeron_interface_parse_and_resolve(host_name.c_str(), &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 16u);
    EXPECT_EQ(m_addr.ss_family, AF_INET);
    EXPECT_EQ(addr_in->sin_family, AF_INET);
    EXPECT_EQ(addr_in->sin_port, htons(8989));
}

TEST_F(UriResolverTest, shouldResolveIpv6Interface)
{
    char buffer[AERON_URI_MAX_LENGTH];

    ASSERT_EQ(aeron_interface_parse_and_resolve("[::1]", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 128u);
    EXPECT_EQ(m_addr.ss_family, AF_INET6);
    EXPECT_EQ(addr_in->sin_family, AF_INET6);
    EXPECT_STREQ(inet_ntop(AF_INET6, &addr_in6->sin6_addr, buffer, sizeof(buffer)), "::1");

    ASSERT_EQ(aeron_interface_parse_and_resolve("[::1]/48", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 48u);

    ASSERT_EQ(aeron_interface_parse_and_resolve("[::1]:1234", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 128u);
    EXPECT_EQ(addr_in6->sin6_port, htons(1234));

    ASSERT_EQ(aeron_interface_parse_and_resolve("[::1]:1234/48", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 48u);
    EXPECT_EQ(addr_in6->sin6_port, htons(1234));

    ASSERT_EQ(aeron_interface_parse_and_resolve("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:1111", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 128u);
    EXPECT_EQ(addr_in6->sin6_port, htons(1111));
    EXPECT_STREQ(inet_ntop(AF_INET6, &addr_in6->sin6_addr, buffer, sizeof(buffer)), "2001:db8:85a3::8a2e:370:7334");

    ASSERT_EQ(aeron_interface_parse_and_resolve("[fe80:3::1ff:fe23:4567:890a]/24", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 24u);
    EXPECT_EQ(addr_in6->sin6_port, htons(0));
    EXPECT_STREQ(inet_ntop(AF_INET6, &addr_in6->sin6_addr, buffer, sizeof(buffer)), "fe80:3::1ff:fe23:4567:890a");

    ASSERT_EQ(aeron_interface_parse_and_resolve("[64:ff9b::]/96", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 96u);
    EXPECT_EQ(addr_in6->sin6_port, htons(0));
    EXPECT_STREQ(inet_ntop(AF_INET6, &addr_in6->sin6_addr, buffer, sizeof(buffer)), "64:ff9b::");

    ASSERT_EQ(aeron_interface_parse_and_resolve("[ff01::1]:22222", &m_addr, &m_prefixlen), 0) << aeron_errmsg();
    EXPECT_EQ(m_prefixlen, 128u);
    EXPECT_EQ(addr_in6->sin6_port, htons(22222));
    EXPECT_STREQ(inet_ntop(AF_INET6, &addr_in6->sin6_addr, buffer, sizeof(buffer)), "ff01::1");
    EXPECT_TRUE(aeron_is_addr_multicast(&m_addr));
}

TEST_F(UriResolverTest, shouldMatchIpv4)
{
    EXPECT_TRUE(ipv4_match("127.0.0.0", "127.0.0.0", 24));
    EXPECT_TRUE(ipv4_match("127.0.0.0", "127.0.0.1", 24));
    EXPECT_TRUE(ipv4_match("127.0.0.0", "127.0.0.255", 24));
    EXPECT_TRUE(ipv4_match("127.1.1.1", "127.0.0.0", 8));
    EXPECT_TRUE(ipv4_match("127.255.1.1", "127.255.255.255", 16));
    EXPECT_TRUE(ipv4_match("127.255.254.1", "127.255.254.255", 24));
    EXPECT_TRUE(ipv4_match("127.255.254.16", "127.255.254.16", 32));
}

TEST_F(UriResolverTest, shouldNotMatchIpv4)
{
    EXPECT_FALSE(ipv4_match("127.0.1.0", "127.0.0.1", 24));
    EXPECT_FALSE(ipv4_match("127.0.0.1", "127.0.0.2", 32));
    EXPECT_FALSE(ipv4_match("127.0.0.1", "126.0.0.2", 8));
}

TEST_F(UriResolverTest, shouldMatchIpv6)
{
    EXPECT_TRUE(ipv6_match("fe80:0001:abcd::", "fe80:0001:abcd::1", 48));
    EXPECT_TRUE(ipv6_match("fe80:0001:abcd::", "fe80:0001:abcd::ff", 48));
    EXPECT_TRUE(ipv6_match("fe80:0001:abcd::", "fe80:0001:abcd::", 48));
    EXPECT_TRUE(ipv6_match("fe80:0001:abcd::1", "fe80:0001:abcd::1", 128));
}

TEST_F(UriResolverTest, shouldNotMatchIpv6)
{
    EXPECT_FALSE(ipv6_match("fe80:0001:abcd::", "fe80:0001:abce::", 48));
    EXPECT_FALSE(ipv6_match("fe80:0001:abcf::", "fe80:0001:abce::", 48));
    EXPECT_FALSE(ipv6_match("fe80:0001:abcd::", "fe80:0001:abcd::1", 128));
    EXPECT_FALSE(ipv6_match("fe80:0001:abcd::", "fe80:0001:abcd::ff", 128));
}

TEST_F(UriResolverTest, shouldCalculateIpv4PrefixlenFromNetmask)
{
    EXPECT_EQ(ipv4_prefixlen("255.255.255.0"), 24u);
    EXPECT_EQ(ipv4_prefixlen("255.255.255.255"), 32u);
    EXPECT_EQ(ipv4_prefixlen("255.255.128.0"), 17u);
    EXPECT_EQ(ipv4_prefixlen("255.255.0.0"), 16u);
    EXPECT_EQ(ipv4_prefixlen("255.0.0.0"), 8u);
    EXPECT_EQ(ipv4_prefixlen("255.240.0.0"), 12u);
    EXPECT_EQ(ipv4_prefixlen("0.0.0.0"), 0u);
}

TEST_F(UriResolverTest, shouldCalculateIpv6PrefixlenFromNetmask)
{
    EXPECT_EQ(ipv6_prefixlen("FFFF:FFFF:FFFF:FFFF::"), 64u);
    EXPECT_EQ(ipv6_prefixlen("FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF"), 128u);
    EXPECT_EQ(ipv6_prefixlen("FFFF:FFFF:FFF0::"), 44u);
    EXPECT_EQ(ipv6_prefixlen("FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FF00::"), 104u);
    EXPECT_EQ(ipv6_prefixlen("FFFF:FF80::"), 25u);
    EXPECT_EQ(ipv6_prefixlen("0000:0000:0000:0000:0000:0000:0000:0000"), 0u);
}
