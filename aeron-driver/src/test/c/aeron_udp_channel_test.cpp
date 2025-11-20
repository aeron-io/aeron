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

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <regex>

extern "C"
{
#include "media/aeron_udp_channel.h"
#include "util/aeron_env.h"
}

class UdpChannelTestBase
{
public:
    UdpChannelTestBase()
    {
        aeron_default_name_resolver_supplier(&m_resolver, nullptr, nullptr);
    }

    virtual ~UdpChannelTestBase()
    {
        aeron_udp_channel_delete(m_channel);
        m_resolver.close_func(&m_resolver);
    }

    static struct sockaddr_in *ipv4_addr(struct sockaddr_storage *addr)
    {
        return (struct sockaddr_in *)addr;
    }

    static struct sockaddr_in6 *ipv6_addr(struct sockaddr_storage *addr)
    {
        return (struct sockaddr_in6 *)addr;
    }

    const char *inet_ntop(struct sockaddr_storage *addr)
    {
        if (AF_INET == addr->ss_family)
        {
            return ::inet_ntop(AF_INET, &(ipv4_addr(addr)->sin_addr), m_buffer, sizeof(m_buffer));
        }
        else if (AF_INET6 == addr->ss_family)
        {
            return ::inet_ntop(AF_INET6, &(ipv6_addr(addr)->sin6_addr), m_buffer, sizeof(m_buffer));
        }

        return nullptr;
    }

    static int port(struct sockaddr_storage *addr)
    {
        if (AF_INET == addr->ss_family)
        {
            return ntohs(ipv4_addr(addr)->sin_port);
        }
        else if (AF_INET6 == addr->ss_family)
        {
            return ntohs(ipv6_addr(addr)->sin6_port);
        }

        return 0;
    }

    int parse_udp_channel(const char *uri)
    {
        aeron_udp_channel_delete(m_channel);
        m_channel = nullptr;

        return aeron_udp_channel_parse(strlen(uri), uri, &m_resolver, &m_channel, false);
    }

    static void assert_error_message_contains(const char* text)
    {
        auto error = std::string(aeron_errmsg());
        EXPECT_NE(0, error.length());
        EXPECT_NE(std::string::npos, error.find(text));
    }

protected:
    char m_buffer[AERON_NETUTIL_FORMATTED_MAX_LENGTH] = {};
    aeron_udp_channel_t *m_channel = nullptr;
    aeron_name_resolver_t m_resolver = {};
};

class UdpChannelTest : public UdpChannelTestBase, public testing::Test
{
protected:
    void TearDown() override
    {
        Test::TearDown();
        aeron_env_unset(AERON_NAME_RESOLVER_CSV_TABLE_ARGS_ENV_VAR);
    }
};

class UdpChannelNamesParameterisedTest :
    public testing::TestWithParam<std::tuple<const char *, const char *, const char *, const char *, const char *>>,
    public UdpChannelTestBase
{
};

class UdpChannelEqualityParameterisedTest :
    public testing::TestWithParam<std::tuple<bool, const char *, const char *>>,
    public UdpChannelTestBase
{
};


TEST_F(UdpChannelTest, shouldComputeMaxMessageLength)
{
    EXPECT_EQ(0, aeron_compute_max_message_length(0));
    EXPECT_EQ(4, aeron_compute_max_message_length(32));
    EXPECT_EQ(8192, aeron_compute_max_message_length(64 * 1024));
    EXPECT_EQ(8 * 1024 * 1024, aeron_compute_max_message_length(64 * 1024 * 1024));
    EXPECT_EQ(AERON_FRAME_MAX_MESSAGE_LENGTH, aeron_compute_max_message_length(1024 * 1024 * 1024));
    EXPECT_EQ(16 * 1024 * 1024, aeron_compute_max_message_length(512 * 1024 * 1024));
}

TEST_F(UdpChannelTest, shouldCheckIfFrameIsValid)
{
    aeron_frame_header_t header = {};

    EXPECT_FALSE(aeron_is_frame_valid(&header, 1)); // length below min header length

    header.version = 123;
    EXPECT_FALSE(aeron_is_frame_valid(&header, AERON_FRAME_HEADER_LENGTH)); // wrong version

    header.version = AERON_FRAME_HEADER_VERSION;
    header.frame_length = 100;
    EXPECT_TRUE(aeron_is_frame_valid(&header, AERON_FRAME_HEADER_LENGTH + 1));
    header.type = 123;
    EXPECT_TRUE(aeron_is_frame_valid(&header, header.frame_length));
    header.flags = 0xAE;
    EXPECT_TRUE(aeron_is_frame_valid(&header, INT32_MAX));
}

TEST_F(UdpChannelTest, shouldParseExplicitLocalAddressAndPortFormat)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=localhost:40123|endpoint=localhost:40124"), 0) << aeron_errmsg();

    EXPECT_EQ(m_channel->local_data.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->local_data), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->local_data), 40123);

    EXPECT_EQ(m_channel->local_control.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->local_control), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->local_control), 40123);

    EXPECT_EQ(m_channel->remote_data.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_data), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->remote_data), 40124);

    EXPECT_EQ(m_channel->remote_control.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_control), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->remote_control), 40124);
}

TEST_F(UdpChannelTest, shouldParseImpliedLocalAddressAndPortFormat)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?endpoint=localhost:40124"), 0) << aeron_errmsg();

    EXPECT_EQ(m_channel->local_data.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->local_data), "0.0.0.0");
    EXPECT_EQ(port(&m_channel->local_data), 0);

    EXPECT_EQ(m_channel->local_control.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->local_control), "0.0.0.0");
    EXPECT_EQ(port(&m_channel->local_control), 0);

    EXPECT_EQ(m_channel->remote_data.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_data), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->remote_data), 40124);

    EXPECT_EQ(m_channel->remote_control.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_control), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->remote_control), 40124);
}

TEST_F(UdpChannelTest, shouldErrorForIncorrectScheme)
{
    ASSERT_EQ(parse_udp_channel("unknownudp://localhost:40124"), -1);
}

TEST_F(UdpChannelTest, shouldErrorForMissingAddressWithAeronUri)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp"), -1);
}

TEST_F(UdpChannelTest, shouldErrorOnEvenMulticastAddress)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?endpoint=224.10.9.8"), -1);
}

TEST_F(UdpChannelTest, shouldParseValidMulticastAddress)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=localhost|endpoint=224.10.9.9:40124"), 0) << aeron_errmsg();

    EXPECT_EQ(m_channel->local_data.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->local_data), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->local_data), 0);

    EXPECT_EQ(m_channel->local_control.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->local_control), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->local_control), 0);

    EXPECT_EQ(m_channel->remote_data.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_data), "224.10.9.9");
    EXPECT_EQ(port(&m_channel->remote_data), 40124);

    EXPECT_EQ(m_channel->remote_control.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_control), "224.10.9.10");
    EXPECT_EQ(port(&m_channel->remote_control), 40124);
}

TEST_F(UdpChannelTest, shouldParseImpliedLocalPortFormat)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=localhost|endpoint=localhost:40124"), 0) << aeron_errmsg();

    EXPECT_EQ(m_channel->local_data.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->local_data), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->local_data), 0);

    EXPECT_EQ(m_channel->local_control.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->local_control), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->local_control), 0);

    EXPECT_EQ(m_channel->remote_data.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_data), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->remote_data), 40124);

    EXPECT_EQ(m_channel->remote_control.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_control), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->remote_control), 40124);
}

TEST_F(UdpChannelTest, shouldParseIpv4AnyAddressAsInterfaceAddressForUnicast)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=0.0.0.0|endpoint=localhost:40124"), 0) << aeron_errmsg();

    EXPECT_EQ(m_channel->local_data.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->local_data), "0.0.0.0");
    EXPECT_EQ(port(&m_channel->local_data), 0);

    EXPECT_EQ(m_channel->local_control.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->local_control), "0.0.0.0");
    EXPECT_EQ(port(&m_channel->local_control), 0);

    EXPECT_EQ(m_channel->remote_data.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_data), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->remote_data), 40124);

    EXPECT_EQ(m_channel->remote_control.ss_family, AF_INET);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_control), "127.0.0.1");
    EXPECT_EQ(port(&m_channel->remote_control), 40124);
}

TEST_F(UdpChannelTest, shouldParseIpv6AnyAddressAsInterfaceAddressForUnicast)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=[::]|endpoint=[::1]:40124"), 0) << aeron_errmsg();

    EXPECT_EQ(m_channel->local_data.ss_family, AF_INET6);
    EXPECT_STREQ(inet_ntop(&m_channel->local_data), "::");
    EXPECT_EQ(port(&m_channel->local_data), 0);

    EXPECT_EQ(m_channel->local_control.ss_family, AF_INET6);
    EXPECT_STREQ(inet_ntop(&m_channel->local_control), "::");
    EXPECT_EQ(port(&m_channel->local_control), 0);

    EXPECT_EQ(m_channel->remote_data.ss_family, AF_INET6);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_data), "::1");
    EXPECT_EQ(port(&m_channel->remote_data), 40124);

    EXPECT_EQ(m_channel->remote_control.ss_family, AF_INET6);
    EXPECT_STREQ(inet_ntop(&m_channel->remote_control), "::1");
    EXPECT_EQ(port(&m_channel->remote_control), 40124);
}

TEST_F(UdpChannelTest, shouldCanonicalizeIpv4ForUnicast)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?endpoint=192.168.0.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-0.0.0.0:0-192.168.0.1:40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.1|endpoint=192.168.0.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-127.0.0.1:0-192.168.0.1:40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.1:40455|endpoint=192.168.0.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-127.0.0.1:40455-192.168.0.1:40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=localhost|endpoint=localhost:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-127.0.0.1:0-localhost:40456");
}

TEST_F(UdpChannelTest, shouldCanonicalizeIpv6ForUnicastWithMixedAddressTypes)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.1:40455|endpoint=[fe80::5246:5dff:fe73:df06]:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-127.0.0.1:40455-[fe80::5246:5dff:fe73:df06]:40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?endpoint=192.168.0.1:40456|interface=[::1]"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-[::1]:0-192.168.0.1:40456");
}

TEST_F(UdpChannelTest, shouldNotDoubleFreeParsedUdpChannel)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.1:40455|endpoint=224.0.1.1:40456"), 0) << aeron_errmsg();
    ASSERT_NE(m_channel, nullptr);

    ASSERT_EQ(parse_udp_channel("this should fail"), -1) << aeron_errmsg();
    ASSERT_EQ(parse_udp_channel("and this as well"), -1) << aeron_errmsg();
    ASSERT_EQ(m_channel, nullptr); // previous channel was removed
}

MATCHER_P(ReMatch, pattern, std::string("ReMatch(").append(pattern).append(")"))
{
    return std::regex_match(arg, std::regex(pattern));
}

TEST_F(UdpChannelTest, shouldCanonicalizeForIpv4Multicast)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.1:40455|endpoint=224.0.1.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ("UDP-127.0.0.1:40455-224.0.1.1:40456", m_channel->canonical_form);

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=localhost|endpoint=224.0.1.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ("UDP-127.0.0.1:0-224.0.1.1:40456", m_channel->canonical_form);

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.1|endpoint=224.0.1.1:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ("UDP-127.0.0.1:0-224.0.1.1:40456", m_channel->canonical_form);

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.0:40455/29|endpoint=224.0.1.1:40456"), 0) << aeron_errmsg();
    EXPECT_THAT((char *)m_channel->canonical_form, ReMatch("UDP-127\\.0\\.0\\.[1-7]:40455-224\\.0\\.1\\.1:40456"));

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=localhost/29|endpoint=224.0.1.1:40456"), 0) << aeron_errmsg();
    EXPECT_THAT((char *)m_channel->canonical_form, ReMatch("UDP-127\\.0\\.0\\.[1-7]:0-224\\.0\\.1\\.1:40456"));

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=127.0.0.0/29|endpoint=224.0.1.1:40456"), 0) << aeron_errmsg();
    EXPECT_THAT((char *)m_channel->canonical_form, ReMatch("UDP-127\\.0\\.0\\.[1-7]:0-224\\.0\\.1\\.1:40456"));
}

TEST_F(UdpChannelTest, shouldCanonicalizeForIpv6Multicast)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=[::1]|endpoint=[FF01::FD]:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-[::1]:0-[ff01::fd]:40456");

    ASSERT_EQ(parse_udp_channel("aeron:udp?interface=[::1]:54321/64|endpoint=[FF01::FD]:40456"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-[::1]:54321-[ff01::fd]:40456");
}

TEST_F(UdpChannelTest, shouldUseTagInCanonicalFormIfWildcardsInUseIPv6)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?tags=1001|endpoint=[::1]:0"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-[::]:0-[::1]:0#1001");

    ASSERT_EQ(parse_udp_channel("aeron:udp?tags=1001|endpoint=[::1]:9999|control=[::1]:0"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-[::1]:0-[::1]:9999#1001");
}

TEST_F(UdpChannelTest, shouldUseTagInCanonicalFormIfWildcardsInUseIPv4)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?tags=1001|endpoint=127.0.0.1:0"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-0.0.0.0:0-127.0.0.1:0#1001");

    ASSERT_EQ(parse_udp_channel("aeron:udp?tags=1001|endpoint=127.0.0.1:9999|control=127.0.0.1:0"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->canonical_form, "UDP-127.0.0.1:0-127.0.0.1:9999#1001");
}

TEST_F(UdpChannelTest, shouldUseUniqueIdInCanonicalFormIfWildcardsInUseIPv6)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?endpoint=[::1]:0"), 0) << aeron_errmsg();
    EXPECT_EQ(0u, std::string(m_channel->canonical_form).rfind("UDP-[::]:0-[::1]:0-"));

    ASSERT_EQ(parse_udp_channel("aeron:udp?endpoint=[::1]:9999|control=[::1]:0"), 0) << aeron_errmsg();
    EXPECT_EQ(0u, std::string(m_channel->canonical_form).rfind("UDP-[::1]:0-[::1]:9999-"));
}

TEST_F(UdpChannelTest, shouldUseUniqueIdInCanonicalFormIfWildcardsInUseIPv4)
{
    ASSERT_EQ(parse_udp_channel("aeron:udp?endpoint=127.0.0.1:0"), 0) << aeron_errmsg();
    EXPECT_EQ(0u, std::string(m_channel->canonical_form).rfind("UDP-0.0.0.0:0-127.0.0.1:0-"));

    ASSERT_EQ(parse_udp_channel("aeron:udp?endpoint=127.0.0.1:9999|control=127.0.0.1:0"), 0) << aeron_errmsg();
    EXPECT_EQ(0u, std::string(m_channel->canonical_form).rfind("UDP-127.0.0.1:0-127.0.0.1:9999-"));
}

TEST_F(UdpChannelTest, DISABLED_shouldResolveWithNameLookup)
{
    const char *config_param =
        "NAME_0," AERON_UDP_CHANNEL_ENDPOINT_KEY ",localhost:9001,localhost:9001|"
        "NAME_1," AERON_UDP_CHANNEL_CONTROL_KEY ",localhost:9002,localhost:9002|";

    aeron_name_resolver_supplier_func_t csv_supplier_func = aeron_name_resolver_supplier_load(
        AERON_NAME_RESOLVER_CSV_TABLE);
    csv_supplier_func(&m_resolver, config_param, nullptr);

    ASSERT_EQ(parse_udp_channel("aeron:udp?endpoint=NAME_0|control=NAME_1"), 0) << aeron_errmsg();
    EXPECT_STREQ(m_channel->uri.params.udp.endpoint, "NAME_0");
    EXPECT_STREQ(m_channel->uri.params.udp.control, "NAME_1");
}

TEST_F(UdpChannelTest, shouldFormatIPv4Address)
{
    char buffer[AERON_NETUTIL_FORMATTED_MAX_LENGTH];
    struct sockaddr_storage addr{};
    auto *addr_in = reinterpret_cast<sockaddr_in *>(&addr);
    inet_pton(AF_INET, "192.168.10.1", &addr_in->sin_addr);
    addr_in->sin_family = AF_INET;
    addr_in->sin_port = htons(UINT16_C(65535));

    aeron_format_source_identity(buffer, sizeof(buffer), &addr);

    ASSERT_STREQ("192.168.10.1:65535", buffer);
}

TEST_F(UdpChannelTest, shouldFormatIPv6Address)
{
    char buffer[AERON_NETUTIL_FORMATTED_MAX_LENGTH];
    struct sockaddr_storage addr{};
    auto *addr_in = reinterpret_cast<sockaddr_in6 *>(&addr);
    inet_pton(AF_INET6, "::1", &addr_in->sin6_addr);
    addr_in->sin6_family = AF_INET6;
    addr_in->sin6_port = htons(UINT16_C(65535));

    aeron_format_source_identity(buffer, sizeof(buffer), &addr);

    ASSERT_STREQ("[::1]:65535", buffer);
}

TEST_F(UdpChannelTest, shouldHandleMaxLengthIPv6)
{
    char buffer[AERON_NETUTIL_FORMATTED_MAX_LENGTH];
    struct sockaddr_storage addr{};
    auto *addr_in = reinterpret_cast<sockaddr_in6 *>(&addr);
    inet_pton(AF_INET6, "ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255", &addr_in->sin6_addr);
    addr_in->sin6_family = AF_INET6;
    addr_in->sin6_port = htons(UINT16_C(65535));

    aeron_format_source_identity(buffer, sizeof(buffer), &addr);

    ASSERT_STREQ("[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535", buffer);
}

TEST_F(UdpChannelTest, shouldHandleTooSmallBuffer)
{
    char buffer[AERON_NETUTIL_FORMATTED_MAX_LENGTH];
    struct sockaddr_storage addr{};
    auto *addr_in = reinterpret_cast<sockaddr_in6 *>(&addr);
    inet_pton(AF_INET6, "ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255", &addr_in->sin6_addr);
    addr_in->sin6_family = AF_INET6;
    addr_in->sin6_port = UINT16_C(65535);

    ASSERT_LE(aeron_format_source_identity(buffer, AERON_NETUTIL_FORMATTED_MAX_LENGTH - 1, &addr), 0);
}

TEST_F(UdpChannelTest, shouldParseSocketBufferParameters)
{
    const char *uri = "aeron:udp?interface=localhost|endpoint=224.10.9.9:40124|so-sndbuf=8k|so-rcvbuf=4k";
    ASSERT_EQ(parse_udp_channel(uri), 0) << aeron_errmsg();

    ASSERT_EQ(8192u, m_channel->socket_sndbuf_length);
    ASSERT_EQ(4096u, m_channel->socket_rcvbuf_length);
}

TEST_F(UdpChannelTest, shouldParseReceiverWindow)
{
    const char *uri = "aeron:udp?interface=localhost|endpoint=224.10.9.9:40124|rcv-wnd=8k";
    ASSERT_EQ(parse_udp_channel(uri), 0) << aeron_errmsg();

    ASSERT_EQ(8192u, m_channel->receiver_window_length);
}

TEST_F(UdpChannelTest, shouldParseTimestampOffsets)
{
    const char *uri = "aeron:udp?endpoint=localhost:0|media-rcv-ts-offset=reserved|channel-snd-ts-offset=0|channel-rcv-ts-offset=8";
    ASSERT_EQ(0, parse_udp_channel(uri)) << aeron_errmsg();

    EXPECT_EQ(AERON_UDP_CHANNEL_RESERVED_VALUE_OFFSET, m_channel->media_rcv_timestamp_offset);
    EXPECT_EQ(0, m_channel->channel_snd_timestamp_offset);
    EXPECT_EQ(8, m_channel->channel_rcv_timestamp_offset);
}

TEST_F(UdpChannelTest, shouldDefaultTimestampOffsetsToMinusOne)
{
    const char *uri = "aeron:udp?endpoint=localhost:0";
    ASSERT_EQ(0, parse_udp_channel(uri)) << aeron_errmsg();

    EXPECT_EQ(AERON_NULL_VALUE, m_channel->media_rcv_timestamp_offset);
    EXPECT_EQ(AERON_NULL_VALUE, m_channel->channel_rcv_timestamp_offset);
    EXPECT_EQ(AERON_NULL_VALUE, m_channel->channel_snd_timestamp_offset);
}

TEST_F(UdpChannelTest, shouldValidateOffsetsDoNotOverlap)
{
    EXPECT_EQ(-1, parse_udp_channel("aeron:udp?endpoint=localhost:0|media-rcv-ts-offset=2|channel-rcv-ts-offset=9")) << aeron_errmsg();
    assert_error_message_contains("media-rcv-ts-offset and channel-rcv-ts-offset overlap");

    EXPECT_EQ(-1, parse_udp_channel("aeron:udp?endpoint=localhost:0|media-rcv-ts-offset=14|channel-snd-ts-offset=19")) << aeron_errmsg();
    assert_error_message_contains("media-rcv-ts-offset and channel-snd-ts-offset overlap");

    EXPECT_EQ(-1, parse_udp_channel("aeron:udp?endpoint=localhost:0|media-rcv-ts-offset=reserved|channel-snd-ts-offset=3|channel-rcv-ts-offset=10"));
    assert_error_message_contains("channel-rcv-ts-offset and channel-snd-ts-offset overlap");
}

TEST_P(UdpChannelNamesParameterisedTest, DISABLED_shouldBeValid)
{
    const char *endpoint_name = std::get<0>(GetParam());
    const char *endpoint_address = std::get<1>(GetParam());
    const char *control_name = std::get<2>(GetParam());
    const char *control_address = std::get<3>(GetParam());
    const char *canonical_form = std::get<4>(GetParam());
    std::stringstream params_ss;
    std::stringstream uri_ss;

    if (nullptr != endpoint_name)
    {
        params_ss << endpoint_name << ',' <<
            AERON_UDP_CHANNEL_ENDPOINT_KEY << ',' <<
            endpoint_address << ":40124" << ',' <<
            endpoint_address << ":40124" << '|';
    }

    if (nullptr != control_name)
    {
        params_ss << control_name << ',' <<
            AERON_UDP_CHANNEL_CONTROL_KEY << ',' <<
            control_address << ":40124" << ',' <<
            control_address << ":40124" << '|';
    }

    const std::string params_string = params_ss.str();
    const char *config_params = params_string.c_str();

    aeron_name_resolver_supplier_func_t csv_supplier_func = aeron_name_resolver_supplier_load(
        AERON_NAME_RESOLVER_CSV_TABLE);
    csv_supplier_func(&m_resolver, config_params, nullptr);

    uri_ss << "aeron:udp?interface=localhost";

    if (nullptr != endpoint_name)
    {
        uri_ss << "|endpoint=" << endpoint_name;
    }

    if (nullptr != control_address)
    {
        uri_ss << "|control=" << control_name;
    }

    uri_ss << '\0';

    const std::string uri_string = uri_ss.str();
    const char *uri = uri_string.c_str();

    ASSERT_EQ(parse_udp_channel(uri), 0) << aeron_errmsg();
    ASSERT_STREQ(canonical_form, m_channel->canonical_form);
}

INSTANTIATE_TEST_SUITE_P(
    UdpChannelNameTests,
    UdpChannelNamesParameterisedTest,
    testing::Values(
        std::make_tuple(
            "NAME_ENDPOINT", "192.168.1.1", (const char *)NULL, (const char *)NULL, "UDP-127.0.0.1:0-NAME_ENDPOINT"),
        std::make_tuple(
            "NAME_ENDPOINT", "224.0.1.1", (const char *)NULL, (const char *)NULL, "UDP-127.0.0.1:0-224.0.1.1:40124"),
        std::make_tuple(
            "NAME_ENDPOINT", "192.168.1.1", "NAME_CONTROL", "192.168.1.2", "UDP-NAME_CONTROL-NAME_ENDPOINT"),
        std::make_tuple(
            "NAME_ENDPOINT", "224.0.1.1", "NAME_CONTROL", "127.0.0.1", "UDP-127.0.0.1:0-224.0.1.1:40124"),
        std::make_tuple(
            "192.168.1.1:40124", "192.168.1.1", "NAME_CONTROL", "192.168.1.2", "UDP-NAME_CONTROL-192.168.1.1:40124"),
        std::make_tuple(
            "192.168.1.1:40124", "192.168.1.1", "192.168.1.2:40192", "192.168.1.2",
            "UDP-192.168.1.2:40192-192.168.1.1:40124"),
        std::make_tuple(
            "[fe80::5246:5dff:fe73:df06]:40456", "[fe80::5246:5dff:fe73:df06]", (const char *)NULL, (const char *)NULL,
            "UDP-127.0.0.1:0-[fe80::5246:5dff:fe73:df06]:40456")));

TEST_P(UdpChannelEqualityParameterisedTest, shouldMatch)
{
    const bool should_match = std::get<0>(GetParam());
    const char *uri_1 = std::get<1>(GetParam());
    const char *uri_2 = std::get<2>(GetParam());

    aeron_udp_channel_t *channel_1 = nullptr;
    aeron_udp_channel_t *channel_2 = nullptr;

    aeron_name_resolver_t resolver;
    aeron_default_name_resolver_supplier(&resolver, nullptr, nullptr);

    if (nullptr != uri_1)
    {
        ASSERT_LE(0, aeron_udp_channel_parse(strlen(uri_1), uri_1, &resolver, &channel_1, false)) << uri_1;
    }

    if (nullptr != uri_2)
    {
        ASSERT_LE(0, aeron_udp_channel_parse(strlen(uri_2), uri_2, &resolver, &channel_2, false)) << uri_2;
    }

    EXPECT_EQ(should_match, aeron_udp_channel_equals(channel_1, channel_2))
        << uri_1 << "(" << (nullptr != channel_1 ? channel_1->canonical_form : "null") << ")"
        << " vs "
        << uri_2 << "(" << (nullptr != channel_2 ? channel_2->canonical_form : "null") << ")";

    aeron_udp_channel_delete(channel_1);
    aeron_udp_channel_delete(channel_2);
}

INSTANTIATE_TEST_SUITE_P(
    UdpChannelEqualityTest,
    UdpChannelEqualityParameterisedTest,
    testing::Values(
        std::make_tuple(true, "aeron:udp?endpoint=localhost:9090", "aeron:udp?endpoint=localhost:9090"),
        std::make_tuple(true, "aeron:udp?endpoint=localhost:9090|session-id=12", "aeron:udp?endpoint=localhost:9090"),
        std::make_tuple(
            true,
            "aeron:udp?endpoint=localhost:9090|session-id=12",
            "aeron:udp?endpoint=localhost:9090|session-id=13"),
        std::make_tuple(
            true,
            "aeron:udp?endpoint=localhost:9090|interface=127.0.0.1:9090",
            "aeron:udp?endpoint=localhost:9090|interface=127.0.0.1:9090"),
        std::make_tuple(
            false,
            "aeron:udp?endpoint=localhost:9090|interface=127.0.0.1:9090",
            "aeron:udp?endpoint=localhost:9090|interface=127.0.0.1:9091"),
        std::make_tuple(false, "aeron:udp?endpoint=localhost:9090", "aeron:udp?endpoint=127.0.0.1:9090"),
        std::make_tuple(false, (const char *)NULL, "aeron:udp?endpoint=localhost:9091"),
        std::make_tuple(true, (const char *)NULL, (const char *)NULL),
        std::make_tuple(true, "aeron:udp?endpoint=localhost:9090", "aeron:udp?endpoint=localhost:9090")));
