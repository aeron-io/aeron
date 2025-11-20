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

extern "C"
{
#include "util/aeron_error.h"
#include "util/aeron_netutil.h"
}

class NetutilTest : public testing::Test
{
public:
    NetutilTest() = default;
};

TEST_F(NetutilTest, shouldGetSocketBufferLengths)
{
    size_t default_so_rcvbuf = 0;
    size_t default_so_sndbuf = 0;

    ASSERT_GE(aeron_netutil_get_so_buf_lengths(
        &default_so_rcvbuf, &default_so_sndbuf), 0) << aeron_errmsg();

    EXPECT_NE(0U, default_so_rcvbuf);
    EXPECT_NE(0U, default_so_sndbuf);
}

TEST_F(NetutilTest, shouldConvertPrefixLengthToMask)
{
    EXPECT_EQ(0, aeron_ipv4_netmask_from_prefixlen(0));
    EXPECT_EQ(0x00000080, aeron_ipv4_netmask_from_prefixlen(1));
    EXPECT_EQ(0x000000C0, aeron_ipv4_netmask_from_prefixlen(2));
    EXPECT_EQ(0x000000E0, aeron_ipv4_netmask_from_prefixlen(3));
    EXPECT_EQ(0x000000F0, aeron_ipv4_netmask_from_prefixlen(4));
    EXPECT_EQ(0x000000F8, aeron_ipv4_netmask_from_prefixlen(5));
    EXPECT_EQ(0x000000FC, aeron_ipv4_netmask_from_prefixlen(6));
    EXPECT_EQ(0x000000FE, aeron_ipv4_netmask_from_prefixlen(7));
    EXPECT_EQ(0x000000FF, aeron_ipv4_netmask_from_prefixlen(8));
    EXPECT_EQ(0x000080FF, aeron_ipv4_netmask_from_prefixlen(9));
    EXPECT_EQ(0x0000C0FF, aeron_ipv4_netmask_from_prefixlen(10));
    EXPECT_EQ(0x0000E0FF, aeron_ipv4_netmask_from_prefixlen(11));
    EXPECT_EQ(0x0000F0FF, aeron_ipv4_netmask_from_prefixlen(12));
    EXPECT_EQ(0x0000F8FF, aeron_ipv4_netmask_from_prefixlen(13));
    EXPECT_EQ(0x0000FCFF, aeron_ipv4_netmask_from_prefixlen(14));
    EXPECT_EQ(0x0000FEFF, aeron_ipv4_netmask_from_prefixlen(15));
    EXPECT_EQ(0x0000FFFF, aeron_ipv4_netmask_from_prefixlen(16));
    EXPECT_EQ(0x0080FFFF, aeron_ipv4_netmask_from_prefixlen(17));
    EXPECT_EQ(0x00C0FFFF, aeron_ipv4_netmask_from_prefixlen(18));
    EXPECT_EQ(0x00E0FFFF, aeron_ipv4_netmask_from_prefixlen(19));
    EXPECT_EQ(0x00F0FFFF, aeron_ipv4_netmask_from_prefixlen(20));
    EXPECT_EQ(0x00F8FFFF, aeron_ipv4_netmask_from_prefixlen(21));
    EXPECT_EQ(0x00FCFFFF, aeron_ipv4_netmask_from_prefixlen(22));
    EXPECT_EQ(0x00FEFFFF, aeron_ipv4_netmask_from_prefixlen(23));
    EXPECT_EQ(0x00FFFFFF, aeron_ipv4_netmask_from_prefixlen(24));
    EXPECT_EQ(0x80FFFFFF, aeron_ipv4_netmask_from_prefixlen(25));
    EXPECT_EQ(0xC0FFFFFF, aeron_ipv4_netmask_from_prefixlen(26));
    EXPECT_EQ(0xE0FFFFFF, aeron_ipv4_netmask_from_prefixlen(27));
    EXPECT_EQ(0xF0FFFFFF, aeron_ipv4_netmask_from_prefixlen(28));
    EXPECT_EQ(0xF8FFFFFF, aeron_ipv4_netmask_from_prefixlen(29));
    EXPECT_EQ(0xFEFFFFFF, aeron_ipv4_netmask_from_prefixlen(31));
    EXPECT_EQ(0xFFFFFFFF, aeron_ipv4_netmask_from_prefixlen(32));
}

TEST_F(NetutilTest, shouldParseValidNamedInterfaces)
{
    aeron_named_interface_t named_interface;

    ASSERT_EQ(0, aeron_parse_named_interface("{eth0}", &named_interface));
    ASSERT_STREQ("eth0", named_interface.name);
    ASSERT_EQ(0, named_interface.port);

    ASSERT_EQ(0, aeron_parse_named_interface("{eth0}:0", &named_interface));
    ASSERT_STREQ("eth0", named_interface.name);
    ASSERT_EQ(0, named_interface.port);

    ASSERT_EQ(0, aeron_parse_named_interface("{eth0}:1234", &named_interface));
    ASSERT_STREQ("eth0", named_interface.name);
    ASSERT_EQ(1234, named_interface.port);

    ASSERT_EQ(0, aeron_parse_named_interface("{eth0:0}:0", &named_interface));
    ASSERT_STREQ("eth0:0", named_interface.name);
    ASSERT_EQ(0, named_interface.port);

    ASSERT_EQ(0, aeron_parse_named_interface("{eth0:1}:2000", &named_interface));
    ASSERT_STREQ("eth0:1", named_interface.name);
    ASSERT_EQ(2000, named_interface.port);

    ASSERT_EQ(0, aeron_parse_named_interface("{eth0.100}:80", &named_interface));
    ASSERT_STREQ("eth0.100", named_interface.name);
    ASSERT_EQ(80, named_interface.port);

    ASSERT_EQ(0, aeron_parse_named_interface("{x}:5678", &named_interface));
    ASSERT_STREQ("x", named_interface.name);
    ASSERT_EQ(5678, named_interface.port);

    ASSERT_EQ(0, aeron_parse_named_interface("{lo}:65535", &named_interface));
    ASSERT_STREQ("lo", named_interface.name);
    ASSERT_EQ(65535, named_interface.port);

    ASSERT_EQ(0, aeron_parse_named_interface("{Ethernet 1}", &named_interface));
    ASSERT_STREQ("Ethernet 1", named_interface.name);
    ASSERT_EQ(0, named_interface.port);

    ASSERT_EQ(0, aeron_parse_named_interface("{ethernet_32768}", &named_interface));
    ASSERT_STREQ("ethernet_32768", named_interface.name);
    ASSERT_EQ(0, named_interface.port);

    ASSERT_EQ(0, aeron_parse_named_interface("{foo{bar-1234}}:9999", &named_interface));
    ASSERT_STREQ("foo{bar-1234}", named_interface.name);
    ASSERT_EQ(9999, named_interface.port);

    auto long_name = std::string(IF_NAMESIZE - 1, 'x');
    auto input = "{" + long_name + "}:10000";
    ASSERT_EQ(0, aeron_parse_named_interface(input.c_str(), &named_interface));
    ASSERT_STREQ(long_name.c_str(), named_interface.name);
    ASSERT_EQ(10000, named_interface.port);
}

TEST_F(NetutilTest, shouldReturnErrorIfParsedNamedInterfaceIsInvalid)
{
    aeron_named_interface_t named_interface;

    ASSERT_EQ(-1, aeron_parse_named_interface("", &named_interface));
    ASSERT_EQ(-1, aeron_parse_named_interface(" ", &named_interface));
    ASSERT_EQ(-1, aeron_parse_named_interface("eth0", &named_interface));
    ASSERT_EQ(-1, aeron_parse_named_interface("eth0:0", &named_interface));
    ASSERT_EQ(-1, aeron_parse_named_interface("{eth0", &named_interface));
    ASSERT_EQ(-1, aeron_parse_named_interface("{eth0}:", &named_interface));
    ASSERT_EQ(-1, aeron_parse_named_interface("{eth0}0", &named_interface));
    ASSERT_EQ(-1, aeron_parse_named_interface("{eth0}:-1000", &named_interface));
    ASSERT_EQ(-1, aeron_parse_named_interface("{eth0};1000", &named_interface));
    ASSERT_EQ(-1, aeron_parse_named_interface("{eth0}:1000:2000", &named_interface));
    ASSERT_EQ(-1, aeron_parse_named_interface("{eth0}:65536", &named_interface));
    ASSERT_EQ(-1, aeron_parse_named_interface("{}", &named_interface));

    auto too_long_name = std::string(IF_NAMESIZE, 'x');
    auto input = "{" + too_long_name + "}";
    ASSERT_EQ(-1, aeron_parse_named_interface(input.c_str(), &named_interface));
}
