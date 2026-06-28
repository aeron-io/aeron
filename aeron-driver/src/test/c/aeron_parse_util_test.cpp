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
#include <climits>

#include <gtest/gtest.h>

extern "C"
{
#include "util/aeron_error.h"
#include "util/aeron_parse_util.h"
}

class ParseUtilTest : public testing::Test
{
public:
    ParseUtilTest() = default;

    ~ParseUtilTest() override = default;
};

TEST_F(ParseUtilTest, shouldNotParseInvalidNumber)
{
    uint64_t value = 0;

    EXPECT_EQ(aeron_parse_size64(nullptr, &value), -1);
    EXPECT_EQ(aeron_parse_size64("", &value), -1);
    EXPECT_EQ(aeron_parse_size64("rubbish", &value), -1);
    EXPECT_EQ(aeron_parse_size64("-8", &value), -1);
    EXPECT_EQ(aeron_parse_size64("123Z", &value), -1);
    EXPECT_EQ(aeron_parse_size64("k", &value), -1);
}

class ParseUtilTestValidSize : public testing::TestWithParam<std::tuple<const char *, uint64_t>>
{
};

INSTANTIATE_TEST_SUITE_P(
    ParseUtilTestValidSize,
    ParseUtilTestValidSize,
    testing::Values(
        std::make_tuple("0", 0ULL),
        std::make_tuple("0k", 0ULL),
        std::make_tuple("0m", 0ULL),
        std::make_tuple("0g", 0ULL),
        std::make_tuple("1", 1ULL),
        std::make_tuple("77777777", 77777777ULL),
        std::make_tuple("9223372036854775807", 9223372036854775807ULL),
        std::make_tuple("1K", 1024ULL),
        std::make_tuple("1M", 1024 * 1024ULL),
        std::make_tuple("1G", 1024 * 1024 * 1024ULL),
        std::make_tuple("5023k", 5023 * 1024ULL),
        std::make_tuple("9m", 9 * 1024 * 1024ULL),
        std::make_tuple("5g", 5 * 1024 * 1024 * 1024ULL),
        std::make_tuple("8589934591g", 8589934591 * 1024 * 1024 * 1024ULL),
        std::make_tuple("8796093022207m", 8796093022207 * 1024 * 1024ULL),
        std::make_tuple("9007199254740991k", 9007199254740991 * 1024ULL)));

TEST_P(ParseUtilTestValidSize, shouldParseValidSize)
{
    uint64_t value;
    EXPECT_EQ(aeron_parse_size64(std::get<0>(GetParam()), &value), 0);
    EXPECT_EQ(value, std::get<1>(GetParam()));
}

class ParseUtilTestTooLargeSize : public testing::TestWithParam<const char *>
{
};

INSTANTIATE_TEST_SUITE_P(
    ParseUtilTestTooLargeSize,
    ParseUtilTestTooLargeSize,
    testing::Values(
        "8589934592g",
        "8796093022208m",
        "9007199254740992k",
        "9223372036854775807g",
        "9223372036854775807m",
        "9223372036854775807k" ));

TEST_P(ParseUtilTestTooLargeSize, shouldRejectTooLargeValue)
{
    uint64_t value = 0;
    EXPECT_EQ(aeron_parse_size64(GetParam(), &value), -1);
    EXPECT_EQ(value, 0);
}

TEST_F(ParseUtilTest, formatSizeShouldRejectValuesLargerThanLLongMaxValue)
{
    char buff[64] = {};
    EXPECT_EQ(-1, aeron_format_size64(ULLONG_MAX, buff, 0));
    EXPECT_EQ(EINVAL, aeron_errcode());
    std::string err = std::string(aeron_errmsg());
    EXPECT_NE(std::string::npos, err.find("value is out of range: 18446744073709551615"));
}

class ParseUtilTestFormatSize : public testing::TestWithParam<std::tuple<uint64_t, const char *, int>>
{
};

INSTANTIATE_TEST_SUITE_P(
    ParseUtilTestFormatSize,
    ParseUtilTestFormatSize,
    testing::Values(
        std::make_tuple(0ULL, "0", 1),
        std::make_tuple(1ULL, "1", 1),
        std::make_tuple(77777777ULL, "77777777", 8),
        std::make_tuple(LLONG_MAX, "9223372036854775807", 19),
        std::make_tuple(1024ULL, "1k", 2),
        std::make_tuple(1024 * 1024ULL, "1m", 2),
        std::make_tuple(1024 * 1024 * 1024ULL, "1g", 2),
        std::make_tuple(5023 * 1024ULL,"5023k", 5),
        std::make_tuple(9 * 1024 * 1024ULL, "9m", 2),
        std::make_tuple(5 * 1024 * 1024 * 1024ULL, "5g", 2),
        std::make_tuple(8589934591 * 1024 * 1024 * 1024ULL, "8589934591g", 11),
        std::make_tuple(8796093022207 * 1024 * 1024ULL, "8796093022207m", 14),
        std::make_tuple(9007199254740991 * 1024ULL, "9007199254740991k", 17)));

TEST_P(ParseUtilTestFormatSize, shouldFormatSize)
{
    char buff[64] = {};

    EXPECT_EQ(aeron_format_size64(std::get<0>(GetParam()), buff, sizeof(buff)), std::get<2>(GetParam()));
    EXPECT_STREQ(buff, std::get<1>(GetParam()));
}

TEST_F(ParseUtilTest, shouldNotParseInvalidDuration)
{
    uint64_t duration_ns = 0;

    EXPECT_EQ(aeron_parse_duration_ns(nullptr, &duration_ns), -1);
    EXPECT_EQ(aeron_parse_duration_ns("", &duration_ns), -1);
    EXPECT_EQ(aeron_parse_duration_ns("rubbish", &duration_ns), -1);
    EXPECT_EQ(aeron_parse_duration_ns("-8", &duration_ns), -1);
    EXPECT_EQ(aeron_parse_duration_ns("123ps", &duration_ns), -1);
    EXPECT_EQ(aeron_parse_duration_ns("s", &duration_ns), -1);
}

class ParseUtilTestValidDuration : public testing::TestWithParam<std::tuple<const char *, uint64_t>>
{
};

INSTANTIATE_TEST_SUITE_P(
    ParseUtilTestValidDuration,
    ParseUtilTestValidDuration,
    testing::Values(
        std::make_tuple("0", 0ULL),
        std::make_tuple("0ns", 0ULL),
        std::make_tuple("0us", 0ULL),
        std::make_tuple("0ms", 0ULL),
        std::make_tuple("0s", 0ULL),
        std::make_tuple("12345", 12345ULL),
        std::make_tuple("12345NS", 12345ULL),
        std::make_tuple("456nS", 456ULL),
        std::make_tuple("789Ns", 789ULL),
        std::make_tuple("456US", 456000ULL),
        std::make_tuple("1000uS", 1000000ULL),
        std::make_tuple("2000Us", 2000000ULL),
        std::make_tuple("123MS", 123000000ULL),
        std::make_tuple("1ms", 1000000ULL),
        std::make_tuple("1Ms", 1000000ULL),
        std::make_tuple("66mS", 66000000ULL),
        std::make_tuple("5S", 5000000000ULL),
        std::make_tuple("345s", 345000000000ULL),
        std::make_tuple("700ms", 700000000ULL)));

TEST_P(ParseUtilTestValidDuration, shouldParseValidDuration)
{
    uint64_t duration_ns;
    EXPECT_EQ(aeron_parse_duration_ns(std::get<0>(GetParam()), &duration_ns), 0);
    EXPECT_EQ(std::get<1>(GetParam()), duration_ns);
}

class ParseUtilTestMaxDuration : public testing::TestWithParam<std::tuple<const char *, uint64_t>>
{
};

INSTANTIATE_TEST_SUITE_P(
    ParseUtilTestMaxDuration,
    ParseUtilTestMaxDuration,
    testing::Values(
        std::make_tuple("9223372036854775us", 9223372036854775000ULL),
        std::make_tuple("9223372036854ms", 9223372036854000000ULL),
        std::make_tuple("9223372036s", 9223372036000000000ULL),
        std::make_tuple("9223372036854776us", (uint64_t)LLONG_MAX),
        std::make_tuple("9223372036855ms", (uint64_t)LLONG_MAX),
        std::make_tuple("9223372037s", (uint64_t)LLONG_MAX),
        std::make_tuple("70000000000s", (uint64_t)LLONG_MAX)));

TEST_P(ParseUtilTestMaxDuration, shouldParseMaxQualifiedDuration)
{
    uint64_t duration_ns;
    EXPECT_EQ(aeron_parse_duration_ns(std::get<0>(GetParam()), &duration_ns), 0);
    EXPECT_EQ(std::get<1>(GetParam()), duration_ns);
}

TEST_F(ParseUtilTest, formatDurationShouldRejectValuesLargerThanLLongMaxValue)
{
    char buff[64] = {};
    EXPECT_EQ(-1, aeron_format_duration_ns(ULLONG_MAX, buff, 0));
    EXPECT_EQ(EINVAL, aeron_errcode());
    std::string err = std::string(aeron_errmsg());
    EXPECT_NE(std::string::npos, err.find("duration is out of range: 18446744073709551615"));
}

class ParseUtilTestFormatDuration : public testing::TestWithParam<std::tuple<uint64_t, const char *, int>>
{
};

INSTANTIATE_TEST_SUITE_P(
    ParseUtilTestFormatDuration,
    ParseUtilTestFormatDuration,
    testing::Values(
        std::make_tuple(0ULL, "0ns", 3),
        std::make_tuple(12345ULL, "12345ns", 7),
        std::make_tuple(456000ULL, "456us",5),
        std::make_tuple(1000000ULL, "1ms", 3),
        std::make_tuple(123000000ULL, "123ms", 5),
        std::make_tuple(1ULL, "1ns", 3),
        std::make_tuple(1000ULL, "1us", 3),
        std::make_tuple(1000000ULL, "1ms", 3),
        std::make_tuple(1000000000ULL, "1s", 2),
        std::make_tuple(66000000ULL, "66ms", 4),
        std::make_tuple(5000000000ULL, "5s", 2),
        std::make_tuple(345000000000ULL, "345s", 4),
        std::make_tuple(700000000ULL, "700ms", 5),
        std::make_tuple(9223372036854775000ULL, "9223372036854775us", 18),
        std::make_tuple(9223372036854000000ULL, "9223372036854ms", 15),
        std::make_tuple(9223372036000000000ULL, "9223372036s", 11),
        std::make_tuple(LLONG_MAX, "9223372036854775807ns", 21)));

TEST_P(ParseUtilTestFormatDuration, shouldFormatDuration)
{
    char buff[64] = {};
    EXPECT_EQ(aeron_format_duration_ns(std::get<0>(GetParam()), buff, sizeof(buff)), std::get<2>(GetParam()));
    EXPECT_STREQ(buff, std::get<1>(GetParam()));
}

TEST_F(ParseUtilTest, shouldSplitAddress)
{
    aeron_parsed_address_t split_address;

    EXPECT_EQ(aeron_address_split("localhost:1234", &split_address), 0);
    EXPECT_EQ(std::string(split_address.host), "localhost");
    EXPECT_EQ(std::string(split_address.port), "1234");
    EXPECT_EQ(split_address.ip_version_hint, 4);

    EXPECT_EQ(aeron_address_split("127.0.0.1:777", &split_address), 0);
    EXPECT_EQ(std::string(split_address.host), "127.0.0.1");
    EXPECT_EQ(std::string(split_address.port), "777");
    EXPECT_EQ(split_address.ip_version_hint, 4);

    EXPECT_EQ(aeron_address_split("localhost.local", &split_address), 0);
    EXPECT_EQ(std::string(split_address.host), "localhost.local");
    EXPECT_EQ(std::string(split_address.port), "");
    EXPECT_EQ(split_address.ip_version_hint, 4);

    EXPECT_EQ(aeron_address_split(":123", &split_address), 0);
    EXPECT_EQ(std::string(split_address.host), "");
    EXPECT_EQ(std::string(split_address.port), "123");
    EXPECT_EQ(split_address.ip_version_hint, 4);

    EXPECT_EQ(aeron_address_split("[FF01::FD]:20000", &split_address), 0);
    EXPECT_EQ(std::string(split_address.host), "FF01::FD");
    EXPECT_EQ(std::string(split_address.port), "20000");
    EXPECT_EQ(split_address.ip_version_hint, 6);
}

TEST_F(ParseUtilTest, shouldSplitInterface)
{
    aeron_parsed_interface_t split_interface;

    EXPECT_EQ(aeron_interface_split("localhost:1234/24", &split_interface), 0);
    EXPECT_EQ(std::string(split_interface.host), "localhost");
    EXPECT_EQ(std::string(split_interface.port), "1234");
    EXPECT_EQ(std::string(split_interface.prefix), "24");
    EXPECT_EQ(split_interface.ip_version_hint, 4);

    EXPECT_EQ(aeron_interface_split("127.0.0.1:777", &split_interface), 0);
    EXPECT_EQ(std::string(split_interface.host), "127.0.0.1");
    EXPECT_EQ(std::string(split_interface.port), "777");
    EXPECT_EQ(std::string(split_interface.prefix), "");
    EXPECT_EQ(split_interface.ip_version_hint, 4);

    EXPECT_EQ(aeron_interface_split("localhost.local", &split_interface), 0);
    EXPECT_EQ(std::string(split_interface.host), "localhost.local");
    EXPECT_EQ(std::string(split_interface.port), "");
    EXPECT_EQ(std::string(split_interface.prefix), "");
    EXPECT_EQ(split_interface.ip_version_hint, 4);

    EXPECT_EQ(aeron_interface_split(":123", &split_interface), 0);
    EXPECT_EQ(std::string(split_interface.host), "");
    EXPECT_EQ(std::string(split_interface.port), "123");
    EXPECT_EQ(std::string(split_interface.prefix), "");
    EXPECT_EQ(split_interface.ip_version_hint, 4);

    EXPECT_EQ(aeron_interface_split("[FF01::FD]:20000/8", &split_interface), 0);
    EXPECT_EQ(std::string(split_interface.host), "FF01::FD");
    EXPECT_EQ(std::string(split_interface.port), "20000");
    EXPECT_EQ(std::string(split_interface.prefix), "8");
    EXPECT_EQ(split_interface.ip_version_hint, 6);

    EXPECT_EQ(aeron_interface_split("[FF01::FD]:20000", &split_interface), 0);
    EXPECT_EQ(std::string(split_interface.host), "FF01::FD");
    EXPECT_EQ(std::string(split_interface.port), "20000");
    EXPECT_EQ(std::string(split_interface.prefix), "");
    EXPECT_EQ(split_interface.ip_version_hint, 6);

    EXPECT_EQ(aeron_interface_split("[FF01::FD]/128", &split_interface), 0);
    EXPECT_EQ(std::string(split_interface.host), "FF01::FD");
    EXPECT_EQ(std::string(split_interface.port), "");
    EXPECT_EQ(std::string(split_interface.prefix), "128");
    EXPECT_EQ(split_interface.ip_version_hint, 6);

    EXPECT_EQ(aeron_interface_split("[FF01::FD%eth0]", &split_interface), 0);
    EXPECT_EQ(std::string(split_interface.host), "FF01::FD");
    EXPECT_EQ(std::string(split_interface.port), "");
    EXPECT_EQ(std::string(split_interface.prefix), "");
    EXPECT_EQ(split_interface.ip_version_hint, 6);

    EXPECT_EQ(aeron_interface_split("[::1%eth0]:1234", &split_interface), 0);
    EXPECT_EQ(std::string(split_interface.host), "::1");
    EXPECT_EQ(std::string(split_interface.port), "1234");
    EXPECT_EQ(std::string(split_interface.prefix), "");
    EXPECT_EQ(split_interface.ip_version_hint, 6);
}
