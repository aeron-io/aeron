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

#include "gtest/gtest.h"

#include "ChannelUriStringBuilder.h"
extern "C"
{
#include "uri/aeron_uri.h"
}

using namespace aeron;

TEST(ChannelUriStringBuilderTest, shouldGenerateBasicIpcChannel)
{
    ChannelUriStringBuilder builder;

    builder.media(IPC_MEDIA);

    ASSERT_EQ(builder.build(), "aeron:ipc");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateBasicUdpChannel)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999");

    ASSERT_EQ(builder.build(), "aeron:udp?endpoint=localhost:9999");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateBasicUdpChannelSpy)
{
    ChannelUriStringBuilder builder;

    builder
        .prefix(SPY_QUALIFIER)
        .media(UDP_MEDIA)
        .endpoint("localhost:9999");

    ASSERT_EQ(builder.build(), "aeron-spy:aeron:udp?endpoint=localhost:9999");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateComplexUdpChannel)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .ttl(9)
        .termLength(1024 * 128);

    ASSERT_EQ(builder.build(), "aeron:udp?endpoint=localhost:9999|term-length=131072|ttl=9");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateReplayUdpChannel)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .termLength(1024 * 128)
        .initialTermId(777)
        .termId(999)
        .termOffset(64);

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|term-length=131072|init-term-id=777|term-id=999|term-offset=64");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateInitialPosition)
{
    ChannelUriStringBuilder builder;

    std::int32_t termLength = 1024 * 128;
    std::int64_t position = (termLength * 3) + 64;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .initialPosition(position, 777, termLength);

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|term-length=131072|init-term-id=777|term-id=780|term-offset=64");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateSocketSndRcvbufLengthsAndTos)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .socketSndbufLength(8192)
        .socketRcvbufLength(4096)
        .socketTos(255);

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|so-sndbuf=8192|so-rcvbuf=4096|so-tos=255");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateReceiverWindowLength)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .receiverWindowLength(4096);

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|rcv-wnd=4096");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateUntetheredLingerTimeout)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .untetheredLingerTimeout(123456789);

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|untethered-linger-timeout=123456789");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateOffsets)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .mediaReceiveTimestampOffset("reserved")
        .channelReceiveTimestampOffset("0")
        .channelSendTimestampOffset("8");

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|media-rcv-ts-offset=reserved|channel-rcv-ts-offset=0|channel-snd-ts-offset=8");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateReceiveTimestampOffset)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .mediaReceiveTimestampOffset("reserved");

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|media-rcv-ts-offset=reserved");
}

TEST(ChannelUriStringBuilderTest, shouldGenerateRxTimestampOffset)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("localhost:9999")
        .mediaReceiveTimestampOffset("reserved");

    ASSERT_EQ(
        builder.build(),
        "aeron:udp?endpoint=localhost:9999|media-rcv-ts-offset=reserved");
}

TEST(ChannelUriStringBuilderTest, shouldHandleMaxRetransmits)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .endpoint("224.10.9.8:777")
        .maxResend(123);

    const std::string uriString = builder.build();

    std::shared_ptr<ChannelUri> channelUri = ChannelUri::parse(uriString);
    ASSERT_NE(std::string::npos, channelUri->toString().find("max-resend=123"));
}

TEST(ChannelUriStringBuilderTest, shouldThrowWhenBuildingWithoutMedia)
{
    ChannelUriStringBuilder builder;

    EXPECT_THROW(builder.build(), IllegalStateException);
}

TEST(ChannelUriStringBuilderTest, shouldClearOnlyRejoin)
{
    ChannelUriStringBuilder builder;

    builder
        .media(UDP_MEDIA)
        .reliable(true)
        .rejoin(false)
        .rejoin(nullptr);

    ASSERT_EQ(builder.build(), "aeron:udp?reliable=true");
}

class InitialPositionTest : public testing::TestWithParam<std::tuple<std::int64_t, std::int32_t, std::int32_t, std::int32_t, std::int32_t>>
{
};

INSTANTIATE_TEST_SUITE_P(
    InitialPositionTests,
    InitialPositionTest,
    testing::Values(
        std::make_tuple(64 * 1024, INT32_MAX, 64 * 1024, INT32_MIN, 0),
        std::make_tuple(0, -5, 1024 * 1024, -5, 0),
        std::make_tuple(4 * 1024 * 1024 + 256, -19, 128 * 1024, 13, 256)
    ));

TEST_P(InitialPositionTest, shouldWrapTermIdOfInitialPosition)
{
    ChannelUriStringBuilder builder;

    std::int64_t position = std::get<0>(GetParam());
    std::int32_t initialTermId = std::get<1>(GetParam());
    std::int32_t termLength = std::get<2>(GetParam());
    std::int32_t expectedTermId = std::get<3>(GetParam());
    std::int32_t expectedTermOffset = std::get<4>(GetParam());

    builder
        .media(IPC_MEDIA)
        .initialPosition(position, initialTermId, termLength);

    auto channel = builder.build();

    aeron_uri_t uri;
    EXPECT_EQ(0, aeron_uri_parse(channel.length(), channel.c_str(), &uri));
    EXPECT_STREQ(std::to_string(initialTermId).c_str(), aeron_uri_find_param_value(&uri.params.ipc.additional_params, AERON_URI_INITIAL_TERM_ID_KEY));
    EXPECT_STREQ(std::to_string(expectedTermId).c_str(), aeron_uri_find_param_value(&uri.params.ipc.additional_params, AERON_URI_TERM_ID_KEY));
    EXPECT_STREQ(std::to_string(expectedTermOffset).c_str(), aeron_uri_find_param_value(&uri.params.ipc.additional_params, AERON_URI_TERM_OFFSET_KEY));
    aeron_uri_close(&uri);
}

TEST(ChannelUriTest, shouldRejectInvalidCharacterInMediaWithTheCharacterInTheMessage)
{
    for (const char c : { ':', '|', '=' })
    {
        const std::string uri = std::string("aeron:ud") + c + "p";
        try
        {
            ChannelUri::parse(uri);
            FAIL() << "expected IllegalArgumentException for " << uri;
        }
        catch (const IllegalArgumentException &ex)
        {
            EXPECT_NE(std::string::npos, std::string(ex.what()).find(std::string("encountered '") + c + "'"))
                << ex.what();
        }
    }
}
