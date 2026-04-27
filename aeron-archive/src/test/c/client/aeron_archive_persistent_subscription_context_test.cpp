/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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
#include "ArchiveClientTestUtils.h"
#include "TestArchive.h"

extern "C"
{
#include "client/aeron_archive.h"
#include "client/aeron_archive_persistent_subscription.h"
#include "uri/aeron_uri.h"
}

class AeronArchivePersistentSubscriptionContextTest : public testing::Test
{
};

TEST_F(AeronArchivePersistentSubscriptionContextTest, shouldFailIfArchiveContextNotSet)
{
    aeron_archive_persistent_subscription_context_t *context;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_init(&context)) << aeron_errmsg();
    aeron_archive_persistent_subscription_context_set_recording_id(context, 0);
    aeron_archive_persistent_subscription_context_set_live_channel(context, "aeron:ipc");
    aeron_archive_persistent_subscription_context_set_live_stream_id(context, 1000);
    aeron_archive_persistent_subscription_context_set_replay_channel(context, "aeron:udp?endpoint=localhost:0");
    aeron_archive_persistent_subscription_context_set_replay_stream_id(context, -5);

    ASSERT_EQ(-1, aeron_archive_persistent_subscription_context_conclude(context));

    aeron_archive_persistent_subscription_context_close(context);
}

TEST_F(AeronArchivePersistentSubscriptionContextTest, shouldFailIfRecordingIdNotSet)
{
    aeron_archive_context_t *archive_ctx;
    ASSERT_EQ(0, aeron_archive_context_init(&archive_ctx)) << aeron_errmsg();

    aeron_archive_persistent_subscription_context_t *context;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_init(&context)) << aeron_errmsg();
    aeron_archive_persistent_subscription_context_set_archive_context(context, archive_ctx);
    // recording_id deliberately not set
    aeron_archive_persistent_subscription_context_set_live_channel(context, "aeron:ipc");
    aeron_archive_persistent_subscription_context_set_live_stream_id(context, 1000);
    aeron_archive_persistent_subscription_context_set_replay_channel(context, "aeron:udp?endpoint=localhost:0");
    aeron_archive_persistent_subscription_context_set_replay_stream_id(context, -5);

    ASSERT_EQ(-1, aeron_archive_persistent_subscription_context_conclude(context));

    aeron_archive_persistent_subscription_context_close(context);
    aeron_archive_context_close(archive_ctx);
}

TEST_F(AeronArchivePersistentSubscriptionContextTest, shouldFailIfLiveChannelNotSet)
{
    aeron_archive_context_t *archive_ctx;
    ASSERT_EQ(0, aeron_archive_context_init(&archive_ctx)) << aeron_errmsg();

    aeron_archive_persistent_subscription_context_t *context;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_init(&context)) << aeron_errmsg();
    aeron_archive_persistent_subscription_context_set_archive_context(context, archive_ctx);
    aeron_archive_persistent_subscription_context_set_recording_id(context, 0);
    // live_channel deliberately not set
    aeron_archive_persistent_subscription_context_set_live_stream_id(context, 1000);
    aeron_archive_persistent_subscription_context_set_replay_channel(context, "aeron:udp?endpoint=localhost:0");
    aeron_archive_persistent_subscription_context_set_replay_stream_id(context, -5);

    ASSERT_EQ(-1, aeron_archive_persistent_subscription_context_conclude(context));

    aeron_archive_persistent_subscription_context_close(context);
    aeron_archive_context_close(archive_ctx);
}

TEST_F(AeronArchivePersistentSubscriptionContextTest, shouldFailIfLiveStreamIdNotSet)
{
    aeron_archive_context_t *archive_ctx;
    ASSERT_EQ(0, aeron_archive_context_init(&archive_ctx)) << aeron_errmsg();

    aeron_archive_persistent_subscription_context_t *context;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_init(&context)) << aeron_errmsg();
    aeron_archive_persistent_subscription_context_set_archive_context(context, archive_ctx);
    aeron_archive_persistent_subscription_context_set_recording_id(context, 0);
    aeron_archive_persistent_subscription_context_set_live_channel(context, "aeron:ipc");
    // live_stream_id deliberately not set
    aeron_archive_persistent_subscription_context_set_replay_channel(context, "aeron:udp?endpoint=localhost:0");
    aeron_archive_persistent_subscription_context_set_replay_stream_id(context, -5);

    ASSERT_EQ(-1, aeron_archive_persistent_subscription_context_conclude(context));

    aeron_archive_persistent_subscription_context_close(context);
    aeron_archive_context_close(archive_ctx);
}

TEST_F(AeronArchivePersistentSubscriptionContextTest, shouldFailIfReplayChannelNotSet)
{
    aeron_archive_context_t *archive_ctx;
    ASSERT_EQ(0, aeron_archive_context_init(&archive_ctx)) << aeron_errmsg();

    aeron_archive_persistent_subscription_context_t *context;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_init(&context)) << aeron_errmsg();
    aeron_archive_persistent_subscription_context_set_archive_context(context, archive_ctx);
    aeron_archive_persistent_subscription_context_set_recording_id(context, 0);
    aeron_archive_persistent_subscription_context_set_live_channel(context, "aeron:ipc");
    aeron_archive_persistent_subscription_context_set_live_stream_id(context, 1000);
    // replay_channel deliberately not set
    aeron_archive_persistent_subscription_context_set_replay_stream_id(context, -5);

    ASSERT_EQ(-1, aeron_archive_persistent_subscription_context_conclude(context));

    aeron_archive_persistent_subscription_context_close(context);
    aeron_archive_context_close(archive_ctx);
}

TEST_F(AeronArchivePersistentSubscriptionContextTest, shouldFailIfReplayStreamIdNotSet)
{
    aeron_archive_context_t *archive_ctx;
    ASSERT_EQ(0, aeron_archive_context_init(&archive_ctx)) << aeron_errmsg();

    aeron_archive_persistent_subscription_context_t *context;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_init(&context)) << aeron_errmsg();
    aeron_archive_persistent_subscription_context_set_archive_context(context, archive_ctx);
    aeron_archive_persistent_subscription_context_set_recording_id(context, 0);
    aeron_archive_persistent_subscription_context_set_live_channel(context, "aeron:ipc");
    aeron_archive_persistent_subscription_context_set_live_stream_id(context, 1000);
    aeron_archive_persistent_subscription_context_set_replay_channel(context, "aeron:udp?endpoint=localhost:0");
    // replay_stream_id deliberately not set

    ASSERT_EQ(-1, aeron_archive_persistent_subscription_context_conclude(context));

    aeron_archive_persistent_subscription_context_close(context);
    aeron_archive_context_close(archive_ctx);
}

TEST_F(AeronArchivePersistentSubscriptionContextTest, shouldFailIfStartPositionIsInvalid)
{
    aeron_archive_context_t *archive_ctx;
    ASSERT_EQ(0, aeron_archive_context_init(&archive_ctx)) << aeron_errmsg();

    aeron_archive_persistent_subscription_context_t *context;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_init(&context)) << aeron_errmsg();
    aeron_archive_persistent_subscription_context_set_archive_context(context, archive_ctx);
    aeron_archive_persistent_subscription_context_set_recording_id(context, 0);
    aeron_archive_persistent_subscription_context_set_live_channel(context, "aeron:ipc");
    aeron_archive_persistent_subscription_context_set_live_stream_id(context, 1000);
    aeron_archive_persistent_subscription_context_set_replay_channel(context, "aeron:udp?endpoint=localhost:0");
    aeron_archive_persistent_subscription_context_set_replay_stream_id(context, -5);
    aeron_archive_persistent_subscription_context_set_start_position(context, -3); // invalid

    ASSERT_EQ(-1, aeron_archive_persistent_subscription_context_conclude(context));

    aeron_archive_persistent_subscription_context_close(context);
    aeron_archive_context_close(archive_ctx);
}

TEST_F(AeronArchivePersistentSubscriptionContextTest, shouldFailIfRecordingIdIsInvalid)
{
    aeron_archive_context_t *archive_ctx;
    ASSERT_EQ(0, aeron_archive_context_init(&archive_ctx)) << aeron_errmsg();

    aeron_archive_persistent_subscription_context_t *context;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_init(&context)) << aeron_errmsg();
    aeron_archive_persistent_subscription_context_set_archive_context(context, archive_ctx);
    aeron_archive_persistent_subscription_context_set_recording_id(context, -5); // invalid negative
    aeron_archive_persistent_subscription_context_set_live_channel(context, "aeron:ipc");
    aeron_archive_persistent_subscription_context_set_live_stream_id(context, 1000);
    aeron_archive_persistent_subscription_context_set_replay_channel(context, "aeron:udp?endpoint=localhost:0");
    aeron_archive_persistent_subscription_context_set_replay_stream_id(context, -5);

    ASSERT_EQ(-1, aeron_archive_persistent_subscription_context_conclude(context));

    aeron_archive_persistent_subscription_context_close(context);
    aeron_archive_context_close(archive_ctx);
}

TEST_F(AeronArchivePersistentSubscriptionContextTest, shouldRejectNullListener)
{
    aeron_archive_persistent_subscription_context_t *context;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_init(&context)) << aeron_errmsg();

    ASSERT_EQ(-1, aeron_archive_persistent_subscription_context_set_listener(context, nullptr));

    aeron_archive_persistent_subscription_context_close(context);
}

TEST_F(AeronArchivePersistentSubscriptionContextTest, testValidContextWithExternalAeron)
{
    DriverResource driver;
    AeronResource aeron{driver.aeronDir()};

    aeron_archive_context_t *archive_context;
    ASSERT_EQ(0, aeron_archive_context_init(&archive_context)) << aeron_errmsg();

    aeron_archive_persistent_subscription_context_t *context;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_init(&context)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_archive_context(context, archive_context)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_recording_id(context, 0)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_live_channel(context, "aeron:ipc")) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_live_stream_id(context, 1000)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_replay_channel(context, "aeron:ipc")) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_replay_stream_id(context, 2000)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_aeron(context, aeron.aeron())) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_conclude(context)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_close(context)) << aeron_errmsg();

    EXPECT_EQ(0, aeron_archive_context_close(archive_context)) << aeron_errmsg();

    EXPECT_FALSE(aeron_is_closed(aeron.aeron()));
}

TEST_F(AeronArchivePersistentSubscriptionContextTest, testValidContextWithOwnedAeron)
{
    DriverResource driver;

    aeron_archive_context_t *archive_context;
    ASSERT_EQ(0, aeron_archive_context_init(&archive_context)) << aeron_errmsg();

    aeron_archive_persistent_subscription_context_t *context;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_init(&context)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_archive_context(context, archive_context)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_recording_id(context, 0)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_live_channel(context, "aeron:ipc")) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_live_stream_id(context, 1000)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_replay_channel(context, "aeron:ipc")) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_replay_stream_id(context, 2000)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_aeron_directory_name(context, driver.aeronDir().c_str())) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_conclude(context)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_close(context)) << aeron_errmsg();

    EXPECT_EQ(0, aeron_archive_context_close(archive_context)) << aeron_errmsg();
}

struct ReplayAndControlChannels
{
    int expectedConcludeResult;
    const char *replayChannel;
    const char *controlRequestChannel;
    const char *controlResponseChannel;
};

class ReplayAndControlChannelsTest : public AeronArchivePersistentSubscriptionContextTest,
                                     public testing::WithParamInterface<ReplayAndControlChannels>
{
protected:
    DriverResource driver;
    AeronResource aeron{driver.aeronDir()};
};

TEST_P(ReplayAndControlChannelsTest, replayAndControlChannelMediaTypesMustMatchWhenUsingResponseChannels)
{
    const ReplayAndControlChannels& param = GetParam();

    aeron_archive_context_t *archive_context;
    ASSERT_EQ(0, aeron_archive_context_init(&archive_context)) << aeron_errmsg();
    aeron_archive_context_set_control_request_channel(archive_context, param.controlRequestChannel);
    aeron_archive_context_set_control_response_channel(archive_context, param.controlResponseChannel);

    aeron_archive_persistent_subscription_context_t *context;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_init(&context)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_archive_context(context, archive_context)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_recording_id(context, 0)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_live_channel(context, "aeron:ipc")) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_live_stream_id(context, 1000)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_replay_channel(context, param.replayChannel)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_replay_stream_id(context, 2000)) << aeron_errmsg();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_set_aeron(context, aeron.aeron())) << aeron_errmsg();
    EXPECT_EQ(param.expectedConcludeResult, aeron_archive_persistent_subscription_context_conclude(context)) << aeron_errmsg();

    if (0 == param.expectedConcludeResult)
    {
        const char *concluded_channel = aeron_archive_persistent_subscription_context_get_replay_channel(context);
        aeron_uri_string_builder_t builder;
        ASSERT_EQ(0, aeron_uri_string_builder_init_on_string(&builder, concluded_channel)) << aeron_errmsg();
        EXPECT_STREQ("false", aeron_uri_string_builder_get(&builder, AERON_URI_REJOIN_KEY))
            << "Expected rejoin=false in concluded replay channel: " << concluded_channel;
        aeron_uri_string_builder_close(&builder);
    }

    EXPECT_EQ(0, aeron_archive_persistent_subscription_context_close(context)) << aeron_errmsg();

    EXPECT_EQ(0, aeron_archive_context_close(archive_context)) << aeron_errmsg();
}

INSTANTIATE_TEST_SUITE_P(, ReplayAndControlChannelsTest, testing::Values(
    ReplayAndControlChannels{0, "aeron:udp?endpoint=localhost:0", nullptr, nullptr},
    ReplayAndControlChannels{0, "aeron:udp?endpoint=localhost:0", "aeron:udp?endpoint=localhost:8010", "aeron:udp?endpoint=localhost:0"},
    ReplayAndControlChannels{0, "aeron:udp?endpoint=localhost:0", "aeron:udp?endpoint=localhost:8010", "aeron:udp?control-mode=response|control=localhost:10002"},
    ReplayAndControlChannels{0, "aeron:udp?endpoint=localhost:0", "aeron:ipc", "aeron:ipc"},
    ReplayAndControlChannels{0, "aeron:udp?endpoint=localhost:0", "aeron:ipc", "aeron:ipc?control-mode=response"},
    ReplayAndControlChannels{0, "aeron:udp?control=localhost:10001|control-mode=response", nullptr, nullptr},
    ReplayAndControlChannels{0, "aeron:udp?control=localhost:10001|control-mode=response", "aeron:udp?endpoint=localhost:8010", "aeron:udp?endpoint=localhost:0"},
    ReplayAndControlChannels{0, "aeron:udp?control=localhost:10001|control-mode=response", "aeron:udp?endpoint=localhost:8010", "aeron:udp?control-mode=response|control=localhost:10002"},
    ReplayAndControlChannels{-1, "aeron:udp?control=localhost:10001|control-mode=response", "aeron:ipc", "aeron:ipc"},
    ReplayAndControlChannels{-1, "aeron:udp?control=localhost:10001|control-mode=response", "aeron:ipc", "aeron:ipc?control-mode=response"},
    ReplayAndControlChannels{0, "aeron:ipc", nullptr, nullptr},
    ReplayAndControlChannels{0, "aeron:ipc", "aeron:udp?endpoint=localhost:8010", "aeron:udp?endpoint=localhost:0"},
    ReplayAndControlChannels{0, "aeron:ipc", "aeron:udp?endpoint=localhost:8010", "aeron:udp?control-mode=response|control=localhost:10002"},
    ReplayAndControlChannels{0, "aeron:ipc", "aeron:ipc", "aeron:ipc"},
    ReplayAndControlChannels{0, "aeron:ipc", "aeron:ipc", "aeron:ipc?control-mode=response"},
    ReplayAndControlChannels{0, "aeron:ipc?control-mode=response", nullptr, nullptr},
    ReplayAndControlChannels{-1, "aeron:ipc?control-mode=response", "aeron:udp?endpoint=localhost:8010", "aeron:udp?endpoint=localhost:0"},
    ReplayAndControlChannels{-1, "aeron:ipc?control-mode=response", "aeron:udp?endpoint=localhost:8010", "aeron:udp?control-mode=response|control=localhost:10002"},
    ReplayAndControlChannels{0, "aeron:ipc?control-mode=response", "aeron:ipc", "aeron:ipc"},
    ReplayAndControlChannels{0, "aeron:ipc?control-mode=response", "aeron:ipc", "aeron:ipc?control-mode=response"}
));
