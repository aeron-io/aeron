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

#include <gtest/gtest.h>

#include "client/archive/AeronArchive.h"
#include "client/archive/PersistentSubscription.h"

using namespace aeron;
using namespace aeron::archive::client;

static const std::int32_t STREAM_ID = 1000;
static const std::int32_t REPLAY_STREAM_ID = -5;
static const std::string CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8010";
static const std::string CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";

class PersistentSubscriptionContextWrapperTest : public testing::Test
{
};

TEST_F(PersistentSubscriptionContextWrapperTest, shouldThrowIfArchiveContextNotSet)
{
    PersistentSubscription::Context ctx;
    ctx.recordingId(0)
        .startPosition(0)
        .liveChannel("aeron:ipc")
        .liveStreamId(STREAM_ID)
        .replayChannel("aeron:udp?endpoint=localhost:0")
        .replayStreamId(REPLAY_STREAM_ID);

    ASSERT_THROW(PersistentSubscription::create(ctx), util::IllegalArgumentException);
}

TEST_F(PersistentSubscriptionContextWrapperTest, shouldThrowIfRecordingIdNotSet)
{
    const std::shared_ptr<AeronArchive::Context_t> archiveCtx = std::make_shared<AeronArchive::Context_t>();
    archiveCtx->controlRequestChannel(CONTROL_REQUEST_CHANNEL)
        .controlResponseChannel(CONTROL_RESPONSE_CHANNEL);

    PersistentSubscription::Context ctx;
    ctx.archiveContext(archiveCtx)
        .startPosition(0)
        .liveChannel("aeron:ipc")
        .liveStreamId(STREAM_ID)
        .replayChannel("aeron:udp?endpoint=localhost:0")
        .replayStreamId(REPLAY_STREAM_ID);

    ASSERT_THROW(PersistentSubscription::create(ctx), util::IllegalArgumentException);
}

TEST_F(PersistentSubscriptionContextWrapperTest, shouldThrowIfLiveChannelNotSet)
{
    const std::shared_ptr<AeronArchive::Context_t> archiveCtx = std::make_shared<AeronArchive::Context_t>();
    archiveCtx->controlRequestChannel(CONTROL_REQUEST_CHANNEL)
        .controlResponseChannel(CONTROL_RESPONSE_CHANNEL);

    PersistentSubscription::Context ctx;
    ctx.archiveContext(archiveCtx)
        .recordingId(0)
        .startPosition(0)
        .liveStreamId(STREAM_ID)
        .replayChannel("aeron:udp?endpoint=localhost:0")
        .replayStreamId(REPLAY_STREAM_ID);

    ASSERT_THROW(PersistentSubscription::create(ctx), util::IllegalArgumentException);
}

TEST_F(PersistentSubscriptionContextWrapperTest, shouldThrowIfLiveStreamIdNotSet)
{
    const std::shared_ptr<AeronArchive::Context_t> archiveCtx = std::make_shared<AeronArchive::Context_t>();
    archiveCtx->controlRequestChannel(CONTROL_REQUEST_CHANNEL)
        .controlResponseChannel(CONTROL_RESPONSE_CHANNEL);

    PersistentSubscription::Context ctx;
    ctx.archiveContext(archiveCtx)
        .recordingId(0)
        .startPosition(0)
        .liveChannel("aeron:ipc")
        // liveStreamId deliberately not set
        .replayChannel("aeron:udp?endpoint=localhost:0")
        .replayStreamId(REPLAY_STREAM_ID);

    ASSERT_THROW(PersistentSubscription::create(ctx), util::IllegalArgumentException);
}

TEST_F(PersistentSubscriptionContextWrapperTest, shouldThrowIfReplayChannelNotSet)
{
    const std::shared_ptr<AeronArchive::Context_t> archiveCtx = std::make_shared<AeronArchive::Context_t>();
    archiveCtx->controlRequestChannel(CONTROL_REQUEST_CHANNEL)
        .controlResponseChannel(CONTROL_RESPONSE_CHANNEL);

    PersistentSubscription::Context ctx;
    ctx.archiveContext(archiveCtx)
        .recordingId(0)
        .startPosition(0)
        .liveChannel("aeron:ipc")
        .liveStreamId(STREAM_ID)
        .replayStreamId(REPLAY_STREAM_ID);

    ASSERT_THROW(PersistentSubscription::create(ctx), util::IllegalArgumentException);
}

TEST_F(PersistentSubscriptionContextWrapperTest, shouldThrowIfReplayStreamIdNotSet)
{
    const std::shared_ptr<AeronArchive::Context_t> archiveCtx = std::make_shared<AeronArchive::Context_t>();
    archiveCtx->controlRequestChannel(CONTROL_REQUEST_CHANNEL)
        .controlResponseChannel(CONTROL_RESPONSE_CHANNEL);

    PersistentSubscription::Context ctx;
    ctx.archiveContext(archiveCtx)
        .recordingId(0)
        .startPosition(0)
        .liveChannel("aeron:ipc")
        .liveStreamId(STREAM_ID)
        .replayChannel("aeron:udp?endpoint=localhost:0");
        // replayStreamId deliberately not set

    ASSERT_THROW(PersistentSubscription::create(ctx), util::IllegalArgumentException);
}

TEST_F(PersistentSubscriptionContextWrapperTest, shouldThrowIfStartPositionIsInvalid)
{
    std::shared_ptr<AeronArchive::Context_t> archiveCtx = std::make_shared<AeronArchive::Context_t>();
    archiveCtx->controlRequestChannel(CONTROL_REQUEST_CHANNEL)
        .controlResponseChannel(CONTROL_RESPONSE_CHANNEL);

    PersistentSubscription::Context ctx;
    ctx.archiveContext(archiveCtx)
        .recordingId(0)
        .startPosition(-3) // invalid: less than FROM_LIVE (-2)
        .liveChannel("aeron:ipc")
        .liveStreamId(STREAM_ID)
        .replayChannel("aeron:udp?endpoint=localhost:0")
        .replayStreamId(REPLAY_STREAM_ID);

    ASSERT_THROW(PersistentSubscription::create(ctx), util::IllegalArgumentException);
}

TEST_F(PersistentSubscriptionContextWrapperTest, shouldThrowIfRecordingIdIsInvalid)
{
    std::shared_ptr<AeronArchive::Context_t> archiveCtx = std::make_shared<AeronArchive::Context_t>();
    archiveCtx->controlRequestChannel(CONTROL_REQUEST_CHANNEL)
        .controlResponseChannel(CONTROL_RESPONSE_CHANNEL);

    PersistentSubscription::Context ctx;
    ctx.archiveContext(archiveCtx)
        .recordingId(-5)
        .startPosition(0)
        .liveChannel("aeron:ipc")
        .liveStreamId(STREAM_ID)
        .replayChannel("aeron:udp?endpoint=localhost:0")
        .replayStreamId(REPLAY_STREAM_ID);

    ASSERT_THROW(PersistentSubscription::create(ctx), util::IllegalArgumentException);
}
