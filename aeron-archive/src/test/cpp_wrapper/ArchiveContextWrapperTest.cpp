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

#include "client/archive/AeronArchive.h"

extern "C"
{
#include "client/aeron_archive_context.h"
#include "util/aeron_env.h"
}

using namespace aeron::archive::client;

class ArchiveContextWrapperTest : public testing::Test
{
protected:
    void SetUp() override
    {
        // the C context only sets these channels from the environment, otherwise they are NULL
        aeron_env_unset(AERON_ARCHIVE_CONTROL_CHANNEL_ENV_VAR);
        aeron_env_unset(AERON_ARCHIVE_CONTROL_RESPONSE_CHANNEL_ENV_VAR);
        aeron_env_unset(AERON_ARCHIVE_RECORDING_EVENTS_CHANNEL_ENV_VAR);
    }
};

TEST_F(ArchiveContextWrapperTest, shouldReturnEmptyChannelsWhenNotSet)
{
    Context context;

    EXPECT_EQ("", context.controlRequestChannel());
    EXPECT_EQ("", context.controlResponseChannel());
    EXPECT_EQ("", context.recordingEventsChannel());
}

TEST_F(ArchiveContextWrapperTest, shouldReturnChannelsThatWereSet)
{
    Context context;
    context
        .controlRequestChannel("aeron:udp?endpoint=localhost:8010")
        .controlResponseChannel("aeron:udp?endpoint=localhost:0")
        .recordingEventsChannel("aeron:udp?control-mode=dynamic|control=localhost:8030");

    EXPECT_EQ("aeron:udp?endpoint=localhost:8010", context.controlRequestChannel());
    EXPECT_EQ("aeron:udp?endpoint=localhost:0", context.controlResponseChannel());
    EXPECT_EQ("aeron:udp?control-mode=dynamic|control=localhost:8030", context.recordingEventsChannel());
}
