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

#include <array>
#include <gtest/gtest.h>

extern "C"
{
#include "aeron_network_publication.h"
#include "media/aeron_send_channel_endpoint.h"
#include "aeron_test_udp_bindings.h"
#include "aeron_driver_sender.h"
#include "aeron_position.h"

int aeron_driver_ensure_dir_is_recreated(aeron_driver_context_t *context);
}

#define CAPACITY (32 * 1024)

typedef std::array<std::uint8_t, CAPACITY> buffer_t;
typedef std::array<std::uint8_t, 4 * CAPACITY> buffer_4x_t;

class PositionTest : public testing::Test
{
protected:
    void SetUp() override
    {
        aeron_counters_manager_init(
            &m_counters_manager,
            m_counter_meta_buffer.data(), m_counter_meta_buffer.size(),
            m_counter_value_buffer.data(), m_counter_value_buffer.size(),
            &m_cached_clock,
            1000);
    }

    void TearDown() override
    {
        aeron_counters_manager_close(&m_counters_manager);
    }

    typedef struct counters_clientd_stct
    {
        int32_t id;
        const char *expected_channel;
        int32_t expended_channel_length;
    }
        counters_clientd_t;

    static void verify_channel_uri_channel_endpoint(
        int32_t id,
        int32_t type_id,
        const uint8_t *key,
        size_t key_length,
        const uint8_t *label,
        size_t label_length,
        void *clientd)
    {
        auto *counters_clientd = static_cast<PositionTest::counters_clientd_t *>(clientd);
        if (counters_clientd->id == id)
        {
            auto *layout = (aeron_channel_endpoint_status_key_layout_t *)key;
            ASSERT_EQ(counters_clientd->expended_channel_length, layout->channel_length);
            ASSERT_EQ(
                0,
                strncmp(counters_clientd->expected_channel, layout->channel, layout->channel_length));
        }
    }

    static void verify_channel_uri_stream_counter(
        int32_t id,
        int32_t type_id,
        const uint8_t *key,
        size_t key_length,
        const uint8_t *label,
        size_t label_length,
        void *clientd)
    {
        auto *counters_clientd = static_cast<PositionTest::counters_clientd_t *>(clientd);
        if (counters_clientd->id == id)
        {
            auto *layout = (aeron_stream_position_counter_key_layout_t *)key;
            ASSERT_EQ(counters_clientd->expended_channel_length, layout->channel_length);
            ASSERT_EQ(
                0,
                strncmp(counters_clientd->expected_channel, layout->channel, layout->channel_length));
        }
    }

protected:
    aeron_counters_manager_t m_counters_manager = {};
private:
    aeron_clock_cache_t m_cached_clock = {};
    AERON_DECL_ALIGNED(buffer_t m_counter_value_buffer, 16) = {};
    AERON_DECL_ALIGNED(buffer_4x_t m_counter_meta_buffer, 16) = {};
};

TEST_F(PositionTest, channelEndpointStatusShouldTruncateChannelUriIfTooLong)
{
    const std::string uri_prefix =
        "aeron:udp?endpoint=localhost:23245|mtu=1408|term-length=65536|term-offset=0|alias=very-long-alias-that-will-";
    const std::string full_uri = uri_prefix + "-be-truncated|sparse=true";

    const int32_t id = aeron_channel_endpoint_status_allocate(
        &m_counters_manager,
        "test-channel-endpoint",
        AERON_COUNTER_SEND_CHANNEL_STATUS_TYPE_ID,
        42,
        full_uri.length(),
        full_uri.c_str());
    ASSERT_NE(-1, id) << aeron_errmsg();

    counters_clientd_t clientd;
    clientd.id = id;
    clientd.expended_channel_length = (int32_t)uri_prefix.length();
    clientd.expected_channel = uri_prefix.c_str();

    aeron_counters_reader_foreach_metadata(
        m_counters_manager.metadata, m_counters_manager.metadata_length, verify_channel_uri_channel_endpoint, &clientd);
}

TEST_F(PositionTest, channelEndpointStatusShouldEncodeChannelUriDirectly)
{
    const std::string channel = "aeron:udp?endpoint=localhost:23245|mtu=1408|term-length=65536|term-offset=0";

    const int32_t id = aeron_channel_endpoint_status_allocate(
        &m_counters_manager,
        "test-receive-endpoint",
        AERON_COUNTER_RECEIVE_CHANNEL_STATUS_TYPE_ID,
        21,
        channel.length(),
        channel.c_str());
    ASSERT_NE(-1, id) << aeron_errmsg();

    counters_clientd_t clientd;
    clientd.id = id;
    clientd.expended_channel_length = (int32_t)channel.length();
    clientd.expected_channel = channel.c_str();

    aeron_counters_reader_foreach_metadata(
        m_counters_manager.metadata, m_counters_manager.metadata_length, verify_channel_uri_channel_endpoint, &clientd);
}


TEST_F(PositionTest, streamCounterShouldTruncateChannelUriIfTooLong)
{
    const std::string uri_prefix = "aeron:udp?endpoint=localhost:23245|mtu=1408|term-length=65536|term-offset=0|alias=that-will-";
    const std::string full_uri = uri_prefix + "be-truncated|sparse=true";

    const int32_t id = aeron_stream_counter_allocate(
        &m_counters_manager,
        AERON_COUNTER_SENDER_BPE_NAME,
        AERON_COUNTER_SENDER_BPE_TYPE_ID,
        -143,
        42,
        5,
        -189,
        full_uri.length(),
        full_uri.c_str(),
        "");
    ASSERT_NE(-1, id) << aeron_errmsg();

    counters_clientd_t clientd;
    clientd.id = id;
    clientd.expended_channel_length = (int32_t)uri_prefix.length();
    clientd.expected_channel = uri_prefix.c_str();

    aeron_counters_reader_foreach_metadata(
        m_counters_manager.metadata, m_counters_manager.metadata_length, verify_channel_uri_stream_counter, &clientd);
}

TEST_F(PositionTest, streamCounterShouldEncodeChannelUriDirectly)
{
    const std::string channel = "aeron:udp?endpoint=localhost:23245|mtu=1408|term-length=65536|term-offset=0";

    const int32_t id = aeron_stream_counter_allocate(
        &m_counters_manager,
        AERON_COUNTER_PUBLISHER_LIMIT_NAME,
        AERON_COUNTER_PUBLISHER_LIMIT_TYPE_ID,
        821,
        42,
        5,
        -189,
        channel.length(),
        channel.c_str(),
        "other");
    ASSERT_NE(-1, id) << aeron_errmsg();

    counters_clientd_t clientd;
    clientd.id = id;
    clientd.expended_channel_length = (int32_t)channel.length();
    clientd.expected_channel = channel.c_str();

    aeron_counters_reader_foreach_metadata(
        m_counters_manager.metadata, m_counters_manager.metadata_length, verify_channel_uri_stream_counter, &clientd);

    int64_t owner_id;
    aeron_counters_reader_t countersReader = {};
    ASSERT_EQ(0, aeron_counters_reader_init(
        &countersReader,
        m_counters_manager.metadata,
        m_counters_manager.metadata_length,
        m_counters_manager.values,
        m_counters_manager.values_length));
    aeron_counters_reader_counter_owner_id(&countersReader, id, &owner_id);
    EXPECT_EQ(821, owner_id);
}
