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
#include "media/aeron_udp_channel_transport.h"
#include "media/aeron_udp_channel_transport_multi_gap_loss.h"
#include "protocol/aeron_udp_protocol.h"
}

#define TEMP_URL_LEN (128)

typedef struct delegate_recv_state_stct
{
    int messages_received;
}
delegate_recv_state_t;

void aeron_udp_channel_interceptor_multi_gap_loss_incoming_delegate(
    void *interceptor_state,
    aeron_udp_channel_incoming_interceptor_t *delegate,
    aeron_udp_channel_transport_t *transport,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr,
    struct timespec *media_timestamp)
{
    ((delegate_recv_state_t *)interceptor_state)->messages_received++;
}

class UdpChannelTransportMultiGapLossTest : public testing::Test
{
public:
    UdpChannelTransportMultiGapLossTest()
    {
        m_delegate_recv_state = { 0 };

        m_delegate.incoming_func = aeron_udp_channel_interceptor_multi_gap_loss_incoming_delegate;
        m_delegate.interceptor_state = &m_delegate_recv_state;
        m_interceptor_state = nullptr;
    }

    void init(const char *const_uri)
    {
        aeron_udp_channel_interceptor_multi_gap_loss_init_incoming(&m_interceptor_state, nullptr, AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_SENDER);
        parse_params(&m_params, const_uri);
        aeron_udp_channel_interceptor_multi_gap_loss_configure(&m_params);
    }

    void close() const
    {
        aeron_udp_channel_interceptor_multi_gap_loss_close_incoming(m_interceptor_state);
    }

    static void parse_params(aeron_udp_channel_interceptor_multi_gap_loss_params_t *params, const char *const_uri)
    {
        char uri[TEMP_URL_LEN + 1];
        strncpy(uri, const_uri, TEMP_URL_LEN);

        EXPECT_EQ(aeron_udp_channel_interceptor_multi_gap_loss_parse_params(uri, params), 0);
    }

    void incoming(int32_t offset, size_t length, bool should_drop)
    {
        uint8_t buffer[1024];

        auto *data_header = (aeron_data_header_t *)buffer;
        data_header->frame_header.type = AERON_HDR_TYPE_DATA;
        data_header->stream_id = 123;
        data_header->session_id = 456;
        data_header->term_id = 0;
        data_header->term_offset = offset;

        int expected_messages_received = m_delegate_recv_state.messages_received + (should_drop ? 0 : 1);
        aeron_udp_channel_interceptor_multi_gap_loss_incoming(
            m_interceptor_state, &m_delegate, nullptr, nullptr, nullptr, nullptr, buffer, length, nullptr, nullptr);
        EXPECT_EQ(expected_messages_received, m_delegate_recv_state.messages_received);
    }

    aeron_udp_channel_interceptor_multi_gap_loss_params_t m_params = {};
    aeron_udp_channel_incoming_interceptor_t m_delegate = {};
    delegate_recv_state_t m_delegate_recv_state = {};
    void *m_interceptor_state;
};

TEST_F(UdpChannelTransportMultiGapLossTest, singleSmallGap)
{
    init("term-id=0|gap-radix=8|gap-length=4|total-gaps=1");

    incoming(0, 8, false);
    incoming(8, 8, true);
    incoming(16, 8, false);
    incoming(24, 8, false);

    close();
}

TEST_F(UdpChannelTransportMultiGapLossTest, singleSmallGapRx)
{
    init("term-id=0|gap-radix=8|gap-length=4|total-gaps=1");

    incoming(0, 8, false);
    incoming(8, 8, true);
    incoming(16, 8, false);

    incoming(8, 8, false);

    close();
}

TEST_F(UdpChannelTransportMultiGapLossTest, multiSmallGap)
{
    init("term-id=0|gap-radix=16|gap-length=8|total-gaps=4");

    incoming(0, 8, false);
    incoming(8, 8, false);
    incoming(16, 8, true);
    incoming(24, 8, false);
    incoming(32, 8, true);
    incoming(40, 8, false);
    incoming(48, 4, true);
    incoming(52, 8, true);
    incoming(60, 2, false);
    incoming(62, 10, true);

    close();
}

TEST_F(UdpChannelTransportMultiGapLossTest, shouldParseAllParams)
{
    aeron_udp_channel_interceptor_multi_gap_loss_params_t params;
    parse_params(&params, "term-id=1|gap-radix=16|gap-length=4|total-gaps=10");

    EXPECT_EQ(params.term_id, 1);
    EXPECT_EQ(params.gap_radix, 16);
    EXPECT_EQ(params.gap_length, 4);
    EXPECT_EQ(params.total_gaps, 10);

    EXPECT_EQ(params.gap_radix_bits, 4);
    EXPECT_EQ(params.gap_radix_mask, ~(0xF));
    EXPECT_EQ(params.last_gap_limit, 164);
}
