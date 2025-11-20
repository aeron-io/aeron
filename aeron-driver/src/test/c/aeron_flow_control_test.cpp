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
#include "util/aeron_properties_util.h"
#include "protocol/aeron_udp_protocol.h"
#include "aeron_flow_control.h"
#include "media/aeron_udp_channel.h"
#include "aeron_driver_context.h"
}

#define WINDOW_LENGTH (2000)

typedef std::array<std::uint8_t, 1024> buffer_t;

static int64_t now()
{
    return 123123;
}

#define FREE_TO_REUSE_TIMEOUT_MS (1000L)

class FlowControlTest : public testing::Test
{
public:
    FlowControlTest()
    {
        aeron_default_name_resolver_supplier(&m_resolver, nullptr, nullptr);
    };

    int64_t apply_status_message(
        aeron_flow_control_strategy_t *strategy,
        int64_t receiver_id,
        int32_t term_offset,
        int64_t gtag,
        int64_t now_ns = 0,
        bool send_gtag = true,
        int64_t snd_lmt = 0)
    {
        uint8_t msg[1024];
        auto *sm = (aeron_status_message_header_t *)msg;
        auto *sm_optional = (aeron_status_message_optional_header_t *)(msg + sizeof(aeron_status_message_header_t));

        sm->frame_header.frame_length = send_gtag ?
            sizeof(aeron_status_message_header_t) + sizeof(aeron_status_message_optional_header_t) :
            sizeof(aeron_status_message_header_t);
        sm->frame_header.flags = 0;
        sm->consumption_term_id = 0;
        sm->consumption_term_offset = term_offset;
        sm->receiver_window = WINDOW_LENGTH;
        sm->receiver_id = receiver_id;
        sm_optional->group_tag = gtag;

        return strategy->on_status_message(
            strategy->state, (uint8_t *)sm, sizeof(struct sockaddr_storage), &address, snd_lmt, 0, 0, now_ns);
    }

    int64_t apply_old_asf_status_message(
        aeron_flow_control_strategy_t *strategy,
        int64_t receiver_id,
        int32_t term_offset,
        int32_t asf_value)
    {
        uint8_t msg[1024];
        auto *sm = (aeron_status_message_header_t *)msg;
        auto *asf = (int32_t *)(msg + sizeof(aeron_status_message_header_t));

        sm->frame_header.frame_length = sizeof(aeron_status_message_header_t) + sizeof(int32_t);
        sm->frame_header.flags = 0;
        sm->consumption_term_id = 0;
        sm->consumption_term_offset = term_offset;
        sm->receiver_window = WINDOW_LENGTH;
        sm->receiver_id = receiver_id;
        *asf = asf_value;

        return strategy->on_status_message(
            strategy->state, (uint8_t *)sm, sizeof(struct sockaddr_storage), &address, sizeof(address), 0, 0, 0);
    }

    void initialise_channel(const char *uri)
    {
        aeron_udp_channel_parse(strlen(uri), uri, &m_resolver, &m_channel, false);
        m_channels.push_back(m_channel);
    }

    struct sockaddr_storage address = {};
    aeron_udp_channel_t *m_channel = nullptr;
    aeron_driver_context_t *context = nullptr;
    aeron_distinct_error_log_t error_log = {};
    buffer_t buffer = {};
    aeron_name_resolver_t m_resolver= {};
    aeron_flow_control_strategy_t *m_strategy = nullptr;
    std::vector<aeron_udp_channel_t *> m_channels;

    static const size_t NUM_COUNTERS = 4;
    std::array<std::uint8_t, NUM_COUNTERS * AERON_COUNTERS_MANAGER_METADATA_LENGTH> m_counters_metadata = {};
    std::array<std::uint8_t, NUM_COUNTERS * AERON_COUNTERS_MANAGER_VALUE_LENGTH> m_counters_values = {};
    aeron_counters_manager_t m_counters_manager = {};
    aeron_clock_cache_t m_cached_clock = {};

protected:
    void TearDown() override
    {
        for (auto channel : m_channels)
        {
            aeron_udp_channel_delete(channel);
        }

        if (nullptr != m_strategy)
        {
            m_strategy->fini(m_strategy);
        }

        aeron_driver_context_close(context);
        aeron_distinct_error_log_close(&error_log);
        aeron_counters_manager_close(&m_counters_manager);
    }

    void SetUp() override
    {
        m_channel = nullptr;
        m_strategy = nullptr;
        aeron_distinct_error_log_init(&error_log, buffer.data(), buffer.size(), now);
        aeron_driver_context_init(&context);
        context->error_log = &error_log;
        context->multicast_flow_control_supplier_func = aeron_min_flow_control_strategy_supplier;

        m_counters_metadata.fill(0);
        m_counters_values.fill(0);
        aeron_counters_manager_init(
            &m_counters_manager,
            m_counters_metadata.data(),
            m_counters_metadata.size(),
            m_counters_values.data(),
            m_counters_values.size(),
            &m_cached_clock,
            0);
    }
};

class TaggedFlowControlTest : public FlowControlTest
{
};

class MinFlowControlTest : public FlowControlTest
{
};

class MaxFlowControlTest : public FlowControlTest
{
};

class ParameterisedSuccessfulOptionsParsingTest :
    public testing::TestWithParam<std::tuple<const char *, const char *, uint64_t, bool, int32_t, bool, int32_t>>
{
};

class ParameterisedFailingOptionsParsingTest :
    public testing::TestWithParam<std::tuple<const char *, int>>
{
};

class RetransmitReceiverWindowMultipleParsingTest :
    public testing::TestWithParam<std::tuple<const char *, size_t, int>>
{
};

TEST_F(MinFlowControlTest, shouldFallbackToMinStrategy)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001,0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(m_strategy, 1, 1000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(m_strategy, 2, 2000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 994, apply_status_message(m_strategy, 3, 994, 2));
}

TEST_F(MinFlowControlTest, shouldUseMinStrategy)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(m_strategy, 1, 1000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(m_strategy, 2, 2000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 994, apply_status_message(m_strategy, 3, 994, 2));
}


INSTANTIATE_TEST_SUITE_P(
    RetransmitReceiverWindowMultipleParsingTests,
    RetransmitReceiverWindowMultipleParsingTest,
    testing::Values(
        std::make_tuple("min,rrwm:2", 2, 0),
        std::make_tuple("min,rrwm:2,t:100ms", 2, 0),
        std::make_tuple("min,g:0,rrwm:2,t:100ms", 2, 0),
        std::make_tuple("min,rrwm:12,t:100ms", 12, 0),
        std::make_tuple("min,rrwm:0", 0, EINVAL),
        std::make_tuple("min,rrwm:-1", 0, EINVAL),
        std::make_tuple("min,rrwm:foo", 0, EINVAL),
        std::make_tuple("min,rrwm: ", 0, EINVAL),
        std::make_tuple("min,rrwm:,t:100ms", 0, EINVAL),
        std::make_tuple("min,rrwm:", 0, EINVAL)
    )
);

TEST_P(RetransmitReceiverWindowMultipleParsingTest, parsesRetransmitReceiverWindowMultipleFromUri)
{
    const char *fc_options = std::get<0>(GetParam());
    const size_t expectedRrwm = std::get<1>(GetParam());
    const int expectedErrorCode = std::get<2>(GetParam());
    const int expectedReturnCode = expectedErrorCode == 0 ? 1 : -1;

    aeron_flow_control_tagged_options_t options;
    options.multicast_flow_control_rrwm = 0;
    int returnCode = aeron_flow_control_parse_tagged_options(strlen(fc_options), fc_options, &options);

    ASSERT_EQ(expectedReturnCode, returnCode);
    ASSERT_EQ(expectedRrwm, options.multicast_flow_control_rrwm);
    if (1 != expectedReturnCode)
    {
        ASSERT_EQ(expectedErrorCode, aeron_errcode());
    }
}

TEST_F(FlowControlTest, unicastStrategyShouldUseDefaultWindowMultipleWhenNotSet)
{
    size_t expected_rrwm = AERON_UNICAST_FLOW_CONTROL_RETRANSMIT_RECEIVER_WINDOW_MULTIPLE; // <-- expect default
    size_t window_length = aeron_driver_context_get_rcv_initial_window_length(nullptr);
    uint64_t expected_retransmit_length = window_length * expected_rrwm;

    initialise_channel("aeron:udp?endpoint=localhost");

    ASSERT_EQ(0, aeron_unicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    size_t rrwm = m_strategy->max_retransmission_length(m_strategy->state, 0, 3000000, 1024 * 1024 * 3, 1408);
    ASSERT_EQ(expected_retransmit_length, rrwm);
}

TEST_F(FlowControlTest, unicastStrategyShouldUseWindowMultipleFromContext)
{
    size_t expected_rrwm = 2;
    size_t window_length = aeron_driver_context_get_rcv_initial_window_length(nullptr);
    uint64_t expected_retransmit_length = window_length * expected_rrwm;

    initialise_channel("aeron:udp?endpoint=localhost");
    context->unicast_flow_control_rrwm = expected_rrwm; // <-- expect context value

    ASSERT_EQ(0, aeron_unicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    size_t rrwm = m_strategy->max_retransmission_length(m_strategy->state, 0, 3000000, 1024 * 1024 * 3, 1408);
    ASSERT_EQ(expected_retransmit_length, rrwm);
}

TEST_F(MinFlowControlTest, minStrategyShouldUseDefaultWindowMultipleWhenNotSet)
{
    size_t expected_rrwm = AERON_MULTICAST_FLOW_CONTROL_RETRANSMIT_RECEIVER_WINDOW_MULTIPLE; // <-- expect default
    size_t window_length = aeron_driver_context_get_rcv_initial_window_length(nullptr);
    uint64_t expected_retransmit_length = window_length * expected_rrwm;

    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|fc=min");

    ASSERT_EQ(0, aeron_min_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    size_t rrwm = m_strategy->max_retransmission_length(m_strategy->state, 0, 1000000, 1024 * 1024, 1408);
    ASSERT_EQ(expected_retransmit_length, rrwm);
}

TEST_F(MinFlowControlTest, minStrategyShouldUseDefaultWindowMultipleFromContext)
{
    size_t expected_rrwm = 2;
    size_t window_length = aeron_driver_context_get_rcv_initial_window_length(nullptr);
    uint64_t expected_retransmit_length = window_length * expected_rrwm;

    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|fc=min");

    context->multicast_flow_control_rrwm = expected_rrwm; // <-- expect context value

    ASSERT_EQ(0, aeron_min_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    size_t rrwm = m_strategy->max_retransmission_length(m_strategy->state, 0, 1000000, 1024 * 1024, 1408);
    ASSERT_EQ(expected_retransmit_length, rrwm);
}

TEST_F(MinFlowControlTest, minStrategyShouldUseDefaultWindowMultipleFromUri)
{
    size_t expected_rrwm = 6;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|fc=min,rrwm:6");  // <-- expect uri value

    size_t window_length = aeron_driver_context_get_rcv_initial_window_length(nullptr);
    uint64_t expected_retransmit_length = window_length * expected_rrwm;

    context->multicast_flow_control_rrwm = 2;

    ASSERT_EQ(0, aeron_min_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    size_t rrwm = m_strategy->max_retransmission_length(m_strategy->state, 0, 1000000, 1024 * 1024, 1408);
    ASSERT_EQ(expected_retransmit_length, rrwm);
}

TEST_F(TaggedFlowControlTest, taggedStrategyShouldUseDefaultWindowMultipleWhenNotSet)
{
    size_t expected_rrwm = AERON_MULTICAST_FLOW_CONTROL_RETRANSMIT_RECEIVER_WINDOW_MULTIPLE; // <-- expect default
    size_t window_length = aeron_driver_context_get_rcv_initial_window_length(nullptr);
    uint64_t expected_retransmit_length = window_length * expected_rrwm;

    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|fc=tagged");

    ASSERT_EQ(0, aeron_tagged_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    size_t rrwm = m_strategy->max_retransmission_length(m_strategy->state, 0, 1000000, 1024 * 1024, 1408);
    ASSERT_EQ(expected_retransmit_length, rrwm);
}

TEST_F(TaggedFlowControlTest, taggedStrategyShouldUseDefaultWindowMultipleFromContext)
{
    size_t expected_rrwm = 2;
    size_t window_length = aeron_driver_context_get_rcv_initial_window_length(nullptr);
    uint64_t expected_retransmit_length = window_length * expected_rrwm;

    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|fc=tagged");

    context->multicast_flow_control_rrwm = expected_rrwm; // <-- expect context value

    ASSERT_EQ(0, aeron_tagged_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    size_t rrwm = m_strategy->max_retransmission_length(m_strategy->state, 0, 1000000, 1024 * 1024, 1408);
    ASSERT_EQ(expected_retransmit_length, rrwm);
}

TEST_F(TaggedFlowControlTest, taggedStrategyShouldUseDefaultWindowMultipleFromUri)
{
    size_t expected_rrwm = 6;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|fc=tagged,rrwm:6");  // <-- expect uri value

    size_t window_length = aeron_driver_context_get_rcv_initial_window_length(nullptr);
    uint64_t expected_retransmit_length = window_length * expected_rrwm;

    context->multicast_flow_control_rrwm = 2;

    ASSERT_EQ(0, aeron_tagged_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    size_t rrwm = m_strategy->max_retransmission_length(m_strategy->state, 0, 1000000, 1024 * 1024, 1408);
    ASSERT_EQ(expected_retransmit_length, rrwm);
}

TEST_F(MaxFlowControlTest, maxStrategyShouldUseDefaultWindowMultipleWhenNotSet)
{
    size_t expected_rrwm = AERON_MULTICAST_FLOW_CONTROL_RETRANSMIT_RECEIVER_WINDOW_MULTIPLE; // <-- expect default
    size_t window_length = aeron_driver_context_get_rcv_initial_window_length(nullptr);
    uint64_t expected_retransmit_length = window_length * expected_rrwm;

    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|fc=max");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    size_t rrwm = m_strategy->max_retransmission_length(m_strategy->state, 0, 1000000, 1024 * 1024, 1408);
    ASSERT_EQ(expected_retransmit_length, rrwm);
}

TEST_F(MaxFlowControlTest, maxStrategyShouldUseDefaultWindowMultipleFromContext)
{
    size_t expected_rrwm = 2;
    size_t window_length = aeron_driver_context_get_rcv_initial_window_length(nullptr);
    uint64_t expected_retransmit_length = window_length * expected_rrwm;

    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|fc=max");

    context->multicast_flow_control_rrwm = expected_rrwm; // <-- expect context value

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    size_t rrwm = m_strategy->max_retransmission_length(m_strategy->state, 0, 1000000, 1024 * 1024, 1408);
    ASSERT_EQ(expected_retransmit_length, rrwm);
}

TEST_F(MaxFlowControlTest, maxStrategyShouldUseDefaultWindowMultipleFromUri)
{
    size_t expected_rrwm = 6;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|fc=max,rrwm:6");  // <-- expect uri value

    size_t window_length = aeron_driver_context_get_rcv_initial_window_length(nullptr);
    uint64_t expected_retransmit_length = window_length * expected_rrwm;

    context->multicast_flow_control_rrwm = 2;

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    size_t rrwm = m_strategy->max_retransmission_length(m_strategy->state, 0, 1000000, 1024 * 1024, 1408);
    ASSERT_EQ(expected_retransmit_length, rrwm);
}

TEST_F(FlowControlTest, shouldUseMinStrategyAndIgnoreGroupParams)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:123");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(m_strategy, 1, 1000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(m_strategy, 2, 2000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 994, apply_status_message(m_strategy, 3, 994, 2));
}

TEST_F(MinFlowControlTest, shouldTimeoutWithMinStrategy)
{
    const int64_t sender_limit = 5000;
    const int64_t position_recv_1 = 1000;
    const int64_t position_recv_2 = 2000;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,t:500ms");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));
    ASSERT_NE(nullptr, m_strategy);

    ASSERT_EQ(WINDOW_LENGTH + position_recv_1, apply_status_message(m_strategy, 1, position_recv_1, 0, 100 * 1000000));
    ASSERT_EQ(WINDOW_LENGTH + position_recv_1, apply_status_message(m_strategy, 2, position_recv_2, 0, 200 * 1000000));

    ASSERT_EQ(
        WINDOW_LENGTH + position_recv_1, m_strategy->on_idle(m_strategy->state, 599 * 1000000, sender_limit, 0, false));
    ASSERT_EQ(
        WINDOW_LENGTH + position_recv_2, m_strategy->on_idle(m_strategy->state, 601 * 1000000, sender_limit, 0, false));
    ASSERT_EQ(sender_limit, m_strategy->on_idle(m_strategy->state, 701 * 1000000, sender_limit, 0, false));
}

TEST_F(MaxFlowControlTest, shouldFallbackToMaxStrategy)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost");
    context->multicast_flow_control_supplier_func = aeron_max_multicast_flow_control_strategy_supplier;

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(m_strategy, 1, 1000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 2000, apply_status_message(m_strategy, 2, 2000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 994, apply_status_message(m_strategy, 3, 994, 2));
}

TEST_F(MaxFlowControlTest, shouldUseMaxStrategy)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(m_strategy, 1, 1000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 2000, apply_status_message(m_strategy, 2, 2000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 994, apply_status_message(m_strategy, 3, 994, 2));
}

TEST_F(TaggedFlowControlTest, shouldUseFallbackToTaggedStrategy)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max");
    context->multicast_flow_control_supplier_func = aeron_tagged_flow_control_strategy_supplier;
    context->receiver_group_tag.is_present = true;
    context->receiver_group_tag.value = 1;
    context->flow_control.group_tag = 1;
    context->flow_control.group_min_size = 0;

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(m_strategy, 1, 1000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 2000, apply_status_message(m_strategy, 2, 2000, 1));
    ASSERT_EQ(WINDOW_LENGTH + 994, apply_status_message(m_strategy, 3, 994, 2));
}

TEST_F(TaggedFlowControlTest, shouldUseTaggedStrategy)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(m_strategy, 1, 1000, 123));
    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(m_strategy, 2, 2000, 123));
    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(m_strategy, 3, 994, 0));
}

TEST_F(TaggedFlowControlTest, shouldAllowUseTaggedStrategyWhenGroupMissing)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));
}

TEST_F(TaggedFlowControlTest, shouldUseTaggedStrategyWith8ByteTag)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:3000000000");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(m_strategy, 1, 1000, 3000000000));
    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(m_strategy, 2, 2000, 3000000000));
    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(m_strategy, 3, 994, -1294967296));
}

TEST_F(TaggedFlowControlTest, shouldUseTaggedStrategyWithOldAsfValue)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    size_t initial_observations = aeron_distinct_error_log_num_observations(&error_log);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_old_asf_status_message(m_strategy, 1, 1000, 123));

    ASSERT_LT(initial_observations, aeron_distinct_error_log_num_observations(&error_log));
}

TEST_F(TaggedFlowControlTest, shouldUsePositionAndWindowFromStatusMessageWhenReceiverAreEmpty)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_NE(nullptr, m_strategy);

    ASSERT_EQ(WINDOW_LENGTH + 1000, apply_status_message(m_strategy, 1, 1000, 0));
}

TEST_F(TaggedFlowControlTest, shouldAlwaysUsePositionFromReceiversIfPresent)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    int tagged_term_offset = 1000;
    ASSERT_EQ(WINDOW_LENGTH + tagged_term_offset, apply_status_message(m_strategy, 1, tagged_term_offset, 123));
    ASSERT_EQ(WINDOW_LENGTH + tagged_term_offset, apply_status_message(m_strategy, 1, tagged_term_offset + 1, -1));
    ASSERT_EQ(WINDOW_LENGTH + tagged_term_offset, apply_status_message(m_strategy, 1, tagged_term_offset - 1, -1));
}

TEST_F(TaggedFlowControlTest, shouldTimeout)
{
    const int64_t sender_position = 5000;
    const int64_t position_recv_1 = 1000;
    const int64_t position_recv_2 = 2000;
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123,t:500ms");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));
    ASSERT_NE(nullptr, m_strategy);

    ASSERT_EQ(position_recv_1 + WINDOW_LENGTH,
              apply_status_message(m_strategy, 1, position_recv_1, 123, 100 * 1000000));
    ASSERT_EQ(position_recv_1 + WINDOW_LENGTH,
              apply_status_message(m_strategy, 2, position_recv_2, 123, 200 * 1000000));

    ASSERT_EQ(
        position_recv_1 + WINDOW_LENGTH,
        m_strategy->on_idle(m_strategy->state, 599 * 1000000, sender_position, 0, false));
    ASSERT_EQ(
        position_recv_2 + WINDOW_LENGTH,
        m_strategy->on_idle(m_strategy->state, 601 * 1000000, sender_position, 0, false));
    ASSERT_EQ(sender_position, m_strategy->on_idle(m_strategy->state, 701 * 1000000, sender_position, 0, false));
}

TEST_F(MaxFlowControlTest, shouldAlwaysHaveRequiredReceivers)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_TRUE(m_strategy->has_required_receivers(m_strategy));
}

TEST_F(MinFlowControlTest, shouldAlwaysHaveRequiredReceiversByDefault)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_TRUE(m_strategy->has_required_receivers(m_strategy));
}

TEST_F(TaggedFlowControlTest, shouldAlwaysHaveRequiredReceiverByDefault)
{
    char buffer[1024];
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_TRUE(nullptr != m_strategy);

    aeron_tagged_flow_control_strategy_to_string(m_strategy, buffer, sizeof(buffer));
    ASSERT_TRUE(m_strategy->has_required_receivers(m_strategy)) << buffer;
}

TEST_F(TaggedFlowControlTest, shouldOnlyHaveRequiredReceiversWhenGroupSizeMet)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123/3");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_FALSE(m_strategy->has_required_receivers(m_strategy));

    apply_status_message(m_strategy, 1, 1000, -1);
    apply_status_message(m_strategy, 2, 1000, -1);
    apply_status_message(m_strategy, 3, 1000, -1);

    ASSERT_FALSE(m_strategy->has_required_receivers(m_strategy));

    apply_status_message(m_strategy, 4, 1000, 123);

    ASSERT_FALSE(m_strategy->has_required_receivers(m_strategy));

    apply_status_message(m_strategy, 5, 1000, 123);

    ASSERT_FALSE(m_strategy->has_required_receivers(m_strategy));

    apply_status_message(m_strategy, 6, 1000, 123);

    ASSERT_TRUE(m_strategy->has_required_receivers(m_strategy));
}

TEST_F(MinFlowControlTest, shouldOnlyHaveRequiredReceiversWhenGroupSizeMet)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:/3");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    ASSERT_FALSE(m_strategy->has_required_receivers(m_strategy));

    apply_status_message(m_strategy, 1, 1000, -1);
    ASSERT_FALSE(m_strategy->has_required_receivers(m_strategy));

    apply_status_message(m_strategy, 2, 1000, -1);
    ASSERT_FALSE(m_strategy->has_required_receivers(m_strategy));

    apply_status_message(m_strategy, 2, 1010, -1);
    ASSERT_FALSE(m_strategy->has_required_receivers(m_strategy));

    apply_status_message(m_strategy, 3, 1000, -1);
    ASSERT_TRUE(m_strategy->has_required_receivers(m_strategy));
}

TEST_F(MinFlowControlTest, shouldUseSenderLimitWhenRequiredReceiverNotMet)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:/3");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    int sender_limit = 500;
    int term_offset = 1000;

    ASSERT_EQ(sender_limit, apply_status_message(m_strategy, 1, term_offset, -1, 0, false, sender_limit));
    ASSERT_EQ(sender_limit, m_strategy->on_idle(m_strategy->state, 0, sender_limit, 0, false));

    ASSERT_EQ(sender_limit, apply_status_message(m_strategy, 2, term_offset, -1, 0, false, sender_limit));
    ASSERT_EQ(sender_limit, m_strategy->on_idle(m_strategy->state, 0, sender_limit, 0, false));

    ASSERT_EQ(term_offset + WINDOW_LENGTH,
              apply_status_message(m_strategy, 3, term_offset, -1, 0, false, sender_limit));
    ASSERT_EQ(term_offset + WINDOW_LENGTH, m_strategy->on_idle(m_strategy->state, 0, sender_limit, 0, false));
}

TEST_F(TaggedFlowControlTest, shouldUseSenderLimitWhenRequiredReceiversNotMet)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123/3");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    int sender_limit = 500;
    int term_offset = 1000;
    int gtag = 123;

    ASSERT_EQ(sender_limit, apply_status_message(m_strategy, 1, term_offset, gtag, 0, true, sender_limit));
    ASSERT_EQ(sender_limit, m_strategy->on_idle(m_strategy->state, 0, sender_limit, 0, false));

    ASSERT_EQ(sender_limit, apply_status_message(m_strategy, 2, term_offset, gtag, 0, true, sender_limit));
    ASSERT_EQ(sender_limit, m_strategy->on_idle(m_strategy->state, 0, sender_limit, 0, false));

    ASSERT_EQ(
        term_offset + WINDOW_LENGTH,
        apply_status_message(m_strategy, 3, term_offset, gtag, 0, true, sender_limit));
    ASSERT_EQ(term_offset + WINDOW_LENGTH, m_strategy->on_idle(m_strategy->state, 0, sender_limit, 0, false));
}

TEST_F(MinFlowControlTest, shouldNotIncludeReceiverMoreThanWindowLengthBehind)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:/2");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel, 1001, 1001, 1001, 0, 64 * 1024));

    int sender_limit = 500;
    int term_offset_0 = 5000 + WINDOW_LENGTH;
    int term_offset_1 = term_offset_0 - (WINDOW_LENGTH + 1);
    int term_offset_2 = term_offset_0 - WINDOW_LENGTH;

    ASSERT_EQ(sender_limit, apply_status_message(m_strategy, 1, term_offset_0, -1, 0, false, sender_limit));
    ASSERT_EQ(sender_limit, apply_status_message(m_strategy, 2, term_offset_1, -1, 0, false, sender_limit));
    ASSERT_FALSE(m_strategy->has_required_receivers(m_strategy));
    ASSERT_EQ(WINDOW_LENGTH + term_offset_2, apply_status_message(m_strategy, 2, term_offset_2, -1, 0, false, sender_limit));
    ASSERT_TRUE(m_strategy->has_required_receivers(m_strategy));
}

TEST_F(TaggedFlowControlTest, shouldNotIncludeReceiverMoreThanWindowLengthBehind)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123/2");

    ASSERT_EQ(0, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel, 1001, 1001, 1001, 0, 64 * 1024));

    int gtag = 123;
    int sender_limit = 500;
    int term_offset_0 = 5000 + WINDOW_LENGTH;
    int term_offset_1 = term_offset_0 - (WINDOW_LENGTH + 1);
    int term_offset_2 = term_offset_0 - WINDOW_LENGTH;

    ASSERT_EQ(sender_limit, apply_status_message(m_strategy, 1, term_offset_0, gtag, 0, true, sender_limit));
    ASSERT_EQ(sender_limit, apply_status_message(m_strategy, 2, term_offset_1, gtag, 0, true, sender_limit));
    ASSERT_FALSE(m_strategy->has_required_receivers(m_strategy));
    ASSERT_EQ(WINDOW_LENGTH + term_offset_2, apply_status_message(m_strategy, 3, term_offset_2, gtag, 0, true, sender_limit));
    ASSERT_TRUE(m_strategy->has_required_receivers(m_strategy));
}

TEST_P(ParameterisedSuccessfulOptionsParsingTest, shouldBeValid)
{
    const char *fc_options = std::get<0>(GetParam());
    const char *strategy = std::get<1>(GetParam());

    aeron_flow_control_tagged_options_t options;
    ASSERT_EQ(1, aeron_flow_control_parse_tagged_options(strlen(fc_options), fc_options, &options));

    ASSERT_EQ(strlen(strategy), options.strategy_name_length);
    ASSERT_TRUE(0 == strncmp(strategy, options.strategy_name, options.strategy_name_length));
    ASSERT_EQ(std::get<2>(GetParam()), options.timeout_ns.value);
    ASSERT_EQ(std::get<3>(GetParam()), options.group_tag.is_present);
    ASSERT_EQ(std::get<4>(GetParam()), options.group_tag.value);
    ASSERT_EQ(std::get<5>(GetParam()), options.group_min_size.is_present);
    ASSERT_EQ(std::get<6>(GetParam()), options.group_min_size.value);
}

INSTANTIATE_TEST_SUITE_P(
    ParsingTests,
    ParameterisedSuccessfulOptionsParsingTest,
    testing::Values(
        std::make_tuple("max", "max", 0, false, -1, false, 0),
        std::make_tuple("min", "min", 0, false, -1, false, 0),
        std::make_tuple("min,t:10s", "min", 10000000000, false, -1, false, 0),
        std::make_tuple("tagged,g:-1", "tagged", 0, true, -1, false, 0),
        std::make_tuple("tagged,g:100", "tagged", 0, true, 100, false, 0),
        std::make_tuple("tagged,t:10s,g:100", "tagged", 10000000000, true, 100, false, 0),
        std::make_tuple("tagged,t:10s,g:100/0", "tagged", 10000000000, true, 100, true, 0),
        std::make_tuple("tagged,t:10s,g:100/10", "tagged", 10000000000, true, 100, true, 10),
        std::make_tuple("tagged,g:/10", "tagged", 0, false, -1, true, 10)));

TEST_F(FlowControlTest, shouldParseNull)
{
    aeron_flow_control_tagged_options_t options;
    ASSERT_EQ(0, aeron_flow_control_parse_tagged_options(0, nullptr, &options));

    ASSERT_EQ(0U, options.strategy_name_length);
    ASSERT_EQ(nullptr, options.strategy_name);
    ASSERT_EQ(false, options.timeout_ns.is_present);
    ASSERT_EQ(0U, options.timeout_ns.value);
    ASSERT_EQ(false, options.group_tag.is_present);
    ASSERT_EQ(-1, options.group_tag.value);
}

TEST_F(FlowControlTest, shouldFallWithInvalidStrategyName)
{
    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=minute");
    ASSERT_EQ(-1, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=maximilien");
    ASSERT_EQ(-1, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));

    initialise_channel("aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=taggedagado");
    ASSERT_EQ(-1, aeron_default_multicast_flow_control_strategy_supplier(
        &m_strategy, context, &m_counters_manager, m_channel,
        1001, 1001, 1001, 0, 64 * 1024));
}

TEST_P(ParameterisedFailingOptionsParsingTest, shouldBeInvalid)
{
    const char *fc_options = std::get<0>(GetParam());

    aeron_flow_control_tagged_options_t options;
    ASSERT_EQ(-1, aeron_flow_control_parse_tagged_options(strlen(fc_options), fc_options, &options));
    ASSERT_EQ(std::get<1>(GetParam()), aeron_errcode());
}

INSTANTIATE_TEST_SUITE_P(
    ParsingTests,
    ParameterisedFailingOptionsParsingTest,
    testing::Values(
        std::make_tuple("min,t1a0s", EINVAL),
        std::make_tuple("min,g:1f2", EINVAL),
        std::make_tuple("min,t:10s,g:1b2", EINVAL),
        std::make_tuple("min,o:-1", EINVAL),
        std::make_tuple("tagged,g:1/", EINVAL),
        std::make_tuple("tagged,g:", EINVAL),
        std::make_tuple("tagged,g:/", EINVAL)));

TEST(CalculateRetransmissionTest, shouldUseResendLengthIfSmallestValue)
{
    int resend_length = 1024;

    ASSERT_EQ(resend_length,
        aeron_flow_control_calculate_retransmission_length(
            resend_length,
            64 * 1024,
            0,
            16));
}

TEST(CalculateRetransmissionTest, shouldClampToTheEndOfTheBuffer)
{
    int expected_length = 512;
    int term_length = 64 * 1024;
    int term_offset = term_length - expected_length;

    ASSERT_EQ(expected_length,
        aeron_flow_control_calculate_retransmission_length(
            1024,
            term_length,
            term_offset,
            16));
}

TEST(CalculateRetransmissionTest, shouldClampToReceiverWindow)
{
    int multiplier = 16;
    size_t expected_length = aeron_driver_context_get_rcv_initial_window_length(nullptr) * multiplier;

    ASSERT_EQ(expected_length,
        aeron_flow_control_calculate_retransmission_length(
            4 * 1024 * 1024,
            8 * 1024 * 1024,
            0,
            16));
}
