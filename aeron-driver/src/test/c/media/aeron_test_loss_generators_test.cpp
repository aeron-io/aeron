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
#include <cstring>
#include <cstdint>
#include <thread>
#include <chrono>

extern "C"
{
#include "media/aeron_loss_generator.h"
#include "media/aeron_random_loss_generator.h"
#include "media/aeron_fixed_loss_generator.h"
#include "media/aeron_multi_gap_loss_generator.h"
#include "media/aeron_debug_channel_endpoint_configuration.h"
#include "media/aeron_send_channel_endpoint.h"
#include "media/aeron_receive_channel_endpoint.h"
#include "util/aeron_env.h"
#include "aeron_stream_id_loss_generator.h"
#include "aeron_port_loss_generator.h"
#include "aeron_frame_data_loss_generator.h"
#include "aeron_stream_id_frame_data_loss_generator.h"
}

static void set_inet_port(struct sockaddr_storage *addr, int port)
{
    auto *in = (struct sockaddr_in *)addr;
    in->sin_family = AF_INET;
    in->sin_port = htons((uint16_t)port);
}

class StreamIdLossGeneratorTest : public testing::Test
{
protected:
    void SetUp() override
    {
        ASSERT_EQ(0, aeron_stream_id_loss_generator_create(&m_generator));
        memset(&m_address, 0, sizeof(m_address));
        memset(m_buffer, 0, sizeof(m_buffer));
    }

    void TearDown() override
    {
        aeron_stream_id_loss_generator_delete(m_generator);
    }

    aeron_loss_generator_t *m_generator = nullptr;
    struct sockaddr_storage m_address = {};
    uint8_t m_buffer[128] = {};
};

TEST_F(StreamIdLossGeneratorTest, forwardWhenDisabled)
{
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 42, 1, 0, 0, 64));
}

TEST_F(StreamIdLossGeneratorTest, dropMatchingStreamId)
{
    aeron_stream_id_loss_generator_enable(m_generator, 42);
    EXPECT_TRUE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 42, 1, 0, 0, 64));
}

TEST_F(StreamIdLossGeneratorTest, forwardNonMatchingStreamId)
{
    aeron_stream_id_loss_generator_enable(m_generator, 42);
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 99, 1, 0, 0, 64));
}

TEST_F(StreamIdLossGeneratorTest, simpleOverloadAlwaysForwards)
{
    aeron_stream_id_loss_generator_enable(m_generator, 42);
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame(m_generator, &m_address, m_buffer, 64));
}

TEST_F(StreamIdLossGeneratorTest, forwardAfterDisable)
{
    aeron_stream_id_loss_generator_enable(m_generator, 42);
    aeron_stream_id_loss_generator_disable(m_generator);
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 42, 1, 0, 0, 64));
}

class PortLossGeneratorTest : public testing::Test
{
protected:
    void SetUp() override
    {
        ASSERT_EQ(0, aeron_port_loss_generator_create(&m_generator));
        memset(&m_address, 0, sizeof(m_address));
        memset(m_buffer, 0, sizeof(m_buffer));
    }

    void TearDown() override
    {
        aeron_port_loss_generator_delete(m_generator);
    }

    aeron_loss_generator_t *m_generator = nullptr;
    struct sockaddr_storage m_address = {};
    uint8_t m_buffer[128] = {};
};

TEST_F(PortLossGeneratorTest, forwardWhenNotDropping)
{
    set_inet_port(&m_address, 9001);
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame(m_generator, &m_address, m_buffer, 64));
}

TEST_F(PortLossGeneratorTest, dropMatchingPort)
{
    set_inet_port(&m_address, 9001);
    aeron_port_loss_generator_start_dropping(m_generator, 9001, 1000LL * 1000LL * 1000LL);
    EXPECT_TRUE(aeron_loss_generator_should_drop_frame(m_generator, &m_address, m_buffer, 64));
}

TEST_F(PortLossGeneratorTest, forwardNonMatchingPort)
{
    set_inet_port(&m_address, 9002);
    aeron_port_loss_generator_start_dropping(m_generator, 9001, 1000LL * 1000LL * 1000LL);
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame(m_generator, &m_address, m_buffer, 64));
}

TEST_F(PortLossGeneratorTest, selfDisableAfterDuration)
{
    set_inet_port(&m_address, 9001);
    aeron_port_loss_generator_start_dropping(m_generator, 9001, 1LL * 1000LL * 1000LL);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame(m_generator, &m_address, m_buffer, 64));
}

TEST_F(PortLossGeneratorTest, detailedOverloadFallsThroughToSimple)
{
    set_inet_port(&m_address, 9001);
    aeron_port_loss_generator_start_dropping(m_generator, 9001, 1000LL * 1000LL * 1000LL);
    EXPECT_TRUE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 0, 0, 0, 0, 64));
}

static bool always_drop(const uint8_t *, size_t, void *)
{
    return true;
}

static bool first_byte_equals(const uint8_t *buffer, size_t, void *clientd)
{
    return buffer[0] == *(uint8_t *)clientd;
}

class FrameDataLossGeneratorTest : public testing::Test
{
protected:
    void SetUp() override
    {
        ASSERT_EQ(0, aeron_frame_data_loss_generator_create(&m_generator));
        memset(&m_address, 0, sizeof(m_address));
        memset(m_buffer, 0, sizeof(m_buffer));
    }

    void TearDown() override
    {
        aeron_frame_data_loss_generator_delete(m_generator);
    }

    aeron_loss_generator_t *m_generator = nullptr;
    struct sockaddr_storage m_address = {};
    uint8_t m_buffer[128] = {};
};

TEST_F(FrameDataLossGeneratorTest, forwardWhenDisabled)
{
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame(m_generator, &m_address, m_buffer, 64));
}

TEST_F(FrameDataLossGeneratorTest, dropWhenPredicateReturnsTrue)
{
    aeron_frame_data_loss_generator_enable(m_generator, always_drop, nullptr);
    EXPECT_TRUE(aeron_loss_generator_should_drop_frame(m_generator, &m_address, m_buffer, 64));
}

TEST_F(FrameDataLossGeneratorTest, forwardWhenPredicateReturnsFalse)
{
    uint8_t expected = 0x42;
    aeron_frame_data_loss_generator_enable(m_generator, first_byte_equals, &expected);
    m_buffer[0] = 0x01;
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame(m_generator, &m_address, m_buffer, 64));
    m_buffer[0] = 0x42;
    EXPECT_TRUE(aeron_loss_generator_should_drop_frame(m_generator, &m_address, m_buffer, 64));
}

TEST_F(FrameDataLossGeneratorTest, forwardAfterDisable)
{
    aeron_frame_data_loss_generator_enable(m_generator, always_drop, nullptr);
    aeron_frame_data_loss_generator_disable(m_generator);
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame(m_generator, &m_address, m_buffer, 64));
}

class StreamIdFrameDataLossGeneratorTest : public testing::Test
{
protected:
    void SetUp() override
    {
        ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&m_generator));
        memset(&m_address, 0, sizeof(m_address));
        memset(m_buffer, 0, sizeof(m_buffer));
    }

    void TearDown() override
    {
        aeron_stream_id_frame_data_loss_generator_delete(m_generator);
    }

    aeron_loss_generator_t *m_generator = nullptr;
    struct sockaddr_storage m_address = {};
    uint8_t m_buffer[128] = {};
};

TEST_F(StreamIdFrameDataLossGeneratorTest, forwardWhenDisabled)
{
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 42, 1, 0, 0, 64));
}

TEST_F(StreamIdFrameDataLossGeneratorTest, dropWhenStreamAndPredicateMatch)
{
    aeron_stream_id_frame_data_loss_generator_enable(m_generator, 42, always_drop, nullptr);
    EXPECT_TRUE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 42, 1, 0, 0, 64));
}

TEST_F(StreamIdFrameDataLossGeneratorTest, forwardWhenStreamMatchesButPredicateFalse)
{
    uint8_t expected = 0x42;
    aeron_stream_id_frame_data_loss_generator_enable(m_generator, 42, first_byte_equals, &expected);
    m_buffer[0] = 0x01;
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 42, 1, 0, 0, 64));
}

TEST_F(StreamIdFrameDataLossGeneratorTest, forwardWhenStreamMismatches)
{
    aeron_stream_id_frame_data_loss_generator_enable(m_generator, 42, always_drop, nullptr);
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 99, 1, 0, 0, 64));
}

TEST_F(StreamIdFrameDataLossGeneratorTest, simpleOverloadAlwaysForwards)
{
    aeron_stream_id_frame_data_loss_generator_enable(m_generator, 42, always_drop, nullptr);
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame(m_generator, &m_address, m_buffer, 64));
}

TEST_F(StreamIdFrameDataLossGeneratorTest, forwardAfterDisable)
{
    aeron_stream_id_frame_data_loss_generator_enable(m_generator, 42, always_drop, nullptr);
    aeron_stream_id_frame_data_loss_generator_disable(m_generator);
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 42, 1, 0, 0, 64));
}

class RandomLossGeneratorTest : public testing::Test
{
protected:
    void TearDown() override
    {
        aeron_random_loss_generator_delete(m_generator);
    }

    aeron_loss_generator_t *m_generator = nullptr;
    struct sockaddr_storage m_address = {};
    uint8_t m_buffer[128] = {};
};

TEST_F(RandomLossGeneratorTest, forwardWhenRateZero)
{
    ASSERT_EQ(0, aeron_random_loss_generator_create(&m_generator, 0.0, 12345));
    for (int i = 0; i < 1000; i++)
    {
        EXPECT_FALSE(aeron_loss_generator_should_drop_frame(m_generator, &m_address, m_buffer, 64));
    }
}

TEST_F(RandomLossGeneratorTest, dropWhenRateOne)
{
    ASSERT_EQ(0, aeron_random_loss_generator_create(&m_generator, 1.0, 12345));
    for (int i = 0; i < 1000; i++)
    {
        EXPECT_TRUE(aeron_loss_generator_should_drop_frame(m_generator, &m_address, m_buffer, 64));
    }
}

TEST_F(RandomLossGeneratorTest, seededDeterministic)
{
    aeron_loss_generator_t *a = nullptr;
    aeron_loss_generator_t *b = nullptr;
    ASSERT_EQ(0, aeron_random_loss_generator_create(&a, 0.5, 12345));
    ASSERT_EQ(0, aeron_random_loss_generator_create(&b, 0.5, 12345));
    for (int i = 0; i < 1000; i++)
    {
        EXPECT_EQ(
            aeron_loss_generator_should_drop_frame(a, &m_address, m_buffer, 64),
            aeron_loss_generator_should_drop_frame(b, &m_address, m_buffer, 64));
    }
    aeron_random_loss_generator_delete(a);
    aeron_random_loss_generator_delete(b);
}

TEST_F(RandomLossGeneratorTest, detailedFallsThroughToSimple)
{
    ASSERT_EQ(0, aeron_random_loss_generator_create(&m_generator, 1.0, 12345));
    EXPECT_TRUE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 1, 1, 0, 0, 64));
}

class FixedLossGeneratorTest : public testing::Test
{
protected:
    void TearDown() override
    {
        aeron_fixed_loss_generator_delete(m_generator);
    }

    aeron_loss_generator_t *m_generator = nullptr;
    struct sockaddr_storage m_address = {};
    uint8_t m_buffer[128] = {};
};

TEST_F(FixedLossGeneratorTest, simpleAlwaysForwards)
{
    ASSERT_EQ(0, aeron_fixed_loss_generator_create(&m_generator, 100, 0, 1024));
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame(m_generator, &m_address, m_buffer, 64));
}

TEST_F(FixedLossGeneratorTest, dropsFirstOccurrenceInRange)
{
    ASSERT_EQ(0, aeron_fixed_loss_generator_create(&m_generator, 100, 256, 1024));
    EXPECT_TRUE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 1, 1, 100, 512, 128));
}

TEST_F(FixedLossGeneratorTest, skipsRetransmission)
{
    ASSERT_EQ(0, aeron_fixed_loss_generator_create(&m_generator, 100, 256, 1024));
    EXPECT_TRUE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 1, 1, 100, 512, 128));
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 1, 1, 100, 256, 128));
}

TEST_F(FixedLossGeneratorTest, skipsWrongTermId)
{
    ASSERT_EQ(0, aeron_fixed_loss_generator_create(&m_generator, 100, 0, 1024));
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 1, 1, 200, 0, 128));
}

TEST_F(FixedLossGeneratorTest, distinctStreamsTrackedIndependently)
{
    ASSERT_EQ(0, aeron_fixed_loss_generator_create(&m_generator, 100, 0, 1024));
    EXPECT_TRUE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 1, 1, 100, 0, 128));
    EXPECT_TRUE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 2, 1, 100, 0, 128));
}

class MultiGapLossGeneratorTest : public testing::Test
{
protected:
    void TearDown() override
    {
        aeron_multi_gap_loss_generator_delete(m_generator);
    }

    aeron_loss_generator_t *m_generator = nullptr;
    struct sockaddr_storage m_address = {};
    uint8_t m_buffer[128] = {};
};

TEST_F(MultiGapLossGeneratorTest, constructorRejectsGapLengthGEQGapRadix)
{
    aeron_loss_generator_t *g = nullptr;
    EXPECT_EQ(-1, aeron_multi_gap_loss_generator_create(&g, 100, 1024, 1024, 4));
}

TEST_F(MultiGapLossGeneratorTest, simpleAlwaysForwards)
{
    ASSERT_EQ(0, aeron_multi_gap_loss_generator_create(&m_generator, 100, 1024, 128, 4));
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame(m_generator, &m_address, m_buffer, 64));
}

TEST_F(MultiGapLossGeneratorTest, dropsAtPeriodicGaps)
{
    ASSERT_EQ(0, aeron_multi_gap_loss_generator_create(&m_generator, 100, 1024, 128, 4));
    /* Gap at offsets 1024, 2048, 3072, 4096. */
    EXPECT_TRUE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 1, 1, 100, 1024, 64));
    EXPECT_TRUE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 2, 1, 100, 2048, 64));
    EXPECT_TRUE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 3, 1, 100, 3072, 64));
    EXPECT_TRUE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 4, 1, 100, 4096, 64));
}

TEST_F(MultiGapLossGeneratorTest, forwardsBetweenGaps)
{
    ASSERT_EQ(0, aeron_multi_gap_loss_generator_create(&m_generator, 100, 1024, 128, 4));
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 1, 1, 100, 512, 64));
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 2, 1, 100, 1536, 64));
}

TEST_F(MultiGapLossGeneratorTest, wrongTermIdForwards)
{
    ASSERT_EQ(0, aeron_multi_gap_loss_generator_create(&m_generator, 100, 1024, 128, 4));
    EXPECT_FALSE(aeron_loss_generator_should_drop_frame_detailed(
        m_generator, &m_address, m_buffer, 1, 1, 200, 1024, 64));
}

class DebugChannelEndpointConfigurationTest : public testing::Test
{
protected:
    void SetUp() override
    {
        aeron_env_unset(AERON_DEBUG_SEND_DATA_LOSS_RATE_ENV_VAR);
        aeron_env_unset(AERON_DEBUG_SEND_DATA_LOSS_SEED_ENV_VAR);
        aeron_env_unset(AERON_DEBUG_SEND_CONTROL_LOSS_RATE_ENV_VAR);
        aeron_env_unset(AERON_DEBUG_SEND_CONTROL_LOSS_SEED_ENV_VAR);
        aeron_env_unset(AERON_DEBUG_RECEIVE_DATA_LOSS_RATE_ENV_VAR);
        aeron_env_unset(AERON_DEBUG_RECEIVE_DATA_LOSS_SEED_ENV_VAR);
        aeron_env_unset(AERON_DEBUG_RECEIVE_CONTROL_LOSS_RATE_ENV_VAR);
        aeron_env_unset(AERON_DEBUG_RECEIVE_CONTROL_LOSS_SEED_ENV_VAR);

        ASSERT_EQ(0, aeron_driver_context_init(&m_context));
    }

    void TearDown() override
    {
        if (nullptr != m_context)
        {
            aeron_driver_context_close(m_context);
        }
    }

    aeron_driver_context_t *m_context = nullptr;
};

TEST_F(DebugChannelEndpointConfigurationTest, noEnvVarsInstallsSuppliersThatStampNothing)
{
    ASSERT_EQ(0, aeron_debug_channel_endpoint_configuration_install(m_context));
    ASSERT_NE(nullptr, m_context->send_channel_loss_supplier_func);
    ASSERT_NE(nullptr, m_context->receive_channel_loss_supplier_func);

    aeron_send_channel_endpoint_t send_endpoint = {};
    m_context->send_channel_loss_supplier_func(
        m_context->send_channel_loss_supplier_clientd, &send_endpoint);
    EXPECT_EQ(nullptr, send_endpoint.data_loss_generator);
    EXPECT_EQ(nullptr, send_endpoint.control_loss_generator);
}

TEST_F(DebugChannelEndpointConfigurationTest, sendDataRateInstallsGenerator)
{
    aeron_env_set(AERON_DEBUG_SEND_DATA_LOSS_RATE_ENV_VAR, "1.0");
    ASSERT_EQ(0, aeron_debug_channel_endpoint_configuration_install(m_context));

    aeron_send_channel_endpoint_t send_endpoint = {};
    m_context->send_channel_loss_supplier_func(
        m_context->send_channel_loss_supplier_clientd, &send_endpoint);

    ASSERT_NE(nullptr, send_endpoint.data_loss_generator);
    EXPECT_EQ(nullptr, send_endpoint.control_loss_generator);
    ASSERT_NE(nullptr, send_endpoint.data_loss_generator->close);

    struct sockaddr_storage addr = {};
    uint8_t buffer[64] = {};
    EXPECT_TRUE(aeron_loss_generator_should_drop_frame(
        send_endpoint.data_loss_generator, &addr, buffer, 64));

    send_endpoint.data_loss_generator->close(
        (aeron_loss_generator_t *)send_endpoint.data_loss_generator);
}

TEST_F(DebugChannelEndpointConfigurationTest, receiveControlRateInstallsGenerator)
{
    aeron_env_set(AERON_DEBUG_RECEIVE_CONTROL_LOSS_RATE_ENV_VAR, "1.0");
    ASSERT_EQ(0, aeron_debug_channel_endpoint_configuration_install(m_context));

    aeron_receive_channel_endpoint_t receive_endpoint = {};
    m_context->receive_channel_loss_supplier_func(
        m_context->receive_channel_loss_supplier_clientd, &receive_endpoint);

    EXPECT_EQ(nullptr, receive_endpoint.data_loss_generator);
    ASSERT_NE(nullptr, receive_endpoint.control_loss_generator);

    receive_endpoint.control_loss_generator->close(
        (aeron_loss_generator_t *)receive_endpoint.control_loss_generator);
}

TEST_F(DebugChannelEndpointConfigurationTest, malformedRateIgnored)
{
    aeron_env_set(AERON_DEBUG_SEND_DATA_LOSS_RATE_ENV_VAR, "notanumber");
    ASSERT_EQ(0, aeron_debug_channel_endpoint_configuration_install(m_context));

    aeron_send_channel_endpoint_t send_endpoint = {};
    m_context->send_channel_loss_supplier_func(
        m_context->send_channel_loss_supplier_clientd, &send_endpoint);
    EXPECT_EQ(nullptr, send_endpoint.data_loss_generator);
}

TEST_F(DebugChannelEndpointConfigurationTest, rateClampedToOne)
{
    aeron_env_set(AERON_DEBUG_SEND_DATA_LOSS_RATE_ENV_VAR, "5.0");
    ASSERT_EQ(0, aeron_debug_channel_endpoint_configuration_install(m_context));

    aeron_send_channel_endpoint_t send_endpoint = {};
    m_context->send_channel_loss_supplier_func(
        m_context->send_channel_loss_supplier_clientd, &send_endpoint);

    ASSERT_NE(nullptr, send_endpoint.data_loss_generator);
    struct sockaddr_storage addr = {};
    uint8_t buffer[64] = {};
    for (int i = 0; i < 100; i++)
    {
        EXPECT_TRUE(aeron_loss_generator_should_drop_frame(
            send_endpoint.data_loss_generator, &addr, buffer, 64));
    }
    send_endpoint.data_loss_generator->close(
        (aeron_loss_generator_t *)send_endpoint.data_loss_generator);
}
