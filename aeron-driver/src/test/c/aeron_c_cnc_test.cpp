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

#include <gtest/gtest.h>

#include "aeron_test_base.h"

extern "C"
{
#include "aeron_socket.h"
#include "concurrent/aeron_atomic.h"
#include "aeron_system_counters.h"
}

#define URI_RESERVED "aeron:udp?endpoint=127.0.0.1:24325"
#define PUB_URI_2 "aeron:udp?endpoint=127.0.0.1:24326"
#define STREAM_ID (117)

using namespace aeron;

class CounterIdFilter
{
public:
    explicit CounterIdFilter(std::int32_t filterId) :
        m_filterId(filterId)
    {
    }

    void apply(int32_t id, int64_t value)
    {
        if (id == m_filterId)
        {
            m_foundValue = value;
            m_matchCount++;
        }
        m_totalCounters++;
    }

    bool matches() const
    {
        return 0 < m_matchCount;
    }

    std::int64_t value() const
    {
        return m_foundValue;
    }

private:
    std::int32_t m_filterId = 0;
    std::int32_t m_totalCounters = 0;
    std::int64_t m_foundValue = 0;
    std::int32_t m_matchCount = 0;
};

class CncTest : public CSystemTestBase, public testing::Test
{
protected:
    CncTest() : CSystemTestBase(
        std::vector<std::pair<std::string, std::string>>
        {
            { "AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS",        "loss" },
            { "AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_LOSS_ARGS", "rate=0.2|recv-msg-mask=0xF" }
        })
    {
    }

    void TearDown() override
    {
        if (nullptr != m_cnc)
        {
            aeron_cnc_close(m_cnc);
        }
        if (-1 != m_socketFd)
        {
            aeron_close_socket(m_socketFd);
        }
    }

protected:
    aeron_cnc_t *m_cnc = nullptr;
    aeron_socket_t m_socketFd = -1;

    bool bindToLocalUdpSocket(uint16_t port)
    {
        sockaddr_in bind_address = {};
        bind_address.sin_family = AF_INET;
        bind_address.sin_port = htons(port);
        inet_pton(AF_INET, "127.0.0.1", &bind_address.sin_addr);

        m_socketFd = aeron_socket(PF_INET, SOCK_DGRAM, 0);
        if (m_socketFd < 0)
        {
            return false;
        }

        if (bind(m_socketFd, (const sockaddr *)&bind_address, sizeof(bind_address)) < 0)
        {
            return false;
        }

        return true;
    }

    bool openAeronCnc()
    {
        return 0 == aeron_cnc_init(&m_cnc, m_driver.directory(), 1000);
    }

    static void countingErrorLogCallback(
        int32_t observation_count,
        int64_t first_observation_timestamp,
        int64_t last_observation_timestamp,
        const char *error,
        size_t error_length,
        void *clientd)
    {
        int *counter = reinterpret_cast<int *>(clientd);
        (*counter)++;
    }

    static void counterFilterCallback(
        int64_t value,
        int32_t id,
        int32_t type_id,
        const uint8_t *key,
        size_t key_length,
        const char *label,
        size_t label_length,
        void *clientd)
    {
        auto *filter = reinterpret_cast<CounterIdFilter *>(clientd);
        filter->apply(id, value);
    }

    static void countingLossReader(
        void *clientd,
        int64_t observation_count,
        int64_t total_bytes_lost,
        int64_t first_observation_timestamp,
        int64_t last_observation_timestamp,
        int32_t session_id,
        int32_t stream_id,
        const char *channel,
        int32_t channel_length,
        const char *source,
        int32_t source_length)
    {
        int *counter = reinterpret_cast<int *>(clientd);
        (*counter)++;
    }
};

TEST_F(CncTest, shouldGetCncConstants)
{
    ASSERT_TRUE(openAeronCnc()) << aeron_errmsg();
    aeron_cnc_constants_t constants;
    aeron_cnc_constants(m_cnc, &constants);

    ASSERT_LT(0, constants.pid);
    ASSERT_LT(0, constants.start_timestamp);
    ASSERT_LT(0, constants.cnc_version);
}

TEST_F(CncTest, shouldGetCountersAndDistinctErrorLogs)
{
    ASSERT_TRUE(connect());
    ASSERT_TRUE(openAeronCnc()) << aeron_errmsg();
    int errorCallbackCounter = 0;

    aeron_counters_reader_t *counters = aeron_cnc_counters_reader(m_cnc);
    CounterIdFilter filter{ AERON_SYSTEM_COUNTER_ERRORS };

    aeron_counters_reader_foreach_counter(counters, counterFilterCallback, &filter);
    ASSERT_TRUE(filter.matches());
    ASSERT_EQ(0, filter.value());

    ASSERT_EQ(0U, aeron_cnc_error_log_read(m_cnc, countingErrorLogCallback, &errorCallbackCounter, 0));

    aeron_socket_t fd = bindToLocalUdpSocket(24325);
    ASSERT_LT(0, fd);

    aeron_async_add_subscription_t *async;
    ASSERT_EQ(aeron_async_add_subscription(
        &async, m_aeron, URI_RESERVED, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0) << aeron_errmsg();
    ASSERT_EQ(nullptr, awaitSubscriptionOrError(async));

    filter = CounterIdFilter{ AERON_SYSTEM_COUNTER_ERRORS };

    int64_t deadline_ms = aeron_epoch_clock() + 1000;
    while (filter.value() < 1)
    {
        aeron_counters_reader_foreach_counter(counters, counterFilterCallback, &filter);
        ASSERT_LT(aeron_epoch_clock(), deadline_ms) << "Timed out waiting for error counter";
    }

    ASSERT_EQ(1U, aeron_cnc_error_log_read(m_cnc, countingErrorLogCallback, &errorCallbackCounter, 0));
    ASSERT_EQ(1, errorCallbackCounter);
}

TEST_F(CncTest, shouldGetLossReport)
{
    ASSERT_TRUE(connect());
    ASSERT_TRUE(openAeronCnc()) << aeron_errmsg();
    int lossCallbackCounter = 0;

    aeron_async_add_subscription_t *async_sub;
    aeron_subscription_t *subscription;
    ASSERT_EQ(aeron_async_add_subscription(
        &async_sub, m_aeron, PUB_URI_2, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    ASSERT_TRUE((subscription = awaitSubscriptionOrError(async_sub))) << aeron_errmsg();

    aeron_async_add_publication_t *async_pub;
    aeron_publication_t *publication;
    ASSERT_EQ(aeron_async_add_publication(&async_pub, m_aeron, PUB_URI_2, STREAM_ID), 0);
    ASSERT_TRUE((publication = awaitPublicationOrError(async_pub))) << aeron_errmsg();

    const char *message = "hello world";

    poll_handler_t handler =
        [&](const uint8_t *buffer, size_t length, aeron_header_t *header)
        {
        };

    aeron_counters_reader_t *counters = aeron_cnc_counters_reader(m_cnc);
    volatile int64_t *retransmitsSentCounter = aeron_counters_reader_addr(counters, AERON_SYSTEM_COUNTER_RETRANSMITS_SENT);

    int64_t retransmits = 0;
    for (int i = 0; i < 100 && 0 == retransmits; i++)
    {
        int64_t offer;
        do
        {
            offer = aeron_publication_offer(
                publication,
                reinterpret_cast<const uint8_t *>(message),
                strlen(message),
                nullptr,
                nullptr);

            ASSERT_NE(AERON_PUBLICATION_ERROR, offer) << aeron_errmsg();
        }
        while (offer < 0);

        while (poll(subscription, handler, 1) < 1)
        {
        }

        AERON_GET_ACQUIRE(retransmits, *retransmitsSentCounter);
    }

    ASSERT_TRUE(0 < aeron_cnc_loss_reporter_read(m_cnc, countingLossReader, &lossCallbackCounter)) << aeron_errmsg();
    ASSERT_NE(0, lossCallbackCounter);
}
