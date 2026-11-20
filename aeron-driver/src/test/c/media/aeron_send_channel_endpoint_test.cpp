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
#include <gmock/gmock.h>

#include "gmock/gmock-matchers.h"

extern "C"
{
#include "util/aeron_error.h"
#include "media/aeron_send_channel_endpoint.h"
}

using testing::HasSubstr;

class SendChannelEndpointTest : public testing::Test
{
    void SetUp() override
    {
        if (0 != aeron_distinct_error_log_init(&m_error_log, m_error_buffer, sizeof(m_error_buffer), aeron_nano_clock))
        {
            throw std::logic_error("aeron_distinct_error_log_init");
        }
    }

    void TearDown() override
    {
        aeron_distinct_error_log_close(&m_error_log);
    }

protected:
    static void initAddress(struct sockaddr_storage *addr, const char *host, const int port)
    {
        auto *addr_in = reinterpret_cast<sockaddr_in *>(addr);
        EXPECT_EQ(1, inet_pton(AF_INET, host, &addr_in->sin_addr));
        addr_in->sin_family = AF_INET;
        addr_in->sin_port = htons(UINT16_C(port));
    }

    aeron_distinct_error_log_t m_error_log = {};
    uint8_t m_error_buffer[AERON_ERROR_MAX_TOTAL_LENGTH] = {};
};

TEST_F(SendChannelEndpointTest, shouldLogSendErrorAsWarningFromSendFunction)
{
    aeron_udp_channel_data_paths_t data_paths = {};
    auto sendFunc = [](
        aeron_udp_channel_data_paths_t *data_paths,
        aeron_udp_channel_transport_t *transport,
        struct sockaddr_storage *address,
        struct iovec *iov,
        size_t iov_length,
        int64_t *bytes_sent)
    {
        aeron_err_set(ENOTSOCK,  "func", "file", 50, "%s", "send has failed");
        return -1;
    };
    data_paths.send_func = sendFunc;

    int64_t errors = 0;
    int64_t shortSends = 0;
    aeron_send_channel_endpoint_t endpoint = {};
    initAddress(&endpoint.current_data_addr, "192.168.10.1", 5555);
    endpoint.transport.error_log = &m_error_log;
    endpoint.transport.errors_counter = &errors;
    endpoint.transport.short_sends_counter = &shortSends;
    endpoint.data_paths = &data_paths;
    iovec iov = {};
    int64_t byteSent = 0;

    EXPECT_EQ(0, aeron_send_channel_send(&endpoint, &iov, 1, &byteSent));
    EXPECT_EQ(0, byteSent);
    EXPECT_EQ(0, aeron_errcode());
    EXPECT_EQ(1, aeron_distinct_error_log_num_observations(&m_error_log));
    EXPECT_EQ(1, errors);
    EXPECT_EQ(0, shortSends);

    char message[AERON_ERROR_MAX_TOTAL_LENGTH];

    auto readerFunc = [](
        int32_t observation_count,
        int64_t first_observation_timestamp,
        int64_t last_observation_timestamp,
        const char *error,
        size_t error_length,
        void *clientd)
    {
        auto p = reinterpret_cast<char *>(clientd);
        memcpy(p, error, error_length);
    };

    EXPECT_EQ(1, aeron_error_log_read(m_error_buffer, sizeof(m_error_buffer), readerFunc, &message, 0));
    EXPECT_THAT(message, HasSubstr("WARN - failed to send datagram to 192.168.10.1:5555"));
    EXPECT_THAT(message, HasSubstr("[func, file:50] send has failed"));
}

TEST_F(SendChannelEndpointTest, shouldIncementShortSendsInSendFunction)
{
    aeron_udp_channel_data_paths_t data_paths = {};
    auto sendFunc = [](
        aeron_udp_channel_data_paths_t *data_paths,
        aeron_udp_channel_transport_t *transport,
        struct sockaddr_storage *address,
        struct iovec *iov,
        size_t iov_length,
        int64_t *bytes_sent)
    {
        (*bytes_sent) = 100;
        return 3;
    };
    data_paths.send_func = sendFunc;

    int64_t errors = 0;
    int64_t shortSends = 0;
    aeron_send_channel_endpoint_t endpoint = {};
    initAddress(&endpoint.current_data_addr, "192.168.10.1", 5555);
    endpoint.transport.error_log = &m_error_log;
    endpoint.transport.errors_counter = &errors;
    endpoint.transport.short_sends_counter = &shortSends;
    endpoint.data_paths = &data_paths;
    iovec iov = {};
    int64_t byteSent = 0;

    EXPECT_EQ(3, aeron_send_channel_send(&endpoint, &iov, 1, &byteSent));
    EXPECT_EQ(100, byteSent);
    EXPECT_EQ(0, aeron_errcode());
    EXPECT_EQ(0, aeron_distinct_error_log_num_observations(&m_error_log));
    EXPECT_EQ(0, errors);
    EXPECT_EQ(1, shortSends);
}

TEST_F(SendChannelEndpointTest, shouldLogSendErrorAsWarningFromSendWithAddress)
{
    aeron_udp_channel_data_paths_t data_paths = {};
    auto sendFunc = [](
        aeron_udp_channel_data_paths_t *data_paths,
        aeron_udp_channel_transport_t *transport,
        struct sockaddr_storage *address,
        struct iovec *iov,
        size_t iov_length,
        int64_t *bytes_sent)
    {
        aeron_err_set(EINVAL,  "other", "than", 1, "%s", "something else");
        return -1;
    };
    data_paths.send_func = sendFunc;

    int64_t errors = 0;
    int64_t shortSends = 0;
    aeron_send_channel_endpoint_t endpoint = {};
    sockaddr_storage addr = {};
    initAddress(&addr, "127.0.0.1", 8080);
    endpoint.transport.error_log = &m_error_log;
    endpoint.transport.errors_counter = &errors;
    endpoint.transport.short_sends_counter = &shortSends;
    endpoint.data_paths = &data_paths;
    iovec iov = {};
    int64_t byteSent = 0;

    EXPECT_EQ(0, aeron_send_channel_send_endpoint_address(&endpoint, &addr, &iov, 1, &byteSent));
    EXPECT_EQ(0, byteSent);
    EXPECT_EQ(0, aeron_errcode());
    EXPECT_EQ(1, aeron_distinct_error_log_num_observations(&m_error_log));
    EXPECT_EQ(1, errors);
    EXPECT_EQ(0, shortSends);

    char message[AERON_ERROR_MAX_TOTAL_LENGTH];

    auto readerFunc = [](
        int32_t observation_count,
        int64_t first_observation_timestamp,
        int64_t last_observation_timestamp,
        const char *error,
        size_t error_length,
        void *clientd)
    {
        auto p = reinterpret_cast<char *>(clientd);
        memcpy(p, error, error_length);
    };

    EXPECT_EQ(1, aeron_error_log_read(m_error_buffer, sizeof(m_error_buffer), readerFunc, &message, 0));
    EXPECT_THAT(message, HasSubstr("WARN - failed to send datagram to 127.0.0.1:8080, cause: (22)"));
    EXPECT_THAT(message, HasSubstr("[other, than:1] something else"));
}


TEST_F(SendChannelEndpointTest, shouldIncementShortSendsInSendWithAddress)
{
    aeron_udp_channel_data_paths_t data_paths = {};
    auto sendFunc = [](
        aeron_udp_channel_data_paths_t *data_paths,
        aeron_udp_channel_transport_t *transport,
        struct sockaddr_storage *address,
        struct iovec *iov,
        size_t iov_length,
        int64_t *bytes_sent)
    {
        (*bytes_sent) = 555;
        return 1;
    };
    data_paths.send_func = sendFunc;

    int64_t errors = 0;
    int64_t shortSends = 0;
    aeron_send_channel_endpoint_t endpoint = {};
    sockaddr_storage addr = {};
    initAddress(&addr, "127.0.0.1", 8080);
    endpoint.transport.error_log = &m_error_log;
    endpoint.transport.errors_counter = &errors;
    endpoint.transport.short_sends_counter = &shortSends;
    endpoint.data_paths = &data_paths;
    iovec iov = {};
    int64_t byteSent = 0;

    EXPECT_EQ(1, aeron_send_channel_send_endpoint_address(&endpoint, &addr, &iov, 8, &byteSent));
    EXPECT_EQ(555, byteSent);
    EXPECT_EQ(0, aeron_errcode());
    EXPECT_EQ(0, aeron_distinct_error_log_num_observations(&m_error_log));
    EXPECT_EQ(0, errors);
    EXPECT_EQ(1, shortSends);
}

TEST_F(SendChannelEndpointTest, shouldDelegateSendToDestinationTracker)
{
    int64_t num_destinations = 0;
    aeron_udp_destination_tracker_t destination_tracker = {};
    destination_tracker.num_destinations_addr = &num_destinations;
    aeron_clock_cache_t cached_clock = {};
    destination_tracker.cached_clock = &cached_clock;

    aeron_udp_channel_data_paths_t data_paths = {};
    auto sendFunc = [](
        aeron_udp_channel_data_paths_t *data_paths,
        aeron_udp_channel_transport_t *transport,
        struct sockaddr_storage *address,
        struct iovec *iov,
        size_t iov_length,
        int64_t *bytes_sent)
    {
        auto *addr_in = reinterpret_cast<sockaddr_in *>(address);
        int port = ntohs(addr_in->sin_port);
        if (8080 == port)
        {
            aeron_err_set(EBADF,  "abc", "xyz", 8080, "%s", "not sending");
            return -1;
        }

        if (5555 == port)
        {
            *bytes_sent = 99;
            return 1; // short send
        }

        *bytes_sent = 1024;
        return (int)iov_length; // full send
    };
    data_paths.send_func = sendFunc;

    EXPECT_EQ(0, aeron_udp_destination_tracker_init(&destination_tracker, &data_paths, &cached_clock, true, 1000000000));

    sockaddr_storage addr1;
    initAddress(&addr1, "127.0.0.1", 8080);
    EXPECT_EQ(0, aeron_udp_destination_tracker_manual_add_destination(
        &destination_tracker, 0, nullptr, &addr1, 1));

    sockaddr_storage addr2;
    initAddress(&addr2, "192.168.10.10", 5555);
    EXPECT_EQ(0, aeron_udp_destination_tracker_manual_add_destination(
        &destination_tracker, 0, nullptr, &addr2, 2));

    sockaddr_storage addr3;
    initAddress(&addr3, "239.18.7.57", 22222);
    EXPECT_EQ(0, aeron_udp_destination_tracker_manual_add_destination(
        &destination_tracker, 0, nullptr, &addr3, 3));

    sockaddr_storage addr4;
    initAddress(&addr4, "192.168.2.2", 5555);
    EXPECT_EQ(0, aeron_udp_destination_tracker_manual_add_destination(
        &destination_tracker, 0, nullptr, &addr4, 4));

    int64_t errors = 0;
    int64_t shortSends = 0;
    aeron_send_channel_endpoint_t endpoint = {};
    endpoint.transport.error_log = &m_error_log;
    endpoint.transport.errors_counter = &errors;
    endpoint.transport.short_sends_counter = &shortSends;
    endpoint.destination_tracker = &destination_tracker;
    iovec iov = {};
    int64_t byteSent = 0;

    EXPECT_EQ(1, aeron_send_channel_send(&endpoint, &iov, 5, &byteSent));
    EXPECT_EQ(1024 + 99 + 99, byteSent);
    EXPECT_EQ(0, aeron_errcode());
    EXPECT_EQ(1, aeron_distinct_error_log_num_observations(&m_error_log));
    EXPECT_EQ(1, errors);
    EXPECT_EQ(2, shortSends);

    char message[AERON_ERROR_MAX_TOTAL_LENGTH];

    auto readerFunc = [](
        int32_t observation_count,
        int64_t first_observation_timestamp,
        int64_t last_observation_timestamp,
        const char *error,
        size_t error_length,
        void *clientd)
    {
        auto p = reinterpret_cast<char *>(clientd);
        memcpy(p, error, error_length);
    };

    EXPECT_EQ(1, aeron_error_log_read(m_error_buffer, sizeof(m_error_buffer), readerFunc, &message, 0));
    EXPECT_THAT(message, HasSubstr("WARN - failed to send datagram to 127.0.0.1:8080, cause: (9)"));
    EXPECT_THAT(message, HasSubstr("[abc, xyz:8080] not sending"));

    aeron_uri_t *tmp = nullptr;
    EXPECT_EQ(0, aeron_udp_destination_tracker_remove_destination(&destination_tracker, &addr1, &tmp));

    errors = shortSends = 0;
    byteSent = 0;
    EXPECT_EQ(1, aeron_send_channel_send(&endpoint, &iov, 5, &byteSent));
    EXPECT_EQ(1024 + 99 + 99, byteSent);
    EXPECT_EQ(0, errors);
    EXPECT_EQ(2, shortSends);

    EXPECT_EQ(0, aeron_udp_destination_tracker_remove_destination(&destination_tracker, &addr2, &tmp));
    EXPECT_EQ(0, aeron_udp_destination_tracker_remove_destination(&destination_tracker, &addr4, &tmp));

    errors = shortSends = 0;
    byteSent = 0;
    EXPECT_EQ(5, aeron_send_channel_send(&endpoint, &iov, 5, &byteSent));
    EXPECT_EQ(1024, byteSent);
    EXPECT_EQ(0, errors);
    EXPECT_EQ(0, shortSends);

    EXPECT_EQ(0, aeron_udp_destination_tracker_close(&destination_tracker));
}
