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
#include "media/aeron_receive_channel_endpoint.h"
}

using testing::HasSubstr;

class ReceiveChannelEndpointTest : public testing::Test
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

TEST_F(ReceiveChannelEndpointTest, shouldLogSendErrorAsWarning)
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
    aeron_receive_destination_t destination = {};
    destination.data_paths = &data_paths;

    int64_t errors = 0;
    int64_t shortSends = 0;
    aeron_receive_channel_endpoint_t endpoint = {};
    endpoint.error_log = &m_error_log;
    endpoint.errors_counter = &errors;
    endpoint.short_sends_counter = &shortSends;
    sockaddr_storage address;
    initAddress(&address, "192.168.10.1", 5555);
    iovec iov = {};
    int64_t byteSent = 0;

    EXPECT_EQ(0, aeron_receive_channel_endpoint_send(&endpoint, &destination, &address, &iov));
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

TEST_F(ReceiveChannelEndpointTest, shouldIncementShortSends)
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
        *bytes_sent = 100;
        return 3;
    };
    data_paths.send_func = sendFunc;
    aeron_receive_destination_t destination = {};
    destination.data_paths = &data_paths;

    int64_t errors = 0;
    int64_t shortSends = 0;
    aeron_receive_channel_endpoint_t endpoint = {};
    endpoint.error_log = &m_error_log;
    endpoint.errors_counter = &errors;
    endpoint.short_sends_counter = &shortSends;
    sockaddr_storage address;
    initAddress(&address, "127.0.0.1", 9999);
    iovec iov = {};
    iov.iov_len = 1024;

    EXPECT_EQ(0, aeron_receive_channel_endpoint_send(&endpoint, &destination, &address, &iov));
    EXPECT_EQ(1, shortSends);
    EXPECT_EQ(0, errors);
    EXPECT_EQ(0, aeron_errcode());
    EXPECT_EQ(0, aeron_distinct_error_log_num_observations(&m_error_log));
}

TEST_F(ReceiveChannelEndpointTest, shouldReturnBytesSent)
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
        *bytes_sent += static_cast<int64_t>(iov->iov_len);
        return static_cast<int>(iov_length);
    };
    data_paths.send_func = sendFunc;
    aeron_receive_destination_t destination = {};
    destination.data_paths = &data_paths;

    int64_t errors = 0;
    int64_t shortSends = 0;
    aeron_receive_channel_endpoint_t endpoint = {};
    endpoint.error_log = &m_error_log;
    endpoint.errors_counter = &errors;
    endpoint.short_sends_counter = &shortSends;
    sockaddr_storage address;
    initAddress(&address, "127.0.0.1", 9999);
    iovec iov = {};
    iov.iov_len = 1024;

    EXPECT_EQ(1024, aeron_receive_channel_endpoint_send(&endpoint, &destination, &address, &iov));
    EXPECT_EQ(0, shortSends);
    EXPECT_EQ(0, errors);
    EXPECT_EQ(0, aeron_errcode());
    EXPECT_EQ(0, aeron_distinct_error_log_num_observations(&m_error_log));
}
