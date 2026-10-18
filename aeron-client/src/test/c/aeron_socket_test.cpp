/*
 * Copyright 2014-2026 Real Logic Limited.
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
#include "aeron_client_test_utils.h"

extern "C"
{
#include "aeron_socket.h"
}

using namespace aeron::test;

class SocketTest : public testing::Test
{

protected:
    aeron_socket_t socket_fd = -1;

    void SetUp() override
    {
        socket_fd = aeron_socket(AF_INET, SOCK_DGRAM, 0);
        ASSERT_NE(-1, socket_fd);
    }

    void TearDown() override
    {
        if (-1 != socket_fd)
        {
            aeron_close_socket(socket_fd);
        }
    }
};

TEST_F(SocketTest, setMulticastInterface)
{
    const char *loopback_name_in = "127.0.0.1";

    struct in_addr loopback_in_addr_in = {};
    int inet_pton_rc = inet_pton(AF_INET, loopback_name_in, &loopback_in_addr_in);
    ASSERT_EQ(1, inet_pton_rc);

    int aeron_setsockopt_rc = aeron_setsockopt(socket_fd, IPPROTO_IP, IP_MULTICAST_IF, (const void *)&loopback_in_addr_in, sizeof(loopback_in_addr_in));
    ASSERT_EQ(0, aeron_setsockopt_rc);

    struct in_addr loopback_in_addr_out = {};
    socklen_t loopback_in_addr_out_len = sizeof(loopback_in_addr_out);
    int get_socket_opt_rc = aeron_getsockopt(socket_fd, IPPROTO_IP, IP_MULTICAST_IF, (void *)&loopback_in_addr_out, &loopback_in_addr_out_len);
    ASSERT_EQ(0, get_socket_opt_rc);

    char loopback_name_out[INET_ADDRSTRLEN];
    const char *inet_ntop_rc = inet_ntop(AF_INET, &loopback_in_addr_out, loopback_name_out, sizeof(loopback_name_out));
    ASSERT_NE(nullptr, inet_ntop_rc);

    ASSERT_STREQ(loopback_name_in, loopback_name_out);
}