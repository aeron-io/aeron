/*
 * Copyright 2014-2021 Real Logic Limited.
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
#include "media/aeron_udp_channel_transport_loss.h"
#include "protocol/aeron_udp_protocol.h"
#include "util/aeron_netutil.h"

#if !defined(HAVE_STRUCT_MMSGHDR)
struct mmsghdr
{
    struct msghdr msg_hdr;
    unsigned int msg_len;
};
#endif
}

#define TEMP_URL_LEN (128)

class UdpChannelTransporTest : public testing::Test
{
public:
    UdpChannelTransporTest() = default;
};

void test_revc_func(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr,
    struct timespec *media_timestamp)
{

}

TEST_F(UdpChannelTransporTest, shouldErrorWithInvalidSendAddress)
{
    aeron_driver_context_t *driver_context = nullptr;
    aeron_driver_context_init(&driver_context);
    aeron_udp_channel_transport_bindings_t *transport_bindings = aeron_udp_channel_transport_bindings_load_media(
        "default");
    aeron_udp_channel_data_paths_t data_paths = {};
    ASSERT_NE(
        -1,
        aeron_udp_channel_data_paths_init(
            &data_paths,
            nullptr,
            nullptr,
            transport_bindings,
            test_revc_func,
            driver_context,
            AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_SENDER));

    ASSERT_NE(nullptr, transport_bindings) << aeron_errmsg();

    struct sockaddr_in bind_addr = {};
    struct sockaddr_in send_addr = {};

    ASSERT_NE(
        -1,
        aeron_ip_addr_resolver("127.0.0.1", (struct sockaddr_storage*)&bind_addr, AF_INET, IPPROTO_UDP));
    bind_addr.sin_port = 0;

    ASSERT_NE(
        -1,
        aeron_ip_addr_resolver("0.0.0.0", (struct sockaddr_storage*)&send_addr, AF_INET, IPPROTO_UDP));
    send_addr.sin_port = 6666;

    aeron_udp_channel_transport_t transport = {};
    ASSERT_NE(-1, transport_bindings->init_func(
        &transport,
        (struct sockaddr_storage*)&bind_addr,
        (struct sockaddr_storage*)nullptr,
        0,
        16,
        65536,
        65536,
        false,
        driver_context,
        AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_SENDER)) << aeron_errmsg();

    const char *data = "Hello World";

    struct iovec message = {};
    message.iov_base = static_cast<void *>(const_cast<char *>(data));
    message.iov_len = strlen(data);

    struct msghdr header = {};
    header.msg_name = static_cast<void *>(&send_addr);
    header.msg_namelen = sizeof(send_addr);
    header.msg_iov = &message;
    header.msg_iovlen = 1;
    header.msg_control = nullptr;
    header.msg_controllen = 1;
    header.msg_flags = 389475;

    ASSERT_EQ(-1, transport_bindings->sendmsg_func(&data_paths, &transport, &header)) << aeron_errmsg();
}
