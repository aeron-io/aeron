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

#ifndef AERON_AERON_TEST_UDP_BINDINGS_H
#define AERON_AERON_TEST_UDP_BINDINGS_H

#include "aeronmd.h"
#include "media/aeron_udp_channel_transport_bindings.h"

typedef struct aeron_test_udp_bindings_state_stct
{
    int mmsg_count;
    int msg_count;
    int sm_count;
    int nak_count;
    int setup_count;
    int rttm_count;
    int heartbeat_count;
}
aeron_test_udp_bindings_state_t;

int aeron_test_udp_channel_transport_init(
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *bind_addr,
    struct sockaddr_storage *multicast_if_addr,
    struct sockaddr_storage *connect_addr,
    aeron_udp_channel_transport_params_t *params,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity)
{
    aeron_test_udp_bindings_state_t *state;

    if (aeron_alloc((void **)&state, sizeof(aeron_test_udp_bindings_state_t)) < 0)
    {
        return -1;
    }

    transport->bindings_clientd = state;

    return 0;
}

int aeron_test_udp_channel_transport_close(aeron_udp_channel_transport_t *transport)
{
    aeron_free(transport->bindings_clientd);

    return 0;
}

int aeron_test_udp_channel_transport_recvmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd)
{
    return 0;
}

int aeron_test_udp_channel_transport_send(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *address,
    struct iovec *iov,
    size_t iov_length,
    int64_t *bytes_sent)
{
    aeron_test_udp_bindings_state_t *state = (aeron_test_udp_bindings_state_t *)transport->bindings_clientd;

    for (size_t i = 0; i < iov_length; i++)
    {
        state->msg_count++;
        aeron_frame_header_t *header = (aeron_frame_header_t *)iov[i].iov_base;
        *bytes_sent = iov[i].iov_len;

        switch (header->type)
        {
            case AERON_HDR_TYPE_SETUP:
                state->setup_count++;
                break;

            case AERON_HDR_TYPE_SM:
                state->sm_count++;
                break;

            case AERON_HDR_TYPE_NAK:
                state->nak_count++;
                break;

            case AERON_HDR_TYPE_RTTM:
                state->rttm_count++;
                break;

            case AERON_HDR_TYPE_DATA:
                state->heartbeat_count++;

            default:
                break;
        }
    }

    return (int)iov_length;
}

int aeron_test_udp_channel_transport_get_so_rcvbuf(aeron_udp_channel_transport_t *transport, size_t *so_rcvbuf)
{
    return 0;
}

int aeron_test_udp_channel_transport_bind_addr_and_port(
    aeron_udp_channel_transport_t *transport, char *buffer, size_t length)
{
    return 0;
}

int aeron_test_udp_transport_poller_init(
    aeron_udp_transport_poller_t *poller,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity)
{
    return 0;
}

int aeron_test_udp_transport_poller_close(aeron_udp_transport_poller_t *poller)
{
    return 0;
}

int aeron_test_udp_transport_poller_add(
    aeron_udp_transport_poller_t *poller, aeron_udp_channel_transport_t *transport)
{
    return 0;
}

int aeron_test_udp_transport_poller_remove(
    aeron_udp_transport_poller_t *poller, aeron_udp_channel_transport_t *transport)
{
    return 0;
}

int aeron_test_udp_transport_poller_poll(
    aeron_udp_transport_poller_t *poller,
    struct mmsghdr *msgvec,
    size_t vlen,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    aeron_udp_channel_transport_recvmmsg_func_t recvmmsg_func,
    void *clientd)
{
    return 0;
}

void aeron_test_udp_bindings_load(aeron_udp_channel_transport_bindings_t *bindings)
{
    bindings->meta_info.type = "test";
    bindings->meta_info.name = "counting";
    bindings->meta_info.source_symbol = "header";
    bindings->close_func = aeron_test_udp_channel_transport_close;

    bindings->poller_close_func = aeron_test_udp_transport_poller_close;
    bindings->poller_remove_func = aeron_test_udp_transport_poller_remove;
    bindings->poller_add_func = aeron_test_udp_transport_poller_add;
    bindings->poller_init_func = aeron_test_udp_transport_poller_init;
    bindings->poller_poll_func = aeron_test_udp_transport_poller_poll;

    bindings->send_func = aeron_test_udp_channel_transport_send;
    bindings->recvmmsg_func = aeron_test_udp_channel_transport_recvmmsg;
    bindings->init_func = aeron_test_udp_channel_transport_init;
    bindings->bind_addr_and_port_func = aeron_test_udp_channel_transport_bind_addr_and_port;
    bindings->get_so_rcvbuf_func = aeron_test_udp_channel_transport_get_so_rcvbuf;
}

#endif //AERON_AERON_TEST_UDP_BINDINGS_H
