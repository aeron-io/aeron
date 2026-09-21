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

#ifndef AERON_UDP_TRANSPORT_POLLER_H
#define AERON_UDP_TRANSPORT_POLLER_H

#include "aeron_driver_conductor.h"

#define AERON_UDP_TRANSPORT_POLLER_ITERATION_THRESHOLD (5)
#define AERON_UDP_TRANSPORT_POLLER_PER_SOCKET_POLL_LIMIT (4)

typedef struct aeron_udp_channel_transport_entry_stct
{
    aeron_udp_channel_transport_t *transport;
}
aeron_udp_channel_transport_entry_t;

typedef struct aeron_udp_transport_poller_stct
{
    struct aeron_udp_channel_transports_stct
    {
        aeron_udp_channel_transport_entry_t *array;
        size_t length;
        size_t capacity;
    }
    transports;

    int fd;
    void *bindings_clientd;
}
aeron_udp_transport_poller_t;

int aeron_udp_transport_poller_init(
    aeron_udp_transport_poller_t *poller,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity);
int aeron_udp_transport_poller_close(aeron_udp_transport_poller_t *poller);

int aeron_udp_transport_poller_add(aeron_udp_transport_poller_t *poller, aeron_udp_channel_transport_t *transport);
int aeron_udp_transport_poller_remove(aeron_udp_transport_poller_t *poller, aeron_udp_channel_transport_t *transport);

int aeron_udp_transport_poller_poll(
    aeron_udp_transport_poller_t *poller,
    struct mmsghdr *msgvec,
    size_t vlen,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    aeron_udp_channel_transport_recvmmsg_func_t recvmmsg_func,
    void *clientd);

int aeron_udp_transport_poller_check_send_endpoint_re_resolutions(
    aeron_udp_transport_poller_t *poller,
    int64_t now_ns,
    aeron_driver_conductor_proxy_t *conductor_proxy);

int aeron_udp_transport_poller_check_receive_endpoint_re_resolutions(
    aeron_udp_transport_poller_t *poller,
    int64_t now_ns,
    aeron_driver_conductor_proxy_t *conductor_proxy);

#endif //AERON_UDP_TRANSPORT_POLLER_H
