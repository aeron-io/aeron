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

#ifndef AERON_UDP_CHANNEL_TRANSPORT_H
#define AERON_UDP_CHANNEL_TRANSPORT_H

#include "aeron_socket.h"
#include "aeron_driver_common.h"
#include "aeron_udp_channel_transport_bindings.h"
#include "concurrent/aeron_distinct_error_log.h"
#include "concurrent/aeron_counters_manager.h"
#include "util/aeron_error.h"

#define AERON_UDP_CHANNEL_TRANSPORT_MEDIA_RCV_TIMESTAMP_NONE (0x0)
#define AERON_UDP_CHANNEL_TRANSPORT_MEDIA_RCV_TIMESTAMP_HW (0x1)
#define AERON_UDP_CHANNEL_TRANSPORT_MEDIA_RCV_TIMESTAMP_SW (0x2)
#define AERON_UDP_CHANNEL_TRANSPORT_MEDIA_RCV_TIMESTAMP (0x3)
#define AERON_UDP_CHANNEL_TRANSPORT_CHANNEL_RCV_TIMESTAMP (0x4)
#define AERON_UDP_CHANNEL_TRANSPORT_CHANNEL_SND_TIMESTAMP (0x8)

struct aeron_udp_channel_transport_params_stct
{
    size_t socket_rcvbuf;
    size_t socket_sndbuf;
    size_t mtu_length;
    unsigned int multicast_if_index;
    uint8_t ttl;
    bool is_media_timestamping;
};
typedef struct aeron_udp_channel_transport_params_stct aeron_udp_channel_transport_params_t;

typedef struct aeron_udp_channel_transport_stct
{
    aeron_socket_t fd;
    aeron_socket_t recv_fd;
    aeron_udp_channel_data_paths_t *data_paths;
    struct sockaddr_storage *connected_address;
    void *dispatch_clientd;
    void *bindings_clientd;
    void *destination_clientd;
    void *interceptor_clientds[AERON_UDP_CHANNEL_TRANSPORT_MAX_INTERCEPTORS];
    aeron_distinct_error_log_t *error_log;
    int64_t *errors_counter;
    uint32_t timestamp_flags;
}
aeron_udp_channel_transport_t;

struct mmsghdr;

int aeron_udp_channel_transport_init(
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *bind_addr,
    struct sockaddr_storage *multicast_if_addr,
    struct sockaddr_storage *connect_addr,
    aeron_udp_channel_transport_params_t *params,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity);

int aeron_udp_channel_transport_reconnect(
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *connect_addr);

int aeron_udp_channel_transport_close(aeron_udp_channel_transport_t *transport);

int aeron_udp_channel_transport_recvmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd);

int aeron_udp_channel_transport_send(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *address,
    struct iovec *iov,
    size_t iov_length,
    int64_t *bytes_sent);

int aeron_udp_channel_transport_get_so_rcvbuf(aeron_udp_channel_transport_t *transport, size_t *so_rcvbuf);
int aeron_udp_channel_transport_bind_addr_and_port(
    aeron_udp_channel_transport_t *transport, char *buffer, size_t length);

inline void *aeron_udp_channel_transport_get_interceptor_clientd(
    aeron_udp_channel_transport_t *transport, int interceptor_index)
{
    return transport->interceptor_clientds[interceptor_index];
}

inline void aeron_udp_channel_transport_set_interceptor_clientd(
    aeron_udp_channel_transport_t *transport, int interceptor_index, void *clientd)
{
    transport->interceptor_clientds[interceptor_index] = clientd;
}

inline void aeron_udp_channel_transport_log_error(aeron_udp_channel_transport_t *transport)
{
    aeron_distinct_error_log_record(transport->error_log, aeron_errcode(), aeron_errmsg());
    aeron_counter_increment_opaque(transport->errors_counter);
    aeron_err_clear();
}

#endif //AERON_UDP_CHANNEL_TRANSPORT_H
