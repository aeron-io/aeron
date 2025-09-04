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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include "util/aeron_platform.h"

#if defined(AERON_COMPILER_MSVC)
#include <io.h>
#else
#include <unistd.h>
#endif

#include "aeron_socket.h"

#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <inttypes.h>
#include "util/aeron_error.h"
#include "util/aeron_netutil.h"
#include "aeron_udp_channel_transport.h"
#include "aeron_driver_context.h"

#if defined(__linux__)
#define HAS_MEDIA_RCV_TIMESTAMPS
#include <linux/net_tstamp.h>
#include <sys/ioctl.h>
#include <linux/sockios.h>
#endif

#if !defined(HAVE_STRUCT_MMSGHDR)
struct mmsghdr
{
    struct msghdr msg_hdr;
    unsigned int msg_len;
};
#endif

static int aeron_udp_channel_transport_setup_media_rcv_timestamps(aeron_udp_channel_transport_t *transport)
{
#if defined(HAS_MEDIA_RCV_TIMESTAMPS)
    uint32_t enable_timestamp = 1;
    if (aeron_setsockopt(transport->fd, SOL_SOCKET, SO_TIMESTAMPNS, &enable_timestamp, sizeof(enable_timestamp)) < 0)
    {
        AERON_SET_ERR(errno, "%s", "setsockopt(SO_TIMESTAMPNS)");
        return -1;
    }

    uint32_t timestamp_flags = SOF_TIMESTAMPING_RX_HARDWARE;
    if (aeron_setsockopt(transport->fd, SOL_SOCKET, SO_TIMESTAMPING, &timestamp_flags, sizeof(timestamp_flags)) < 0)
    {
        AERON_SET_ERR(errno, "%s", "setsockopt(SO_TIMESTAMPING)");
        return -1;
    }

    // The kernel does both falling back when required.  Essentially we just need a non-zero value for normal UDP.
    transport->timestamp_flags = AERON_UDP_CHANNEL_TRANSPORT_MEDIA_RCV_TIMESTAMP;
    return 0;
#endif

    AERON_SET_ERR(EINVAL, "%s", "Timestamps are not supported on this platform");
    return -1;
}

int aeron_udp_channel_transport_init(
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *bind_addr,
    struct sockaddr_storage *multicast_if_addr,
    struct sockaddr_storage *connect_addr,
    aeron_udp_channel_transport_params_t *params,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity)
{
    bool is_ipv6, is_multicast;
    struct sockaddr_in *in4 = (struct sockaddr_in *)bind_addr;
    struct sockaddr_in6 *in6 = (struct sockaddr_in6 *)bind_addr;

    transport->fd = -1;
    transport->bindings_clientd = NULL;
    transport->timestamp_flags = AERON_UDP_CHANNEL_TRANSPORT_MEDIA_RCV_TIMESTAMP_NONE;
    transport->error_log = context->error_log;
    transport->errors_counter = aeron_system_counter_addr(context->system_counters, AERON_SYSTEM_COUNTER_ERRORS);
    for (size_t i = 0; i < AERON_UDP_CHANNEL_TRANSPORT_MAX_INTERCEPTORS; i++)
    {
        transport->interceptor_clientds[i] = NULL;
    }

    if (NULL == params)
    {
        AERON_SET_ERR(EINVAL, "%s", "channel transport params is NULL");
        goto error;
    }

    if (0 == params->mtu_length)
    {
        AERON_SET_ERR(EINVAL, "%s", "mtu_length must be greater than 0");
        goto error;
    }

    if ((transport->fd = aeron_socket(bind_addr->ss_family, SOCK_DGRAM, 0)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }
    transport->recv_fd = transport->fd;

    is_ipv6 = AF_INET6 == bind_addr->ss_family;
    is_multicast = aeron_is_addr_multicast(bind_addr);
    socklen_t bind_addr_len = is_ipv6 ? sizeof(struct sockaddr_in6) : sizeof(struct sockaddr_in);

    if (!is_multicast)
    {
        if (aeron_bind(transport->recv_fd, (struct sockaddr *)bind_addr, bind_addr_len) < 0)
        {
            AERON_APPEND_ERR("unicast bind, affinity=%d", affinity);
            goto error;
        }
    }
    else
    {
        if (NULL != connect_addr)
        {
            if ((transport->recv_fd = aeron_socket(bind_addr->ss_family, SOCK_DGRAM, 0)) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                goto error;
            }
        }

        int reuse = 1;

#if defined(SO_REUSEADDR)
        if (aeron_setsockopt(transport->recv_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
        {
            AERON_APPEND_ERR("failed to set SOL_SOCKET/SO_REUSEADDR option to: %d", reuse);
            goto error;
        }
#endif

#if defined(SO_REUSEPORT)
        if (aeron_setsockopt(transport->recv_fd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse)) < 0)
        {
            AERON_SET_ERR(errno, "%s", "setsockopt(SO_REUSEPORT)");
            goto error;
        }
#endif

        if (is_ipv6)
        {
            struct sockaddr_in6 addr;
            memcpy(&addr, bind_addr, sizeof(addr));
            addr.sin6_addr = in6addr_any;

            if (aeron_bind(transport->recv_fd, (struct sockaddr *)&addr, bind_addr_len) < 0)
            {
                AERON_APPEND_ERR("multicast IPv6 bind, affinity=%d", affinity);
                goto error;
            }

            struct ipv6_mreq mreq;

            memcpy(&mreq.ipv6mr_multiaddr, &in6->sin6_addr, sizeof(in6->sin6_addr));
            mreq.ipv6mr_interface = params->multicast_if_index;

            if (aeron_setsockopt(transport->recv_fd, IPPROTO_IPV6, IPV6_JOIN_GROUP, &mreq, sizeof(mreq)) < 0)
            {
                char addr_buf[AERON_NETUTIL_FORMATTED_MAX_LENGTH] = { 0 };
                inet_ntop(AF_INET6, &mreq.ipv6mr_multiaddr, addr_buf, sizeof(addr_buf));
                AERON_APPEND_ERR(
                    "failed to set IPPROTO_IPV6/IPV6_JOIN_GROUP option to: struct ipv6_mreq{.ipv6mr_multiaddr=%s, .ipv6mr_interface=%u}",
                    addr_buf,
                    mreq.ipv6mr_interface);
                goto error;
            }

            if (aeron_setsockopt(
                transport->fd, IPPROTO_IPV6, IPV6_MULTICAST_IF, &params->multicast_if_index, sizeof(params->multicast_if_index)) < 0)
            {
                AERON_APPEND_ERR("failed to set IPPROTO_IPV6/IPV6_MULTICAST_IF option to: %u", params->multicast_if_index);
                goto error;
            }

            if (params->ttl > 0)
            {
                if (aeron_setsockopt(transport->fd, IPPROTO_IPV6, IPV6_MULTICAST_HOPS, &params->ttl, sizeof(params->ttl)) < 0)
                {
                    AERON_APPEND_ERR(
                        "failed to set IPPROTO_IPV6/IPV6_MULTICAST_HOPS option to: %" PRIu8, params->ttl);
                    goto error;
                }
            }
        }
        else
        {
            struct sockaddr_in addr;
            memcpy(&addr, bind_addr, sizeof(addr));
            addr.sin_addr.s_addr = INADDR_ANY;

            if (aeron_bind(transport->recv_fd, (struct sockaddr *)&addr, bind_addr_len) < 0)
            {
                AERON_APPEND_ERR("multicast IPv4 bind, affinity=%d", affinity);
                goto error;
            }

            struct ip_mreq mreq;
            struct sockaddr_in *interface_addr = (struct sockaddr_in *)multicast_if_addr;

            mreq.imr_multiaddr.s_addr = in4->sin_addr.s_addr;
            mreq.imr_interface.s_addr = interface_addr->sin_addr.s_addr;

            if (aeron_setsockopt(transport->recv_fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0)
            {
                char addr_buf[AERON_NETUTIL_FORMATTED_MAX_LENGTH] = { 0 };
                char intr_buf[AERON_NETUTIL_FORMATTED_MAX_LENGTH] = { 0 };
                inet_ntop(AF_INET, &mreq.imr_multiaddr, addr_buf, sizeof(addr_buf));
                inet_ntop(AF_INET, &mreq.imr_interface, intr_buf, sizeof(intr_buf));
                AERON_APPEND_ERR(
                    "failed to set IPPROTO_IP/IP_ADD_MEMBERSHIP option to: struct ip_mreq{.imr_multiaddr=%s, .imr_interface=%s}",
                    addr_buf,
                    intr_buf);

                goto error;
            }

            if (aeron_setsockopt(
                transport->fd, IPPROTO_IP, IP_MULTICAST_IF, &interface_addr->sin_addr, sizeof(struct in_addr)) < 0)
            {
                char intr_buf[AERON_NETUTIL_FORMATTED_MAX_LENGTH] = { 0 };
                inet_ntop(AF_INET, &interface_addr->sin_addr, intr_buf, sizeof(intr_buf));
                AERON_APPEND_ERR("failed to set IPPROTO_IP/IP_MULTICAST_IF option to: %s", intr_buf);
                goto error;
            }

            if (params->ttl > 0)
            {
                if (aeron_setsockopt(transport->fd, IPPROTO_IP, IP_MULTICAST_TTL, &params->ttl, sizeof(params->ttl)) < 0)
                {
                    AERON_APPEND_ERR(
                        "failed to set IPPROTO_IP/IP_MULTICAST_TTL option to: %" PRIu8, params->ttl);
                    goto error;
                }
            }
        }
    }

    if (NULL != connect_addr)
    {
        if (aeron_connect(transport->fd, (struct sockaddr *)connect_addr, AERON_ADDR_LEN(connect_addr)) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
        transport->connected_address = connect_addr;
    }

    if (params->socket_rcvbuf > 0)
    {
        if (aeron_setsockopt(transport->recv_fd, SOL_SOCKET, SO_RCVBUF, &params->socket_rcvbuf, sizeof(params->socket_rcvbuf)) < 0)
        {
            AERON_APPEND_ERR(
                "failed to set SOL_SOCKET/SO_RCVBUF option to: %" PRIu64, (uint64_t)params->socket_rcvbuf);
            goto error;
        }
    }

    if (params->socket_sndbuf > 0)
    {
        if (aeron_setsockopt(transport->fd, SOL_SOCKET, SO_SNDBUF, &params->socket_sndbuf, sizeof(params->socket_sndbuf)) < 0)
        {
            AERON_APPEND_ERR(
                "failed to set SOL_SOCKET/SO_SNDBUF option to: %" PRIu64, (uint64_t)params->socket_sndbuf);
            goto error;
        }
    }

    if (params->is_media_timestamping)
    {
        if (aeron_udp_channel_transport_setup_media_rcv_timestamps(transport) < 0)
        {
            AERON_APPEND_ERR("%s", "WARNING: unable to setup media timestamping");
            aeron_distinct_error_log_record(context->error_log, aeron_errcode(), aeron_errmsg());
            aeron_err_clear();
        }
    }

    if (aeron_set_socket_non_blocking(transport->fd) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to set transport->fd to be non-blocking");
        goto error;
    }

    if (aeron_set_socket_non_blocking(transport->recv_fd) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to set transport->recv_fd to be non-blocking");
        goto error;
    }


    return 0;

error:
    if (-1 != transport->fd)
    {
        aeron_close_socket(transport->fd);
    }

    transport->fd = -1;
    return -1;
}

int aeron_udp_channel_transport_reconnect(
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *connect_addr)
{
    if (NULL != connect_addr && NULL != transport->connected_address)
    {
        if (aeron_connect(transport->fd, (struct sockaddr *)connect_addr, AERON_ADDR_LEN(connect_addr)) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        transport->connected_address = connect_addr;
    }

    return 0;
}

int aeron_udp_channel_transport_close(aeron_udp_channel_transport_t *transport)
{
    if (transport->fd != -1)
    {
        aeron_close_socket(transport->fd);
    }
    if (transport->recv_fd != transport->fd)
    {
        aeron_close_socket(transport->recv_fd);
    }

    return 0;
}

static inline int aeron_udp_channel_transport_recvmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd)
{
    struct timespec *media_rcv_timestamp = NULL;
#if defined(HAS_MEDIA_RCV_TIMESTAMPS)
    AERON_DECL_ALIGNED(
        char buf[AERON_DRIVER_RECEIVER_IO_VECTOR_LENGTH_MAX][CMSG_SPACE(sizeof(struct timespec))],
        sizeof(struct cmsghdr));

    if (transport->timestamp_flags)
    {
        for (int i = 0; i < (int)vlen; i++)
        {
            msgvec[i].msg_hdr.msg_control = (void *)buf[i];
            msgvec[i].msg_hdr.msg_controllen = CMSG_LEN(sizeof(buf[i]));
        }
    }
#endif

    int work_count = 0;

    for (size_t i = 0, length = vlen; i < length; i++)
    {
        ssize_t result = aeron_recvmsg(transport->recv_fd, &msgvec[i].msg_hdr, 0);

        if (result < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        if (0 == result)
        {
            break;
        }

#if defined(HAS_MEDIA_RCV_TIMESTAMPS)
        struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msgvec[i].msg_hdr);
        if (NULL != cmsg &&
            SOL_SOCKET == cmsg->cmsg_level &&
            SCM_TIMESTAMPNS == cmsg->cmsg_type &&
            CMSG_LEN(sizeof(struct timespec)) == cmsg->cmsg_len)
        {
            media_rcv_timestamp = (struct timespec *)CMSG_DATA(cmsg);
        }
#endif

        msgvec[i].msg_len = (unsigned int)result;
        recv_func(
            transport->data_paths,
            transport,
            clientd,
            transport->dispatch_clientd,
            transport->destination_clientd,
            msgvec[i].msg_hdr.msg_iov[0].iov_base,
            msgvec[i].msg_len,
            msgvec[i].msg_hdr.msg_name,
            media_rcv_timestamp);
        *bytes_rcved += msgvec[i].msg_len;
        work_count++;
    }

    return work_count;
}

int aeron_udp_channel_transport_recvmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd)
{
#if defined(HAVE_RECVMMSG)
    if (vlen > 1)
    {
        struct timespec tv = { .tv_nsec = 0, .tv_sec = 0 };
        struct timespec *media_rcv_timestamp = NULL;
#if defined(HAS_MEDIA_RCV_TIMESTAMPS)
        AERON_DECL_ALIGNED(
            char buf[AERON_DRIVER_RECEIVER_IO_VECTOR_LENGTH_MAX][CMSG_SPACE(sizeof(struct timespec))],
            sizeof(struct cmsghdr));

        if (transport->timestamp_flags)
        {
            for (int i = 0; i < (int)vlen; i++)
            {
                msgvec[i].msg_hdr.msg_control = (void *)buf[i];
                msgvec[i].msg_hdr.msg_controllen = CMSG_LEN(sizeof(buf[i]));
            }
        }
#endif

        int result = recvmmsg(transport->recv_fd, msgvec, vlen, 0, &tv);
        if (result < 0)
        {
            int err = errno;

            // ECONNREFUSED can sometimes occur with connected UDP sockets if ICMP traffic is able to indicate that the
            // remote end had closed on a previous send.
            if (aeron_is_acceptable_socket_error())
            {
                return 0;
            }

            AERON_SET_ERR(
                err,
                "Failed to recvmmsg, fd=%d, recv_fd=%d, connected=%s",
                transport->fd,
                transport->recv_fd,
                NULL != transport->connected_address ? "true" : "false");

            return -1;
        }
        else if (0 == result)
        {
            return 0;
        }
        else
        {
            for (size_t i = 0, length = (size_t)result; i < length; i++)
            {
#if defined(HAS_MEDIA_RCV_TIMESTAMPS)
                struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msgvec[i].msg_hdr);
                if (NULL != cmsg &&
                    SOL_SOCKET == cmsg->cmsg_level &&
                    SCM_TIMESTAMPNS == cmsg->cmsg_type &&
                    CMSG_LEN(sizeof(struct timespec)) == cmsg->cmsg_len)
                {
                    media_rcv_timestamp = (struct timespec *)CMSG_DATA(cmsg);
                }
#endif

                recv_func(
                    transport->data_paths,
                    transport,
                    clientd,
                    transport->dispatch_clientd,
                    transport->destination_clientd,
                    msgvec[i].msg_hdr.msg_iov[0].iov_base,
                    msgvec[i].msg_len,
                    msgvec[i].msg_hdr.msg_name,
                    media_rcv_timestamp);
                *bytes_rcved += msgvec[i].msg_len;
            }

            return result;
        }
    }
    else
#endif
    {
        return aeron_udp_channel_transport_recvmsg(transport, msgvec, vlen, bytes_rcved, recv_func, clientd);
    }
}

static int aeron_udp_channel_transport_send_connected(
    aeron_udp_channel_transport_t *transport,
    struct iovec *iov,
    int64_t *bytes_sent)
{
    ssize_t send_result = aeron_send(transport->fd, iov->iov_base, iov->iov_len, 0);
    if (send_result < 0)
    {
        *bytes_sent = 0;
        char addr[AERON_NETUTIL_FORMATTED_MAX_LENGTH];
        aeron_format_source_identity(addr, sizeof(addr), transport->connected_address);
        AERON_APPEND_ERR("address=%s (protocol_family=%i)", addr, transport->connected_address->ss_family);
        return -1;
    }
    else
    {
        *bytes_sent += send_result;
        return 0 == send_result ? 0 : 1;
    }
}

#if !defined(HAVE_SENDMMSG)
static int aeron_udp_channel_transport_send_unconnected(
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *address,
    struct iovec *iov,
    int64_t *bytes_sent)
{
    struct msghdr msg;
    msg.msg_control = NULL;
    msg.msg_controllen = 0;
    msg.msg_name = address;
    msg.msg_namelen = AERON_ADDR_LEN(address);
    msg.msg_iovlen = 1;
    msg.msg_flags = 0;
    msg.msg_iov = iov;

    ssize_t send_result = aeron_sendmsg(transport->fd, &msg, 0);
    if (send_result < 0)
    {
        char addr[AERON_NETUTIL_FORMATTED_MAX_LENGTH];
        aeron_format_source_identity(addr, sizeof(addr), address);
        AERON_APPEND_ERR("address=%s", addr);
        return -1;
    }
    else
    {
        *bytes_sent += send_result;
        return (int)send_result;
    }
}
#endif

#if defined(HAVE_SENDMMSG)

static int aeron_udp_channel_transport_sendv(
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *address,
    struct iovec *iov,
    size_t iov_length,
    int64_t *bytes_sent)
{
    struct mmsghdr msg[AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND];
    size_t msg_i;

    size_t msg_namelen = address == NULL ? 0 : AERON_ADDR_LEN(address);

    for (msg_i = 0; msg_i < iov_length && msg_i < AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND; msg_i++)
    {
        msg[msg_i].msg_hdr.msg_control = NULL;
        msg[msg_i].msg_hdr.msg_controllen = 0;
        msg[msg_i].msg_hdr.msg_name = address;
        msg[msg_i].msg_hdr.msg_namelen = msg_namelen;
        msg[msg_i].msg_hdr.msg_flags = 0;
        msg[msg_i].msg_hdr.msg_iov = &iov[msg_i];
        msg[msg_i].msg_hdr.msg_iovlen = 1;
        msg[msg_i].msg_len = 0;
    }

    int num_sent = sendmmsg(transport->fd, msg, msg_i, 0);
    if (num_sent < 0)
    {
        if (aeron_is_acceptable_socket_error())
        {
            return 0;
        }
        else
        {
            char addr[AERON_NETUTIL_FORMATTED_MAX_LENGTH];
            aeron_format_source_identity(addr, sizeof(addr), address);
            AERON_SET_ERR(errno, "%s: address=%s (protocol_family=%i)", "failed to sendmmsg", addr, address->ss_family);
            return -1;
        }
    }
    else
    {
        for (int i = 0; i < num_sent; i++)
        {
            *bytes_sent += msg[i].msg_len;
        }

        return num_sent;
    }
}

#endif


int aeron_udp_channel_transport_send(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *address,
    struct iovec *iov,
    size_t iov_length,
    int64_t *bytes_sent)
{
#if defined(HAVE_SENDMMSG)
    if (NULL != transport->connected_address)
    {
        if (1 == iov_length)
        {
            return aeron_udp_channel_transport_send_connected(transport, iov, bytes_sent);
        }
        else
        {
            return aeron_udp_channel_transport_sendv(transport, NULL, iov, iov_length, bytes_sent);
        }
    }
    else
    {
        return aeron_udp_channel_transport_sendv(transport, address, iov, iov_length, bytes_sent);
    }
#else
    int result = 0;
    if (NULL != transport->connected_address)
    {
        for (size_t i = 0; i < iov_length; i++)
        {
            int send_result = aeron_udp_channel_transport_send_connected(transport, &iov[i], bytes_sent);
            if (send_result < 0)
            {
                result = -1;
                break;
            }
            else if (0 == send_result)
            {
                break;
            }
            else
            {
                result++;
            }
        }
    }
    else
    {
        for (size_t i = 0; i < iov_length; i++)
        {
            int send_result = aeron_udp_channel_transport_send_unconnected(transport, address, &iov[i], bytes_sent);
            if (send_result < 0)
            {
                result = -1;
                break;
            }
            else if (0 == send_result)
            {
                break;
            }
            else
            {
                result++;
            }
        }
    }
    return result;
#endif
}

int aeron_udp_channel_transport_get_so_rcvbuf(aeron_udp_channel_transport_t *transport, size_t *so_rcvbuf)
{
    socklen_t len = sizeof(size_t);

    if (aeron_getsockopt(transport->recv_fd, SOL_SOCKET, SO_RCVBUF, so_rcvbuf, &len) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to get SOL_SOCKET/SO_RCVBUF option");
        return -1;
    }

    return 0;
}

int aeron_udp_channel_transport_bind_addr_and_port(
    aeron_udp_channel_transport_t *transport, char *buffer, size_t length)
{
    struct sockaddr_storage addr;
    socklen_t addr_len = sizeof(addr);

    if (getsockname(transport->recv_fd, (struct sockaddr *)&addr, &addr_len) < 0)
    {
        AERON_SET_ERR(errno, "Failed to get socket name for fd: %d", transport->fd);
        return -1;
    }

    return aeron_format_source_identity(buffer, length, &addr);
}

extern void *aeron_udp_channel_transport_get_interceptor_clientd(
    aeron_udp_channel_transport_t *transport, int interceptor_index);

extern void aeron_udp_channel_transport_set_interceptor_clientd(
    aeron_udp_channel_transport_t *transport, int interceptor_index, void *clientd);

extern void aeron_udp_channel_transport_log_error(aeron_udp_channel_transport_t *transport);
