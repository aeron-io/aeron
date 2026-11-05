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

#if !defined(AERON_COMPILER_MSVC)
#include <unistd.h>
#endif

#if defined(HAVE_EPOLL)
#include <sys/epoll.h>
#elif defined(HAVE_POLL)
#include <poll.h>
#elif defined(HAVE_WSAPOLL)
#include "aeron_windows.h"
#endif

#include "util/aeron_arrayutil.h"
#include "aeron_alloc.h"
#include "media/aeron_udp_transport_poller.h"

int aeron_udp_transport_poller_init(
    aeron_udp_transport_poller_t *poller,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity)
{
    poller->transports.array = NULL;
    poller->transports.length = 0;
    poller->transports.capacity = 0;
    poller->is_epoll_registered = 0;

    poller->iteration_threshold = context->udp_channel_transport_poller_iteration_threshold;
    poller->socket_poll_limit = context->udp_channel_transport_poller_socket_poll_limit;

#if defined(HAVE_EPOLL)
    if ((poller->fd = epoll_create1(0)) < 0)
    {
        AERON_SET_ERR(errno, "%s", "epoll_create1");
        return -1;
    }
#endif

    poller->bindings_clientd = NULL;
    return 0;
}

int aeron_udp_transport_poller_close(aeron_udp_transport_poller_t *poller)
{
    aeron_free(poller->transports.array);
#if defined(HAVE_EPOLL)
    close(poller->fd);
#endif
    aeron_free(poller->bindings_clientd);
    return 0;
}

int aeron_udp_transport_poller_add(aeron_udp_transport_poller_t *poller, aeron_udp_channel_transport_t *transport)
{
    int ensure_capacity_result = 0;
    size_t old_capacity = poller->transports.capacity, index = poller->transports.length;

    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, poller->transports, aeron_udp_channel_transport_entry_t)
    if (ensure_capacity_result < 0)
    {
        return -1;
    }

    poller->transports.array[index].transport = transport;

#if defined(HAVE_EPOLL)
    size_t new_capacity = poller->transports.capacity;

    if (new_capacity > old_capacity)
    {
        if (aeron_array_ensure_capacity(
            (uint8_t **)&poller->bindings_clientd, sizeof(struct epoll_event), old_capacity, new_capacity) < 0)
        {
            return -1;
        }
    }

    /*
     * Registration waits until the transport count actually crosses
     * `poller->iteration_threshold`, because below it
     * `aeron_udp_transport_poller_poll` reads every transport directly and
     * never calls `epoll_wait` -- `poller->fd` would sit there unused.
     *
     * "Unused" is not the same as "free": a socket that is in an epoll set has
     * a waiter on its sleep queue, so *every* datagram delivered to it pays the
     * wake-up path (`sock_def_readable` -> `__wake_up_common` ->
     * `ep_poll_callback`, plus two IRQ-disabling spinlocks). On loopback that
     * delivery runs in the sending thread's softirq, so a driver that sends a
     * datagram pays it for its own send. Measured at 501K messages/s: 6.09% of
     * the sender thread's kernel instructions, and zero once the sockets are
     * out of the set.
     *
     * `index + 1` is the count this transport brings the poller to, since
     * `transports.length` is not incremented until after this block.
     */
    struct epoll_event event;

    event.events = EPOLLIN;

    if (index + 1 > poller->iteration_threshold)
    {
        /* Crossing the threshold registers everything, not just this one. */
        const size_t first = poller->is_epoll_registered ? index : 0;

        for (size_t i = first; i <= index; i++)
        {
            aeron_udp_channel_transport_t *entry = poller->transports.array[i].transport;

            event.data.fd = entry->recv_fd;
            event.data.ptr = entry;
            if (epoll_ctl(poller->fd, EPOLL_CTL_ADD, entry->recv_fd, &event) < 0)
            {
                AERON_SET_ERR(errno, "Failed to epoll_ctl(EPOLL_CTL_ADD), fd: %d", entry->recv_fd);
                return -1;
            }
        }

        poller->is_epoll_registered = 1;
    }

#elif defined(HAVE_POLL) || defined(HAVE_WSAPOLL)
    size_t new_capacity = poller->transports.capacity;

    if (new_capacity > old_capacity)
    {
        if (aeron_array_ensure_capacity(
            (uint8_t **)&poller->bindings_clientd, sizeof(struct pollfd), old_capacity, new_capacity) < 0)
        {
            return -1;
        }
    }

    struct pollfd *pollfds = (struct pollfd *)poller->bindings_clientd;
    pollfds[index].fd = transport->recv_fd;
    pollfds[index].events = POLLIN;
    pollfds[index].revents = 0;
#endif

    poller->transports.length++;

    return 0;
}

int aeron_udp_transport_poller_remove(aeron_udp_transport_poller_t *poller, aeron_udp_channel_transport_t *transport)
{
    int index = -1, last_index = (int)poller->transports.length - 1;

    for (int i = last_index; i >= 0; i--)
    {
        if (poller->transports.array[i].transport == transport)
        {
            index = i;
            break;
        }
    }

    if (index >= 0)
    {
        aeron_array_fast_unordered_remove(
            (uint8_t *)poller->transports.array,
            sizeof(aeron_udp_channel_transport_entry_t),
            (size_t)index,
            (size_t)last_index);
        poller->transports.length--;

#if defined(HAVE_EPOLL)
        aeron_array_fast_unordered_remove(
            (uint8_t *)poller->bindings_clientd,
            sizeof(struct epoll_event),
            (size_t)index,
            (size_t)last_index);

        /*
         * The set is empty at or below the threshold, so there is usually
         * nothing to delete -- and deleting a descriptor that was never added
         * fails with ENOENT, which would be a spurious error. Dropping back
         * below the threshold empties the set again, so that the "registered"
         * flag always means "all of them are in, and the poller is waiting on
         * epoll".
         */
        if (poller->is_epoll_registered)
        {
            struct epoll_event event;

            event.events = EPOLLIN;
            event.data.fd = transport->recv_fd;
            event.data.ptr = transport;
            if (epoll_ctl(poller->fd, EPOLL_CTL_DEL, transport->recv_fd, &event) < 0)
            {
                AERON_SET_ERR(errno, "%s", "epoll_ctl(EPOLL_CTL_DEL)");
                return -1;
            }

            if (poller->transports.length <= poller->iteration_threshold)
            {
                for (size_t i = 0, length = poller->transports.length; i < length; i++)
                {
                    aeron_udp_channel_transport_t *entry = poller->transports.array[i].transport;

                    event.data.fd = entry->recv_fd;
                    event.data.ptr = entry;
                    if (epoll_ctl(poller->fd, EPOLL_CTL_DEL, entry->recv_fd, &event) < 0)
                    {
                        AERON_SET_ERR(errno, "%s", "epoll_ctl(EPOLL_CTL_DEL)");
                        return -1;
                    }
                }

                poller->is_epoll_registered = 0;
            }
        }

#elif defined(HAVE_POLL) || defined(HAVE_WSAPOLL)
        aeron_array_fast_unordered_remove(
            (uint8_t *)poller->bindings_clientd,
            sizeof(struct pollfd),
            (size_t)index,
            (size_t)last_index);
#endif
    }

    return 0;
}

int aeron_udp_transport_poller_poll(
    aeron_udp_transport_poller_t *poller,
    struct mmsghdr *msgvec,
    size_t vlen,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    aeron_udp_channel_transport_recvmmsg_func_t recvmmsg_func,
    void *clientd)
{
    int work_count = 0;

    if (poller->transports.length <= poller->iteration_threshold)
    {
        for (size_t i = 0, length = poller->transports.length; i < length; i++)
        {
            aeron_udp_channel_transport_t *transport = poller->transports.array[i].transport;
            for (int32_t j = 0; j < poller->socket_poll_limit; j++)
            {
                int recv_result = recvmmsg_func(transport, msgvec, vlen, bytes_rcved, recv_func, clientd);
                if (recv_result < 0)
                {
                    AERON_APPEND_ERR("%s", "aeron_udp_transport_poller_poll");
                    aeron_udp_channel_transport_log_error(transport);
                    break;
                }

                work_count += recv_result;
                if (recv_result != (int)vlen)
                {
                    break;
                }
            }
        }
    }
    else
    {
#if defined(HAVE_EPOLL)
        struct epoll_event *epoll_events = (struct epoll_event *)poller->bindings_clientd;
        int result = epoll_wait(poller->fd, epoll_events, (int) poller->transports.length, 0);

        if (result < 0)
        {
            int err = errno;

            if (EINTR == err || EAGAIN == err)
            {
                return 0;
            }

            AERON_SET_ERR(err, "%s", "epoll_wait");
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
                if (epoll_events[i].events & EPOLLIN)
                {
                    aeron_udp_channel_transport_t *transport = epoll_events[i].data.ptr;
                    for (int32_t j = 0; j < poller->socket_poll_limit; j++)
                    {
                        int recv_result = recvmmsg_func(transport, msgvec, vlen, bytes_rcved, recv_func, clientd);
                        if (recv_result < 0)
                        {
                            AERON_APPEND_ERR("%s", "aeron_udp_transport_poller_poll");
                            aeron_udp_channel_transport_log_error(transport);
                            break;
                        }

                        work_count += recv_result;
                        if (recv_result != (int)vlen)
                        {
                            break;
                        }
                    }
                }

                epoll_events[i].events = 0;
            }
        }

#elif defined(HAVE_POLL) || defined(HAVE_WSAPOLL)
        struct pollfd *pollfds = (struct pollfd *)poller->bindings_clientd;
        const int result = aeron_poll(pollfds, (unsigned long)poller->transports.length, 0);

        if (result < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
        else if (0 == result)
        {
            return 0;
        }
        else
        {
            for (size_t i = 0, length = poller->transports.length; i < length; i++)
            {
                if (pollfds[i].revents & POLLIN)
                {
                    aeron_udp_channel_transport_t *transport = poller->transports.array[i].transport;
                    for (int32_t j = 0; j < poller->socket_poll_limit; j++)
                    {
                        int recv_result = recvmmsg_func(transport, msgvec, vlen, bytes_rcved, recv_func, clientd);
                        if (recv_result < 0)
                        {
                            AERON_APPEND_ERR("%s", "aeron_udp_transport_poller_poll");
                            aeron_udp_channel_transport_log_error(transport);
                            break;
                        }

                        work_count += recv_result;
                        if (recv_result != (int)vlen)
                        {
                            break;
                        }
                    }
                }

                pollfds[i].revents = 0;
            }
        }
#endif
    }

    return work_count;
}

int aeron_udp_transport_poller_check_send_endpoint_re_resolutions(
    aeron_udp_transport_poller_t *poller,
    int64_t now_ns,
    struct aeron_driver_conductor_proxy_stct *conductor_proxy)
{
    for (size_t i = 0; i < poller->transports.length; i++)
    {
        aeron_udp_channel_transport_t *transport = poller->transports.array[i].transport;
        aeron_send_channel_endpoint_t *endpoint = transport->dispatch_clientd;

        aeron_send_channel_endpoint_check_for_re_resolution(endpoint, now_ns, conductor_proxy);
    }

    return 0;
}

int aeron_udp_transport_poller_check_receive_endpoint_re_resolutions(
    aeron_udp_transport_poller_t *poller,
    int64_t now_ns,
    aeron_driver_conductor_proxy_t *conductor_proxy)
{
    for (size_t i = 0; i < poller->transports.length; i++)
    {
        aeron_udp_channel_transport_t *transport = poller->transports.array[i].transport;
        aeron_receive_channel_endpoint_t *endpoint = transport->dispatch_clientd;

        aeron_receive_channel_endpoint_check_for_re_resolution(endpoint, now_ns, conductor_proxy);
    }

    return 0;
}
