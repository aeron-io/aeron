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

#ifndef AERON_DRIVER_CONDUCTOR_TEST_H
#define AERON_DRIVER_CONDUCTOR_TEST_H

#include <array>
#include <cstdint>
#include <thread>
#include <exception>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

extern "C"
{
#include "aeron_driver_conductor.h"
#include "util/aeron_error.h"
#include "aeron_driver_sender.h"
#include "aeron_driver_receiver.h"
#include "aeron_counters.h"
#include "concurrent/aeron_broadcast_receiver.h"
#include "concurrent/aeron_counters_manager.h"
#include "concurrent/aeron_mpsc_rb.h"
}

#define STR_HELPER(x) #x
#define STR(x) STR_HELPER(x)

#define CHANNEL_1 "aeron:udp?endpoint=localhost:10001"
#define CHANNEL_1_UNRELIABLE "aeron:udp?endpoint=localhost:10001|reliable=false"
#define CHANNEL_2 "aeron:udp?endpoint=localhost:10002"
#define CHANNEL_3 "aeron:udp?endpoint=localhost:10003"
#define CHANNEL_4 "aeron:udp?endpoint=localhost:10004"
#define CHANNEL_MDC_MANUAL "aeron:udp?control-mode=manual"
#define INVALID_URI "aeron:udp://"

#define STREAM_ID_1 (101)
#define STREAM_ID_2 (102)
#define STREAM_ID_3 (103)
#define STREAM_ID_4 (104)

#define SESSION_ID_1_ 1000

#define MTU_1 (4096)
#define MTU_2 (8192)

#define CHANNEL_1_WITH_TAG_1001 "aeron:udp?endpoint=localhost:10001|tags=1001"
#define CHANNEL_TAG_1001 "aeron:udp?tags=1001"

#define CHANNEL_1_WITH_SESSION_ID_1 "aeron:udp?endpoint=localhost:10001|session-id=" STR(SESSION_ID_1_)

#define SESSION_ID_1 (SESSION_ID_1_)
#define SESSION_ID_3 (100000)
#define SESSION_ID_4 (100002)
#define SESSION_ID_5 (100003)

#define SESSION_ID (0x5E5510)
#define INITIAL_TERM_ID (0x3456)

#define TERM_LENGTH (64 * 1024)

#define SRC_IP_ADDR "127.0.0.1"
#define SRC_UDP_PORT (43657)
#define SOURCE_IDENTITY "127.0.0.1:43657"

#define CONTROL_IP_ADDR "127.0.0.1"
#define CONTROL_UDP_PORT (43657)

#define TEST_CONDUCTOR_CYCLE_TIME_THRESHOLD (600 * 1000 * 1000)

#define CAPACITY (32 * 1024)
typedef std::array<std::uint8_t, CAPACITY> buffer_t;

static int64_t nano_time = 0;
static bool free_map_raw_log = true;

static int64_t test_nano_clock()
{
    return nano_time;
}

static int64_t test_epoch_clock()
{
    return nano_time / (1000 * 1000);
}

static void test_set_nano_time(int64_t timestamp_ns)
{
    nano_time = timestamp_ns;
}

static void test_increment_nano_time(int64_t delta_ns)
{
    nano_time += delta_ns;
}

static int test_malloc_raw_log_map(
    aeron_mapped_raw_log_t *log, const char *path, bool use_sparse_file, uint64_t term_length, uint64_t page_size)
{
    uint64_t log_length = aeron_logbuffer_compute_log_length(term_length, page_size);

    log->mapped_file.length = 0;
    log->mapped_file.addr = malloc(log_length);

    memset(log->mapped_file.addr, 0, log_length);

    for (size_t i = 0; i < AERON_LOGBUFFER_PARTITION_COUNT; i++)
    {
        log->term_buffers[i].addr = (uint8_t *)log->mapped_file.addr + (i * term_length);
        log->term_buffers[i].length = term_length;
    }

    log->log_meta_data.addr = (uint8_t *)log->mapped_file.addr + (log_length - AERON_LOGBUFFER_META_DATA_LENGTH);
    log->log_meta_data.length = AERON_LOGBUFFER_META_DATA_LENGTH;

    log->term_length = term_length;

    return 0;
}

static int test_malloc_raw_log_close(aeron_mapped_raw_log_t *log, const char *filename)
{
    free(log->mapped_file.addr);
    log->mapped_file.addr = nullptr;
    return 0;
}

static bool test_malloc_raw_log_free(aeron_mapped_raw_log_t *log, const char *filename)
{
    if (free_map_raw_log)
    {
        test_malloc_raw_log_close(log, filename);
        return true;
    }
    return false;
}

static uint64_t test_uint64_max_usable_fs_space(const char *path)
{
    return UINT64_MAX;
}

class DriverCallbacks
{
public:
    virtual ~DriverCallbacks() = default;

    virtual void broadcastToClient(int32_t type_id, uint8_t *buffer, size_t length) = 0;

    virtual void onCounter(
        int32_t id,
        int32_t type_id,
        const uint8_t *key,
        size_t key_len,
        const uint8_t *label,
        size_t label_len) = 0;
};

class MockDriverCallbacks : public DriverCallbacks
{
public:
    MOCK_METHOD3(broadcastToClient, void(int32_t, uint8_t *, size_t));
    MOCK_METHOD6(onCounter, void(int32_t, int32_t, const uint8_t *, size_t, const uint8_t *, size_t));
};

void mock_broadcast_handler(int32_t type_id, uint8_t *buffer, size_t length, void *clientd)
{
    auto *callback = static_cast<DriverCallbacks *>(clientd);
    callback->broadcastToClient(type_id, buffer, length);
}

void null_broadcast_handler(int32_t type_id, uint8_t *buffer, size_t length, void *clientd)
{
}

void mock_counter_handler(
    int32_t id,
    int32_t type_id,
    const uint8_t *key,
    size_t key_len,
    const uint8_t *label,
    size_t label_len,
    void *clientd)
{
    auto *callback = static_cast<DriverCallbacks *>(clientd);
    callback->onCounter(id, type_id, key, key_len, label, label_len);
}

struct TestDriverContext
{
    TestDriverContext()
    {
        test_set_nano_time(0); /* single threaded */

        if (aeron_driver_context_init(&m_context) < 0)
        {
            throw std::runtime_error("could not init context: " + std::string(aeron_errmsg()));
        }

        m_context->threading_mode = AERON_THREADING_MODE_SHARED;
        m_context->async_executor_threads = 0;
        m_context->cnc_map.length = aeron_cnc_length(m_context);
        m_cnc = std::unique_ptr<uint8_t[]>(new uint8_t[m_context->cnc_map.length]);
        m_context->cnc_map.addr = m_cnc.get();

        memset(m_context->cnc_map.addr, 0, m_context->cnc_map.length);

        aeron_driver_fill_cnc_metadata(m_context);

        m_context->term_buffer_length = TERM_LENGTH;
        m_context->ipc_term_buffer_length = TERM_LENGTH;
        m_context->term_buffer_sparse_file = true;

        /* control time */
        m_context->nano_clock = test_nano_clock;
        m_context->epoch_clock = test_epoch_clock;

        /* control files */
        m_context->usable_fs_space_func = test_uint64_max_usable_fs_space;
        m_context->raw_log_map_func = test_malloc_raw_log_map;
        m_context->raw_log_close_func = test_malloc_raw_log_close;
        m_context->raw_log_free_func = test_malloc_raw_log_free;

        aeron_driver_context_set_conductor_cycle_threshold_ns(m_context, TEST_CONDUCTOR_CYCLE_TIME_THRESHOLD);
    }

    virtual ~TestDriverContext()
    {
        m_context->cnc_map.addr = nullptr;
        aeron_driver_context_close(m_context);
    }

    aeron_driver_context_t *m_context = nullptr;
    aeron_counters_manager_t m_counters_manager = {};
    aeron_system_counters_t m_system_counters = {};
    aeron_distinct_error_log_t m_error_log = {};
    AERON_DECL_ALIGNED(buffer_t m_error_log_buffer, 16) = {};

    std::unique_ptr<uint8_t[]> m_cnc;
};

struct TestDriverConductor
{
    explicit TestDriverConductor(TestDriverContext &context)
    {
        if (aeron_driver_conductor_init(&m_conductor, context.m_context) < 0)
        {
            throw std::runtime_error("could not init context: " + std::string(aeron_errmsg()));
        }

        context.m_context->conductor_proxy = &m_conductor.conductor_proxy;

        if (aeron_driver_sender_init(
            &m_sender, context.m_context, &m_conductor.system_counters, &m_conductor.error_log) < 0)
        {
            throw std::runtime_error("could not init sender: " + std::string(aeron_errmsg()));
        }

        context.m_context->sender_proxy = &m_sender.sender_proxy;

        if (aeron_driver_receiver_init(
            &m_receiver, context.m_context, &m_conductor.system_counters, &m_conductor.error_log) < 0)
        {
            throw std::runtime_error("could not init receiver: " + std::string(aeron_errmsg()));
        }

        context.m_context->receiver_proxy = &m_receiver.receiver_proxy;
        m_destination.has_control_addr = false;
    }

    virtual ~TestDriverConductor()
    {
        aeron_driver_conductor_on_close(&m_conductor);
        aeron_driver_sender_on_close(&m_sender);
        aeron_driver_receiver_on_close(&m_receiver);
    }

    void manuallySetNextSessionId(int32_t nextSessionId)
    {
        m_conductor.next_session_id = nextSessionId;
    }

    aeron_driver_conductor_t m_conductor = {};
    aeron_driver_sender_t m_sender = {};
    aeron_driver_receiver_t m_receiver = {};
    aeron_receive_destination_t m_destination = {};
};

class DriverConductorTest
{
public:

    DriverConductorTest() :
        m_conductor(m_context)
    {
        aeron_mpsc_rb_init(
            &m_to_driver, m_context.m_context->to_driver_buffer, m_context.m_context->to_driver_buffer_length);
        aeron_broadcast_receiver_init(
            &m_broadcast_receiver,
            m_context.m_context->to_clients_buffer,
            m_context.m_context->to_clients_buffer_length);

        int64_t free_to_reuse_timeout_ms = 0;
        if (m_context.m_context->counter_free_to_reuse_ns > 0)
        {
            free_to_reuse_timeout_ms = (int64_t)m_context.m_context->counter_free_to_reuse_ns / (1000 * 1000LL);
            free_to_reuse_timeout_ms = free_to_reuse_timeout_ms <= 0 ? 1 : free_to_reuse_timeout_ms;
        }

        aeron_counters_manager_init(
            &m_context.m_counters_manager,
            m_context.m_context->counters_metadata_buffer,
            AERON_COUNTERS_METADATA_BUFFER_LENGTH(m_context.m_context->counters_values_buffer_length),
            m_context.m_context->counters_values_buffer,
            m_context.m_context->counters_values_buffer_length,
            m_context.m_context->cached_clock,
            free_to_reuse_timeout_ms);

        aeron_system_counters_init(&m_context.m_system_counters, &m_context.m_counters_manager);

        aeron_distinct_error_log_init(
            &m_context.m_error_log, m_context.m_error_log_buffer.data(), m_context.m_error_log_buffer.size(), aeron_epoch_clock);

        m_context.m_context->counters_manager = &m_context.m_counters_manager;
        m_context.m_context->system_counters = &m_context.m_system_counters;
        m_context.m_context->error_log = &m_context.m_error_log;
        m_context.m_context->error_buffer = m_context.m_error_log_buffer.data();
        m_context.m_context->error_buffer_length = m_context.m_error_log_buffer.size();
    }

    ~DriverConductorTest()
    {
        aeron_broadcast_receiver_close(&m_broadcast_receiver);
        aeron_system_counters_close(&m_context.m_system_counters);
        aeron_counters_manager_close(&m_context.m_counters_manager);
        aeron_distinct_error_log_close(&m_context.m_error_log);
    }

    size_t readAllBroadcastsFromConductor(aeron_broadcast_receiver_handler_t handler, size_t message_limit = SIZE_MAX)
    {
        size_t messages = 0;
        while (messages < message_limit &&
            0 != aeron_broadcast_receiver_receive(&m_broadcast_receiver, handler, &m_mockCallbacks))
        {
            messages++;
        }

        return messages;
    }

    size_t readCounters(aeron_counters_reader_foreach_metadata_func_t callback)
    {
        aeron_driver_context_t *ctx = m_context.m_context;
        aeron_counters_reader_t reader;
        aeron_counters_reader_init(
            &reader,
            ctx->counters_metadata_buffer,
            AERON_COUNTERS_METADATA_BUFFER_LENGTH(ctx->counters_values_buffer_length),
            ctx->counters_values_buffer,
            ctx->counters_values_buffer_length);

        aeron_counters_reader_foreach_metadata(
            reader.metadata, reader.metadata_length, callback, &m_mockCallbacks);
        return 0;
    }

    int64_t nextCorrelationId()
    {
        return aeron_mpsc_rb_next_correlation_id(&m_to_driver);
    }

    inline int writeCommand(int32_t msg_type_id, size_t length)
    {
        return aeron_mpsc_rb_write(&m_to_driver, msg_type_id, m_command_buffer, length);
    }

    int addIpcPublication(int64_t client_id, int64_t correlation_id, int32_t stream_id, bool is_exclusive)
    {
        return addPublication(client_id, correlation_id, AERON_IPC_CHANNEL, stream_id, is_exclusive);
    }

    int addPublication(
        int64_t client_id, int64_t correlation_id, std::string & channel, int32_t stream_id, bool is_exclusive)
    {
        return addPublication(client_id, correlation_id, channel.c_str(), stream_id, is_exclusive);
    }

    int addPublication(
        int64_t client_id, int64_t correlation_id, const char *channel, int32_t stream_id, bool is_exclusive)
    {
        auto *cmd = reinterpret_cast<aeron_publication_command_t *>(m_command_buffer);

        int32_t msg_type_id = is_exclusive ? AERON_COMMAND_ADD_EXCLUSIVE_PUBLICATION : AERON_COMMAND_ADD_PUBLICATION;

        cmd->correlated.client_id = client_id;
        cmd->correlated.correlation_id = correlation_id;
        cmd->stream_id = stream_id;
        cmd->channel_length = (int32_t)strlen(channel);
        memcpy(m_command_buffer + sizeof(aeron_publication_command_t), channel, (size_t)cmd->channel_length);

        return writeCommand(msg_type_id, sizeof(aeron_publication_command_t) + cmd->channel_length);
    }

    int removePublication(int64_t client_id, int64_t correlation_id, int64_t registration_id)
    {
        auto *cmd = reinterpret_cast<aeron_remove_publication_command_t *>(m_command_buffer);

        cmd->correlated.client_id = client_id;
        cmd->correlated.correlation_id = correlation_id;
        cmd->registration_id = registration_id;
        cmd->flags = 0;

        return writeCommand(AERON_COMMAND_REMOVE_PUBLICATION, sizeof(aeron_remove_publication_command_t));
    }

    int addIpcSubscription(int64_t client_id, int64_t correlation_id, int32_t stream_id, int64_t registration_id)
    {
        return addNetworkSubscription(client_id, correlation_id, AERON_IPC_CHANNEL, stream_id);
    }

    int addSubscription(
        int64_t client_id, int64_t correlation_id, std::string &channel, int32_t stream_id, int64_t registration_id)
    {
        return addNetworkSubscription(client_id, correlation_id, channel.c_str(), stream_id);
    }

    int addNetworkSubscription(int64_t client_id, int64_t correlation_id, const char *channel, int32_t stream_id)
    {
        auto *cmd = reinterpret_cast<aeron_subscription_command_t *>(m_command_buffer);

        cmd->correlated.client_id = client_id;
        cmd->correlated.correlation_id = correlation_id;
        cmd->stream_id = stream_id;
        cmd->registration_correlation_id = -1;
        cmd->channel_length = (int32_t)strlen(channel);
        memcpy(m_command_buffer + sizeof(aeron_subscription_command_t), channel, (size_t)cmd->channel_length);

        return writeCommand(AERON_COMMAND_ADD_SUBSCRIPTION, sizeof(aeron_subscription_command_t) + cmd->channel_length);
    }

    int addSpySubscription(
        int64_t client_id, int64_t correlation_id, const char *channel, int32_t stream_id, int64_t registration_id)
    {
        std::string channel_str(AERON_SPY_PREFIX + std::string(channel));
        return addNetworkSubscription(client_id, correlation_id, channel_str.c_str(), stream_id);
    }

    int removeSubscription(int64_t client_id, int64_t correlation_id, int64_t registration_id)
    {
        auto *cmd = reinterpret_cast<aeron_remove_subscription_command_t *>(m_command_buffer);

        cmd->correlated.client_id = client_id;
        cmd->correlated.correlation_id = correlation_id;
        cmd->registration_id = registration_id;

        return writeCommand(AERON_COMMAND_REMOVE_SUBSCRIPTION, sizeof(aeron_remove_subscription_command_t));
    }

    int clientKeepalive(int64_t client_id)
    {
        auto *cmd = reinterpret_cast<aeron_correlated_command_t *>(m_command_buffer);

        cmd->client_id = client_id;

        return writeCommand(AERON_COMMAND_CLIENT_KEEPALIVE, sizeof(aeron_correlated_command_t));
    }

    int addCounter(
        int64_t client_id,
        int64_t correlation_id,
        int32_t type_id,
        const uint8_t *key,
        size_t key_length,
        std::string &label)
    {
        auto *cmd = reinterpret_cast<aeron_counter_command_t *>(m_command_buffer);
        auto *cmd_ptr = reinterpret_cast<uint8_t *>(m_command_buffer);

        cmd->correlated.client_id = client_id;
        cmd->correlated.correlation_id = correlation_id;
        cmd->type_id = type_id;

        uint8_t *cursor = cmd_ptr + sizeof(aeron_counter_command_t);
        memcpy(cursor, &key_length, sizeof(int32_t));
        cursor += sizeof(int32_t);
        memcpy(cursor, key, key_length);
        cursor += AERON_ALIGN(key_length, sizeof(int32_t));

        size_t label_len = label.length();
        memcpy(cursor, &label_len, sizeof(int32_t));
        cursor += sizeof(int32_t);
        memcpy(cursor, label.c_str(), label_len);
        cursor += label_len;

        size_t message_len = cursor - cmd_ptr;
        return writeCommand(AERON_COMMAND_ADD_COUNTER, message_len);
    }

    int removeCounter(int64_t client_id, int64_t correlation_id, int64_t registration_id)
    {
        auto *cmd = reinterpret_cast<aeron_remove_counter_command_t *>(m_command_buffer);

        cmd->correlated.client_id = client_id;
        cmd->correlated.correlation_id = correlation_id;
        cmd->registration_id = registration_id;

        return writeCommand(AERON_COMMAND_REMOVE_COUNTER, sizeof(aeron_remove_counter_command_t));
    }

    int addDestination(
        int64_t client_id, int64_t correlation_id, int64_t publication_registration_id, const char *channel)
    {
        auto *cmd = reinterpret_cast<aeron_destination_command_t *>(m_command_buffer);

        cmd->correlated.client_id = client_id;
        cmd->correlated.correlation_id = correlation_id;
        cmd->registration_id = publication_registration_id;
        cmd->channel_length = (int32_t)strlen(channel);
        memcpy(m_command_buffer + sizeof(aeron_destination_command_t), channel, (size_t)cmd->channel_length);

        return writeCommand(AERON_COMMAND_ADD_DESTINATION, sizeof(aeron_destination_command_t) + cmd->channel_length);
    }

    int addReceiveDestination(int64_t client_id, int64_t correlation_id, int64_t subscription_id, const char *channel)
    {
        auto *cmd = reinterpret_cast<aeron_destination_command_t *>(m_command_buffer);

        cmd->correlated.client_id = client_id;
        cmd->correlated.correlation_id = correlation_id;
        cmd->registration_id = subscription_id;
        cmd->channel_length = (int32_t)strlen(channel);
        memcpy(m_command_buffer + sizeof(aeron_destination_command_t), channel, (size_t)cmd->channel_length);

        return writeCommand(AERON_COMMAND_ADD_RCV_DESTINATION, sizeof(aeron_destination_command_t) + cmd->channel_length);
    }

    int removeReceiveDestination(int64_t client_id, int64_t correlation_id, int64_t subscription_id, const char *channel)
    {
        auto *cmd = reinterpret_cast<aeron_destination_command_t *>(m_command_buffer);

        cmd->correlated.client_id = client_id;
        cmd->correlated.correlation_id = correlation_id;
        cmd->registration_id = subscription_id;
        cmd->channel_length = (int32_t)strlen(channel);
        memcpy(m_command_buffer + sizeof(aeron_destination_command_t), channel, (size_t)cmd->channel_length);

        return writeCommand(AERON_COMMAND_REMOVE_RCV_DESTINATION, sizeof(aeron_destination_command_t) + cmd->channel_length);
    }

    int removeDestination(
        int64_t client_id, int64_t correlation_id, int64_t publication_registration_id, const char *channel)
    {
        auto *cmd = reinterpret_cast<aeron_destination_command_t *>(m_command_buffer);

        cmd->correlated.client_id = client_id;
        cmd->correlated.correlation_id = correlation_id;
        cmd->registration_id = publication_registration_id;
        cmd->channel_length = (int32_t)strlen(channel);
        memcpy(m_command_buffer + sizeof(aeron_destination_command_t), channel, (size_t)cmd->channel_length);

        return writeCommand(AERON_COMMAND_REMOVE_DESTINATION, sizeof(aeron_destination_command_t) + cmd->channel_length);
    }

    int closeClient(int64_t client_id)
    {
        auto *cmd = reinterpret_cast<aeron_correlated_command_t *>(m_command_buffer);

        cmd->client_id = client_id;
        cmd->correlation_id = 0;

        return writeCommand(AERON_COMMAND_CLIENT_CLOSE, sizeof(aeron_correlated_command_t));
    }

    int doWork()
    {
        return aeron_driver_conductor_do_work(&m_conductor.m_conductor);
    }

    void doWorkUntilDone()
    {
        while (true)
        {
            if (0 == doWork())
            {
                break;
            }
        }
    }

    void doWorkForNs(int64_t duration_ns, int64_t num_increments = 100, std::function<void()> func = [](){})
    {
        int64_t initial_ns = test_nano_clock();
        int64_t increment_ns = duration_ns / num_increments;

        if (increment_ns <= 0)
        {
            throw std::runtime_error("increment must be positive");
        }

        do
        {
            test_increment_nano_time(increment_ns);
            func();
            doWork();
        }
        while ((test_nano_clock() - initial_ns) <= duration_ns);
    }

    static void fill_sockaddr_ipv4(struct sockaddr_storage *addr, const char *ip, unsigned short int port)
    {
        auto *ipv4addr = (struct sockaddr_in *)addr;

        ipv4addr->sin_family = AF_INET;
        if (inet_pton(AF_INET, ip, &ipv4addr->sin_addr) != 1)
        {
            throw std::runtime_error("can't get IPv4 address");
        }
        ipv4addr->sin_port = htons(port);
    }

    void createPublicationImage(aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id, int64_t position)
    {
        aeron_command_create_publication_image_t cmd;
        auto position_bits_to_shift = (size_t)aeron_number_of_trailing_zeroes(TERM_LENGTH);

        cmd.base.func = aeron_driver_conductor_on_create_publication_image;
        cmd.base.item = nullptr;
        cmd.endpoint = endpoint;
        cmd.destination = &m_conductor.m_destination;
        cmd.session_id = SESSION_ID;
        cmd.stream_id = stream_id;
        cmd.term_offset = 0;
        cmd.active_term_id = aeron_logbuffer_compute_term_id_from_position(
            position, position_bits_to_shift, INITIAL_TERM_ID);
        cmd.initial_term_id = INITIAL_TERM_ID;
        cmd.mtu_length = (int32_t)m_context.m_context->mtu_length;
        cmd.term_length = TERM_LENGTH;

        fill_sockaddr_ipv4(&cmd.src_address, SRC_IP_ADDR, SRC_UDP_PORT);
        fill_sockaddr_ipv4(&cmd.control_address, CONTROL_IP_ADDR, CONTROL_UDP_PORT);

        aeron_driver_conductor_on_create_publication_image(&m_conductor.m_conductor, &cmd);
    }

protected:
    uint8_t m_command_buffer[128 * 1024] = {};
    TestDriverContext m_context = {};
    TestDriverConductor m_conductor;
    aeron_broadcast_receiver_t m_broadcast_receiver = {};
    aeron_mpsc_rb_t m_to_driver = {};
    MockDriverCallbacks m_mockCallbacks = {};
};

void aeron_image_buffers_ready_get_log_file_name(
    const aeron_image_buffers_ready_t *msg, const char **log_file_name, int32_t *log_file_name_len)
{
    uint8_t *log_file_name_ptr = ((uint8_t *)msg) + sizeof(aeron_image_buffers_ready_t);
    memcpy(log_file_name_len, log_file_name_ptr, sizeof(int32_t));
    *log_file_name = reinterpret_cast<const char *>(log_file_name_ptr + sizeof(int32_t));
}

void aeron_publication_buffers_ready_get_log_file_name(
    const aeron_publication_buffers_ready_t *msg, const char **log_file_name, int32_t *log_file_name_len)
{
    *log_file_name_len = msg->log_file_length;
    *log_file_name = ((char *)msg) + sizeof(aeron_publication_buffers_ready_t);
}

void aeron_image_buffers_ready_get_source_identity(
    const aeron_image_buffers_ready_t *msg, const char **source_identity, int32_t *source_identity_len)
{
    uint8_t *log_file_name_ptr = ((uint8_t *)msg) + sizeof(aeron_image_buffers_ready_t);
    int32_t log_file_name_len;
    memcpy(&log_file_name_len, log_file_name_ptr, sizeof(int32_t));
    int32_t aligned_log_file_name_len = AERON_ALIGN(log_file_name_len, sizeof(int32_t));
    uint8_t *source_identity_ptr = log_file_name_ptr + sizeof(int32_t) + aligned_log_file_name_len;
    memcpy(source_identity_len, source_identity_ptr, sizeof(int32_t));
    *source_identity = reinterpret_cast<const char *>(source_identity_ptr + sizeof(int32_t));
}

void aeron_image_message_get_channel(
    const aeron_image_message_t *msg, const char **channel, int32_t *channel_len)
{
    uint8_t *channel_ptr = ((uint8_t *)msg) + sizeof(aeron_image_message_t);
    *channel_len = msg->channel_length;
    *channel = reinterpret_cast<const char *>(channel_ptr);
}

MATCHER_P(
    IsSubscriptionReady,
    correlation_id,
    std::string("IsSubscriptionReady: correlationId = ").append(testing::PrintToString(correlation_id)))
{
    const aeron_subscription_ready_t *response = reinterpret_cast<aeron_subscription_ready_t *>(std::get<1>(arg));
    const bool result = response->correlation_id == correlation_id;

    if (!result)
    {
        *result_listener << "response.correlation_id = " << response->correlation_id;
    }

    return result;
}

MATCHER_P(
    IsError,
    correlation_id,
    std::string("IsError: correlation_id = ").append(testing::PrintToString(correlation_id)))
{
    const aeron_error_response_t *response = reinterpret_cast<aeron_error_response_t *>(std::get<1>(arg));
    const bool result = response->offending_command_correlation_id == correlation_id;

    if (!result)
    {
        *result_listener << "response.offending_command_correlation_id = " << response->offending_command_correlation_id;
    }

    return result;
}

MATCHER_P(
    IsOperationSuccess,
    correlation_id,
    std::string("IsOperationSuccess: correlationId = ").append(testing::PrintToString(correlation_id)))
{
    const aeron_operation_succeeded_t *response  = reinterpret_cast<aeron_operation_succeeded_t *>(std::get<1>(arg));
    const bool result = response->correlation_id == correlation_id;

    if (!result)
    {
        *result_listener << "response.correlationId() = " << response->correlation_id;
    }

    return result;
}

MATCHER_P3(
    IsPublicationReady,
    correlation_id,
    stream_id,
    session_id,
    std::string("IsPublicationReady: correlationId = ").append(testing::PrintToString(correlation_id))
        .append(", streamId = ").append(testing::DescribeMatcher<int32_t>(stream_id))
        .append(", sessionId = ").append(testing::DescribeMatcher<int32_t>(session_id)))
{
    const aeron_publication_buffers_ready_t *response = reinterpret_cast<aeron_publication_buffers_ready_t *>(
        std::get<1>(arg));
    bool result = testing::Value(response->stream_id, stream_id) &&
        testing::Value(response->session_id, session_id) &&
        testing::Value(response->correlation_id, correlation_id) &&
        0 < response->log_file_length;

    if (!result)
    {
        *result_listener <<
            "response.streamId() = " << response->stream_id <<
            ", response.correlationId() = " << response->stream_id <<
            ", response.logFileName().length() = " << response->log_file_length;
    }

    return result;
}

MATCHER_P5(
    IsPubReadyWithFile,
    registration_id,
    correlation_id,
    stream_id,
    session_id,
    log_file_name,
    std::string("IsPubReadyWithFile: ")
        .append("registration_id = ").append(testing::DescribeMatcher<int64_t>(correlation_id))
        .append(", correlation_id = ").append(testing::DescribeMatcher<int64_t>(correlation_id))
        .append(", stream_id = ").append(testing::DescribeMatcher<int32_t>(stream_id))
        .append(", session_id = ").append(testing::DescribeMatcher<int32_t>(session_id))
        .append(", log_file_name = ").append(testing::DescribeMatcher<std::string>(log_file_name)))
{
    const aeron_publication_buffers_ready_t *response = reinterpret_cast<aeron_publication_buffers_ready_t *>(
        std::get<1>(arg));
    bool result = true;
    result &= testing::Value(response->stream_id, stream_id);
    result &= testing::Value(response->session_id, session_id);
    result &= testing::Value(response->correlation_id, correlation_id);
    result &= testing::Value(response->registration_id, registration_id);

    const char *response_log_file_name;
    int32_t log_file_name_length;
    aeron_publication_buffers_ready_get_log_file_name(response, &response_log_file_name, &log_file_name_length);
    std::string log_file_name_str = std::string(response_log_file_name, (size_t)log_file_name_length);

    result &= testing::Value(log_file_name_str, log_file_name);

    if (!result)
    {
        *result_listener <<
             "response.stream_id = " << response->stream_id <<
             ", response.session_id = " << response->session_id <<
             ", response.correlation_id = " << response->correlation_id <<
             ", response.registration_id = " << response->registration_id <<
             ", response.log_file_name = " << log_file_name_str;
    }

    return result;
}

ACTION_P2(CapturePublicationReady, session_id_out, log_file_name_out)
{
    const aeron_publication_buffers_ready_t *response = reinterpret_cast<aeron_publication_buffers_ready_t *>(arg1);
    *session_id_out = response->session_id;
    const char *log_file_name;
    int32_t log_file_name_length;
    aeron_publication_buffers_ready_get_log_file_name(response, &log_file_name, &log_file_name_length);
    log_file_name_out->append(log_file_name, log_file_name_length);
}

MATCHER_P(
    IsTimeout,
    client_id,
    std::string("IsTimeout: client_id = ").append(testing::PrintToString(client_id)))
{
    const aeron_client_timeout_t *response = reinterpret_cast<aeron_client_timeout_t *>(std::get<1>(arg));
    bool result = testing::Value(response->client_id, client_id);

    if (!result)
    {
        *result_listener << "response.client_id = " << response->client_id;
    }

    return result;
}

MATCHER_P6(
    IsAvailableImage,
    image_registration_id,
    subscription_registration_id,
    stream_id,
    session_id,
    log_file_name,
    source_identity,
    std::string("IsAvailableImage: ")
        .append("image_registration_id = ").append(testing::PrintToString(image_registration_id))
        .append(", subscription_registration_id = ").append(testing::PrintToString(subscription_registration_id))
        .append(", stream_id = ").append(testing::PrintToString(stream_id))
        .append(", session_id = ").append(testing::PrintToString(session_id))
        .append(", log_file_name = ").append(testing::PrintToString(log_file_name))
        .append(", source_identity = ").append(testing::PrintToString(source_identity)))
{
    const aeron_image_buffers_ready_t *response = reinterpret_cast<aeron_image_buffers_ready_t *>(std::get<1>(arg));
    bool result = true;
    result &= response->session_id == session_id;
    result &= response->stream_id == stream_id;
    result &= testing::Value(response->correlation_id, image_registration_id);
    result &= response->subscriber_registration_id == subscription_registration_id;

    int32_t response_log_file_name_length;
    const char *response_log_file_name;
    aeron_image_buffers_ready_get_log_file_name(response, &response_log_file_name, &response_log_file_name_length);

    int32_t response_source_identity_length;
    const char *response_source_identity;
    aeron_image_buffers_ready_get_source_identity(response, &response_source_identity, &response_source_identity_length);
    const std::string str_log_file_name = std::string(response_log_file_name, (size_t)response_log_file_name_length);
    const std::string str_source_identity = std::string(response_source_identity, (size_t)response_source_identity_length);

    result &= 0 == std::string(log_file_name).compare(str_log_file_name);
    result &= 0 == std::string(source_identity).compare(str_source_identity);

    if (!result)
    {
        *result_listener <<
            "response.correlation_id = " << response->correlation_id <<
            ", response.subscription_registration_id = " << response->subscriber_registration_id <<
            ", response.stream_id = " << response->stream_id <<
            ", response.session_id = " << response->session_id <<
            ", response.log_file_name = " << str_log_file_name <<
            ", response.source_identity = " << str_source_identity;
    }

    return result;
}

MATCHER_P5(
    IsImageBuffersReady,
    subscription_registration_id,
    stream_id,
    session_id,
    log_file_name,
    source_identity,
    std::string("IsAvailableImage: ")
        .append(", subscription_registration_id = ").append(testing::PrintToString(subscription_registration_id))
        .append(", stream_id = ").append(testing::PrintToString(stream_id))
        .append(", session_id = ").append(testing::PrintToString(session_id))
        .append(", log_file_name = ").append(testing::PrintToString(log_file_name))
        .append(", source_identity = ").append(testing::PrintToString(source_identity)))
{
    bool result = true;
    result &= arg->session_id == session_id;
    result &= arg->stream_id == stream_id;
    result &= arg->subscriber_registration_id == subscription_registration_id;

    int32_t response_log_file_name_length;
    const char *response_log_file_name;
    aeron_image_buffers_ready_get_log_file_name(arg, &response_log_file_name, &response_log_file_name_length);

    int32_t response_source_identity_length;
    const char *response_source_identity;
    aeron_image_buffers_ready_get_source_identity(arg, &response_source_identity, &response_source_identity_length);
    const std::string str_log_file_name = std::string(response_log_file_name, (size_t)response_log_file_name_length);
    const std::string str_source_identity = std::string(response_source_identity, (size_t)response_source_identity_length);

    result &= 0 == log_file_name.compare(str_log_file_name);
    result &= 0 == source_identity.compare(str_source_identity);

    if (!result)
    {
        *result_listener <<
             "response.correlation_id = " << arg->correlation_id <<
             ", response.subscription_registration_id = " << arg->subscriber_registration_id <<
             ", response.stream_id = " << arg->stream_id <<
             ", response.session_id = " << arg->session_id <<
             ", response.log_file_name = " << str_log_file_name <<
             ", response.source_identity = " << str_source_identity;
    }

    return result;
}

MATCHER_P4(
    IsUnavailableImage,
    stream_id,
    correlation_id,
    subscription_registration_id,
    channel,
    std::string("IsAvailableImage: ")
        .append("stream_id = ").append(testing::PrintToString(stream_id))
        .append(", correlation_id = ").append(testing::PrintToString(correlation_id))
        .append(", subscription_registration_id = ").append(testing::PrintToString(subscription_registration_id))
        .append(", channel = ").append(testing::PrintToString(channel)))
{
    const aeron_image_message_t *response = reinterpret_cast<aeron_image_message_t *>(std::get<1>(arg));
    bool result = true;
    result &= response->correlation_id == correlation_id;
    result &= response->stream_id == stream_id;
    result &= response->subscription_registration_id == subscription_registration_id;

    int32_t response_channel_len;
    const char *response_channel;
    aeron_image_message_get_channel(response, &response_channel, &response_channel_len);
    const std::string str_channel = std::string(response_channel, (size_t)response_channel_len);

    result &= str_channel == std::string(channel);

    if (!result)
    {
        *result_listener <<
            "response.correlation_id = " << response->correlation_id <<
            ", response.subscription_registration_id = " << response->subscription_registration_id <<
            ", response.stream_id = " << response->stream_id <<
            ", response.channel = " << str_channel;
    }

    return result;
}

MATCHER_P2(
    IsCounterUpdate,
    correlation_id,
    counter_id,
    std::string("IsCounterUnavailable: ")
        .append("correlation_id = ").append(testing::PrintToString(correlation_id))
        .append(", counter_id = ").append(testing::PrintToString(counter_id))
    )
{
    const aeron_counter_update_t *response = reinterpret_cast<aeron_counter_update_t *>(std::get<1>(arg));

    bool result = true;
    result &= testing::Value(response->correlation_id, correlation_id);
    result &= testing::Value(response->counter_id, counter_id);

    if (!result)
    {
        *result_listener <<
            "response.correlation_id = " << response->correlation_id <<
            ", response.counter_id = " << response->counter_id;
    }

    return result;
}

MATCHER_P2(
    IsIdCounter,
    id,
    label,
    std::string("IsIdCounter: ")
        .append("key = ").append(testing::PrintToString(id))
        .append(", label = ").append(label)
    )
{
    const uint8_t *key_buffer = std::get<2>(arg);
    int64_t counter_id;
    memcpy(&counter_id, key_buffer, sizeof(counter_id));
    const std::string counter_label = std::string((char *)std::get<4>(arg), std::get<5>(arg));

    bool result = true;
    result &= testing::Value(counter_id, id);
    result &= testing::Value(counter_label, label);

    if (!result)
    {
        *result_listener <<
            "counter.id = " << counter_id <<
            "counter.label = " << counter_label;
    }

    return result;
}

ACTION_P(CaptureCounterId, counter_id_out)
{
    const aeron_counter_update_t *response = reinterpret_cast<aeron_counter_update_t *>(arg1);
    *counter_id_out = response->counter_id;
}

#endif //AERON_DRIVER_CONDUCTOR_TEST_H
