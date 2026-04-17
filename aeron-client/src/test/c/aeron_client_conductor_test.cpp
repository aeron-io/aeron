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

#include <cstdint>
#include <thread>
#include <functional>

#include <gtest/gtest.h>

#include "aeron_client_test_utils.h"

extern "C"
{
#include "aeron_publication.h"
#include "aeron_exclusive_publication.h"
#include "aeron_subscription.h"
#include "aeron_context.h"
#include "aeron_cnc_file_descriptor.h"
#include "concurrent/aeron_mpsc_rb.h"
#include "concurrent/aeron_broadcast_transmitter.h"
#include "concurrent/aeron_counters_manager.h"
#include "aeron_client_conductor.h"
#include "aeron_counter.h"
#include "aeron_counters.h"
#include "aeron_client.h"
}

#define CAPACITY (16 * 1024)
#define MAX_MESSAGE_SIZE (CAPACITY / 8 - AERON_RB_RECORD_HEADER_LENGTH)
#define TO_DRIVER_RING_BUFFER_LENGTH (CAPACITY + AERON_RB_TRAILER_LENGTH)
#define TO_CLIENTS_BUFFER_LENGTH (CAPACITY + AERON_BROADCAST_BUFFER_TRAILER_LENGTH)
#define COUNTER_VALUES_BUFFER_LENGTH (1024 * 1024)
#define COUNTER_METADATA_BUFFER_LENGTH (AERON_COUNTERS_METADATA_BUFFER_LENGTH(COUNTER_VALUES_BUFFER_LENGTH))
#define ERROR_BUFFER_LENGTH (CAPACITY)
#define FILE_PAGE_SIZE (4 * 1024)

#define CLIENT_IDLE_SLEEP_INTERVAL (16 * 1000 * 1000LL)
#define CLIENT_LIVENESS_TIMEOUT (5 * 1000 * 1000 * 1000LL)
#define DRIVER_TIMEOUT_INTERVAL_MS (1 * 1000)
#define DRIVER_TIMEOUT_INTERVAL_NS (DRIVER_TIMEOUT_INTERVAL_MS * 1000 * 1000LL)

#define TIME_ADVANCE_INTERVAL_NS (1000 * 1000LL)

#define URI_RESERVED "aeron:udp?endpoint=localhost:24567"
#define DEST_URI "aeron:udp?endpoint=localhost:24568"
#define SUB_URI "aeron:udp?endpoint=localhost:24567"
#define STREAM_ID (101)
#define SESSION_ID (110)
#define COUNTER_TYPE_ID (102)

static int64_t now_ms = 0;
static int64_t now_ns = 0;

static int64_t test_epoch_clock()
{
    return now_ms;
}

static int64_t test_nano_clock()
{
    return now_ns;
}

static void save_last_errorcode(void *clientd, int errcode, const char *message)
{
    int *last_errorcode = static_cast<int *>(clientd);
    *last_errorcode = errcode;
}

using namespace aeron::test;

class ClientConductorTest : public testing::Test
{
public:

    static void on_new_publication(
        void *clientd,
        const char *channel,
        int32_t stream_id,
        int32_t session_id,
        int64_t correlation_id)
    {
        auto conductorTest = reinterpret_cast<ClientConductorTest *>(clientd);

        if (conductorTest->m_on_new_publication)
        {
            conductorTest->m_on_new_publication(channel, stream_id, session_id, correlation_id);
        }
    }

    static void on_new_exclusive_publication(
        void *clientd,
        const char *channel,
        int32_t stream_id,
        int32_t session_id,
        int64_t correlation_id)
    {
        auto conductorTest = reinterpret_cast<ClientConductorTest *>(clientd);

        if (conductorTest->m_on_new_exclusive_publication)
        {
            conductorTest->m_on_new_exclusive_publication(channel, stream_id, session_id, correlation_id);
        }
    }

    static void on_new_subscription(
        void *clientd,
        const char *channel,
        int32_t stream_id,
        int64_t correlation_id)
    {
        auto conductorTest = reinterpret_cast<ClientConductorTest *>(clientd);

        if (conductorTest->m_on_new_subscription)
        {
            conductorTest->m_on_new_subscription(channel, stream_id, correlation_id);
        }
    }

    ClientConductorTest() :
        m_logFileName(tempFileName()),
        m_on_new_publication(nullptr),
        m_on_new_exclusive_publication(nullptr)
    {
        now_ns = 0;
        now_ms = 0;

        if (aeron_context_init(&m_context) < 0)
        {
            throw std::runtime_error("could not init context: " + std::string(aeron_errmsg()));
        }

        m_context->cnc_map.length = aeron_cnc_computed_length(
            TO_DRIVER_RING_BUFFER_LENGTH +
            TO_CLIENTS_BUFFER_LENGTH +
            COUNTER_VALUES_BUFFER_LENGTH +
            COUNTER_METADATA_BUFFER_LENGTH +
            ERROR_BUFFER_LENGTH,
            FILE_PAGE_SIZE);
        m_cnc = std::unique_ptr<uint8_t[]>(new uint8_t[m_context->cnc_map.length]);
        m_context->cnc_map.addr = m_cnc.get();
        memset(m_context->cnc_map.addr, 0, m_context->cnc_map.length);

        m_context->epoch_clock = test_epoch_clock;
        m_context->nano_clock = test_nano_clock;
        m_context->driver_timeout_ms = DRIVER_TIMEOUT_INTERVAL_MS;
        m_context->keepalive_interval_ns = DRIVER_TIMEOUT_INTERVAL_NS;
        m_context->idle_sleep_duration_ns = CLIENT_IDLE_SLEEP_INTERVAL;

        aeron_context_set_use_conductor_agent_invoker(m_context, true);

        aeron_context_set_on_new_publication(m_context, on_new_publication, this);
        aeron_context_set_on_new_exclusive_publication(m_context, on_new_exclusive_publication, this);
        aeron_context_set_on_new_subscription(m_context, on_new_subscription, this);

        auto *metadata = (aeron_cnc_metadata_t *)m_context->cnc_map.addr;
        metadata->to_driver_buffer_length = (int32_t)TO_DRIVER_RING_BUFFER_LENGTH;
        metadata->to_clients_buffer_length = (int32_t)TO_CLIENTS_BUFFER_LENGTH;
        metadata->counter_metadata_buffer_length = (int32_t)COUNTER_METADATA_BUFFER_LENGTH;
        metadata->counter_values_buffer_length = (int32_t)COUNTER_VALUES_BUFFER_LENGTH;
        metadata->error_log_buffer_length = (int32_t)ERROR_BUFFER_LENGTH;
        metadata->client_liveness_timeout = (int64_t)CLIENT_LIVENESS_TIMEOUT;
        metadata->start_timestamp = test_epoch_clock();
        metadata->pid = 101;
        AERON_SET_RELEASE(metadata->cnc_version, AERON_CNC_VERSION);

        if (aeron_mpsc_rb_init(
            &m_to_driver, aeron_cnc_to_driver_buffer(metadata), TO_DRIVER_RING_BUFFER_LENGTH) < 0)
        {
            throw std::runtime_error("could not init to_driver: " + std::string(aeron_errmsg()));
        }

        if (aeron_broadcast_transmitter_init(
            &m_to_clients, aeron_cnc_to_clients_buffer(metadata), TO_CLIENTS_BUFFER_LENGTH) < 0)
        {
            throw std::runtime_error("could not init to_clients: " + std::string(aeron_errmsg()));
        }

        if (aeron_counters_manager_init(
            &m_counters_manager,
            aeron_cnc_counters_metadata_buffer(metadata),
            (size_t)metadata->counter_metadata_buffer_length,
            aeron_cnc_counters_values_buffer(metadata),
            (size_t)metadata->counter_values_buffer_length,
            &m_cached_clock,
            1000) < 0)
        {
            throw std::runtime_error("could not init counters manager: " + std::string(aeron_errmsg()));
        }

        if (aeron_client_conductor_init(&m_conductor, m_context) < 0)
        {
            throw std::runtime_error("could not init conductor: " + std::string(aeron_errmsg()));
        }
    }

    ~ClientConductorTest() override
    {
        if (!m_conductor_isClosed)
        {
            aeron_client_conductor_on_close(&m_conductor);
        }
        aeron_counters_manager_close(&m_counters_manager);
        m_context->cnc_map.addr = nullptr;
        aeron_context_close(m_context);

        ::unlink(m_logFileName.c_str());
    }

    static void ToDriverHandler(int32_t type_id, const void *buffer, size_t length, void *clientd)
    {
        auto conductorTest = reinterpret_cast<ClientConductorTest *>(clientd);

        conductorTest->m_to_driver_handler(type_id, buffer, length);
    }

     static std::string allocateStringWithPrefix(
         const std::string& prefix, const std::string suffix, const char c, const size_t length)
    {
        std::string result = std::string(prefix).append(suffix);
        if (length > result.length())
        {
            result.append(length - result.length(), c);
        }

        return result;
    }

    int doWork(bool updateDriverHeartbeat = true)
    {
        int work_count;

        if (updateDriverHeartbeat)
        {
            aeron_mpsc_rb_consumer_heartbeat_time(&m_to_driver, test_epoch_clock());
        }

        if ((work_count = aeron_client_conductor_do_work(&m_conductor)) < 0)
        {
            throw std::runtime_error("error from do_work: " + std::string(aeron_errmsg()));
        }

        return work_count;
    }

    int doWorkForNs(
        int64_t interval_ns, bool updateDriverHeartbeat = true, int64_t advance_interval_ns = TIME_ADVANCE_INTERVAL_NS)
    {
        int work_count = 0;
        int64_t target_ns = now_ns + interval_ns;

        do
        {
            now_ns += advance_interval_ns;
            now_ms = now_ns / 1000000LL;
            work_count += doWork(updateDriverHeartbeat);
        }
        while (now_ns < target_ns);

        return work_count;
    }

    void transmitOnPublicationReady(aeron_async_add_publication_t *async, const std::string &logFile, bool isExclusive)
    {
        char response_buffer[sizeof(aeron_publication_buffers_ready_t) + AERON_ERROR_MAX_TOTAL_LENGTH];
        auto response = reinterpret_cast<aeron_publication_buffers_ready_t *>(response_buffer);
        int32_t position_limit_counter_id = 10, channel_status_indicator_id = 11;

        response->correlation_id = async->registration_id;
        response->registration_id = async->registration_id;
        response->stream_id = async->stream_id;
        response->session_id = SESSION_ID;
        response->position_limit_counter_id = position_limit_counter_id;
        response->channel_status_indicator_id = channel_status_indicator_id;
        response->log_file_length = static_cast<int32_t>(logFile.length());
        memcpy(response_buffer + sizeof(aeron_publication_buffers_ready_t), logFile.c_str(), logFile.length());

        if (aeron_broadcast_transmitter_transmit(
            &m_to_clients,
            isExclusive ? AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY : AERON_RESPONSE_ON_PUBLICATION_READY,
            response_buffer,
            sizeof(aeron_publication_buffers_ready_t) + logFile.length()) < 0)
        {
            throw std::runtime_error("error transmitting ON_PUBLICATION_READY: " + std::string(aeron_errmsg()));
        }
    }

    void transmitOnOperationSuccess(aeron_async_destination_t *async)
    {
        char response_buffer[sizeof(aeron_operation_succeeded_t)];
        auto response = reinterpret_cast<aeron_operation_succeeded_t *>(response_buffer);

        response->correlation_id = async->registration_id;

        if (aeron_broadcast_transmitter_transmit(
            &m_to_clients,
            AERON_RESPONSE_ON_OPERATION_SUCCESS,
            response_buffer,
            sizeof(aeron_operation_succeeded_t)) < 0)
        {
            throw std::runtime_error("error transmitting ON_OPERATION_SUCCESS: " + std::string(aeron_errmsg()));
        }
    }

    void transmitOnError(aeron_async_add_publication_t *async, int32_t errorCode, const std::string &errorMessage)
    {
        char response_buffer[sizeof(aeron_error_response_t) + AERON_ERROR_MAX_TOTAL_LENGTH];
        auto response = reinterpret_cast<aeron_error_response_t *>(response_buffer);

        response->offending_command_correlation_id = async->registration_id;
        response->error_code = errorCode;
        response->error_message_length = static_cast<int32_t>(errorMessage.length());
        memcpy(response_buffer + sizeof(aeron_error_response_t), errorMessage.c_str(), errorMessage.length());

        if (aeron_broadcast_transmitter_transmit(
            &m_to_clients,
            AERON_RESPONSE_ON_ERROR,
            response_buffer,
            sizeof(aeron_error_response_t) + errorMessage.length()) < 0)
        {
            throw std::runtime_error("error transmitting ON_ERROR: " + std::string(aeron_errmsg()));
        }
    }

    void transmitOnSubscriptionReady(aeron_async_add_subscription_t *async)
    {
        char response_buffer[sizeof(aeron_subscription_ready_t)];
        auto response = reinterpret_cast<aeron_subscription_ready_t *>(response_buffer);
        int32_t channel_status_indicator_id = 11;

        response->correlation_id = async->registration_id;
        response->channel_status_indicator_id = channel_status_indicator_id;

        if (aeron_broadcast_transmitter_transmit(
            &m_to_clients,
            AERON_RESPONSE_ON_SUBSCRIPTION_READY,
            response_buffer,
            sizeof(aeron_subscription_ready_t)) < 0)
        {
            throw std::runtime_error("error transmitting ON_SUBSCRIPTION_READY: " + std::string(aeron_errmsg()));
        }
    }

    void transmitOnCounterReady(aeron_async_add_counter_t *async)
    {
        char response_buffer[sizeof(aeron_counter_update_t)];
        auto response = reinterpret_cast<aeron_counter_update_t *>(response_buffer);
        int32_t counter_id = 11;

        response->correlation_id = async->registration_id;
        response->counter_id = counter_id;

        if (aeron_broadcast_transmitter_transmit(
            &m_to_clients,
            AERON_RESPONSE_ON_COUNTER_READY,
            response_buffer,
            sizeof(aeron_counter_update_t)) < 0)
        {
            throw std::runtime_error("error transmitting ON_COUNTER_READY: " + std::string(aeron_errmsg()));
        }
    }

    void transmitOnStaticCounter(aeron_async_add_counter_t *async, int32_t counter_id)
    {
        char response_buffer[sizeof(aeron_static_counter_response_t)];
        auto response = reinterpret_cast<aeron_static_counter_response_t *>(response_buffer);

        response->correlation_id = async->registration_id;
        response->counter_id = counter_id;

        if (aeron_broadcast_transmitter_transmit(
            &m_to_clients,
            AERON_RESPONSE_ON_STATIC_COUNTER,
            response_buffer,
            sizeof(aeron_static_counter_response_t)) < 0)
        {
            throw std::runtime_error("error transmitting ON_STATIC_COUNTER: " + std::string(aeron_errmsg()));
        }
    }

protected:
    aeron_context_t *m_context = nullptr;
    bool m_conductor_isClosed = false;
    aeron_client_conductor_t m_conductor = {};
    aeron_counters_manager_t m_counters_manager = {};
    aeron_clock_cache_t m_cached_clock = {};
    std::unique_ptr<uint8_t[]> m_cnc;
    aeron_mpsc_rb_t m_to_driver = {};
    aeron_broadcast_transmitter_t m_to_clients = {};
    std::string m_logFileName;

    std::function<void(int32_t, const void *, size_t)> m_to_driver_handler;

    std::function<void(const char *, int32_t, int32_t, int64_t)> m_on_new_publication;
    std::function<void(const char *, int32_t, int32_t, int64_t)> m_on_new_exclusive_publication;
    std::function<void(const char *, int32_t, int64_t)> m_on_new_subscription;
};

TEST_F(ClientConductorTest, shouldInitAndClose)
{
    // nothing to do
}

TEST_F(ClientConductorTest, shouldAddPublicationSuccessfully)
{
    aeron_async_add_publication_t *async = nullptr;
    aeron_publication_t *publication = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_publication(&async, &m_conductor, URI_RESERVED, STREAM_ID), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == publication);

    transmitOnPublicationReady(async, m_logFileName, false);
    createLogFile(m_logFileName);
    doWork();

    ASSERT_GT(aeron_async_add_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr != publication);

    ASSERT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldAddPublicationSuccessfullyMaxMessageSize)
{
    aeron_async_add_publication_t *async = nullptr;
    aeron_publication_t *publication = nullptr;
    const size_t uri_length = MAX_MESSAGE_SIZE - sizeof(aeron_publication_command_t);
    std::string uri = allocateStringWithPrefix(URI_RESERVED, "|alias=", 'X', uri_length);

    ASSERT_EQ(aeron_client_conductor_async_add_publication(&async, &m_conductor, uri.c_str(), STREAM_ID), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == publication);

    transmitOnPublicationReady(async, m_logFileName, false);
    createLogFile(m_logFileName);
    doWork();

    ASSERT_GT(aeron_async_add_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr != publication);
    EXPECT_EQ(strcmp(uri.c_str(), publication->channel), 0);

    ASSERT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldErrorOnAddPublicationFromDriverError)
{
    aeron_async_add_publication_t *async = nullptr;
    aeron_publication_t *publication = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_publication(&async, &m_conductor, URI_RESERVED, STREAM_ID), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == publication);

    transmitOnError(async, AERON_ERROR_CODE_INVALID_CHANNEL, "invalid channel");
    doWork();

    ASSERT_EQ(aeron_async_add_publication_poll(&publication, async), -1);
    ASSERT_EQ(-AERON_ERROR_CODE_INVALID_CHANNEL, aeron_errcode());
}

TEST_F(ClientConductorTest, shouldErrorOnAddPublicationFromDriverTimeout)
{
    aeron_async_add_publication_t *async = nullptr;
    aeron_publication_t *publication = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_publication(&async, &m_conductor, URI_RESERVED, STREAM_ID), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == publication);

    doWorkForNs((m_context->driver_timeout_ms + 1000) * 1000000);

    ASSERT_EQ(aeron_async_add_publication_poll(&publication, async), -1);
    ASSERT_EQ(-AERON_CLIENT_ERROR_DRIVER_TIMEOUT, aeron_errcode());
}

TEST_F(ClientConductorTest, shouldAddExclusivePublicationSuccessfully)
{
    aeron_async_add_exclusive_publication_t *async = nullptr;
    aeron_exclusive_publication_t *publication = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_exclusive_publication(&async, &m_conductor, URI_RESERVED, STREAM_ID), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_exclusive_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == publication);

    transmitOnPublicationReady(async, m_logFileName, true);
    createLogFile(m_logFileName);
    doWork();

    ASSERT_GT(aeron_async_add_exclusive_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr != publication);

    ASSERT_EQ(aeron_exclusive_publication_close(publication, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldAddExclusivePublicationSuccessfullyMaxMessageSize)
{
    aeron_async_add_exclusive_publication_t *async = nullptr;
    aeron_exclusive_publication_t *publication = nullptr;
    const size_t uri_length = MAX_MESSAGE_SIZE - sizeof(aeron_publication_command_t);
    std::string uri = allocateStringWithPrefix(URI_RESERVED, "|alias=", 'C', uri_length);

    ASSERT_EQ(aeron_client_conductor_async_add_exclusive_publication(&async, &m_conductor, uri.c_str(), STREAM_ID), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_exclusive_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == publication);

    transmitOnPublicationReady(async, m_logFileName, true);
    createLogFile(m_logFileName);
    doWork();

    ASSERT_GT(aeron_async_add_exclusive_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr != publication);
    EXPECT_EQ(strcmp(uri.c_str(), publication->channel), 0);

    ASSERT_EQ(aeron_exclusive_publication_close(publication, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldErrorOnAddExclusivePublicationFromDriverError)
{
    aeron_async_add_exclusive_publication_t *async = nullptr;
    aeron_exclusive_publication_t *publication = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_exclusive_publication(&async, &m_conductor, URI_RESERVED, STREAM_ID), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_exclusive_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == publication);

    transmitOnError(async, AERON_ERROR_CODE_INVALID_CHANNEL, "invalid channel");
    doWork();

    ASSERT_EQ(aeron_async_add_exclusive_publication_poll(&publication, async), -1);
}

TEST_F(ClientConductorTest, shouldErrorOnAddExclusivePublicationFromDriverTimeout)
{
    aeron_async_add_exclusive_publication_t *async = nullptr;
    aeron_exclusive_publication_t *publication = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_exclusive_publication(&async, &m_conductor, URI_RESERVED, STREAM_ID), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_exclusive_publication_poll(&publication, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == publication);

    doWorkForNs((m_context->driver_timeout_ms + 1000) * 1000000);

    ASSERT_EQ(aeron_async_add_exclusive_publication_poll(&publication, async), -1);
    ASSERT_EQ(-AERON_CLIENT_ERROR_DRIVER_TIMEOUT, aeron_errcode());
}

TEST_F(ClientConductorTest, shouldAddSubscriptionSuccessfully)
{
    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_subscription(
        &async, &m_conductor, SUB_URI, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_subscription_poll(&subscription, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == subscription);

    transmitOnSubscriptionReady(async);
    doWork();

    ASSERT_GT(aeron_async_add_subscription_poll(&subscription, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr != subscription);

    ASSERT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldAddSubscriptionSuccessfullyMaxMessageSize)
{
    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription = nullptr;
    const size_t uri_length = MAX_MESSAGE_SIZE - sizeof(aeron_subscription_command_t);
    std::string uri = allocateStringWithPrefix(SUB_URI, "|alias=", 'Z', uri_length);

    ASSERT_EQ(aeron_client_conductor_async_add_subscription(
        &async, &m_conductor, uri.c_str(), STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_subscription_poll(&subscription, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == subscription);

    transmitOnSubscriptionReady(async);
    doWork();

    ASSERT_GT(aeron_async_add_subscription_poll(&subscription, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr != subscription);
    EXPECT_EQ(strcmp(uri.c_str(), subscription->channel), 0);

    ASSERT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldErrorOnAddSubscriptionFromDriverError)
{
    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_subscription(
        &async, &m_conductor, SUB_URI, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_subscription_poll(&subscription, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == subscription);

    transmitOnError(async, AERON_ERROR_CODE_INVALID_CHANNEL, "invalid channel");
    doWork();

    ASSERT_EQ(aeron_async_add_subscription_poll(&subscription, async), -1);
}

TEST_F(ClientConductorTest, shouldErrorOnAddSubscriptionFromDriverTimeout)
{
    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_subscription(
        &async, &m_conductor, SUB_URI, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_subscription_poll(&subscription, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == subscription);

    doWorkForNs((m_context->driver_timeout_ms + 1000) * 1000000LL);

    ASSERT_EQ(aeron_async_add_subscription_poll(&subscription, async), -1);
    ASSERT_EQ(-AERON_CLIENT_ERROR_DRIVER_TIMEOUT, aeron_errcode());
}

TEST_F(ClientConductorTest, shouldAddCounterSuccessfully)
{
    aeron_async_add_counter_t *async = nullptr;
    aeron_counter_t *counter = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_counter(
        &async, &m_conductor, COUNTER_TYPE_ID, nullptr, 0, nullptr, 0), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_counter_poll(&counter, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == counter);

    transmitOnCounterReady(async);
    doWork();

    ASSERT_GT(aeron_async_add_counter_poll(&counter, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr != counter);

    ASSERT_EQ(aeron_counter_close(counter, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldAddCounterSuccessfullyMaxMessageSize)
{
    aeron_async_add_counter_t *async = nullptr;
    aeron_counter_t *counter = nullptr;
    const size_t key_buffer_length = 256;
    auto *key_buffer = new uint8_t[key_buffer_length];
    key_buffer[5] = 13;
    const size_t label_buffer_length =
        MAX_MESSAGE_SIZE - key_buffer_length - 2 * sizeof(int32_t) - sizeof(aeron_counter_command_t);
    std::string label_buffer = allocateStringWithPrefix("label", "=", 'x', label_buffer_length);
    ASSERT_EQ(label_buffer_length, label_buffer.length());

    ASSERT_EQ(aeron_client_conductor_async_add_counter(
        &async, &m_conductor, COUNTER_TYPE_ID, key_buffer, key_buffer_length, label_buffer.c_str(), label_buffer_length), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_counter_poll(&counter, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == counter);

    transmitOnCounterReady(async);
    doWork();

    ASSERT_GT(aeron_async_add_counter_poll(&counter, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr != counter);

    delete[] key_buffer;

    ASSERT_EQ(aeron_counter_close(counter, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldErrorOnAddCounterFromDriverError)
{
    aeron_async_add_counter_t *async = nullptr;
    aeron_counter_t *counter = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_counter(
        &async, &m_conductor, COUNTER_TYPE_ID, nullptr, 0, nullptr, 0), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_counter_poll(&counter, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == counter);

    transmitOnError(async, AERON_ERROR_CODE_GENERIC_ERROR, "can not add counter");
    doWork();

    ASSERT_EQ(aeron_async_add_counter_poll(&counter, async), -1);
}

TEST_F(ClientConductorTest, shouldErrorOnAddCounterFromDriverTimeout)
{
    aeron_async_add_counter_t *async = nullptr;
    aeron_counter_t *counter = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_counter(
        &async, &m_conductor, COUNTER_TYPE_ID, nullptr, 0, nullptr, 0), 0);
    doWork();

    ASSERT_EQ(aeron_async_add_counter_poll(&counter, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == counter);

    doWorkForNs((m_context->driver_timeout_ms + 1000) * 1000000LL);

    ASSERT_EQ(aeron_async_add_counter_poll(&counter, async), -1);
    ASSERT_EQ(-AERON_CLIENT_ERROR_DRIVER_TIMEOUT, aeron_errcode());
}

TEST_F(ClientConductorTest, shouldAddPublicationAndHandleOnNewPublication)
{
    aeron_async_add_publication_t *async = nullptr;
    aeron_publication_t *publication = nullptr;
    bool was_on_new_publication_called = false;

    ASSERT_EQ(aeron_client_conductor_async_add_publication(&async, &m_conductor, URI_RESERVED, STREAM_ID), 0);
    const int64_t registration_id = aeron_async_add_publication_get_registration_id(async);;
    doWork();

    m_on_new_publication = [&](
        const char *channel,
        int32_t stream_id,
        int32_t session_id,
        int64_t correlation_id)
    {
        EXPECT_EQ(strcmp(channel, URI_RESERVED), 0);
        EXPECT_EQ(stream_id, STREAM_ID);
        EXPECT_EQ(session_id, SESSION_ID);
        EXPECT_EQ(correlation_id, registration_id);

        was_on_new_publication_called = true;
    };

    transmitOnPublicationReady(async, m_logFileName, false);
    createLogFile(m_logFileName);
    doWork();

    EXPECT_TRUE(was_on_new_publication_called);

    EXPECT_GT(aeron_async_add_publication_poll(&publication, async), 0) << aeron_errmsg();
    EXPECT_NE(nullptr, publication);

    // graceful close and reclaim for sanitize
    ASSERT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldAddExclusivePublicationAndHandleOnNewPublication)
{
    aeron_async_add_exclusive_publication_t *async = nullptr;
    aeron_exclusive_publication_t *publication = nullptr;
    bool was_on_new_exclusive_publication_called = false;

    ASSERT_EQ(aeron_client_conductor_async_add_exclusive_publication(&async, &m_conductor, URI_RESERVED, STREAM_ID), 0);
    const int64_t registrationId = aeron_async_add_exclusive_publication_get_registration_id(async);
    doWork();

    m_on_new_exclusive_publication = [&](
        const char *channel,
        int32_t stream_id,
        int32_t session_id,
        int64_t correlation_id)
    {
        EXPECT_EQ(strcmp(channel, URI_RESERVED), 0);
        EXPECT_EQ(stream_id, STREAM_ID);
        EXPECT_EQ(session_id, SESSION_ID);
        EXPECT_EQ(correlation_id, registrationId);

        was_on_new_exclusive_publication_called = true;
    };

    transmitOnPublicationReady(async, m_logFileName, false);
    createLogFile(m_logFileName);
    doWork();

    EXPECT_TRUE(was_on_new_exclusive_publication_called);

    EXPECT_GT(aeron_async_add_exclusive_publication_poll(&publication, async), 0) << aeron_errmsg();
    EXPECT_NE(nullptr, publication);

    // graceful close and reclaim for sanitize
    ASSERT_EQ(aeron_exclusive_publication_close(publication, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldAddSubscriptionAndHandleOnNewSubscription)
{
    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription = nullptr;
    bool was_on_new_subscription_called = false;

    ASSERT_EQ(aeron_client_conductor_async_add_subscription(
        &async, &m_conductor, SUB_URI, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    const int64_t registrationId = aeron_async_add_subscription_get_registration_id(async);
    doWork();

    m_on_new_subscription = [&](
        const char *channel,
        int32_t stream_id,
        int64_t correlation_id)
    {
        EXPECT_EQ(strcmp(channel, SUB_URI), 0);
        EXPECT_EQ(stream_id, STREAM_ID);
        EXPECT_EQ(correlation_id, registrationId);

        was_on_new_subscription_called = true;
    };

    transmitOnSubscriptionReady(async);
    doWork();

    EXPECT_TRUE(was_on_new_subscription_called);

    EXPECT_GT(aeron_async_add_subscription_poll(&subscription, async), 0) << aeron_errmsg();
    EXPECT_NE(nullptr, subscription);

    // graceful close and reclaim for sanitize
    ASSERT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldHandlePublicationAddRemoveDestination)
{
    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_destination_t *async_add_dest = nullptr;
    aeron_async_destination_t *async_remove_dest = nullptr;
    aeron_publication_t *publication = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_publication(&async_pub, &m_conductor, URI_RESERVED, STREAM_ID), 0);
    doWork();

    transmitOnPublicationReady(async_pub, m_logFileName, false);
    createLogFile(m_logFileName);
    doWork();
    ASSERT_GT(aeron_async_add_publication_poll(&publication, async_pub), 0) << aeron_errmsg();

    ASSERT_EQ(
        aeron_client_conductor_async_add_publication_destination(&async_add_dest, &m_conductor, publication, DEST_URI),
        0);

    transmitOnOperationSuccess(async_add_dest);
    doWork();
    ASSERT_EQ(async_add_dest->registration_status, AERON_CLIENT_REGISTRATION_STATUS_REGISTERED);
    ASSERT_EQ(async_add_dest->resource.publication->registration_id, publication->registration_id);
    ASSERT_GT(aeron_publication_async_destination_poll(async_add_dest), 0) << aeron_errmsg();

    ASSERT_EQ(
        aeron_client_conductor_async_remove_publication_destination(
            &async_remove_dest, &m_conductor, publication, DEST_URI),
        0);
    transmitOnOperationSuccess(async_remove_dest);
    doWork();
    ASSERT_GT(aeron_publication_async_destination_poll(async_remove_dest), 0) << aeron_errmsg();

    // graceful close and reclaim for sanitize
    ASSERT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldHandlePublicationAddRemoveDestinationMaxMessageSize)
{
    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_destination_t *async_add_dest = nullptr;
    aeron_async_destination_t *async_remove_dest = nullptr;
    aeron_publication_t *publication = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_publication(&async_pub, &m_conductor, URI_RESERVED, STREAM_ID), 0);
    doWork();

    transmitOnPublicationReady(async_pub, m_logFileName, false);
    createLogFile(m_logFileName);
    doWork();
    ASSERT_GT(aeron_async_add_publication_poll(&publication, async_pub), 0) << aeron_errmsg();

    const size_t uri_length = MAX_MESSAGE_SIZE - sizeof(aeron_destination_command_t);
    std::string destUri = allocateStringWithPrefix(DEST_URI, "|alias=", 'a', uri_length);
    ASSERT_EQ(
        aeron_client_conductor_async_add_publication_destination(&async_add_dest, &m_conductor, publication, destUri.c_str()),
        0);

    transmitOnOperationSuccess(async_add_dest);
    doWork();
    ASSERT_EQ(async_add_dest->registration_status, AERON_CLIENT_REGISTRATION_STATUS_REGISTERED);
    ASSERT_EQ(async_add_dest->resource.publication->registration_id, publication->registration_id);
    ASSERT_GT(aeron_publication_async_destination_poll(async_add_dest), 0) << aeron_errmsg();

    ASSERT_EQ(
        aeron_client_conductor_async_remove_publication_destination(
            &async_remove_dest, &m_conductor, publication, destUri.c_str()),
        0);
    transmitOnOperationSuccess(async_remove_dest);

    doWork();
    ASSERT_GT(aeron_publication_async_destination_poll(async_remove_dest), 0) << aeron_errmsg();

    // graceful close and reclaim for sanitize
    ASSERT_EQ(aeron_publication_close(publication, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldHandleExclusivePublicationAddDestination)
{
    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_destination_t *async_dest = nullptr;
    aeron_exclusive_publication_t *publication = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_exclusive_publication(&async_pub, &m_conductor, URI_RESERVED, STREAM_ID), 0);
    doWork();

    transmitOnPublicationReady(async_pub, m_logFileName, false);
    createLogFile(m_logFileName);
    doWork();
    ASSERT_GT(aeron_async_add_exclusive_publication_poll(&publication, async_pub), 0) << aeron_errmsg();

    ASSERT_EQ(
        aeron_client_conductor_async_add_exclusive_publication_destination(
            &async_dest, &m_conductor, publication, DEST_URI),
        0);

    transmitOnOperationSuccess(async_dest);
    doWork();
    ASSERT_EQ(async_dest->registration_status, AERON_CLIENT_REGISTRATION_STATUS_REGISTERED);
    ASSERT_EQ(async_dest->resource.publication->registration_id, publication->registration_id);
    ASSERT_GT(aeron_exclusive_publication_async_destination_poll(async_dest), 0) << aeron_errmsg();

    ASSERT_EQ(
        aeron_client_conductor_async_remove_exclusive_publication_destination(
            &async_dest, &m_conductor, publication, DEST_URI),
        0);
    transmitOnOperationSuccess(async_dest);
    doWork();
    ASSERT_GT(aeron_exclusive_publication_async_destination_poll(async_dest), 0) << aeron_errmsg();

    // graceful close and reclaim for sanitize
    ASSERT_EQ(aeron_exclusive_publication_close(publication, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldHandleExclusivePublicationAddDestinationMaxMessageSize)
{
    aeron_async_add_publication_t *async_pub = nullptr;
    aeron_async_destination_t *async_dest = nullptr;
    aeron_exclusive_publication_t *publication = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_exclusive_publication(&async_pub, &m_conductor, URI_RESERVED, STREAM_ID), 0);
    doWork();

    transmitOnPublicationReady(async_pub, m_logFileName, false);
    createLogFile(m_logFileName);
    doWork();
    ASSERT_GT(aeron_async_add_exclusive_publication_poll(&publication, async_pub), 0) << aeron_errmsg();

    const size_t uri_length = MAX_MESSAGE_SIZE - sizeof(aeron_destination_command_t);
    std::string destUri = allocateStringWithPrefix(DEST_URI, "|alias=", 'a', uri_length);
    ASSERT_EQ(
        aeron_client_conductor_async_add_exclusive_publication_destination(
            &async_dest, &m_conductor, publication, destUri.c_str()),
        0);

    transmitOnOperationSuccess(async_dest);
    doWork();
    ASSERT_EQ(async_dest->registration_status, AERON_CLIENT_REGISTRATION_STATUS_REGISTERED);
    ASSERT_EQ(async_dest->resource.publication->registration_id, publication->registration_id);
    ASSERT_GT(aeron_exclusive_publication_async_destination_poll(async_dest), 0) << aeron_errmsg();

    ASSERT_EQ(
        aeron_client_conductor_async_remove_exclusive_publication_destination(
            &async_dest, &m_conductor, publication, destUri.c_str()),
        0);

    transmitOnOperationSuccess(async_dest);
    doWork();
    ASSERT_GT(aeron_exclusive_publication_async_destination_poll(async_dest), 0) << aeron_errmsg();

    // graceful close and reclaim for sanitize
    ASSERT_EQ(aeron_exclusive_publication_close(publication, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldHandleSubscriptionAddDestination)
{
    aeron_async_add_subscription_t *async_sub = nullptr;
    aeron_async_destination_t *async_dest = nullptr;
    aeron_subscription_t *subscription = nullptr;

    ASSERT_EQ(
        aeron_client_conductor_async_add_subscription(
            &async_sub, &m_conductor, URI_RESERVED, STREAM_ID, nullptr, nullptr, nullptr, nullptr),
        0);
    doWork();

    transmitOnSubscriptionReady(async_sub);
    createLogFile(m_logFileName);
    doWork();
    ASSERT_GT(aeron_async_add_subscription_poll(&subscription, async_sub), 0) << aeron_errmsg();

    ASSERT_EQ(
        aeron_client_conductor_async_add_subscription_destination(
            &async_dest, &m_conductor, subscription, DEST_URI),
        0);

    transmitOnOperationSuccess(async_dest);
    doWork();
    ASSERT_EQ(async_dest->registration_status, AERON_CLIENT_REGISTRATION_STATUS_REGISTERED);
    ASSERT_EQ(async_dest->resource.subscription->registration_id, subscription->registration_id);
    ASSERT_GT(aeron_subscription_async_destination_poll(async_dest), 0) << aeron_errmsg();

    ASSERT_EQ(
        aeron_client_conductor_async_remove_subscription_destination(&async_dest, &m_conductor, subscription, DEST_URI),
        0);
    transmitOnOperationSuccess(async_dest);
    doWork();
    ASSERT_GT(aeron_subscription_async_destination_poll(async_dest), 0) << aeron_errmsg();

    // graceful close and reclaim for sanitize
    ASSERT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldHandleSubscriptionAddDestinationMaxMessageSize)
{
    aeron_async_add_subscription_t *async_sub = nullptr;
    aeron_async_destination_t *async_dest = nullptr;
    aeron_subscription_t *subscription = nullptr;

    ASSERT_EQ(
        aeron_client_conductor_async_add_subscription(
            &async_sub, &m_conductor, URI_RESERVED, STREAM_ID, nullptr, nullptr, nullptr, nullptr),
        0);
    doWork();

    transmitOnSubscriptionReady(async_sub);
    createLogFile(m_logFileName);
    doWork();
    ASSERT_GT(aeron_async_add_subscription_poll(&subscription, async_sub), 0) << aeron_errmsg();

    const size_t uri_length = MAX_MESSAGE_SIZE - sizeof(aeron_destination_command_t);
    std::string destUri = allocateStringWithPrefix(DEST_URI, "|alias=", 'a', uri_length);
    ASSERT_EQ(
        aeron_client_conductor_async_add_subscription_destination(
            &async_dest, &m_conductor, subscription, destUri.c_str()),
        0);

    transmitOnOperationSuccess(async_dest);
    doWork();
    ASSERT_EQ(async_dest->registration_status, AERON_CLIENT_REGISTRATION_STATUS_REGISTERED);
    ASSERT_EQ(async_dest->resource.subscription->registration_id, subscription->registration_id);
    ASSERT_GT(aeron_subscription_async_destination_poll(async_dest), 0) << aeron_errmsg();

    ASSERT_EQ(
        aeron_client_conductor_async_remove_subscription_destination(&async_dest, &m_conductor, subscription, destUri.c_str()),
        0);
    transmitOnOperationSuccess(async_dest);

    doWork();
    ASSERT_GT(aeron_subscription_async_destination_poll(async_dest), 0) << aeron_errmsg();

    // graceful close and reclaim for sanitize
    ASSERT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldSetCloseFlagOnTimeout)
{
    int errorcode = 0;
    m_conductor.error_handler_clientd = &errorcode;
    m_conductor.error_handler = save_last_errorcode;

    aeron_client_timeout_t timeout;
    timeout.client_id = m_conductor.client_id;

    aeron_client_conductor_on_client_timeout(&m_conductor, &timeout);

    ASSERT_TRUE(aeron_client_conductor_is_closed(&m_conductor));
    ASSERT_EQ(AERON_CLIENT_ERROR_CLIENT_TIMEOUT, errorcode);
}

TEST_F(ClientConductorTest, shouldCreateServiceIntervalTimeoutError)
{
    int errorcode = 0;
    m_conductor.error_handler_clientd = &errorcode;
    m_conductor.error_handler = save_last_errorcode;

    doWork();
    doWorkForNs(CLIENT_LIVENESS_TIMEOUT + 1, false, CLIENT_LIVENESS_TIMEOUT + 1);

    ASSERT_EQ(AERON_CLIENT_ERROR_CONDUCTOR_SERVICE_TIMEOUT, errorcode);
}

TEST_F(ClientConductorTest, shouldCreateDriverTimeoutError)
{
    int errorcode = 0;
    m_conductor.error_handler_clientd = &errorcode;
    m_conductor.error_handler = save_last_errorcode;

    doWork();
    doWorkForNs(DRIVER_TIMEOUT_INTERVAL_NS * 2, false, DRIVER_TIMEOUT_INTERVAL_NS * 2);

    ASSERT_EQ(AERON_CLIENT_ERROR_DRIVER_TIMEOUT, errorcode);
}

TEST_F(ClientConductorTest, shouldAddClientInfoToTheHeartbeatCounterLabel)
{
    m_conductor.client_id = 0x60001da400;
    m_conductor.client_name = "test client name";
    aeron_heartbeat_timestamp_key_layout_t key = {};
    key.registration_id = m_conductor.client_id;
    std::string label = std::string("test 42 and beyond");

    int32_t counter_id = aeron_counters_manager_allocate(
        &m_counters_manager,
        AERON_COUNTER_CLIENT_HEARTBEAT_TIMESTAMP_TYPE_ID,
        (const uint8_t *)&key,
        sizeof(key),
        label.c_str(),
        label.length());
    ASSERT_GT(counter_id, -1);

    char counterLabel[100];
    memset(counterLabel, '\0', sizeof(counterLabel));
    ASSERT_EQ(label.length(), aeron_counters_reader_counter_label(
        &m_conductor.counters_reader, counter_id, counterLabel, sizeof(counterLabel)));

    ASSERT_STREQ(label.c_str(), counterLabel);

    doWorkForNs((int64_t)m_context->keepalive_interval_ns * 2, true);

    ASSERT_GT(aeron_counters_reader_counter_label(
        &m_conductor.counters_reader, counter_id, counterLabel, sizeof(counterLabel)), label.length());
    ASSERT_EQ(
        label + " name=test client name version=" + aeron_version_text() + " commit=" + aeron_version_gitsha(),
        std::string(counterLabel));
}

TEST_F(ClientConductorTest, shouldAddClientInfoToTheHeartbeatCounterLabelUpToMaxLength)
{
    m_conductor.client_id = -4373543;
    auto clientName = std::string("test").append(1000, 'p');
    m_conductor.client_name = clientName.c_str();
    aeron_heartbeat_timestamp_key_layout_t key = {};
    key.registration_id = m_conductor.client_id;
    std::string label = std::string("abc");

    int32_t counter_id = aeron_counters_manager_allocate(
        &m_counters_manager,
        AERON_COUNTER_CLIENT_HEARTBEAT_TIMESTAMP_TYPE_ID,
        (const uint8_t *)&key,
        sizeof(key),
        label.c_str(),
        label.length());
    ASSERT_GT(counter_id, -1);

    char counterLabel[AERON_COUNTER_MAX_LABEL_LENGTH];
    memset(counterLabel, '\0', sizeof(counterLabel));
    ASSERT_EQ(label.length(), aeron_counters_reader_counter_label(
        &m_conductor.counters_reader, counter_id, counterLabel, sizeof(counterLabel)));

    ASSERT_STREQ(label.c_str(), counterLabel);

    doWorkForNs((int64_t)m_context->keepalive_interval_ns * 2, true);

    ASSERT_GT(aeron_counters_reader_counter_label(
        &m_conductor.counters_reader, counter_id, counterLabel, sizeof(counterLabel)), label.length());
    ASSERT_EQ(
        label + std::string(" name=test").append(366, 'p'),
        std::string(counterLabel));
}

TEST_F(ClientConductorTest, shouldAddStaticCounterSuccessfully)
{
    aeron_async_add_counter_t *async = nullptr;
    aeron_counter_t *counter = nullptr;

    const char *label = "first static counter from C";
    const size_t label_length = strlen(label);
    const int registration_id = 508;
    ASSERT_EQ(aeron_client_conductor_async_add_static_counter(
        &async, &m_conductor, COUNTER_TYPE_ID, nullptr, 0, label, label_length, registration_id), 0);
    ASSERT_EQ(registration_id, async->counter.registration_id);
    ASSERT_EQ(label_length, async->counter.label_buffer_length);
    doWork();

    ASSERT_EQ(aeron_async_add_counter_poll(&counter, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr == counter);

    const int32_t counter_id = 777;
    transmitOnStaticCounter(async, counter_id);
    doWork();

    ASSERT_GT(aeron_async_add_counter_poll(&counter, async), 0) << aeron_errmsg();
    ASSERT_TRUE(nullptr != counter);
    ASSERT_EQ(registration_id, counter->registration_id);
    ASSERT_EQ(counter_id, counter->counter_id);

    ASSERT_EQ(aeron_counter_close(counter, nullptr, nullptr), 0);
    doWork();
}

TEST_F(ClientConductorTest, shouldReturnRandomSessionIdIfControlProtolVersionNotSatisfied)
{
    aeron_async_get_next_available_session_id_t *async = nullptr;

    m_conductor.control_protocol_version = 1;

    const int32_t streamId = 43;
    ASSERT_EQ(aeron_client_conductor_async_get_next_available_session_id(&async, &m_conductor, streamId), 0);
    ASSERT_EQ(streamId, async->stream_id);
    ASSERT_EQ(AERON_CLIENT_REGISTRATION_STATUS_REGISTERED, async->registration_status);
    int32_t nextSessionId = async->resource.next_session_id;

    int32_t sessionId;
    ASSERT_EQ(aeron_async_next_session_id_poll(&sessionId, async), 1) << aeron_errmsg();
    ASSERT_EQ(nextSessionId, sessionId);

    ASSERT_EQ(aeron_client_conductor_async_get_next_available_session_id(&async, &m_conductor, streamId), 0);
    ASSERT_EQ(streamId, async->stream_id);
    ASSERT_EQ(AERON_CLIENT_REGISTRATION_STATUS_REGISTERED, async->registration_status);
    int32_t nextSessionId2 = async->resource.next_session_id;
    ASSERT_EQ(aeron_async_next_session_id_poll(&sessionId, async), 1) << aeron_errmsg();

    ASSERT_NE(nextSessionId, nextSessionId2);
}

TEST_F(ClientConductorTest, asyncPublicationResourceMustBeExplicitlyFreedIfPollAbandonedBeforeCompletion)
{
    aeron_async_add_publication_t *async = nullptr;
    aeron_publication_t *publication = nullptr;

    EXPECT_EQ(aeron_client_conductor_async_add_publication(&async, &m_conductor, URI_RESERVED, STREAM_ID), 0);
    doWork();

    EXPECT_EQ(aeron_async_add_publication_poll(&publication, async), 0) << aeron_errmsg();
    EXPECT_EQ(nullptr, publication);

    aeron_async_cmd_free(async);
}

TEST_F(ClientConductorTest, asyncExclusivePublicationResourceMustBeExplicitlyFreedIfPollAbandonedBeforeCompletion)
{
    aeron_async_add_exclusive_publication_t *async = nullptr;
    aeron_exclusive_publication_t *publication = nullptr;

    EXPECT_EQ(aeron_client_conductor_async_add_exclusive_publication(&async, &m_conductor, URI_RESERVED, STREAM_ID), 0);
    doWork();

    EXPECT_EQ(aeron_async_add_exclusive_publication_poll(&publication, async), 0) << aeron_errmsg();
    EXPECT_EQ(nullptr, publication);

    aeron_async_cmd_free(async);
}

TEST_F(ClientConductorTest, asyncSubscriptionResourceMustBeExplicitlyFreedIfPollAbandonedBeforeCompletion)
{
    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription = nullptr;

    EXPECT_EQ(aeron_client_conductor_async_add_subscription(&async, &m_conductor, URI_RESERVED, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    doWork();

    EXPECT_EQ(aeron_async_add_subscription_poll(&subscription, async), 0) << aeron_errmsg();
    EXPECT_EQ(nullptr, subscription);

    aeron_async_cmd_free(async);
}

TEST_F(ClientConductorTest, asyncCounterResourceMustBeExplicitlyFreedIfPollAbandonedBeforeCompletion)
{
    aeron_async_add_counter_t *async = nullptr;
    aeron_counter_t *counter = nullptr;

    EXPECT_EQ(aeron_client_conductor_async_add_counter(&async, &m_conductor, 1000, nullptr, 0, "test", strlen("test")), 0);
    doWork();

    EXPECT_EQ(aeron_async_add_counter_poll(&counter, async), 0) << aeron_errmsg();
    EXPECT_EQ(nullptr, counter);

    aeron_async_cmd_free(async);
}

TEST_F(ClientConductorTest, shouldSetIdleStartegy)
{
    auto func = m_context->idle_strategy_func;
    EXPECT_STREQ("sleep-ns", m_context->idle_strategy_name);
    EXPECT_STREQ("16000000", m_context->idle_strategy_init_args);
    EXPECT_NE(nullptr, m_context->idle_strategy_state);
    EXPECT_NE(nullptr, func);

    EXPECT_EQ(-1, aeron_context_set_idle_strategy(m_context, "unknown value"));
    EXPECT_EQ(nullptr, m_context->idle_strategy_name);
    EXPECT_EQ(nullptr, m_context->idle_strategy_state);
    EXPECT_EQ(nullptr, m_context->idle_strategy_func);
    EXPECT_STREQ("16000000", m_context->idle_strategy_init_args);

    EXPECT_EQ(0, aeron_context_set_idle_strategy_init_args(m_context, nullptr));
    EXPECT_EQ(nullptr, aeron_context_get_idle_strategy_init_args(m_context));

    EXPECT_EQ(0, aeron_context_set_idle_strategy_init_args(m_context, "200-50-1us-2500ns"));
    EXPECT_STREQ("200-50-1us-2500ns", aeron_context_get_idle_strategy_init_args(m_context));

    EXPECT_EQ(0, aeron_context_set_idle_strategy(m_context, "backoff"));
    EXPECT_STREQ("backoff", m_context->idle_strategy_name);
    EXPECT_NE(nullptr, m_context->idle_strategy_state);
    EXPECT_NE(func, m_context->idle_strategy_func);
    EXPECT_STREQ("200-50-1us-2500ns", m_context->idle_strategy_init_args);
}

TEST_F(ClientConductorTest, shouldRejectCloseHandlerIfClosed)
{
    m_conductor.is_closed = true;
    aeron_t aeron = {};
    aeron.conductor = m_conductor;
    aeron_on_close_client_pair_t pair = {};

    EXPECT_EQ(-1, aeron_add_close_handler(&aeron, &pair));
    EXPECT_EQ(EPERM, aeron_errcode());
}

TEST_F(ClientConductorTest, shouldRejectRemoveCloseHandlerIfClosed)
{
    m_conductor.is_closed = true;
    aeron_t aeron = {};
    aeron.conductor = m_conductor;
    aeron_on_close_client_pair_t pair = {};

    EXPECT_EQ(-1, aeron_remove_close_handler(&aeron, &pair));
    EXPECT_EQ(EPERM, aeron_errcode());
}

TEST_F(ClientConductorTest, shouldRejectAddAvailableCounterHandlerIfClosed)
{
    m_conductor.is_closed = true;
    aeron_t aeron = {};
    aeron.conductor = m_conductor;
    aeron_on_available_counter_pair_t pair = {};

    EXPECT_EQ(-1, aeron_add_available_counter_handler(&aeron, &pair));
    EXPECT_EQ(EPERM, aeron_errcode());
}

TEST_F(ClientConductorTest, shouldRejectRemoveAvailableCounterHandlerIfClosed)
{
    m_conductor.is_closed = true;
    aeron_t aeron = {};
    aeron.conductor = m_conductor;
    aeron_on_available_counter_pair_t pair = {};

    EXPECT_EQ(-1, aeron_remove_available_counter_handler(&aeron, &pair));
    EXPECT_EQ(EPERM, aeron_errcode());
}

TEST_F(ClientConductorTest, shouldRejectAddUnavailableCounterHandlerIfClosed)
{
    m_conductor.is_closed = true;
    aeron_t aeron = {};
    aeron.conductor = m_conductor;
    aeron_on_unavailable_counter_pair_t pair = {};

    EXPECT_EQ(-1, aeron_add_unavailable_counter_handler(&aeron, &pair));
    EXPECT_EQ(EPERM, aeron_errcode());
}

TEST_F(ClientConductorTest, shouldRejectRemoveUnavailableCounterHandlerIfClosed)
{
    m_conductor.is_closed = true;
    aeron_t aeron = {};
    aeron.conductor = m_conductor;
    aeron_on_unavailable_counter_pair_t pair = {};

    EXPECT_EQ(-1, aeron_remove_unavailable_counter_handler(&aeron, &pair));
    EXPECT_EQ(EPERM, aeron_errcode());
}

class ClientConductorIsLengthSufficientTest : public testing::TestWithParam<std::tuple<aeron_mapped_file_t*, bool>>
{
};

// cnc_version, to_driver, to_clients, counter_metadata, counter_values, error_log, client_liveness_timeout, start_timestamp, pid, file_page_size
static aeron_cnc_metadata_t s_metadata[] =
{
    { 0, (int32_t)TO_DRIVER_RING_BUFFER_LENGTH, 0, 0, 0, 0, 0, 0, 0, 0 },
    { 0, (int32_t)TO_DRIVER_RING_BUFFER_LENGTH, (int32_t)TO_CLIENTS_BUFFER_LENGTH, 0, 0, 0, 0, 0, 0, 0 },
    { 0, (int32_t)TO_DRIVER_RING_BUFFER_LENGTH, (int32_t)TO_CLIENTS_BUFFER_LENGTH, (int32_t)COUNTER_METADATA_BUFFER_LENGTH, 0, 0, 0, 0, 0, 0 },
    { 0, (int32_t)TO_DRIVER_RING_BUFFER_LENGTH, (int32_t)TO_CLIENTS_BUFFER_LENGTH, (int32_t)COUNTER_METADATA_BUFFER_LENGTH, (int32_t)COUNTER_VALUES_BUFFER_LENGTH, 0, 0, 0, 0, 0 },
    { 0, (int32_t)TO_DRIVER_RING_BUFFER_LENGTH, (int32_t)TO_CLIENTS_BUFFER_LENGTH, (int32_t)COUNTER_METADATA_BUFFER_LENGTH, (int32_t)COUNTER_VALUES_BUFFER_LENGTH, (int32_t)ERROR_BUFFER_LENGTH, 0, 0, 0, 0 },
    { 0, (int32_t)TO_DRIVER_RING_BUFFER_LENGTH, (int32_t)TO_CLIENTS_BUFFER_LENGTH, (int32_t)COUNTER_METADATA_BUFFER_LENGTH, (int32_t)COUNTER_VALUES_BUFFER_LENGTH, (int32_t)ERROR_BUFFER_LENGTH, 0, 0, 0, 0 },
    { 0, (int32_t)TO_DRIVER_RING_BUFFER_LENGTH, (int32_t)TO_CLIENTS_BUFFER_LENGTH, (int32_t)COUNTER_METADATA_BUFFER_LENGTH, (int32_t)COUNTER_VALUES_BUFFER_LENGTH, (int32_t)ERROR_BUFFER_LENGTH, 0, 0, 0, 0 },
};

static aeron_mapped_file_t s_mapped_files[] =
{
    { nullptr, 0 },
    { nullptr, AERON_CNC_VERSION_AND_META_DATA_LENGTH - 1 },
    { &s_metadata[0], AERON_CNC_VERSION_AND_META_DATA_LENGTH },
    { &s_metadata[1], AERON_CNC_VERSION_AND_META_DATA_LENGTH + TO_DRIVER_RING_BUFFER_LENGTH },
    { &s_metadata[2], AERON_CNC_VERSION_AND_META_DATA_LENGTH + TO_DRIVER_RING_BUFFER_LENGTH + TO_CLIENTS_BUFFER_LENGTH },
    { &s_metadata[3], AERON_CNC_VERSION_AND_META_DATA_LENGTH + TO_DRIVER_RING_BUFFER_LENGTH + TO_CLIENTS_BUFFER_LENGTH + COUNTER_METADATA_BUFFER_LENGTH },
    { &s_metadata[4], AERON_CNC_VERSION_AND_META_DATA_LENGTH + TO_DRIVER_RING_BUFFER_LENGTH + TO_CLIENTS_BUFFER_LENGTH + COUNTER_METADATA_BUFFER_LENGTH + COUNTER_VALUES_BUFFER_LENGTH },
    { &s_metadata[5], AERON_CNC_VERSION_AND_META_DATA_LENGTH + TO_DRIVER_RING_BUFFER_LENGTH + TO_CLIENTS_BUFFER_LENGTH + COUNTER_METADATA_BUFFER_LENGTH + COUNTER_VALUES_BUFFER_LENGTH + ERROR_BUFFER_LENGTH },
    { &s_metadata[6], INT64_MAX },
};

INSTANTIATE_TEST_SUITE_P(
    ClientConductorIsLengthSufficientTest,
    ClientConductorIsLengthSufficientTest,
    testing::Values(
        std::make_tuple(&s_mapped_files[0], false),
        std::make_tuple(&s_mapped_files[1], false),
        std::make_tuple(&s_mapped_files[2], false),
        std::make_tuple(&s_mapped_files[3], false),
        std::make_tuple(&s_mapped_files[4], false),
        std::make_tuple(&s_mapped_files[5], false),
        std::make_tuple(&s_mapped_files[6], false),
        std::make_tuple(&s_mapped_files[7], true),
        std::make_tuple(&s_mapped_files[8], true)
    ));

TEST_P(ClientConductorIsLengthSufficientTest, shouldCheckIfLengthIsSufficient)
{
    const auto mappedFile = std::get<0>(GetParam());
    const bool expected = std::get<1>(GetParam());
    ASSERT_EQ(expected, aeron_cnc_is_file_length_sufficient(mappedFile));
}

TEST_F(ClientConductorTest, shouldAsyncCloseSubscription)
{
    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription = nullptr;
    m_conductor.invoker_mode = false;

    const char *uri = "aeron:ipc?term-length=64k";
    int32_t stream_id = 1000;
    EXPECT_EQ(aeron_client_conductor_async_add_subscription(
        &async, &m_conductor, uri, stream_id, nullptr, nullptr, nullptr, nullptr), 0);
    doWork();

    transmitOnSubscriptionReady(async);
    doWork();

    const int64_t registration_id = aeron_async_add_subscription_get_registration_id(async);
    EXPECT_EQ(aeron_async_add_subscription_poll(&subscription, async), 1) << aeron_errmsg();
    EXPECT_NE(nullptr, subscription);

    std::atomic<bool> on_close_called(false);
    auto on_close_complete = [](void* clientd)
    {
        auto flag = static_cast<std::atomic<bool>*>(clientd);
        flag->store(true);
    };

    EXPECT_EQ(
        0,
        aeron_client_conductor_async_close_subscription(&m_conductor, subscription, on_close_complete, &on_close_called));

    EXPECT_EQ(subscription, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));
    EXPECT_FALSE(subscription->pending_close_action);

    doWork();

    EXPECT_TRUE(on_close_called);
    EXPECT_EQ(nullptr, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));
}

TEST_F(ClientConductorTest, shouldAsyncCloseSubscriptionIfClientBufferIsFull)
{
    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription = nullptr;
    m_conductor.invoker_mode = false;
    m_conductor.control_protocol_version = aeron_semantic_version_compose(
        AERON_CONTROL_PROTOCOL_MAJOR_VERSION, AERON_CONTROL_PROTOCOL_MINOR_VERSION, AERON_CONTROL_PROTOCOL_PATCH_VERSION);

    const char *uri = "aeron:ipc?term-length=64k";
    int32_t stream_id = 1000;

    EXPECT_EQ(aeron_client_conductor_async_add_subscription(
        &async, &m_conductor, uri, stream_id, nullptr, nullptr, nullptr, nullptr), 0);
    doWork();

    transmitOnSubscriptionReady(async);
    doWork();

    const int64_t registration_id = aeron_async_add_subscription_get_registration_id(async);
    EXPECT_EQ(aeron_async_add_subscription_poll(&subscription, async), 1) << aeron_errmsg();
    EXPECT_NE(nullptr, subscription);

    std::vector<aeron_async_get_next_available_session_id_t *> pending_session_ids;
    while (true)
    {
        aeron_async_get_next_available_session_id_t *next_async;
        if (aeron_client_conductor_async_get_next_available_session_id(&next_async, &m_conductor, stream_id) < 0)
        {
            break;
        }
        pending_session_ids.push_back(next_async);
    }

    std::atomic<bool> on_close_called(false);
    auto on_close_complete = [](void* clientd)
    {
        auto flag = static_cast<std::atomic<bool>*>(clientd);
        flag->store(true);
    };

    EXPECT_EQ(
        0,
        aeron_client_conductor_async_close_subscription(&m_conductor, subscription, on_close_complete, &on_close_called));

    EXPECT_EQ(subscription, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));
    EXPECT_TRUE(subscription->pending_close_action);

    void *res;
    while (nullptr != (res = aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id)))
    {
        EXPECT_EQ(subscription, res);
        EXPECT_TRUE(subscription->pending_close_action);
        EXPECT_FALSE(on_close_called);
        doWorkForNs((int64_t)m_conductor.idle_sleep_duration_ns);
    }

    EXPECT_TRUE(on_close_called);

    for (auto async_session_id : pending_session_ids)
    {
        aeron_async_cmd_free(async_session_id);
    }
}

TEST_F(ClientConductorTest, shouldAsyncClosePublication)
{
    aeron_async_add_publication_t *async = nullptr;
    aeron_publication_t *publication = nullptr;
    m_conductor.invoker_mode = false;

    const char *uri = "aeron:ipc?term-length=64k";
    int32_t stream_id = 1000;
    EXPECT_EQ(aeron_client_conductor_async_add_publication(&async, &m_conductor, uri, stream_id), 0);
    doWork();

    transmitOnPublicationReady(async, m_logFileName, false);
    createLogFile(m_logFileName);
    doWork();

    const int64_t registration_id = aeron_async_add_publication_get_registration_id(async);
    EXPECT_EQ(aeron_async_add_publication_poll(&publication, async), 1) << aeron_errmsg();
    EXPECT_NE(nullptr, publication);

    std::atomic<bool> on_close_called(false);
    auto on_close_complete = [](void* clientd)
    {
        auto flag = static_cast<std::atomic<bool>*>(clientd);
        flag->store(true);
    };

    EXPECT_EQ(
        0,
        aeron_client_conductor_async_close_publication(&m_conductor, publication, on_close_complete, &on_close_called));

    EXPECT_EQ(publication, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));
    EXPECT_FALSE(publication->pending_close_action);

    doWork();

    EXPECT_TRUE(on_close_called);
    EXPECT_EQ(nullptr, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));
}

TEST_F(ClientConductorTest, shouldAsyncClosePublicationIfClientBufferIsFull)
{
    aeron_async_add_publication_t *async = nullptr;
    aeron_publication_t *publication = nullptr;
    m_conductor.invoker_mode = false;
    m_conductor.control_protocol_version = aeron_semantic_version_compose(
        AERON_CONTROL_PROTOCOL_MAJOR_VERSION, AERON_CONTROL_PROTOCOL_MINOR_VERSION, AERON_CONTROL_PROTOCOL_PATCH_VERSION);

    const char *uri = "aeron:ipc?term-length=64k";
    int32_t stream_id = 1000;
    EXPECT_EQ(aeron_client_conductor_async_add_publication(&async, &m_conductor, uri, stream_id), 0);
    doWork();

    transmitOnPublicationReady(async, m_logFileName, false);
    createLogFile(m_logFileName);
    doWork();

    const int64_t registration_id = aeron_async_add_publication_get_registration_id(async);
    EXPECT_EQ(aeron_async_add_publication_poll(&publication, async), 1) << aeron_errmsg();
    EXPECT_NE(nullptr, publication);

    std::vector<aeron_async_get_next_available_session_id_t *> pending_session_ids;
    while (true)
    {
        aeron_async_get_next_available_session_id_t *next_async;
        if (aeron_client_conductor_async_get_next_available_session_id(&next_async, &m_conductor, stream_id) < 0)
        {
            break;
        }
        pending_session_ids.push_back(next_async);
    }

    std::atomic<bool> on_close_called(false);
    auto on_close_complete = [](void* clientd)
    {
        auto flag = static_cast<std::atomic<bool>*>(clientd);
        flag->store(true);
    };

    EXPECT_EQ(
        0,
        aeron_client_conductor_async_close_publication(&m_conductor, publication, on_close_complete, &on_close_called));

    EXPECT_EQ(publication, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));
    EXPECT_TRUE(publication->pending_close_action);

    void *res;
    while (nullptr != (res = aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id)))
    {
        EXPECT_EQ(publication, res);
        EXPECT_TRUE(publication->pending_close_action);
        EXPECT_FALSE(on_close_called);
        doWorkForNs((int64_t)m_conductor.idle_sleep_duration_ns);
    }

    EXPECT_TRUE(on_close_called);

    for (auto async_session_id : pending_session_ids)
    {
        aeron_async_cmd_free(async_session_id);
    }
}

TEST_F(ClientConductorTest, shouldAsyncCloseExclusivePublication)
{
    aeron_async_add_publication_t *async = nullptr;
    aeron_exclusive_publication_t *publication = nullptr;
    m_conductor.invoker_mode = false;

    const char *uri = "aeron:ipc?term-length=64k";
    int32_t stream_id = 1000;
    EXPECT_EQ(aeron_client_conductor_async_add_exclusive_publication(&async, &m_conductor, uri, stream_id), 0);
    doWork();

    transmitOnPublicationReady(async, m_logFileName, true);
    createLogFile(m_logFileName);
    doWork();

    const int64_t registration_id = aeron_async_add_exclusive_publication_get_registration_id(async);
    EXPECT_EQ(aeron_async_add_exclusive_publication_poll(&publication, async), 1) << aeron_errmsg();
    EXPECT_NE(nullptr, publication);

    std::atomic<bool> on_close_called(false);
    auto on_close_complete = [](void* clientd)
    {
        auto flag = static_cast<std::atomic<bool>*>(clientd);
        flag->store(true);
    };

    EXPECT_EQ(
        0,
        aeron_client_conductor_async_close_exclusive_publication(&m_conductor, publication, on_close_complete, &on_close_called));

    EXPECT_EQ(publication, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));
    EXPECT_FALSE(publication->pending_close_action);

    doWork();

    EXPECT_TRUE(on_close_called);
    EXPECT_EQ(nullptr, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));
}

TEST_F(ClientConductorTest, shouldAsyncCloseExclusivePublicationIfClientBufferIsFull)
{
    aeron_async_add_publication_t *async = nullptr;
    aeron_exclusive_publication_t *publication = nullptr;
    m_conductor.invoker_mode = false;
    m_conductor.control_protocol_version = aeron_semantic_version_compose(
        AERON_CONTROL_PROTOCOL_MAJOR_VERSION, AERON_CONTROL_PROTOCOL_MINOR_VERSION, AERON_CONTROL_PROTOCOL_PATCH_VERSION);

    const char *uri = "aeron:ipc?term-length=64k";
    int32_t stream_id = 1000;
    EXPECT_EQ(aeron_client_conductor_async_add_exclusive_publication(&async, &m_conductor, uri, stream_id), 0);
    doWork();

    transmitOnPublicationReady(async, m_logFileName, true);
    createLogFile(m_logFileName);
    doWork();

    const int64_t registration_id = aeron_async_add_exclusive_publication_get_registration_id(async);
    EXPECT_EQ(aeron_async_add_exclusive_publication_poll(&publication, async), 1) << aeron_errmsg();
    EXPECT_NE(nullptr, publication);

    std::vector<aeron_async_get_next_available_session_id_t *> pending_session_ids;
    while (true)
    {
        aeron_async_get_next_available_session_id_t *next_async;
        if (aeron_client_conductor_async_get_next_available_session_id(&next_async, &m_conductor, stream_id) < 0)
        {
            break;
        }
        pending_session_ids.push_back(next_async);
    }

    std::atomic<bool> on_close_called(false);
    auto on_close_complete = [](void* clientd)
    {
        auto flag = static_cast<std::atomic<bool>*>(clientd);
        flag->store(true);
    };

    EXPECT_EQ(
        0,
        aeron_client_conductor_async_close_exclusive_publication(&m_conductor, publication, on_close_complete, &on_close_called));

    EXPECT_EQ(publication, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));
    EXPECT_TRUE(publication->pending_close_action);

    void *res;
    while (nullptr != (res = aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id)))
    {
        EXPECT_EQ(publication, res);
        EXPECT_TRUE(publication->pending_close_action);
        EXPECT_FALSE(on_close_called);
        doWorkForNs((int64_t)m_conductor.idle_sleep_duration_ns);
    }

    EXPECT_TRUE(on_close_called);

    for (auto async_session_id : pending_session_ids)
    {
        aeron_async_cmd_free(async_session_id);
    }
}

TEST_F(ClientConductorTest, shouldAsyncCloseCounter)
{
    aeron_async_add_counter_t *async = nullptr;
    aeron_counter_t *counter = nullptr;
    m_conductor.invoker_mode = false;

    int32_t type_id = 1000;
    std::string label = "my counter";
    EXPECT_EQ(aeron_client_conductor_async_add_counter(
        &async, &m_conductor, type_id, nullptr, 0, label.c_str(), label.length()), 0);
    doWork();

    transmitOnCounterReady(async);
    doWork();

    const int64_t registration_id = aeron_async_add_counter_get_registration_id(async);
    EXPECT_EQ(aeron_async_add_counter_poll(&counter, async), 1) << aeron_errmsg();
    EXPECT_NE(nullptr, counter);

    std::atomic<bool> on_close_called(false);
    auto on_close_complete = [](void* clientd)
    {
        auto flag = static_cast<std::atomic<bool>*>(clientd);
        flag->store(true);
    };

    EXPECT_EQ(
        0,
        aeron_client_conductor_async_close_counter(&m_conductor, counter, on_close_complete, &on_close_called));

    EXPECT_EQ(counter, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));
    EXPECT_FALSE(counter->pending_close_action);

    doWork();

    EXPECT_TRUE(on_close_called);
    EXPECT_EQ(nullptr, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));
}

TEST_F(ClientConductorTest, shouldAsyncCloseCounterIfClientBufferIsFull)
{
    aeron_async_add_counter_t *async = nullptr;
    aeron_counter_t *counter = nullptr;
    m_conductor.invoker_mode = false;
    m_conductor.control_protocol_version = aeron_semantic_version_compose(
        AERON_CONTROL_PROTOCOL_MAJOR_VERSION, AERON_CONTROL_PROTOCOL_MINOR_VERSION, AERON_CONTROL_PROTOCOL_PATCH_VERSION);

    int32_t type_id = 1000;
    std::string label = "my counter";
    EXPECT_EQ(aeron_client_conductor_async_add_counter(
        &async, &m_conductor, type_id, nullptr, 0, label.c_str(), label.length()), 0);
    doWork();

    transmitOnCounterReady(async);
    doWork();

    const int64_t registration_id = aeron_async_add_counter_get_registration_id(async);
    EXPECT_EQ(aeron_async_add_counter_poll(&counter, async), 1) << aeron_errmsg();
    EXPECT_NE(nullptr, counter);

    std::vector<aeron_async_get_next_available_session_id_t *> pending_session_ids;
    while (true)
    {
        aeron_async_get_next_available_session_id_t *next_async;
        if (aeron_client_conductor_async_get_next_available_session_id(&next_async, &m_conductor, type_id) < 0)
        {
            break;
        }
        pending_session_ids.push_back(next_async);
    }

    std::atomic<bool> on_close_called(false);
    auto on_close_complete = [](void* clientd)
    {
        auto flag = static_cast<std::atomic<bool>*>(clientd);
        flag->store(true);
    };

    EXPECT_EQ(
        0,
        aeron_client_conductor_async_close_counter(&m_conductor, counter, on_close_complete, &on_close_called));

    EXPECT_EQ(counter, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));
    EXPECT_TRUE(counter->pending_close_action);

    void *res;
    while (nullptr != (res = aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id)))
    {
        EXPECT_EQ(counter, res);
        EXPECT_TRUE(counter->pending_close_action);
        EXPECT_FALSE(on_close_called);
        doWorkForNs((int64_t)m_conductor.idle_sleep_duration_ns);
    }

    EXPECT_TRUE(on_close_called);

    for (auto async_session_id : pending_session_ids)
    {
        aeron_async_cmd_free(async_session_id);
    }
}

TEST_F(ClientConductorTest, shouldNotifyOnCloseCompleteWhenClientConductorIsBeingClosedAndCounterIsForcefullyDeleted)
{
    aeron_async_add_counter_t *async = nullptr;
    aeron_counter_t *counter = nullptr;
    m_conductor.invoker_mode = false;

    int32_t type_id = 1000;
    std::string label = "my counter";
    EXPECT_EQ(aeron_client_conductor_async_add_counter(
        &async, &m_conductor, type_id, nullptr, 0, label.c_str(), label.length()), 0);
    doWork();

    transmitOnCounterReady(async);
    doWork();

    const int64_t registration_id = aeron_async_add_counter_get_registration_id(async);
    EXPECT_EQ(aeron_async_add_counter_poll(&counter, async), 1) << aeron_errmsg();
    EXPECT_NE(nullptr, counter);

    std::atomic<bool> on_close_called(false);
    auto on_close_complete = [](void* clientd)
    {
        auto flag = static_cast<std::atomic<bool>*>(clientd);
        flag->store(true);
    };

    // register `on_close` callback but do not process actual close
    EXPECT_EQ(
        0,
        aeron_client_conductor_async_close_counter(&m_conductor, counter, on_close_complete, &on_close_called));
    EXPECT_FALSE(on_close_called);
    EXPECT_EQ(counter, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));

    aeron_client_conductor_on_close(&m_conductor);
    m_conductor_isClosed = true;

    EXPECT_TRUE(on_close_called);
}

TEST_F(ClientConductorTest, shouldAsyncCloseStaticCounter)
{
    aeron_async_add_counter_t *async = nullptr;
    aeron_counter_t *counter = nullptr;
    m_conductor.invoker_mode = false;

    int32_t type_id = 1000;
    int64_t registration_id = 437294837249;
    std::string label = "my counter";
    EXPECT_EQ(
        aeron_client_conductor_async_add_static_counter(&async, &m_conductor, type_id, nullptr, 0, label.c_str(), label.length(), registration_id),
        0);
    int64_t correlation_id = aeron_async_add_counter_get_registration_id(async);
    doWork();

    transmitOnStaticCounter(async, 42);
    doWork();

    EXPECT_EQ(aeron_async_add_counter_poll(&counter, async), 1) << aeron_errmsg();
    EXPECT_NE(nullptr, counter);
    EXPECT_EQ(counter, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, correlation_id));
    EXPECT_EQ(nullptr, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));

    std::atomic<bool> on_close_called(false);
    auto on_close_complete = [](void* clientd)
    {
        auto flag = static_cast<std::atomic<bool>*>(clientd);
        flag->store(true);
    };

    EXPECT_EQ(
        0,
        aeron_client_conductor_async_close_counter(&m_conductor, counter, on_close_complete, &on_close_called));
    EXPECT_FALSE(aeron_counter_is_closed(counter));
    EXPECT_FALSE(counter->pending_close_action);

    doWork();

    EXPECT_TRUE(on_close_called);
    EXPECT_EQ(nullptr, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, correlation_id));
    EXPECT_EQ(nullptr, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));
}

TEST_F(ClientConductorTest, shouldAsyncCloseStaticCounterIfClientBufferIsFull)
{
    aeron_async_add_counter_t *async = nullptr;
    aeron_counter_t *counter = nullptr;
    m_conductor.invoker_mode = false;
    m_conductor.control_protocol_version = aeron_semantic_version_compose(
        AERON_CONTROL_PROTOCOL_MAJOR_VERSION, AERON_CONTROL_PROTOCOL_MINOR_VERSION, AERON_CONTROL_PROTOCOL_PATCH_VERSION);

    int32_t type_id = 1000;
    int64_t registration_id = 437294837249;
    std::string label = "my counter";
    EXPECT_EQ(
        aeron_client_conductor_async_add_static_counter(&async, &m_conductor, type_id, nullptr, 0, label.c_str(), label.length(), registration_id),
        0);
    int64_t correlation_id = aeron_async_add_counter_get_registration_id(async);
    doWork();

    transmitOnStaticCounter(async, 42);
    doWork();

    EXPECT_EQ(aeron_async_add_counter_poll(&counter, async), 1) << aeron_errmsg();
    EXPECT_NE(nullptr, counter);
    EXPECT_EQ(counter, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, correlation_id));
    EXPECT_EQ(nullptr, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));

    std::vector<aeron_async_get_next_available_session_id_t *> pending_session_ids;
    while (true)
    {
        aeron_async_get_next_available_session_id_t *next_async;
        if (aeron_client_conductor_async_get_next_available_session_id(&next_async, &m_conductor, type_id) < 0)
        {
            break;
        }
        pending_session_ids.push_back(next_async);
    }

    std::atomic<bool> on_close_called(false);
    auto on_close_complete = [](void* clientd)
    {
        auto flag = static_cast<std::atomic<bool>*>(clientd);
        flag->store(true);
    };

    EXPECT_EQ(
        0,
        aeron_client_conductor_async_close_counter(&m_conductor, counter, on_close_complete, &on_close_called));
    EXPECT_FALSE(aeron_counter_is_closed(counter));
    EXPECT_TRUE(counter->pending_close_action);

    while (!on_close_called)
    {
        doWorkForNs((int64_t)m_conductor.idle_sleep_duration_ns);
    }

    EXPECT_TRUE(on_close_called);
    EXPECT_EQ(nullptr, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, correlation_id));
    EXPECT_EQ(nullptr, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));

    for (auto async_session_id : pending_session_ids)
    {
        aeron_async_cmd_free(async_session_id);
    }
}

TEST_F(ClientConductorTest, shouldNotCloseStaticCounterWhenConductorIsClosed)
{
    aeron_async_add_counter_t *async = nullptr;
    aeron_counter_t *counter = nullptr;
    m_conductor.invoker_mode = false;

    int32_t type_id = 1000;
    int64_t registration_id = 437294837249;
    std::string label = "my counter";
    EXPECT_EQ(
        aeron_client_conductor_async_add_static_counter(&async, &m_conductor, type_id, nullptr, 0, label.c_str(), label.length(), registration_id),
        0);
    int64_t correlation_id = aeron_async_add_counter_get_registration_id(async);
    doWork();

    transmitOnStaticCounter(async, 42);
    doWork();

    EXPECT_EQ(aeron_async_add_counter_poll(&counter, async), 1) << aeron_errmsg();
    EXPECT_NE(nullptr, counter);
    EXPECT_EQ(counter, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, correlation_id));
    EXPECT_EQ(nullptr, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));

    std::atomic<bool> on_close_called(false);
    auto on_close_complete = [](void* clientd)
    {
        auto flag = static_cast<std::atomic<bool>*>(clientd);
        flag->store(true);
    };

    // register `on_close` callback but do not process actual close
    EXPECT_EQ(
        0,
        aeron_client_conductor_async_close_counter(&m_conductor, counter, on_close_complete, &on_close_called));
    EXPECT_FALSE(on_close_called);

    aeron_client_conductor_on_close(&m_conductor);
    m_conductor_isClosed = true;

    EXPECT_FALSE(aeron_counter_is_closed(counter));
    EXPECT_FALSE(on_close_called);

    EXPECT_EQ(0, aeron_counter_delete(counter));
}

TEST_F(ClientConductorTest, shouldNotifyOnCloseCompleteWhenClientConductorIsBeingClosedAndSubscriptionIsForcefullyDeleted)
{
    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription = nullptr;
    m_conductor.invoker_mode = false;

    const char *uri = "aeron:ipc?term-length=64k";
    int32_t stream_id = 1000;
    EXPECT_EQ(aeron_client_conductor_async_add_subscription(
        &async, &m_conductor, uri, stream_id, nullptr, nullptr, nullptr, nullptr), 0);
    doWork();

    transmitOnSubscriptionReady(async);
    doWork();

    const int64_t registration_id = aeron_async_add_subscription_get_registration_id(async);
    EXPECT_EQ(aeron_async_add_subscription_poll(&subscription, async), 1) << aeron_errmsg();
    EXPECT_NE(nullptr, subscription);

    std::atomic<bool> on_close_called(false);
    auto on_close_complete = [](void* clientd)
    {
        auto flag = static_cast<std::atomic<bool>*>(clientd);
        flag->store(true);
    };

    // register `on_close` callback but do not process actual close
    EXPECT_EQ(
        0,
        aeron_client_conductor_async_close_subscription(&m_conductor, subscription, on_close_complete, &on_close_called));
    EXPECT_FALSE(on_close_called);
    EXPECT_EQ(subscription, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));

    aeron_client_conductor_on_close(&m_conductor);
    m_conductor_isClosed = true;

    EXPECT_TRUE(on_close_called);
}

TEST_F(ClientConductorTest, shouldNotifyOnCloseCompleteWhenClientConductorIsBeingClosedAndPublicationIsForcefullyDeleted)
{
    aeron_async_add_publication_t *async = nullptr;
    aeron_publication_t *publication = nullptr;
    m_conductor.invoker_mode = false;

    const char *uri = "aeron:ipc?term-length=64k";
    int32_t stream_id = 1000;
    EXPECT_EQ(aeron_client_conductor_async_add_publication(&async, &m_conductor, uri, stream_id), 0);
    doWork();

    transmitOnPublicationReady(async, m_logFileName, false);
    createLogFile(m_logFileName);
    doWork();

    const int64_t registration_id = aeron_async_add_publication_get_registration_id(async);
    EXPECT_EQ(aeron_async_add_publication_poll(&publication, async), 1) << aeron_errmsg();
    EXPECT_NE(nullptr, publication);

    std::atomic<bool> on_close_called(false);
    auto on_close_complete = [](void* clientd)
    {
        auto flag = static_cast<std::atomic<bool>*>(clientd);
        flag->store(true);
    };

    // register `on_close` callback but do not process actual close
    EXPECT_EQ(
        0,
        aeron_client_conductor_async_close_publication(&m_conductor, publication, on_close_complete, &on_close_called));
    EXPECT_FALSE(on_close_called);
    EXPECT_EQ(publication, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));

    aeron_client_conductor_on_close(&m_conductor);
    m_conductor_isClosed = true;

    EXPECT_TRUE(on_close_called);
}

TEST_F(ClientConductorTest, shouldNotifyOnCloseCompleteWhenClientConductorIsBeingClosedAndExclusivePublicationIsForcefullyDeleted)
{
    aeron_async_add_publication_t *async = nullptr;
    aeron_exclusive_publication_t *publication = nullptr;
    m_conductor.invoker_mode = false;

    const char *uri = "aeron:ipc?term-length=64k";
    int32_t stream_id = 1000;
    EXPECT_EQ(aeron_client_conductor_async_add_exclusive_publication(&async, &m_conductor, uri, stream_id), 0);
    doWork();

    transmitOnPublicationReady(async, m_logFileName, true);
    createLogFile(m_logFileName);
    doWork();

    const int64_t registration_id = aeron_async_add_exclusive_publication_get_registration_id(async);
    EXPECT_EQ(aeron_async_add_exclusive_publication_poll(&publication, async), 1) << aeron_errmsg();
    EXPECT_NE(nullptr, publication);

    std::atomic<bool> on_close_called(false);
    auto on_close_complete = [](void* clientd)
    {
        auto flag = static_cast<std::atomic<bool>*>(clientd);
        flag->store(true);
    };

    // register `on_close` callback but do not process actual close
    EXPECT_EQ(
        0,
        aeron_client_conductor_async_close_exclusive_publication(&m_conductor, publication, on_close_complete, &on_close_called));
    EXPECT_FALSE(on_close_called);
    EXPECT_EQ(publication, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));

    aeron_client_conductor_on_close(&m_conductor);
    m_conductor_isClosed = true;

    EXPECT_TRUE(on_close_called);
}

TEST_F(ClientConductorTest, shouldAddMultipleStaticCountersWithTheSameRegistrationId)
{
    aeron_async_add_counter_t *async = nullptr, *async2 = nullptr;
    aeron_counter_t *counter = nullptr, *counter2 = nullptr;
    m_conductor.invoker_mode = false;

    int64_t registration_id = 437294837249;
    std::string label = "my counter";
    EXPECT_EQ(
        aeron_client_conductor_async_add_static_counter(&async, &m_conductor, 1000, nullptr, 0, label.c_str(), label.length(), registration_id),
        0);
    int64_t correlation_id = aeron_async_add_counter_get_registration_id(async);
    doWork();

    std::string label2 = "my counter";
    EXPECT_EQ(
        aeron_client_conductor_async_add_static_counter(&async2, &m_conductor, 2000, nullptr, 0, label2.c_str(), label2.length(), registration_id),
        0);
    int64_t correlation_id2 = aeron_async_add_counter_get_registration_id(async2);
    EXPECT_NE(correlation_id, correlation_id2);
    doWork();

    transmitOnStaticCounter(async, 42);
    doWork();

    EXPECT_EQ(aeron_async_add_counter_poll(&counter, async), 1) << aeron_errmsg();
    EXPECT_NE(nullptr, counter);
    EXPECT_EQ(counter, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, correlation_id));
    EXPECT_EQ(nullptr, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));

    transmitOnStaticCounter(async2, 24);
    doWork();

    EXPECT_EQ(aeron_async_add_counter_poll(&counter2, async2), 1) << aeron_errmsg();
    EXPECT_NE(nullptr, counter2);
    EXPECT_EQ(counter2, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, correlation_id2));
    EXPECT_EQ(nullptr, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, registration_id));
    EXPECT_NE(counter->counter_id, counter2->counter_id);

    auto addr1 = aeron_counters_manager_addr(&m_counters_manager, counter->counter_id);
    auto addr2 = aeron_counters_manager_addr(&m_counters_manager, counter2->counter_id);
    aeron_counter_set_release(addr1, 1984);
    aeron_counter_set_release(addr2, 2026);
    EXPECT_EQ(aeron_counter_get_acquire(addr1), 1984);
    EXPECT_EQ(aeron_counter_get_acquire(addr2), 2026);

    std::atomic<bool> on_close_called(false);
    auto on_close_complete = [](void* clientd)
    {
        auto flag = static_cast<std::atomic<bool>*>(clientd);
        flag->store(true);
    };

    EXPECT_EQ(
        0,
        aeron_client_conductor_async_close_counter(&m_conductor, counter, on_close_complete, &on_close_called));
    EXPECT_FALSE(aeron_counter_is_closed(counter));
    EXPECT_FALSE(counter->pending_close_action);

    doWork();

    EXPECT_TRUE(on_close_called);

    EXPECT_EQ(aeron_counter_get_acquire(addr2), 2026);
    EXPECT_FALSE(aeron_counter_is_closed(counter2));
    EXPECT_EQ(nullptr, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, correlation_id));
    EXPECT_EQ(counter2, aeron_int64_to_ptr_hash_map_get(&m_conductor.resource_by_id_map, correlation_id2));

    EXPECT_EQ(0, aeron_client_conductor_async_close_counter(&m_conductor, counter2, nullptr, nullptr));

    doWork();
}

TEST_F(ClientConductorTest, shouldFreeRemoveResourceCmdAfterConductorProcessesIt)
{
    m_conductor.invoker_mode = false;

    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_subscription(
        &async, &m_conductor, SUB_URI, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    doWork();

    transmitOnSubscriptionReady(async);
    doWork();

    ASSERT_EQ(aeron_async_add_subscription_poll(&subscription, async), 1) << aeron_errmsg();
    ASSERT_NE(nullptr, subscription);
    const int64_t registration_id = subscription->registration_id;

    std::atomic<bool> removed(false);
    auto on_remove = [](void *clientd)
    {
        static_cast<std::atomic<bool> *>(clientd)->store(true);
    };

    ASSERT_EQ(aeron_client_conductor_async_remove_resource(
        registration_id, AERON_CLIENT_MANAGED_RESOURCE_TYPE_SUBSCRIPTION, &m_conductor, on_remove, &removed), 0);

    EXPECT_FALSE(removed);

    doWork();

    EXPECT_TRUE(removed) << "on_complete should fire after the conductor processes the remove cmd";
}

TEST_F(ClientConductorTest, shouldFreeUnprocessedRemoveResourceCmdOnClose)
{
    m_conductor.invoker_mode = false;

    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_subscription(
        &async, &m_conductor, SUB_URI, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    doWork();

    transmitOnSubscriptionReady(async);
    doWork();

    ASSERT_EQ(aeron_async_add_subscription_poll(&subscription, async), 1) << aeron_errmsg();
    ASSERT_NE(nullptr, subscription);
    const int64_t registration_id = subscription->registration_id;

    std::atomic<bool> removed(false);
    auto on_remove = [](void *clientd)
    {
        static_cast<std::atomic<bool> *>(clientd)->store(true);
    };

    ASSERT_EQ(aeron_client_conductor_async_remove_resource(
        registration_id, AERON_CLIENT_MANAGED_RESOURCE_TYPE_SUBSCRIPTION, &m_conductor, on_remove, &removed), 0);

    /* Intentionally do not doWork() — cmd stays queued. Close should drain it. */
    aeron_client_conductor_on_close(&m_conductor);
    m_conductor_isClosed = true;

    EXPECT_TRUE(removed) << "on_complete should fire from the close-time queue drain";
}

TEST_F(ClientConductorTest, shouldFreeRemoveResourceCmdImmediatelyInInvokerMode)
{
    /* Fixture defaults to invoker_mode = true. */
    ASSERT_TRUE(m_conductor.invoker_mode);

    aeron_async_add_subscription_t *async = nullptr;
    aeron_subscription_t *subscription = nullptr;

    ASSERT_EQ(aeron_client_conductor_async_add_subscription(
        &async, &m_conductor, SUB_URI, STREAM_ID, nullptr, nullptr, nullptr, nullptr), 0);
    transmitOnSubscriptionReady(async);
    doWork();

    ASSERT_EQ(aeron_async_add_subscription_poll(&subscription, async), 1) << aeron_errmsg();
    ASSERT_NE(nullptr, subscription);
    const int64_t registration_id = subscription->registration_id;

    std::atomic<bool> removed(false);
    auto on_remove = [](void *clientd)
    {
        static_cast<std::atomic<bool> *>(clientd)->store(true);
    };

    ASSERT_EQ(aeron_client_conductor_async_remove_resource(
        registration_id, AERON_CLIENT_MANAGED_RESOURCE_TYPE_SUBSCRIPTION, &m_conductor, on_remove, &removed), 0);

    EXPECT_TRUE(removed) << "invoker mode processes the remove cmd synchronously in the caller";
}

TEST_F(ClientConductorTest, shouldSupportConcurrentAsyncRemoveResourceFromMultipleThreads)
{
    m_conductor.invoker_mode = false;

    constexpr int kProducerThreads = 4;
    constexpr int kCmdsPerThread = 32;
    constexpr int kTotalCmds = kProducerThreads * kCmdsPerThread;

    std::atomic<int> completed(0);
    auto on_remove = [](void *clientd)
    {
        static_cast<std::atomic<int> *>(clientd)->fetch_add(1, std::memory_order_relaxed);
    };

    std::atomic<bool> go(false);
    std::vector<std::thread> producers;
    producers.reserve(kProducerThreads);

    for (int t = 0; t < kProducerThreads; t++)
    {
        producers.emplace_back([&, t]()
        {
            while (!go.load(std::memory_order_acquire))
            {
                std::this_thread::yield();
            }
            for (int i = 0; i < kCmdsPerThread; i++)
            {
                const int64_t registration_id = (int64_t)(t * kCmdsPerThread + i);
                /* No registered resource behind these ids — the remove cmd will be a no-op
                 * for driver-side work, but the conductor still frees the cmd and fires
                 * on_complete. Exercises the MPSC offer + tag path from multiple producers. */
                ASSERT_EQ(aeron_client_conductor_async_remove_resource(
                    registration_id,
                    AERON_CLIENT_MANAGED_RESOURCE_TYPE_SUBSCRIPTION,
                    &m_conductor,
                    on_remove,
                    &completed), 0);
            }
        });
    }

    go.store(true, std::memory_order_release);

    int total_work = 0;
    while (completed.load(std::memory_order_relaxed) < kTotalCmds && total_work < (kTotalCmds * 100))
    {
        total_work += doWork();
        std::this_thread::yield();
    }

    for (auto &p : producers)
    {
        p.join();
    }

    /* Drain anything still queued. */
    doWork();

    EXPECT_EQ(kTotalCmds, completed.load());
}

TEST_F(ClientConductorTest, shouldDrainQueuedRemoveCmdsFromProducerThreadOnClose)
{
    m_conductor.invoker_mode = false;

    std::atomic<bool> stop(false);
    std::atomic<int> offers_succeeded(0);
    std::atomic<int> callbacks_fired(0);

    auto on_remove = [](void *clientd)
    {
        static_cast<std::atomic<int> *>(clientd)->fetch_add(1, std::memory_order_relaxed);
    };

    /* Producer thread offers remove cmds until the main thread signals stop. Exercises the
     * MPSC queue's multi-producer offer while the main thread alternates doWork (the
     * single-consumer path) — mirrors how a user thread would issue async_remove_resource
     * while the conductor agent drains in production. */
    std::thread producer([&]()
    {
        int64_t next_id = 0;
        while (!stop.load(std::memory_order_acquire))
        {
            if (aeron_client_conductor_async_remove_resource(
                next_id++,
                AERON_CLIENT_MANAGED_RESOURCE_TYPE_SUBSCRIPTION,
                &m_conductor,
                on_remove,
                &callbacks_fired) == 0)
            {
                offers_succeeded.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });

    /* Interleave processing with production for a short burst. */
    auto start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - start < std::chrono::milliseconds(5))
    {
        doWork();
    }

    /* Stop the producer and join before triggering close. In production, aeron_close's
     * internal pthread_join establishes the same ordering guarantee: no producer offers
     * after the close-path drain starts. */
    stop.store(true, std::memory_order_release);
    producer.join();

    /* Close must drain everything the producer left queued. */
    aeron_client_conductor_on_close(&m_conductor);
    m_conductor_isClosed = true;

    EXPECT_EQ(offers_succeeded.load(), callbacks_fired.load())
        << "Every successfully-offered cmd must have had its on_complete fired "
           "(either by doWork during the burst or by the close-time drain).";
}
