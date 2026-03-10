/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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

#include "gtest/gtest.h"
#include <gmock/gmock-matchers.h>
#include "../EmbeddedMediaDriver.h"
#include "../TestArchive.h"
#include "ArchiveClientTestUtils.h"

extern "C"
{
#include <inttypes.h>
#include "aeronc.h"
#include "aeron_client.h"
#include "aeron_archive_client/controlResponse.h"
#include "client/aeron_archive.h"
#include "client/aeron_archive_async_client.h"
#include "client/aeron_archive_client.h"
#include "uri/aeron_uri_string_builder.h"
}

typedef struct fragment_handler_clientd_stct
{
    size_t received;
    int64_t position;
}
fragment_handler_clientd_t;

void fragment_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_stct *header)
{
    auto *cd = static_cast<fragment_handler_clientd_t*>(clientd);
    cd->received++;
    cd->position = aeron_header_position(header);
}

class AeronArchiveAsyncClientTest : public testing::Test
{
protected:
    const std::string m_recordingChannel = "aeron:udp?endpoint=localhost:3333";
    const std::int32_t m_recordingStreamId = 33;

    void connect(const std::string& aeronDir)
    {
        aeron_archive_context_t *ctx;
        ASSERT_EQ(0, aeron_archive_context_init(&ctx)) << aeron_errmsg();
        ASSERT_EQ(0, aeron_archive_context_set_aeron_directory_name(ctx, aeronDir.c_str())) << aeron_errmsg();
        ASSERT_EQ(0, aeron_archive_context_set_control_request_channel(ctx, "aeron:udp?endpoint=localhost:8010")) << aeron_errmsg();
        ASSERT_EQ(0, aeron_archive_context_set_control_response_channel(ctx, "aeron:udp?endpoint=localhost:0")) << aeron_errmsg();
        ASSERT_EQ(0, Credentials::defaultCredentials().configure(ctx)) << aeron_errmsg();

        ASSERT_EQ(0, aeron_archive_connect(&m_archive, ctx)) << aeron_errmsg();
    }

    void disconnect()
    {
        if (nullptr != m_archive)
        {
            ASSERT_EQ(0, aeron_archive_close(m_archive)) << aeron_errmsg();
            m_archive = nullptr;
        }
    }

    void recordData(int64_t* recording_id, int64_t* stop_position)
    {
        ASSERT_NE(m_archive, nullptr);

        aeron_t *aeron = aeron_archive_context_get_aeron(m_archive->ctx);

        int64_t subscription_id;
        ASSERT_EQ(0, aeron_archive_start_recording(
            &subscription_id,
            m_archive,
            m_recordingChannel.c_str(),
            m_recordingStreamId,
            AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
            true)) << aeron_errmsg();

        aeron_publication_t *publication = addPublication(aeron, m_recordingChannel, m_recordingStreamId);
        ASSERT_NE(publication, nullptr) << aeron_errmsg();

        int32_t session_id = aeron_publication_session_id(publication);

        aeron_counters_reader_t *m_counters_reader = aeron_counters_reader(aeron);
        int32_t m_counter_id = getRecordingCounterId(session_id, m_counters_reader);
        int64_t m_recording_id_from_counter = aeron_archive_recording_pos_get_recording_id(m_counters_reader, m_counter_id);
        *recording_id = m_recording_id_from_counter;

        EXPECT_EQ(m_counter_id, aeron_archive_recording_pos_find_counter_id_by_recording_id(
            m_counters_reader,
            m_recording_id_from_counter));

        const char *buffer = "Hello, World!";
        size_t length = strlen(buffer);

        while (aeron_publication_offer(publication, reinterpret_cast<const uint8_t*>(buffer), length, nullptr, nullptr) < 0)
        {
            std::this_thread::yield();
        }

        *stop_position = aeron_publication_position(publication);

        aeron_publication_close(publication, nullptr, nullptr);

        while (true)
        {
            int64_t pos;

            ASSERT_EQ(0, aeron_archive_get_stop_position(&pos, m_archive, *recording_id)) << aeron_errmsg();

            if (pos != AERON_NULL_POSITION)
            {
                break;
            }
        }
    }

    static aeron_publication_t* addPublication(aeron_t* aeron, const std::string& channel, const int32_t streamId)
    {
        aeron_async_add_publication_t *async_add_publication;
        if (aeron_async_add_publication(&async_add_publication, aeron, channel.c_str(), streamId) < 0)
        {
            return nullptr;
        }

        aeron_publication_t* publication = nullptr;
        while (publication == nullptr)
        {
            if (aeron_async_add_publication_poll(&publication, async_add_publication) < 0)
            {
                break;
            }
        }

        return publication;
    }

    static aeron_subscription_t *addSubscription(aeron_t *aeron, const std::string& channel, const int32_t streamId)
    {
        aeron_async_add_subscription_t *async_add_subscription;
        if (aeron_async_add_subscription(
            &async_add_subscription,
            aeron,
            channel.c_str(),
            streamId,
            nullptr,
            nullptr,
            nullptr,
            nullptr) < 0)
        {
            return nullptr;
        }

        aeron_subscription_t *subscription = nullptr;
        while (nullptr == subscription)
        {
            if (aeron_async_add_subscription_poll(&subscription, async_add_subscription) < 0)
            {
                break;
            }
        }

        return subscription;
    }

    static int32_t getRecordingCounterId(int32_t session_id, aeron_counters_reader_t *counters_reader)
    {
        int32_t counter_id;

        while (AERON_NULL_COUNTER_ID ==
            (counter_id = aeron_archive_recording_pos_find_counter_id_by_session_id(counters_reader, session_id)))
        {
            std::this_thread::yield();
        }

        return counter_id;
    }

    static TestArchive createTestArchive(const std::string& aeron_dir)
    {
        return {
            aeron_dir,
            ARCHIVE_DIR,
            std::cout,
            "aeron:udp?endpoint=localhost:8010",
            "aeron:udp?endpoint=localhost:0",
            17
        };
    }

private:
    aeron_archive_t *m_archive = nullptr;
};

struct ControlResponse
{
    int64_t correlation_id;
    int64_t relevant_id;
    int32_t code;
    std::string error_message;
};

struct RecordingDescriptor
{
    int64_t correlation_id;
    int64_t recording_id;
    int64_t start_timestamp;
    int64_t stop_timestamp;
    int64_t start_position;
    int64_t stop_position;
    int32_t initial_term_id;
    int32_t segment_file_length;
    int32_t term_buffer_length;
    int32_t mtu_length;
    int32_t session_id;
    int32_t stream_id;
    std::string stripped_channel;
    std::string original_channel;
    std::string source_identity;
};

struct TestAsyncArchiveClientListener
{
    aeron_archive_async_client_listener_t listener;
    int connectedCount;
    int disconnectedCount;
    std::vector<ControlResponse> controlResponses;
    RecordingDescriptor* lastRecordingDescriptor;

    TestAsyncArchiveClientListener() :
        listener({
            this,
            onConnected,
            onDisconnected,
            onControlResponse,
            onRecordingDescriptor
        }),
        connectedCount(0),
        disconnectedCount(0),
        controlResponses(std::vector<ControlResponse>{}),
        lastRecordingDescriptor(nullptr)
    {
    }

    ~TestAsyncArchiveClientListener()
    {
        delete lastRecordingDescriptor;
    }

    static void onConnected(void *clientd)
    {
        const auto self = static_cast<TestAsyncArchiveClientListener*>(clientd);
        self->connectedCount++;
    }

    static void onDisconnected(void *clientd)
    {
        const auto self = static_cast<TestAsyncArchiveClientListener*>(clientd);
        self->disconnectedCount++;
    }

    static void onControlResponse(
        void *clientd,
        int64_t correlation_id,
        int64_t relevant_id,
        int32_t code,
        const char *error_message)
    {
        const auto self = static_cast<TestAsyncArchiveClientListener*>(clientd);
        self->controlResponses.emplace_back(ControlResponse{
            correlation_id,
            relevant_id,
            code,
            std::string(error_message)
        });
    }

    ControlResponse* controlResponseFor(int64_t correlation_id)
    {
        for (ControlResponse& controlResponse : controlResponses)
        {
            if (controlResponse.correlation_id == correlation_id)
            {
                return &controlResponse;
            }
        }

        return nullptr;
    }

    static void onRecordingDescriptor(void *clientd, aeron_archive_recording_descriptor_t *recording_descriptor)
    {
        const auto self = static_cast<TestAsyncArchiveClientListener*>(clientd);
        self->lastRecordingDescriptor = new RecordingDescriptor({
            recording_descriptor->correlation_id,
            recording_descriptor->recording_id,
            recording_descriptor->start_timestamp,
            recording_descriptor->stop_timestamp,
            recording_descriptor->start_position,
            recording_descriptor->stop_position,
            recording_descriptor->initial_term_id,
            recording_descriptor->segment_file_length,
            recording_descriptor->term_buffer_length,
            recording_descriptor->mtu_length,
            recording_descriptor->session_id,
            recording_descriptor->stream_id,
            std::string(recording_descriptor->stripped_channel),
            std::string(recording_descriptor->original_channel),
            std::string(recording_descriptor->source_identity)
        });
    }
};

template<typename T>
void pollUntil(
    const std::string& label,
    aeron_archive_async_client_t *client,
    const std::function<T()>& supplier,
    const std::function<bool(T)>& predicate)
{
    const int64_t deadline_ms = aeron_epoch_clock() + 10000;

    while (true)
    {
        int result = aeron_archive_async_client_poll(client);
        if (result < 0)
        {
            FAIL() << "aeron_archive_async_client_poll failed " << aeron_errmsg();
        }

        T value = supplier();
        if (predicate(value))
        {
            break;
        }

        if (aeron_epoch_clock() >= deadline_ms)
        {
            FAIL() << "timed out waiting for \"" << label << "\", last value was " << value;
        }
    }
}

ControlResponse* pollUntilControlResponseReceived(
    aeron_archive_async_client_t *client,
    TestAsyncArchiveClientListener& listener,
    const int64_t correlation_id)
{
    auto label = "ControlResponse " + std::to_string(correlation_id) + " received";
    pollUntil<ControlResponse*>(
        label,
        client,
        [&] { return listener.controlResponseFor(correlation_id); },
        [](auto x) { return x != nullptr; });
    return listener.controlResponseFor(correlation_id);
}

RecordingDescriptor* pollUntilRecordingDescriptorReceived(
    aeron_archive_async_client_t *client,
    const TestAsyncArchiveClientListener& listener)
{
    pollUntil<RecordingDescriptor*>(
        "RecordingDescriptor received",
        client,
        [&] { return listener.lastRecordingDescriptor; },
        [](auto x) { return x != nullptr; });
    return listener.lastRecordingDescriptor;
}

class DriverResource
{
public:
    DriverResource()
    {
        char path[AERON_MAX_PATH];
        aeron_default_path(path, sizeof(path));
        const auto aeronDir = std::string(path) + "-" + std::to_string(aeron_randomised_int32());
        m_driver.aeronDir(aeronDir);
        m_driver.start();
    }

    ~DriverResource()
    {
        m_driver.stop();
    }

    std::string aeronDir()
    {
        return m_driver.aeronDir();
    }

protected:
    aeron::EmbeddedMediaDriver m_driver;
};

TEST_F(AeronArchiveAsyncClientTest, testAeronArchiveAsyncClient)
{
    char aeron_dir[AERON_MAX_PATH];
    aeron_default_path(aeron_dir, sizeof(aeron_dir));

    DriverResource driver;
    AeronResource aeron(driver.aeronDir());

    aeron_archive_context_t *context;
    ASSERT_EQ(0, aeron_archive_context_init(&context)) << aeron_errmsg();
    ASSERT_EQ(0, aeron_archive_context_set_aeron(context, aeron.aeron())) << aeron_errmsg();
    ASSERT_EQ(0, aeron_archive_context_set_control_request_channel(context, "aeron:udp?endpoint=localhost:8010")) << aeron_errmsg();
    ASSERT_EQ(0, aeron_archive_context_set_control_response_channel(context, "aeron:udp?endpoint=localhost:0")) << aeron_errmsg();
    ASSERT_EQ(0, Credentials::defaultCredentials().configure(context)) << aeron_errmsg();

    TestAsyncArchiveClientListener listener;
    int64_t recording_id, stop_position;

    aeron_archive_async_client_t *client;
    ASSERT_EQ(0, aeron_archive_async_client_create(&client, context, &listener.listener));

    {
        TestArchive testArchive = createTestArchive(aeron_dir);

        ASSERT_FALSE(aeron_archive_async_client_is_connected(client));
        pollUntil<int>("is connected", client, [&] { return listener.connectedCount; }, [](const int x) { return x == 1; });
        ASSERT_TRUE(aeron_archive_async_client_is_connected(client));

        ASSERT_TRUE(aeron_archive_async_client_try_send_list_recording_request(client, 1, 10));
        auto controlResponse1 = pollUntilControlResponseReceived(client, listener, 1);
        ASSERT_EQ(1, controlResponse1->correlation_id);
        ASSERT_EQ(10, controlResponse1->relevant_id);
        ASSERT_EQ(aeron_archive_client_controlResponseCode_RECORDING_UNKNOWN, controlResponse1->code);
        ASSERT_EQ("", controlResponse1->error_message);

        ASSERT_TRUE(aeron_archive_async_client_try_send_max_recorded_position_request(client, 2, 11));
        auto controlResponse2 = pollUntilControlResponseReceived(client, listener, 2);
        ASSERT_EQ(2, controlResponse2->correlation_id);
        ASSERT_EQ(ARCHIVE_ERROR_CODE_UNKNOWN_RECORDING, controlResponse2->relevant_id);
        ASSERT_EQ(aeron_archive_client_controlResponseCode_ERROR, controlResponse2->code);
        ASSERT_EQ("unknown recording id: 11", controlResponse2->error_message);

        aeron_archive_replay_params_t replay_params;
        aeron_archive_replay_params_init(&replay_params);
        ASSERT_TRUE(aeron_archive_async_client_try_send_replay_request(client, 3, 12, "aeron:ipc", 2000, &replay_params));
        auto controlResponse3 = pollUntilControlResponseReceived(client, listener, 3);
        ASSERT_EQ(3, controlResponse3->correlation_id);
        ASSERT_EQ(ARCHIVE_ERROR_CODE_UNKNOWN_RECORDING, controlResponse3->relevant_id);
        ASSERT_EQ(aeron_archive_client_controlResponseCode_ERROR, controlResponse3->code);
        ASSERT_EQ("unknown recording id: 12", controlResponse3->error_message);

        connect(aeron_dir);
        recordData(&recording_id, &stop_position);

        ASSERT_TRUE(aeron_archive_async_client_try_send_list_recording_request(client, 4, recording_id));
        auto recordingDescriptor = pollUntilRecordingDescriptorReceived(client, listener);
        ASSERT_EQ(4, recordingDescriptor->correlation_id);
        ASSERT_EQ(recording_id, recordingDescriptor->recording_id);
        ASSERT_EQ(0, recordingDescriptor->start_position);
        ASSERT_EQ(stop_position, recordingDescriptor->stop_position);
        ASSERT_EQ(m_recordingStreamId, recordingDescriptor->stream_id);

        ASSERT_TRUE(aeron_archive_async_client_try_send_max_recorded_position_request(client, 5, recording_id));
        auto controlResponse5 = pollUntilControlResponseReceived(client, listener, 5);
        ASSERT_EQ(5, controlResponse5->correlation_id);
        ASSERT_EQ(stop_position, controlResponse5->relevant_id);
        ASSERT_EQ(aeron_archive_client_controlResponseCode_OK, controlResponse5->code);
        ASSERT_EQ("", controlResponse5->error_message);

        aeron_subscription_t *subscription = addSubscription(aeron.aeron(), "aeron:udp?endpoint=localhost:0", 2000);
        ASSERT_NE(subscription, nullptr) << aeron_errmsg();
        char uri_buffer[AERON_URI_MAX_LENGTH];
        ASSERT_LT(0, aeron_subscription_try_resolve_channel_endpoint_port(subscription, uri_buffer, sizeof(uri_buffer))) << aeron_errmsg();

        aeron_archive_replay_params_init(&replay_params);
        ASSERT_TRUE(aeron_archive_async_client_try_send_replay_request(client, 6, recording_id, uri_buffer, 2000, &replay_params));
        auto controlResponse6 = pollUntilControlResponseReceived(client, listener, 6);
        ASSERT_EQ(6, controlResponse6->correlation_id);
        ASSERT_EQ(aeron_archive_client_controlResponseCode_OK, controlResponse6->code);

        const auto session_id = static_cast<int32_t>(controlResponse6->relevant_id);
        aeron_image_t* image = nullptr;
        while ((image = aeron_subscription_image_by_session_id(subscription, session_id)) == nullptr)
        {
            aeron_archive_async_client_poll(client);
        }

        fragment_handler_clientd_t clientd = {};
        while (!aeron_image_is_end_of_stream(image))
        {
            if (0 == aeron_subscription_poll(subscription, fragment_handler, &clientd, 10))
            {
                aeron_archive_async_client_poll(client);
            }
        }
        ASSERT_EQ(clientd.received, 1);
        ASSERT_EQ(clientd.position, stop_position);

        aeron_subscription_close(subscription, nullptr, nullptr);

        disconnect();
    }

    pollUntil<int>("is disconnected", client, [&] { return listener.disconnectedCount; }, [](const int x) { return x == 1; });

    ASSERT_FALSE(aeron_archive_async_client_is_connected(client));
    ASSERT_FALSE(aeron_archive_async_client_try_send_max_recorded_position_request(client, 7, recording_id));

    {
        TestArchive testArchive = createTestArchive(aeron_dir);

        pollUntil<int>("is reconnected", client, [&] { return listener.connectedCount; }, [](const int x) { return x == 2; });
        ASSERT_TRUE(aeron_archive_async_client_is_connected(client));

        ASSERT_EQ(0, aeron_archive_async_client_close(client));
    }
}
