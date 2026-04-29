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

#include <algorithm>
#include <cinttypes>
#include <random>
#include <climits>
#include <vector>

#include "gtest/gtest.h"
#include "gmock/gmock-matchers.h"
#include "TestArchive.h"
#include "TestMediaDriver.h"
#include "TestStandaloneArchive.h"
#include "ArchiveClientTestUtils.h"

extern "C"
{
#include "aeron_common.h"
#include "aeron_counters.h"
#include "concurrent/aeron_logbuffer_descriptor.h"
#include "client/aeron_archive.h"
#include "client/aeron_archive_persistent_subscription.h"
#include "uri/aeron_uri_string_builder.h"
#include "util/aeron_env.h"
#include "protocol/aeron_udp_protocol.h"
#include "media/aeron_loss_generator.h"
#include "media/aeron_receive_channel_endpoint.h"
#include "aeron_stream_id_loss_generator.h"
#include "aeron_stream_id_frame_data_loss_generator.h"
#include "aeron_frame_data_loss_generator.h"
}

static const std::string IPC_CHANNEL = "aeron:ipc";
static const std::string MDC_PUBLICATION_CHANNEL = "aeron:udp?control=localhost:2000|control-mode=dynamic|fc=max";
static const std::string MDC_SUBSCRIPTION_CHANNEL = "aeron:udp?control=localhost:2000";
static const std::string UNICAST_CHANNEL = "aeron:udp?endpoint=localhost:2000";
static const std::string MULTICAST_CHANNEL = "aeron:udp?endpoint=224.20.30.39:40456|interface=localhost";
static const std::string LOCALHOST_CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8010";
static const std::string LOCALHOST_CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";
static const int32_t STREAM_ID = 1000;
static const int32_t ONE_KB_MESSAGE_SIZE = 1024 - AERON_DATA_HEADER_LENGTH;
static const int32_t FLOW_CONTROL_RECEIVERS_COUNTER_TYPE_ID = 17;

/*
 * RAII guards for aeron archive C handles. These guards close the resource
 * on scope exit so an early-returning ASSERT does not leak.
 *
 * Use release() when ownership has been transferred to another resource
 */
struct ArchiveContextGuard
{
    aeron_archive_context_t *p;
    explicit ArchiveContextGuard(aeron_archive_context_t *ctx) : p(ctx) {}
    ~ArchiveContextGuard() noexcept { if (p != nullptr) aeron_archive_context_close(p); }
    ArchiveContextGuard(const ArchiveContextGuard&) = delete;
    ArchiveContextGuard& operator=(const ArchiveContextGuard&) = delete;
    aeron_archive_context_t *release() noexcept { auto *r = p; p = nullptr; return r; }
};

struct PersistentSubscriptionContextGuard
{
    aeron_archive_persistent_subscription_context_t *p;
    explicit PersistentSubscriptionContextGuard(aeron_archive_persistent_subscription_context_t *ctx) : p(ctx) {}
    ~PersistentSubscriptionContextGuard() noexcept
    {
        if (p != nullptr) aeron_archive_persistent_subscription_context_close(p);
    }
    PersistentSubscriptionContextGuard(const PersistentSubscriptionContextGuard&) = delete;
    PersistentSubscriptionContextGuard& operator=(const PersistentSubscriptionContextGuard&) = delete;
    aeron_archive_persistent_subscription_context_t *release() noexcept { auto *r = p; p = nullptr; return r; }
};

struct PersistentSubscriptionGuard
{
    aeron_archive_persistent_subscription_t *p;
    explicit PersistentSubscriptionGuard(aeron_archive_persistent_subscription_t *ps) : p(ps) {}
    ~PersistentSubscriptionGuard() noexcept { if (p != nullptr) aeron_archive_persistent_subscription_close(p); }
    PersistentSubscriptionGuard(const PersistentSubscriptionGuard&) = delete;
    PersistentSubscriptionGuard& operator=(const PersistentSubscriptionGuard&) = delete;
    aeron_archive_persistent_subscription_t *release() noexcept { auto *r = p; p = nullptr; return r; }
};

class MessageCapturingFragmentHandler
{
public:
    static aeron_controlled_fragment_handler_action_t onFragment(
        void *clientd,
        const uint8_t *buffer,
        size_t length,
        aeron_header_t *header)
    {
        MessageCapturingFragmentHandler *const receiver = static_cast<MessageCapturingFragmentHandler *>(clientd);
        receiver->m_messages.emplace_back(buffer, buffer + length);
        return AERON_ACTION_CONTINUE;
    }

    size_t messageCount() const
    {
        return m_messages.size();
    }

    const std::vector<std::vector<uint8_t>>& messages() const
    {
        return m_messages;
    }

    void addMessage(const uint8_t *buffer, size_t length)
    {
        m_messages.emplace_back(buffer, buffer + length);
    }

private:
    std::vector<std::vector<uint8_t>> m_messages;
};

static auto makeControlledPoller(
    aeron_archive_persistent_subscription_t *ps,
    MessageCapturingFragmentHandler &handler,
    int fragment_limit = 10)
{
    return [ps, &handler, fragment_limit]
    {
        return aeron_archive_persistent_subscription_controlled_poll(
            ps, MessageCapturingFragmentHandler::onFragment, &handler, fragment_limit);
    };
}

static auto isLive(aeron_archive_persistent_subscription_t *ps)
{
    return [ps] { return aeron_archive_persistent_subscription_is_live(ps); };
}

static auto isReplaying(aeron_archive_persistent_subscription_t *ps)
{
    return [ps] { return aeron_archive_persistent_subscription_is_replaying(ps); };
}

static auto hasFailed(aeron_archive_persistent_subscription_t *ps)
{
    return [ps] { return aeron_archive_persistent_subscription_has_failed(ps); };
}

static auto makeUncontrolledPoller(
    aeron_archive_persistent_subscription_t *ps,
    MessageCapturingFragmentHandler &handler,
    int fragment_limit = 10)
{
    return [ps, &handler, fragment_limit]
    {
        return aeron_archive_persistent_subscription_poll(
            ps,
            [](void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *)
            {
                static_cast<MessageCapturingFragmentHandler *>(clientd)->addMessage(buffer, length);
            },
            &handler, fragment_limit);
    };
}

class TestListener
{
public:
    int error_count = 0;
    int last_errcode = 0;
    std::string last_error_message;
    int live_joined_count = 0;
    int live_left_count = 0;

    // Set before attachTo to snapshot join_difference inside on_live_joined. Polling after
    // is_live() is racy against later state transitions that reset the value.
    aeron_archive_persistent_subscription_t *ps_for_snapshot = nullptr;
    int64_t join_difference_at_join = INT64_MIN;

    void attachTo(aeron_archive_persistent_subscription_context_t *context)
    {
        aeron_archive_persistent_subscription_listener_t listener = { onLiveJoined, onLiveLeft, onError, this };
        aeron_archive_persistent_subscription_context_set_listener(context, &listener);
    }

private:
    static void onLiveJoined(void *clientd)
    {
        TestListener *listener = static_cast<TestListener*>(clientd);
        listener->live_joined_count++;
        if (nullptr != listener->ps_for_snapshot)
        {
            listener->join_difference_at_join =
                aeron_archive_persistent_subscription_join_difference(listener->ps_for_snapshot);
        }
    }

    static void onLiveLeft(void *clientd)
    {
        TestListener *listener = static_cast<TestListener*>(clientd);
        listener->live_left_count++;
    }

    static void onError(void *clientd, int errcode, const char *message)
    {
        TestListener *listener = static_cast<TestListener*>(clientd);
        listener->last_errcode = errcode;
        listener->last_error_message = message;
        listener->error_count++;
    }
};

class PrintingListener
{
    aeron_archive_persistent_subscription_listener_t m_listener;

    static void onLiveJoined(void *clientd)
    {
        std::cout << "live joined" << std::endl;
    }

    static void onLiveLeft(void *clientd)
    {
        std::cout << "live left" << std::endl;
    }

    static void onError(void *clientd, int errcode, const char *message)
    {
        std::cout << "error " << errcode << " " << message << std::endl;
    }

public:
    PrintingListener() : m_listener({onLiveJoined, onLiveLeft, onError, nullptr})
    {
    }

    aeron_archive_persistent_subscription_listener_t *listener()
    {
        return &m_listener;
    }
};

class PersistentPublication
{
public:
    explicit PersistentPublication(const std::string& aeronDir, const std::string& channel, const int32_t streamId)
    {
        aeron_archive_context_init(&m_archiveCtx);
        aeron_archive_context_set_aeron_directory_name(m_archiveCtx, aeronDir.c_str());
        aeron_archive_context_set_control_request_channel(m_archiveCtx, LOCALHOST_CONTROL_REQUEST_CHANNEL.c_str());
        aeron_archive_context_set_control_response_channel(m_archiveCtx, LOCALHOST_CONTROL_RESPONSE_CHANNEL.c_str());
        Credentials::defaultCredentials().configure(m_archiveCtx);

        aeron_archive_t *archive = nullptr;
        if (aeron_archive_connect(&archive, m_archiveCtx) < 0)
        {
            aeron_archive_context_close(m_archiveCtx);
            m_archiveCtx = nullptr;
            throw std::runtime_error("failed to connect to archive " + std::string(aeron_errmsg()));
        }

        aeron_exclusive_publication_t *publication = nullptr;
        if (aeron_archive_add_recorded_exclusive_publication(&publication, archive, channel.c_str(), streamId) < 0)
        {
            aeron_archive_close(archive);
            aeron_archive_context_close(m_archiveCtx);
            throw std::runtime_error("failed to add recorded publication " + std::string(aeron_errmsg()));
        }

        aeron_publication_constants_t constants;
        aeron_exclusive_publication_constants(publication, &constants);
        m_maxPayloadLength = constants.max_payload_length;

        aeron_t *aeron = aeron_archive_context_get_aeron(aeron_archive_get_archive_context(archive));
        aeron_counters_reader_t *counters_reader = aeron_counters_reader(aeron);
        const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        int32_t rec_pos_id;
        while (AERON_NULL_COUNTER_ID == (rec_pos_id = aeron_archive_recording_pos_find_counter_id_by_session_id(
            counters_reader, constants.session_id)))
        {
            if (std::chrono::steady_clock::now() >= deadline)
            {
                aeron_archive_close(archive);
                aeron_archive_context_close(m_archiveCtx);
                throw std::runtime_error("timed out waiting for recording position counter");
            }
            std::this_thread::yield();
        }
        int64_t recording_id = aeron_archive_recording_pos_get_recording_id(counters_reader, rec_pos_id);

        m_archive = archive;
        m_publication = publication;
        m_countersReader = counters_reader;
        m_recordingId = recording_id;
        m_recPosId = rec_pos_id;
    }

    ~PersistentPublication()
    {
        aeron_archive_close(m_archive);
        aeron_archive_context_close(m_archiveCtx);
    }

    int64_t recordingId() const
    {
        return m_recordingId;
    }

    int32_t maxPayloadLength() const
    {
        return m_maxPayloadLength;
    }

    aeron_archive_t *archive() const
    {
        return m_archive;
    }

    aeron_exclusive_publication_t *publication() const
    {
        return m_publication;
    }

    int64_t stop()
    {
        if (aeron_archive_stop_recording_exclusive_publication(m_archive, m_publication) < 0)
        {
            throw std::runtime_error("failed to stop recording " + std::string(aeron_errmsg()));
        }

        const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        int64_t stop_position = AERON_NULL_VALUE;
        while (stop_position == AERON_NULL_VALUE)
        {
            aeron_archive_get_stop_position(&stop_position, m_archive, m_recordingId);
            if (std::chrono::steady_clock::now() >= deadline)
            {
                throw std::runtime_error("timed out waiting for stop position");
            }
            std::this_thread::yield();
        }

        return stop_position;
    }

    void persist(const std::vector<std::vector<uint8_t>>& messages) const
    {
        if (messages.empty())
        {
            return;
        }

        const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);

        int64_t position = 0;
        for (const std::vector<uint8_t>& message : messages)
        {
            while (true)
            {
                position = aeron_exclusive_publication_offer(
                    m_publication,
                    message.data(),
                    message.size(),
                    nullptr,
                    nullptr);

                if (position > 0)
                {
                    break;
                }

                if (std::chrono::steady_clock::now() >= deadline)
                {
                    throw std::runtime_error(
                        "persist timed out, offer returned " + std::to_string(position));
                }

                std::this_thread::yield();
            }
        }

        while (*aeron_counters_reader_addr(m_countersReader, m_recPosId) < position)
        {
            if (std::chrono::steady_clock::now() >= deadline)
            {
                throw std::runtime_error("persist timed out waiting for recording position");
            }

            std::this_thread::yield();
        }
    }

    void offer(const std::vector<std::vector<uint8_t>>& messages) const
    {
        const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);

        for (const std::vector<uint8_t>& message : messages)
        {
            while (true)
            {
                const int64_t result = aeron_exclusive_publication_offer(
                    m_publication,
                    message.data(),
                    message.size(),
                    nullptr,
                    nullptr);

                if (result > 0)
                {
                    break;
                }

                // CLOSED / MAX_POSITION_EXCEEDED / ERROR are terminal — fail fast. NOT_CONNECTED
                // is transient and expected when the caller offers right after the PS transitions
                // to LIVE: the publication's subscriber-image accounting briefly lags the PS's
                // own live-state signal, so we retry within the 30s deadline rather than throw.
                if (result == AERON_PUBLICATION_CLOSED ||
                    result == AERON_PUBLICATION_MAX_POSITION_EXCEEDED ||
                    result == AERON_PUBLICATION_ERROR)
                {
                    throw std::runtime_error("offer returned " + std::to_string(result));
                }

                if (std::chrono::steady_clock::now() >= deadline)
                {
                    throw std::runtime_error(
                        "offer timed out, last result " + std::to_string(result));
                }

                std::this_thread::yield();
            }
        }
    }

    static PersistentPublication resume(
        const std::string& aeronDir,
        const std::string& channel,
        const int32_t streamId,
        const int64_t recordingId)
    {
        aeron_archive_context_t *archiveCtx;
        aeron_archive_context_init(&archiveCtx);
        aeron_archive_context_set_aeron_directory_name(archiveCtx, aeronDir.c_str());
        aeron_archive_context_set_control_request_channel(archiveCtx, LOCALHOST_CONTROL_REQUEST_CHANNEL.c_str());
        aeron_archive_context_set_control_response_channel(archiveCtx, LOCALHOST_CONTROL_RESPONSE_CHANNEL.c_str());
        Credentials::defaultCredentials().configure(archiveCtx);

        aeron_archive_t *archive = nullptr;
        if (aeron_archive_connect(&archive, archiveCtx) < 0)
        {
            aeron_archive_context_close(archiveCtx);
            throw std::runtime_error("failed to connect to archive " + std::string(aeron_errmsg()));
        }

        // List the recording to get its stop position and initial term params
        struct RecordingInfo
        {
            int64_t stop_position;
            int32_t initial_term_id;
            int32_t term_buffer_length;
        } info = {};

        int32_t count = 0;
        if (aeron_archive_list_recording(
            &count,
            archive,
            recordingId,
            [](aeron_archive_recording_descriptor_t *descriptor, void *clientd)
            {
                RecordingInfo *i = static_cast<RecordingInfo *>(clientd);
                i->stop_position = descriptor->stop_position;
                i->initial_term_id = descriptor->initial_term_id;
                i->term_buffer_length = descriptor->term_buffer_length;
            },
            &info) < 0)
        {
            aeron_archive_close(archive);
            aeron_archive_context_close(archiveCtx);
            throw std::runtime_error("failed to list recording " + std::string(aeron_errmsg()));
        }

        // Build channel with initial position so the new publication continues from stop position
        aeron_uri_string_builder_t builder;
        aeron_uri_string_builder_init_on_string(&builder, channel.c_str());
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_INITIAL_TERM_ID_KEY, info.initial_term_id);

        const int32_t term_id = aeron_logbuffer_compute_term_id_from_position(
            info.stop_position, aeron_number_of_trailing_zeroes(info.term_buffer_length), info.initial_term_id);
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_TERM_ID_KEY, term_id);

        const int32_t term_offset = (int32_t)(info.stop_position & (info.term_buffer_length - 1));
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_TERM_OFFSET_KEY, term_offset);

        aeron_uri_string_builder_put_int32(&builder, AERON_URI_TERM_LENGTH_KEY, info.term_buffer_length);

        char channel_uri[AERON_URI_MAX_LENGTH];
        aeron_uri_string_builder_sprint(&builder, channel_uri, sizeof(channel_uri));
        aeron_uri_string_builder_close(&builder);

        aeron_t *aeron = aeron_archive_context_get_aeron(aeron_archive_get_archive_context(archive));

        aeron_exclusive_publication_t *publication = nullptr;
        aeron_async_add_exclusive_publication_t *async_add = nullptr;
        if (aeron_async_add_exclusive_publication(&async_add, aeron, channel_uri, streamId) < 0)
        {
            aeron_archive_close(archive);
            aeron_archive_context_close(archiveCtx);
            throw std::runtime_error("failed to add publication " + std::string(aeron_errmsg()));
        }
        {
            const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
            while (aeron_async_add_exclusive_publication_poll(&publication, async_add) == 0)
            {
                if (std::chrono::steady_clock::now() >= deadline)
                {
                    aeron_archive_close(archive);
                    aeron_archive_context_close(archiveCtx);
                    throw std::runtime_error("timed out waiting for exclusive publication");
                }
                std::this_thread::yield();
            }
        }
        if (publication == nullptr)
        {
            aeron_archive_close(archive);
            aeron_archive_context_close(archiveCtx);
            throw std::runtime_error("failed to create publication " + std::string(aeron_errmsg()));
        }

        int64_t subscription_id;
        if (aeron_archive_extend_recording(
            &subscription_id, archive, recordingId, channel_uri, streamId, AERON_ARCHIVE_SOURCE_LOCATION_LOCAL, false) < 0)
        {
            aeron_archive_close(archive);
            aeron_archive_context_close(archiveCtx);
            throw std::runtime_error("failed to extend recording " + std::string(aeron_errmsg()));
        }

        aeron_counters_reader_t *counters_reader = aeron_counters_reader(aeron);
        aeron_publication_constants_t constants;
        aeron_exclusive_publication_constants(publication, &constants);

        const std::chrono::steady_clock::time_point counter_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        int32_t rec_pos_id;
        while (AERON_NULL_COUNTER_ID == (rec_pos_id = aeron_archive_recording_pos_find_counter_id_by_session_id(
            counters_reader, constants.session_id)))
        {
            if (std::chrono::steady_clock::now() >= counter_deadline)
            {
                aeron_archive_close(archive);
                aeron_archive_context_close(archiveCtx);
                throw std::runtime_error("timed out waiting for recording position counter in resume");
            }
            std::this_thread::yield();
        }

        return PersistentPublication(archive, archiveCtx, publication, counters_reader, recordingId, rec_pos_id,
            constants.max_payload_length);
    }

    int64_t receiverCount() const
    {
        aeron_publication_constants_t constants;
        aeron_exclusive_publication_constants(m_publication, &constants);

        int32_t counter_id = aeron_counters_reader_find_by_type_id_and_registration_id(
            m_countersReader,
            FLOW_CONTROL_RECEIVERS_COUNTER_TYPE_ID,
            constants.registration_id);

        if (counter_id == AERON_NULL_COUNTER_ID)
        {
            return -1;
        }

        return *aeron_counters_reader_addr(m_countersReader, counter_id);
    }

private:
    PersistentPublication(
        aeron_archive_t *archive,
        aeron_archive_context_t *archiveCtx,
        aeron_exclusive_publication_t *publication,
        aeron_counters_reader_t *countersReader,
        int64_t recordingId,
        int32_t recPosId,
        int32_t maxPayloadLength)
        : m_maxPayloadLength(maxPayloadLength),
          m_archiveCtx(archiveCtx),
          m_archive(archive),
          m_publication(publication),
          m_countersReader(countersReader),
          m_recordingId(recordingId),
          m_recPosId(recPosId)
    {
    }

    int32_t m_maxPayloadLength;
    aeron_archive_context_t *m_archiveCtx = nullptr;
    aeron_archive_t *m_archive;
    aeron_exclusive_publication_t *m_publication;
    aeron_counters_reader_t *m_countersReader;
    int64_t m_recordingId;
    int32_t m_recPosId;
};

std::string to_hex(const std::vector<unsigned char>& vector)
{
    std::string s;
    s.resize(vector.size() * 2);
    char *ptr = &s.front();
    for (const unsigned char c : vector)
    {
        snprintf(ptr, 3, "%02x", c);
        ptr += 2;
    }
    return s;
}

testing::AssertionResult MessagesEq(
    const std::vector<std::vector<uint8_t>>& expected,
    const std::vector<std::vector<uint8_t>>& actual)
{
    bool eq = expected.size() == actual.size();

    if (eq)
    {
        for (size_t i = 0; i < expected.size(); i++)
        {
            if (expected[i] != actual[i])
            {
                eq = false;
                break;
            }
        }
    }

    if (eq)
    {
        return testing::AssertionSuccess();
    }

    std::string description;
    description += "\nexpected " + std::to_string(expected.size()) + " messages:";
    for (const std::vector<uint8_t>& message : expected)
    {
        description += "\n" + to_hex(message);
    }
    description += "\n\nbut got " + std::to_string(actual.size()) + " messages:";
    for (const std::vector<uint8_t>& message : actual)
    {
        description += "\n" + to_hex(message);
    }

    return testing::AssertionFailure() << description;
}

class AeronArchivePersistentSubscriptionTest : public testing::Test
{
protected:
    const std::string m_aeronDir;

    AeronArchivePersistentSubscriptionTest()
        : m_aeronDir(defaultAeronDir())
    {
    }

    static std::string defaultAeronDir()
    {
        char aeron_dir[AERON_MAX_PATH];
        aeron_default_path(aeron_dir, sizeof(aeron_dir));
        return {aeron_dir};
    }

    static TestArchive createArchive(const std::string& aeronDir)
    {
        std::unordered_map<std::string, std::string> properties = TestArchive::defaultProperties();
        properties["aeron.timer.interval"] = "100ms";
        properties["aeron.image.liveness.timeout"] = "2s";
        properties["aeron.untethered.window.limit.timeout"] = "1s";
        properties["aeron.untethered.linger.timeout"] = "1s";
        properties["aeron.publication.linger.timeout"] = "1s";

        return {
            aeronDir,
            ARCHIVE_DIR,
            std::cout,
            LOCALHOST_CONTROL_REQUEST_CHANNEL,
            "aeron:udp?endpoint=localhost:0",
            1,
            10,
            properties
        };
    }

    static aeron_archive_context_t *createArchiveContext()
    {
        aeron_archive_context_t *ctx;
        aeron_archive_context_init(&ctx);
        aeron_archive_context_set_control_request_channel(ctx, LOCALHOST_CONTROL_REQUEST_CHANNEL.c_str());
        aeron_archive_context_set_control_response_channel(ctx, LOCALHOST_CONTROL_RESPONSE_CHANNEL.c_str());
        Credentials::defaultCredentials().configure(ctx);
        return ctx;
    }

    static aeron_archive_persistent_subscription_context_t *createPersistentSubscriptionContext(
        aeron_t *aeron,
        aeron_archive_context_t *archiveContext,
        const int64_t recordingId,
        const std::string& liveChannel,
        const int32_t liveStreamId,
        const std::string& replayChannel,
        const int32_t replayStreamId,
        const int64_t startPosition)
    {
        aeron_archive_persistent_subscription_context_t *ctx;
        aeron_archive_persistent_subscription_context_init(&ctx);
        aeron_archive_persistent_subscription_context_set_aeron(ctx, aeron);
        aeron_archive_persistent_subscription_context_set_archive_context(ctx, archiveContext);
        aeron_archive_persistent_subscription_context_set_recording_id(ctx, recordingId);
        aeron_archive_persistent_subscription_context_set_live_channel(ctx, liveChannel.c_str());
        aeron_archive_persistent_subscription_context_set_live_stream_id(ctx, liveStreamId);
        aeron_archive_persistent_subscription_context_set_replay_channel(ctx, replayChannel.c_str());
        aeron_archive_persistent_subscription_context_set_replay_stream_id(ctx, replayStreamId);
        aeron_archive_persistent_subscription_context_set_start_position(ctx, startPosition);
        return ctx;
    }

    static aeron_archive_persistent_subscription_context_t *createDefaultPersistentSubscriptionContext(
        aeron_t *aeron,
        aeron_archive_context_t *archiveContext,
        const int64_t recordingId)
    {
        return createPersistentSubscriptionContext(
            aeron,
            archiveContext,
            recordingId,
            IPC_CHANNEL,
            STREAM_ID,
            "aeron:udp?endpoint=localhost:0",
            -5,
            0);
    }

    std::vector<std::vector<uint8_t>> generateRandomMessages(const int count)
    {
        std::vector<std::vector<uint8_t>> messages(count);

        for (int i = 0; i < count; i++)
        {
            std::vector<uint8_t> *const message = &messages[i];
            const int length = m_lengthGenerator(m_randomEngine);
            message->reserve(std::max(1, length)); // at least 1 so that we don't pass a nullptr to offer
            for (int j = 0; j < length; j++)
            {
                message->push_back(m_byteGenerator(m_randomEngine));
            }
        }

        return messages;
    }

    std::vector<uint8_t> generateRandomBytes(const int count)
    {
        std::vector<uint8_t> bytes(count);
        for (int i = 0; i < count; i++)
        {
            bytes[i] = m_byteGenerator(m_randomEngine);
        }
        return bytes;
    }

    std::vector<std::vector<uint8_t>> generateFixedMessages(const int count, const int size)
    {
        std::vector<std::vector<uint8_t>> messages(count);
        for (int i = 0; i < count; i++)
        {
            messages[i] = generateRandomBytes(size);
        }
        return messages;
    }

    static void executeUntil(
        const std::string& label,
        const std::function<int()>& action,
        const std::function<bool()>& predicate,
        int timeout_seconds = 15)
    {
        const std::chrono::steady_clock::time_point deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(timeout_seconds);
        while (true)
        {
            if (std::chrono::steady_clock::now() >= deadline)
            {
                FAIL() << "timed out waiting for '" << label << "'";
            }

            const int result = action();

            if (result < 0)
            {
                FAIL() << "error occurred while waiting for '" << label << "': " << aeron_errmsg();
            }

            if (predicate())
            {
                break;
            }

            if (result == 0)
            {
                std::this_thread::yield();
            }
        }
    }

    static void waitUntil(
        const std::string& label,
        const std::function<bool()>& predicate,
        int timeout_seconds = 15)
    {
        executeUntil(label, [] { return 0; }, predicate, timeout_seconds);
    }

    // Defined out-of-class below because it depends on
    // EmbeddedMediaDriverWithLossGenerator, which is declared later in this
    // file.
    void shouldHandleReplayImageBecomingUnavailable(int replayableMessageCount);

private:
    std::random_device m_randomDevice;
    std::default_random_engine m_randomEngine = std::default_random_engine(m_randomDevice());
    std::uniform_int_distribution<> m_lengthGenerator = std::uniform_int_distribution<>(0, 2048);
    std::uniform_int_distribution<unsigned short> m_byteGenerator = std::uniform_int_distribution<unsigned short>(0, UINT8_MAX);
};

TEST_F(AeronArchivePersistentSubscriptionTest, shouldAutoAllocateCounters)
{
    TestArchive archive = createArchive(m_aeronDir);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        123);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    aeron_counters_reader_t *counters_reader = aeron_counters_reader(aeron.aeron());

    int32_t state_counter_id = AERON_NULL_COUNTER_ID;
    int32_t join_difference_counter_id = AERON_NULL_COUNTER_ID;
    int32_t live_left_counter_id = AERON_NULL_COUNTER_ID;
    int32_t live_joined_counter_id = AERON_NULL_COUNTER_ID;

    int32_t *counter_id_ptrs[] = {
        &state_counter_id, &join_difference_counter_id,
        &live_left_counter_id, &live_joined_counter_id };

    aeron_counters_reader_foreach_counter(
        counters_reader,
        [](int64_t value, int32_t id, int32_t type_id,
           const uint8_t *, size_t, const char *, size_t, void *clientd)
        {
            int32_t **ptrs = static_cast<int32_t **>(clientd);
            if (type_id == AERON_PERSISTENT_SUBSCRIPTION_STATE_TYPE_ID)
                *ptrs[0] = id;
            else if (type_id == AERON_PERSISTENT_SUBSCRIPTION_JOIN_DIFFERENCE_TYPE_ID)
                *ptrs[1] = id;
            else if (type_id == AERON_PERSISTENT_SUBSCRIPTION_LIVE_LEFT_COUNT_TYPE_ID)
                *ptrs[2] = id;
            else if (type_id == AERON_PERSISTENT_SUBSCRIPTION_LIVE_JOINED_COUNT_TYPE_ID)
                *ptrs[3] = id;
        },
        counter_id_ptrs);

    ASSERT_NE(AERON_NULL_COUNTER_ID, state_counter_id);
    ASSERT_NE(AERON_NULL_COUNTER_ID, join_difference_counter_id);
    ASSERT_NE(AERON_NULL_COUNTER_ID, live_left_counter_id);
    ASSERT_NE(AERON_NULL_COUNTER_ID, live_joined_counter_id);

    ASSERT_EQ(0, *aeron_counters_reader_addr(counters_reader, state_counter_id));
    ASSERT_EQ(INT64_MIN, *aeron_counters_reader_addr(counters_reader, join_difference_counter_id));
    ASSERT_EQ(0, *aeron_counters_reader_addr(counters_reader, live_joined_counter_id));
    ASSERT_EQ(0, *aeron_counters_reader_addr(counters_reader, live_left_counter_id));

}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldUseUserProvidedCounters)
{
    TestArchive archive = createArchive(m_aeronDir);

    AeronResource aeron(m_aeronDir);

    // Create 4 user-provided counters
    auto allocate_counter = [&](const char *label) -> aeron_counter_t *
    {
        aeron_async_add_counter_t *async = nullptr;
        EXPECT_EQ(0, aeron_async_add_counter(
            &async, aeron.aeron(), 999, nullptr, 0, label, strlen(label))) << aeron_errmsg();
        aeron_counter_t *counter = nullptr;
        while (nullptr == counter)
        {
            int result = aeron_async_add_counter_poll(&counter, async);
            EXPECT_GE(result, 0) << aeron_errmsg();
            if (0 == result) std::this_thread::yield();
        }
        return counter;
    };

    aeron_counter_t *state_counter = allocate_counter("test-state");
    aeron_counter_t *join_difference_counter = allocate_counter("test-join-difference");
    aeron_counter_t *live_left_counter = allocate_counter("test-to-replay");
    aeron_counter_t *live_joined_counter = allocate_counter("test-to-live");

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        123);

    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_set_state_counter(context, state_counter));
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_set_join_difference_counter(context, join_difference_counter));
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_set_live_left_counter(context, live_left_counter));
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_set_live_joined_counter(context, live_joined_counter));

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    ASSERT_EQ(0, *aeron_counter_addr(state_counter));
    ASSERT_EQ(INT64_MIN, *aeron_counter_addr(join_difference_counter));
    ASSERT_EQ(0, *aeron_counter_addr(live_joined_counter));
    ASSERT_EQ(0, *aeron_counter_addr(live_left_counter));

    // Counters are closed by context_close (called from persistent_subscription_close)
    ps_guard.release();
    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();

    ASSERT_TRUE(aeron_counter_is_closed(state_counter));
    ASSERT_TRUE(aeron_counter_is_closed(join_difference_counter));
    ASSERT_TRUE(aeron_counter_is_closed(live_left_counter));
    ASSERT_TRUE(aeron_counter_is_closed(live_joined_counter));

    archive_ctx_guard.release();
    aeron_archive_context_close(archive_ctx);
}

struct FragmentLimitAndChannel
{
    int fragment_limit;
    std::string pub_channel;
    std::string sub_channel;
};

class AeronArchivePersistentSubscriptionReplayAndJoinLiveTest
    : public AeronArchivePersistentSubscriptionTest,
      public testing::WithParamInterface<FragmentLimitAndChannel>
{
};

INSTANTIATE_TEST_SUITE_P(
    ,
    AeronArchivePersistentSubscriptionReplayAndJoinLiveTest,
    testing::Values(
        FragmentLimitAndChannel{1, IPC_CHANNEL, IPC_CHANNEL},
        FragmentLimitAndChannel{10, IPC_CHANNEL, IPC_CHANNEL},
        FragmentLimitAndChannel{INT_MAX, IPC_CHANNEL, IPC_CHANNEL},
        FragmentLimitAndChannel{1, UNICAST_CHANNEL, UNICAST_CHANNEL},
        FragmentLimitAndChannel{10, UNICAST_CHANNEL, UNICAST_CHANNEL},
        FragmentLimitAndChannel{INT_MAX, UNICAST_CHANNEL, UNICAST_CHANNEL},
        FragmentLimitAndChannel{1, MULTICAST_CHANNEL, MULTICAST_CHANNEL},
        FragmentLimitAndChannel{10, MULTICAST_CHANNEL, MULTICAST_CHANNEL},
        FragmentLimitAndChannel{INT_MAX, MULTICAST_CHANNEL, MULTICAST_CHANNEL},
        FragmentLimitAndChannel{1, MDC_PUBLICATION_CHANNEL, MDC_SUBSCRIPTION_CHANNEL},
        FragmentLimitAndChannel{10, MDC_PUBLICATION_CHANNEL, MDC_SUBSCRIPTION_CHANNEL},
        FragmentLimitAndChannel{INT_MAX, MDC_PUBLICATION_CHANNEL, MDC_SUBSCRIPTION_CHANNEL}));

// Verifies that a persistent subscription replays an existing recording and then transitions
// to live. Five messages are persisted before the subscription is created. The subscription
// is confirmed to be replaying after receiving the first message. Once all five messages are
// received, the subscription transitions to live and on_live_joined is invoked exactly once.
// Five further messages are then published and the subscription is expected to receive all
// ten messages in order.
TEST_P(AeronArchivePersistentSubscriptionReplayAndJoinLiveTest, shouldReplayExistingRecordingThenJoinLive)
{
    TestArchive archive = createArchive(m_aeronDir);

    const int fragment_limit = GetParam().fragment_limit;
    const std::string &param_pub_channel = GetParam().pub_channel;
    const std::string &sub_channel = GetParam().sub_channel;

    PersistentPublication persistent_publication(m_aeronDir, param_pub_channel, STREAM_ID);

    const std::vector<std::vector<uint8_t>> payloads = generateRandomMessages(5);
    persistent_publication.persist(payloads);

    AeronResource aeron(m_aeronDir);

    int live_joined_count = 0;
    aeron_archive_persistent_subscription_listener_t listener = {};
    listener.clientd = &live_joined_count;
    listener.on_live_joined = [](void *clientd)
    {
        (*static_cast<int *>(clientd))++;
    };

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_live_channel(context, sub_channel.c_str());
    aeron_archive_persistent_subscription_context_set_listener(context, &listener);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler, fragment_limit);

    ASSERT_EQ(0, live_joined_count);

    executeUntil(
        "receives first message",
        [&]
        {
            return aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                MessageCapturingFragmentHandler::onFragment,
                &handler,
                1);
        },
        [&] { return handler.messageCount() == 1; });

    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    executeUntil(
        "receives all payloads",
        poller,
        [&] { return handler.messageCount() == payloads.size(); });

    ASSERT_EQ(payloads, handler.messages());

    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));

    ASSERT_EQ(1, live_joined_count);
    ASSERT_EQ(payloads.size(), handler.messageCount());

    const std::vector<std::vector<uint8_t>> payloads2 = generateRandomMessages(5);
    persistent_publication.persist(payloads2);

    executeUntil(
        "receives all live messages",
        poller,
        [&] { return handler.messageCount() == payloads.size() + payloads2.size(); });

    ASSERT_TRUE(aeron_archive_persistent_subscription_is_live(persistent_subscription));
    ASSERT_FALSE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    std::vector<std::vector<uint8_t>> all_messages;
    all_messages.insert(all_messages.end(), payloads.begin(), payloads.end());
    all_messages.insert(all_messages.end(), payloads2.begin(), payloads2.end());
    ASSERT_EQ(all_messages, handler.messages());

}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldStartFromLiveWithNoInitialReplayIfRequested)
{
    TestArchive archive = createArchive(m_aeronDir);

    const std::vector<std::vector<uint8_t>> messages = generateRandomMessages(3);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_start_position(
        context, AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));
    ASSERT_EQ(0, handler.messageCount());

    const std::vector<std::vector<uint8_t>> live_messages = generateRandomMessages(3);
    persistent_publication.offer(live_messages);

    executeUntil(
        "receives all live messages",
        poller,
        [&] { return handler.messageCount() == live_messages.size(); });
    ASSERT_EQ(live_messages, handler.messages());

}

// Verifies that a persistent subscription transitions immediately to live when there
// are no recorded messages to replay. No messages are published before the subscription
// is created. The subscription is expected to become live without receiving any messages.
// Five messages are then published and the subscription is expected to receive all of them.
TEST_F(AeronArchivePersistentSubscriptionTest, shouldStartFromLiveWhenThereIsNoDataToReplay)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_live_channel(context, MDC_SUBSCRIPTION_CHANNEL.c_str());

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));

    ASSERT_EQ(0, handler.messageCount());

    const std::vector<std::vector<uint8_t>> messages = generateRandomMessages(5);
    persistent_publication.persist(messages);

    executeUntil(
        "receives all messages",
        poller,
        [&] { return handler.messageCount() == messages.size(); });

    ASSERT_EQ(messages, handler.messages());

}

// Verifies that a persistent subscription configured with AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_START
// replays from the recording's actual start position. The channel is configured with an initial
// term offset of 1024, so the recording starts at position 1024 rather than 0. Five messages are
// persisted before the subscription is created. The subscription is expected to replay all five
// messages and transition to live. Three further messages are then published and the subscription
// is expected to receive all eight messages in order.
TEST_F(AeronArchivePersistentSubscriptionTest, shouldReplayFromRecordingStartPositionWhenStartingFromStart)
{
    TestArchive archive = createArchive(m_aeronDir);

    const std::string channel = "aeron:ipc?init-term-id=0|term-id=0|term-offset=1024|term-length=65536";

    PersistentPublication persistent_publication(m_aeronDir, channel, STREAM_ID);

    const std::vector<std::vector<uint8_t>> old_messages = generateRandomMessages(5);
    persistent_publication.persist(old_messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_start_position(
        context, AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_START);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));

    const std::vector<std::vector<uint8_t>> new_messages = generateRandomMessages(3);
    persistent_publication.persist(new_messages);

    executeUntil(
        "receives all messages",
        poller,
        [&] { return handler.messageCount() == old_messages.size() + new_messages.size(); });

    std::vector<std::vector<uint8_t>> all_messages;
    all_messages.insert(all_messages.end(), old_messages.begin(), old_messages.end());
    all_messages.insert(all_messages.end(), new_messages.begin(), new_messages.end());
    ASSERT_EQ(all_messages, handler.messages());

}

// Verifies that a persistent subscription configured with AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE
// does not replay messages from the archive. Five messages are persisted before the subscription
// is created. The subscription becomes live without receiving any of those messages, and on_live_joined
// is invoked exactly once. Three further messages are then published and the subscription is
// expected to receive only those.
TEST_F(AeronArchivePersistentSubscriptionTest, shouldNotReplayOldMessagesWhenStartingFromLive)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> old_messages = generateRandomMessages(5);
    persistent_publication.persist(old_messages);

    AeronResource aeron(m_aeronDir);

    int live_joined_count = 0;
    aeron_archive_persistent_subscription_listener_t listener = {};
    listener.clientd = &live_joined_count;
    listener.on_live_joined = [](void *clientd)
    {
        (*static_cast<int *>(clientd))++;
    };

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_start_position(
        context, AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE);
    aeron_archive_persistent_subscription_context_set_listener(context, &listener);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    ASSERT_EQ(0, live_joined_count);

    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));

    ASSERT_EQ(1, live_joined_count);
    ASSERT_EQ(0, handler.messageCount());

    const std::vector<std::vector<uint8_t>> new_messages = generateRandomMessages(3);
    persistent_publication.persist(new_messages);

    executeUntil(
        "receives all new messages",
        poller,
        [&] { return handler.messageCount() == new_messages.size(); });

    ASSERT_EQ(new_messages, handler.messages());

}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldReplayFromSpecificMidRecordingPosition)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> first_messages = generateFixedMessages(2, 128);
    persistent_publication.persist(first_messages);

    // Get mid-recording position via a separate archive connection
    aeron_archive_context_t *position_archive_ctx = createArchiveContext();
    aeron_archive_t *pos_archive = nullptr;
    ASSERT_EQ(0, aeron_archive_connect(&pos_archive, position_archive_ctx)) << aeron_errmsg();
    ASSERT_NE(nullptr, pos_archive);

    int64_t mid_position = 0;
    ASSERT_EQ(0, aeron_archive_get_recording_position(&mid_position, pos_archive, persistent_publication.recordingId())) << aeron_errmsg();
    ASSERT_GT(mid_position, 0);

    aeron_archive_close(pos_archive);
    aeron_archive_context_close(position_archive_ctx);

    const std::vector<std::vector<uint8_t>> remaining_messages = generateFixedMessages(3, 128);
    persistent_publication.persist(remaining_messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId(),
        IPC_CHANNEL,
        STREAM_ID,
        "aeron:udp?endpoint=localhost:0",
        -5,
        mid_position);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));
    ASSERT_EQ(remaining_messages.size(), handler.messageCount());
    ASSERT_TRUE(MessagesEq(remaining_messages, handler.messages()));

}

// This test verifies that a persistent subscription can catchup from archive and then continues
// with the live stream.
// So first a set of messages are written to the archive. And the persistent subscription is expected
// to process all these messages and switch to live. Then additional set of messages are send and
// the persistent subscription is expected to process these as well.
TEST_F(AeronArchivePersistentSubscriptionTest, shouldTransitionFromReplayToLiveWhileLiveIsAdvancing)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> messages = generateRandomMessages(5);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil(
        "receives first message",
        [&]
        {
            return aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                MessageCapturingFragmentHandler::onFragment,
                &handler,
                1);
        },
        [&] { return handler.messageCount() == 1; });

    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    executeUntil(
        "receives all messages",
        poller,
        [&] { return handler.messageCount() == messages.size(); });

    ASSERT_EQ(messages, handler.messages());

    const std::vector<std::vector<uint8_t>> messages2 = generateRandomMessages(1);
    persistent_publication.persist(messages2);

    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));

    ASSERT_FALSE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

}

class AeronArchivePersistentSubscriptionCatchupTest
    : public AeronArchivePersistentSubscriptionTest,
      public testing::WithParamInterface<int>
{
};

INSTANTIATE_TEST_SUITE_P(
    ,
    AeronArchivePersistentSubscriptionCatchupTest,
    testing::Values(1, 10));

// Verifies that a persistent subscription catches up on replay before transitioning to live.
// Five messages are persisted before the subscription is created. The subscription is confirmed
// to be replaying after receiving the first message. While still replaying, 25 further messages
// are persisted. The subscription is expected to catch up on all 30 messages before transitioning
// to live, with on_live_joined invoked exactly once. Five more messages are then published and
// the subscription is expected to receive all 35 messages in order. The test is parameterised
// over fragment limit.
TEST_P(AeronArchivePersistentSubscriptionCatchupTest, shouldCatchupOnReplayBeforeSwitchingToLive)
{
    TestArchive archive = createArchive(m_aeronDir);

    const int fragment_limit = GetParam();

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> payloads = generateRandomMessages(5);
    persistent_publication.persist(payloads);

    AeronResource aeron(m_aeronDir);

    int live_joined_count = 0;
    aeron_archive_persistent_subscription_listener_t listener = {};
    listener.clientd = &live_joined_count;
    listener.on_live_joined = [](void *clientd)
    {
        (*static_cast<int *>(clientd))++;
    };

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_listener(context, &listener);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler, fragment_limit);

    executeUntil(
        "receives first message",
        [&]
        {
            return aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                MessageCapturingFragmentHandler::onFragment,
                &handler,
                1);
        },
        [&] { return handler.messageCount() == 1; });

    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    const std::vector<std::vector<uint8_t>> payloads2 = generateRandomMessages(25);
    persistent_publication.persist(payloads2);

    executeUntil(
        "receives all payloads and payloads2",
        poller,
        [&] { return handler.messageCount() == payloads.size() + payloads2.size(); });

    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));

    ASSERT_EQ(1, live_joined_count);
    ASSERT_EQ(payloads.size() + payloads2.size(), handler.messageCount());

    const std::vector<std::vector<uint8_t>> payloads3 = generateRandomMessages(5);
    persistent_publication.persist(payloads3);

    executeUntil(
        "receives all live messages",
        poller,
        [&]
        {
            return handler.messageCount() == payloads.size() + payloads2.size() + payloads3.size() &&
                   aeron_archive_persistent_subscription_is_live(persistent_subscription);
        });

    ASSERT_TRUE(aeron_archive_persistent_subscription_is_live(persistent_subscription));
    ASSERT_FALSE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    std::vector<std::vector<uint8_t>> all_messages;
    all_messages.insert(all_messages.end(), payloads.begin(), payloads.end());
    all_messages.insert(all_messages.end(), payloads2.begin(), payloads2.end());
    all_messages.insert(all_messages.end(), payloads3.begin(), payloads3.end());
    ASSERT_EQ(all_messages, handler.messages());

}

// A publisher publishes messages on an MDC channel which are recorded by the archive.
// A persistent subscriber replays the recorded messages and then joins the live stream.
// Once live, a second fast subscriber (on a separate media driver) joins the same MDC channel.
// The publisher floods 64 large messages. Only the fast subscriber is polled during
// this time — the persistent subscriber is not polled and falls behind, causing
// its live image to be closed.
// The persistent subscriber drops back to replay to catch up on the missed messages,
// then rejoins the live stream.
// All messages must be received exactly once and in order.
TEST_F(AeronArchivePersistentSubscriptionTest, shouldDropFromLiveBackToReplayThenJoinLiveAgain)
{
    TestArchive archive = createArchive(m_aeronDir);

    // Phase 1: publish initial messages and start persistent subscription
    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> payloads = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(payloads);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_live_channel(context, MDC_SUBSCRIPTION_CHANNEL.c_str());

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    ASSERT_EQ(0, listener.live_joined_count);

    // Phase 2: replay recorded messages
    executeUntil(
        "receives first message",
        [&]
        {
            return aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                MessageCapturingFragmentHandler::onFragment,
                &handler,
                1);
        },
        [&] { return handler.messageCount() == 1; });

    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    executeUntil(
        "receives all payloads",
        poller,
        [&] { return handler.messageCount() == payloads.size(); });

    // Phase 3: join live
    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));

    ASSERT_EQ(1, listener.live_joined_count);
    ASSERT_EQ(0, listener.live_left_count);
    ASSERT_EQ(payloads.size(), handler.messageCount());

    // Phase 4: consume more messages while live
    const std::vector<std::vector<uint8_t>> live_payloads = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(live_payloads);

    executeUntil(
        "receives live_payloads",
        poller,
        [&] { return handler.messageCount() == payloads.size() + live_payloads.size(); });

    ASSERT_TRUE(aeron_archive_persistent_subscription_is_live(persistent_subscription));

    // Phase 5: introduce a fast subscription on a separate media driver to cause the persistent subscription to fall behind.
    {
        DriverResource driver2;
        AeronResource aeron2(driver2.aeronDir());

        aeron_subscription_t *fast_subscription = nullptr;
        aeron_async_add_subscription_t *async_add = nullptr;
        ASSERT_EQ(0, aeron_async_add_subscription(
            &async_add,
            aeron2.aeron(),
            MDC_SUBSCRIPTION_CHANNEL.c_str(),
            STREAM_ID,
            nullptr, nullptr, nullptr, nullptr)) << aeron_errmsg();

        executeUntil(
            "fast subscription created",
            [&] { return aeron_async_add_subscription_poll(&fast_subscription, async_add) >= 0 ? 1 : -1; },
            [&] { return fast_subscription != nullptr; });

        executeUntil(
            "fast subscription has image",
            [&] { return 0; },
            [&] { return aeron_subscription_image_count(fast_subscription) > 0; });

        const std::vector<std::vector<uint8_t>> back_pressure_messages = generateFixedMessages(64, ONE_KB_MESSAGE_SIZE);
        persistent_publication.offer(back_pressure_messages);

        size_t fast_count = 0;
        executeUntil(
            "fast subscription receives 64 messages",
            [&]
            {
                return aeron_subscription_poll(
                    fast_subscription,
                    [](void *clientd, const uint8_t *, size_t, aeron_header_t *)
                    {
                        (*static_cast<size_t *>(clientd))++;
                    },
                    &fast_count,
                    1);
            },
            [&] { return fast_count == 64; });

        // Phase 6: persistent subscription drops back to replay
        executeUntil(
            "drops to replaying",
            poller,
            isReplaying(persistent_subscription));

        ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));
        ASSERT_EQ(1, listener.live_left_count);

        // Phase 7: recover - persistent subscription catches up via replay and rejoins live
        const std::vector<std::vector<uint8_t>> messages_after_rejoin = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
        persistent_publication.persist(messages_after_rejoin);

        const size_t expected_count =
            payloads.size() + live_payloads.size() + back_pressure_messages.size() + messages_after_rejoin.size();

        executeUntil(
            "receives all messages and becomes live",
            poller,
            [&]
            {
                return handler.messageCount() == expected_count &&
                       aeron_archive_persistent_subscription_is_live(persistent_subscription);
            });

        ASSERT_EQ(2, listener.live_joined_count);

        std::vector<std::vector<uint8_t>> all_messages;
        all_messages.insert(all_messages.end(), payloads.begin(), payloads.end());
        all_messages.insert(all_messages.end(), live_payloads.begin(), live_payloads.end());
        all_messages.insert(all_messages.end(), back_pressure_messages.begin(), back_pressure_messages.end());
        all_messages.insert(all_messages.end(), messages_after_rejoin.begin(), messages_after_rejoin.end());
        ASSERT_EQ(all_messages, handler.messages());

        aeron_subscription_close(fast_subscription, nullptr, nullptr);
    }

}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldHandlePublisherStoppingWhileLive)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> messages = generateFixedMessages(3, 128);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));
    ASSERT_EQ(1, listener.live_joined_count);
    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

    // Close the publication to force the live image to close
    aeron_exclusive_publication_close(persistent_publication.publication(), nullptr, nullptr);

    executeUntil(
        "leaves live",
        poller,
        [&] { return listener.live_left_count == 1; });
    ASSERT_EQ(1, listener.live_left_count);

}

TEST_F(AeronArchivePersistentSubscriptionTest, canFallbackToReplayAfterStartingFromLive)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> first_messages = generateFixedMessages(2, 128);
    persistent_publication.persist(first_messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL,
        STREAM_ID,
        "aeron:udp?endpoint=localhost:0",
        -5,
        AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));
    ASSERT_EQ(1, listener.live_joined_count);
    ASSERT_EQ(0, handler.messageCount());

    const std::vector<std::vector<uint8_t>> second_messages = generateFixedMessages(5, 128);
    persistent_publication.persist(second_messages);

    executeUntil(
        "receives second batch",
        poller,
        [&] { return handler.messageCount() == second_messages.size(); });
    ASSERT_EQ(second_messages, handler.messages());

    // Force the PS to fall behind by flooding with a fast subscriber on a second driver
    {
        const std::string aeronDir2 = defaultAeronDir() + "_2";
        TestArchive archive2(
            aeronDir2,
            std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "driver2",
            std::cout,
            "aeron:udp?endpoint=localhost:8011",
            "aeron:udp?endpoint=localhost:0",
            2);

        AeronResource aeron2(aeronDir2);

        aeron_subscription_t *fast_subscription = nullptr;
        aeron_async_add_subscription_t *async_add = nullptr;
        ASSERT_EQ(0, aeron_async_add_subscription(
            &async_add,
            aeron2.aeron(),
            MDC_SUBSCRIPTION_CHANNEL.c_str(),
            STREAM_ID,
            nullptr, nullptr, nullptr, nullptr)) << aeron_errmsg();

        executeUntil(
            "fast subscription created",
            [&] { return aeron_async_add_subscription_poll(&fast_subscription, async_add) >= 0 ? 1 : -1; },
            [&] { return fast_subscription != nullptr; });

        executeUntil(
            "fast subscription has image",
            [&] { return 0; },
            [&] { return aeron_subscription_image_count(fast_subscription) > 0; });

        const std::vector<std::vector<uint8_t>> back_pressure_messages = generateFixedMessages(64, ONE_KB_MESSAGE_SIZE);
        persistent_publication.offer(back_pressure_messages);

        size_t fast_count = 0;
        executeUntil(
            "fast sub drains",
            [&]
            {
                return aeron_subscription_poll(
                    fast_subscription,
                    [](void *clientd, const uint8_t *, size_t, aeron_header_t *)
                    {
                        (*static_cast<size_t *>(clientd))++;
                    },
                    &fast_count,
                    1);
            },
            [&] { return fast_count == back_pressure_messages.size(); });

        executeUntil(
            "drops to replaying",
            poller,
            isReplaying(persistent_subscription));
        ASSERT_EQ(1, listener.live_left_count);

        const std::vector<std::vector<uint8_t>> messages_after_rejoin = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
        persistent_publication.persist(messages_after_rejoin);

        executeUntil(
            "recovers to live",
            poller,
            [&]
            {
                return handler.messageCount() == second_messages.size() + back_pressure_messages.size() + messages_after_rejoin.size() &&
                       aeron_archive_persistent_subscription_is_live(persistent_subscription);
            });

        ASSERT_EQ(2, listener.live_joined_count);

        std::vector<std::vector<uint8_t>> all_messages;
        all_messages.insert(all_messages.end(), second_messages.begin(), second_messages.end());
        all_messages.insert(all_messages.end(), back_pressure_messages.begin(), back_pressure_messages.end());
        all_messages.insert(all_messages.end(), messages_after_rejoin.begin(), messages_after_rejoin.end());
        ASSERT_EQ(all_messages, handler.messages());

        aeron_subscription_close(fast_subscription, nullptr, nullptr);
    }

}

// Verifies that fragmented messages are correctly reassembled by the persistent subscription.
// Two messages are generated, each one byte larger than the maximum payload length, forcing
// fragmentation. The first message is persisted before the subscription is created and received
// during replay. The subscription transitions to live, after which the second message is
// persisted and received on the live stream. Both messages are expected to be fully reassembled
// and received in order.
TEST_F(AeronArchivePersistentSubscriptionTest, shouldAssembleMessages)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);

    const int32_t size_requiring_fragmentation = persistent_publication.maxPayloadLength() + 1;

    const std::vector<uint8_t> payload0 = generateRandomBytes(size_requiring_fragmentation);
    const std::vector<uint8_t> payload1 = generateRandomBytes(size_requiring_fragmentation);

    persistent_publication.persist({{ payload0 }});

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler, 1);

    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));

    persistent_publication.persist({{ payload1 }});

    executeUntil(
        "receives both messages",
        poller,
        [&] { return handler.messageCount() == 2; });

    ASSERT_EQ((std::vector<std::vector<uint8_t>>{{ payload0, payload1 }}), handler.messages());

}

struct ReplayChannelAndStream
{
    std::string replay_channel;
    int32_t replay_stream_id;
    std::string archive_control_request_channel;
    std::string archive_control_response_channel;
};

class AeronArchivePersistentSubscriptionReplayOverConfiguredChannelTest
    : public AeronArchivePersistentSubscriptionTest,
      public testing::WithParamInterface<ReplayChannelAndStream>
{
};

INSTANTIATE_TEST_SUITE_P(
    ,
    AeronArchivePersistentSubscriptionReplayOverConfiguredChannelTest,
    testing::Values(
        ReplayChannelAndStream{"aeron:udp?endpoint=localhost:0", -10, LOCALHOST_CONTROL_REQUEST_CHANNEL, LOCALHOST_CONTROL_RESPONSE_CHANNEL},
        ReplayChannelAndStream{"aeron:udp?endpoint=localhost:10001", -11, LOCALHOST_CONTROL_REQUEST_CHANNEL, LOCALHOST_CONTROL_RESPONSE_CHANNEL},
        ReplayChannelAndStream{"aeron:ipc", -12, LOCALHOST_CONTROL_REQUEST_CHANNEL, LOCALHOST_CONTROL_RESPONSE_CHANNEL},
        ReplayChannelAndStream{"aeron:udp?control=localhost:10001|control-mode=response", -11, LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?control-mode=response|control=localhost:10002"},
        ReplayChannelAndStream{"aeron:udp?control=localhost:10001|control-mode=response|endpoint=localhost:5006", -11, LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?control-mode=response|control=localhost:10002"},
        ReplayChannelAndStream{"aeron:udp?control=localhost:10001|control-mode=response|endpoint=localhost:0", -11, LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?control-mode=response|control=localhost:10002"},
        ReplayChannelAndStream{"aeron:ipc?control-mode=response", -11, "aeron:ipc", "aeron:ipc?control-mode=response"}
    ));

TEST_P(AeronArchivePersistentSubscriptionReplayOverConfiguredChannelTest, shouldReplayOverConfiguredChannel)
{
    TestArchive archive = createArchive(m_aeronDir);

    const std::string& replay_channel = GetParam().replay_channel;
    const int32_t replay_stream_id = GetParam().replay_stream_id;
    const std::string& archive_control_request_channel = GetParam().archive_control_request_channel;
    const std::string& archive_control_response_channel = GetParam().archive_control_response_channel;

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> payloads = generateRandomMessages(5);
    persistent_publication.persist(payloads);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_context_set_control_request_channel(archive_ctx, archive_control_request_channel.c_str());
    aeron_archive_context_set_control_response_channel(archive_ctx, archive_control_response_channel.c_str());

    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_replay_channel(context, replay_channel.c_str());
    aeron_archive_persistent_subscription_context_set_replay_stream_id(context, replay_stream_id);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;

    executeUntil(
        "receives first message",
        [&]
        {
            return aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                MessageCapturingFragmentHandler::onFragment,
                &handler,
                1);
        },
        [&] { return handler.messageCount() == 1; });

    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    executeUntil(
        "becomes live",
        [&]
        {
            return aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                MessageCapturingFragmentHandler::onFragment,
                &handler,
                10);
        },
        isLive(persistent_subscription));

    ASSERT_EQ(payloads, handler.messages());

}

static const std::string SPY_PREFIX = "aeron-spy:";

class AeronArchivePersistentSubscriptionSpyOnLiveTest
    : public AeronArchivePersistentSubscriptionTest,
      public testing::WithParamInterface<int>
{
};

INSTANTIATE_TEST_SUITE_P(
    ,
    AeronArchivePersistentSubscriptionSpyOnLiveTest,
    testing::Values(1, 10));

TEST_P(AeronArchivePersistentSubscriptionSpyOnLiveTest, shouldReplayExistingRecordingThenSpyOnLive)
{
    TestArchive archive = createArchive(m_aeronDir);

    const int fragment_limit = GetParam();

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> payloads = generateFixedMessages(8, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(payloads);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_live_channel(
        context, (SPY_PREFIX + "aeron:udp?control=localhost:2000").c_str());

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler, fragment_limit);

    executeUntil(
        "receives first 8 messages",
        [&]
        {
            return aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                MessageCapturingFragmentHandler::onFragment,
                &handler,
                1);
        },
        [&] { return handler.messageCount() == 8; });

    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    executeUntil(
        "receives all payloads",
        poller,
        [&] { return handler.messageCount() == payloads.size(); });

    ASSERT_EQ(payloads, handler.messages());

    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));

    ASSERT_EQ(payloads.size(), handler.messageCount());

    const std::vector<std::vector<uint8_t>> payloads2 = generateFixedMessages(16, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(payloads2);

    executeUntil(
        "receives all live messages",
        poller,
        [&]
        {
            return handler.messageCount() == payloads.size() + payloads2.size() &&
                   aeron_archive_persistent_subscription_is_live(persistent_subscription);
        });

    ASSERT_TRUE(aeron_archive_persistent_subscription_is_live(persistent_subscription));
    ASSERT_FALSE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    std::vector<std::vector<uint8_t>> all_messages;
    all_messages.insert(all_messages.end(), payloads.begin(), payloads.end());
    all_messages.insert(all_messages.end(), payloads2.begin(), payloads2.end());
    ASSERT_EQ(all_messages, handler.messages());

}

// Verifies that subscribing to a non-existent recording id causes the subscription
// to fail and invokes on_error exactly once with an appropriate error message
TEST_F(AeronArchivePersistentSubscriptionTest, shouldErrorIfRecordingDoesNotExist)
{
    TestArchive archive = createArchive(m_aeronDir);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        13); // does not exist

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    executeUntil(
        "has failed",
        [&]
        {
            return aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                nullptr,
                nullptr,
                1);
        },
        hasFailed(persistent_subscription));

    ASSERT_EQ(1, listener.error_count);
    ASSERT_NE(std::string::npos, listener.last_error_message.find("recording"));

}

// Verifies that the stream id of the recording must match the configured live stream id.
// A recording is created on STREAM_ID, but the persistent subscription is configured
// with a mismatched live stream id (STREAM_ID + 1). The subscription is expected to
// fail and invoke on_error exactly once with an appropriate error message.
TEST_F(AeronArchivePersistentSubscriptionTest, shouldErrorIfRecordingStreamDoesNotMatchLiveStream)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_live_stream_id(context, STREAM_ID + 1); // <-- mismatched

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    executeUntil(
        "has failed",
        [&]
        {
            return aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                nullptr,
                nullptr,
                1);
        },
        hasFailed(persistent_subscription));

    ASSERT_EQ(1, listener.error_count);
    ASSERT_NE(std::string::npos, listener.last_error_message.find("stream"));

}

// Verifies that a persistent subscription fails if the requested start position is before
// the recording's start position. The recording is configured with an initial term offset
// of 1024, giving it a start position of 1024. The persistent subscription is configured
// with the default start position of 0, which is below the recording start. The subscription
// is expected to fail and invoke on_error exactly once with an error message containing
// the word "position".
TEST_F(AeronArchivePersistentSubscriptionTest, shouldErrorIfStartPositionIsBeforeRecordingStartPosition)
{
    TestArchive archive = createArchive(m_aeronDir);

    const std::string channel = "aeron:ipc?init-term-id=0|term-id=0|term-offset=1024|term-length=65536";

    PersistentPublication persistent_publication(m_aeronDir, channel, STREAM_ID);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_live_channel(context, channel.c_str());
    // start_position is already 0 from default, below recording start of 1024

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    executeUntil(
        "has failed",
        [&]
        {
            return aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                nullptr,
                nullptr,
                1);
        },
        hasFailed(persistent_subscription));

    ASSERT_EQ(1, listener.error_count);
    ASSERT_NE(std::string::npos, listener.last_error_message.find("position"));

}

// Verifies that a persistent subscription cannot be created with a start position
// that exceeds the recording's stop position. A single message is persisted and
// the recording is stopped to obtain a known stop position. The subscription is
// configured with a start position of twice the stop position. The subscription
// is expected to fail and invoke on_error exactly once with an appropriate error
// message.
TEST_F(AeronArchivePersistentSubscriptionTest, shouldErrorIfStartPositionIsAfterStopPosition)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);

    const std::vector<uint8_t> message(1024, 0);
    persistent_publication.persist({{ message }});

    const int64_t stop_position = persistent_publication.stop();
    ASSERT_GT(stop_position, 0);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_start_position(context, stop_position * 2); // <-- after end

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    executeUntil(
        "has failed",
        [&]
        {
            return aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                nullptr,
                nullptr,
                1);
        },
        hasFailed(persistent_subscription));

    ASSERT_EQ(1, listener.error_count);
    ASSERT_NE(std::string::npos, listener.last_error_message.find("position"));

}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldFailIfLiveChannelIsInvalid)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_live_channel(context, "invalid");

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil(
        "has failed",
        poller,
        hasFailed(persistent_subscription));
    EXPECT_THAT(aeron_errmsg(), testing::HasSubstr("failed to add live subscription"));

    ps_guard.release();
    EXPECT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
    archive_ctx_guard.release();
    aeron_archive_context_close(archive_ctx);
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldErrorWhenStartPositionDoesNotAlignWithFrame)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> messages = generateFixedMessages(2, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL,
        STREAM_ID,
        "aeron:udp?endpoint=localhost:0",
        -5,
        ONE_KB_MESSAGE_SIZE - 32); // misaligned start position

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    auto poller = [&] {
        return aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription, MessageCapturingFragmentHandler::onFragment, nullptr, 1);
    };

    executeUntil(
        "has error",
        poller,
        [&] { return listener.error_count > 0; });

    ASSERT_TRUE(aeron_archive_persistent_subscription_has_failed(persistent_subscription));

}

TEST_F(AeronArchivePersistentSubscriptionTest, canStartAtRecordingStopPositionWhenLiveHasNotAdvanced)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> messages = generateFixedMessages(1, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    const int64_t stop_position = persistent_publication.stop();
    ASSERT_GT(stop_position, 0);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId(),
        IPC_CHANNEL,
        STREAM_ID,
        "aeron:udp?endpoint=localhost:0",
        -5,
        stop_position); // start position == stop position — now allowed

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler, 1);

    executeUntil(
        "is live",
        poller,
        [&] { return aeron_archive_persistent_subscription_is_live(persistent_subscription); });

    const std::vector<std::vector<uint8_t>> liveMessages = generateFixedMessages(3, ONE_KB_MESSAGE_SIZE);
    persistent_publication.offer(liveMessages);

    executeUntil(
        "received live messages",
        poller,
        [&] { return handler.messages().size() == liveMessages.size(); });

    ASSERT_EQ(0, listener.error_count);

    ps_guard.release();
    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
    archive_ctx_guard.release();
    aeron_archive_context_close(archive_ctx);
}

TEST_F(AeronArchivePersistentSubscriptionTest, fallbackFromLiveFailsWhenRecordingStoppedBeforeLivePosition)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> messages = generateFixedMessages(1, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId(),
        IPC_CHANNEL,
        STREAM_ID,
        "aeron:udp?endpoint=localhost:0",
        -5,
        AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler capture;
    auto poller = makeControlledPoller(persistent_subscription, capture, 1);

    executeUntil(
        "is live",
        poller,
        isLive(persistent_subscription));

    persistent_publication.stop();

    // These messages advance live past the now-frozen recording stopPosition.
    const std::vector<std::vector<uint8_t>> liveOnlyMessages = generateFixedMessages(3, ONE_KB_MESSAGE_SIZE);
    persistent_publication.offer(liveOnlyMessages);

    executeUntil(
        "received live-only messages",
        poller,
        [&] { return capture.messages().size() == liveOnlyMessages.size(); });

    aeron_exclusive_publication_close(persistent_publication.publication(), nullptr, nullptr);

    executeUntil(
        "has failed",
        poller,
        hasFailed(persistent_subscription));

    ASSERT_NE(std::string::npos,
        listener.last_error_message.find("earlier than last observed live position"));

    ps_guard.release();
    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
    archive_ctx_guard.release();
    aeron_archive_context_close(archive_ctx);
}

TEST_F(AeronArchivePersistentSubscriptionTest, startAtStopPositionFailsWhenLiveAheadAndRecordingDoesNotResume)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> payloads = generateFixedMessages(1, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(payloads);

    const int64_t stop_position = persistent_publication.stop();
    ASSERT_GT(stop_position, 0);

    // Advance live past the recording: attach a temporary subscriber so the publication
    // is connected, then offer the unrecorded messages and drain them.
    const std::vector<std::vector<uint8_t>> advance_payloads = generateFixedMessages(3, ONE_KB_MESSAGE_SIZE);

    {
        aeron_t *pub_aeron = aeron_archive_context_get_aeron(
            aeron_archive_get_archive_context(persistent_publication.archive()));

        aeron_subscription_t *temp_subscription = nullptr;
        aeron_async_add_subscription_t *async_add = nullptr;
        ASSERT_EQ(0, aeron_async_add_subscription(
            &async_add, pub_aeron,
            MDC_SUBSCRIPTION_CHANNEL.c_str(), STREAM_ID,
            nullptr, nullptr, nullptr, nullptr)) << aeron_errmsg();

        executeUntil(
            "temp subscription created",
            [&] { return aeron_async_add_subscription_poll(&temp_subscription, async_add) >= 0 ? 1 : -1; },
            [&] { return temp_subscription != nullptr; });

        executeUntil(
            "temp subscription connected",
            [&] { return 0; },
            [&] { return aeron_subscription_is_connected(temp_subscription); });

        persistent_publication.offer(advance_payloads);

        size_t received = 0;
        executeUntil(
            "drain temp subscription",
            [&]
            {
                return aeron_subscription_poll(
                    temp_subscription,
                    [](void *clientd, const uint8_t *, size_t, aeron_header_t *)
                    {
                        (*static_cast<size_t *>(clientd))++;
                    },
                    &received,
                    10);
            },
            [&] { return received == advance_payloads.size(); });

        aeron_subscription_close(temp_subscription, nullptr, nullptr);
    }

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL,
        STREAM_ID,
        "aeron:udp?endpoint=localhost:0",
        -5,
        stop_position); // start position == stop position; live is ahead and recording does not resume

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler, 7);

    executeUntil(
        "has failed",
        poller,
        hasFailed(persistent_subscription));

    ASSERT_EQ(1, listener.error_count);
    ASSERT_NE(std::string::npos,
        listener.last_error_message.find("earlier than last observed live position"));

    ps_guard.release();
    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
    archive_ctx_guard.release();
    aeron_archive_context_close(archive_ctx);
}

// Recording is stopped at stop_0, the publication is closed, then the recording is resumed via
// extend_recording and more messages are persisted so live (= the current recorded end) is ahead of
// stop_0. The subscription then starts with start_position = stop_0. Because the recording is now
// active (stop_position = NULL_VALUE), PS does NOT shortcut to ADD_LIVE_SUBSCRIPTION — it takes the
// normal replay path, replays the newly-recorded gap, catches up via ATTEMPT_SWITCH, and joins live.
TEST_F(AeronArchivePersistentSubscriptionTest, shouldCatchUpWhenStartingAtStopPositionOfExtendedRecording)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    const std::vector<std::vector<uint8_t>> initial_messages = generateFixedMessages(1, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(initial_messages);

    const int64_t stop_position = persistent_publication.stop();
    ASSERT_GT(stop_position, 0);
    const int64_t recording_id = persistent_publication.recordingId();

    aeron_publication_constants_t pub_constants;
    aeron_exclusive_publication_constants(persistent_publication.publication(), &pub_constants);
    aeron_counters_reader_t *counters = aeron_counters_reader(
        aeron_archive_context_get_aeron(aeron_archive_get_archive_context(persistent_publication.archive())));
    aeron_exclusive_publication_close(persistent_publication.publication(), nullptr, nullptr);
    waitUntil("publication counters removed",
        [&]
        {
            return AERON_NULL_COUNTER_ID == aeron_counters_reader_find_by_type_id_and_registration_id(
                counters, AERON_COUNTER_PUBLISHER_POSITION_TYPE_ID, pub_constants.registration_id);
        });

    // Resume the recording at stop_0 and persist catch-up messages so live advances past stop_0.
    PersistentPublication resumed_publication =
        PersistentPublication::resume(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID, recording_id);
    const std::vector<std::vector<uint8_t>> catchup_messages = generateFixedMessages(3, ONE_KB_MESSAGE_SIZE);
    resumed_publication.persist(catchup_messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        recording_id,
        MDC_SUBSCRIPTION_CHANNEL,
        STREAM_ID,
        "aeron:udp?endpoint=localhost:0",
        -5,
        stop_position);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil(
        "received catch-up messages via replay",
        poller,
        [&] { return handler.messages().size() == catchup_messages.size(); });
    ASSERT_EQ(catchup_messages, handler.messages());

    executeUntil(
        "is live",
        poller,
        [&] { return aeron_archive_persistent_subscription_is_live(persistent_subscription); });
    ASSERT_EQ(1, listener.live_joined_count);

    const std::vector<std::vector<uint8_t>> live_messages = generateFixedMessages(2, ONE_KB_MESSAGE_SIZE);
    resumed_publication.offer(live_messages);

    executeUntil(
        "received live messages after joining live",
        poller,
        [&] { return handler.messages().size() == catchup_messages.size() + live_messages.size(); });

    ASSERT_EQ(0, listener.error_count);

    ps_guard.release();
    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
    archive_ctx_guard.release();
    aeron_archive_context_close(archive_ctx);
}

// Deterministically exercises the shortcut → live-ahead → refresh_recording_descriptor → replay → live
// path. PS starts while the recording is stopped, so its first list_recording returns the stopped
// descriptor and PS takes the shortcut to ADD_LIVE_SUBSCRIPTION. After PS has parked in AWAIT_LIVE
// (confirmed by the deadline-breach error), the test resumes the recording and persists messages on
// a fresh publisher. Because the new publisher has already advanced past stop_0 by the time its
// live image reaches PS, `live_position > position` triggers the refresh. The second list_recording
// sees the extended recording, validation passes, and PS replays the gap before joining live.
TEST_F(AeronArchivePersistentSubscriptionTest, shouldRefreshAndReplayWhenLiveAheadOfStopPositionAfterResume)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    const std::vector<std::vector<uint8_t>> initial_messages = generateFixedMessages(1, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(initial_messages);

    const int64_t stop_position = persistent_publication.stop();
    ASSERT_GT(stop_position, 0);
    const int64_t recording_id = persistent_publication.recordingId();

    aeron_publication_constants_t pub_constants;
    aeron_exclusive_publication_constants(persistent_publication.publication(), &pub_constants);
    aeron_counters_reader_t *counters = aeron_counters_reader(
        aeron_archive_context_get_aeron(aeron_archive_get_archive_context(persistent_publication.archive())));
    aeron_exclusive_publication_close(persistent_publication.publication(), nullptr, nullptr);
    waitUntil("publication counters removed",
        [&]
        {
            return AERON_NULL_COUNTER_ID == aeron_counters_reader_find_by_type_id_and_registration_id(
                counters, AERON_COUNTER_PUBLISHER_POSITION_TYPE_ID, pub_constants.registration_id);
        });

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    // Short timeout so the AWAIT_LIVE deadline breach fires quickly while the recording is stopped
    // and no publisher is available yet.
    aeron_archive_context_set_message_timeout_ns(archive_ctx, 500LL * 1000 * 1000);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        recording_id,
        MDC_SUBSCRIPTION_CHANNEL,
        STREAM_ID,
        "aeron:udp?endpoint=localhost:0",
        -5,
        stop_position);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    bool observed_replaying = false;
    auto poller = [&]
    {
        const int fragments = aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription,
            MessageCapturingFragmentHandler::onFragment,
            &handler,
            10);
        if (aeron_archive_persistent_subscription_is_replaying(persistent_subscription))
        {
            observed_replaying = true;
        }
        return fragments;
    };

    // PS shortcuts to ADD_LIVE_SUBSCRIPTION and parks in AWAIT_LIVE; no publisher exists so the
    // live-image deadline breaches as a non-terminal error — fired exactly once by this AWAIT_LIVE
    // entry (guarded by `live_image_deadline_breached`).
    executeUntil(
        "live-image deadline breach fires",
        poller,
        [&] { return listener.error_count > 0; });
    ASSERT_EQ(1, listener.error_count);
    ASSERT_NE(std::string::npos,
        listener.last_error_message.find("No image became available on the live subscription"));
    ASSERT_FALSE(aeron_archive_persistent_subscription_is_live(persistent_subscription));
    ASSERT_FALSE(observed_replaying);

    PersistentPublication resumed_publication =
        PersistentPublication::resume(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID, recording_id);

    // Push the publisher tail past stop_position with a priming offer before the catchup
    // messages. Without this, PS's live image often attaches at exactly stop_position, the
    // await_live check `live_position > start_position` is false, and PS takes the direct
    // AWAIT_LIVE → LIVE path instead of the refresh-replay path this test exercises.
    const std::vector<std::vector<uint8_t>> priming_messages = generateFixedMessages(1, ONE_KB_MESSAGE_SIZE);
    resumed_publication.persist(priming_messages);

    const std::vector<std::vector<uint8_t>> catchup_messages = generateFixedMessages(3, ONE_KB_MESSAGE_SIZE);
    resumed_publication.persist(catchup_messages);

    executeUntil(
        "is live",
        poller,
        [&] { return aeron_archive_persistent_subscription_is_live(persistent_subscription); });

    std::vector<std::vector<uint8_t>> expected_messages;
    expected_messages.insert(expected_messages.end(), priming_messages.begin(), priming_messages.end());
    expected_messages.insert(expected_messages.end(), catchup_messages.begin(), catchup_messages.end());

    executeUntil(
        "received all post-resume messages",
        poller,
        [&] { return handler.messages().size() == expected_messages.size(); });
    ASSERT_EQ(expected_messages, handler.messages());

    // Messages offered before PS's live image attached can only have arrived via the refresh
    // → replay path; mid-stream UDP subscribers don't backfill historical bytes on the live
    // stream.
    ASSERT_TRUE(observed_replaying)
        << "PS did not transition through REPLAY/ATTEMPT_SWITCH; refresh path was not exercised";
    ASSERT_EQ(1, listener.live_joined_count);
    ASSERT_EQ(0, listener.live_left_count);
    // Still exactly the one initial deadline-breach error — PS refreshed once, went to REPLAY then
    // LIVE, and has stayed there. No additional AWAIT_LIVE entries, so no additional breaches.
    ASSERT_EQ(1, listener.error_count);

    ps_guard.release();
    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
    archive_ctx_guard.release();
    aeron_archive_context_close(archive_ctx);
}

// Recording is extended (no gap), PS lags the live stream, then the publisher is revoked.
// The recording end is past PS's position when the live image dies, so PS must REPLAY to
// catch up.
TEST_F(AeronArchivePersistentSubscriptionTest, shouldReplayAndCatchUpWhenExtendedRecordingIsAheadOfLivePosition)
{
    TestArchive archive = createArchive(m_aeronDir);

    // Set the publication's term-length to the Aeron minimum (64 KiB) so the receiver-side
    // flow-control overrun threshold (`last_sm_position + term_length/2`) is just 32 KiB.
    // With a 32 KiB threshold we can publish enough catchup data after PS's last SM
    // to guarantee the trailing messages are dropped at PS and must come via REPLAY.
    const std::string short_term_publication_channel = MDC_PUBLICATION_CHANNEL + "|term-length=65536";

    PersistentPublication persistent_publication(m_aeronDir, short_term_publication_channel, STREAM_ID);
    persistent_publication.persist(generateFixedMessages(1, ONE_KB_MESSAGE_SIZE));
    const int64_t stop_position = persistent_publication.stop();
    ASSERT_GT(stop_position, 0);
    const int64_t recording_id = persistent_publication.recordingId();

    aeron_publication_constants_t pub_constants;
    aeron_exclusive_publication_constants(persistent_publication.publication(), &pub_constants);
    aeron_counters_reader_t *counters = aeron_counters_reader(
        aeron_archive_context_get_aeron(aeron_archive_get_archive_context(persistent_publication.archive())));
    aeron_exclusive_publication_close(persistent_publication.publication(), nullptr, nullptr);
    waitUntil("publication counters removed",
        [&]
        {
            return AERON_NULL_COUNTER_ID == aeron_counters_reader_find_by_type_id_and_registration_id(
                counters, AERON_COUNTER_PUBLISHER_POSITION_TYPE_ID, pub_constants.registration_id);
        });

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_context_set_message_timeout_ns(archive_ctx, 500LL * 1000 * 1000);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        recording_id,
        MDC_SUBSCRIPTION_CHANNEL,
        STREAM_ID,
        "aeron:udp?endpoint=localhost:0",
        -5,
        stop_position);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    bool observed_replaying = false;
    auto poller = [&]
    {
        const int fragments = aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription,
            MessageCapturingFragmentHandler::onFragment,
            &handler,
            1);
        if (aeron_archive_persistent_subscription_is_replaying(persistent_subscription))
        {
            observed_replaying = true;
        }
        return fragments;
    };

    // PS shortcuts to AWAIT_LIVE; no publisher, deadline breaches exactly once.
    executeUntil(
        "deadline breach fires",
        poller,
        [&] { return listener.error_count > 0; });
    ASSERT_EQ(1, listener.error_count);

    // Resume: creates publisher B at stop_0 and extends the recording. Because extend happens
    // before any offers, the recording is contiguous from stop_0.
    PersistentPublication resumed_publication =
        PersistentPublication::resume(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID, recording_id);

    // Publisher B offers and records the first message.
    const std::vector<std::vector<uint8_t>> first_batch = generateFixedMessages(1, ONE_KB_MESSAGE_SIZE);
    resumed_publication.persist(first_batch);

    // Poll PS until it has received the first message (may arrive via live or replay).
    executeUntil(
        "received first message",
        poller,
        [&] { return handler.messages().size() == first_batch.size(); });

    // Publisher B offers many more messages. Archive records all of them, but PS isn't being
    // polled here so its `last_sm_position` is pinned ~2 KiB and the receiver-side overrun
    // threshold is pinned ~34 KiB (2 KiB + term_length/2). Catchup data spans past that
    // threshold, so the trailing messages are dropped at PS's image and only ever appear in
    // the recording — PS must replay to receive them.
    const std::vector<std::vector<uint8_t>> catchup_messages = generateFixedMessages(40, ONE_KB_MESSAGE_SIZE);
    resumed_publication.persist(catchup_messages);

    // Revoke ends the live image and discards any unsent term-buffer data, so the over-run
    // bytes can't be recovered via NAK retransmits. After the term buffer is drained, PS sees
    // the image closed, refreshes the recording descriptor, and replays the bytes the live
    // channel never delivered.
    aeron_exclusive_publication_revoke(resumed_publication.publication(), nullptr, nullptr);

    std::vector<std::vector<uint8_t>> expected;
    expected.insert(expected.end(), first_batch.begin(), first_batch.end());
    expected.insert(expected.end(), catchup_messages.begin(), catchup_messages.end());

    executeUntil(
        "received all messages",
        poller,
        [&] { return handler.messages().size() == expected.size(); });

    ASSERT_EQ(expected, handler.messages());
    ASSERT_TRUE(observed_replaying)
        << "PS did not transition through REPLAY/ATTEMPT_SWITCH; refresh-replay path was not exercised";

    ps_guard.release();
    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
    archive_ctx_guard.release();
    aeron_archive_context_close(archive_ctx);
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldNotRequireEventListener)
{
    TestArchive archive = createArchive(m_aeronDir);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        13); // does not exist

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    executeUntil(
        "has failed",
        [&]
        {
            return aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                nullptr,
                nullptr,
                1);
        },
        hasFailed(persistent_subscription));

}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldPropagateErrorCodeAndMessageToListener)
{
    TestArchive archive = createArchive(m_aeronDir);
    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        99999);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    auto poller = [&] {
        return aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription, MessageCapturingFragmentHandler::onFragment, nullptr, 1);
    };

    executeUntil(
        "has failed",
        poller,
        hasFailed(persistent_subscription));

    ASSERT_EQ(1, listener.error_count);
    ASSERT_NE(0, listener.last_errcode);
    ASSERT_FALSE(listener.last_error_message.empty());
    ASSERT_NE(std::string::npos, listener.last_error_message.find("recording"));

}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldNotReportFailedDuringNormalOperation)
{
    TestArchive archive = createArchive(m_aeronDir);

    const std::vector<std::vector<uint8_t>> messages = generateFixedMessages(5, 128);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    ASSERT_FALSE(aeron_archive_persistent_subscription_has_failed(persistent_subscription));

    MessageCapturingFragmentHandler handler;

    executeUntil(
        "receives first",
        [&]
        {
            return aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                MessageCapturingFragmentHandler::onFragment,
                &handler,
                1);
        },
        [&] { return handler.messageCount() == 1; });

    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));
    ASSERT_FALSE(aeron_archive_persistent_subscription_has_failed(persistent_subscription));

    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));

    ASSERT_TRUE(aeron_archive_persistent_subscription_is_live(persistent_subscription));
    ASSERT_FALSE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));
    ASSERT_FALSE(aeron_archive_persistent_subscription_has_failed(persistent_subscription));

}

// Verifies that starting replay at a position ahead of the latest recorded position fails.
TEST_F(AeronArchivePersistentSubscriptionTest, shouldErrorIfStartPositionIsAfterRecordingLivePosition)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);
    persistent_publication.persist(generateFixedMessages(1, ONE_KB_MESSAGE_SIZE));

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId(),
        IPC_CHANNEL,
        STREAM_ID,
        "aeron:udp?endpoint=localhost:0",
        -5,
        aeron_exclusive_publication_position(persistent_publication.publication()) * 2);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    executeUntil(
        "has failed",
        [&] { return aeron_archive_persistent_subscription_controlled_poll(persistent_subscription, nullptr, nullptr, 1); },
        hasFailed(persistent_subscription));

    ASSERT_EQ(1, listener.error_count);

}

// Verifies that a persistent subscription can start from live when the recording has been stopped.
TEST_F(AeronArchivePersistentSubscriptionTest, canStartFromLiveWhenRecordingHasStopped)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> first_batch = generateRandomMessages(1);
    persistent_publication.persist(first_batch);
    const int64_t stop_position = persistent_publication.stop();
    ASSERT_GT(stop_position, 0);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId(),
        IPC_CHANNEL,
        STREAM_ID,
        "aeron:udp?endpoint=localhost:0",
        -5,
        AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler, 1);

    executeUntil("becomes live", poller,
        isLive(persistent_subscription));

    const std::vector<std::vector<uint8_t>> second_batch = generateRandomMessages(1);
    persistent_publication.offer(second_batch);

    executeUntil("receives live messages", poller,
        [&] { return handler.messageCount() == second_batch.size(); });
    ASSERT_EQ(second_batch, handler.messages());

}

// Verifies that a persistent subscription replays a stopped recording, waits for the live
// publication to become available, then joins live.
TEST_F(AeronArchivePersistentSubscriptionTest, shouldJoinLiveUponReachingEndOfRecordingWhenLiveBecomesAvailable)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    const std::vector<std::vector<uint8_t>> old_messages = generateFixedMessages(8, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(old_messages);
    const int64_t recording_id = persistent_publication.recordingId();

    // Stop recording and close the publication so no live image is available.
    // Save registration_id before closing since the publication pointer becomes invalid.
    aeron_publication_constants_t pub_constants;
    aeron_exclusive_publication_constants(persistent_publication.publication(), &pub_constants);
    aeron_counters_reader_t *counters = aeron_counters_reader(
        aeron_archive_context_get_aeron(aeron_archive_get_archive_context(persistent_publication.archive())));

    persistent_publication.stop();
    aeron_exclusive_publication_close(persistent_publication.publication(), nullptr, nullptr);

    waitUntil("publication counters removed",
        [&]
        {
            return AERON_NULL_COUNTER_ID == aeron_counters_reader_find_by_type_id_and_registration_id(
                counters, AERON_COUNTER_PUBLISHER_POSITION_TYPE_ID, pub_constants.registration_id);
        });

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_context_set_message_timeout_ns(archive_ctx, 500 * 1000 * 1000LL);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        recording_id,
        MDC_SUBSCRIPTION_CHANNEL,
        STREAM_ID,
        "aeron:udp?endpoint=localhost:0",
        -5,
        0);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // Should replay all old messages
    executeUntil("replays old messages", poller,
        [&] { return handler.messageCount() == old_messages.size(); });
    ASSERT_EQ(old_messages, handler.messages());

    // Should get a timeout error about no live image being available
    executeUntil("gets live timeout error", poller,
        [&] { return listener.error_count > 0; });
    EXPECT_NE(std::string::npos, listener.last_error_message.find("No image became available"));

    // Restart the publication using extendRecording to resume the recording
    PersistentPublication resumed_publication =
        PersistentPublication::resume(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID,
            recording_id);

    executeUntil("becomes live", poller,
        isLive(persistent_subscription));

    const std::vector<std::vector<uint8_t>> new_messages = generateFixedMessages(16, ONE_KB_MESSAGE_SIZE);
    resumed_publication.persist(new_messages);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == old_messages.size() + new_messages.size(); });

    ASSERT_TRUE(aeron_archive_persistent_subscription_is_live(persistent_subscription));

    std::vector<std::vector<uint8_t>> all_messages;
    all_messages.insert(all_messages.end(), old_messages.begin(), old_messages.end());
    all_messages.insert(all_messages.end(), new_messages.begin(), new_messages.end());
    ASSERT_EQ(all_messages, handler.messages());

    // After recovery, no further breach errors should fire; live_joined should have fired exactly once.
    ASSERT_EQ(1, listener.live_joined_count)
        << "Expected exactly one live_joined callback after recovery";
    const int errors_at_recovery = listener.error_count;
    for (int i = 0; i < 500; ++i) { poller(); }
    ASSERT_EQ(errors_at_recovery, listener.error_count)
        << "PS reported additional errors after successful LIVE recovery";
    ASSERT_EQ(1, listener.live_joined_count)
        << "PS reported additional live_joined callbacks after successful LIVE recovery";
}

// Verifies that a persistent subscription fails when it falls back to replay but the recording
// has been stopped at a position earlier than where the subscription was consuming live.
TEST_F(AeronArchivePersistentSubscriptionTest, cannotFallbackToReplayWhenRecordingHasStoppedAtAnEarlierPosition)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    DriverResource driver2;
    AeronResource aeron2(driver2.aeronDir());

    aeron_subscription_t *fast_subscription = nullptr;
    aeron_async_add_subscription_t *async_add = nullptr;
    ASSERT_EQ(0, aeron_async_add_subscription(
        &async_add, aeron2.aeron(), MDC_SUBSCRIPTION_CHANNEL.c_str(), STREAM_ID,
        nullptr, nullptr, nullptr, nullptr)) << aeron_errmsg();
    executeUntil("fast subscription created",
        [&] { return aeron_async_add_subscription_poll(&fast_subscription, async_add) >= 0 ? 1 : -1; },
        [&] { return fast_subscription != nullptr; });

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", -5,
        AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil("becomes live", poller,
        isLive(persistent_subscription));

    // Consume messages on live and ensure the fast consumer keeps up
    const std::vector<std::vector<uint8_t>> first_batch = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(first_batch);

    executeUntil("fast consumer has image",
        [&] { return aeron_subscription_poll(fast_subscription, [](void*, const uint8_t*, size_t, aeron_header_t*){}, nullptr, 10); },
        [&] { return aeron_subscription_image_count(fast_subscription) > 0; });

    // Stop the recording
    aeron_archive_stop_recording_exclusive_publication(persistent_publication.archive(), persistent_publication.publication());

    // Consume more messages on live that will NOT be recorded
    const std::vector<std::vector<uint8_t>> second_batch = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
    persistent_publication.offer(second_batch);

    executeUntil("receives both batches", poller,
        [&] { return handler.messageCount() == first_batch.size() + second_batch.size(); });

    // Close the publication so the live image ends via EOS rather than relying on a flow-control timeout.
    // The persistent subscription then refreshes the recording descriptor and the validate step rejects
    // the replay because its position is past the recording's stop position.
    aeron_exclusive_publication_close(persistent_publication.publication(), nullptr, nullptr);

    // PS should fail because it can't replay past the recording's stop position
    executeUntil("has failed", poller,
        hasFailed(persistent_subscription));

    ASSERT_GE(listener.error_count, 1);

    aeron_subscription_close(fast_subscription, nullptr, nullptr);
}

// Verifies that a persistent subscription fails when it falls back to replay but the recording
// has been purged (removed).
TEST_F(AeronArchivePersistentSubscriptionTest, cannotFallbackToReplayWhenRecordingHasBeenRemoved)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    DriverResource driver2;
    AeronResource aeron2(driver2.aeronDir());

    aeron_subscription_t *fast_subscription = nullptr;
    aeron_async_add_subscription_t *async_add = nullptr;
    ASSERT_EQ(0, aeron_async_add_subscription(
        &async_add, aeron2.aeron(), MDC_SUBSCRIPTION_CHANNEL.c_str(), STREAM_ID,
        nullptr, nullptr, nullptr, nullptr)) << aeron_errmsg();
    executeUntil("fast subscription created",
        [&] { return aeron_async_add_subscription_poll(&fast_subscription, async_add) >= 0 ? 1 : -1; },
        [&] { return fast_subscription != nullptr; });

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", -5,
        AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil("becomes live", poller,
        isLive(persistent_subscription));

    // Consume messages on live
    const std::vector<std::vector<uint8_t>> first_batch = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(first_batch);

    executeUntil("fast consumer receives",
        [&] { return aeron_subscription_poll(fast_subscription, [](void*, const uint8_t*, size_t, aeron_header_t*){}, nullptr, 10); },
        [&] { return aeron_subscription_image_count(fast_subscription) > 0; });

    // Stop recording and purge it
    aeron_archive_stop_recording_exclusive_publication(persistent_publication.archive(), persistent_publication.publication());

    int64_t deleted_segments;
    aeron_archive_purge_recording(
        &deleted_segments,
        persistent_publication.archive(),
        persistent_publication.recordingId());

    // Flood messages to make the persistent subscription fall behind and drop from live.
    // Interleave offering with polling the fast consumer so the publication can advance.
    for (int i = 0; i < 3; i++)
    {
        const std::vector<std::vector<uint8_t>> flood = generateFixedMessages(32, ONE_KB_MESSAGE_SIZE);
        for (const std::vector<uint8_t> &msg : flood)
        {
            while (aeron_exclusive_publication_offer(
                persistent_publication.publication(), msg.data(), msg.size(), nullptr, nullptr) < 0)
            {
                aeron_subscription_poll(fast_subscription,
                    [](void*, const uint8_t*, size_t, aeron_header_t*){}, nullptr, 10);
            }
        }
        while (aeron_subscription_poll(fast_subscription,
            [](void*, const uint8_t*, size_t, aeron_header_t*){}, nullptr, 10) > 0)
        {
        }
    }

    // PS should fail because the recording no longer exists
    executeUntil("has failed", poller,
        hasFailed(persistent_subscription));

    ASSERT_GE(listener.error_count, 1);
    EXPECT_NE(std::string::npos, listener.last_error_message.find("recording"));

    aeron_subscription_close(fast_subscription, nullptr, nullptr);
}

// Verifies that an untethered spy subscription can fall back to replay when it falls behind.
TEST_F(AeronArchivePersistentSubscriptionTest, untetheredSpyCanFallbackToReplay)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        (SPY_PREFIX + MDC_PUBLICATION_CHANNEL + "|tether=false"),
        STREAM_ID,
        "aeron:udp?endpoint=localhost:0", -5,
        AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil("becomes live", poller,
        isLive(persistent_subscription));
    ASSERT_EQ(0, listener.live_left_count);

    // Flood messages via a fast consumer on a separate driver to make the untethered PS fall behind
    {
        DriverResource driver2;
        AeronResource aeron2(driver2.aeronDir());

        aeron_subscription_t *fast_subscription = nullptr;
        aeron_async_add_subscription_t *async_sub = nullptr;
        ASSERT_EQ(0, aeron_async_add_subscription(
            &async_sub, aeron2.aeron(), MDC_SUBSCRIPTION_CHANNEL.c_str(), STREAM_ID,
            nullptr, nullptr, nullptr, nullptr)) << aeron_errmsg();
        executeUntil("fast sub created",
            [&] { return aeron_async_add_subscription_poll(&fast_subscription, async_sub) >= 0 ? 1 : -1; },
            [&] { return fast_subscription != nullptr; });

        executeUntil("fast sub connected",
            [&] { return 0; },
            [&] { return aeron_subscription_image_count(fast_subscription) > 0; });

        std::vector<std::vector<uint8_t>> all_messages;
        for (int i = 0; i < 3; i++)
        {
            const std::vector<std::vector<uint8_t>> batch = generateFixedMessages(32, ONE_KB_MESSAGE_SIZE);
            for (const std::vector<uint8_t> &msg : batch)
            {
                while (aeron_exclusive_publication_offer(
                    persistent_publication.publication(), msg.data(), msg.size(), nullptr, nullptr) < 0)
                {
                    aeron_subscription_poll(fast_subscription,
                        [](void*, const uint8_t*, size_t, aeron_header_t*){}, nullptr, 10);
                }
            }
            all_messages.insert(all_messages.end(), batch.begin(), batch.end());
            while (aeron_subscription_poll(fast_subscription,
                [](void*, const uint8_t*, size_t, aeron_header_t*){}, nullptr, 10) > 0)
            {
            }
        }

        // PS should drop from live and fall back to replay
        executeUntil("drops from live to replay", poller,
            isReplaying(persistent_subscription));
        ASSERT_EQ(1, listener.live_left_count);

        // PS should replay and rejoin live
        executeUntil("replays and rejoins live", poller,
            [&] {
                return handler.messageCount() == all_messages.size() &&
                       aeron_archive_persistent_subscription_is_live(persistent_subscription);
            });

        ASSERT_EQ(all_messages, handler.messages());

        aeron_subscription_close(fast_subscription, nullptr, nullptr);
    }

}

// Verifies that an untethered persistent subscription can fall behind a tethered subscription
// without blocking it. An untethered persistent subscription and a tethered subscription are
// created on the same channel. Once the persistent subscription becomes live, 64 messages of
// 1KB each are published, filling the 64KB term buffer. Only the tethered subscription is
// polled during this time, causing the publisher to advance past the untethered persistent
// subscription's image, which is then closed by the media driver. The persistent subscription
// drops back to replay to catch up on all 64 messages and then transitions back to live.
TEST_F(AeronArchivePersistentSubscriptionTest, anUntetheredPersistentSubscriptionCanFallBehindATetheredSubscription)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, UNICAST_CHANNEL, STREAM_ID);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_live_channel(
        context, (UNICAST_CHANNEL + "|tether=false").c_str());

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    aeron_subscription_t *tethered_subscription = nullptr;
    aeron_async_add_subscription_t *async_add = nullptr;
    ASSERT_EQ(0, aeron_async_add_subscription(
        &async_add,
        aeron.aeron(),
        (UNICAST_CHANNEL + "|tether=true").c_str(),
        STREAM_ID,
        nullptr, nullptr, nullptr, nullptr)) << aeron_errmsg();

    executeUntil(
        "tethered subscription created",
        [&] { return aeron_async_add_subscription_poll(&tethered_subscription, async_add) >= 0 ? 1 : -1; },
        [&] { return tethered_subscription != nullptr; });

    executeUntil(
        "tethered subscription has image",
        [&] { return 0; },
        [&] { return aeron_subscription_image_count(tethered_subscription) > 0; });

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));

    // the term buffer is 64 KB
    const std::vector<std::vector<uint8_t>> payloads = generateFixedMessages(64, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(payloads);

    size_t fast_count = 0;
    executeUntil(
        "tethered subscription receives 64 messages",
        [&]
        {
            return aeron_subscription_poll(
                tethered_subscription,
                [](void *clientd, const uint8_t *, size_t, aeron_header_t *)
                {
                    (*static_cast<size_t *>(clientd))++;
                },
                &fast_count,
                10);
        },
        [&] { return fast_count == 64; });

    executeUntil(
        "persistent subscription receives 64 messages",
        [&]
        {
            return aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                MessageCapturingFragmentHandler::onFragment,
                &handler,
                1);
        },
        [&] { return handler.messageCount() == 64; });

    ASSERT_EQ(payloads, handler.messages());
    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    executeUntil(
        "becomes live again",
        [&]
        {
            return aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                MessageCapturingFragmentHandler::onFragment,
                &handler,
                1);
        },
        isLive(persistent_subscription));

    aeron_subscription_close(tethered_subscription, nullptr, nullptr);

}

TEST_F(AeronArchivePersistentSubscriptionTest, aTetheredPersistentSubscriptionDoesNotFallBehindAnUntetheredSubscription)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, UNICAST_CHANNEL, STREAM_ID);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_live_channel(
        context, (UNICAST_CHANNEL + "|tether=true").c_str());

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    aeron_subscription_t *untethered_subscription = nullptr;
    aeron_async_add_subscription_t *async_add = nullptr;
    ASSERT_EQ(0, aeron_async_add_subscription(
        &async_add,
        aeron.aeron(),
        (UNICAST_CHANNEL + "|tether=false").c_str(),
        STREAM_ID,
        nullptr, nullptr, nullptr, nullptr)) << aeron_errmsg();

    executeUntil(
        "untethered subscription created",
        [&] { return aeron_async_add_subscription_poll(&untethered_subscription, async_add) >= 0 ? 1 : -1; },
        [&] { return untethered_subscription != nullptr; });

    executeUntil(
        "untethered subscription has image",
        [&] { return 0; },
        [&] { return aeron_subscription_image_count(untethered_subscription) > 0; });

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));

    const std::vector<std::vector<uint8_t>> payloads = generateFixedMessages(64, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(payloads);

    executeUntil(
        "receives 64 messages",
        [&]
        {
            return aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                MessageCapturingFragmentHandler::onFragment,
                &handler,
                1);
        },
        [&] { return handler.messageCount() == 64; });

    ASSERT_EQ(payloads, handler.messages());
    ASSERT_TRUE(aeron_archive_persistent_subscription_is_live(persistent_subscription));

    executeUntil(
        "untethered subscription disconnects",
        [&] { return 0; },
        [&] { return !aeron_subscription_is_connected(untethered_subscription); });

    aeron_subscription_close(untethered_subscription, nullptr, nullptr);

}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldHandleReplayBeingAheadOfLive)
{
    TestArchive archive = createArchive(m_aeronDir);

    const std::string fc_min_pub_channel = "aeron:udp?control=localhost:2000|control-mode=dynamic|fc=min";
    const std::string sub_channel = "aeron:udp?control=localhost:2000|rcv-wnd=4k";

    PersistentPublication persistent_publication(m_aeronDir, fc_min_pub_channel, STREAM_ID);

    // Phase 1: create a slow subscription on a separate media driver with a limited receive window.
    DriverResource driver2;
    AeronResource aeron2(driver2.aeronDir());

    aeron_subscription_t *slow_subscription = nullptr;
    aeron_async_add_subscription_t *async_add = nullptr;
    ASSERT_EQ(0, aeron_async_add_subscription(
        &async_add,
        aeron2.aeron(),
        sub_channel.c_str(),
        STREAM_ID,
        nullptr, nullptr, nullptr, nullptr)) << aeron_errmsg();

    executeUntil(
        "slow subscription created",
        [&] { return aeron_async_add_subscription_poll(&slow_subscription, async_add) >= 0 ? 1 : -1; },
        [&] { return slow_subscription != nullptr; });

    executeUntil(
        "slow subscription has image",
        [&] { return 0; },
        [&] { return aeron_subscription_image_count(slow_subscription) > 0; });

    // Phase 2: publish 32 large messages and persist
    persistent_publication.persist(generateFixedMessages(32, ONE_KB_MESSAGE_SIZE));

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_live_channel(context, sub_channel.c_str());

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // Phase 3: replay 32 messages
    executeUntil(
        "receives 32 messages",
        poller,
        [&] { return handler.messageCount() == 32; });

    // Phase 4: wait for 2 receivers (persistent subscription's live sub + slow subscription)
    executeUntil(
        "2 receivers connected",
        poller,
        [&] { return persistent_publication.receiverCount() == 2; });

    // Phase 5: switch to live, polling both subscriptions concurrently
    executeUntil(
        "becomes live",
        [&]
        {
            int work = poller();
            work += aeron_subscription_poll(
                slow_subscription,
                [](void *, const uint8_t *, size_t, aeron_header_t *) {},
                nullptr,
                10);
            return work;
        },
        isLive(persistent_subscription));

    // join_difference = live_position - replay_position. Replay consumed all 32 frames
    // (~32 KB) while live is held at the slow subscription's 4 KB receive window, so any
    // value the PS exposes here must be <= 0 — replay is at-or-ahead-of live, never behind.
    // We don't pin a specific value because the path through the PS state machine is racy:
    //   * If the live image attaches while PS is still in REPLAY, the REPLAY handler enters
    //     ATTEMPT_SWITCH with join_difference = live_position - replay_position (negative,
    //     ~rcv_wnd - 32*frame_length), and ATTEMPT_SWITCH preserves that value through to LIVE.
    //   * If the replay image closes first (e.g. PS already at recorded stop_position when
    //     the live image arrives), PS shortcuts via ADD_LIVE_SUBSCRIPTION → AWAIT_LIVE → LIVE,
    //     which sets join_difference = 0 before transitioning. Both are valid handling.
    ASSERT_LE(aeron_archive_persistent_subscription_join_difference(persistent_subscription), 0);

    aeron_subscription_close(slow_subscription, nullptr, nullptr);

}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldCloseCleanlyDuringReplay)
{
    TestArchive archive = createArchive(m_aeronDir);

    const std::vector<std::vector<uint8_t>> messages = generateFixedMessages(20, ONE_KB_MESSAGE_SIZE);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;

    executeUntil(
        "receives first message",
        [&]
        {
            return aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                MessageCapturingFragmentHandler::onFragment,
                &handler,
                1);
        },
        [&] { return handler.messageCount() == 1; });

    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));
    ASSERT_LT(handler.messageCount(), messages.size());

}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldCloseCleanlyDuringAwaitArchiveConnection)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    // Close immediately without polling
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldStayOnReplayWhenLiveCannotConnect)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> payloads = generateFixedMessages(5, 128);
    persistent_publication.persist(payloads);

    AeronResource aeron(m_aeronDir);

    // Use an unreachable live channel
    const std::string unreachable_live_channel = "aeron:udp?control=localhost:49582|control-mode=dynamic";

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_context_set_message_timeout_ns(archive_ctx, UINT64_C(500000000));
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId(),
        unreachable_live_channel,
        STREAM_ID,
        "aeron:udp?endpoint=localhost:0",
        -5,
        AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_START);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil(
        "receives all payloads",
        poller,
        [&] { return handler.messageCount() == payloads.size(); });
    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    // Poll a while longer — should stay replaying since live is unreachable
    executeUntil(
        "warns about no live image",
        poller,
        [&] { return listener.error_count > 0; });
    ASSERT_EQ(
        "No image became available on the live subscription within the message timeout.",
        listener.last_error_message);
    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    // Publish more while still replaying
    const std::vector<std::vector<uint8_t>> payloads2 = generateFixedMessages(3, 128);
    persistent_publication.persist(payloads2);

    executeUntil(
        "receives more",
        poller,
        [&] { return handler.messageCount() == payloads.size() + payloads2.size(); });
    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    std::vector<std::vector<uint8_t>> allMessages;
    allMessages.insert(allMessages.end(), payloads.begin(), payloads.end());
    allMessages.insert(allMessages.end(), payloads2.begin(), payloads2.end());
    ASSERT_TRUE(MessagesEq(allMessages, handler.messages()));

    // Live is unreachable, so no live_joined callback should ever fire.
    ASSERT_EQ(0, listener.live_joined_count);

    // The sticky `live_image_deadline_breached` flag should prevent repeated
    // breach-error firing. If the flag mis-resets while live stays unreachable,
    // error_count would grow unboundedly.
    const int errors_before_extended_polling = listener.error_count;
    for (int i = 0; i < 500; ++i) { poller(); }
    ASSERT_EQ(errors_before_extended_polling, listener.error_count)
        << "PS re-fired breach error while live remained unreachable — sticky flag likely misbehaving";
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldStartFromStoppedRecordingAndJoinLiveWhenLiveHasNotAdvanced)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> payloads = generateFixedMessages(8, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(payloads);

    persistent_publication.stop();

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL,
        STREAM_ID,
        "aeron:udp?endpoint=localhost:0",
        -5,
        0);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil(
        "receives all",
        poller,
        [&] { return handler.messageCount() == payloads.size(); });
    ASSERT_TRUE(MessagesEq(payloads, handler.messages()));

    // Should transition to live since live hasn't advanced past the recording
    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));

    const std::vector<std::vector<uint8_t>> live_payloads = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
    persistent_publication.offer(live_payloads);

    executeUntil(
        "receives live",
        poller,
        [&] { return handler.messageCount() == payloads.size() + live_payloads.size(); });

    ASSERT_TRUE(aeron_archive_persistent_subscription_is_live(persistent_subscription));

    std::vector<std::vector<uint8_t>> all_messages;
    all_messages.insert(all_messages.end(), payloads.begin(), payloads.end());
    all_messages.insert(all_messages.end(), live_payloads.begin(), live_payloads.end());
    ASSERT_EQ(all_messages, handler.messages());

}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldStartFromStoppedRecordingAndErrorWhenLiveHasAdvanced)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> payloads = generateFixedMessages(8, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(payloads);

    persistent_publication.stop();

    // Advance live past the recording: first add a temporary subscriber so the publication
    // is connected, then offer the unrecorded messages and drain them.
    const std::vector<std::vector<uint8_t>> advance_payloads = generateFixedMessages(1, ONE_KB_MESSAGE_SIZE);

    {
        aeron_t *pub_aeron = aeron_archive_context_get_aeron(
            aeron_archive_get_archive_context(persistent_publication.archive()));

        aeron_subscription_t *temp_subscription = nullptr;
        aeron_async_add_subscription_t *async_add = nullptr;
        ASSERT_EQ(0, aeron_async_add_subscription(
            &async_add, pub_aeron,
            MDC_SUBSCRIPTION_CHANNEL.c_str(), STREAM_ID,
            nullptr, nullptr, nullptr, nullptr)) << aeron_errmsg();

        executeUntil(
            "temp subscription created",
            [&] { return aeron_async_add_subscription_poll(&temp_subscription, async_add) >= 0 ? 1 : -1; },
            [&] { return temp_subscription != nullptr; });

        executeUntil(
            "temp subscription connected",
            [&] { return 0; },
            [&] { return aeron_subscription_is_connected(temp_subscription); });

        // Now offer the unrecorded messages (publication is connected to temp subscriber)
        persistent_publication.offer(advance_payloads);

        size_t received = 0;
        executeUntil(
            "drain temp subscription",
            [&]
            {
                int fragments = aeron_subscription_poll(
                    temp_subscription,
                    [](void *clientd, const uint8_t *, size_t, aeron_header_t *)
                    {
                        (*static_cast<size_t *>(clientd))++;
                    },
                    &received,
                    10);
                return fragments;
            },
            [&] { return received == advance_payloads.size(); });

        aeron_subscription_close(temp_subscription, nullptr, nullptr);
    }

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL,
        STREAM_ID,
        "aeron:udp?endpoint=localhost:0",
        -5,
        0);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler, 7);

    executeUntil(
        "receives all recorded",
        poller,
        [&] { return handler.messageCount() == payloads.size(); });
    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    // Should fail because live has advanced past the recording
    executeUntil(
        "has failed",
        poller,
        hasFailed(persistent_subscription));

    ASSERT_EQ(payloads.size(), handler.messageCount());
    ASSERT_EQ(1, listener.error_count);

}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldRetryAndRecoverWhenLiveIsNotAvailableDuringStartUp)
{
    TestArchive archive = createArchive(m_aeronDir);

    AeronResource aeron(m_aeronDir);

    aeron_exclusive_publication_t *exclusive_publication = nullptr;
    aeron_async_add_exclusive_publication_t *async_add_pub = nullptr;
    ASSERT_EQ(0, aeron_async_add_exclusive_publication(
        &async_add_pub, aeron.aeron(), MDC_PUBLICATION_CHANNEL.c_str(), STREAM_ID)) << aeron_errmsg();

    executeUntil(
        "exclusive publication created",
        [&] { return aeron_async_add_exclusive_publication_poll(&exclusive_publication, async_add_pub) >= 0 ? 1 : -1; },
        [&] { return exclusive_publication != nullptr; });

    aeron_archive_context_t *recording_archive_ctx = createArchiveContext();
    aeron_archive_t *recording_archive = nullptr;
    ASSERT_EQ(0, aeron_archive_connect(&recording_archive, recording_archive_ctx)) << aeron_errmsg();

    int64_t subscription_id;
    ASSERT_EQ(0, aeron_archive_start_recording(
        &subscription_id, recording_archive, MDC_PUBLICATION_CHANNEL.c_str(), STREAM_ID,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL, true)) << aeron_errmsg();

    aeron_publication_constants_t constants;
    aeron_exclusive_publication_constants(exclusive_publication, &constants);
    aeron_counters_reader_t *counters_reader = aeron_counters_reader(aeron.aeron());
    int32_t rec_pos_id;
    waitUntil("recording counter found",
        [&] { rec_pos_id = aeron_archive_recording_pos_find_counter_id_by_session_id(
            counters_reader, constants.session_id);
            return rec_pos_id != AERON_NULL_COUNTER_ID; });
    int64_t recording_id = aeron_archive_recording_pos_get_recording_id(counters_reader, rec_pos_id);

    const std::vector<std::vector<uint8_t>> messages = generateFixedMessages(5, 128);
    for (const std::vector<uint8_t> &msg : messages)
    {
        int64_t offer_result;
        executeUntil("offer message",
            [&]
            {
                offer_result = aeron_exclusive_publication_offer(
                    exclusive_publication, msg.data(), msg.size(), nullptr, nullptr);
                return offer_result > 0 ? 1 : 0;
            },
            [&] { return offer_result > 0; });
    }
    waitUntil("recording persisted",
        [&] { return *aeron_counters_reader_addr(counters_reader, rec_pos_id) >= aeron_exclusive_publication_position(exclusive_publication); });

    // Verify a temporary subscriber can connect, then revoke the publication
    aeron_subscription_t *temp_subscription = nullptr;
    aeron_async_add_subscription_t *async_add_sub = nullptr;
    ASSERT_EQ(0, aeron_async_add_subscription(
        &async_add_sub, aeron.aeron(), MDC_SUBSCRIPTION_CHANNEL.c_str(), STREAM_ID,
        nullptr, nullptr, nullptr, nullptr)) << aeron_errmsg();

    executeUntil(
        "temp subscription created",
        [&] { return aeron_async_add_subscription_poll(&temp_subscription, async_add_sub) >= 0 ? 1 : -1; },
        [&] { return temp_subscription != nullptr; });
    executeUntil(
        "temp subscription connected",
        [&] { return 0; },
        [&] { return aeron_subscription_is_connected(temp_subscription); });
    executeUntil(
        "temp subscription has image",
        [&] { return 0; },
        [&] { return aeron_subscription_image_count(temp_subscription) > 0; });

    aeron_exclusive_publication_revoke(exclusive_publication, nullptr, nullptr);
    executeUntil(
        "exclusive publication closed",
        [&] { return 0; },
        [&] { return aeron_exclusive_publication_is_closed(exclusive_publication); });
    executeUntil(
        "temp subscription image gone",
        [&] { return 0; },
        [&] { return aeron_subscription_image_count(temp_subscription) == 0; });

    aeron_archive_context_t *persistent_subscription_archive_ctx = createArchiveContext();
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        persistent_subscription_archive_ctx,
        recording_id,
        MDC_SUBSCRIPTION_CHANNEL,
        STREAM_ID,
        "aeron:udp?endpoint=localhost:0",
        -5,
        AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    auto poller = [&] {
        return aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription, MessageCapturingFragmentHandler::onFragment, nullptr, 1);
    };

    executeUntil(
        "has error",
        poller,
        [&] { return listener.error_count > 0; });
    ASSERT_EQ(0, listener.live_joined_count);

    // Create a new publication — subscription should recover
    aeron_exclusive_publication_t *new_publication = nullptr;
    aeron_async_add_exclusive_publication_t *async_add_new_pub = nullptr;
    ASSERT_EQ(0, aeron_async_add_exclusive_publication(
        &async_add_new_pub, aeron.aeron(), MDC_PUBLICATION_CHANNEL.c_str(), STREAM_ID)) << aeron_errmsg();

    executeUntil(
        "new publication created",
        [&] { return aeron_async_add_exclusive_publication_poll(&new_publication, async_add_new_pub) >= 0 ? 1 : -1; },
        [&] { return new_publication != nullptr; });

    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));

    // After recovery, verify live_joined fires exactly once and no further
    // breach errors fire. Catches sticky-deadline-breach flag misbehavior.
    ASSERT_EQ(1, listener.live_joined_count)
        << "Expected exactly one live_joined after recovery";
    const int errors_at_recovery = listener.error_count;
    for (int i = 0; i < 500; ++i) { poller(); }
    ASSERT_EQ(errors_at_recovery, listener.error_count)
        << "PS reported additional errors after successful LIVE recovery";
    ASSERT_EQ(1, listener.live_joined_count)
        << "PS reported additional live_joined callbacks after successful LIVE recovery";

    ps_guard.release();
    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
    aeron_subscription_close(temp_subscription, nullptr, nullptr);
    aeron_archive_close(recording_archive);
    aeron_archive_context_close(persistent_subscription_archive_ctx);
    aeron_archive_context_close(recording_archive_ctx);
}

// Verifies that closing a persistent subscription with an externally provided client
// doesn't close the client when the persistent subscription is closed
TEST_F(AeronArchivePersistentSubscriptionTest, shouldCloseContextWhenClosingSubscriptionWithExternalAeronClient)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    // Close the persistent subscription
    ps_guard.release();
    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();

    // Ensure that the externally supplied Aeron instance isn't closed
    ASSERT_FALSE(aeron_is_closed(aeron.aeron()));

    archive_ctx_guard.release();
    aeron_archive_context_close(archive_ctx);
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldCatchUpToLiveDuringAttemptSwitchWithControlledPoll)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> initial_messages = generateFixedMessages(3, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(initial_messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL,
        STREAM_ID,
        "aeron:udp?endpoint=localhost:0",
        -5,
        0);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;

    // Poll with fragment limit of 1 to slow down replay consumption
    auto slow_poller = [&]
    {
        return aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription,
            MessageCapturingFragmentHandler::onFragment,
            &handler,
            1);
    };

    executeUntil(
        "receives first message",
        slow_poller,
        [&] { return handler.messageCount() == 1; });
    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    // While replaying, aggressively publish more messages so live advances well ahead of replay
    const std::vector<std::vector<uint8_t>> concurrent_messages = generateFixedMessages(50, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(concurrent_messages);

    // Now poll to live — the PS must go through ATTEMPT_SWITCH with a gap between
    // replay and live positions, exercising the catchup handlers
    auto fast_poller = [&]
    {
        return aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription,
            MessageCapturingFragmentHandler::onFragment,
            &handler,
            10);
    };

    executeUntil(
        "becomes live",
        fast_poller,
        isLive(persistent_subscription));

    ASSERT_EQ(initial_messages.size() + concurrent_messages.size(), handler.messageCount());

}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldCatchUpToLiveDuringAttemptSwitchWithUncontrolledPoll)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> initial_messages = generateFixedMessages(3, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(initial_messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL,
        STREAM_ID,
        "aeron:udp?endpoint=localhost:0",
        -5,
        0);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;

    auto slow_poller = [&]
    {
        return aeron_archive_persistent_subscription_poll(
            persistent_subscription,
            [](void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
            {
                static_cast<MessageCapturingFragmentHandler *>(clientd)->addMessage(buffer, length);
            },
            &handler,
            1);
    };

    executeUntil(
        "receives first message",
        slow_poller,
        [&] { return handler.messageCount() == 1; });
    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    const std::vector<std::vector<uint8_t>> concurrent_messages = generateFixedMessages(50, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(concurrent_messages);

    auto fast_poller = [&]
    {
        return aeron_archive_persistent_subscription_poll(
            persistent_subscription,
            [](void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
            {
                static_cast<MessageCapturingFragmentHandler *>(clientd)->addMessage(buffer, length);
            },
            &handler,
            10);
    };

    executeUntil(
        "becomes live",
        fast_poller,
        isLive(persistent_subscription));

    ASSERT_EQ(initial_messages.size() + concurrent_messages.size(), handler.messageCount());

}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldReplayAndSwitchToLiveWithUncontrolledPoll)
{
    TestArchive archive = createArchive(m_aeronDir);

    const std::vector<std::vector<uint8_t>> messages = generateRandomMessages(3);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeUncontrolledPoller(persistent_subscription, handler);

    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));

    const std::vector<std::vector<uint8_t>> live_messages = generateRandomMessages(3);
    persistent_publication.offer(live_messages);

    executeUntil(
        "receives all live messages",
        poller,
        [&] { return handler.messageCount() == messages.size() + live_messages.size(); });

    std::vector<std::vector<uint8_t>> all_messages = messages;
    all_messages.insert(all_messages.end(), live_messages.begin(), live_messages.end());
    ASSERT_TRUE(MessagesEq(all_messages, handler.messages()));

}

class AeronArchivePersistentSubscriptionAllReplayChannelTypesTest
    : public AeronArchivePersistentSubscriptionTest,
      public testing::WithParamInterface<std::string>
{
};

INSTANTIATE_TEST_SUITE_P(
    ,
    AeronArchivePersistentSubscriptionAllReplayChannelTypesTest,
    testing::Values(
        "aeron:udp?endpoint=localhost:0",
        "aeron:udp?endpoint=localhost:10001",
        "aeron:udp?control=localhost:10001|control-mode=response"
    ));

TEST_P(AeronArchivePersistentSubscriptionAllReplayChannelTypesTest, shouldCloseCleanlyInDifferentStates)
{
    TestArchive archive = createArchive(m_aeronDir);

    const std::vector<std::vector<uint8_t>> messages = generateRandomMessages(3);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);
    persistent_publication.persist(messages);

    const std::string& replay_channel = GetParam();
    std::vector<int64_t> states_up_to_live;
    PrintingListener printingListener;

    {
        AeronResource aeron(m_aeronDir);

        aeron_archive_context_t *archive_ctx = createArchiveContext();
        ArchiveContextGuard archive_ctx_guard(archive_ctx);
        aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
            aeron.aeron(),
            archive_ctx,
            persistent_publication.recordingId(),
            IPC_CHANNEL,
            STREAM_ID,
            replay_channel,
            STREAM_ID + 1,
            0);

        aeron_archive_persistent_subscription_context_set_listener(context, printingListener.listener());

        PersistentSubscriptionContextGuard context_guard(context);
        aeron_archive_persistent_subscription_t *persistent_subscription;
        ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
        context_guard.release();
        PersistentSubscriptionGuard ps_guard(persistent_subscription);

        int64_t *state = aeron_counter_addr(aeron_archive_persistent_subscription_context_get_state_counter(context));

        MessageCapturingFragmentHandler handler;
        auto poller = [&]
        {
            int work_count = aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                MessageCapturingFragmentHandler::onFragment,
                &handler,
                1);

            int64_t current_state = *state;
            if (states_up_to_live.empty() || current_state != states_up_to_live.back())
            {
                states_up_to_live.push_back(current_state);
            }

            return work_count;
        };

        executeUntil(
            "becomes live",
            poller,
            isLive(persistent_subscription));

    }

    for (size_t i = 0; i < states_up_to_live.size() - 1; i++)
    {
        int64_t close_state = states_up_to_live[i];

        AeronResource aeron(m_aeronDir);

        aeron_archive_context_t *archive_ctx = createArchiveContext();
        ArchiveContextGuard archive_ctx_guard(archive_ctx);
        aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
            aeron.aeron(),
            archive_ctx,
            persistent_publication.recordingId(),
            IPC_CHANNEL,
            STREAM_ID,
            replay_channel,
            STREAM_ID + 1,
            0);

        aeron_archive_persistent_subscription_context_set_listener(context, printingListener.listener());

        PersistentSubscriptionContextGuard context_guard(context);
        aeron_archive_persistent_subscription_t *persistent_subscription;
        ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
        context_guard.release();
        PersistentSubscriptionGuard ps_guard(persistent_subscription);

        int64_t *state = aeron_counter_addr(aeron_archive_persistent_subscription_context_get_state_counter(context));

        MessageCapturingFragmentHandler handler;
        auto poller = makeControlledPoller(persistent_subscription, handler, 1);

        executeUntil(
            "reaches close state " + std::to_string(close_state),
            poller,
            [&] { return *state == close_state; });

    }
}
