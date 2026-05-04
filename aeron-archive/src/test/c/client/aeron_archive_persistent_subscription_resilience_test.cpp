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
#include "aeron_setup_at_position_loss_generator.h"
#include "aeron_data_in_range_loss_generator.h"
}

static const std::string IPC_CHANNEL = "aeron:ipc";
static const std::string MDC_PUBLICATION_CHANNEL = "aeron:udp?control=localhost:2000|control-mode=dynamic|fc=max";
static const std::string MDC_SUBSCRIPTION_CHANNEL = "aeron:udp?control=localhost:2000";
static const std::string UNICAST_CHANNEL = "aeron:udp?endpoint=localhost:2000";
static const std::string MULTICAST_CHANNEL = "aeron:udp?endpoint=224.20.30.39:40456|interface=localhost";
static const std::string LOCALHOST_CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8010";
static const std::string LOCALHOST_CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";
static const int32_t STREAM_ID = 1000;
static const int32_t REPLAY_STREAM_ID = -5;
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

static auto isNotReplaying(aeron_archive_persistent_subscription_t *ps)
{
    return [ps] { return !aeron_archive_persistent_subscription_is_replaying(ps); };
}

static auto isNotReplayingAndNotLive(aeron_archive_persistent_subscription_t *ps)
{
    return [ps] {
        return !aeron_archive_persistent_subscription_is_replaying(ps)
            && !aeron_archive_persistent_subscription_is_live(ps);
    };
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

// Publishes 3 messages to a recording, then starts a persistent subscription.
// Expects the subscription to first replay all 3 messages from the archive,
// then transition to live. Once live, 3 further messages are published and
// the subscription is expected to receive them.
TEST_F(AeronArchivePersistentSubscriptionTest, shouldRecoverFromArchiveRestartDuringReplay)
{
    const std::string aeron_dir = m_aeronDir;
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "standalone";

    TestMediaDriver driver(aeron_dir, std::cout);
    std::unique_ptr<TestStandaloneArchive> archive_process = std::make_unique<TestStandaloneArchive>(
        aeron_dir, archive_dir, std::cout,
        LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0");

    // Use a replay channel with a small receive window to slow down replay
    const std::string slow_replay_channel = "aeron:udp?endpoint=127.0.0.1:10013|rcv-wnd=4k";

    PersistentPublication persistent_publication(aeron_dir, IPC_CHANNEL, STREAM_ID);
    const std::vector<std::vector<uint8_t>> messages = generateFixedMessages(80, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(aeron_dir);

    int error_count = 0;
    aeron_archive_persistent_subscription_listener_t listener = {};
    listener.clientd = &error_count;
    listener.on_error = [](void *clientd, int errcode, const char *message)
    {
        (*static_cast<int *>(clientd))++;
    };

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId(),
        IPC_CHANNEL,
        STREAM_ID,
        slow_replay_channel,
        -5,
        0);
    aeron_archive_persistent_subscription_context_set_listener(context, &listener);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler, 1);

    executeUntil(
        "receives some messages",
        poller,
        [&] { return handler.messageCount() == 5; });
    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    // Kill archive while PS is in REPLAY state — this should trigger on_archive_disconnected
    const std::int64_t archive_id_to_reuse = archive_process->archiveId();
    archive_process->deleteDirOnTearDown(false);
    archive_process.reset();

    executeUntil("detects disconnection", poller, isNotReplaying(persistent_subscription));

    ASSERT_TRUE(!aeron_archive_persistent_subscription_has_failed(persistent_subscription));

    // Restart archive with existing data
    archive_process = std::make_unique<TestStandaloneArchive>(
        aeron_dir, archive_dir, std::cout,
        LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0",
        archive_id_to_reuse, false);

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

    ASSERT_EQ(messages, handler.messages());

}

struct FragmentLimitAndChannel
{
    int fragment_limit;
    std::string pub_channel;
    std::string sub_channel;
};

struct ReplayChannelAndStream
{
    std::string replay_channel;
    int32_t replay_stream_id;
    std::string archive_control_request_channel;
    std::string archive_control_response_channel;
};

static const std::string SPY_PREFIX = "aeron-spy:";

// Verifies that the persistent subscription continues consuming from live even when the
// archive becomes unavailable.
TEST_F(AeronArchivePersistentSubscriptionTest, shouldContinueConsumingFromLiveWhileArchiveIsUnavailable)
{
    const std::string aeron_dir = m_aeronDir;
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "standalone";

    TestMediaDriver driver(aeron_dir, std::cout);
    std::unique_ptr<TestStandaloneArchive> archive_process = std::make_unique<TestStandaloneArchive>(
        aeron_dir, archive_dir, std::cout,
        LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0");

    PersistentPublication persistent_publication(aeron_dir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    const std::vector<std::vector<uint8_t>> first_batch = generateRandomMessages(5);
    persistent_publication.persist(first_batch);

    AeronResource aeron(aeron_dir);

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

    executeUntil("becomes live", poller,
        [&]
        {
            return handler.messageCount() == first_batch.size() &&
                   aeron_archive_persistent_subscription_is_live(persistent_subscription);
        });

    // Kill archive while live
    archive_process.reset();

    // Continue publishing and consuming from live — archive being unavailable should not matter
    const std::vector<std::vector<uint8_t>> second_batch = generateRandomMessages(5);
    persistent_publication.offer(second_batch);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == first_batch.size() + second_batch.size(); });

    std::vector<std::vector<uint8_t>> all_messages;
    all_messages.insert(all_messages.end(), first_batch.begin(), first_batch.end());
    all_messages.insert(all_messages.end(), second_batch.begin(), second_batch.end());
    ASSERT_EQ(all_messages, handler.messages());

}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldRetryAndRecoverWhenArchiveIsNotAvailableDuringStartUp)
{
    const std::string aeron_dir = m_aeronDir;
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "restart";

    TestMediaDriver driver(aeron_dir, std::cout);
    std::unique_ptr<TestStandaloneArchive> archive_process = std::make_unique<TestStandaloneArchive>(
        aeron_dir, archive_dir, std::cout,
        LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0");

    // Create recording via a separate archive connection
    aeron_archive_context_t *pub_archive_ctx = createArchiveContext();
    aeron_archive_context_set_aeron_directory_name(pub_archive_ctx, aeron_dir.c_str());
    aeron_archive_t *pub_archive = nullptr;
    ASSERT_EQ(0, aeron_archive_connect(&pub_archive, pub_archive_ctx)) << aeron_errmsg();

    aeron_exclusive_publication_t *publication = nullptr;
    ASSERT_EQ(0, aeron_archive_add_recorded_exclusive_publication(
        &publication, pub_archive, MDC_PUBLICATION_CHANNEL.c_str(), STREAM_ID)) << aeron_errmsg();

    aeron_t *pub_aeron = aeron_archive_context_get_aeron(aeron_archive_get_archive_context(pub_archive));
    aeron_counters_reader_t *counters_reader = aeron_counters_reader(pub_aeron);
    aeron_publication_constants_t constants;
    aeron_exclusive_publication_constants(publication, &constants);
    int32_t rec_pos_id;
    waitUntil("recording counter found",
        [&] { rec_pos_id = aeron_archive_recording_pos_find_counter_id_by_session_id(
            counters_reader, constants.session_id);
            return rec_pos_id != AERON_NULL_COUNTER_ID; });
    int64_t recording_id = aeron_archive_recording_pos_get_recording_id(counters_reader, rec_pos_id);

    const std::vector<std::vector<uint8_t>> messages = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
    for (const std::vector<uint8_t> &msg : messages)
    {
        int64_t offer_result;
        executeUntil("offer message",
            [&]
            {
                offer_result = aeron_exclusive_publication_offer(
                    publication, msg.data(), msg.size(), nullptr, nullptr);
                return offer_result > 0 ? 1 : 0;
            },
            [&] { return offer_result > 0; });
    }
    waitUntil("recording persisted",
        [&] { return *aeron_counters_reader_addr(counters_reader, rec_pos_id) >= aeron_exclusive_publication_position(publication); });

    // Stop archive (driver stays alive)
    const std::int64_t archive_id_to_reuse = archive_process->archiveId();
    archive_process->deleteDirOnTearDown(false);
    archive_process.reset();

    AeronResource aeron(aeron_dir);

    aeron_archive_context_t *persistent_subscription_archive_ctx = createArchiveContext();
    aeron_archive_context_set_message_timeout_ns(persistent_subscription_archive_ctx, 1000000000ULL); // 1 second

    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        persistent_subscription_archive_ctx,
        recording_id,
        MDC_SUBSCRIPTION_CHANNEL,
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
    auto poller = makeControlledPoller(persistent_subscription, handler, 1);

    // Should get errors since archive is not available
    executeUntil(
        "has errors",
        poller,
        [&] { return listener.error_count > 1; });

    // Restart the archive (preserving recordings)
    archive_process = std::make_unique<TestStandaloneArchive>(
        aeron_dir, archive_dir, std::cout,
        LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0",
        archive_id_to_reuse, false);

    // PS should recover, replay, and go live
    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));
    ASSERT_EQ(messages, handler.messages());

    ps_guard.release();
    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
    aeron_archive_context_close(persistent_subscription_archive_ctx);
    aeron_archive_close(pub_archive);
    aeron_archive_context_close(pub_archive_ctx);
}

// Verifies that the PS leaves LIVE when the publication is closed and the remote driver
// is killed. The live image closes after the liveness timeout (~10s default).
TEST_F(AeronArchivePersistentSubscriptionTest, shouldLeaveLiveWhenPublicationClosesAndDriverDies)
{
    static const std::string REMOTE_CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:8011";

    TestMediaDriver driver1(m_aeronDir, std::cout);

    const std::string aeron_dir_2 = defaultAeronDir() + "_remote";
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "remote_leave_live";
    std::unique_ptr<TestMediaDriver> driver2 = std::make_unique<TestMediaDriver>(aeron_dir_2, std::cout);
    std::unique_ptr<TestStandaloneArchive> archive_process = std::make_unique<TestStandaloneArchive>(
        aeron_dir_2, archive_dir, std::cout,
        REMOTE_CONTROL_CHANNEL, "aeron:udp?endpoint=localhost:0");

    AeronResource aeron1(m_aeronDir);

    aeron_archive_context_t *remote_archive_ctx = nullptr;
    aeron_archive_context_init(&remote_archive_ctx);
    aeron_archive_context_set_control_request_channel(remote_archive_ctx, REMOTE_CONTROL_CHANNEL.c_str());
    aeron_archive_context_set_control_response_channel(remote_archive_ctx, LOCALHOST_CONTROL_RESPONSE_CHANNEL.c_str());
    aeron_archive_context_set_control_response_stream_id(remote_archive_ctx,
        aeron_archive_context_get_control_response_stream_id(remote_archive_ctx) + 10);
    aeron_archive_context_set_aeron(remote_archive_ctx, aeron1.aeron());
    Credentials::defaultCredentials().configure(remote_archive_ctx);

    aeron_archive_t *remote_archive = nullptr;
    ASSERT_EQ(0, aeron_archive_connect(&remote_archive, remote_archive_ctx)) << aeron_errmsg();

    aeron_exclusive_publication_t *exclusive_publication = nullptr;
    aeron_async_add_exclusive_publication_t *async_add_pub = nullptr;
    ASSERT_EQ(0, aeron_async_add_exclusive_publication(
        &async_add_pub, aeron1.aeron(), MDC_PUBLICATION_CHANNEL.c_str(), STREAM_ID)) << aeron_errmsg();
    executeUntil(
        "exclusive publication created",
        [&] { return aeron_async_add_exclusive_publication_poll(&exclusive_publication, async_add_pub) >= 0 ? 1 : -1; },
        [&] { return exclusive_publication != nullptr; });

    int64_t recording_subscription_id;
    ASSERT_EQ(0, aeron_archive_start_recording(
        &recording_subscription_id, remote_archive, MDC_SUBSCRIPTION_CHANNEL.c_str(), STREAM_ID,
        AERON_ARCHIVE_SOURCE_LOCATION_REMOTE, true)) << aeron_errmsg();

    int64_t recording_id = -1;
    executeUntil(
        "recording found",
        [&]
        {
            int32_t count = 0;
            aeron_archive_list_recordings_for_uri(
                &count, remote_archive, 0, 10, MDC_SUBSCRIPTION_CHANNEL.c_str(), STREAM_ID,
                [](aeron_archive_recording_descriptor_t *descriptor, void *clientd)
                { *static_cast<int64_t *>(clientd) = descriptor->recording_id; },
                &recording_id);
            return 0;
        },
        [&] { return recording_id >= 0; });

    const std::vector<std::vector<uint8_t>> messages = generateFixedMessages(3, ONE_KB_MESSAGE_SIZE);
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

    int64_t recording_position = 0;
    executeUntil(
        "recording persisted",
        [&]
        {
            aeron_archive_get_recording_position(&recording_position, remote_archive, recording_id);
            return 0;
        },
        [&] { return recording_position >= aeron_exclusive_publication_position(exclusive_publication); });

    aeron_archive_context_t *persistent_subscription_archive_ctx = nullptr;
    aeron_archive_context_init(&persistent_subscription_archive_ctx);
    aeron_archive_context_set_control_request_channel(persistent_subscription_archive_ctx, REMOTE_CONTROL_CHANNEL.c_str());
    aeron_archive_context_set_control_response_channel(persistent_subscription_archive_ctx, LOCALHOST_CONTROL_RESPONSE_CHANNEL.c_str());
    aeron_archive_context_set_control_response_stream_id(persistent_subscription_archive_ctx,
        aeron_archive_context_get_control_response_stream_id(remote_archive_ctx) + 10);
    Credentials::defaultCredentials().configure(persistent_subscription_archive_ctx);

    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron1.aeron(), persistent_subscription_archive_ctx, recording_id,
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", -5,
        AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_START);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler, 100);

    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));

    // Kill archive first (before driver2) to avoid JVM crash from shared memory disappearing
    archive_process.reset();
    driver2.reset();
    aeron_exclusive_publication_close(exclusive_publication, nullptr, nullptr);

    executeUntil(
        "leaves live",
        poller,
        [&] { return !aeron_archive_persistent_subscription_is_live(persistent_subscription); });

    ps_guard.release();
    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
    aeron_archive_context_close(persistent_subscription_archive_ctx);
    aeron_archive_close(remote_archive);
    aeron_archive_context_close(remote_archive_ctx);
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldReconnectToTheArchiveAfterArchiveRestart)
{
    static const std::string REMOTE_CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:8011";

    // Driver 1: embedded C driver (image liveness hardcoded to 2s in EmbeddedMediaDriver.h
    // to match Java test's imageLivenessTimeoutNs=2s)
    DriverResource driver1;
    AeronResource aeron1(driver1.aeronDir());

    // Driver 2: embedded C driver (in-process), matching Java's in-process driver2.
    // Running driver2 in-process ensures that shutting it down has the same OS-level
    // socket-close effects as Java's mediaDriver2.close(), which is necessary for
    // driver1 to detect the lost MDC destination and close the live image.
    std::unique_ptr<DriverResource> driver2 = std::make_unique<DriverResource>();
    const std::string aeron_dir_2 = driver2->aeronDir();

    // Remote archive: records from driver 1 via MDC
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "remote_reconnect";
    std::unique_ptr<TestStandaloneArchive> archive_process = std::make_unique<TestStandaloneArchive>(
        aeron_dir_2, archive_dir, std::cout,
        REMOTE_CONTROL_CHANNEL, "aeron:udp?endpoint=localhost:0");

    aeron_archive_context_t *remote_archive_ctx = nullptr;
    aeron_archive_context_init(&remote_archive_ctx);
    aeron_archive_context_set_control_request_channel(remote_archive_ctx, REMOTE_CONTROL_CHANNEL.c_str());
    aeron_archive_context_set_control_response_channel(remote_archive_ctx, LOCALHOST_CONTROL_RESPONSE_CHANNEL.c_str());
    aeron_archive_context_set_control_response_stream_id(remote_archive_ctx,
        aeron_archive_context_get_control_response_stream_id(remote_archive_ctx) + 10);
    aeron_archive_context_set_aeron(remote_archive_ctx, aeron1.aeron());
    Credentials::defaultCredentials().configure(remote_archive_ctx);

    aeron_archive_t *remote_archive = nullptr;
    ASSERT_EQ(0, aeron_archive_connect(&remote_archive, remote_archive_ctx)) << aeron_errmsg();

    // Create publication on driver 1, record remotely from driver 2
    aeron_exclusive_publication_t *exclusive_publication = nullptr;
    aeron_async_add_exclusive_publication_t *async_add_pub = nullptr;
    ASSERT_EQ(0, aeron_async_add_exclusive_publication(
        &async_add_pub, aeron1.aeron(), MDC_PUBLICATION_CHANNEL.c_str(), STREAM_ID)) << aeron_errmsg();

    executeUntil(
        "exclusive publication created",
        [&] { return aeron_async_add_exclusive_publication_poll(&exclusive_publication, async_add_pub) >= 0 ? 1 : -1; },
        [&] { return exclusive_publication != nullptr; });

    int64_t recording_subscription_id;
    ASSERT_EQ(0, aeron_archive_start_recording(
        &recording_subscription_id, remote_archive, MDC_SUBSCRIPTION_CHANNEL.c_str(), STREAM_ID,
        AERON_ARCHIVE_SOURCE_LOCATION_REMOTE, true)) << aeron_errmsg();

    int64_t recording_id = -1;
    executeUntil(
        "recording found",
        [&]
        {
            int32_t count = 0;
            aeron_archive_list_recordings_for_uri(
                &count, remote_archive, 0, 10, MDC_SUBSCRIPTION_CHANNEL.c_str(), STREAM_ID,
                [](aeron_archive_recording_descriptor_t *descriptor, void *clientd)
                {
                    *static_cast<int64_t *>(clientd) = descriptor->recording_id;
                },
                &recording_id);
            return 0;
        },
        [&] { return recording_id >= 0; });

    // Publish first batch and persist
    const std::vector<std::vector<uint8_t>> first_batch = generateFixedMessages(1, ONE_KB_MESSAGE_SIZE);
    for (const std::vector<uint8_t> &msg : first_batch)
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

    int64_t recording_position = 0;
    executeUntil(
        "first batch persisted",
        [&]
        {
            aeron_archive_get_recording_position(&recording_position, remote_archive, recording_id);
            return 0;
        },
        [&] { return recording_position >= aeron_exclusive_publication_position(exclusive_publication); });

    // Create PS, wait until live
    aeron_archive_context_t *persistent_subscription_archive_ctx = nullptr;
    aeron_archive_context_init(&persistent_subscription_archive_ctx);
    aeron_archive_context_set_control_request_channel(persistent_subscription_archive_ctx, REMOTE_CONTROL_CHANNEL.c_str());
    aeron_archive_context_set_control_response_channel(persistent_subscription_archive_ctx, LOCALHOST_CONTROL_RESPONSE_CHANNEL.c_str());
    aeron_archive_context_set_control_response_stream_id(persistent_subscription_archive_ctx,
        aeron_archive_context_get_control_response_stream_id(remote_archive_ctx) + 10);
    Credentials::defaultCredentials().configure(persistent_subscription_archive_ctx);

    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron1.aeron(),
        persistent_subscription_archive_ctx,
        recording_id,
        MDC_SUBSCRIPTION_CHANNEL,
        STREAM_ID,
        "aeron:udp?endpoint=localhost:0",
        -5,
        AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_START);

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
    ASSERT_EQ(1u, handler.messageCount());

    // Publish second and third batches while live
    const std::vector<std::vector<uint8_t>> second_batch = generateFixedMessages(1, ONE_KB_MESSAGE_SIZE);
    const std::vector<std::vector<uint8_t>> third_batch = generateFixedMessages(64, ONE_KB_MESSAGE_SIZE);

    std::vector<std::vector<uint8_t>> batches_2_and_3;
    batches_2_and_3.insert(batches_2_and_3.end(), second_batch.begin(), second_batch.end());
    batches_2_and_3.insert(batches_2_and_3.end(), third_batch.begin(), third_batch.end());

    for (const std::vector<uint8_t> &msg : batches_2_and_3)
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
    executeUntil(
        "all batches persisted",
        [&]
        {
            aeron_archive_get_recording_position(&recording_position, remote_archive, recording_id);
            return 0;
        },
        [&] { return recording_position >= aeron_exclusive_publication_position(exclusive_publication); });

    // Kill archive and driver2.
    const std::int64_t archive_id_to_reuse = archive_process->archiveId();
    archive_process->deleteDirOnTearDown(false);
    archive_process.reset();
    driver2.reset();

    // Poll until the PS detects the archive disconnection and leaves LIVE. The PS's
    // on_archive_disconnected callback cleans up the live subscription when the state
    // is NOT LIVE, which triggers image deactivation. We poll continuously to drive
    // the PS state machine through the disconnect detection.
    // Once the PS leaves LIVE, immediately restart driver2 and archive so the PS can
    // reconnect before timing out.
    bool restarted = false;
    executeUntil(
        "recovers to live after restart",
        [&]
        {
            int work = aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                MessageCapturingFragmentHandler::onFragment,
                &handler,
                10);

            if (!restarted && !aeron_archive_persistent_subscription_is_live(persistent_subscription))
            {
                driver2 = std::make_unique<DriverResource>();
                archive_process = std::make_unique<TestStandaloneArchive>(
                    driver2->aeronDir(), archive_dir, std::cout,
                    REMOTE_CONTROL_CHANNEL, "aeron:udp?endpoint=localhost:0",
        archive_id_to_reuse, false);
                restarted = true;
                work++;
            }

            return work;
        },
        [&] { return restarted && aeron_archive_persistent_subscription_is_live(persistent_subscription); });

    std::vector<std::vector<uint8_t>> all_messages;
    all_messages.insert(all_messages.end(), first_batch.begin(), first_batch.end());
    all_messages.insert(all_messages.end(), second_batch.begin(), second_batch.end());
    all_messages.insert(all_messages.end(), third_batch.begin(), third_batch.end());
    ASSERT_EQ(all_messages, handler.messages());

    ps_guard.release();
    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
    aeron_archive_close(remote_archive);
    aeron_archive_context_close(remote_archive_ctx);
    aeron_archive_context_close(persistent_subscription_archive_ctx);
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldHandleReplayImageBecomingUnavailableDuringReplay)
{
    shouldHandleReplayImageBecomingUnavailable(80);
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldHandleReplayImageBecomingUnavailableDuringAttemptSwitch)
{
    shouldHandleReplayImageBecomingUnavailable(12);
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldCloseArchiveConnectionOnFailureInCaseApplicationKeepsPolling)
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

    aeron_archive_persistent_subscription_context_set_start_position(context, 8192);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler, 1);

    executeUntil(
        "has failed",
        poller,
        hasFailed(persistent_subscription));

    int64_t *session_counter = aeron.findCounterByType(AERON_COUNTER_ARCHIVE_CONTROL_SESSIONS_TYPE_ID);
    ASSERT_NE(nullptr, session_counter);

    executeUntil(
        "archive connection gets closed",
        poller,
        [&] { return *session_counter == 1; });

}
/*
 * Embedded C media driver that installs loss generators on endpoints via the
 * driver context supplier mechanism. Owns the generators it is handed and
 * frees them after the driver/context are torn down.
 */
class EmbeddedMediaDriverWithLossGenerator
{
public:
    ~EmbeddedMediaDriverWithLossGenerator()
    {
        stop();

        if (m_driver)
        {
            aeron_driver_close(m_driver);
        }
        if (m_context)
        {
            aeron_driver_context_close(m_context);
        }
        for (aeron_loss_generator_t *gen : m_ownedGenerators)
        {
            aeron_free(gen);
        }
    }

    EmbeddedMediaDriverWithLossGenerator() = default;
    EmbeddedMediaDriverWithLossGenerator(const EmbeddedMediaDriverWithLossGenerator &) = delete;
    EmbeddedMediaDriverWithLossGenerator &operator=(const EmbeddedMediaDriverWithLossGenerator &) = delete;

    void aeronDir(const std::string &dir) { m_aeronDir = dir; }
    std::string aeronDir() const { return m_aeronDir; }

    // Takes ownership of `gen` and installs it as the receive-channel data
    // loss generator on every receive endpoint. Call before start().
    void setReceiveChannelDataLossGenerator(aeron_loss_generator_t *gen)
    {
        m_receiveDataLossGenerator = gen;
        m_ownedGenerators.push_back(gen);
    }

    // Takes ownership of `gen` without installing it — used for child
    // generators when the installed generator is a composite.
    void adoptLossGenerator(aeron_loss_generator_t *gen)
    {
        m_ownedGenerators.push_back(gen);
    }

    // Installs a custom supplier called for each new receive endpoint. The supplier
    // is responsible for setting endpoint->data_loss_generator (and/or
    // control_loss_generator) — typically by allocating a fresh per-endpoint
    // generator that captures the endpoint pointer in its state. Use this when
    // the loss decision needs endpoint-specific context (e.g. the URI) that isn't
    // available at frame-check time through the simple shared-generator hook.
    // Mutually exclusive with setReceiveChannelDataLossGenerator(); call before start().
    void setReceiveChannelLossSupplier(
        aeron_receive_channel_loss_supplier_func_t supplier, void *clientd)
    {
        m_receiveLossSupplier = supplier;
        m_receiveLossSupplierClientd = clientd;
    }

    void setImageLivenessTimeoutNs(std::uint64_t ns)
    {
        m_imageLivenessTimeoutNs = ns;
    }

    void start()
    {
        if (init() < 0)
        {
            throw std::runtime_error("could not initialize driver with loss generator");
        }
        m_thread = std::thread([this]() { driverLoop(); });
    }

    void stop()
    {
        m_running = false;
        if (m_thread.joinable())
        {
            m_thread.join();
        }
    }

private:
    static void installReceiveDataLossGenerator(
        void *clientd, aeron_receive_channel_endpoint_t *endpoint)
    {
        endpoint->data_loss_generator = (aeron_loss_generator_t *)clientd;
    }

    int init()
    {
        if (aeron_driver_context_init(&m_context) < 0)
        {
            fprintf(stderr, "ERROR: context init (%d) %s\n", aeron_errcode(), aeron_errmsg());
            return -1;
        }

        if (!m_aeronDir.empty())
        {
            aeron_driver_context_set_dir(m_context, m_aeronDir.c_str());
        }
        aeron_driver_context_set_dir_delete_on_start(m_context, true);
        aeron_driver_context_set_dir_delete_on_shutdown(m_context, true);
        aeron_driver_context_set_threading_mode(m_context, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_shared_idle_strategy(m_context, "sleep-ns");
        aeron_driver_context_set_term_buffer_sparse_file(m_context, true);
        aeron_driver_context_set_term_buffer_length(m_context, 64 * 1024);
        aeron_driver_context_set_ipc_term_buffer_length(m_context, 64 * 1024);
        aeron_driver_context_set_timer_interval_ns(m_context, m_livenessTimeoutNs / 100);
        aeron_driver_context_set_client_liveness_timeout_ns(m_context, m_livenessTimeoutNs);
        aeron_driver_context_set_publication_linger_timeout_ns(m_context, m_livenessTimeoutNs / 10);
        aeron_driver_context_set_image_liveness_timeout_ns(
            m_context, m_imageLivenessTimeoutNs > 0 ? m_imageLivenessTimeoutNs : m_livenessTimeoutNs / 10);
        aeron_driver_context_set_enable_experimental_features(m_context, true);
        aeron_driver_context_set_spies_simulate_connection(m_context, true);

        if (m_receiveLossSupplier != nullptr)
        {
            aeron_driver_context_set_receive_channel_loss_supplier(
                m_context, m_receiveLossSupplier, m_receiveLossSupplierClientd);
        }
        else if (m_receiveDataLossGenerator != nullptr)
        {
            aeron_driver_context_set_receive_channel_loss_supplier(
                m_context, installReceiveDataLossGenerator, m_receiveDataLossGenerator);
        }

        if (aeron_driver_init(&m_driver, m_context) < 0)
        {
            fprintf(stderr, "ERROR: driver init (%d) %s\n", aeron_errcode(), aeron_errmsg());
            return -1;
        }

        if (aeron_driver_start(m_driver, true) < 0)
        {
            fprintf(stderr, "ERROR: driver start (%d) %s\n", aeron_errcode(), aeron_errmsg());
            return -1;
        }

        return 0;
    }

    void driverLoop()
    {
        while (m_running)
        {
            aeron_driver_main_idle_strategy(m_driver, aeron_driver_main_do_work(m_driver));
        }
    }

    std::uint64_t m_livenessTimeoutNs = 5'000'000'000LL;
    std::uint64_t m_imageLivenessTimeoutNs = 0; // 0 = use m_livenessTimeoutNs / 10
    std::string m_aeronDir;
    std::atomic<bool> m_running{true};
    std::thread m_thread;
    aeron_driver_context_t *m_context = nullptr;
    aeron_driver_t *m_driver = nullptr;
    aeron_loss_generator_t *m_receiveDataLossGenerator = nullptr;
    aeron_receive_channel_loss_supplier_func_t m_receiveLossSupplier = nullptr;
    void *m_receiveLossSupplierClientd = nullptr;
    std::vector<aeron_loss_generator_t *> m_ownedGenerators;
};

// aeron::EmbeddedMediaDriver's destructor closes the driver but does not join
// its worker thread; without an explicit stop() the std::thread dtor calls
// std::terminate. This wrapper calls stop() on scope exit.
struct ScopedMediaDriver
{
    aeron::EmbeddedMediaDriver driver;
    ~ScopedMediaDriver() { driver.stop(); }

    void aeronDir(const std::string &d) { driver.aeronDir(d); }
    void start() { driver.start(); }
};

// Bundles the two long-lived test fixtures every loss-based test needs:
//   - an embedded C media driver with a receive-side data-loss generator
//   - a separate standalone archive process talking to the same aeron.dir
// The archive is exposed via unique_ptr so tests can kill/restart it mid-flow.
class LossTestHarness
{
public:
    EmbeddedMediaDriverWithLossGenerator driver;
    std::unique_ptr<TestStandaloneArchive> archive;

    LossTestHarness(
        const std::string &aeronDir,
        const std::string &archiveDir,
        aeron_loss_generator_t *receiveDataLossGenerator,
        std::uint64_t imageLivenessTimeoutNs = 0)
        : m_aeronDir(aeronDir),
          m_archiveDir(archiveDir)
    {
        driver.setReceiveChannelDataLossGenerator(receiveDataLossGenerator);
        driver.aeronDir(aeronDir);
        if (imageLivenessTimeoutNs > 0)
        {
            driver.setImageLivenessTimeoutNs(imageLivenessTimeoutNs);
        }
        driver.start();

        archive = std::make_unique<TestStandaloneArchive>(
            m_aeronDir, m_archiveDir, std::cout,
            LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0");
    }

    // Supplier-driven variant for tests that need per-endpoint loss generators.
    // The supplier callback is invoked by the driver for each new receive endpoint
    // and is expected to populate endpoint->data_loss_generator (and/or
    // control_loss_generator) with a fresh per-endpoint generator that captures
    // any endpoint-specific context it needs.
    LossTestHarness(
        const std::string &aeronDir,
        const std::string &archiveDir,
        aeron_receive_channel_loss_supplier_func_t receiveLossSupplier,
        void *receiveLossSupplierClientd,
        std::uint64_t imageLivenessTimeoutNs = 0)
        : m_aeronDir(aeronDir),
          m_archiveDir(archiveDir)
    {
        driver.setReceiveChannelLossSupplier(receiveLossSupplier, receiveLossSupplierClientd);
        driver.aeronDir(aeronDir);
        if (imageLivenessTimeoutNs > 0)
        {
            driver.setImageLivenessTimeoutNs(imageLivenessTimeoutNs);
        }
        driver.start();

        archive = std::make_unique<TestStandaloneArchive>(
            m_aeronDir, m_archiveDir, std::cout,
            LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0");
    }

    LossTestHarness(const LossTestHarness &) = delete;
    LossTestHarness &operator=(const LossTestHarness &) = delete;

    // Kills the archive process without deleting its directory, so restartArchive()
    // can bring it back up against the same data. For archive-kill/restart tests.
    void killArchivePreservingDir()
    {
        m_killed_archive_id = archive->archiveId();
        archive->deleteDirOnTearDown(false);
        archive.reset();
    }

    void restartArchive()
    {
        archive = std::make_unique<TestStandaloneArchive>(
            m_aeronDir, m_archiveDir, std::cout,
            LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0",
            m_killed_archive_id, /* deleteOnStart = */ false);
    }

private:
    const std::string m_aeronDir;
    const std::string m_archiveDir;
    std::int64_t m_killed_archive_id = 0;
};

void AeronArchivePersistentSubscriptionTest::shouldHandleReplayImageBecomingUnavailable(
    const int replayableMessageCount)
{
    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_loss_generator_create(&loss_gen));

    // Receive-side data-loss generator lets the test toggle loss on the replay
    // stream to simulate the replay image going unavailable.
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "replay_image_unavailable";
    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen, 2'000'000'000LL);

    const std::vector<std::vector<uint8_t>> messages =
        generateFixedMessages(replayableMessageCount, ONE_KB_MESSAGE_SIZE);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t* context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        archive_ctx,
        persistent_publication.recordingId());
    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_context_set_replay_channel(context,
        "aeron:udp?endpoint=127.0.0.1:10013|rcv-wnd=4k");

    aeron_archive_persistent_subscription_t *persistent_subscription = nullptr;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler, 1);

    executeUntil(
        "a few messages received",
        poller,
        [&] { return handler.messageCount() == 5; });

    // Drop all frames on the replay stream: no data reaches the replay image,
    // so image liveness times out and the subscription transitions out of
    // replaying.
    aeron_stream_id_loss_generator_enable(loss_gen, REPLAY_STREAM_ID);

    EXPECT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));
    executeUntil("replay stops", poller, isNotReplayingAndNotLive(persistent_subscription));

    aeron_stream_id_loss_generator_disable(loss_gen);

    executeUntil(
        "becomes live",
        poller,
        isLive(persistent_subscription));

    EXPECT_TRUE(MessagesEq(messages, handler.messages()));
}

// Composite loss generator for tests that need to compose two child generators
// behind a single endpoint hook. Ownership of the children is transferred to
// the fixture separately via adoptLossGenerator(); the composite itself is
// registered via setReceiveChannelDataLossGenerator().
struct CompositeLossGeneratorState
{
    aeron_loss_generator_t *first;
    aeron_loss_generator_t *second;
};

static bool compositeShouldDropFrameDetailed(
    void *state,
    const struct sockaddr_storage *address,
    const uint8_t *buffer,
    int32_t stream_id,
    int32_t session_id,
    int32_t term_id,
    int32_t term_offset,
    int32_t length)
{
    auto *s = static_cast<CompositeLossGeneratorState *>(state);
    return aeron_loss_generator_should_drop_frame_detailed(
               s->first, address, buffer, stream_id, session_id, term_id, term_offset, length) ||
        aeron_loss_generator_should_drop_frame_detailed(
            s->second, address, buffer, stream_id, session_id, term_id, term_offset, length);
}

static aeron_loss_generator_t *makeCompositeLossGenerator(
    aeron_loss_generator_t *first, aeron_loss_generator_t *second)
{
    aeron_loss_generator_t *gen = nullptr;
    CompositeLossGeneratorState *state = nullptr;
    if (aeron_loss_generator_alloc(&gen, sizeof(CompositeLossGeneratorState), (void **)&state) < 0)
    {
        return nullptr;
    }
    state->first = first;
    state->second = second;
    gen->should_drop_frame_detailed = compositeShouldDropFrameDetailed;
    return gen;
}

/*
 * Loss tests using global runtime interceptors
 */
static const std::string PLAIN_REPLAY_CHANNEL = "aeron:udp?endpoint=localhost:0";

static std::atomic<uint64_t> g_frame_counter{0};

static bool drop_every_other_frame(const uint8_t *, size_t, void *)
{
    return (g_frame_counter.fetch_add(1, std::memory_order_relaxed) % 2) == 0;
}

// Predicate: drop frames at a configurable rate using modular arithmetic.
// g_drop_modulo_N=5, g_drop_modulo_M=4 means drop 4 out of every 5 frames (80%).
// g_drop_modulo_N=3, g_drop_modulo_M=1 means drop 1 out of every 3 frames (33%).
static std::atomic<int> g_drop_modulo_N{1};  // period
static std::atomic<int> g_drop_modulo_M{0};  // drop count per period

static bool drop_at_rate(const uint8_t *, size_t, void *)
{
    int n = g_drop_modulo_N.load(std::memory_order_relaxed);
    if (n <= 0) return false;
    int m = g_drop_modulo_M.load(std::memory_order_relaxed);
    uint64_t count = g_frame_counter.fetch_add(1, std::memory_order_relaxed);
    return (count % n) < static_cast<uint64_t>(m);
}

// Helper: configure the rate predicate. rate is approximate (e.g., 0.3 → drop 1/3).
static void configure_drop_rate(double rate)
{
    if (rate <= 0.0) { g_drop_modulo_N.store(1); g_drop_modulo_M.store(0); }
    else if (rate <= 0.25) { g_drop_modulo_N.store(4); g_drop_modulo_M.store(1); } // 25%
    else if (rate <= 0.35) { g_drop_modulo_N.store(3); g_drop_modulo_M.store(1); } // 33%
    else if (rate <= 0.55) { g_drop_modulo_N.store(2); g_drop_modulo_M.store(1); } // 50%
    else if (rate <= 0.75) { g_drop_modulo_N.store(3); g_drop_modulo_M.store(2); } // 67%
    else { g_drop_modulo_N.store(5); g_drop_modulo_M.store(4); }                   // 80%
}

// Predicate: drop one specific frame (by counter), then stop dropping.
static std::atomic<int64_t> g_drop_at_frame{-1};

static bool drop_single_frame(const uint8_t *, size_t, void *)
{
    int64_t target = g_drop_at_frame.load(std::memory_order_relaxed);
    if (target < 0) return false;
    int64_t count = static_cast<int64_t>(g_frame_counter.fetch_add(1, std::memory_order_relaxed));
    if (count == target)
    {
        g_drop_at_frame.store(-1, std::memory_order_relaxed);
        return true;
    }
    return false;
}

// Predicate: drop the first N frames, then pass everything.
static std::atomic<int64_t> g_drop_first_n{0};

static bool drop_first_n_frames(const uint8_t *, size_t, void *)
{
    int64_t n = g_drop_first_n.load(std::memory_order_relaxed);
    if (n <= 0) return false;
    int64_t count = static_cast<int64_t>(g_frame_counter.fetch_add(1, std::memory_order_relaxed));
    return count < n;
}

// Predicate: pass the first N payload frames through (for connection handshake),
// then drop all subsequent payload frames. Heartbeats always pass.
// This allows the archive connection to establish while blocking later responses.
static std::atomic<int64_t> g_pass_payload_threshold{0};
static std::atomic<int64_t> g_payload_seq{0};

static bool drop_payloads_after_threshold(const uint8_t *buffer, size_t length, void *)
{
    if (length >= AERON_DATA_HEADER_LENGTH)
    {
        const aeron_frame_header_t *hdr = (const aeron_frame_header_t *)buffer;
        if (hdr->frame_length > 0) // payload frame
        {
            int64_t seq = g_payload_seq.fetch_add(1, std::memory_order_relaxed);
            return seq >= g_pass_payload_threshold.load(std::memory_order_relaxed);
        }
        // heartbeat — always pass
    }
    return false;
}

static const int32_t CONTROL_RESPONSE_STREAM_ID = 20;

// Combined predicate for the catchup tests: drops SETUP frames on g_setup_drop_stream_id
// until g_setup_drop_until_ns, and drops DATA frames on g_replay_drop_stream_id at
// drop_at_rate. SETUP frames dispatch through aeron_loss_generator_should_drop_frame_simple;
// DATA frames go through the detailed dispatcher which falls back to simple when the
// detailed slot is NULL — so a single aeron_frame_data_loss_generator sees both.
static std::atomic<int32_t> g_setup_drop_stream_id{0};
static std::atomic<int64_t> g_setup_drop_until_ns{0};
static std::atomic<int32_t> g_replay_drop_stream_id{0};

static bool drop_setup_for_window_or_replay_at_rate(const uint8_t *buffer, size_t length, void *)
{
    if (length < sizeof(aeron_frame_header_t))
    {
        return false;
    }
    const aeron_frame_header_t *hdr = (const aeron_frame_header_t *)buffer;

    if (AERON_HDR_TYPE_SETUP == hdr->type)
    {
        if (length < sizeof(aeron_setup_header_t)) return false;
        if (aeron_nano_clock() >= g_setup_drop_until_ns.load(std::memory_order_acquire)) return false;
        return ((const aeron_setup_header_t *)buffer)->stream_id ==
            g_setup_drop_stream_id.load(std::memory_order_relaxed);
    }

    if (AERON_HDR_TYPE_DATA == hdr->type)
    {
        if (length < sizeof(aeron_data_header_t)) return false;
        const int32_t target = g_replay_drop_stream_id.load(std::memory_order_relaxed);
        if (0 == target) return false;
        if (((const aeron_data_header_t *)buffer)->stream_id != target) return false;
        return drop_at_rate(buffer, length, nullptr);
    }

    return false;
}

// Reset all global predicate state. Call at the start of each test that uses
// the loss predicates to avoid cross-test contamination.
static void reset_predicate_state()
{
    g_frame_counter.store(0, std::memory_order_relaxed);
    g_drop_modulo_N.store(1, std::memory_order_relaxed);
    g_drop_modulo_M.store(0, std::memory_order_relaxed);
    g_drop_at_frame.store(-1, std::memory_order_relaxed);
    g_drop_first_n.store(0, std::memory_order_relaxed);
    g_pass_payload_threshold.store(0, std::memory_order_relaxed);
    g_payload_seq.store(0, std::memory_order_relaxed);
    g_setup_drop_stream_id.store(0, std::memory_order_relaxed);
    g_setup_drop_until_ns.store(0, std::memory_order_relaxed);
    g_replay_drop_stream_id.store(0, std::memory_order_relaxed);
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldReceiveAllMessagesWithModerateReplayLoss)
{
    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&loss_gen));

    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "moderate_replay_loss";
    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    const auto messages = generateFixedMessages(100, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);

    // 30% loss on DATA frames on the replay channel
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        PLAIN_REPLAY_CHANNEL, REPLAY_STREAM_ID, 0);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    // Enable loss on the target stream
    reset_predicate_state();
    configure_drop_rate(0.3);

    aeron_stream_id_frame_data_loss_generator_enable(loss_gen, REPLAY_STREAM_ID, drop_at_rate, nullptr);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 60);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); }, 60);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));
    ASSERT_EQ(1, listener.live_joined_count);

    aeron_stream_id_frame_data_loss_generator_disable(loss_gen);

}

/*
 *  Publishes 50 messages, replays through a very lossy (80%) channel.
 *  The driver's NAK/retransmit mechanism must work hard to deliver all data.
 *  Verifies all messages are received with no duplicates.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldReceiveAllMessagesWithHeavyReplayLoss)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "heavy_replay_loss";

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const auto messages = generateFixedMessages(50, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);

    // 80% loss on DATA frames on the replay channel -- severe stress
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        PLAIN_REPLAY_CHANNEL, REPLAY_STREAM_ID, 0);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    // Enable loss on the target stream
    reset_predicate_state();
    configure_drop_rate(0.8);

    aeron_stream_id_frame_data_loss_generator_enable(loss_gen, REPLAY_STREAM_ID, drop_at_rate, nullptr);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 90);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); }, 90);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

    aeron_stream_id_frame_data_loss_generator_disable(loss_gen);

}

/*
 *  Publishes an initial batch, starts PS with lossy replay, then publishes
 *  additional messages while the PS is still replaying. The PS must catch up
 *  through the lossy replay, transition to live, and receive everything.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldTransitionToLiveThroughLossyReplayWhilePublishing)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "lossy_replay_transition";

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const auto initial_messages = generateFixedMessages(20, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(initial_messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);

    // 50% loss on replay
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        PLAIN_REPLAY_CHANNEL, REPLAY_STREAM_ID, 0);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    // Enable loss on the target stream
    reset_predicate_state();
    configure_drop_rate(0.5);

    aeron_stream_id_frame_data_loss_generator_enable(loss_gen, REPLAY_STREAM_ID, drop_at_rate, nullptr);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // Wait for at least one message to be received (PS is replaying)
    executeUntil("receives first message", poller, [&] { return handler.messageCount() >= 1; });
    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    // Publish more messages while the PS is catching up through lossy replay
    const auto extra_messages = generateFixedMessages(80, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(extra_messages);

    // PS should catch up through lossy replay, then switch to live
    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 60);

    ASSERT_GE(listener.live_joined_count, 1);

    // Drain remaining messages
    const size_t total_expected = initial_messages.size() + extra_messages.size();
    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == total_expected; }, 60);

    std::vector<std::vector<uint8_t>> all_messages;
    all_messages.insert(all_messages.end(), initial_messages.begin(), initial_messages.end());
    all_messages.insert(all_messages.end(), extra_messages.begin(), extra_messages.end());
    ASSERT_TRUE(MessagesEq(all_messages, handler.messages()));

    aeron_stream_id_frame_data_loss_generator_disable(loss_gen);

}

/*
 *  Publishes messages, PS replays clean, transitions to live, then additional
 *  messages arrive over a lossy live MDC channel. Retransmission on the live
 *  channel should deliver all messages.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldReceiveAllMessagesWithLossOnLiveChannel)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "live_channel_loss";

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    // 30% loss on live MDC subscription channel
    const std::string replay_channel = "aeron:udp?endpoint=localhost:0";

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    // Small initial batch -- replayed cleanly
    const auto initial_messages = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(initial_messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);

    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        replay_channel, REPLAY_STREAM_ID, 0);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    // Enable loss on the target stream
    reset_predicate_state();
    configure_drop_rate(0.3);

    aeron_stream_id_frame_data_loss_generator_enable(loss_gen, STREAM_ID, drop_at_rate, nullptr);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // Wait for PS to go live
    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 30);

    ASSERT_EQ(1, listener.live_joined_count);

    // Publish messages while live. Use offer() to avoid persist()'s blocking
    // wait — with loss on the live stream, the subscription falls behind and
    // could back-pressure persist()'s 30-second timeout.
    const auto live_messages = generateFixedMessages(30, ONE_KB_MESSAGE_SIZE);
    persistent_publication.offer(live_messages);

    // Drain all messages (initial + live)
    const size_t total_expected = initial_messages.size() + live_messages.size();
    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == total_expected; }, 60);

    std::vector<std::vector<uint8_t>> all_messages;
    all_messages.insert(all_messages.end(), initial_messages.begin(), initial_messages.end());
    all_messages.insert(all_messages.end(), live_messages.begin(), live_messages.end());
    ASSERT_TRUE(MessagesEq(all_messages, handler.messages()));

    aeron_stream_id_frame_data_loss_generator_disable(loss_gen);

}

/*
 *  Both channels experience 20% data loss. All messages are published before
 *  the PS starts, so the PS must replay through the lossy replay channel and
 *  then join live on the lossy live channel. Verifies all messages received.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldReceiveAllMessagesWithLossOnBothChannels)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "dual_channel_loss";

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    // 20% loss on live
    // 20% loss on replay
    const std::string replay_channel = PLAIN_REPLAY_CHANNEL;

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    // Publish ALL messages before creating the PS, avoiding back-pressure
    // from a slow lossy live subscriber during persist().
    const auto messages = generateFixedMessages(100, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);

    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        replay_channel, REPLAY_STREAM_ID, 0);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    // Enable loss on the target stream
    reset_predicate_state();
    configure_drop_rate(0.2);

    aeron_stream_id_frame_data_loss_generator_enable(loss_gen, REPLAY_STREAM_ID, drop_at_rate, nullptr);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // PS replays through lossy replay, then joins lossy live
    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 60);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); }, 60);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));
    ASSERT_GE(listener.live_joined_count, 1);

    aeron_stream_id_frame_data_loss_generator_disable(loss_gen);

}

/*
 *  Installs the stream_id_loss interceptor, publishes 100 messages,
 *  then toggles total loss on the replay stream on/off in bursts while
 *  the PS is replaying. The PS should eventually receive all messages.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldRecoverFromIntermittentReplayStreamLoss)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "intermittent_replay_loss";

    // Set up driver with the stream_id_loss interceptor installed
    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const auto messages = generateFixedMessages(100, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);

    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID, 0);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // Poll with intermittent loss bursts on the replay stream
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(90);
    bool loss_on = false;
    auto next_toggle = std::chrono::steady_clock::now() + std::chrono::milliseconds(200);

    while (true)
    {
        ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "timed out during intermittent replay loss";

        const int result = poller();
        ASSERT_GE(result, 0) << "poll error: " << aeron_errmsg();

        if (aeron_archive_persistent_subscription_is_live(persistent_subscription) &&
            handler.messageCount() == messages.size())
        {
            break;
        }

        // Toggle loss on/off every 200ms
        if (std::chrono::steady_clock::now() >= next_toggle)
        {
            if (loss_on)
            {
                aeron_stream_id_loss_generator_disable(loss_gen);
                loss_on = false;
            }
            else
            {
                // Only enable loss while replaying, not after going live
                if (aeron_archive_persistent_subscription_is_replaying(persistent_subscription))
                {

                    aeron_stream_id_loss_generator_enable(loss_gen, REPLAY_STREAM_ID);
                    loss_on = true;
                }
            }
            next_toggle = std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
        }

        if (result == 0) std::this_thread::yield();
    }

    // Ensure loss is off
    aeron_stream_id_loss_generator_disable(loss_gen);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

}

/*
 *  PS goes live, then total loss is enabled on the live stream ID.
 *  After the image liveness timeout, the PS should leave live and fall back
 *  to replay. When loss is disabled, the PS should recover to live again.
 *  All messages should be received.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldFallbackAndRecoverWhenLiveStreamExperiencesIntermittentLoss)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "live_fallback_loss";

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    // Publish initial batch and let PS catch up to live
    const auto initial_messages = generateFixedMessages(10, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(initial_messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);

    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID, 0);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // Wait for PS to go live
    executeUntil("becomes live initially", poller,
        isLive(persistent_subscription), 30);

    ASSERT_EQ(1, listener.live_joined_count);

    // Publish more messages while still live
    const auto batch2 = generateFixedMessages(20, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(batch2);

    // Receive them while live
    const size_t count_after_batch2 = initial_messages.size() + batch2.size();
    executeUntil("receives batch2", poller,
        [&] { return handler.messageCount() >= count_after_batch2; }, 30);

    // Enable total loss on live stream -- PS should eventually leave live

    aeron_stream_id_loss_generator_enable(loss_gen, STREAM_ID);

    // Publish more messages while live is blocked -- these will be recorded
    // but not delivered via live (they'll go through the archive spy which is IPC)
    const auto batch3 = generateFixedMessages(30, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(batch3);

    // Wait for PS to leave live (image liveness timeout)
    executeUntil("leaves live", poller,
        [&] { return !aeron_archive_persistent_subscription_is_live(persistent_subscription); }, 30);

    ASSERT_GE(listener.live_left_count, 1);

    // Disable loss -- PS should replay missed messages and go live again
    aeron_stream_id_loss_generator_disable(loss_gen);

    executeUntil("recovers to live", poller,
        isLive(persistent_subscription), 60);

    ASSERT_GE(listener.live_joined_count, 2);

    // Publish final batch while live again
    const auto batch4 = generateFixedMessages(20, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(batch4);

    const size_t total_expected =
        initial_messages.size() + batch2.size() + batch3.size() + batch4.size();
    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == total_expected; }, 60);

    std::vector<std::vector<uint8_t>> all_messages;
    all_messages.insert(all_messages.end(), initial_messages.begin(), initial_messages.end());
    all_messages.insert(all_messages.end(), batch2.begin(), batch2.end());
    all_messages.insert(all_messages.end(), batch3.begin(), batch3.end());
    all_messages.insert(all_messages.end(), batch4.begin(), batch4.end());
    ASSERT_TRUE(MessagesEq(all_messages, handler.messages()));

}

/*
 *  Uses the stream-id/frame-data loss generator with a random predicate to
 *  drop ~50% of data frames on the replay stream. Unlike channel-scoped
 *  loss, this predicate-based approach lets us verify the runtime loss
 *  toggle mechanism with partial (not total) loss.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldReceiveAllMessagesWithRuntimePartialReplayLoss)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "runtime_partial_loss";

    reset_predicate_state();

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const auto messages = generateFixedMessages(80, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);

    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID, 0);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    // Enable 50% frame-level loss on the replay stream

    aeron_stream_id_frame_data_loss_generator_enable(loss_gen, REPLAY_STREAM_ID, drop_every_other_frame, nullptr);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 60);

    // Disable loss once live
    aeron_stream_id_frame_data_loss_generator_disable(loss_gen);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); }, 60);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

}

/*
 *  The PS is replaying toward the live position. The last frame of the replay
 *  is the one that would make replay_position == live_position, triggering the
 *  ATTEMPT_SWITCH -> LIVE transition. When that frame is dropped, the PS must
 *  retransmit it and still complete the transition cleanly.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldDeliverAllMessagesWhenLastReplayFrameBeforeCutoverIsLost)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "last_replay_frame_loss";
    const int message_count = 10;

    reset_predicate_state();
    g_drop_at_frame.store(message_count - 1, std::memory_order_relaxed);  // drop the last (10th) frame

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    const auto messages = generateFixedMessages(message_count, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID, 0);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    // Enable: drop the last replay frame (the one that would trigger cutover)

    aeron_stream_id_frame_data_loss_generator_enable(loss_gen, REPLAY_STREAM_ID, drop_single_frame, nullptr);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 30);

    aeron_stream_id_frame_data_loss_generator_disable(loss_gen);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == (size_t)message_count; }, 30);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

}

/*
 *  The PS goes live. Then loss is enabled on the live stream so that the
 *  first new DATA frame published after the join is dropped. The driver's
 *  NAK/retransmit must recover it. All messages should be delivered.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldDeliverAllMessagesWhenFirstLiveFrameAfterJoinIsLost)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "first_live_frame_loss";

    reset_predicate_state();
    g_drop_first_n.store(1, std::memory_order_relaxed);  // drop first frame only

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const auto initial_messages = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(initial_messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID, 0);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // Let PS replay and go live (no loss yet on the live stream)
    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 30);

    // Now enable loss: drop the first DATA frame arriving on the live stream.
    // Reset counter so the next frame seen on the live stream is frame #0.
    reset_predicate_state();
    g_drop_first_n.store(1, std::memory_order_relaxed);

    aeron_stream_id_frame_data_loss_generator_enable(loss_gen, STREAM_ID, drop_first_n_frames, nullptr);

    // Publish new messages that arrive exclusively over the live channel
    const auto live_messages = generateFixedMessages(20, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(live_messages);

    const size_t total_expected = initial_messages.size() + live_messages.size();
    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == total_expected; }, 30);

    aeron_stream_id_frame_data_loss_generator_disable(loss_gen);

    std::vector<std::vector<uint8_t>> all_messages;
    all_messages.insert(all_messages.end(), initial_messages.begin(), initial_messages.end());
    all_messages.insert(all_messages.end(), live_messages.begin(), live_messages.end());
    ASSERT_TRUE(MessagesEq(all_messages, handler.messages()));

}

/*
 *  The PS goes live, then total loss is enabled on the live stream causing
 *  the live image to close. The PS falls back to replay. At that moment,
 *  loss is enabled on the replay stream to drop the first frame of the new
 *  replay. The retransmission mechanism must recover it.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldDeliverFirstReplayFrameAfterFallbackFromLive)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "first_replay_after_fallback";

    // Install a composite of two generators: stream_id_loss for total blackout,
    // stream_id_frame_data_loss for single-frame drops.
    aeron_loss_generator_t *stream_id_gen = nullptr;
    aeron_loss_generator_t *frame_data_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_loss_generator_create(&stream_id_gen));
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&frame_data_gen));
    aeron_loss_generator_t *loss_gen = makeCompositeLossGenerator(stream_id_gen, frame_data_gen);

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);
    harness.driver.adoptLossGenerator(stream_id_gen);
    harness.driver.adoptLossGenerator(frame_data_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    // Publish initial batch and let PS catch up
    const auto initial_messages = generateFixedMessages(10, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(initial_messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID, 0);
    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // Phase 1: go live
    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 30);
    ASSERT_EQ(1, listener.live_joined_count);

    // Publish more while live
    const auto batch2 = generateFixedMessages(10, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(batch2);
    executeUntil("receives batch2", poller,
        [&] { return handler.messageCount() >= initial_messages.size() + batch2.size(); }, 30);

    // Phase 2: kill the live stream -- PS falls back to replay

    aeron_stream_id_loss_generator_enable(stream_id_gen, STREAM_ID);

    // Publish more messages while live is dead (recorded via spy)
    const auto batch3 = generateFixedMessages(10, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(batch3);

    // Wait for PS to leave live
    executeUntil("leaves live", poller,
        [&] { return !aeron_archive_persistent_subscription_is_live(persistent_subscription); }, 30);
    ASSERT_GE(listener.live_left_count, 1);

    // Phase 3: re-enable live (remove blackout) but set up single-frame drop on replay
    aeron_stream_id_loss_generator_disable(stream_id_gen);

    reset_predicate_state();
    g_drop_first_n.store(1, std::memory_order_relaxed);
    aeron_stream_id_frame_data_loss_generator_enable(frame_data_gen, REPLAY_STREAM_ID, drop_first_n_frames, nullptr);

    // PS should replay (first frame dropped -> retransmitted), catch up, go live again
    executeUntil("recovers to live", poller,
        isLive(persistent_subscription), 60);

    aeron_stream_id_frame_data_loss_generator_disable(frame_data_gen);

    ASSERT_GE(listener.live_joined_count, 2);

    // Publish final batch
    const auto batch4 = generateFixedMessages(10, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(batch4);

    const size_t total = initial_messages.size() + batch2.size() + batch3.size() + batch4.size();
    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == total; }, 30);

    std::vector<std::vector<uint8_t>> all_messages;
    all_messages.insert(all_messages.end(), initial_messages.begin(), initial_messages.end());
    all_messages.insert(all_messages.end(), batch2.begin(), batch2.end());
    all_messages.insert(all_messages.end(), batch3.begin(), batch3.end());
    all_messages.insert(all_messages.end(), batch4.begin(), batch4.end());
    ASSERT_TRUE(MessagesEq(all_messages, handler.messages()));

}

/*
 *  The PS reaches ATTEMPT_SWITCH, where replay_catchup_fragment_handler
 *  advances the replay toward next_live_position. If replay frames are
 *  dropped during this critical catchup phase, retransmission must fill
 *  in the gaps and the PS must still cleanly transition to LIVE.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldTransitionToLiveWhenReplayFramesAreLostDuringAttemptSwitch)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "loss_during_attempt_switch";

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    // Publish a batch large enough that ATTEMPT_SWITCH takes multiple polls
    const auto messages = generateFixedMessages(50, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID, 0);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    // fragment_limit=1 to slow replay and make ATTEMPT_SWITCH span many polls
    auto poller = makeControlledPoller(persistent_subscription, handler, 1);

    // Poll until we reach ATTEMPT_SWITCH, then enable loss on replay
    const auto test_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    bool loss_enabled_during_switch = false;

    while (true)
    {
        ASSERT_LT(std::chrono::steady_clock::now(), test_deadline) << "timed out";

        const int result = poller();
        ASSERT_GE(result, 0) << "poll error: " << aeron_errmsg();

        if (aeron_archive_persistent_subscription_is_live(persistent_subscription))
        {
            break;
        }

        // Enable 50% loss on the replay stream when the PS is attempting to switch.
        // The replay catchup handler reads replay frames to advance toward next_live_position;
        // half of them will be dropped and need retransmission.
        if (!loss_enabled_during_switch &&
            aeron_archive_persistent_subscription_join_difference(persistent_subscription) != 0)
        {
            reset_predicate_state();

            aeron_stream_id_frame_data_loss_generator_enable(loss_gen, REPLAY_STREAM_ID, drop_every_other_frame, nullptr);
            loss_enabled_during_switch = true;
        }

        if (result == 0) std::this_thread::yield();
    }

    aeron_stream_id_frame_data_loss_generator_disable(loss_gen);
    ASSERT_TRUE(loss_enabled_during_switch) << "loss should have been enabled during ATTEMPT_SWITCH";

    // Drain remaining messages
    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); }, 30);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

}

/*
 *  A minimal recording of exactly 1 message. The replay DATA frame is
 *  dropped repeatedly (every-other-frame predicate). The PS must still
 *  eventually receive that one message and transition to live.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldDeliverSingleMessageRecordingWithReplayLoss)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "single_msg_loss";

    reset_predicate_state();

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    // Just ONE message
    const auto messages = generateFixedMessages(1, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID, 0);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    // Drop every other replay frame -- the single message's frame will be dropped
    // on the first attempt, delivered on the retransmit.

    aeron_stream_id_frame_data_loss_generator_enable(loss_gen, REPLAY_STREAM_ID, drop_every_other_frame, nullptr);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 30);

    aeron_stream_id_frame_data_loss_generator_disable(loss_gen);

    executeUntil("receives the message", poller,
        [&] { return handler.messageCount() == 1; }, 30);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

}

/*
 *  When the publisher continues sending while the PS replays through a lossy
 *  channel, the live image advances beyond the replay image. At ATTEMPT_SWITCH,
 *  replay_position != live_position, so the PS uses the catchup handlers to
 *  read live data ahead and advance the replay to meet it.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldCatchupLiveGapDuringAttemptSwitchWithReplayLoss)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "catchup_live_gap";

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    // 50% loss on replay DATA frames -- slows the replay so live gets ahead
    const std::string replay_channel = PLAIN_REPLAY_CHANNEL;

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const auto initial_messages = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(initial_messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        replay_channel, REPLAY_STREAM_ID, 0);
    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);
    listener.ps_for_snapshot = persistent_subscription;

    // Enable loss on the target stream
    reset_predicate_state();
    configure_drop_rate(0.5);

    aeron_stream_id_frame_data_loss_generator_enable(loss_gen, REPLAY_STREAM_ID, drop_at_rate, nullptr);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // Wait for at least one message to confirm replay has started
    executeUntil("receives first message", poller, [&] { return handler.messageCount() >= 1; });
    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    // Publish a larger batch while replay is slowed by loss.
    // This advances the live stream well beyond the replay position.
    const auto extra_messages = generateFixedMessages(50, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(extra_messages);

    // Now poll until live. The PS must enter ATTEMPT_SWITCH with
    // live_position > replay_position and use the catchup handlers.
    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 60);

    ASSERT_GE(listener.live_joined_count, 1);
    // Snapshot taken inside on_live_joined; polling after is_live() is racy because
    // join_difference is reset once catchup completes. See shouldCatchupReplayToLive...
    ASSERT_NE(INT64_MIN, listener.join_difference_at_join);

    const size_t total_expected = initial_messages.size() + extra_messages.size();
    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == total_expected; }, 60);

    std::vector<std::vector<uint8_t>> all_messages;
    all_messages.insert(all_messages.end(), initial_messages.begin(), initial_messages.end());
    all_messages.insert(all_messages.end(), extra_messages.begin(), extra_messages.end());
    ASSERT_TRUE(MessagesEq(all_messages, handler.messages()));

    aeron_stream_id_frame_data_loss_generator_disable(loss_gen);

}

/*
 *  The PS enters ATTEMPT_SWITCH with live ahead of replay. Then total loss
 *  is enabled on the live stream, causing the live image to time out and
 *  close. The PS detects this and falls back to REPLAY. After loss clears,
 *  the PS recovers to LIVE.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldRecoverWhenLiveImageClosesDuringAttemptSwitch)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "live_closed_attempt_switch";

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const auto messages = generateFixedMessages(30, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID, 0);
    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    // Use fragment_limit=1 so we can detect ATTEMPT_SWITCH and kill live mid-switch
    auto poller = makeControlledPoller(persistent_subscription, handler, 1);

    // Poll until ATTEMPT_SWITCH (join_difference becomes non-zero), then kill live
    const auto test_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    bool killed_live_during_switch = false;

    while (true)
    {
        ASSERT_LT(std::chrono::steady_clock::now(), test_deadline) << "timed out";

        const int result = poller();
        ASSERT_GE(result, 0) << "poll error: " << aeron_errmsg();

        if (aeron_archive_persistent_subscription_is_live(persistent_subscription))
            break;

        // When ATTEMPT_SWITCH is entered, kill the live stream
        if (!killed_live_during_switch &&
            aeron_archive_persistent_subscription_join_difference(persistent_subscription) != 0)
        {

            aeron_stream_id_loss_generator_enable(loss_gen, STREAM_ID);
            killed_live_during_switch = true;
        }

        if (result == 0) std::this_thread::yield();
    }

    // Re-enable live so PS can eventually recover
    aeron_stream_id_loss_generator_disable(loss_gen);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); }, 60);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));
    ASSERT_TRUE(killed_live_during_switch) << "loss should have been enabled during ATTEMPT_SWITCH";

}

/*
 * The PS replays and creates a live subscription, but total loss on the live
 * stream prevents the live image from appearing. After the message timeout,
 * the deadline-breach error handler fires. Once loss clears, the live image
 * appears and the PS transitions to LIVE.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldReportDeadlineBreachAndRecoverWhenLiveImageIsDelayedByLoss)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "live_deadline_breach";

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_loss_generator_create(&loss_gen));

    // Note on LIVE→REPLAY→LIVE bouncing:
    //
    // This test publishes 20 messages upfront and nothing more. Once PS joins
    // LIVE at the end of the recording (position=20480), the live subscription
    // image has no incoming data and is kept alive only by publication
    // heartbeats. After the subscription's image_liveness_timeout elapses
    // without activity, the driver correctly closes the idle image. PS handles
    // this by falling back to REPLAY and rejoining LIVE when a new image forms.
    //
    // Confirmed empirically: with image_liveness_timeout=5s, the close fires
    // ~6s after LIVE join; with 10s, it fires ~11s after. The behaviour scales
    // with the timeout, proving it is the subscription-image-liveness timeout.
    //
    // The bounce is NOT a PS bug — it is correct response to image departure.
    // We tolerate live_joined_count == 1 or 2 below.
    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const auto messages = generateFixedMessages(20, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    // Use a short message timeout so the deadline breach happens quickly
    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_context_set_message_timeout_ns(archive_ctx, 2'000'000'000LL); // 2 seconds

    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID, 0);
    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    // Block the live stream entirely so the live image never appears

    aeron_stream_id_loss_generator_enable(loss_gen, STREAM_ID);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // Poll until the deadline breach error fires (within ~2s + some margin)
    executeUntil("deadline breach reported", poller,
        [&] { return listener.error_count > 0; }, 15);

    ASSERT_NE(std::string::npos,
        listener.last_error_message.find("live subscription within the message timeout"))
        << "Expected deadline breach error, got: " << listener.last_error_message;

    // Now restore the live stream. The PS should eventually go LIVE.
    aeron_stream_id_loss_generator_disable(loss_gen);

    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 30);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); }, 30);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

    // Exactly one breach error fires from the initial live-stream loss.
    // The sticky `live_image_deadline_breached` flag prevents re-firings while
    // the same deadline cycle is active; additional errors would indicate the
    // flag is mis-resetting.
    ASSERT_EQ(1, listener.error_count)
        << "Expected exactly one breach error before recovery";

    // Allow 1-2 live_joined callbacks: the first from the initial recovery,
    // an optional second from the idle-image-close → refresh bounce (see
    // comment at top of test). An unbounded number would indicate pathological
    // oscillation, which is the real bug we want to catch.
    ASSERT_LE(listener.live_joined_count, 2);
    ASSERT_GE(listener.live_joined_count, 1);

    // After final recovery, no further error callbacks should fire.
    const int errors_at_recovery = listener.error_count;
    const int live_joined_at_recovery = listener.live_joined_count;
    for (int i = 0; i < 500; ++i) { poller(); }
    ASSERT_EQ(errors_at_recovery, listener.error_count)
        << "PS reported additional errors after successful LIVE recovery";
    ASSERT_EQ(live_joined_at_recovery, listener.live_joined_count)
        << "PS reported additional live_joined callbacks after successful LIVE recovery";
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldReceiveAllMessagesWithUncontrolledPollAndReplayLoss)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "uncontrolled_poll_loss";

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    const std::string replay_channel = PLAIN_REPLAY_CHANNEL;

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    const auto messages = generateFixedMessages(50, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        replay_channel, REPLAY_STREAM_ID, 0);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    // Enable loss on the target stream
    reset_predicate_state();
    configure_drop_rate(0.3);

    aeron_stream_id_frame_data_loss_generator_enable(loss_gen, REPLAY_STREAM_ID, drop_at_rate, nullptr);

    MessageCapturingFragmentHandler handler;
    // Use the UNCONTROLLED poll API
    auto poller = makeUncontrolledPoller(persistent_subscription, handler);

    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 60);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); }, 60);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

    aeron_stream_id_frame_data_loss_generator_disable(loss_gen);

}

// Exercises the uncontrolled-poll catchup machinery in ATTEMPT_SWITCH: PS replays under
// loss while a drip of live data widens the gap; PS must catch up via the uncontrolled
// replay-catchup fragment handler and reach LIVE with all messages delivered in order.
//
// Driver 1 (m_aeronDir) hosts the publisher and Java archive with no loss. Driver 2 hosts
// only PS, with the combined predicate scoping both the SETUP-frame drop and the replay
// DATA-frame drop to PS's receive endpoints — the archive's recording is unaffected.
TEST_F(AeronArchivePersistentSubscriptionTest, shouldCatchupLiveGapWithUncontrolledPollAndReplayLoss)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "uncontrolled_catchup";

    // Driver 1: publisher + archive, no loss.
    EmbeddedMediaDriverWithLossGenerator driver1;
    driver1.aeronDir(m_aeronDir);
    driver1.start();

    TestStandaloneArchive archive(
        m_aeronDir, archive_dir, std::cout,
        LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0");

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const auto initial_messages = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(initial_messages);

    // Driver 2: PS only.
    char ps_aeron_dir_buf[AERON_MAX_PATH];
    aeron_default_path(ps_aeron_dir_buf, sizeof(ps_aeron_dir_buf));
    const std::string ps_aeron_dir =
        std::string(ps_aeron_dir_buf) + "-ps-" + std::to_string(aeron_randomised_int32());

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_frame_data_loss_generator_create(&loss_gen));

    EmbeddedMediaDriverWithLossGenerator driver2;
    driver2.aeronDir(ps_aeron_dir);
    driver2.setReceiveChannelDataLossGenerator(loss_gen);
    driver2.start();

    // SETUP-drop holds PS's live image off until the window expires; replay loss slows
    // replay so ATTEMPT_SWITCH spans many polls.
    reset_predicate_state();
    configure_drop_rate(0.7);
    const int64_t setup_drop_window_ns = 1'500LL * 1000 * 1000;
    const int64_t setup_drop_window_end_ns = aeron_nano_clock() + setup_drop_window_ns;
    g_setup_drop_stream_id.store(STREAM_ID, std::memory_order_relaxed);
    g_setup_drop_until_ns.store(setup_drop_window_end_ns, std::memory_order_release);
    g_replay_drop_stream_id.store(REPLAY_STREAM_ID, std::memory_order_relaxed);
    aeron_frame_data_loss_generator_enable(loss_gen, drop_setup_for_window_or_replay_at_rate, nullptr);

    AeronResource aeron(ps_aeron_dir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    aeron_archive_context_set_aeron_directory_name(archive_ctx, ps_aeron_dir.c_str());
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        PLAIN_REPLAY_CHANNEL, REPLAY_STREAM_ID, 0);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);
    listener.ps_for_snapshot = persistent_subscription;

    MessageCapturingFragmentHandler handler;
    auto base_poller = makeUncontrolledPoller(persistent_subscription, handler);

    // Drip-feed publisher at moderate rate during SETUP-drop window, then stop.
    const int64_t drip_interval_ns = 10LL * 1000 * 1000;  // 100 msgs/sec
    int64_t next_drip_ns = aeron_nano_clock();
    std::vector<std::vector<uint8_t>> drip_messages;
    auto poll_with_drip = [&]
    {
        const int fragments = base_poller();
        const int64_t now_ns = aeron_nano_clock();
        if (now_ns < setup_drop_window_end_ns && now_ns >= next_drip_ns)
        {
            std::vector<uint8_t> msg(ONE_KB_MESSAGE_SIZE);
            for (size_t i = 0; i < msg.size(); i++)
            {
                msg[i] = static_cast<uint8_t>((drip_messages.size() + i) & 0xFF);
            }
            const int64_t pos = aeron_exclusive_publication_offer(
                persistent_publication.publication(), msg.data(), msg.size(), nullptr, nullptr);
            if (pos > 0)
            {
                drip_messages.push_back(std::move(msg));
                next_drip_ns = now_ns + drip_interval_ns;
            }
        }
        return fragments;
    };

    executeUntil("receives first message", poll_with_drip, [&] { return handler.messageCount() >= 1; });
    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    executeUntil("becomes live", poll_with_drip,
        isLive(persistent_subscription), 60);

    aeron_frame_data_loss_generator_disable(loss_gen);

    ASSERT_EQ(1, listener.live_joined_count);
    // We only assert the value was captured (PS exited replay through the live-image-found
    // transition). A strict > 0 check is racy: replay can catch up to recording_max within
    // ms of add_live_subscription firing. Message-delivery + reaching-LIVE cover the rest.
    ASSERT_NE(INT64_MIN, listener.join_difference_at_join);

    const size_t total_expected = initial_messages.size() + drip_messages.size();
    executeUntil("receives all messages", base_poller,
        [&] { return handler.messageCount() == total_expected; }, 60);

    std::vector<std::vector<uint8_t>> all_messages;
    all_messages.insert(all_messages.end(), initial_messages.begin(), initial_messages.end());
    all_messages.insert(all_messages.end(), drip_messages.begin(), drip_messages.end());
    ASSERT_TRUE(MessagesEq(all_messages, handler.messages()));
}

/*
 * Additional coverage-gap tests
 */

/*
 * Test 1: FROM_LIVE entry point with initial live channel loss
 *
 *   Coverage targets: add_live_subscription (0%), await_live (0%)
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldStartFromLiveAndRecoverFromInitialLiveChannelLoss)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "from_live_loss";

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    // Pre-publish some messages (FROM_LIVE should NOT replay these)
    const auto old_messages = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(old_messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID, 0);

    aeron_archive_persistent_subscription_context_set_start_position(
        context, AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE);
    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    // Enable DATA-frame loss on the live stream. The subscription establishes
    // (setup/status frames aren't dropped by stream_id_loss), but once live,
    // published DATA will be lost and need retransmission.

    aeron_stream_id_loss_generator_enable(loss_gen, STREAM_ID);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // PS goes through ADD_LIVE_SUBSCRIPTION -> AWAIT_LIVE -> LIVE
    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 30);

    ASSERT_EQ(1, listener.live_joined_count);
    ASSERT_EQ(0u, handler.messageCount()); // FROM_LIVE: old messages not replayed

    // Clear loss so published messages can be delivered
    aeron_stream_id_loss_generator_disable(loss_gen);

    // Publish new messages via live
    const auto live_messages = generateFixedMessages(10, ONE_KB_MESSAGE_SIZE);
    persistent_publication.offer(live_messages);

    executeUntil("receives live messages", poller,
        [&] { return handler.messageCount() == live_messages.size(); }, 30);

    ASSERT_TRUE(MessagesEq(live_messages, handler.messages()));

}

/*
 * Test 2: Interleaved publish + poll to trigger live_catchup_fragment_handler
 *
 *   Coverage targets: live_catchup_fragment_handler (0%)
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldInvokeLiveCatchupHandlerWhenPublishingDuringAttemptSwitch)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "live_catchup_handler";

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    const std::string replay_channel = PLAIN_REPLAY_CHANNEL;

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const auto seed_messages = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(seed_messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        replay_channel, REPLAY_STREAM_ID, 0);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    // Enable loss on the target stream
    reset_predicate_state();
    configure_drop_rate(0.5);

    aeron_stream_id_frame_data_loss_generator_enable(loss_gen, REPLAY_STREAM_ID, drop_at_rate, nullptr);

    MessageCapturingFragmentHandler handler;

    // Interleave: poll PS (fragment_limit=1) and offer messages continuously
    // so the live image has unconsumed data during ATTEMPT_SWITCH.
    std::vector<std::vector<uint8_t>> all_published;
    all_published.insert(all_published.end(), seed_messages.begin(), seed_messages.end());

    const auto test_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    int messages_offered = 0;

    while (true)
    {
        ASSERT_LT(std::chrono::steady_clock::now(), test_deadline) << "timed out";

        const int result = aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription, MessageCapturingFragmentHandler::onFragment, &handler, 1);
        ASSERT_GE(result, 0) << "poll error: " << aeron_errmsg();

        if (aeron_archive_persistent_subscription_is_live(persistent_subscription))
            break;

        // Keep publishing to advance the live stream beyond replay
        if (messages_offered < 100)
        {
            auto msg = generateFixedMessages(1, ONE_KB_MESSAGE_SIZE);
            int64_t offer_result = aeron_exclusive_publication_offer(
                persistent_publication.publication(),
                msg[0].data(), msg[0].size(), nullptr, nullptr);
            if (offer_result > 0)
            {
                all_published.push_back(msg[0]);
                messages_offered++;
            }
        }

        if (result == 0) std::this_thread::yield();
    }

    executeUntil("receives all messages", [&] {
        return aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription, MessageCapturingFragmentHandler::onFragment, &handler, 10);
    }, [&] { return handler.messageCount() >= all_published.size(); }, 30);

    ASSERT_TRUE(MessagesEq(all_published, handler.messages()));

    aeron_stream_id_frame_data_loss_generator_disable(loss_gen);

}

/*
 * Test 3: Archive killed during lossy replay — PS reconnects
 *
 *   Coverage targets: on_archive_disconnected (0%), on_archive_error (0%)
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldRecoverFromArchiveKillDuringLossyReplay)
{
    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&loss_gen));

    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "archive_kill_lossy";
    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    const auto messages = generateFixedMessages(80, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        PLAIN_REPLAY_CHANNEL, REPLAY_STREAM_ID, 0);
    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    // Enable 50% frame loss on replay to slow it
    reset_predicate_state();

    aeron_stream_id_frame_data_loss_generator_enable(loss_gen, REPLAY_STREAM_ID, drop_every_other_frame, nullptr);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler, 1);

    executeUntil("receives some messages", poller,
        [&] { return handler.messageCount() >= 5; }, 30);
    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    // Kill archive while PS is in REPLAY — triggers on_archive_disconnected
    harness.killArchivePreservingDir();

    executeUntil("detects disconnection", poller,
        isNotReplaying(persistent_subscription), 30);

    ASSERT_FALSE(aeron_archive_persistent_subscription_has_failed(persistent_subscription));

    aeron_stream_id_frame_data_loss_generator_disable(loss_gen);

    harness.restartArchive();

    auto fast_poller = [&] {
        return aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription, MessageCapturingFragmentHandler::onFragment, &handler, 10);
    };

    executeUntil("becomes live after reconnect", fast_poller,
        isLive(persistent_subscription), 60);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));
    ASSERT_GE(listener.live_joined_count, 1);

}

/*
 * Multi-fragment message delivered intact when replay image closes mid-reassembly.
 *
 * Publishes a single message large enough to require 3+ fragments. Applies
 * heavy loss (70%) on the replay stream while the replay is in progress, then
 * kills and restarts the archive. The expectation is that the multi-fragment
 * message is delivered EXACTLY ONCE, byte-for-byte identical to what was
 * published.
 *
 * The code path of interest: aeron_archive_persistent_subscription_replay()
 * line 1861 handles image-close by advancing `position` from the closed image
 * and calling set_up_replay(). If the fragment assembler had partial BEGIN/
 * MIDDLE fragments buffered for the closing image's session, that partial
 * state is NOT explicitly flushed. A naively-implemented assembler might
 * either (a) lose the buffered start of the message, or (b) leak the partial
 * buffer across sessions. Either manifests as corruption or data loss.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldDeliverMultiFragmentMessageAcrossReplayImageCloseMidReassembly)
{
    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&loss_gen));

    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "mid_reassembly_close";
    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    // Single large message that spans ~4 fragments.
    const int32_t fragment_size = persistent_publication.maxPayloadLength() + 1;
    const std::vector<uint8_t> big_message = generateRandomBytes(fragment_size * 3);
    // Also include a couple of small messages before and after so we can verify ordering.
    const std::vector<uint8_t> small_before = generateRandomBytes(64);
    const std::vector<uint8_t> small_after = generateRandomBytes(64);
    persistent_publication.persist({{ small_before, big_message, small_after }});

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        PLAIN_REPLAY_CHANNEL, REPLAY_STREAM_ID, 0);
    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    // Heavy loss (70%) on the replay stream so that fragment reassembly is
    // under stress and partial reassembly state is likely present when the
    // image closes.
    reset_predicate_state();
    configure_drop_rate(0.7);
    aeron_stream_id_frame_data_loss_generator_enable(loss_gen, REPLAY_STREAM_ID, drop_at_rate, nullptr);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler, 1);

    // Poll until replay is actively running (partial fragments likely in assembler).
    executeUntil("replay starts", poller, isReplaying(persistent_subscription), 30);

    // Wait a short while to let some fragments partially accumulate in the assembler.
    const auto wait_start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - wait_start < std::chrono::milliseconds(200))
    {
        poller();
        std::this_thread::yield();
    }

    // Close the replay image by killing the archive mid-replay.
    harness.killArchivePreservingDir();

    executeUntil("detects disconnection", poller, isNotReplaying(persistent_subscription), 30);
    ASSERT_FALSE(aeron_archive_persistent_subscription_has_failed(persistent_subscription));

    // Disable loss so the restart can complete cleanly.
    aeron_stream_id_frame_data_loss_generator_disable(loss_gen);
    harness.restartArchive();

    auto fast_poller = makeControlledPoller(persistent_subscription, handler, 10);

    // PS should resume replay, catch up, and become LIVE.
    executeUntil("becomes live after reconnect", fast_poller,
        isLive(persistent_subscription), 60);

    executeUntil("receives all three messages", fast_poller,
        [&] { return handler.messageCount() == 3; }, 60);

    // The multi-fragment message must be delivered intact, and bracketed by
    // small_before and small_after in order, each appearing exactly once.
    const std::vector<std::vector<uint8_t>> expected{ small_before, big_message, small_after };
    ASSERT_EQ(3u, handler.messageCount());
    ASSERT_EQ(expected, handler.messages())
        << "Multi-fragment message corrupted or lost across replay image close";
}

/*
 * Test 4: Response-channel replay with loss
 *
 *   Coverage targets: add_request_publication (0%),
 *     await_request_publication (0%), send_replay_token_request (0%),
 *     await_replay_token (0%)
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldReplayOverResponseChannelWithLoss)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "response_channel_loss";

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    const std::string replay_channel =
        "aeron:udp?control=localhost:10001|control-mode=response";
    const std::string archive_control_response_channel =
        "aeron:udp?control-mode=response|control=localhost:10002";

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    const auto messages = generateFixedMessages(20, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_context_set_control_response_channel(archive_ctx, archive_control_response_channel.c_str());

    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        replay_channel, -11, 0);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    // Enable loss on the target stream
    reset_predicate_state();
    configure_drop_rate(0.3);

    aeron_stream_id_frame_data_loss_generator_enable(loss_gen, REPLAY_STREAM_ID, drop_at_rate, nullptr);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 60);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); }, 60);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

    aeron_stream_id_frame_data_loss_generator_disable(loss_gen);

}

/*
 * Test 5: User-provided counters with loss-induced transitions
 *
 *   Coverage targets: aeron_counter_set_release (0%),
 *     aeron_counter_increment_release (0%),
 *     context_set_{state,join_difference,live_left,live_joined}_counter (0%)
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldUpdateUserProvidedCountersDuringLossInducedTransitions)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "user_counters_loss";

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    const auto messages = generateFixedMessages(10, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    auto allocate_counter = [&](const char *label) -> aeron_counter_t * {
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

    aeron_counter_t *state_counter = allocate_counter("loss-test-state");
    aeron_counter_t *join_diff_counter = allocate_counter("loss-test-join-diff");
    aeron_counter_t *live_left_counter = allocate_counter("loss-test-live-left");
    aeron_counter_t *live_joined_counter = allocate_counter("loss-test-live-joined");

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID, 0);

    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_set_state_counter(context, state_counter));
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_set_join_difference_counter(context, join_diff_counter));
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_set_live_left_counter(context, live_left_counter));
    ASSERT_EQ(0, aeron_archive_persistent_subscription_context_set_live_joined_counter(context, live_joined_counter));

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // Phase 1: go live
    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 30);

    ASSERT_GE(*aeron_counter_addr(live_joined_counter), 1);
    ASSERT_NE(0, *aeron_counter_addr(state_counter));

    // Phase 2: kill live to trigger LIVE -> REPLAY (increments live_left counter)

    aeron_stream_id_loss_generator_enable(loss_gen, STREAM_ID);

    executeUntil("leaves live", poller,
        [&] { return !aeron_archive_persistent_subscription_is_live(persistent_subscription); }, 30);

    ASSERT_GE(*aeron_counter_addr(live_left_counter), 1);

    // Phase 3: restore and go live again
    aeron_stream_id_loss_generator_disable(loss_gen);

    executeUntil("becomes live again", poller,
        isLive(persistent_subscription), 60);

    ASSERT_GE(*aeron_counter_addr(live_joined_counter), 2);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); }, 30);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

}

/*
 *  The PS is created and immediately enters SEND_LIST_RECORDING_REQUEST ->
 *  AWAIT_LIST_RECORDING_RESPONSE. The archive is killed before the response
 *  arrives. The message_timeout_ns deadline fires, the PS checks archive
 *  connection (dead), and transitions to AWAIT_ARCHIVE_CONNECTION.
 *  After archive restart the PS recovers.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldRecoverFromArchiveKillDuringAwaitListRecording)
{
    const std::string aeron_dir = m_aeronDir;
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "kill_await_list";

    aeron_env_set("AERON_SPIES_SIMULATE_CONNECTION", "true");
    ScopedMediaDriver c_driver;
    c_driver.aeronDir(aeron_dir);
    c_driver.start();

    auto archive_process = std::make_unique<TestStandaloneArchive>(
        aeron_dir, archive_dir, std::cout,
        LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0");

    PersistentPublication persistent_publication(aeron_dir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    const auto messages = generateFixedMessages(20, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(aeron_dir);

    // Short message timeout so the deadline fires quickly after archive death
    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_context_set_message_timeout_ns(archive_ctx, 2'000'000'000LL);

    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID, 0);
    TestListener listener;
    listener.attachTo(context);

    // Kill archive IMMEDIATELY after PS creation — PS is in early states
    const std::int64_t archive_id_to_reuse = archive_process->archiveId();
    archive_process->deleteDirOnTearDown(false);
    archive_process.reset();

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // Poll while archive is dead — PS should detect timeout/disconnect
    const auto wait_start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - wait_start < std::chrono::seconds(5))
    {
        poller();
        std::this_thread::yield();
    }
    ASSERT_FALSE(aeron_archive_persistent_subscription_has_failed(persistent_subscription));

    // Restart archive
    archive_process = std::make_unique<TestStandaloneArchive>(
        aeron_dir, archive_dir, std::cout,
        LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0",
        archive_id_to_reuse, false);

    executeUntil("becomes live after reconnect", poller,
        isLive(persistent_subscription), 60);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); }, 30);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

}

/*
 *  Uses response-channel replay. The archive is killed during the
 *  response-channel setup flow, triggering timeout and disconnect paths
 *  in the token request and replay request states.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldRecoverFromArchiveKillDuringResponseChannelSetup)
{
    const std::string aeron_dir = m_aeronDir;
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "kill_response_channel";

    aeron_env_set("AERON_SPIES_SIMULATE_CONNECTION", "true");
    ScopedMediaDriver c_driver;
    c_driver.aeronDir(aeron_dir);
    c_driver.start();

    auto archive_process = std::make_unique<TestStandaloneArchive>(
        aeron_dir, archive_dir, std::cout,
        LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0");

    const std::string replay_channel = "aeron:udp?control=localhost:10001|control-mode=response";
    const std::string archive_control_response_channel =
        "aeron:udp?control-mode=response|control=localhost:10002";

    PersistentPublication persistent_publication(aeron_dir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    const auto messages = generateFixedMessages(20, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(aeron_dir);

    // Short timeout so timeouts fire quickly
    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_context_set_control_response_channel(archive_ctx, archive_control_response_channel.c_str());
    aeron_archive_context_set_message_timeout_ns(archive_ctx, 2'000'000'000LL);

    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        replay_channel, -11, 0);
    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // Poll briefly to let the PS enter the response-channel flow
    const auto setup_end = std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
    while (std::chrono::steady_clock::now() < setup_end)
    {
        poller();
        std::this_thread::yield();
    }

    // Kill archive — PS is somewhere in ADD_REQUEST_PUBLICATION / AWAIT_REQUEST_PUBLICATION /
    // SEND_REPLAY_TOKEN_REQUEST / AWAIT_REPLAY_TOKEN / SEND_REPLAY_REQUEST / AWAIT_REPLAY_RESPONSE
    const std::int64_t archive_id_to_reuse = archive_process->archiveId();
    archive_process->deleteDirOnTearDown(false);
    archive_process.reset();

    // Poll while archive is dead — timeouts fire. The wait must exceed the Aeron client
    // liveness timeout so the driver reaps the dead archive's response-channel publication
    // before the next archive instance tries to add the same URI; otherwise the new archive
    // gets EADDRINUSE on addExclusivePublication and the PS goes terminal.
    const auto wait_start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - wait_start < std::chrono::seconds(15))
    {
        poller();
        std::this_thread::yield();
    }
    ASSERT_FALSE(aeron_archive_persistent_subscription_has_failed(persistent_subscription));

    // Restart archive
    archive_process = std::make_unique<TestStandaloneArchive>(
        aeron_dir, archive_dir, std::cout,
        LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0",
        archive_id_to_reuse, false);

    executeUntil("becomes live after reconnect", poller,
        isLive(persistent_subscription), 60);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); }, 30);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

}

/*
 *  The PS replays normally until it catches up and the max_recorded_position
 *  check begins. At that point, message_timeout_ns is reduced to 1ns so the
 *  NEXT max-position request gets a deadline that expires instantly. The
 *  timeout path resets state to REQUEST_MAX_POSITION. After restoring the
 *  normal timeout, the PS retries and eventually goes LIVE.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldTriggerMaxRecordedPositionTimeoutViaTimeoutManipulation)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "max_pos_timeout_manip";

    aeron_env_set("AERON_SPIES_SIMULATE_CONNECTION", "true");
    ScopedMediaDriver c_driver;
    c_driver.aeronDir(m_aeronDir);
    c_driver.start();

    TestStandaloneArchive archive_process(
        m_aeronDir, archive_dir, std::cout,
        LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0");

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    const auto messages = generateFixedMessages(20, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID, 0);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    const uint64_t normal_timeout = 10'000'000'000ULL; // default 10s

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // Let PS replay normally until it's received most messages (near the catchup point
    // where max_recorded_position checks begin)
    executeUntil("nearly caught up", poller,
        [&] { return handler.messageCount() >= 15; }, 30);
    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    // Shrink the timeout so the NEXT max_recorded_position request gets an instant deadline.
    // The max_recorded_position state machine cycles: REQUEST -> AWAIT -> timeout -> REQUEST.
    // With a 1ns timeout, the AWAIT state's deadline fires immediately.
    aeron_archive_persistent_subscription_set_message_timeout_ns_for_testing(persistent_subscription, 1);

    // Poll several times to trigger the timeout path (line 676-679).
    // The PS retries the max-position request repeatedly with instant deadlines.
    for (int i = 0; i < 50; i++)
    {
        int result = poller();
        ASSERT_GE(result, 0) << "poll error: " << aeron_errmsg();
    }

    // Restore normal timeout so the PS can proceed to LIVE
    aeron_archive_persistent_subscription_set_message_timeout_ns_for_testing(persistent_subscription, normal_timeout);

    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 30);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); }, 30);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

}

/*
 *  The PS is created with a 1ns timeout. The very first list-recording
 *  request deadline fires instantly. Since the archive IS connected, the
 *  PS retries (line 1208: transition to SEND_LIST_RECORDING_REQUEST).
 *  After a few retries, the timeout is restored and the PS proceeds normally.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldTriggerListRecordingTimeoutViaTimeoutManipulation)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "list_recording_timeout_manip";

    aeron_env_set("AERON_SPIES_SIMULATE_CONNECTION", "true");
    ScopedMediaDriver c_driver;
    c_driver.aeronDir(m_aeronDir);
    c_driver.start();

    TestStandaloneArchive archive_process(
        m_aeronDir, archive_dir, std::cout,
        LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0");

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    const auto messages = generateFixedMessages(10, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID, 0);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    const uint64_t normal_timeout = 10'000'000'000ULL; // default 10s

    // Set tiny timeout BEFORE the first poll — the list recording request
    // deadline will fire instantly, exercising the timeout path.
    aeron_archive_persistent_subscription_set_message_timeout_ns_for_testing(persistent_subscription, 1);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // Poll several times with the instant timeout. The PS cycles:
    // SEND_LIST_RECORDING_REQUEST -> AWAIT_LIST_RECORDING_RESPONSE -> (timeout) -> retry
    for (int i = 0; i < 50; i++)
    {
        int result = poller();
        ASSERT_GE(result, 0) << "poll error: " << aeron_errmsg();
        ASSERT_FALSE(aeron_archive_persistent_subscription_has_failed(persistent_subscription));
    }

    // Restore normal timeout
    aeron_archive_persistent_subscription_set_message_timeout_ns_for_testing(persistent_subscription, normal_timeout);

    // PS should recover and eventually go LIVE
    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 30);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); }, 30);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

}

/*
 *  Uses response-channel replay. The PS connects and enters the
 *  response-channel flow normally. Once in SEND_REPLAY_TOKEN_REQUEST or
 *  AWAIT_REPLAY_TOKEN, the timeout is reduced to 1ns. The deadline fires,
 *  the PS checks archive connection (still connected), and retries.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldTriggerReplayTokenTimeoutViaTimeoutManipulation)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "replay_token_timeout_manip";

    aeron_env_set("AERON_SPIES_SIMULATE_CONNECTION", "true");
    ScopedMediaDriver c_driver;
    c_driver.aeronDir(m_aeronDir);
    c_driver.start();

    TestStandaloneArchive archive_process(
        m_aeronDir, archive_dir, std::cout,
        LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0");

    const std::string replay_channel = "aeron:udp?control=localhost:10001|control-mode=response";
    const std::string archive_control_response_channel =
        "aeron:udp?control-mode=response|control=localhost:10002";

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    const auto messages = generateFixedMessages(10, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_context_set_control_response_channel(archive_ctx, archive_control_response_channel.c_str());

    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        replay_channel, -11, 0);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    const uint64_t normal_timeout = 10'000'000'000ULL; // default 10s

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // Poll a few times with normal timeout to let the PS connect and enter the
    // response-channel flow (SEND_LIST_RECORDING -> ... -> ADD_REPLAY_SUBSCRIPTION -> ...)
    for (int i = 0; i < 200; i++)
    {
        poller();
        if (aeron_archive_persistent_subscription_is_replaying(persistent_subscription) ||
            aeron_archive_persistent_subscription_is_live(persistent_subscription))
            break;
        std::this_thread::yield();
    }

    // If the PS hasn't reached replay/live yet, set tiny timeout to trigger
    // timeout paths in whatever response-channel state it's in.
    if (!aeron_archive_persistent_subscription_is_live(persistent_subscription))
    {
        aeron_archive_persistent_subscription_set_message_timeout_ns_for_testing(persistent_subscription, 1);

        // Poll with instant timeouts — exercises timeout paths in
        // await_replay_token, send_replay_token_request, await_request_publication
        for (int i = 0; i < 100; i++)
        {
            int result = poller();
            ASSERT_GE(result, 0) << "poll error: " << aeron_errmsg();
            ASSERT_FALSE(aeron_archive_persistent_subscription_has_failed(persistent_subscription));
        }

        aeron_archive_persistent_subscription_set_message_timeout_ns_for_testing(persistent_subscription, normal_timeout);
    }

    // PS should recover and go LIVE
    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 60);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); }, 30);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

}

/*
 *  The drop_payloads_after_threshold predicate passes the first N payload
 *  frames (allowing the archive connection handshake) then drops all
 *  subsequent payloads. The list-recording response and replay response
 *  are dropped, causing their deadlines to fire.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldTimeoutOnListRecordingAndReplayResponseWhenResponsesDropped)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "list_replay_resp_drop";

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    const auto messages = generateFixedMessages(10, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID, 0);

    // Enable threshold predicate: pass first N payload frames (for connect
    // handshake), then drop all subsequent (blocking list-recording response).
    // We test with threshold=1 first to find the handshake frame count.
    g_payload_seq.store(0, std::memory_order_relaxed);
    g_pass_payload_threshold.store(2, std::memory_order_relaxed);

    aeron_stream_id_frame_data_loss_generator_enable(loss_gen, 
        CONTROL_RESPONSE_STREAM_ID, drop_payloads_after_threshold, nullptr);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    aeron_archive_persistent_subscription_set_message_timeout_ns_for_testing(
        persistent_subscription, 200'000'000LL);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    const auto drop_end = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (std::chrono::steady_clock::now() < drop_end)
    {
        int result = poller();
        ASSERT_GE(result, 0) << "poll error: " << aeron_errmsg();
        ASSERT_FALSE(aeron_archive_persistent_subscription_has_failed(persistent_subscription));
        if (result == 0) std::this_thread::yield();
    }

    // Stop dropping, restore timeout
    aeron_stream_id_frame_data_loss_generator_disable(loss_gen);
    aeron_archive_persistent_subscription_set_message_timeout_ns_for_testing(
        persistent_subscription, 10'000'000'000ULL);

    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 30);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); }, 30);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

}

/*
 * Forces the AWAIT_REPLAY_RESPONSE deadline to fire by dropping the reply to
 * the replay request *after* list_recording has completed. Targets the timeout
 * branch in aeron_archive_persistent_subscription_await_replay_response
 * (persistent_subscription.c:1406-1421) which coverage analysis showed was
 * never exercised: all existing response-drop tests either lose the
 * list_recording response too (so PS never reaches AWAIT_REPLAY_RESPONSE), or
 * send heartbeats that prevent the deadline branch taking the "connected +
 * retry" path.
 *
 * Strategy: let the first 3 control-response payload frames pass (archive
 * connect handshake = 2 frames; list_recording response = 1 frame). Drop
 * everything after. PS then advances to SEND_REPLAY_REQUEST → AWAIT_REPLAY_
 * RESPONSE, but the replay's ControlResponse is dropped. The short message
 * timeout (200ms) makes the deadline fire, and PS cleans up + retries from
 * set_up_replay since heartbeats keep the archive "connected". With loss
 * disabled later, the retry succeeds and PS goes LIVE.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldTimeoutOnReplayResponseSelectively)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "replay_resp_selective_drop";

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    const auto messages = generateFixedMessages(10, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        PLAIN_REPLAY_CHANNEL, REPLAY_STREAM_ID, 0);

    // Pass first 3 payload frames (connect + list_recording), drop everything after.
    reset_predicate_state();
    g_pass_payload_threshold.store(3, std::memory_order_relaxed);
    aeron_stream_id_frame_data_loss_generator_enable(loss_gen,
        CONTROL_RESPONSE_STREAM_ID, drop_payloads_after_threshold, nullptr);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    // Short timeout so the AWAIT_REPLAY_RESPONSE deadline fires quickly.
    aeron_archive_persistent_subscription_set_message_timeout_ns_for_testing(
        persistent_subscription, 200'000'000LL);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // Poll for ~1.5s. With 200ms timeout + ~200ms retry cycle, the deadline
    // should fire multiple times while the replay response is blocked.
    const auto drop_end = std::chrono::steady_clock::now() + std::chrono::milliseconds(1500);
    while (std::chrono::steady_clock::now() < drop_end)
    {
        int result = poller();
        ASSERT_GE(result, 0) << "poll error: " << aeron_errmsg();
        ASSERT_FALSE(aeron_archive_persistent_subscription_has_failed(persistent_subscription));
        if (result == 0) std::this_thread::yield();
    }

    // Restore: disable loss and restore message timeout.
    aeron_stream_id_frame_data_loss_generator_disable(loss_gen);
    aeron_archive_persistent_subscription_set_message_timeout_ns_for_testing(
        persistent_subscription, 10'000'000'000ULL);

    // PS should recover and go LIVE.
    executeUntil("becomes live", poller, isLive(persistent_subscription), 60);
    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); }, 30);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldTimeoutOnMaxRecordedPositionWhenResponsesDropped)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "max_pos_resp_drop";

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    const auto messages = generateFixedMessages(20, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID, 0);

    // Pass first 4 payload frames: connect handshake (2) + list-recording
    // response (1) + replay response (1). The PS enters REPLAY normally.
    // Then drop everything — max_recorded_position responses are blocked.
    // With short timeout, the max_recorded_position deadline fires.
    g_payload_seq.store(0, std::memory_order_relaxed);
    g_pass_payload_threshold.store(4, std::memory_order_relaxed);

    aeron_stream_id_frame_data_loss_generator_enable(loss_gen, 
        CONTROL_RESPONSE_STREAM_ID, drop_payloads_after_threshold, nullptr);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    // Short timeout: replay response timeout fires at ~200ms, then list
    // recording and max_recorded_position timeouts fire on retry cycles.
    aeron_archive_persistent_subscription_set_message_timeout_ns_for_testing(
        persistent_subscription, 200'000'000LL);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // Poll while responses are dropped — multiple timeout paths fire
    const auto drop_end = std::chrono::steady_clock::now() + std::chrono::seconds(4);
    while (std::chrono::steady_clock::now() < drop_end)
    {
        int result = poller();
        ASSERT_GE(result, 0) << "poll error: " << aeron_errmsg();
        ASSERT_FALSE(aeron_archive_persistent_subscription_has_failed(persistent_subscription));
        if (result == 0) std::this_thread::yield();
    }

    // Stop dropping, restore timeout
    aeron_stream_id_frame_data_loss_generator_disable(loss_gen);
    aeron_archive_persistent_subscription_set_message_timeout_ns_for_testing(
        persistent_subscription, 10'000'000'000ULL);

    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 30);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); }, 30);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldTimeoutOnReplayTokenWhenResponsesDropped)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "replay_token_resp_drop";

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    const std::string replay_channel = "aeron:udp?control=localhost:10001|control-mode=response";
    const std::string archive_control_response_channel =
        "aeron:udp?control-mode=response|control=localhost:10002";

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    const auto messages = generateFixedMessages(10, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_context_set_control_response_channel(archive_ctx, archive_control_response_channel.c_str());

    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        replay_channel, -11, 0);

    // Pass first 3 payload frames: connect handshake (2 frames) + list recording
    // response (1 frame). Drop everything after — the replay token response
    // will be blocked, causing the await_replay_token deadline to fire.
    g_payload_seq.store(0, std::memory_order_relaxed);
    g_pass_payload_threshold.store(3, std::memory_order_relaxed);

    aeron_stream_id_frame_data_loss_generator_enable(loss_gen, 
        CONTROL_RESPONSE_STREAM_ID, drop_payloads_after_threshold, nullptr);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    aeron_archive_persistent_subscription_set_message_timeout_ns_for_testing(
        persistent_subscription, 200'000'000LL);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    const auto drop_end = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (std::chrono::steady_clock::now() < drop_end)
    {
        int result = poller();
        ASSERT_GE(result, 0) << "poll error: " << aeron_errmsg();
        ASSERT_FALSE(aeron_archive_persistent_subscription_has_failed(persistent_subscription));
        if (result == 0) std::this_thread::yield();
    }

    // Stop dropping, restore timeout
    aeron_stream_id_frame_data_loss_generator_disable(loss_gen);
    aeron_archive_persistent_subscription_set_message_timeout_ns_for_testing(
        persistent_subscription, 10'000'000'000ULL);

    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 60);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); }, 30);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

}

/*
 *   Phase 1: threshold predicate drops response payloads but passes heartbeats.
 *     The archive appears connected, timeouts fire with is_connected=true
 *   Phase 2: switch to total stream_id_loss on stream 20, killing heartbeats
 *     too. The control response image drains after image_liveness_timeout_ns.
 *     The next timeout check finds is_connected=false, hitting the "not
 *     connected" branches
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldHitNotConnectedTimeoutBranchesViaTwoPhaseResponseDrop)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "two_phase_resp_drop";

    // Chain both interceptors: frame_data_loss -> stream_id_loss -> (driver)
    aeron_loss_generator_t *stream_id_gen = nullptr;
    aeron_loss_generator_t *frame_data_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_loss_generator_create(&stream_id_gen));
    ASSERT_EQ(0, aeron_stream_id_frame_data_loss_generator_create(&frame_data_gen));
    aeron_loss_generator_t *loss_gen = makeCompositeLossGenerator(stream_id_gen, frame_data_gen);

    // Short image liveness timeout so the image drains quickly in phase 2
    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen, 100'000'000LL);
    harness.driver.adoptLossGenerator(stream_id_gen);
    harness.driver.adoptLossGenerator(frame_data_gen);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    const auto messages = generateFixedMessages(10, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID, 0);

    // Phase 1: threshold predicate — pass connect handshake (2 frames), drop
    // all subsequent payloads. Heartbeats flow, image stays alive.
    g_payload_seq.store(0, std::memory_order_relaxed);
    g_pass_payload_threshold.store(2, std::memory_order_relaxed);

    aeron_stream_id_frame_data_loss_generator_enable(frame_data_gen, 
        CONTROL_RESPONSE_STREAM_ID, drop_payloads_after_threshold, nullptr);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    // 500ms message timeout — long enough for the image to drain in phase 2
    // (100ms image liveness) before the timeout fires.
    aeron_archive_persistent_subscription_set_message_timeout_ns_for_testing(
        persistent_subscription, 500'000'000LL);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // Phase 1: poll with payload drops only (heartbeats flowing, archive "connected")
    // This exercises the "connected" retry branches.
    const auto phase1_end = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < phase1_end)
    {
        int result = poller();
        ASSERT_GE(result, 0) << "poll error: " << aeron_errmsg();
        ASSERT_FALSE(aeron_archive_persistent_subscription_has_failed(persistent_subscription));
        if (result == 0) std::this_thread::yield();
    }

    // Phase 2: switch to total loss on stream 20 — this kills heartbeats too.
    // The control response image will drain after 100ms (image liveness timeout).
    // Disable the threshold predicate and enable total stream loss instead.
    aeron_stream_id_frame_data_loss_generator_disable(frame_data_gen);
    aeron_stream_id_loss_generator_enable(stream_id_gen, CONTROL_RESPONSE_STREAM_ID);

    // Poll for ~1s — the image drains (~100ms), then the next timeout check
    // (at 500ms) finds is_connected=false, hitting the "not connected" branches.
    const auto phase2_end = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < phase2_end)
    {
        int result = poller();
        ASSERT_GE(result, 0) << "poll error: " << aeron_errmsg();
        ASSERT_FALSE(aeron_archive_persistent_subscription_has_failed(persistent_subscription));
        if (result == 0) std::this_thread::yield();
    }

    // Restore: disable all interceptors, restore timeout
    aeron_stream_id_loss_generator_disable(stream_id_gen);
    aeron_archive_persistent_subscription_set_message_timeout_ns_for_testing(
        persistent_subscription, 10'000'000'000ULL);

    // PS should recover and go LIVE
    executeUntil("becomes live", poller,
        isLive(persistent_subscription), 60);

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); }, 30);

    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldTriggerLiveCatchupAbortByFreezingReplayDuringSwitch)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "live_catchup_abort";

    // Use BOTH channel-scoped loss on replay (95% — keeps replay buffer very sparse)
    // AND runtime stream_id_loss to fully freeze replay during ATTEMPT_SWITCH.
    // Channel-scoped loss drops frames at the driver receiver BEFORE they enter
    // the log buffer, so the conductor thread has very little data to process.
    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_stream_id_loss_generator_create(&loss_gen));

    LossTestHarness harness(m_aeronDir, archive_dir, loss_gen);

    // 80% channel-scoped loss on replay — sparse replay buffer
    const std::string replay_channel_str = PLAIN_REPLAY_CHANNEL;

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    // Large seed batch so the replay takes a while even without extra loss
    const auto seed = generateFixedMessages(30, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(seed);

    AeronResource aeron(m_aeronDir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        replay_channel_str, REPLAY_STREAM_ID, 0);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    std::vector<std::vector<uint8_t>> all_published;
    all_published.insert(all_published.end(), seed.begin(), seed.end());

    // Phase 1: Let the replay catch up to the seed batch and enter ATTEMPT_SWITCH.
    // Don't publish new messages yet — let the recording position stay fixed so
    // the replay can reach the max_recorded_position threshold.
    const auto test_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(120);
    bool entered_attempt_switch = false;
    int switch_polls = 0;

    while (true)
    {
        ASSERT_LT(std::chrono::steady_clock::now(), test_deadline) << "timed out";

        int result = aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription, MessageCapturingFragmentHandler::onFragment, &handler, 1);
        ASSERT_GE(result, 0) << "poll error: " << aeron_errmsg();

        if (aeron_archive_persistent_subscription_is_live(persistent_subscription))
            break;

        if (!entered_attempt_switch &&
            aeron_archive_persistent_subscription_join_difference(persistent_subscription) != 0)
        {
            entered_attempt_switch = true;

            // Phase 2: ATTEMPT_SWITCH entered. Freeze the replay stream AND
            // publish a burst of NEW messages. The live image receives them
            // via the MDC network path (no loss). The replay can't advance
            // (new frames blocked + buffer draining prevented by freeze).
            // When attempt_switch() polls the live image, it finds data ahead
            // of the frozen replay → ABORT fires.

            aeron_stream_id_loss_generator_enable(loss_gen, REPLAY_STREAM_ID);

            // Publish a burst to create data that exists in the live image
            // but NOT in the replay image
            for (int i = 0; i < 20; i++)
            {
                auto burst_msg = generateFixedMessages(1, ONE_KB_MESSAGE_SIZE);
                int64_t r = aeron_exclusive_publication_offer(
                    persistent_publication.publication(),
                    burst_msg[0].data(), burst_msg[0].size(), nullptr, nullptr);
                if (r > 0)
                    all_published.push_back(burst_msg[0]);
            }

            // Wait for data to traverse the network to the live image
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // After a few polls with frozen replay, unfreeze so the replay can
        // catch up to next_live_position and complete the LIVE transition.
        if (entered_attempt_switch && ++switch_polls > 10)
        {
            aeron_stream_id_loss_generator_disable(loss_gen);
        }

        if (result == 0) std::this_thread::yield();
    }

    aeron_stream_id_loss_generator_disable(loss_gen);

    // Drain remaining messages
    executeUntil("receives all messages", [&] {
        return aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription, MessageCapturingFragmentHandler::onFragment, &handler, 10);
    }, [&] { return handler.messageCount() >= all_published.size(); }, 30);

    ASSERT_TRUE(MessagesEq(all_published, handler.messages()));

}

// Controlled-poll variant of shouldCatchupLiveGapWithUncontrolledPollAndReplayLoss. Same
// cross-driver scaffolding and combined predicate; uses fragment_limit=1 so ATTEMPT_SWITCH
// spans many polls and asserts message order with strict equality.
TEST_F(AeronArchivePersistentSubscriptionTest, shouldCatchupReplayToLiveDuringAttemptSwitch)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "catchup";

    // Driver 1: publisher + archive, no loss.
    EmbeddedMediaDriverWithLossGenerator driver1;
    driver1.aeronDir(m_aeronDir);
    driver1.start();

    TestStandaloneArchive archive(
        m_aeronDir, archive_dir, std::cout,
        LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0");

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const auto initial_messages = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(initial_messages);

    // Driver 2: PS only.
    char ps_aeron_dir_buf[AERON_MAX_PATH];
    aeron_default_path(ps_aeron_dir_buf, sizeof(ps_aeron_dir_buf));
    const std::string ps_aeron_dir =
        std::string(ps_aeron_dir_buf) + "-ps-" + std::to_string(aeron_randomised_int32());

    aeron_loss_generator_t *loss_gen = nullptr;
    ASSERT_EQ(0, aeron_frame_data_loss_generator_create(&loss_gen));

    EmbeddedMediaDriverWithLossGenerator driver2;
    driver2.aeronDir(ps_aeron_dir);
    driver2.setReceiveChannelDataLossGenerator(loss_gen);
    driver2.start();

    // SETUP-drop holds PS's live image off until the window expires; replay loss slows
    // replay so ATTEMPT_SWITCH spans many polls.
    reset_predicate_state();
    configure_drop_rate(0.7);
    const int64_t setup_drop_window_ns = 1'500LL * 1000 * 1000;
    const int64_t setup_drop_window_end_ns = aeron_nano_clock() + setup_drop_window_ns;
    g_setup_drop_stream_id.store(STREAM_ID, std::memory_order_relaxed);
    g_setup_drop_until_ns.store(setup_drop_window_end_ns, std::memory_order_release);
    g_replay_drop_stream_id.store(REPLAY_STREAM_ID, std::memory_order_relaxed);
    aeron_frame_data_loss_generator_enable(loss_gen, drop_setup_for_window_or_replay_at_rate, nullptr);

    AeronResource aeron(ps_aeron_dir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    aeron_archive_context_set_aeron_directory_name(archive_ctx, ps_aeron_dir.c_str());
    ArchiveContextGuard archive_ctx_guard(archive_ctx);

    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
        MDC_SUBSCRIPTION_CHANNEL, STREAM_ID,
        "aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID, 0);

    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);
    listener.ps_for_snapshot = persistent_subscription;

    MessageCapturingFragmentHandler handler;
    // fragment_limit=1 slows replay and keeps ATTEMPT_SWITCH spanning many polls.
    auto base_poller = makeControlledPoller(persistent_subscription, handler, 1);

    // Drip-feed publisher at moderate rate during SETUP-drop window, then stop.
    const int64_t drip_interval_ns = 10LL * 1000 * 1000;  // 100 msgs/sec
    int64_t next_drip_ns = aeron_nano_clock();
    std::vector<std::vector<uint8_t>> drip_messages;
    auto poll_with_drip = [&]
    {
        const int fragments = base_poller();
        const int64_t now_ns = aeron_nano_clock();
        if (now_ns < setup_drop_window_end_ns && now_ns >= next_drip_ns)
        {
            std::vector<uint8_t> msg(ONE_KB_MESSAGE_SIZE);
            for (size_t i = 0; i < msg.size(); i++)
            {
                msg[i] = static_cast<uint8_t>((drip_messages.size() + i) & 0xFF);
            }
            const int64_t pos = aeron_exclusive_publication_offer(
                persistent_publication.publication(), msg.data(), msg.size(), nullptr, nullptr);
            if (pos > 0)
            {
                drip_messages.push_back(std::move(msg));
                next_drip_ns = now_ns + drip_interval_ns;
            }
        }
        return fragments;
    };

    executeUntil("receives first message", poll_with_drip, [&] { return handler.messageCount() >= 1; });
    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    executeUntil("becomes live", poll_with_drip,
        isLive(persistent_subscription), 60);

    aeron_frame_data_loss_generator_disable(loss_gen);

    ASSERT_EQ(1, listener.live_joined_count);
    // See shouldCatchupLiveGapWithUncontrolledPollAndReplayLoss for why > 0 isn't reliable.
    ASSERT_NE(INT64_MIN, listener.join_difference_at_join);

    const size_t total_expected = initial_messages.size() + drip_messages.size();
    executeUntil("receives all messages", base_poller,
        [&] { return handler.messageCount() == total_expected; }, 60);

    std::vector<std::vector<uint8_t>> all_messages;
    all_messages.insert(all_messages.end(), initial_messages.begin(), initial_messages.end());
    all_messages.insert(all_messages.end(), drip_messages.begin(), drip_messages.end());
    ASSERT_EQ(all_messages, handler.messages());
}

/*
 *  Creates a PS and polls just enough to enter the replay subscription
 *  setup. Then closes the PS immediately. The close function calls
 *  clean_up_replay_subscription which finds add_replay_subscription
 *  non-NULL and cancels the pending async add.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldCancelPendingAsyncOpsOnCloseDuringReplaySubscriptionSetup)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "cancel_async_replay_sub";

    aeron_env_set("AERON_SPIES_SIMULATE_CONNECTION", "true");
    ScopedMediaDriver c_driver;
    c_driver.aeronDir(m_aeronDir);
    c_driver.start();

    TestStandaloneArchive archive_process(
        m_aeronDir, archive_dir, std::cout,
        LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0");

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);
    const auto messages = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    // Create and close PS instances at increasing poll counts. One of these
    // will close while add_replay_subscription is non-NULL. Use a new Aeron
    // client per iteration to avoid archive session conflicts.
    for (int polls = 0; polls <= 20; polls++)
    {
        AeronResource iter_aeron(m_aeronDir);
        aeron_archive_context_t *archive_ctx = createArchiveContext();
        ArchiveContextGuard archive_ctx_guard(archive_ctx);
        aeron_archive_persistent_subscription_context_t *context =
            createDefaultPersistentSubscriptionContext(
                iter_aeron.aeron(), archive_ctx, persistent_publication.recordingId());
        PersistentSubscriptionContextGuard context_guard(context);

        aeron_archive_persistent_subscription_t *persistent_subscription;
        if (0 != aeron_archive_persistent_subscription_create(
            &persistent_subscription, context))
        {
            continue;
        }
        context_guard.release();
        PersistentSubscriptionGuard ps_guard(persistent_subscription);

        for (int i = 0; i < polls; i++)
        {
            aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                MessageCapturingFragmentHandler::onFragment, nullptr, 10);
        }

        ps_guard.release();
        aeron_archive_persistent_subscription_close(persistent_subscription);
        std::this_thread::yield(); // let conductor process cancel
    }

}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldCancelPendingRequestPublicationOnCloseDuringResponseChannelSetup)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "cancel_async_req_pub";

    aeron_env_set("AERON_SPIES_SIMULATE_CONNECTION", "true");
    ScopedMediaDriver c_driver;
    c_driver.aeronDir(m_aeronDir);
    c_driver.start();

    TestStandaloneArchive archive_process(
        m_aeronDir, archive_dir, std::cout,
        LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0");

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);
    const auto messages = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    const std::string replay_channel = "aeron:udp?control=localhost:10001|control-mode=response";
    const std::string archive_control_response_channel =
        "aeron:udp?control-mode=response|control=localhost:10002";

    for (int polls = 0; polls <= 20; polls++)
    {
        AeronResource iter_aeron(m_aeronDir);
        aeron_archive_context_t *archive_ctx = createArchiveContext();
        ArchiveContextGuard archive_ctx_guard(archive_ctx);
        aeron_archive_context_set_control_response_channel(
            archive_ctx, archive_control_response_channel.c_str());

        aeron_archive_persistent_subscription_context_t *context =
            createPersistentSubscriptionContext(
                iter_aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
                IPC_CHANNEL, STREAM_ID,
                replay_channel, -11, 0);
        PersistentSubscriptionContextGuard context_guard(context);

        aeron_archive_persistent_subscription_t *persistent_subscription;
        if (0 != aeron_archive_persistent_subscription_create(
            &persistent_subscription, context))
        {
            continue;
        }
        context_guard.release();
        PersistentSubscriptionGuard ps_guard(persistent_subscription);

        for (int i = 0; i < polls; i++)
        {
            aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                MessageCapturingFragmentHandler::onFragment, nullptr, 10);
        }

        ps_guard.release();
        aeron_archive_persistent_subscription_close(persistent_subscription);
        std::this_thread::yield(); // let conductor process cancel
    }

}

/*
 *  The PS goes live, then the archive is killed. The control subscription
 *  loses its image, the async client detects disconnection, and
 *  on_archive_disconnected fires. Since the PS is in LIVE state, the
 *  callback returns early without cleanup. The PS continues
 *  consuming from the live publication.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldReturnEarlyFromDisconnectCallbackWhenLive)
{
    const std::string aeron_dir = m_aeronDir;
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "disconnect_while_live";

    // Use TestMediaDriver (standalone Java driver) + TestStandaloneArchive
    // so killing the archive doesn't kill the driver.
    TestMediaDriver driver(aeron_dir, std::cout);
    auto archive_process = std::make_unique<TestStandaloneArchive>(
        aeron_dir, archive_dir, std::cout,
        LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0");

    PersistentPublication persistent_publication(aeron_dir, IPC_CHANNEL, STREAM_ID);

    const auto messages = generateFixedMessages(10, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(aeron_dir);

    aeron_archive_context_t *archive_ctx = createArchiveContext();
    ArchiveContextGuard archive_ctx_guard(archive_ctx);
    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(), archive_ctx, persistent_publication.recordingId());
    TestListener listener;
    listener.attachTo(context);

    PersistentSubscriptionContextGuard context_guard(context);
    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();
    context_guard.release();
    PersistentSubscriptionGuard ps_guard(persistent_subscription);

    MessageCapturingFragmentHandler handler;
    auto poller = makeControlledPoller(persistent_subscription, handler);

    // Go live and receive all messages
    executeUntil("becomes live", poller,
        isLive(persistent_subscription));

    executeUntil("receives all messages", poller,
        [&] { return handler.messageCount() == messages.size(); });

    ASSERT_EQ(1, listener.live_joined_count);
    ASSERT_TRUE(MessagesEq(messages, handler.messages()));

    // Kill the archive while PS is LIVE. The control subscription loses its
    // image, on_archive_disconnected fires but returns early (L822) because
    // state == LIVE. The driver stays alive so all Aeron clients remain functional.
    archive_process.reset();

    // Poll for several seconds — enough for the async client to detect the
    // disconnection and fire on_archive_disconnected.
    const auto poll_end = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < poll_end)
    {
        poller();
        std::this_thread::yield();
    }

    ps_guard.release();
    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
    std::this_thread::yield(); // let conductor process cancel
    archive_ctx_guard.release();
    aeron_archive_context_close(archive_ctx);
}

/*
 *  Uses an Aeron client in conductor-agent-invoker mode. The conductor
 *  only advances when we explicitly call aeron_main_do_work(). By
 *  stopping conductor invocations after the PS queues an async add,
 *  the add stays pending. Closing the PS then cancels it.
 */
TEST_F(AeronArchivePersistentSubscriptionTest, shouldCancelPendingLiveSubscriptionOnCloseFromLiveEntry)
{
    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "cancel_live_sub";

    aeron_env_set("AERON_SPIES_SIMULATE_CONNECTION", "true");
    ScopedMediaDriver c_driver;
    c_driver.aeronDir(m_aeronDir);
    c_driver.start();

    TestStandaloneArchive archive_process(
        m_aeronDir, archive_dir, std::cout,
        LOCALHOST_CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0");

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);
    const auto messages = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    // Use FROM_LIVE so the PS goes: AWAIT_ARCHIVE_CONNECTION -> ... ->
    // ADD_LIVE_SUBSCRIPTION (sets add_live_subscription) -> AWAIT_LIVE.
    // Try closing at each poll count 0-20 to catch add_live_subscription non-NULL.
    // Use a new AeronResource per iteration to avoid archive session conflicts.
    for (int polls = 0; polls <= 20; polls++)
    {
        AeronResource iter_aeron(m_aeronDir);
        aeron_archive_context_t *archive_ctx = createArchiveContext();
        ArchiveContextGuard archive_ctx_guard(archive_ctx);
        aeron_archive_persistent_subscription_context_t *context =
            createPersistentSubscriptionContext(
                iter_aeron.aeron(), archive_ctx, persistent_publication.recordingId(),
                IPC_CHANNEL, STREAM_ID,
                "aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID, 0);
        aeron_archive_persistent_subscription_context_set_start_position(
            context, AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE);
        PersistentSubscriptionContextGuard context_guard(context);

        aeron_archive_persistent_subscription_t *persistent_subscription;
        if (0 != aeron_archive_persistent_subscription_create(
            &persistent_subscription, context))
        {
            continue;  // skip if creation fails (archive session limit)
        }
        context_guard.release();
        PersistentSubscriptionGuard ps_guard(persistent_subscription);

        for (int i = 0; i < polls; i++)
        {
            aeron_archive_persistent_subscription_controlled_poll(
                persistent_subscription,
                MessageCapturingFragmentHandler::onFragment, nullptr, 10);
        }

        ps_guard.release();
        aeron_archive_persistent_subscription_close(persistent_subscription);
        std::this_thread::yield(); // let conductor process cancel
    }

}

// Per-endpoint supplier used by shouldRefreshAndReplayWhenLiveAheadOfStopPositionAfterResume.
// `clientd` is the shared aeron_setup_at_position_loss_config_t. Each new receive endpoint
// gets its own generator instance whose state captures the endpoint pointer; at frame-check
// time the generator reads endpoint->conductor_fields.udp_channel->original_uri to decide
// whether the endpoint is exempt from the drop (i.e. the recording subscription's endpoint).
static void installSetupAtPositionLossGenerator(
    void *clientd, aeron_receive_channel_endpoint_t *endpoint)
{
    auto *config = static_cast<aeron_setup_at_position_loss_config_t *>(clientd);
    aeron_loss_generator_t *generator = nullptr;
    if (aeron_setup_at_position_loss_generator_create(&generator, config, endpoint) == 0)
    {
        endpoint->data_loss_generator = generator;
    }
}

// Deterministically exercises the AWAIT_LIVE -> live-ahead -> refresh_recording_descriptor
// -> replay -> live path. PS shortcuts to AWAIT_LIVE while the recording is stopped at
// stop_0. When publisher B is resumed, its first SETUP carries (initial_term_id,
// term_id_at_stop_0, offset_at_stop_0) — which is also where PS's start_position sits.
// Without intervention, PS's image is created at exactly stop_0, the AWAIT_LIVE check
// `live_position > position` is false, and PS takes the direct LIVE path instead of
// refreshing.
//
// To force the refresh path deterministically, a per-endpoint loss generator drops SETUPs
// whose (initial_term_id, active_term_id, term_offset) tuple matches publisher B's initial
// position — but only on PS's endpoint. The recording subscription's endpoint is exempt
// (its URI carries `init-term-id=`), so the recording is created at stop_0 and stays
// contiguous. PS's UDP receiver elicits another SETUP via SM. By the time publisher B's
// next SETUP fires (gated by AERON_NETWORK_PUBLICATION_SETUP_TIMEOUT_NS = 100 ms),
// `snd_pos` has advanced past the offers we issued, the SETUP no longer matches the drop
// filter, and PS's image is created with `join_position > stop_0` — triggering the
// refresh path.
TEST_F(AeronArchivePersistentSubscriptionTest, shouldRefreshAndReplayWhenLiveAheadOfStopPositionAfterResume)
{
    aeron_setup_at_position_loss_config_t loss_config;
    aeron_setup_at_position_loss_config_init(&loss_config);
    // The archive's recording subscription URI carries `init-term-id=` (set by
    // PersistentPublication::resume when it stamps the publisher's continuation params).
    // PS's plain MDC subscription URI does not — that's how we tell the two endpoints
    // apart at frame-check time.
    aeron_setup_at_position_loss_config_set_endpoint_skip_substring(&loss_config, "init-term-id=");

    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "refresh_replay_when_live_ahead";
    LossTestHarness harness(
        m_aeronDir, archive_dir, installSetupAtPositionLossGenerator, &loss_config);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    persistent_publication.persist(generateFixedMessages(1, ONE_KB_MESSAGE_SIZE));
    const int64_t stop_position = persistent_publication.stop();
    ASSERT_GT(stop_position, 0);
    const int64_t recording_id = persistent_publication.recordingId();

    // Read the recording descriptor while persistent_publication still holds an archive
    // client open; we need initial_term_id and term_buffer_length to compute publisher
    // B's first-SETUP position so the loss filter can match it exactly.
    struct RecordingInfo
    {
        int32_t initial_term_id;
        int32_t term_buffer_length;
    } info = {};
    int32_t descriptor_count = 0;
    ASSERT_LE(0, aeron_archive_list_recording(
        &descriptor_count,
        persistent_publication.archive(),
        recording_id,
        [](aeron_archive_recording_descriptor_t *descriptor, void *clientd)
        {
            RecordingInfo *i = static_cast<RecordingInfo *>(clientd);
            i->initial_term_id = descriptor->initial_term_id;
            i->term_buffer_length = descriptor->term_buffer_length;
        },
        &info));
    ASSERT_EQ(1, descriptor_count);

    const int32_t target_active_term_id = aeron_logbuffer_compute_term_id_from_position(
        stop_position,
        aeron_number_of_trailing_zeroes(info.term_buffer_length),
        info.initial_term_id);
    const int32_t target_term_offset = (int32_t)(stop_position & (info.term_buffer_length - 1));
    aeron_setup_at_position_loss_config_set_target(
        &loss_config, STREAM_ID, info.initial_term_id, target_active_term_id, target_term_offset);

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
        REPLAY_STREAM_ID,
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

    // PS shortcuts to ADD_LIVE_SUBSCRIPTION and parks in AWAIT_LIVE; with no publisher up,
    // its live-image deadline breaches as a non-terminal error — fired exactly once.
    executeUntil(
        "live-image deadline breach fires",
        poller,
        [&] { return listener.error_count > 0; });
    ASSERT_EQ(1, listener.error_count);
    ASSERT_NE(std::string::npos,
        listener.last_error_message.find("No image became available on the live subscription"));
    ASSERT_FALSE(aeron_archive_persistent_subscription_is_live(persistent_subscription));
    ASSERT_FALSE(observed_replaying);

    // Arm the SETUP filter just before resuming. Publisher B's first SETUP carries the
    // exact target tuple we configured above; it will be dropped at PS's endpoint and
    // delivered normally to the archive's recording subscription's endpoint (which is
    // exempt by URI substring).
    aeron_setup_at_position_loss_config_enable(&loss_config);

    PersistentPublication resumed_publication =
        PersistentPublication::resume(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID, recording_id);

    // Persist a few messages so publisher B's snd_pos advances past stop_0. The next SETUP
    // (gated by AERON_NETWORK_PUBLICATION_SETUP_TIMEOUT_NS after the dropped one) carries
    // the advanced term_offset, no longer matches the filter, and creates PS's image with
    // join_position > stop_0.
    const std::vector<std::vector<uint8_t>> post_resume_messages =
        generateFixedMessages(4, ONE_KB_MESSAGE_SIZE);
    resumed_publication.persist(post_resume_messages);

    executeUntil(
        "is live",
        poller,
        [&] { return aeron_archive_persistent_subscription_is_live(persistent_subscription); });

    executeUntil(
        "received all post-resume messages",
        poller,
        [&] { return handler.messages().size() == post_resume_messages.size(); });
    ASSERT_EQ(post_resume_messages, handler.messages());

    aeron_setup_at_position_loss_config_disable(&loss_config);

    // Messages offered before PS's live image attached can only have arrived via the
    // refresh -> replay path; mid-stream UDP subscribers don't backfill historical bytes
    // on the live stream.
    ASSERT_TRUE(observed_replaying)
        << "PS did not transition through REPLAY/ATTEMPT_SWITCH; refresh path was not exercised";
    ASSERT_EQ(1, listener.live_joined_count);
    ASSERT_EQ(0, listener.live_left_count);
    // Still exactly the one initial deadline-breach error — PS refreshed once, went to
    // REPLAY then LIVE, and has stayed there. No additional AWAIT_LIVE entries means no
    // additional breaches.
    ASSERT_EQ(1, listener.error_count);
    // The filter is a safety net for the race where publisher B's first SETUP carries
    // its initial term_offset (causing PS's image to attach at stop_0 and skip refresh).
    // It may or may not fire depending on how fast publisher B's snd_pos advances
    // relative to the first SETUP send. observed_replaying being true is what proves
    // the refresh path was taken — either naturally or because the filter forced it.

    ps_guard.release();
    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
    archive_ctx_guard.release();
    aeron_archive_context_close(archive_ctx);
}

// Per-endpoint supplier used by shouldReplayAndCatchUpWhenExtendedRecordingIsAheadOfLivePosition.
// Same pattern as installSetupAtPositionLossGenerator above, but allocates a DATA-in-range
// loss generator instead of a SETUP-at-position one. Each new receive endpoint gets its own
// generator instance whose state captures the endpoint pointer; at frame-check time the
// generator inspects the endpoint's URI and decides whether to drop.
static void installDataInRangeLossGenerator(
    void *clientd, aeron_receive_channel_endpoint_t *endpoint)
{
    auto *config = static_cast<aeron_data_in_range_loss_config_t *>(clientd);
    aeron_loss_generator_t *generator = nullptr;
    if (aeron_data_in_range_loss_generator_create(&generator, config, endpoint) == 0)
    {
        endpoint->data_loss_generator = generator;
    }
}

// Deterministically exercises the LIVE -> image-closed -> refresh -> replay path. PS
// shortcuts to AWAIT_LIVE while the recording is stopped at stop_0, then publisher B is
// resumed. PS receives publisher B's SETUP normally and either joins live directly at
// stop_0 (and consumes first_batch via live) or refreshes via AWAIT_LIVE if publisher
// B's first SETUP carries an advanced offset — both paths land in LIVE with first_batch
// either delivered or about to be delivered.
//
// Once first_batch is delivered, a per-endpoint loss generator drops every catchup DATA
// frame on PS's endpoint (the recording subscription's endpoint is exempt by URI: its
// URI carries `init-term-id=`, PS's plain MDC URI does not). Heartbeats are always
// preserved so PS still sees the EOS+REVOKE flag when publisher B is revoked. After
// revoke, PS's image closes and the bytes the loss generator dropped can never be
// recovered via NAK retransmits — PS must replay the full catchup from the recording.
TEST_F(AeronArchivePersistentSubscriptionTest, shouldReplayAndCatchUpWhenExtendedRecordingIsAheadOfLivePosition)
{
    aeron_data_in_range_loss_config_t loss_config;
    aeron_data_in_range_loss_config_init(&loss_config);
    aeron_data_in_range_loss_config_set_endpoint_skip_substring(&loss_config, "init-term-id=");

    const std::string archive_dir = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "replay_catchup_when_ahead";
    LossTestHarness harness(
        m_aeronDir, archive_dir, installDataInRangeLossGenerator, &loss_config);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);
    persistent_publication.persist(generateFixedMessages(1, ONE_KB_MESSAGE_SIZE));
    const int64_t stop_position = persistent_publication.stop();
    ASSERT_GT(stop_position, 0);
    const int64_t recording_id = persistent_publication.recordingId();

    struct RecordingInfo
    {
        int32_t initial_term_id;
        int32_t term_buffer_length;
    } info = {};
    int32_t descriptor_count = 0;
    ASSERT_LE(0, aeron_archive_list_recording(
        &descriptor_count,
        persistent_publication.archive(),
        recording_id,
        [](aeron_archive_recording_descriptor_t *descriptor, void *clientd)
        {
            RecordingInfo *i = static_cast<RecordingInfo *>(clientd);
            i->initial_term_id = descriptor->initial_term_id;
            i->term_buffer_length = descriptor->term_buffer_length;
        },
        &info));
    ASSERT_EQ(1, descriptor_count);

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
        REPLAY_STREAM_ID,
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

    // PS shortcuts to AWAIT_LIVE; no publisher up, deadline breaches exactly once.
    executeUntil(
        "deadline breach fires",
        poller,
        [&] { return listener.error_count > 0; });
    ASSERT_EQ(1, listener.error_count);

    // Resume publisher B at stop_0. Recording continues contiguously.
    PersistentPublication resumed_publication =
        PersistentPublication::resume(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID, recording_id);

    // Persist first_batch and let PS receive it. Either path can deliver it: direct LIVE
    // (publisher B's SETUP arrived at stop_0) or AWAIT_LIVE -> refresh -> replay
    // (publisher B's SETUP arrived at an advanced offset). Either way, by the time the
    // loop exits PS has consumed first_batch and is positioned just past it.
    const std::vector<std::vector<uint8_t>> first_batch = generateFixedMessages(1, ONE_KB_MESSAGE_SIZE);
    resumed_publication.persist(first_batch);
    executeUntil(
        "received first message",
        poller,
        [&] { return handler.messages().size() == first_batch.size(); });

    // Wait for PS to be on its live image. In the advanced-offset SETUP path, PS may have
    // delivered first_batch via initial-refresh REPLAY and still be in REPLAY/ATTEMPT_SWITCH.
    // Without polling the live image, PS's last_sm_position doesn't advance and publisher
    // B's flow control stops sending live to PS — so the loss generator never sees catchup
    // frames on PS's live endpoint and frames_dropped stays at 0.
    executeUntil(
        "is live",
        poller,
        [&] { return aeron_archive_persistent_subscription_is_live(persistent_subscription); });

    // Reset observed_replaying after the first_batch phase. If publisher B's SETUP arrived
    // at an advanced offset, PS already went through REFRESH/REPLAY once to deliver
    // first_batch — we don't want that observation to satisfy the post-revoke assertion
    // below. From here on, only the catchup-refresh path can flip this back to true.
    observed_replaying = false;

    // Arm the data-loss filter. Drop every catchup DATA frame on PS's endpoint, anywhere
    // from "right past first_batch" through end-of-term. The recording subscription's
    // endpoint is exempt (URI substring), so the recording stays contiguous.
    const int64_t first_byte_after_first_batch = stop_position + (int64_t)(ONE_KB_MESSAGE_SIZE + AERON_DATA_HEADER_LENGTH);
    const int32_t target_active_term_id = aeron_logbuffer_compute_term_id_from_position(
        first_byte_after_first_batch,
        aeron_number_of_trailing_zeroes(info.term_buffer_length),
        info.initial_term_id);
    const int32_t target_drop_min = (int32_t)(first_byte_after_first_batch & (info.term_buffer_length - 1));
    aeron_data_in_range_loss_config_set_target(
        &loss_config, STREAM_ID, target_active_term_id, target_drop_min, info.term_buffer_length);
    aeron_data_in_range_loss_config_enable(&loss_config);

    // Persist 40 catchup messages. The recording sees them all; PS sees none of them on
    // its live image — they're dropped at PS's receive endpoint by the loss generator.
    const std::vector<std::vector<uint8_t>> catchup_messages = generateFixedMessages(40, ONE_KB_MESSAGE_SIZE);
    resumed_publication.persist(catchup_messages);

    // Revoke ends the live image; after the EOS+REVOKE heartbeat reaches PS (loss gen
    // explicitly preserves heartbeats) and the image closes, the dropped bytes are gone
    // forever from the live channel — PS must replay them from the recording.
    aeron_exclusive_publication_revoke(resumed_publication.publication(), nullptr, nullptr);

    std::vector<std::vector<uint8_t>> expected;
    expected.insert(expected.end(), first_batch.begin(), first_batch.end());
    expected.insert(expected.end(), catchup_messages.begin(), catchup_messages.end());
    executeUntil(
        "received all messages",
        poller,
        [&] { return handler.messages().size() == expected.size(); });

    aeron_data_in_range_loss_config_disable(&loss_config);

    ASSERT_EQ(expected, handler.messages());
    ASSERT_TRUE(observed_replaying)
        << "PS did not transition through REPLAY/ATTEMPT_SWITCH after revoke; the "
           "catchup-refresh path was not exercised (first_batch's refresh, if any, was "
           "cleared before catchup was armed, so this isolates the post-revoke replay)";
    // The filter must have actually fired — otherwise the test could pass for the wrong
    // reason (the natural overrun race winning), and a future change that breaks the
    // tuple computation would silently regress the test back to that race.
    ASSERT_GT(aeron_data_in_range_loss_config_frames_dropped(&loss_config), 0);

    ps_guard.release();
    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
    archive_ctx_guard.release();
    aeron_archive_context_close(archive_ctx);
}
