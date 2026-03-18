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
#include <random>
#include <utility>

#include "gtest/gtest.h"
#include "../TestArchive.h"
#include "ArchiveClientTestUtils.h"
#include "uri/aeron_uri_string_builder.h"

extern "C"
{
#include "aeron_common.h"
#include "client/aeron_archive.h"
#include "client/aeron_archive_persistent_subscription.h"
}

static const std::string IPC_CHANNEL = "aeron:ipc";
static const std::string MDC_PUBLICATION_CHANNEL = "aeron:udp?control=localhost:2000|control-mode=dynamic|fc=max";
static const std::string MDC_SUBSCRIPTION_CHANNEL = "aeron:udp?control=localhost:2000";
static const std::string UNICAST_CHANNEL = "aeron:udp?endpoint=localhost:2000";
static const int32_t STREAM_ID = 1000;
static const int32_t ONE_KB_MESSAGE_SIZE = 1024 - AERON_DATA_HEADER_LENGTH;

struct ListenerState
{
    int error_count = 0;
    int last_errcode = 0;
    std::string last_error_message;
};

struct LiveEventListenerState
{
    int live_joined_count = 0;
    int live_left_count = 0;
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
        const auto receiver = static_cast<MessageCapturingFragmentHandler *>(clientd);
        std::vector<uint8_t> message(length);
        message.assign(buffer, buffer + length);
        receiver->m_messages.push_back(message);
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

    void clear()
    {
        m_messages.clear();
    }

private:
    std::vector<std::vector<uint8_t>> m_messages;
};

class PersistentPublication
{
public:
    explicit PersistentPublication(const std::string& aeronDir, const std::string& channel, const int32_t streamId)
    {
        aeron_archive_context_t *archive_ctx = nullptr;
        aeron_archive_context_init(&archive_ctx);
        aeron_archive_context_set_aeron_directory_name(archive_ctx, aeronDir.c_str());
        aeron_archive_context_set_control_request_channel(archive_ctx, "aeron:udp?endpoint=localhost:8010");
        aeron_archive_context_set_control_response_channel(archive_ctx, "aeron:udp?endpoint=localhost:0");
        Credentials::defaultCredentials().configure(archive_ctx);

        aeron_archive_t *archive = nullptr;
        const auto connect_result = aeron_archive_connect(&archive, archive_ctx);
        aeron_archive_context_close(archive_ctx);
        if (connect_result < 0)
        {
            throw std::runtime_error("failed to connect to archive " + std::string(aeron_errmsg()));
        }

        aeron_exclusive_publication_t *publication = nullptr;
        if (aeron_archive_add_recorded_exclusive_publication(&publication, archive, channel.c_str(), streamId) < 0)
        {
            aeron_archive_close(archive);
            throw std::runtime_error("failed to add recorded publication " + std::string(aeron_errmsg()));
        }

        aeron_publication_constants_t constants;
        aeron_exclusive_publication_constants(publication, &constants);
        m_maxPayloadLength = constants.max_payload_length;

        aeron_t *aeron = aeron_archive_context_get_aeron(aeron_archive_get_archive_context(archive));
        aeron_counters_reader_t *counters_reader = aeron_counters_reader(aeron);
        int32_t rec_pos_id;
        while (AERON_NULL_COUNTER_ID == (rec_pos_id = aeron_archive_recording_pos_find_counter_id_by_session_id(
            counters_reader, constants.session_id)))
        {
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
    }

    int64_t recordingId() const
    {
        return m_recordingId;
    }

    int32_t maxPayloadLength() const
    {
        return m_maxPayloadLength;
    }

    int64_t stop()
    {
        if (aeron_archive_stop_recording_exclusive_publication(m_archive, m_publication) < 0)
        {
            throw std::runtime_error("failed to stop recording " + std::string(aeron_errmsg()));
        }

        int64_t stop_position = AERON_NULL_VALUE;
        while (stop_position == AERON_NULL_VALUE)
        {
            aeron_archive_get_stop_position(&stop_position, m_archive, m_recordingId);
            std::this_thread::yield();
        }

        return stop_position;
    }

    void persist(const std::vector<std::vector<uint8_t>>& messages) const
    {
        offer(messages);

        const auto position = aeron_exclusive_publication_position(m_publication);
        while (*aeron_counters_reader_addr(m_countersReader, m_recPosId) < position)
        {
            std::this_thread::yield();
        }
    }

    void offer(const std::vector<std::vector<uint8_t>>& messages) const
    {
        for (auto& message : messages)
        {
            while (true)
            {
                const auto result = aeron_exclusive_publication_offer(
                    m_publication,
                    message.data(),
                    message.size(),
                    nullptr,
                    nullptr);

                if (result > 0)
                {
                    break;
                }

                if (result == AERON_PUBLICATION_NOT_CONNECTED ||
                    result == AERON_PUBLICATION_CLOSED ||
                    result == AERON_PUBLICATION_MAX_POSITION_EXCEEDED ||
                    result == AERON_PUBLICATION_ERROR)
                {
                    throw std::runtime_error("offer returned " + std::to_string(result));
                }
            }
        }
    }

private:
    int32_t m_maxPayloadLength;
    aeron_archive_t *m_archive;
    aeron_exclusive_publication_t *m_publication;
    aeron_counters_reader_t *m_countersReader;
    int64_t m_recordingId;
    int32_t m_recPosId;
};

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
        return {
            aeronDir,
            ARCHIVE_DIR,
            std::cout,
            "aeron:udp?endpoint=localhost:8010",
            "aeron:udp?endpoint=localhost:0",
            1
        };
    }

    static aeron_archive_context_t *createArchiveContext()
    {
        aeron_archive_context_t *ctx;
        aeron_archive_context_init(&ctx);
        aeron_archive_context_set_control_request_channel(ctx, "aeron:udp?endpoint=localhost:8010");
        aeron_archive_context_set_control_response_channel(ctx, "aeron:udp?endpoint=localhost:0");
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

    aeron_archive_persistent_subscription_context_t *createDefaultPersistentSubscriptionContext(
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
            const auto message = &messages[i];
            const int length = m_lengthGenerator(m_randomEngine);
            message->reserve(length);
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
        const std::function<bool()>& predicate)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
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

private:
    std::random_device m_randomDevice;
    std::default_random_engine m_randomEngine = std::default_random_engine(m_randomDevice());
    std::uniform_int_distribution<> m_lengthGenerator = std::uniform_int_distribution<>(0, 2048);
    std::uniform_int_distribution<uint8_t> m_byteGenerator = std::uniform_int_distribution<uint8_t>(0, UINT8_MAX);
};

TEST_F(AeronArchivePersistentSubscriptionTest, shouldReplayAndSwitchToLiveWithNoMessagesBeingPublishedDuringSwitch)
{
    TestArchive archive = createArchive(m_aeronDir);

    const std::vector<std::vector<uint8_t>> messages = generateRandomMessages(3);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        createArchiveContext(),
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();

    MessageCapturingFragmentHandler handler;
    auto poller = [&]
    {
        return aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription,
            MessageCapturingFragmentHandler::onFragment,
            &handler,
            10);
    };

    executeUntil(
        "becomes live",
        poller,
        [&] { return aeron_archive_persistent_subscription_is_live(persistent_subscription); });
    ASSERT_EQ(messages, handler.messages());

    const std::vector<std::vector<uint8_t>> liveMessages = generateRandomMessages(3);
    persistent_publication.offer(liveMessages);

    handler.clear();
    executeUntil(
        "receives all live messages",
        poller,
        [&] { return handler.messageCount() >= liveMessages.size(); });
    ASSERT_EQ(liveMessages, handler.messages());

    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldStartFromLiveWithNoInitialReplayIfRequested)
{
    TestArchive archive = createArchive(m_aeronDir);

    const std::vector<std::vector<uint8_t>> messages = generateRandomMessages(3);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        createArchiveContext(),
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_start_position(
        context, AERON_PERSISTENT_SUBSCRIPTION_FROM_LIVE);

    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();

    MessageCapturingFragmentHandler handler;
    auto poller = [&]
    {
        return aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription,
            MessageCapturingFragmentHandler::onFragment,
            &handler,
            10);
    };

    executeUntil(
        "becomes live",
        poller,
        [&] { return aeron_archive_persistent_subscription_is_live(persistent_subscription); });
    ASSERT_EQ(0, handler.messageCount());

    const std::vector<std::vector<uint8_t>> liveMessages = generateRandomMessages(3);
    persistent_publication.offer(liveMessages);

    executeUntil(
        "receives all live messages",
        poller,
        [&] { return handler.messageCount() >= liveMessages.size(); });
    ASSERT_EQ(liveMessages, handler.messages());

    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldNotRequireEventListener)
{
    TestArchive archive = createArchive(m_aeronDir);

    AeronResource aeron(m_aeronDir);

    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        createArchiveContext(),
        13); // does not exist

    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();

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
        [&] { return aeron_archive_persistent_subscription_has_failed(persistent_subscription); });

    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldErrorIfRecordingDoesNotExist)
{
    TestArchive archive = createArchive(m_aeronDir);

    AeronResource aeron(m_aeronDir);

    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        createArchiveContext(),
        13); // does not exist

    ListenerState listener_state;
    aeron_archive_persistent_subscription_listener_t listener = {};
    listener.clientd = &listener_state;
    listener.on_error = [](void *clientd, int errcode, const char *message)
    {
        auto *state = static_cast<ListenerState *>(clientd);
        state->error_count++;
        state->last_errcode = errcode;
        state->last_error_message = message;
    };
    aeron_archive_persistent_subscription_context_set_listener(context, &listener);

    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();

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
        [&] { return aeron_archive_persistent_subscription_has_failed(persistent_subscription); });

    ASSERT_EQ(1, listener_state.error_count);
    ASSERT_NE(std::string::npos, listener_state.last_error_message.find("recording"));

    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldErrorIfRecordingStreamDoesNotMatchLiveStream)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);

    AeronResource aeron(m_aeronDir);

    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        createArchiveContext(),
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_live_stream_id(context, STREAM_ID + 1); // <-- mismatched

    ListenerState listener_state;
    aeron_archive_persistent_subscription_listener_t listener = {};
    listener.clientd = &listener_state;
    listener.on_error = [](void *clientd, int errcode, const char *message)
    {
        auto *state = static_cast<ListenerState *>(clientd);
        state->error_count++;
        state->last_errcode = errcode;
        state->last_error_message = message;
    };
    aeron_archive_persistent_subscription_context_set_listener(context, &listener);

    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();

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
        [&] { return aeron_archive_persistent_subscription_has_failed(persistent_subscription); });

    ASSERT_EQ(1, listener_state.error_count);
    ASSERT_NE(std::string::npos, listener_state.last_error_message.find("stream"));

    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldErrorIfRecordingPositionIsBeforeStartPosition)
{
    TestArchive archive = createArchive(m_aeronDir);

    const std::string channel = "aeron:ipc?init-term-id=0|term-id=0|term-offset=1024|term-length=65536";

    PersistentPublication persistent_publication(m_aeronDir, channel, STREAM_ID);

    AeronResource aeron(m_aeronDir);

    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        createArchiveContext(),
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_live_channel(context, channel.c_str());
    // start_position is already 0 from default, below recording start of 1024

    ListenerState listener_state;
    aeron_archive_persistent_subscription_listener_t listener = {};
    listener.clientd = &listener_state;
    listener.on_error = [](void *clientd, int errcode, const char *message)
    {
        auto *state = static_cast<ListenerState *>(clientd);
        state->error_count++;
        state->last_errcode = errcode;
        state->last_error_message = message;
    };
    aeron_archive_persistent_subscription_context_set_listener(context, &listener);

    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();

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
        [&] { return aeron_archive_persistent_subscription_has_failed(persistent_subscription); });

    ASSERT_EQ(1, listener_state.error_count);
    ASSERT_NE(std::string::npos, listener_state.last_error_message.find("position"));

    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldErrorIfRecordingPositionIsAfterStopPosition)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);

    const std::vector<uint8_t> message(1024, 0);
    persistent_publication.persist({{ message }});

    const int64_t stop_position = persistent_publication.stop();
    ASSERT_GT(stop_position, 0);

    AeronResource aeron(m_aeronDir);

    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        createArchiveContext(),
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_start_position(context, stop_position * 2); // <-- after end

    ListenerState listener_state;
    aeron_archive_persistent_subscription_listener_t listener = {};
    listener.clientd = &listener_state;
    listener.on_error = [](void *clientd, int errcode, const char *message)
    {
        auto *state = static_cast<ListenerState *>(clientd);
        state->error_count++;
        state->last_errcode = errcode;
        state->last_error_message = message;
    };
    aeron_archive_persistent_subscription_context_set_listener(context, &listener);

    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();

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
        [&] { return aeron_archive_persistent_subscription_has_failed(persistent_subscription); });

    ASSERT_EQ(1, listener_state.error_count);
    ASSERT_NE(std::string::npos, listener_state.last_error_message.find("position"));

    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
}

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

    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        createArchiveContext(),
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_start_position(
        context, AERON_PERSISTENT_SUBSCRIPTION_FROM_LIVE);
    aeron_archive_persistent_subscription_context_set_listener(context, &listener);

    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();

    MessageCapturingFragmentHandler handler;
    auto poller = [&]
    {
        return aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription,
            MessageCapturingFragmentHandler::onFragment,
            &handler,
            10);
    };

    ASSERT_EQ(0, live_joined_count);

    executeUntil(
        "becomes live",
        poller,
        [&] { return aeron_archive_persistent_subscription_is_live(persistent_subscription); });

    ASSERT_EQ(1, live_joined_count);
    ASSERT_EQ(0, handler.messageCount());

    const std::vector<std::vector<uint8_t>> new_messages = generateRandomMessages(3);
    persistent_publication.persist(new_messages);

    executeUntil(
        "receives all new messages",
        poller,
        [&] { return handler.messageCount() >= new_messages.size(); });

    ASSERT_EQ(new_messages, handler.messages());

    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldTransitionFromReplayToLiveWhileLiveIsAdvancing)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> messages = generateRandomMessages(5);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        createArchiveContext(),
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();

    MessageCapturingFragmentHandler handler;
    auto poller = [&]
    {
        return aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription,
            MessageCapturingFragmentHandler::onFragment,
            &handler,
            10);
    };

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
        [&] { return handler.messageCount() >= 1; });

    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    executeUntil(
        "receives all messages",
        poller,
        [&] { return handler.messageCount() >= messages.size(); });

    ASSERT_EQ(messages, handler.messages());

    const std::vector<std::vector<uint8_t>> messages2 = generateRandomMessages(1);
    persistent_publication.persist(messages2);

    executeUntil(
        "becomes live",
        poller,
        [&] { return aeron_archive_persistent_subscription_is_live(persistent_subscription); });

    ASSERT_FALSE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldStartFromLiveWhenThereIsNoDataToReplay)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    AeronResource aeron(m_aeronDir);

    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        createArchiveContext(),
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_live_channel(context, MDC_SUBSCRIPTION_CHANNEL.c_str());

    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();

    MessageCapturingFragmentHandler handler;
    auto poller = [&]
    {
        return aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription,
            MessageCapturingFragmentHandler::onFragment,
            &handler,
            10);
    };

    executeUntil(
        "becomes live",
        poller,
        [&] { return aeron_archive_persistent_subscription_is_live(persistent_subscription); });

    ASSERT_EQ(0, handler.messageCount());

    const std::vector<std::vector<uint8_t>> messages = generateRandomMessages(5);
    persistent_publication.persist(messages);

    executeUntil(
        "receives all messages",
        poller,
        [&] { return handler.messageCount() >= messages.size(); });

    ASSERT_EQ(messages, handler.messages());

    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldAssembleMessages)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, IPC_CHANNEL, STREAM_ID);

    const int32_t size_requiring_fragmentation = persistent_publication.maxPayloadLength() + 1;

    const std::vector<uint8_t> payload0 = generateRandomBytes(size_requiring_fragmentation);
    const std::vector<uint8_t> payload1 = generateRandomBytes(size_requiring_fragmentation);

    persistent_publication.persist({{ payload0 }});

    AeronResource aeron(m_aeronDir);

    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        createArchiveContext(),
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();

    MessageCapturingFragmentHandler handler;
    auto poller = [&]
    {
        return aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription,
            MessageCapturingFragmentHandler::onFragment,
            &handler,
            1);
    };

    executeUntil(
        "becomes live",
        poller,
        [&] { return aeron_archive_persistent_subscription_is_live(persistent_subscription); });

    persistent_publication.persist({{ payload1 }});

    executeUntil(
        "receives both messages",
        poller,
        [&] { return handler.messageCount() >= 2; });

    ASSERT_EQ((std::vector<std::vector<uint8_t>>{{ payload0, payload1 }}), handler.messages());

    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldReplayFromRecordingStartPositionWhenStartingFromStart)
{
    TestArchive archive = createArchive(m_aeronDir);

    const std::string channel = "aeron:ipc?init-term-id=0|term-id=0|term-offset=1024|term-length=65536";

    PersistentPublication persistent_publication(m_aeronDir, channel, STREAM_ID);

    const std::vector<std::vector<uint8_t>> old_messages = generateRandomMessages(5);
    persistent_publication.persist(old_messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        createArchiveContext(),
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_start_position(
        context, AERON_PERSISTENT_SUBSCRIPTION_FROM_START);

    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();

    MessageCapturingFragmentHandler handler;
    auto poller = [&]
    {
        return aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription,
            MessageCapturingFragmentHandler::onFragment,
            &handler,
            10);
    };

    executeUntil(
        "becomes live",
        poller,
        [&] { return aeron_archive_persistent_subscription_is_live(persistent_subscription); });

    const std::vector<std::vector<uint8_t>> new_messages = generateRandomMessages(3);
    persistent_publication.persist(new_messages);

    executeUntil(
        "receives all messages",
        poller,
        [&] { return handler.messageCount() >= old_messages.size() + new_messages.size(); });

    std::vector<std::vector<uint8_t>> all_messages;
    all_messages.insert(all_messages.end(), old_messages.begin(), old_messages.end());
    all_messages.insert(all_messages.end(), new_messages.begin(), new_messages.end());
    ASSERT_EQ(all_messages, handler.messages());

    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
}

TEST_F(AeronArchivePersistentSubscriptionTest, shouldDropFromLiveBackToReplayThenJoinLiveAgain)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, MDC_PUBLICATION_CHANNEL, STREAM_ID);

    const std::vector<std::vector<uint8_t>> payloads = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(payloads);

    AeronResource aeron(m_aeronDir);

    LiveEventListenerState listener_state;
    aeron_archive_persistent_subscription_listener_t listener = {};
    listener.clientd = &listener_state;
    listener.on_live_joined = [](void *clientd)
    {
        static_cast<LiveEventListenerState *>(clientd)->live_joined_count++;
    };
    listener.on_live_left = [](void *clientd)
    {
        static_cast<LiveEventListenerState *>(clientd)->live_left_count++;
    };

    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        createArchiveContext(),
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_live_channel(context, MDC_SUBSCRIPTION_CHANNEL.c_str());
    aeron_archive_persistent_subscription_context_set_listener(context, &listener);

    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();

    MessageCapturingFragmentHandler handler;
    auto poller = [&]
    {
        return aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription,
            MessageCapturingFragmentHandler::onFragment,
            &handler,
            10);
    };

    ASSERT_EQ(0, listener_state.live_joined_count);

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
        [&] { return handler.messageCount() >= 1; });

    ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));

    executeUntil(
        "receives all payloads",
        poller,
        [&] { return handler.messageCount() >= payloads.size(); });

    executeUntil(
        "becomes live",
        poller,
        [&] { return aeron_archive_persistent_subscription_is_live(persistent_subscription); });

    ASSERT_EQ(1, listener_state.live_joined_count);
    ASSERT_EQ(0, listener_state.live_left_count);
    ASSERT_EQ(payloads.size(), handler.messageCount());

    const std::vector<std::vector<uint8_t>> payloads2 = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
    persistent_publication.persist(payloads2);

    executeUntil(
        "receives payloads2",
        poller,
        [&] { return handler.messageCount() >= payloads.size() + payloads2.size(); });

    ASSERT_TRUE(aeron_archive_persistent_subscription_is_live(persistent_subscription));

    {
        AeronResource aeron2(m_aeronDir);

        aeron_subscription_t *fast_subscription = nullptr;
        aeron_async_add_subscription_t *async_add = nullptr;
        ASSERT_EQ(0, aeron_async_add_subscription(
            &async_add,
            aeron2.aeron(),
            MDC_SUBSCRIPTION_CHANNEL.c_str(),
            STREAM_ID,
            nullptr, nullptr, nullptr, nullptr)) << aeron_errmsg();

        while (fast_subscription == nullptr)
        {
            aeron_async_add_subscription_poll(&fast_subscription, async_add);
            std::this_thread::yield();
        }

        while (aeron_subscription_image_count(fast_subscription) == 0)
        {
            std::this_thread::yield();
        }

        const std::vector<std::vector<uint8_t>> payloads3 = generateFixedMessages(64, ONE_KB_MESSAGE_SIZE);
        persistent_publication.persist(payloads3);

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
            [&] { return fast_count >= 64; });

        executeUntil(
            "drops to replaying",
            poller,
            [&] { return aeron_archive_persistent_subscription_is_replaying(persistent_subscription); });

        ASSERT_TRUE(aeron_archive_persistent_subscription_is_replaying(persistent_subscription));
        ASSERT_EQ(1, listener_state.live_left_count);

        const std::vector<std::vector<uint8_t>> payloads4 = generateFixedMessages(5, ONE_KB_MESSAGE_SIZE);
        persistent_publication.persist(payloads4);

        const size_t expected_count =
            payloads.size() + payloads2.size() + payloads3.size() + payloads4.size();

        executeUntil(
            "receives all messages and becomes live",
            poller,
            [&]
            {
                return handler.messageCount() >= expected_count &&
                       aeron_archive_persistent_subscription_is_live(persistent_subscription);
            });

        ASSERT_EQ(2, listener_state.live_joined_count);

        std::vector<std::vector<uint8_t>> all_messages;
        all_messages.insert(all_messages.end(), payloads.begin(), payloads.end());
        all_messages.insert(all_messages.end(), payloads2.begin(), payloads2.end());
        all_messages.insert(all_messages.end(), payloads3.begin(), payloads3.end());
        all_messages.insert(all_messages.end(), payloads4.begin(), payloads4.end());
        ASSERT_EQ(all_messages, handler.messages());

        aeron_subscription_close(fast_subscription, nullptr, nullptr);
    }

    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
}

TEST_F(AeronArchivePersistentSubscriptionTest, anUntetheredPersistentSubscriptionCanFallBehindATetheredSubscription)
{
    TestArchive archive = createArchive(m_aeronDir);

    PersistentPublication persistent_publication(m_aeronDir, UNICAST_CHANNEL, STREAM_ID);

    AeronResource aeron(m_aeronDir);

    aeron_archive_persistent_subscription_context_t *context = createDefaultPersistentSubscriptionContext(
        aeron.aeron(),
        createArchiveContext(),
        persistent_publication.recordingId());

    aeron_archive_persistent_subscription_context_set_live_channel(
        context, (UNICAST_CHANNEL + "|tether=false").c_str());

    aeron_archive_persistent_subscription_t *persistent_subscription;
    ASSERT_EQ(0, aeron_archive_persistent_subscription_create(&persistent_subscription, context)) << aeron_errmsg();

    aeron_subscription_t *tethered_subscription = nullptr;
    aeron_async_add_subscription_t *async_add = nullptr;
    ASSERT_EQ(0, aeron_async_add_subscription(
        &async_add,
        aeron.aeron(),
        (UNICAST_CHANNEL + "|tether=true").c_str(),
        STREAM_ID,
        nullptr, nullptr, nullptr, nullptr)) << aeron_errmsg();

    while (tethered_subscription == nullptr)
    {
        aeron_async_add_subscription_poll(&tethered_subscription, async_add);
        std::this_thread::yield();
    }

    while (aeron_subscription_image_count(tethered_subscription) == 0)
    {
        std::this_thread::yield();
    }

    MessageCapturingFragmentHandler handler;
    auto poller = [&]
    {
        return aeron_archive_persistent_subscription_controlled_poll(
            persistent_subscription,
            MessageCapturingFragmentHandler::onFragment,
            &handler,
            10);
    };

    executeUntil(
        "becomes live",
        poller,
        [&] { return aeron_archive_persistent_subscription_is_live(persistent_subscription); });

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
        [&] { return fast_count >= 64; });

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
        [&] { return handler.messageCount() >= 64; });

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
        [&] { return aeron_archive_persistent_subscription_is_live(persistent_subscription); });

    aeron_subscription_close(tethered_subscription, nullptr, nullptr);

    ASSERT_EQ(0, aeron_archive_persistent_subscription_close(persistent_subscription)) << aeron_errmsg();
}