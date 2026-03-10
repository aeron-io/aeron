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

#include <random>
#include <utility>

#include "gtest/gtest.h"
#include "../TestArchive.h"
#include "ArchiveClientTestUtils.h"

extern "C"
{
#include "aeron_common.h"
#include "client/aeron_archive.h"
#include "client/aeron_archive_persistent_subscription.h"
}

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

    const std::string liveChannel = "aeron:ipc";
    const int32_t liveStreamId = 1000;

    PersistentPublication persistent_publication(m_aeronDir, liveChannel, liveStreamId);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        createArchiveContext(),
        persistent_publication.recordingId(),
        liveChannel,
        liveStreamId,
        "aeron:ipc",
        2000,
        0);

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

    const std::string liveChannel = "aeron:ipc";
    const int32_t liveStreamId = 1000;

    PersistentPublication persistent_publication(m_aeronDir, liveChannel, liveStreamId);
    persistent_publication.persist(messages);

    AeronResource aeron(m_aeronDir);

    aeron_archive_persistent_subscription_context_t *context = createPersistentSubscriptionContext(
        aeron.aeron(),
        createArchiveContext(),
        persistent_publication.recordingId(),
        liveChannel,
        liveStreamId,
        "aeron:ipc",
        2000,
        AERON_PERSISTENT_SUBSCRIPTION_FROM_LIVE);

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
