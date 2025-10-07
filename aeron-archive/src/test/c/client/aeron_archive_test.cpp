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

#include "gtest/gtest.h"
#include <gmock/gmock-matchers.h>

#ifdef _MSC_VER
#define AERON_FILE_SEP '\\'
#else
#define AERON_FILE_SEP '/'
#endif

extern "C"
{
#include <inttypes.h>
#include "aeronc.h"
#include "aeron_agent.h"
#include "aeron_client.h"
#include "aeron_counter.h"
#include "aeron_counters.h"
#include "client/aeron_archive.h"
#include "client/aeron_archive_client.h"
#include "client/aeron_archive_client_version.h"
#include "client/aeron_archive_context.h"
#include "uri/aeron_uri_string_builder.h"
#include "util/aeron_env.h"
}

#include "../TestArchive.h"
#include "aeron_archive_client/controlResponse.h"

testing::AssertionResult EqualOrErrmsg(const int x, const int y)
{
    if (x == y)
    {
        return testing::AssertionSuccess();
    }
    else
    {
        return testing::AssertionFailure() << aeron_errmsg();
    }
}

#define ASSERT_EQ_ERR(__x, __y) ASSERT_TRUE(EqualOrErrmsg((__x), (__y)))

typedef struct fragment_handler_clientd_stct
{
    size_t received;
    int64_t position;
}
fragment_handler_clientd_t;

void fragment_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_stct *header)
{
    auto *cd = (fragment_handler_clientd_t *)clientd;
    cd->received++;
    cd->position = aeron_header_position(header);
}

typedef struct credentials_supplier_clientd_stct
{
    aeron_archive_encoded_credentials_t *credentials;
    aeron_archive_encoded_credentials_t *on_challenge_credentials;
}
credentials_supplier_clientd_t;

aeron_archive_encoded_credentials_t default_creds = { "admin:admin", 11 };
aeron_archive_encoded_credentials_t bad_creds = { "admin:NotAdmin", 14 };

credentials_supplier_clientd_t default_creds_clientd = { &default_creds, nullptr };

static aeron_archive_encoded_credentials_t *encoded_credentials_supplier(void *clientd)
{
    return ((credentials_supplier_clientd_t *)clientd)->credentials;
}

static aeron_archive_encoded_credentials_t *encoded_credentials_on_challenge(
    aeron_archive_encoded_credentials_t *encoded_challenge, void *clientd)
{
    return ((credentials_supplier_clientd_t *)clientd)->on_challenge_credentials;
}

typedef struct recording_signal_consumer_clientd_stct
{
    std::set<std::int32_t> signals;
}
recording_signal_consumer_clientd_t;

void recording_signal_consumer(
    aeron_archive_recording_signal_t *signal,
    void *clientd)
{
    auto cd = (recording_signal_consumer_clientd_t *)clientd;
    cd->signals.insert(signal->recording_signal_code);
}

class AeronCArchiveTestBase
{
public:
    ~AeronCArchiveTestBase()
    {
        if (m_debug)
        {
            std::cout << m_stream.str();
        }
    }

    void DoSetUp(std::int64_t archiveId = 42, std::int32_t maxConcurrentReplays = 100)
    {
        char aeron_dir[AERON_MAX_PATH];
        aeron_default_path(aeron_dir, sizeof(aeron_dir));

        std::string sourceArchiveDir = m_archiveDir + AERON_FILE_SEP + "source";
        m_testArchive = std::make_shared<TestArchive>(
            aeron_dir,
            sourceArchiveDir,
            std::cout,
            "aeron:udp?endpoint=localhost:8010",
            "aeron:udp?endpoint=localhost:0",
            archiveId,
            maxConcurrentReplays);
    }

    void DoTearDown()
    {
        if (nullptr != m_archive)
        {
            ASSERT_EQ_ERR(0, aeron_archive_close(m_archive));
        }
        if (nullptr != m_ctx)
        {
            ASSERT_EQ_ERR(0, aeron_archive_context_close(m_ctx));
        }
        if (nullptr != m_dest_archive)
        {
            ASSERT_EQ_ERR(0, aeron_archive_close(m_dest_archive));
        }
        if (nullptr != m_dest_ctx)
        {
            ASSERT_EQ_ERR(0, aeron_archive_context_close(m_dest_ctx));
        }
    }

    void idle()
    {
        aeron_idle_strategy_sleeping_idle((void *)&m_idle_duration_ns, 0);

        if (m_invoke_on_idle)
        {
            aeron_main_do_work(m_aeron);
        }
    }

    void connect(
        void *recording_signal_consumer_clientd = nullptr,
        const char *request_channel = "aeron:udp?endpoint=localhost:8010",
        const char *response_channel = "aeron:udp?endpoint=localhost:0",
        const char *client_name = "")
    {
        ASSERT_EQ_ERR(0, aeron_archive_context_init(&m_ctx));
        ASSERT_EQ_ERR(0, aeron_archive_context_set_client_name(m_ctx, client_name));
        ASSERT_EQ_ERR(0, aeron_archive_context_set_control_request_channel(m_ctx, request_channel));
        ASSERT_EQ_ERR(0, aeron_archive_context_set_control_response_channel(m_ctx, response_channel));
        ASSERT_EQ_ERR(0, aeron_archive_context_set_idle_strategy(m_ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
        ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
            m_ctx,
            encoded_credentials_supplier,
            nullptr,
            nullptr,
            &default_creds_clientd));

        if (nullptr != recording_signal_consumer_clientd)
        {
            ASSERT_EQ_ERR(0, aeron_archive_context_set_recording_signal_consumer(
                m_ctx,
                recording_signal_consumer,
                recording_signal_consumer_clientd));
        }

        ASSERT_EQ_ERR(0, aeron_archive_connect(&m_archive, m_ctx));

        m_aeron = aeron_archive_context_get_aeron(m_ctx);
    }

    aeron_subscription_t *addSubscription(std::string channel, int32_t stream_id)
    {
        aeron_async_add_subscription_t *async_add_subscription;
        aeron_subscription_t *subscription = nullptr;

        if (aeron_async_add_subscription(
            &async_add_subscription,
            m_aeron,
            channel.c_str(),
            stream_id,
            nullptr,
            nullptr,
            nullptr,
            nullptr) < 0)
        {
            fprintf(stderr, " -- GOT AN ERROR :: %s\n", aeron_errmsg());
        }

        if (aeron_async_add_subscription_poll(&subscription, async_add_subscription) < 0)
        {
            fprintf(stderr, " -- GOT AN ERROR :: %s\n", aeron_errmsg());
        }
        while (nullptr == subscription)
        {
            idle();
            if (aeron_async_add_subscription_poll(&subscription, async_add_subscription) < 0)
            {
                fprintf(stderr, " -- GOT AN ERROR :: %s\n", aeron_errmsg());
            }
        }

        return subscription;
    }

    aeron_publication_t *addPublication(std::string channel, int32_t stream_id)
    {
        aeron_async_add_publication_t *async_add_publication;
        aeron_publication_t *publication = nullptr;

        if (aeron_async_add_publication(
            &async_add_publication,
            m_aeron,
            channel.c_str(),
            stream_id) < 0)
        {
            fprintf(stderr, " -- GOT AN ERROR :: %s\n", aeron_errmsg());
        }

        if (aeron_async_add_publication_poll(&publication, async_add_publication) < 0)
        {
            fprintf(stderr, " -- GOT AN ERROR :: %s\n", aeron_errmsg());
        }
        while (nullptr == publication)
        {
            idle();
            if (aeron_async_add_publication_poll(&publication, async_add_publication) < 0)
            {
                fprintf(stderr, " -- GOT AN ERROR :: %s\n", aeron_errmsg());
            }
        }

        return publication;
    }

    void setupCounters(int32_t session_id)
    {
        m_counters_reader = aeron_counters_reader(m_aeron);
        m_counter_id = getRecordingCounterId(session_id, m_counters_reader);
        m_recording_id_from_counter = aeron_archive_recording_pos_get_recording_id(m_counters_reader, m_counter_id);
    }

    void waitUntilCaughtUp(int64_t position)
    {
        while (*aeron_counters_reader_addr(m_counters_reader, m_counter_id) < position)
        {
            idle();
        }
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

    static void offerMessages(
        aeron_publication_t *publication,
        size_t message_count = 10,
        size_t start_count = 0,
        const char *message_prefix = "Message ")
    {
        for (size_t i = 0; i < message_count; i++)
        {
            size_t index = i + start_count;
            char message[1000];
            size_t len = snprintf(message, 1000, "%s%zu", message_prefix, index);

            while (aeron_publication_offer(publication, (uint8_t *)message, len, nullptr, nullptr) < 0)
            {
                aeron_idle_strategy_yielding_idle(nullptr, 0);
            }
        }
    }

    static void offerMessagesToPosition(
        aeron_publication_t *publication,
        int64_t minimum_position,
        const char *message_prefix = "Message ")
    {
        for (size_t i = 0; aeron_publication_position(publication) < minimum_position; i++)
        {
            char message[1000];
            size_t len = snprintf(message, 1000, "%s%zu", message_prefix, i);

            while (aeron_publication_offer(publication, (uint8_t *)message, len, nullptr, nullptr) < 0)
            {
                aeron_idle_strategy_yielding_idle(nullptr, 0);
            }
        }
    }

    static void offerMessages(
        aeron_exclusive_publication_t *exclusive_publication,
        size_t message_count = 10,
        size_t start_count = 0,
        const char *message_prefix = "Message ")
    {
        for (size_t i = 0; i < message_count; i++)
        {
            size_t index = i + start_count;
            char message[1000];
            size_t len = snprintf(message, 1000, "%s%zu", message_prefix, index);

            while (aeron_exclusive_publication_offer(exclusive_publication, (uint8_t *)message, len, nullptr, nullptr) < 0)
            {
                aeron_idle_strategy_yielding_idle(nullptr, 0);
            }
        }
    }

    static void consumeMessages(
        aeron_subscription_t *subscription,
        size_t message_count = 10)
    {
        fragment_handler_clientd_t clientd;
        clientd.received = 0;

        while (clientd.received < message_count)
        {
            if (0 == aeron_subscription_poll(subscription, fragment_handler, (void *)&clientd, 10))
            {
                aeron_idle_strategy_yielding_idle(nullptr, 0);
            }
        }

        ASSERT_EQ(clientd.received, message_count);
    }

    static int64_t consumeMessagesExpectingBound(
        aeron_subscription_t *subscription,
        int64_t bound_position,
        int64_t timeout_ms)
    {
        fragment_handler_clientd_t clientd;
        clientd.received = 0;
        clientd.position = 0;

        int64_t deadline_ms = aeron_epoch_clock() + timeout_ms;

        while (aeron_epoch_clock() < deadline_ms)
        {
            if (0 == aeron_subscription_poll(subscription, fragment_handler, (void *)&clientd, 10))
            {
                aeron_idle_strategy_yielding_idle(nullptr, 0);
            }
        }

        return clientd.position;
    }

    bool attemptReplayMerge(
        aeron_archive_replay_merge_t *replay_merge,
        aeron_publication_t *publication,
        aeron_fragment_handler_t handler,
        void *clientd,
        size_t total_message_count,
        size_t *messages_published,
        size_t *received_message_count,
        const char *message_prefix = "Message ")
    {
        for (size_t i = *messages_published; i < total_message_count; i++)
        {
            char message[1000];
            size_t len = snprintf(message, 1000, "%s%zu", message_prefix, i);

            while (aeron_publication_offer(publication, (uint8_t *)message, len, nullptr, nullptr) < 0)
            {
                idle();

                int fragments = aeron_archive_replay_merge_poll(replay_merge, handler, clientd, m_fragment_limit);

                if (0 == fragments && aeron_archive_replay_merge_has_failed(replay_merge))
                {
                    return false;
                }
            }

            (*messages_published)++;
        }

        while (!aeron_archive_replay_merge_is_merged(replay_merge))
        {
            int fragments = aeron_archive_replay_merge_poll(replay_merge, handler, clientd, m_fragment_limit);

            if (0 == fragments && aeron_archive_replay_merge_has_failed(replay_merge))
            {
                return false;
            }

            idle();
        }

        aeron_image_t *image = aeron_archive_replay_merge_image(replay_merge);
        while (*received_message_count < total_message_count)
        {
            int fragments = aeron_image_poll(image, handler, clientd, m_fragment_limit);

            if (0 == fragments && aeron_image_is_closed(image))
            {
                return false;
            }

            idle();
        }

        return true;
    }

    void startDestArchive()
    {
        char aeron_dir[AERON_MAX_PATH];
        aeron_default_path(aeron_dir, sizeof(aeron_dir));
        std::string dest_aeron_dir = std::string(aeron_dir) + "_dest";

        const std::string archiveDir = m_archiveDir + AERON_FILE_SEP + "dest";
        const std::string controlChannel = "aeron:udp?endpoint=localhost:8011";
        const std::string replicationChannel = "aeron:udp?endpoint=localhost:8012";

        m_destTestArchive = std::make_shared<TestArchive>(
            dest_aeron_dir, archiveDir, m_stream, controlChannel, replicationChannel, -7777);

        ASSERT_EQ_ERR(0, aeron_archive_context_init(&m_dest_ctx));
        ASSERT_EQ_ERR(0, aeron_archive_context_set_control_request_channel(m_dest_ctx, controlChannel.c_str()));
        ASSERT_EQ_ERR(0, aeron_archive_context_set_control_response_channel(m_dest_ctx, "aeron:udp?endpoint=localhost:0"));
        ASSERT_EQ_ERR(0, aeron_archive_context_set_idle_strategy(
            m_dest_ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
        ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
            m_dest_ctx,
            encoded_credentials_supplier,
            nullptr,
            nullptr,
            &default_creds_clientd));
        ASSERT_EQ_ERR(0, aeron_archive_context_set_control_request_channel(m_dest_ctx, controlChannel.c_str()));
    }

    void recordData(
        bool tryStop,
        int64_t *recording_id,
        int64_t *stop_position,
        int64_t *halfway_position,
        size_t message_count = 1000)
    {
        int64_t subscription_id;
        ASSERT_EQ_ERR(0, aeron_archive_start_recording(
            &subscription_id,
            m_archive,
            m_recordingChannel.c_str(),
            m_recordingStreamId,
            AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
            false));

        aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(m_recordingChannel, m_recordingStreamId);

        int32_t session_id = aeron_publication_session_id(publication);

        setupCounters(session_id);
        *recording_id = m_recording_id_from_counter;

        bool is_active;
        ASSERT_EQ_ERR(0, aeron_archive_recording_pos_is_active(
            &is_active,
            m_counters_reader,
            m_counter_id,
            m_recording_id_from_counter));
        EXPECT_TRUE(is_active);

        EXPECT_EQ(m_counter_id, aeron_archive_recording_pos_find_counter_id_by_recording_id(
            m_counters_reader,
            m_recording_id_from_counter));

        {
            size_t sib_len = AERON_COUNTER_MAX_LABEL_LENGTH;
            const char source_identity_buffer[AERON_COUNTER_MAX_LABEL_LENGTH] = { '\0' };

            ASSERT_EQ_ERR(0,
                aeron_archive_recording_pos_get_source_identity(
                    m_counters_reader,
                    m_counter_id,
                    source_identity_buffer,
                    &sib_len));
            EXPECT_EQ(9, sib_len);
            EXPECT_STREQ("aeron:ipc", source_identity_buffer);
        }

        int64_t half_count = message_count / 2;

        offerMessages(publication, half_count);
        *halfway_position = aeron_publication_position(publication);
        offerMessages(publication, half_count, half_count);
        consumeMessages(subscription, message_count);

        *stop_position = aeron_publication_position(publication);

        waitUntilCaughtUp(*stop_position);

        if (tryStop)
        {
            bool stopped;
            ASSERT_EQ_ERR(0, aeron_archive_try_stop_recording_subscription(
                &stopped,
                m_archive,
                subscription_id));
            EXPECT_TRUE(stopped);
        }
        else
        {
            ASSERT_EQ_ERR(0, aeron_archive_stop_recording_subscription(
                m_archive,
                subscription_id));
        }
    }

protected:
    const std::string m_archiveDir = ARCHIVE_DIR;

    const std::string m_recordingChannel = "aeron:udp?endpoint=localhost:3333";
    const std::int32_t m_recordingStreamId = 33;
    const std::string m_replayChannel = "aeron:udp?endpoint=localhost:6666";
    const std::int32_t m_replayStreamId = 66;

    const int m_fragment_limit = 10;

    aeron_counters_reader_t *m_counters_reader;
    std::int32_t m_counter_id;
    std::int64_t m_recording_id_from_counter;

    std::ostringstream m_stream;

    std::shared_ptr<TestArchive> m_testArchive;
    std::shared_ptr<TestArchive> m_destTestArchive;

    bool m_debug = true;

    const uint64_t m_idle_duration_ns = UINT64_C(1000) * UINT64_C(1000); /* 1ms */

    aeron_archive_context_t *m_ctx = nullptr;
    aeron_archive_t *m_archive = nullptr;
    aeron_t *m_aeron = nullptr;

    aeron_archive_context_t *m_dest_ctx = nullptr;
    aeron_archive_t *m_dest_archive = nullptr;

    bool m_invoke_on_idle = false;
};

class AeronCArchiveTest : public AeronCArchiveTestBase, public testing::Test
{
public:
    void SetUp() final
    {
        DoSetUp();
    }

    void TearDown() final
    {
        DoTearDown();
    }
};

class AeronCArchiveParamTest : public AeronCArchiveTestBase, public testing::TestWithParam<bool>
{
public:
    void SetUp() final
    {
        DoSetUp();
    }

    void TearDown() final
    {
        DoTearDown();
    }
};

INSTANTIATE_TEST_SUITE_P(AeronCArchive, AeronCArchiveParamTest, testing::Values(true, false));

class AeronCArchiveIdTest : public AeronCArchiveTestBase, public testing::Test
{
};

typedef struct recording_descriptor_consumer_clientd_stct
{
    bool verify_recording_id = false;
    int64_t recording_id;
    bool verify_stream_id = false;
    int32_t stream_id;
    bool verify_start_equals_stop_position = false;
    bool verify_session_id = false;
    int32_t session_id;
    const char *original_channel = nullptr;
    std::set<std::int32_t> session_ids;
    aeron_archive_recording_descriptor_t last_descriptor;
    char last_descriptor_stripped_channel[AERON_URI_MAX_LENGTH];
    char last_descriptor_source_identity[512];
}
recording_descriptor_consumer_clientd_t;

static void recording_descriptor_consumer(
    aeron_archive_recording_descriptor_t *descriptor,
    void *clientd)
{
    auto *cd = (recording_descriptor_consumer_clientd_t *)clientd;

    if (cd->verify_recording_id)
    {
        EXPECT_EQ(cd->recording_id, descriptor->recording_id);
    }
    if (cd->verify_stream_id)
    {
        EXPECT_EQ(cd->stream_id, descriptor->stream_id);
    }
    if (cd->verify_start_equals_stop_position)
    {
        EXPECT_EQ(descriptor->start_position, descriptor->stop_position);
    }
    if (cd->verify_session_id)
    {
        EXPECT_EQ(cd->session_id, descriptor->session_id);
    }
    if (nullptr != cd->original_channel)
    {
        EXPECT_EQ(strlen(cd->original_channel), descriptor->original_channel_length);
        EXPECT_STREQ(cd->original_channel, descriptor->original_channel);
    }

    cd->session_ids.insert(descriptor->session_id);

    memcpy(&cd->last_descriptor, descriptor, sizeof(aeron_archive_recording_descriptor_t));
    strcpy(cd->last_descriptor_stripped_channel, descriptor->stripped_channel);
    strcpy(cd->last_descriptor_source_identity, descriptor->source_identity);
}

struct SubscriptionDescriptor
{
    const std::int64_t m_controlSessionId;
    const std::int64_t m_correlationId;
    const std::int64_t m_subscriptionId;
    const std::int32_t m_streamId;

    SubscriptionDescriptor(
        std::int64_t controlSessionId,
        std::int64_t correlationId,
        std::int64_t subscriptionId,
        std::int32_t streamId) :
        m_controlSessionId(controlSessionId),
        m_correlationId(correlationId),
        m_subscriptionId(subscriptionId),
        m_streamId(streamId)
    {
    }
};

struct subscription_descriptor_consumer_clientd
{
    std::vector<SubscriptionDescriptor> descriptors;
};

static void recording_subscription_descriptor_consumer(
    aeron_archive_recording_subscription_descriptor_t *descriptor,
    void *clientd)
{
    auto cd = (subscription_descriptor_consumer_clientd *)clientd;
    cd->descriptors.emplace_back(
        descriptor->control_session_id,
        descriptor->correlation_id,
        descriptor->subscription_id,
        descriptor->stream_id);
}

TEST_F(AeronCArchiveTest, shouldAsyncConnectToArchive)
{
    aeron_archive_context_t *ctx;
    aeron_archive_async_connect_t *async;
    aeron_archive_t *archive = nullptr;

    ASSERT_EQ_ERR(0, aeron_archive_context_init(&ctx));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_request_channel(ctx, "aeron:udp?endpoint=localhost:8010"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_response_channel(ctx, "aeron:udp?endpoint=localhost:0"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_idle_strategy(
        ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
        ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds_clientd));
    ASSERT_EQ_ERR(0, aeron_archive_async_connect(&async, ctx));

    // the ctx passed into async_connect gets duplicated, so it should be safe to delete it now
    ASSERT_EQ_ERR(0, aeron_archive_context_close(ctx));

    ASSERT_EQ_ERR(0, aeron_archive_async_connect_poll(&archive, async));

    while (nullptr == archive)
    {
        idle();

        ASSERT_NE(-1, aeron_archive_async_connect_poll(&archive, async));
    }

    ctx = aeron_archive_get_archive_context(archive);
    ASSERT_TRUE(aeron_archive_context_get_owns_aeron_client(ctx));

    aeron_subscription_t *subscription = aeron_archive_get_control_response_subscription(archive);
    ASSERT_TRUE(aeron_subscription_is_connected(subscription));

    ASSERT_EQ(42, aeron_archive_get_archive_id(archive));

    ASSERT_EQ_ERR(0, aeron_archive_close(archive));
}

TEST_F(AeronCArchiveTest, shouldAsyncConnectToArchiveWithPrebuiltAeron)
{

    aeron_archive_context_t *ctx;
    aeron_archive_async_connect_t *async;
    aeron_archive_t *archive = nullptr;

    aeron_context_t *aeron_ctx;
    aeron_t *aeron;

    ASSERT_EQ_ERR(0, aeron_archive_context_init(&ctx));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_request_channel(ctx, "aeron:udp?endpoint=localhost:8010"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_response_channel(ctx, "aeron:udp?endpoint=localhost:0"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_idle_strategy(
        ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
        ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds_clientd));

    ASSERT_EQ_ERR(0, aeron_context_init(&aeron_ctx));
    ASSERT_EQ_ERR(0, aeron_context_set_dir(aeron_ctx, aeron_archive_context_get_aeron_directory_name(ctx)));
    ASSERT_EQ_ERR(0, aeron_init(&aeron, aeron_ctx));
    ASSERT_EQ_ERR(0, aeron_start(aeron));

    ASSERT_EQ_ERR(0, aeron_archive_context_set_aeron(ctx, aeron));
    ASSERT_EQ_ERR(0, aeron_archive_async_connect(&async, ctx));

    // the ctx passed into async_connect gets duplicated, so it should be safe to delete it now
    ASSERT_EQ_ERR(0, aeron_archive_context_close(ctx));

    ASSERT_EQ_ERR(0, aeron_archive_async_connect_poll(&archive, async));

    while (nullptr == archive)
    {
        idle();

        ASSERT_NE(-1, aeron_archive_async_connect_poll(&archive, async));
    }

    ctx = aeron_archive_get_archive_context(archive);
    ASSERT_FALSE(aeron_archive_context_get_owns_aeron_client(ctx));

    aeron_subscription_t *subscription = aeron_archive_get_control_response_subscription(archive);
    ASSERT_TRUE(aeron_subscription_is_connected(subscription));

    ASSERT_EQ(42, aeron_archive_get_archive_id(archive));

    ASSERT_EQ_ERR(0, aeron_archive_close(archive));

    ASSERT_EQ_ERR(0, aeron_close(aeron));
    ASSERT_EQ_ERR(0, aeron_context_close(aeron_ctx));
}

TEST_F(AeronCArchiveTest, shouldConnectToArchive)
{
    aeron_archive_context_t *ctx;
    aeron_archive_t *archive = nullptr;

    ASSERT_EQ_ERR(0, aeron_archive_context_init(&ctx));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_request_channel(ctx, "aeron:udp?endpoint=localhost:8010"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_response_channel(ctx, "aeron:udp?endpoint=localhost:0"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_idle_strategy(
        ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
        ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds_clientd));
    ASSERT_EQ_ERR(0, aeron_archive_connect(&archive, ctx));
    ASSERT_EQ_ERR(0, aeron_archive_context_close(ctx));

    ctx = aeron_archive_get_archive_context(archive);
    ASSERT_TRUE(aeron_archive_context_get_owns_aeron_client(ctx));

    aeron_subscription_t *subscription = aeron_archive_get_control_response_subscription(archive);
    ASSERT_TRUE(aeron_subscription_is_connected(subscription));

    ASSERT_EQ(42, aeron_archive_get_archive_id(archive));

    ASSERT_EQ_ERR(0, aeron_archive_close(archive));
}

TEST_F(AeronCArchiveTest, shouldConnectToArchiveWithPrebuiltAeron)
{
    aeron_archive_context_t *ctx;
    aeron_archive_t *archive = nullptr;

    aeron_context_t *aeron_ctx;
    aeron_t *aeron;

    ASSERT_EQ_ERR(0, aeron_archive_context_init(&ctx));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_request_channel(ctx, "aeron:udp?endpoint=localhost:8010"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_response_channel(ctx, "aeron:udp?endpoint=localhost:0"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_idle_strategy(
        ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
        ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds_clientd));

    ASSERT_EQ_ERR(0, aeron_context_init(&aeron_ctx));
    ASSERT_EQ_ERR(0, aeron_context_set_dir(aeron_ctx, aeron_archive_context_get_aeron_directory_name(ctx)));
    ASSERT_EQ_ERR(0, aeron_init(&aeron, aeron_ctx));
    ASSERT_EQ_ERR(0, aeron_start(aeron));

    ASSERT_EQ_ERR(0, aeron_archive_context_set_aeron(ctx, aeron));
    ASSERT_EQ_ERR(0, aeron_archive_connect(&archive, ctx));
    ASSERT_EQ_ERR(0, aeron_archive_context_close(ctx));

    ctx = aeron_archive_get_archive_context(archive);
    ASSERT_FALSE(aeron_archive_context_get_owns_aeron_client(ctx));

    aeron_subscription_t *subscription = aeron_archive_get_control_response_subscription(archive);
    ASSERT_TRUE(aeron_subscription_is_connected(subscription));

    ASSERT_EQ(42, aeron_archive_get_archive_id(archive));

    ASSERT_EQ_ERR(0, aeron_archive_close(archive));

    ASSERT_EQ_ERR(0, aeron_close(aeron));
    ASSERT_EQ_ERR(0, aeron_context_close(aeron_ctx));
}

void invoker_func(void *clientd)
{
    *(bool *)clientd = true;
}

TEST_F(AeronCArchiveTest, shouldConnectToArchiveAndCallInvoker)
{
    aeron_archive_context_t *ctx;
    aeron_archive_t *archive = nullptr;

    ASSERT_EQ_ERR(0, aeron_archive_context_init(&ctx));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_request_channel(ctx, "aeron:udp?endpoint=localhost:8010"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_response_channel(ctx, "aeron:udp?endpoint=localhost:0"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_idle_strategy(
        ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
        ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds_clientd));
    bool invokerCalled = false;
    ASSERT_EQ_ERR(0, aeron_archive_context_set_delegating_invoker(
        ctx,
        invoker_func,
        &invokerCalled));
    ASSERT_EQ_ERR(0, aeron_archive_connect(&archive, ctx));
    ASSERT_TRUE(invokerCalled);
    ASSERT_EQ_ERR(0, aeron_archive_context_close(ctx));

    ctx = aeron_archive_get_archive_context(archive);
    ASSERT_TRUE(aeron_archive_context_get_owns_aeron_client(ctx));

    aeron_subscription_t *subscription = aeron_archive_get_control_response_subscription(archive);
    ASSERT_TRUE(aeron_subscription_is_connected(subscription));

    ASSERT_EQ(42, aeron_archive_get_archive_id(archive));

    ASSERT_EQ_ERR(0, aeron_archive_close(archive));
}

TEST_F(AeronCArchiveTest, shouldConnectFromTwoClientsUsingIpc)
{
    aeron_archive_context_t *ctx1, *ctx2;
    aeron_archive_t *archive1 = nullptr, *archive2 = nullptr;

    ASSERT_EQ_ERR(0, aeron_archive_context_init(&ctx1));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_request_channel(ctx1, "aeron:ipc"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_response_channel(ctx1, "aeron:ipc"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_idle_strategy(
        ctx1, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
        ctx1,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds_clientd));
    ASSERT_EQ_ERR(0, aeron_archive_connect(&archive1, ctx1));
    ASSERT_EQ_ERR(0, aeron_archive_context_close(ctx1));

    ASSERT_EQ_ERR(0, aeron_archive_context_init(&ctx2));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_request_channel(ctx2, "aeron:ipc"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_response_channel(ctx2, "aeron:ipc"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_idle_strategy(
        ctx2, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
        ctx2,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds_clientd));
    ASSERT_EQ_ERR(0, aeron_archive_connect(&archive2, ctx2));
    ASSERT_EQ_ERR(0, aeron_archive_context_close(ctx2));

    ASSERT_EQ(42, aeron_archive_get_archive_id(archive1));
    ASSERT_EQ(42, aeron_archive_get_archive_id(archive2));
    ctx1 = aeron_archive_get_archive_context(archive1);
    ctx2 = aeron_archive_get_archive_context(archive2);
    const auto *requestChannel1 = aeron_archive_context_get_control_request_channel(ctx1);
    aeron_uri_t reqChannel1;
    ASSERT_EQ(0, aeron_uri_parse(strlen(requestChannel1), requestChannel1, &reqChannel1));
    const auto *responseChannel1 = aeron_archive_context_get_control_response_channel(ctx1);
    aeron_uri_t respChannel1;
    ASSERT_EQ(0, aeron_uri_parse(strlen(responseChannel1), responseChannel1, &respChannel1));
    const char *sessionId1 = aeron_uri_find_param_value(&reqChannel1.params.ipc.additional_params, AERON_URI_SESSION_ID_KEY);
    ASSERT_STREQ(sessionId1, aeron_uri_find_param_value(&respChannel1.params.ipc.additional_params, AERON_URI_SESSION_ID_KEY));
    aeron_uri_close(&reqChannel1);
    aeron_uri_close(&respChannel1);

    const auto *requestChannel2 = aeron_archive_context_get_control_request_channel(ctx2);
    aeron_uri_t reqChannel2;
    ASSERT_EQ(0, aeron_uri_parse(strlen(requestChannel2), requestChannel2, &reqChannel2));
    const auto *responseChannel2 = aeron_archive_context_get_control_response_channel(ctx2);
    aeron_uri_t respChannel2;
    ASSERT_EQ(0, aeron_uri_parse(strlen(responseChannel2), responseChannel2, &respChannel2));
    const char *sessionId2 = aeron_uri_find_param_value(&reqChannel2.params.ipc.additional_params, AERON_URI_SESSION_ID_KEY);
    ASSERT_STREQ(sessionId2, aeron_uri_find_param_value(&respChannel2.params.ipc.additional_params, AERON_URI_SESSION_ID_KEY));
    aeron_uri_close(&reqChannel2);
    aeron_uri_close(&respChannel2);

    ASSERT_STRNE(sessionId1, sessionId2);

    ASSERT_EQ_ERR(0, aeron_archive_close(archive1));
    ASSERT_EQ_ERR(0, aeron_archive_close(archive2));
}

TEST_F(AeronCArchiveTest, shouldObserveErrorOnBadDataOnControlResponseChannel)
{
    aeron_archive_context_t *ctx;
    aeron_archive_t *archive = nullptr;

    ASSERT_EQ_ERR(0, aeron_archive_context_init(&ctx));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_request_channel(ctx, "aeron:udp?endpoint=localhost:8010"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_response_channel(ctx, "aeron:udp?endpoint=localhost:0"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_idle_strategy(
        ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
        ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds_clientd));
    ASSERT_EQ_ERR(0, aeron_archive_connect(&archive, ctx));

    EXPECT_FALSE(aeron_archive_context_get_owns_aeron_client(ctx));

    aeron_subscription_t *subscription = aeron_archive_get_control_response_subscription(archive);
    EXPECT_TRUE(aeron_subscription_is_connected(subscription));

    int64_t found_start_position;
    EXPECT_EQ(-1, aeron_archive_get_start_position(&found_start_position, archive, INT64_MAX));
    EXPECT_NE(
        std::string::npos,
        std::string(aeron_errmsg()).find("errorCode=5, error: unknown recording id: 9223372036854775807"));

    EXPECT_EQ(0, aeron_archive_close(archive));
    EXPECT_EQ(0, aeron_archive_context_close(ctx));
}

typedef struct error_handler_clientd_stct
{
    bool called;
    int err_code;
    char message[1000];
}
error_handler_clientd_t;

void error_handler(void *clientd, int errcode, const char *message)
{
    auto *ehc = (error_handler_clientd_t *)clientd;
    ehc->called = true;
    ehc->err_code = errcode;
    snprintf(ehc->message, sizeof(ehc->message), "%s", message);
}

TEST_F(AeronCArchiveTest, shouldCallErrorHandlerOnError)
{
    aeron_archive_context_t *ctx;
    aeron_archive_t *archive = nullptr;
    error_handler_clientd_t ehc;

    ehc.called = false;
    ehc.message[0] = '\0';

    ASSERT_EQ_ERR(0, aeron_archive_context_init(&ctx));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_request_channel(ctx, "aeron:udp?endpoint=localhost:8010"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_response_channel(ctx, "aeron:udp?endpoint=localhost:0"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_idle_strategy(
        ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
        ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds_clientd));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_error_handler(ctx, error_handler, &ehc));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_message_timeout_ns(ctx, 500000000));
    ASSERT_EQ_ERR(0, aeron_archive_connect(&archive, ctx));

    EXPECT_TRUE(aeron_archive_proxy_get_start_position(archive->archive_proxy, 1000, 12345));

    int64_t found_start_position;
    EXPECT_EQ(-1, aeron_archive_poll_for_response(
        &found_start_position, archive, "AeronArchive::getStartPosition", 2222));

    EXPECT_TRUE(ehc.called);
    EXPECT_EQ(AERON_ERROR_CODE_GENERIC_ERROR, ehc.err_code);
    EXPECT_STREQ("response for correlationId=1000, errorCode=5, error: unknown recording id: 12345", ehc.message);

    EXPECT_EQ(0, aeron_archive_close(archive));
    EXPECT_EQ(0, aeron_archive_context_close(ctx));
}

TEST_F(AeronCArchiveTest, shouldRecordPublicationAndFindRecording)
{
    int32_t session_id;
    int64_t stop_position;

    connect();

    int64_t subscription_id;
    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    {
        aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(m_recordingChannel, m_recordingStreamId);

        session_id = aeron_publication_session_id(publication);

        setupCounters(session_id);

        offerMessages(publication);
        consumeMessages(subscription);

        stop_position = aeron_publication_position(publication);

        waitUntilCaughtUp(stop_position);

        int64_t found_recording_position;
        ASSERT_EQ_ERR(0, aeron_archive_get_recording_position(
            &found_recording_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(stop_position, found_recording_position);

        int64_t found_stop_position;
        ASSERT_EQ_ERR(0, aeron_archive_get_stop_position(
            &found_stop_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(AERON_NULL_VALUE, found_stop_position);

        int64_t found_max_recorded_position;
        ASSERT_EQ_ERR(0, aeron_archive_get_max_recorded_position(
            &found_max_recorded_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(stop_position, found_max_recorded_position);
    }

    ASSERT_EQ_ERR(0, aeron_archive_stop_recording_subscription(
        m_archive,
        subscription_id));

    int64_t found_recording_id;
    const char *channel_fragment = "endpoint=localhost:3333";
    ASSERT_EQ_ERR(0, aeron_archive_find_last_matching_recording(
        &found_recording_id,
        m_archive,
        0,
        channel_fragment,
        m_recordingStreamId,
        session_id));

    EXPECT_EQ(m_recording_id_from_counter, found_recording_id);

    int64_t found_stop_position;
    ASSERT_EQ_ERR(0, aeron_archive_get_stop_position(
        &found_stop_position,
        m_archive,
        m_recording_id_from_counter));
    EXPECT_EQ(stop_position, found_stop_position);

    int32_t count;

    recording_descriptor_consumer_clientd_t clientd;
    clientd.verify_recording_id = true;
    clientd.recording_id = found_recording_id;
    clientd.verify_stream_id = true;
    clientd.stream_id = m_recordingStreamId;

    ASSERT_EQ_ERR(0, aeron_archive_list_recording(
        &count,
        m_archive,
        found_recording_id,
        recording_descriptor_consumer,
        &clientd));
    EXPECT_EQ(1, count);
}

TEST_F(AeronCArchiveTest, shouldRecordPublicationAndTryStopById)
{
    int32_t session_id;
    int64_t stop_position;

    connect();

    int64_t subscription_id;
    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    {
        aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(m_recordingChannel, m_recordingStreamId);

        session_id = aeron_publication_session_id(publication);

        setupCounters(session_id);

        offerMessages(publication);
        consumeMessages(subscription);

        stop_position = aeron_publication_position(publication);

        waitUntilCaughtUp(stop_position);

        int64_t found_recording_position;
        ASSERT_EQ_ERR(0, aeron_archive_get_recording_position(
            &found_recording_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(stop_position, found_recording_position);

        int64_t found_stop_position;
        ASSERT_EQ_ERR(0, aeron_archive_get_stop_position(
            &found_stop_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(AERON_NULL_VALUE, found_stop_position);

        int64_t found_max_recorded_position;
        ASSERT_EQ_ERR(0, aeron_archive_get_max_recorded_position(
            &found_max_recorded_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(stop_position, found_max_recorded_position);
    }

    bool stopped;
    EXPECT_EQ(-1, aeron_archive_try_stop_recording_by_identity(
        &stopped,
        m_archive,
        m_recording_id_from_counter + 5)); // invalid recording id

    ASSERT_EQ_ERR(0, aeron_archive_try_stop_recording_by_identity(
        &stopped,
        m_archive,
        m_recording_id_from_counter));
    EXPECT_TRUE(stopped);

    int64_t found_recording_id;
    const char *channel_fragment = "endpoint=localhost:3333";
    ASSERT_EQ_ERR(0, aeron_archive_find_last_matching_recording(
        &found_recording_id,
        m_archive,
        0,
        channel_fragment,
        m_recordingStreamId,
        session_id));

    EXPECT_EQ(m_recording_id_from_counter, found_recording_id);

    int64_t found_stop_position;
    ASSERT_EQ_ERR(0, aeron_archive_get_stop_position(
        &found_stop_position,
        m_archive,
        m_recording_id_from_counter));
    EXPECT_EQ(stop_position, found_stop_position);
}

TEST_F(AeronCArchiveTest, shouldRecordThenReplay)
{
    int32_t session_id;
    int64_t stop_position;

    connect();

    int64_t subscription_id;
    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    {
        aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(m_recordingChannel, m_recordingStreamId);

        session_id = aeron_publication_session_id(publication);

        setupCounters(session_id);

        bool is_active;
        ASSERT_EQ_ERR(0, aeron_archive_recording_pos_is_active(
            &is_active,
            m_counters_reader,
            m_counter_id,
            m_recording_id_from_counter));
        EXPECT_TRUE(is_active);

        EXPECT_EQ(m_counter_id, aeron_archive_recording_pos_find_counter_id_by_recording_id(
            m_counters_reader,
            m_recording_id_from_counter));

        {
            size_t sib_len = AERON_COUNTER_MAX_LABEL_LENGTH;
            const char source_identity_buffer[AERON_COUNTER_MAX_LABEL_LENGTH] = { '\0' };

            ASSERT_EQ_ERR(0,
                aeron_archive_recording_pos_get_source_identity(
                    m_counters_reader,
                    m_counter_id,
                    source_identity_buffer,
                    &sib_len));
            EXPECT_EQ(9, sib_len);
            EXPECT_STREQ("aeron:ipc", source_identity_buffer);
        }

        offerMessages(publication);
        consumeMessages(subscription);

        stop_position = aeron_publication_position(publication);

        waitUntilCaughtUp(stop_position);
    }

    ASSERT_EQ_ERR(0, aeron_archive_stop_recording_subscription(
        m_archive,
        subscription_id));

    int64_t found_stop_position;
    ASSERT_EQ_ERR(0, aeron_archive_get_stop_position(
        &found_stop_position,
        m_archive,
        m_recording_id_from_counter));
    while (found_stop_position != stop_position)
    {
        idle();

        ASSERT_EQ_ERR(0, aeron_archive_get_stop_position(
            &found_stop_position,
            m_archive,
            m_recording_id_from_counter));
    }

    {
        int64_t position = 0;
        int64_t length = stop_position - position;

        aeron_subscription_t *subscription = addSubscription(m_replayChannel, m_replayStreamId);

        aeron_archive_replay_params_t replay_params;
        aeron_archive_replay_params_init(&replay_params);

        replay_params.position = position;
        replay_params.length = length;
        replay_params.file_io_max_length = 4096;

        ASSERT_EQ_ERR(0, aeron_archive_start_replay(
            nullptr,
            m_archive,
            m_recording_id_from_counter,
            m_replayChannel.c_str(),
            m_replayStreamId,
            &replay_params));

        consumeMessages(subscription);

        aeron_image_t *image = aeron_subscription_image_at_index(subscription, 0);
        ASSERT_EQ(stop_position, aeron_image_position(image));
    }
}

TEST_F(AeronCArchiveTest, shouldRecordThenBoundedReplay)
{
    int32_t session_id;
    int64_t stop_position;

    connect();

    int64_t subscription_id;
    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    {

        aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(m_recordingChannel, m_recordingStreamId);

        session_id = aeron_publication_session_id(publication);

        setupCounters(session_id);

        offerMessages(publication);
        consumeMessages(subscription);

        stop_position = aeron_publication_position(publication);

        waitUntilCaughtUp(stop_position);
    }

    ASSERT_EQ_ERR(0, aeron_archive_stop_recording_subscription(
        m_archive,
        subscription_id));

    const char *counter_name = "BoundedTestCounter";

    aeron_async_add_counter_t *async_add_counter;
    ASSERT_EQ_ERR(0, aeron_async_add_counter(
        &async_add_counter,
        m_aeron,
        10001,
        (const uint8_t *)counter_name,
        strlen(counter_name),
        counter_name,
        strlen(counter_name)));

    aeron_counter_t *counter = nullptr;
    aeron_async_add_counter_poll(&counter, async_add_counter);
    while (nullptr == counter)
    {
        idle();
        aeron_async_add_counter_poll(&counter, async_add_counter);
    }

    int64_t found_stop_position;
    ASSERT_EQ_ERR(0, aeron_archive_get_stop_position(
        &found_stop_position,
        m_archive,
        m_recording_id_from_counter));
    while (found_stop_position != stop_position)
    {
        idle();

        ASSERT_EQ_ERR(0, aeron_archive_get_stop_position(
            &found_stop_position,
            m_archive,
            m_recording_id_from_counter));
    }

    {
        int64_t position = 0;
        int64_t length = stop_position - position;
        int64_t bounded_length = (length / 4) * 3;
        aeron_counter_set_release(aeron_counter_addr(counter), bounded_length);

        aeron_subscription_t *subscription = addSubscription(m_replayChannel, m_replayStreamId);

        aeron_archive_replay_params_t replay_params;
        aeron_archive_replay_params_init(&replay_params);

        replay_params.position = position;
        replay_params.length = length;
        replay_params.bounding_limit_counter_id = counter->counter_id;
        replay_params.file_io_max_length = 4096;

        ASSERT_EQ_ERR(0, aeron_archive_start_replay(
            nullptr,
            m_archive,
            m_recording_id_from_counter,
            m_replayChannel.c_str(),
            m_replayStreamId,
            &replay_params));

        int64_t position_consumed = consumeMessagesExpectingBound(
            subscription, position + bounded_length, 1000);

        EXPECT_LT(position + (length / 2), position_consumed);
        EXPECT_LE(position_consumed, position + bounded_length);
    }
}

TEST_F(AeronCArchiveTest, shouldRecordThenReplayThenTruncate)
{
    int32_t session_id;
    int64_t stop_position;

    connect();

    int64_t subscription_id;
    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    {
        aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(m_recordingChannel, m_recordingStreamId);

        session_id = aeron_publication_session_id(publication);

        setupCounters(session_id);

        offerMessages(publication);
        consumeMessages(subscription);

        stop_position = aeron_publication_position(publication);

        waitUntilCaughtUp(stop_position);

        int64_t found_recording_position;
        ASSERT_EQ_ERR(0, aeron_archive_get_recording_position(
            &found_recording_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(stop_position, found_recording_position);

        int64_t found_stop_position;
        ASSERT_EQ_ERR(0, aeron_archive_get_stop_position(
            &found_stop_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(AERON_NULL_VALUE, found_stop_position);

        int64_t found_max_recorded_position;
        ASSERT_EQ_ERR(0, aeron_archive_get_max_recorded_position(
            &found_max_recorded_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(stop_position, found_max_recorded_position);
    }

    ASSERT_EQ_ERR(0, aeron_archive_stop_recording_subscription(
        m_archive,
        subscription_id));

    int64_t found_recording_id;
    const char *channel_fragment = "endpoint=localhost:3333";
    ASSERT_EQ_ERR(0, aeron_archive_find_last_matching_recording(
        &found_recording_id,
        m_archive,
        0,
        channel_fragment,
        m_recordingStreamId,
        session_id));

    EXPECT_EQ(m_recording_id_from_counter, found_recording_id);

    int64_t found_stop_position;
    ASSERT_EQ_ERR(0, aeron_archive_get_stop_position(
        &found_stop_position,
        m_archive,
        m_recording_id_from_counter));
    EXPECT_EQ(stop_position, found_stop_position);

    int64_t position = 0;

    {
        int64_t length = stop_position - position;

        aeron_archive_replay_params_t replay_params;
        aeron_archive_replay_params_init(&replay_params);

        replay_params.position = position;
        replay_params.length = length;
        replay_params.file_io_max_length = 4096;

        aeron_subscription_t *subscription;

        ASSERT_EQ_ERR(0, aeron_archive_replay(
            &subscription,
            m_archive,
            m_recording_id_from_counter,
            m_replayChannel.c_str(),
            m_replayStreamId,
            &replay_params));

        consumeMessages(subscription);

        aeron_image_t *image = aeron_subscription_image_at_index(subscription, 0);
        EXPECT_EQ(stop_position, aeron_image_position(image));
    }

    ASSERT_EQ_ERR(0, aeron_archive_truncate_recording(
        nullptr,
        m_archive,
        m_recording_id_from_counter,
        position));

    int32_t count;

    recording_descriptor_consumer_clientd_t clientd;
    clientd.verify_start_equals_stop_position = true;

    ASSERT_EQ_ERR(0, aeron_archive_list_recording(
        &count,
        m_archive,
        found_recording_id,
        recording_descriptor_consumer,
        &clientd));
    EXPECT_EQ(1, count);
}

TEST_F(AeronCArchiveTest, shouldRecordAndCancelReplayEarly)
{
    int32_t session_id;
    int64_t stop_position;

    connect();

    {
        aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);

        aeron_publication_t *publication;
        ASSERT_EQ_ERR(0, aeron_archive_add_recorded_publication(
            &publication,
            m_archive,
            m_recordingChannel.c_str(),
            m_recordingStreamId));

        {
            aeron_publication_t *duplicate_publication;
            EXPECT_EQ(-1, aeron_archive_add_recorded_publication(
                &duplicate_publication,
                m_archive,
                m_recordingChannel.c_str(),
                m_recordingStreamId));
        }

        session_id = aeron_publication_session_id(publication);

        setupCounters(session_id);

        offerMessages(publication);
        consumeMessages(subscription);

        stop_position = aeron_publication_position(publication);

        waitUntilCaughtUp(stop_position);

        int64_t found_recording_position;
        ASSERT_EQ_ERR(0, aeron_archive_get_recording_position(
            &found_recording_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(stop_position, found_recording_position);

        ASSERT_EQ_ERR(0, aeron_archive_stop_recording_publication(m_archive, publication));

        ASSERT_EQ_ERR(0, aeron_archive_get_recording_position(
            &found_recording_position,
            m_archive,
            m_recording_id_from_counter));
        while (AERON_NULL_VALUE != found_recording_position)
        {
            idle();

            ASSERT_EQ_ERR(0, aeron_archive_get_recording_position(
                &found_recording_position,
                m_archive,
                m_recording_id_from_counter));
        }
    }

    const int64_t position = 0;
    const int64_t length = stop_position - position;
    int64_t replay_session_id;

    aeron_archive_replay_params_t replay_params;
    aeron_archive_replay_params_init(&replay_params);

    replay_params.position = position;
    replay_params.length = length;
    replay_params.file_io_max_length = 4096;

    ASSERT_EQ_ERR(0, aeron_archive_start_replay(
        &replay_session_id,
        m_archive,
        m_recording_id_from_counter,
        m_replayChannel.c_str(),
        m_replayStreamId,
        &replay_params));

    ASSERT_EQ_ERR(0, aeron_archive_stop_replay(m_archive, replay_session_id));
}

TEST_F(AeronCArchiveTest, shouldRecordAndCancelReplayEarlyWithExclusivePublication)
{
    int32_t session_id;
    int64_t stop_position;

    connect();

    {
        aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);

        aeron_exclusive_publication_t *exclusive_publication;
        ASSERT_EQ_ERR(0, aeron_archive_add_recorded_exclusive_publication(
            &exclusive_publication,
            m_archive,
            m_recordingChannel.c_str(),
            m_recordingStreamId));

        aeron_publication_constants_t constants;
        aeron_exclusive_publication_constants(exclusive_publication, &constants);
        session_id = constants.session_id;

        setupCounters(session_id);

        offerMessages(exclusive_publication);
        consumeMessages(subscription);

        stop_position = aeron_exclusive_publication_position(exclusive_publication);

        waitUntilCaughtUp(stop_position);

        int64_t found_recording_position;
        ASSERT_EQ_ERR(0, aeron_archive_get_recording_position(
            &found_recording_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(stop_position, found_recording_position);

        ASSERT_EQ_ERR(0, aeron_archive_stop_recording_exclusive_publication(m_archive, exclusive_publication));

        ASSERT_EQ_ERR(0, aeron_archive_get_recording_position(
            &found_recording_position,
            m_archive,
            m_recording_id_from_counter));
        while (AERON_NULL_VALUE != found_recording_position)
        {
            idle();

            ASSERT_EQ_ERR(0, aeron_archive_get_recording_position(
                &found_recording_position,
                m_archive,
                m_recording_id_from_counter));
        }
    }

    const int64_t position = 0;
    const int64_t length = stop_position - position;
    int64_t replay_session_id;

    aeron_archive_replay_params_t replay_params;
    aeron_archive_replay_params_init(&replay_params);

    replay_params.position = position;
    replay_params.length = length;
    replay_params.file_io_max_length = 4096;

    ASSERT_EQ_ERR(0, aeron_archive_start_replay(
        &replay_session_id,
        m_archive,
        m_recording_id_from_counter,
        m_replayChannel.c_str(),
        m_replayStreamId,
        &replay_params));

    ASSERT_EQ_ERR(0, aeron_archive_stop_replay(m_archive, replay_session_id));
}

TEST_F(AeronCArchiveTest, shouldGetStartPosition)
{
    int32_t session_id;

    connect();

    aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);
    aeron_publication_t *publication = addPublication(m_recordingChannel, m_recordingStreamId);

    session_id = aeron_publication_session_id(publication);

    offerMessages(publication);
    consumeMessages(subscription);

    int64_t halfway_position = aeron_publication_position(publication);

    int64_t subscription_id;
    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    setupCounters(session_id);

    offerMessages(publication);
    consumeMessages(subscription);

    int64_t end_position = aeron_publication_position(publication);

    waitUntilCaughtUp(end_position);

    int64_t found_start_position;
    ASSERT_EQ_ERR(0, aeron_archive_get_start_position(
        &found_start_position,
        m_archive,
        m_recording_id_from_counter));
    ASSERT_EQ(found_start_position, halfway_position);
}

TEST_F(AeronCArchiveTest, shouldReplayRecordingFromLateJoinPosition)
{
    int32_t session_id;

    connect();

    int64_t subscription_id;
    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    {
        aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(m_recordingChannel, m_recordingStreamId);

        session_id = aeron_publication_session_id(publication);

        setupCounters(session_id);

        offerMessages(publication);
        consumeMessages(subscription);

        int64_t current_position = aeron_publication_position(publication);

        waitUntilCaughtUp(current_position);

        aeron_archive_replay_params_t replay_params;
        aeron_archive_replay_params_init(&replay_params);

        replay_params.position = current_position;
        replay_params.file_io_max_length = 4096;

        aeron_subscription_t *replay_subscription;

        ASSERT_EQ_ERR(0, aeron_archive_replay(
            &replay_subscription,
            m_archive,
            m_recording_id_from_counter,
            m_replayChannel.c_str(),
            m_replayStreamId,
            &replay_params));

        offerMessages(publication);
        consumeMessages(subscription);
        consumeMessages(replay_subscription);

        int64_t end_position = aeron_publication_position(publication);

        aeron_image_t *image = aeron_subscription_image_at_index(replay_subscription, 0);
        EXPECT_EQ(end_position, aeron_image_position(image));
    }
}

TEST_F(AeronCArchiveTest, shouldListRegisteredRecordingSubscriptions)
{
    subscription_descriptor_consumer_clientd clientd;

    int32_t expected_stream_id = 7;
    const char *channelOne = "aeron:ipc";
    const char *channelTwo = "aeron:udp?endpoint=localhost:5678";
    const char *channelThree = "aeron:udp?endpoint=localhost:4321";

    connect();

    int64_t subscription_id_one;
    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        &subscription_id_one,
        m_archive,
        channelOne,
        expected_stream_id,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    int64_t subscription_id_two;
    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        &subscription_id_two,
        m_archive,
        channelTwo,
        expected_stream_id + 1,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    int64_t subscription_id_three;
    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        &subscription_id_three,
        m_archive,
        channelThree,
        expected_stream_id + 2,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    const auto pub2 = addPublication(channelTwo, expected_stream_id + 1);
    const auto pub3 = addPublication(channelThree, expected_stream_id + 2);

    // await recording started
    auto countersReader = aeron_counters_reader(m_aeron);
    const auto sub2CounterId =
        getRecordingCounterId(aeron_publication_session_id(pub2), countersReader);
    const auto sub3CounterId = getRecordingCounterId(aeron_publication_session_id(pub3), countersReader);

    int32_t count_one;
    ASSERT_EQ_ERR(0, aeron_archive_list_recording_subscriptions(
        &count_one,
        m_archive,
        0,
        5,
        "ipc",
        expected_stream_id,
        true,
        recording_subscription_descriptor_consumer,
        &clientd));
    EXPECT_EQ(1, clientd.descriptors.size());
    EXPECT_EQ(1, count_one);

    clientd.descriptors.clear();

    int32_t count_two;
    ASSERT_EQ_ERR(0, aeron_archive_list_recording_subscriptions(
        &count_two,
        m_archive,
        0,
        5,
        "",
        expected_stream_id,
        false,
        recording_subscription_descriptor_consumer,
        &clientd));
    EXPECT_EQ(3, clientd.descriptors.size());
    EXPECT_EQ(3, count_two);

    ASSERT_EQ_ERR(0, aeron_archive_stop_recording_subscription(
        m_archive,
        subscription_id_two));
    clientd.descriptors.clear();

    // await recording stopped
    int state;
    while (true)
    {
        EXPECT_EQ(0, aeron_counters_reader_counter_state(countersReader, sub2CounterId, &state));
        if (AERON_COUNTER_RECORD_ALLOCATED != state)
        {
            break;
        }
        std::this_thread::yield();
    }

    EXPECT_EQ(0, aeron_counters_reader_counter_state(countersReader, sub3CounterId, &state));
    EXPECT_EQ(AERON_COUNTER_RECORD_ALLOCATED, state);

    int32_t count_three;
    ASSERT_EQ_ERR(0, aeron_archive_list_recording_subscriptions(
        &count_three,
        m_archive,
        0,
        5,
        "",
        expected_stream_id,
        false,
        recording_subscription_descriptor_consumer,
        &clientd));
    EXPECT_EQ(2, clientd.descriptors.size());
    EXPECT_EQ(2, count_three);
}

TEST_F(AeronCArchiveTest, shouldMergeFromReplayToLive)
{
    const std::size_t termLength = 64 * 1024;
    const std::string message_prefix = "Message ";
    const std::size_t min_messages_per_term = termLength / (message_prefix.length() + AERON_DATA_HEADER_LENGTH);
    const char *control_endpoint = "localhost:23265";
    const char *recording_endpoint = "localhost:23266";
    const char *live_endpoint = "localhost:23267";
    const char *replay_endpoint = "localhost:0";

    char publication_channel[AERON_URI_MAX_LENGTH];
    char live_destination[AERON_URI_MAX_LENGTH];
    char replay_destination[AERON_URI_MAX_LENGTH];
    char recording_channel[AERON_URI_MAX_LENGTH];
    char subscription_channel[AERON_URI_MAX_LENGTH];

    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_new(&builder);

        aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_CONTROL_KEY, control_endpoint);
        aeron_uri_string_builder_put(
            &builder, AERON_UDP_CHANNEL_CONTROL_MODE_KEY, AERON_UDP_CHANNEL_CONTROL_MODE_DYNAMIC_VALUE);
        aeron_uri_string_builder_put(&builder, AERON_URI_FC_KEY, "tagged,g:99901/1,t:5s");
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_TERM_LENGTH_KEY, termLength);

        aeron_uri_string_builder_sprint(&builder, publication_channel, sizeof(publication_channel));
        aeron_uri_string_builder_close(&builder);
    }

    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_new(&builder);

        aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_ENDPOINT_KEY, live_endpoint);
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_CONTROL_KEY, control_endpoint);

        aeron_uri_string_builder_sprint(&builder, live_destination, sizeof(live_destination));
        aeron_uri_string_builder_close(&builder);
    }

    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_new(&builder);

        aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_ENDPOINT_KEY, replay_endpoint);

        aeron_uri_string_builder_sprint(&builder, replay_destination, sizeof(replay_destination));
        aeron_uri_string_builder_close(&builder);
    }

    const size_t initial_message_count = min_messages_per_term * 3;
    const size_t subsequent_message_count = min_messages_per_term * 3;
    const size_t total_message_count = min_messages_per_term + subsequent_message_count;

    connect();

    aeron_publication_t *publication = addPublication(publication_channel, m_recordingStreamId);

    int32_t session_id = aeron_publication_session_id(publication);

    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_new(&builder);

        aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
        aeron_uri_string_builder_put(&builder, AERON_URI_GTAG_KEY, "99901");
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_SESSION_ID_KEY, session_id);
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_ENDPOINT_KEY, recording_endpoint);
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_CONTROL_KEY, control_endpoint);

        aeron_uri_string_builder_sprint(&builder, recording_channel, sizeof(recording_channel));
        aeron_uri_string_builder_close(&builder);
    }

    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_new(&builder);

        aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
        aeron_uri_string_builder_put(
            &builder, AERON_UDP_CHANNEL_CONTROL_MODE_KEY, AERON_UDP_CHANNEL_CONTROL_MODE_MANUAL_VALUE);
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_SESSION_ID_KEY, session_id);

        aeron_uri_string_builder_sprint(&builder, subscription_channel, sizeof(subscription_channel));
        aeron_uri_string_builder_close(&builder);
    }

    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        nullptr,
        m_archive,
        recording_channel,
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_REMOTE,
        true));

    setupCounters(session_id);

    bool is_active;
    ASSERT_EQ_ERR(0, aeron_archive_recording_pos_is_active(
        &is_active,
        m_counters_reader,
        m_counter_id,
        m_recording_id_from_counter));
    EXPECT_TRUE(is_active);

    ASSERT_EQ_ERR(m_counter_id, aeron_archive_recording_pos_find_counter_id_by_recording_id(
        m_counters_reader,
        m_recording_id_from_counter));

    {
        size_t sib_len = AERON_COUNTER_MAX_LABEL_LENGTH;
        const char source_identity_buffer[AERON_COUNTER_MAX_LABEL_LENGTH] = { '\0' };

        ASSERT_EQ_ERR(0,
            aeron_archive_recording_pos_get_source_identity(
                m_counters_reader,
                m_counter_id,
                source_identity_buffer,
                &sib_len));
        EXPECT_STREQ("127.0.0.1:23265", source_identity_buffer);
    }

    offerMessages(publication, initial_message_count);

    waitUntilCaughtUp(aeron_publication_position(publication));

    size_t messages_published = initial_message_count;

    fragment_handler_clientd_t clientd;
    clientd.received = 0;
    clientd.position = 0;

    while (true)
    {
        aeron_subscription_t *subscription = addSubscription(subscription_channel, m_recordingStreamId);

        char replay_channel[AERON_URI_MAX_LENGTH];
        {
            aeron_uri_string_builder_t builder;

            aeron_uri_string_builder_init_new(&builder);

            aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
            aeron_uri_string_builder_put_int32(&builder, AERON_URI_SESSION_ID_KEY, session_id);

            aeron_uri_string_builder_sprint(&builder, replay_channel, sizeof(replay_channel));
            aeron_uri_string_builder_close(&builder);
        }

        aeron_archive_replay_merge_t *replay_merge;

        ASSERT_EQ_ERR(0, aeron_archive_replay_merge_init(
            &replay_merge,
            subscription,
            m_archive,
            replay_channel,
            replay_destination,
            live_destination,
            m_recording_id_from_counter,
            clientd.position,
            aeron_epoch_clock(),
            REPLAY_MERGE_PROGRESS_TIMEOUT_DEFAULT_MS));

        if (attemptReplayMerge(
            replay_merge,
            publication,
            fragment_handler,
            &clientd,
            total_message_count,
            &messages_published,
            &clientd.received))
        {
            ASSERT_EQ_ERR(0, aeron_archive_replay_merge_close(replay_merge));
            break;
        }

        ASSERT_EQ_ERR(0, aeron_archive_replay_merge_close(replay_merge));
        idle();
    }

    EXPECT_EQ(clientd.received, total_message_count);
    EXPECT_EQ(clientd.position, aeron_publication_position(publication));
}

TEST_F(AeronCArchiveTest, shouldFailForIncorrectInitialCredentials)
{
    credentials_supplier_clientd_t bad_creds_clientd = { &bad_creds, nullptr };

    ASSERT_EQ_ERR(0, aeron_archive_context_init(&m_ctx));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_idle_strategy(
        m_ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
        m_ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &bad_creds_clientd));

    ASSERT_EQ(-1, aeron_archive_connect(&m_archive, m_ctx));
}

TEST_F(AeronCArchiveTest, shouldBeAbleToHandleBeingChallenged)
{
    aeron_archive_encoded_credentials_t creds = { "admin:adminC", 12 };
    aeron_archive_encoded_credentials_t challenge_creds = { "admin:CSadmin", 13 };
    credentials_supplier_clientd_t creds_clientd = { &creds, &challenge_creds };

    ASSERT_EQ_ERR(0, aeron_archive_context_init(&m_ctx));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_request_channel(m_ctx, "aeron:udp?endpoint=localhost:8010"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_response_channel(m_ctx, "aeron:udp?endpoint=localhost:0"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_idle_strategy(
        m_ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
        m_ctx,
        encoded_credentials_supplier,
        encoded_credentials_on_challenge,
        nullptr,
        &creds_clientd));

    ASSERT_EQ_ERR(0, aeron_archive_connect(&m_archive, m_ctx));
}

TEST_F(AeronCArchiveTest, shouldExceptionForIncorrectChallengeCredentials)
{
    aeron_archive_encoded_credentials_t creds = { "admin:adminC", 12 };
    aeron_archive_encoded_credentials_t bad_challenge_creds = { "admin:adminNoCS", 15 };
    credentials_supplier_clientd_t creds_clientd = { &creds, &bad_challenge_creds };

    ASSERT_EQ_ERR(0, aeron_archive_context_init(&m_ctx));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_idle_strategy(
        m_ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
        m_ctx,
        encoded_credentials_supplier,
        encoded_credentials_on_challenge,
        nullptr,
        &creds_clientd));

    ASSERT_EQ(-1, aeron_archive_connect(&m_archive, m_ctx));
}

TEST_F(AeronCArchiveTest, shouldPurgeStoppedRecording)
{
    int32_t session_id;
    int64_t stop_position;

    connect();

    int64_t subscription_id;
    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    {
        aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(m_recordingChannel, m_recordingStreamId);

        session_id = aeron_publication_session_id(publication);

        setupCounters(session_id);

        offerMessages(publication);
        consumeMessages(subscription);

        stop_position = aeron_publication_position(publication);

        waitUntilCaughtUp(stop_position);

        int64_t found_recording_position;
        ASSERT_EQ_ERR(0, aeron_archive_get_recording_position(
            &found_recording_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(stop_position, found_recording_position);

        int64_t found_stop_position;
        ASSERT_EQ_ERR(0, aeron_archive_get_stop_position(
            &found_stop_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(AERON_NULL_VALUE, found_stop_position);
    }

    ASSERT_EQ_ERR(0, aeron_archive_stop_recording_subscription(
        m_archive,
        subscription_id));

    int64_t found_recording_id;
    const char *channel_fragment = "endpoint=localhost:3333";
    ASSERT_EQ_ERR(0, aeron_archive_find_last_matching_recording(
        &found_recording_id,
        m_archive,
        0,
        channel_fragment,
        m_recordingStreamId,
        session_id));

    EXPECT_EQ(m_recording_id_from_counter, found_recording_id);

    int64_t found_stop_position;
    ASSERT_EQ_ERR(0, aeron_archive_get_stop_position(
        &found_stop_position,
        m_archive,
        m_recording_id_from_counter));
    EXPECT_EQ(stop_position, found_stop_position);

    int64_t deleted_segments_count;
    ASSERT_EQ_ERR(0, aeron_archive_purge_recording(&deleted_segments_count, m_archive, m_recording_id_from_counter));
    EXPECT_EQ(1, deleted_segments_count);

    int32_t count = 1234; // <-- just to make sure later when it's zero it's because it was explicitly set to 0.

    recording_descriptor_consumer_clientd_t clientd;
    clientd.verify_start_equals_stop_position = true;

    ASSERT_EQ_ERR(0, aeron_archive_list_recording(
        &count,
        m_archive,
        found_recording_id,
        recording_descriptor_consumer,
        &clientd));
    EXPECT_EQ(0, count);
}

TEST_F(AeronCArchiveTest, shouldReadRecordingDescriptor)
{
    int32_t session_id;

    connect();

    aeron_publication_t *publication = addPublication(m_recordingChannel, m_recordingStreamId);

    session_id = aeron_publication_session_id(publication);

    int64_t subscription_id;
    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    setupCounters(session_id);

    ASSERT_EQ_ERR(0, aeron_archive_stop_recording_subscription(
        m_archive,
        subscription_id));

    int32_t count = 1234;

    recording_descriptor_consumer_clientd_t clientd;
    clientd.verify_recording_id = true;
    clientd.recording_id = m_recording_id_from_counter;
    clientd.verify_stream_id = true;
    clientd.stream_id = m_recordingStreamId;
    clientd.verify_session_id = true;
    clientd.session_id = session_id;
    clientd.original_channel = m_recordingChannel.c_str();

    ASSERT_EQ_ERR(0, aeron_archive_list_recording(
        &count,
        m_archive,
        m_recording_id_from_counter,
        recording_descriptor_consumer,
        &clientd));
    EXPECT_EQ(1, count);
}

TEST_F(AeronCArchiveTest, shouldFindMultipleRecordingDescriptors)
{
    aeron_publication_t *publication;
    int32_t session_id;
    int64_t subscription_id;
    int64_t subscription_id2;

    std::set<std::int32_t> session_ids;

    connect();

    publication = addPublication(m_recordingChannel, m_recordingStreamId);

    session_id = aeron_publication_session_id(publication);
    session_ids.insert(session_id);

    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    setupCounters(session_id);

    const std::string recordingChannel2 = "aeron:udp?endpoint=localhost:3334";
    publication = addPublication(recordingChannel2, m_recordingStreamId);

    session_id = aeron_publication_session_id(publication);
    session_ids.insert(session_id);

    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        &subscription_id2,
        m_archive,
        recordingChannel2.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    setupCounters(session_id);

    int32_t count = 1234;

    recording_descriptor_consumer_clientd_t clientd;

    ASSERT_EQ_ERR(0, aeron_archive_list_recordings(
        &count,
        m_archive,
        INT64_MIN,
        10,
        recording_descriptor_consumer,
        &clientd));
    EXPECT_EQ(2, count);
    EXPECT_EQ(session_ids, clientd.session_ids);

    ASSERT_EQ_ERR(0, aeron_archive_list_recordings(
        &count,
        m_archive,
        INT64_MIN,
        1,
        recording_descriptor_consumer,
        &clientd));
    EXPECT_EQ(1, count);

    ASSERT_EQ_ERR(0, aeron_archive_stop_recording_subscription(
        m_archive,
        subscription_id));

    ASSERT_EQ_ERR(0, aeron_archive_stop_recording_subscription(
        m_archive,
        subscription_id2));
}

TEST_F(AeronCArchiveTest, shouldFindRecordingDescriptorForUri)
{
    aeron_publication_t *publication;
    int32_t session_id;
    int64_t subscription_id;
    int64_t subscription_id2;

    std::set<std::int32_t> session_ids;

    connect();

    publication = addPublication(m_recordingChannel, m_recordingStreamId);

    session_id = aeron_publication_session_id(publication);
    session_ids.insert(session_id);

    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    setupCounters(session_id);

    const std::string recordingChannel2 = "aeron:udp?endpoint=localhost:3334";
    publication = addPublication(recordingChannel2, m_recordingStreamId);

    session_id = aeron_publication_session_id(publication);
    session_ids.insert(session_id);

    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        &subscription_id2,
        m_archive,
        recordingChannel2.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    setupCounters(session_id);

    int32_t count = 1234;

    recording_descriptor_consumer_clientd_t clientd;

    clientd.verify_session_id = true;
    clientd.session_id = session_id;

    ASSERT_EQ_ERR(0, aeron_archive_list_recordings_for_uri(
        &count,
        m_archive,
        INT64_MIN,
        2,
        "3334",
        m_recordingStreamId,
        recording_descriptor_consumer,
        &clientd));
    EXPECT_EQ(1, count);

    clientd.verify_session_id = false;
    clientd.session_ids.clear();

    ASSERT_EQ_ERR(0, aeron_archive_list_recordings_for_uri(
        &count,
        m_archive,
        INT64_MIN,
        10,
        "333",
        m_recordingStreamId,
        recording_descriptor_consumer,
        &clientd));
    EXPECT_EQ(2, count);
    EXPECT_EQ(session_ids, clientd.session_ids);

    ASSERT_EQ_ERR(0, aeron_archive_list_recordings_for_uri(
        &count,
        m_archive,
        INT64_MIN,
        10,
        "no-match",
        m_recordingStreamId,
        recording_descriptor_consumer,
        &clientd));
    EXPECT_EQ(0, count);

    ASSERT_EQ_ERR(0, aeron_archive_stop_recording_subscription(
        m_archive,
        subscription_id));

    ASSERT_EQ_ERR(0, aeron_archive_stop_recording_subscription(
        m_archive,
        subscription_id2));
}

TEST_F(AeronCArchiveTest, shouldReadJumboRecordingDescriptor)
{
    int32_t session_id;
    int64_t stop_position;

    std::string recordingChannel = "aeron:udp?endpoint=localhost:3333|term-length=64k|alias=";
    recordingChannel.append(2000, 'X');

    connect();

    int64_t subscription_id;
    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    {
        aeron_subscription_t *subscription = addSubscription(recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(recordingChannel, m_recordingStreamId);

        session_id = aeron_publication_session_id(publication);

        setupCounters(session_id);

        offerMessages(publication);
        consumeMessages(subscription);

        stop_position = aeron_publication_position(publication);

        waitUntilCaughtUp(stop_position);

        int64_t found_recording_position;
        ASSERT_EQ_ERR(0, aeron_archive_get_recording_position(
            &found_recording_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(stop_position, found_recording_position);

        int64_t found_stop_position;
        ASSERT_EQ_ERR(0, aeron_archive_get_stop_position(
            &found_stop_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(AERON_NULL_VALUE, found_stop_position);
    }

    ASSERT_EQ_ERR(0, aeron_archive_stop_recording_subscription(
        m_archive,
        subscription_id));

    int64_t found_stop_position;
    ASSERT_EQ_ERR(0, aeron_archive_get_stop_position(
        &found_stop_position,
        m_archive,
        m_recording_id_from_counter));
    EXPECT_EQ(stop_position, found_stop_position);

    int32_t count = 1234;

    recording_descriptor_consumer_clientd_t clientd;
    clientd.verify_recording_id = true;
    clientd.recording_id = m_recording_id_from_counter;
    clientd.verify_stream_id = true;
    clientd.stream_id = m_recordingStreamId;
    clientd.original_channel = recordingChannel.c_str();

    ASSERT_EQ_ERR(0, aeron_archive_list_recording(
        &count,
        m_archive,
        m_recording_id_from_counter,
        recording_descriptor_consumer,
        &clientd));
    EXPECT_EQ(1, count);
}

TEST_F(AeronCArchiveTest, shouldRecordReplicateThenReplay)
{
    int32_t session_id;
    int64_t stop_position;

    startDestArchive();

    recording_signal_consumer_clientd_t rsc_cd;
    rsc_cd.signals.clear();

    ASSERT_EQ_ERR(0, aeron_archive_context_set_recording_signal_consumer(
        m_dest_ctx, recording_signal_consumer, &rsc_cd));

    connect();

    ASSERT_EQ_ERR(0, aeron_archive_connect(&m_dest_archive, m_dest_ctx));

    ASSERT_EQ(42, aeron_archive_get_archive_id(m_archive));
    ASSERT_EQ(-7777, aeron_archive_get_archive_id(m_dest_archive));

    int64_t subscription_id;
    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    {
        aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(m_recordingChannel, m_recordingStreamId);

        session_id = aeron_publication_session_id(publication);

        setupCounters(session_id);

        bool is_active;
        ASSERT_EQ_ERR(0, aeron_archive_recording_pos_is_active(
            &is_active,
            m_counters_reader,
            m_counter_id,
            m_recording_id_from_counter));
        EXPECT_TRUE(is_active);

        EXPECT_EQ(m_counter_id, aeron_archive_recording_pos_find_counter_id_by_recording_id(
            m_counters_reader,
            m_recording_id_from_counter));

        {
            size_t sib_len = AERON_COUNTER_MAX_LABEL_LENGTH;
            const char source_identity_buffer[AERON_COUNTER_MAX_LABEL_LENGTH] = { '\0' };

            ASSERT_EQ_ERR(0,
                aeron_archive_recording_pos_get_source_identity(
                    m_counters_reader,
                    m_counter_id,
                    source_identity_buffer,
                    &sib_len));
            EXPECT_EQ(9, sib_len);
            EXPECT_STREQ("aeron:ipc", source_identity_buffer);
        }

        offerMessages(publication);
        consumeMessages(subscription);

        stop_position = aeron_publication_position(publication);

        waitUntilCaughtUp(stop_position);
    }

    ASSERT_EQ_ERR(0, aeron_archive_stop_recording_subscription(
        m_archive,
        subscription_id));

    int64_t found_stop_position;
    do {
        ASSERT_EQ_ERR(0, aeron_archive_get_stop_position(
            &found_stop_position,
            m_archive,
            m_recording_id_from_counter));

        idle();
    }
    while (found_stop_position != stop_position);

    aeron_archive_replication_params_t replication_params;
    aeron_archive_replication_params_init(&replication_params);

    replication_params.encoded_credentials = &default_creds;

    ASSERT_EQ_ERR(0, aeron_archive_replicate(
        nullptr,
        m_dest_archive,
        m_recording_id_from_counter,
        aeron_archive_context_get_control_request_channel(m_ctx),
        aeron_archive_context_get_control_request_stream_id(m_ctx),
        &replication_params));

    while (0 == rsc_cd.signals.count(AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_SYNC))
    {
        aeron_archive_poll_for_recording_signals(nullptr, m_dest_archive);

        idle();
    }

    int64_t position = 0;
    int64_t length = stop_position - position;

    aeron_subscription_t *subscription = addSubscription(m_replayChannel, m_replayStreamId);

    aeron_archive_replay_params_t replay_params;
    aeron_archive_replay_params_init(&replay_params);

    replay_params.position = position;
    replay_params.length = length;
    replay_params.file_io_max_length = 4096;

    ASSERT_EQ_ERR(0, aeron_archive_start_replay(
        nullptr,
        m_dest_archive,
        m_recording_id_from_counter,
        m_replayChannel.c_str(),
        m_replayStreamId,
        &replay_params));

    consumeMessages(subscription);

    aeron_image_t *image = aeron_subscription_image_at_index(subscription, 0);
    EXPECT_EQ(stop_position, aeron_image_position(image));
}

TEST_P(AeronCArchiveParamTest, shouldRecordReplicateThenStop)
{
    bool tryStop = GetParam();

    int32_t session_id;
    int64_t stop_position;

    startDestArchive();

    recording_signal_consumer_clientd_t rsc_cd;
    rsc_cd.signals.clear();

    ASSERT_EQ_ERR(0, aeron_archive_context_set_recording_signal_consumer(
        m_dest_ctx, recording_signal_consumer, &rsc_cd));

    connect();

    ASSERT_EQ_ERR(0, aeron_archive_connect(&m_dest_archive, m_dest_ctx));

    ASSERT_EQ(42, aeron_archive_get_archive_id(m_archive));
    ASSERT_EQ(-7777, aeron_archive_get_archive_id(m_dest_archive));

    int64_t subscription_id;
    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);
    aeron_publication_t *publication = addPublication(m_recordingChannel, m_recordingStreamId);

    session_id = aeron_publication_session_id(publication);

    setupCounters(session_id);

    offerMessages(publication);
    consumeMessages(subscription);

    stop_position = aeron_publication_position(publication);

    waitUntilCaughtUp(stop_position);

    aeron_archive_replication_params_t replication_params;
    aeron_archive_replication_params_init(&replication_params);

    replication_params.encoded_credentials = &default_creds;

    int64_t replication_id;
    ASSERT_EQ_ERR(0, aeron_archive_replicate(
        &replication_id,
        m_dest_archive,
        m_recording_id_from_counter,
        aeron_archive_context_get_control_request_channel(m_ctx),
        aeron_archive_context_get_control_request_stream_id(m_ctx),
        &replication_params));

    while (
        0 == rsc_cd.signals.count(AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_REPLICATE) ||
        0 == rsc_cd.signals.count(AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_EXTEND))
    {
        aeron_archive_poll_for_recording_signals(nullptr, m_dest_archive);

        idle();
    }

    int64_t position = 0;

    aeron_subscription_t *replay_subscription = addSubscription(m_replayChannel, m_replayStreamId);

    aeron_archive_replay_params_t replay_params;
    aeron_archive_replay_params_init(&replay_params);

    replay_params.position = position;
    replay_params.file_io_max_length = 4096;

    ASSERT_EQ_ERR(0, aeron_archive_start_replay(
        nullptr,
        m_dest_archive,
        m_recording_id_from_counter,
        m_replayChannel.c_str(),
        m_replayStreamId,
        &replay_params));

    consumeMessages(replay_subscription);

    if (tryStop)
    {
        bool stopped;
        ASSERT_EQ_ERR(0, aeron_archive_try_stop_replication(&stopped, m_dest_archive, replication_id));
        ASSERT_TRUE(stopped);
    }
    else
    {
        ASSERT_EQ_ERR(0, aeron_archive_stop_replication(m_dest_archive, replication_id));
    }

    offerMessages(publication);

    ASSERT_EQ_ERR(0, consumeMessagesExpectingBound(replay_subscription, 0, 1000));

    while (0 == rsc_cd.signals.count(AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_REPLICATE_END))
    {
        aeron_archive_poll_for_recording_signals(nullptr, m_dest_archive);

        idle();
    }

    aeron_image_t *image = aeron_subscription_image_at_index(replay_subscription, 0);
    EXPECT_EQ(stop_position, aeron_image_position(image));
}

TEST_F(AeronCArchiveTest, shouldRecordReplicateTwice)
{
    int32_t session_id;
    int64_t stop_position;

    startDestArchive();

    recording_signal_consumer_clientd_t rsc_cd;
    rsc_cd.signals.clear();

    ASSERT_EQ_ERR(
        0, aeron_archive_context_set_recording_signal_consumer(m_dest_ctx, recording_signal_consumer, &rsc_cd));

    connect();

    ASSERT_EQ_ERR(0, aeron_archive_connect(&m_dest_archive, m_dest_ctx));

    ASSERT_EQ(42, aeron_archive_get_archive_id(m_archive));
    ASSERT_EQ(-7777, aeron_archive_get_archive_id(m_dest_archive));

    int64_t subscription_id;
    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    int64_t halfway_position;

    {
        aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(m_recordingChannel, m_recordingStreamId);

        session_id = aeron_publication_session_id(publication);

        setupCounters(session_id);

        bool is_active;
        ASSERT_EQ_ERR(0, aeron_archive_recording_pos_is_active(
            &is_active,
            m_counters_reader,
            m_counter_id,
            m_recording_id_from_counter));
        EXPECT_TRUE(is_active);

        EXPECT_EQ(m_counter_id, aeron_archive_recording_pos_find_counter_id_by_recording_id(
            m_counters_reader,
            m_recording_id_from_counter));

        {
            size_t sib_len = AERON_COUNTER_MAX_LABEL_LENGTH;
            const char source_identity_buffer[AERON_COUNTER_MAX_LABEL_LENGTH] = { '\0' };

            ASSERT_EQ_ERR(0,
                aeron_archive_recording_pos_get_source_identity(
                    m_counters_reader,
                    m_counter_id,
                    source_identity_buffer,
                    &sib_len));
            EXPECT_EQ(9, sib_len);
            EXPECT_STREQ("aeron:ipc", source_identity_buffer);
        }

        offerMessages(publication);
        consumeMessages(subscription);
        halfway_position = aeron_publication_position(publication);
        waitUntilCaughtUp(halfway_position);

        offerMessages(publication);
        consumeMessages(subscription);
        stop_position = aeron_publication_position(publication);
        waitUntilCaughtUp(stop_position);
    }

    ASSERT_EQ_ERR(0, aeron_archive_stop_recording_subscription(
        m_archive,
        subscription_id));

    int64_t found_stop_position;
    do
    {
        ASSERT_EQ_ERR(0, aeron_archive_get_stop_position(
            &found_stop_position,
            m_archive,
            m_recording_id_from_counter));

        idle();
    }
    while (found_stop_position != stop_position);

    aeron_archive_replication_params_t replication_params1;
    aeron_archive_replication_params_init(&replication_params1);

    replication_params1.encoded_credentials = &default_creds;
    replication_params1.stop_position = halfway_position;
    replication_params1.replication_session_id = 1;

    ASSERT_EQ_ERR(0, aeron_archive_replicate(
        nullptr,
        m_dest_archive,
        m_recording_id_from_counter,
        aeron_archive_context_get_control_request_channel(m_ctx),
        aeron_archive_context_get_control_request_stream_id(m_ctx),
        &replication_params1));

    while (0 == rsc_cd.signals.count(AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_REPLICATE_END))
    {
        aeron_archive_poll_for_recording_signals(nullptr, m_dest_archive);

        idle();
    }

    aeron_archive_replication_params_t replication_params2;
    aeron_archive_replication_params_init(&replication_params2);

    replication_params2.encoded_credentials = &default_creds;
    replication_params2.replication_session_id = 2;

    ASSERT_EQ_ERR(0, aeron_archive_replicate(
        nullptr,
        m_dest_archive,
        m_recording_id_from_counter,
        aeron_archive_context_get_control_request_channel(m_ctx),
        aeron_archive_context_get_control_request_stream_id(m_ctx),
        &replication_params2));

    rsc_cd.signals.clear();

    while (0 == rsc_cd.signals.count(AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_REPLICATE_END))
    {
        aeron_archive_poll_for_recording_signals(nullptr, m_dest_archive);

        idle();
    }
}

TEST_F(AeronCArchiveIdTest, shouldInitializeContextWithDefaultValues)
{
    ASSERT_EQ_ERR(0, aeron_archive_context_init(&m_ctx));

    EXPECT_EQ(nullptr, m_ctx->aeron);
    EXPECT_NE(nullptr, m_ctx->aeron_directory_name);
    EXPECT_NE(0, strlen(m_ctx->aeron_directory_name));
    EXPECT_FALSE(m_ctx->owns_aeron_client);

    EXPECT_EQ(nullptr, m_ctx->control_request_channel);
    EXPECT_EQ(AERON_ARCHIVE_CONTROL_STREAM_ID_DEFAULT, m_ctx->control_request_stream_id);

    EXPECT_EQ(nullptr, m_ctx->control_response_channel);
    EXPECT_EQ(AERON_ARCHIVE_CONTROL_RESPONSE_STREAM_ID_DEFAULT, m_ctx->control_response_stream_id);

    EXPECT_EQ(nullptr, m_ctx->recording_events_channel);
    EXPECT_EQ(AERON_ARCHIVE_RECORDING_EVENTS_STREAM_ID_DEFAULT, m_ctx->recording_events_stream_id);

    EXPECT_EQ(AERON_ARCHIVE_MESSAGE_TIMEOUT_NS_DEFAULT, m_ctx->message_timeout_ns);

    EXPECT_EQ(AERON_ARCHIVE_CONTROL_TERM_BUFFER_LENGTH_DEFAULT, m_ctx->control_term_buffer_length);
    EXPECT_EQ(AERON_ARCHIVE_CONTROL_TERM_BUFFER_SPARSE_DEFAULT, m_ctx->control_term_buffer_sparse);
    EXPECT_EQ(1408, m_ctx->control_mtu_length);

    EXPECT_EQ(0, aeron_archive_context_close(m_ctx));
}

TEST_F(AeronCArchiveIdTest, shouldInitializeContextWithValuesSpecifiedViaEnvironment)
{
    const auto aeron_dir = "/dev/shm/aeron-test-dir";
    const auto control_channel = "aeron:udp?endpoint=localhost:5555";
    const auto response_channel = "aeron:udp?endpoint=localhost:0";
    const auto recording_events_channel = "aeron:udp?endpoint=localhost:8888|alias=events";
    aeron_env_set(AERON_DIR_ENV_VAR, aeron_dir);
    aeron_env_set(AERON_ARCHIVE_CONTROL_CHANNEL_ENV_VAR, control_channel);
    aeron_env_set(AERON_ARCHIVE_CONTROL_STREAM_ID_ENV_VAR, "-4321");
    aeron_env_set(AERON_ARCHIVE_CONTROL_RESPONSE_CHANNEL_ENV_VAR, response_channel);
    aeron_env_set(AERON_ARCHIVE_CONTROL_RESPONSE_STREAM_ID_ENV_VAR, "2009");
    aeron_env_set(AERON_ARCHIVE_RECORDING_EVENTS_CHANNEL_ENV_VAR, recording_events_channel);
    aeron_env_set(AERON_ARCHIVE_RECORDING_EVENTS_STREAM_ID_ENV_VAR, "2147483647");
    aeron_env_set(AERON_ARCHIVE_MESSAGE_TIMEOUT_ENV_VAR, "9223372036s");
    aeron_env_set(AERON_ARCHIVE_CONTROL_TERM_BUFFER_LENGTH_ENV_VAR, "128k");
    aeron_env_set(AERON_ARCHIVE_CONTROL_TERM_BUFFER_SPARSE_ENV_VAR, "false");
    aeron_env_set(AERON_ARCHIVE_CONTROL_MTU_LENGTH_ENV_VAR, "8k");

    EXPECT_EQ(0, aeron_archive_context_init(&m_ctx));

    aeron_env_unset(AERON_DIR_ENV_VAR);
    aeron_env_unset(AERON_ARCHIVE_CONTROL_CHANNEL_ENV_VAR);
    aeron_env_unset(AERON_ARCHIVE_CONTROL_STREAM_ID_ENV_VAR);
    aeron_env_unset(AERON_ARCHIVE_CONTROL_RESPONSE_CHANNEL_ENV_VAR);
    aeron_env_unset(AERON_ARCHIVE_CONTROL_RESPONSE_STREAM_ID_ENV_VAR);
    aeron_env_unset(AERON_ARCHIVE_RECORDING_EVENTS_CHANNEL_ENV_VAR);
    aeron_env_unset(AERON_ARCHIVE_RECORDING_EVENTS_STREAM_ID_ENV_VAR);
    aeron_env_unset(AERON_ARCHIVE_MESSAGE_TIMEOUT_ENV_VAR);
    aeron_env_unset(AERON_ARCHIVE_CONTROL_TERM_BUFFER_LENGTH_ENV_VAR);
    aeron_env_unset(AERON_ARCHIVE_CONTROL_TERM_BUFFER_SPARSE_ENV_VAR);
    aeron_env_unset(AERON_ARCHIVE_CONTROL_MTU_LENGTH_ENV_VAR);

    EXPECT_EQ(nullptr, m_ctx->aeron);
    EXPECT_STREQ(aeron_dir, m_ctx->aeron_directory_name);
    EXPECT_FALSE(m_ctx->owns_aeron_client);

    EXPECT_STREQ(control_channel, m_ctx->control_request_channel);
    EXPECT_EQ(-4321, m_ctx->control_request_stream_id);

    EXPECT_STREQ(response_channel, m_ctx->control_response_channel);
    EXPECT_EQ(2009, m_ctx->control_response_stream_id);

    EXPECT_STREQ(recording_events_channel, m_ctx->recording_events_channel);
    EXPECT_EQ(INT32_MAX, m_ctx->recording_events_stream_id);

    EXPECT_EQ(9223372036000000000UL, m_ctx->message_timeout_ns);

    EXPECT_EQ(128 * 1024, m_ctx->control_term_buffer_length);
    EXPECT_EQ(false, m_ctx->control_term_buffer_sparse);
    EXPECT_EQ(8192, m_ctx->control_mtu_length);

    EXPECT_EQ(0, aeron_archive_context_close(m_ctx));
}

TEST_F(AeronCArchiveIdTest, shouldFailWithErrorIfControlRequestChannelIsNotDefined)
{
    ASSERT_EQ_ERR(0, aeron_archive_context_init(&m_ctx));
    ASSERT_EQ_ERR(-1, aeron_archive_context_conclude(m_ctx));

    EXPECT_EQ(EINVAL, aeron_errcode());
    EXPECT_NE(std::string::npos, std::string(aeron_errmsg()).find("control request channel is required"));

    EXPECT_EQ(0, aeron_archive_context_close(m_ctx));
}

TEST_F(AeronCArchiveIdTest, shouldFailWithErrorIfControlResponseChannelIsNotDefined)
{
    ASSERT_EQ_ERR(0, aeron_archive_context_init(&m_ctx));

    aeron_archive_context_set_control_request_channel(m_ctx, "aeron:ipc");
    ASSERT_EQ_ERR(-1, aeron_archive_context_conclude(m_ctx));

    EXPECT_EQ(EINVAL, aeron_errcode());
    EXPECT_NE(std::string::npos, std::string(aeron_errmsg()).find("control response channel is required"));

    EXPECT_EQ(0, aeron_archive_context_close(m_ctx));
}

TEST_F(AeronCArchiveIdTest, shouldFailWithErrorIfAeronClientFailsToConnect)
{
    ASSERT_EQ_ERR(0, aeron_archive_context_init(&m_ctx));

    aeron_archive_context_set_control_request_channel(m_ctx, "aeron:ipc");
    aeron_archive_context_set_control_response_channel(m_ctx, "aeron:ipc");
    aeron_env_set(AERON_CLIENT_NAME_ENV_VAR, std::string("super very long client name").append(100, 'x').c_str());
    ASSERT_EQ_ERR(-1, aeron_archive_context_conclude(m_ctx));
    aeron_env_unset(AERON_CLIENT_NAME_ENV_VAR);

    EXPECT_EQ(EINVAL, aeron_errcode());
    EXPECT_NE(std::string::npos, std::string(aeron_errmsg()).find("client_name length must <= 100"));

    EXPECT_EQ(0, aeron_archive_context_close(m_ctx));
}

TEST_F(AeronCArchiveIdTest, shouldApplyDefaultParametersToRequestAndResponseChannels)
{
    ASSERT_EQ_ERR(0, aeron_archive_context_init(&m_ctx));

    aeron_archive_context_set_control_request_channel(m_ctx, "aeron:ipc");
    aeron_archive_context_set_control_response_channel(m_ctx, "aeron:udp?endpoint=127.0.0.1:0");
    aeron_t aeron = {};
    aeron.conductor.control_protocol_version = 0;
    const size_t buffer_capacity = 128 + AERON_RB_TRAILER_LENGTH;
    auto *buffer = new uint8_t[buffer_capacity];
    ASSERT_EQ_ERR(0, aeron_mpsc_rb_init(&aeron.conductor.to_driver_buffer, buffer, buffer_capacity));
    aeron_archive_context_set_aeron(m_ctx, &aeron);
    aeron_archive_context_set_error_handler(m_ctx, error_handler, nullptr);
    aeron_archive_context_set_control_term_buffer_length(m_ctx, 256 * 1024);
    aeron_archive_context_set_control_mtu_length(m_ctx, 2048);
    aeron_archive_context_set_control_term_buffer_sparse(m_ctx, false);
    ASSERT_EQ_ERR(0, aeron_archive_context_conclude(m_ctx));

    aeron_uri_string_builder_t request_channel;
    EXPECT_EQ(0, aeron_uri_string_builder_init_on_string(
        &request_channel,
        aeron_archive_context_get_control_request_channel(m_ctx)));
    EXPECT_STREQ("262144", aeron_uri_string_builder_get(&request_channel, AERON_URI_TERM_LENGTH_KEY));
    EXPECT_STREQ("2048", aeron_uri_string_builder_get(&request_channel, AERON_URI_MTU_LENGTH_KEY));
    EXPECT_STREQ("false", aeron_uri_string_builder_get(&request_channel, AERON_URI_SPARSE_TERM_KEY));
    EXPECT_STRNE("", aeron_uri_string_builder_get(&request_channel, AERON_URI_SESSION_ID_KEY));

    aeron_uri_string_builder_t response_channel;
    EXPECT_EQ(0, aeron_uri_string_builder_init_on_string(
        &response_channel,
        aeron_archive_context_get_control_response_channel(m_ctx)));
    EXPECT_STREQ("262144", aeron_uri_string_builder_get(&response_channel, AERON_URI_TERM_LENGTH_KEY));
    EXPECT_STREQ("2048", aeron_uri_string_builder_get(&response_channel, AERON_URI_MTU_LENGTH_KEY));
    EXPECT_STREQ("false", aeron_uri_string_builder_get(&response_channel, AERON_URI_SPARSE_TERM_KEY));
    EXPECT_STREQ("127.0.0.1:0", aeron_uri_string_builder_get(&response_channel, AERON_UDP_CHANNEL_ENDPOINT_KEY));
    EXPECT_STRNE("", aeron_uri_string_builder_get(&response_channel, AERON_URI_SESSION_ID_KEY));

    EXPECT_STREQ(aeron_uri_string_builder_get(&request_channel, AERON_URI_SESSION_ID_KEY), aeron_uri_string_builder_get(&response_channel, AERON_URI_SESSION_ID_KEY));
    EXPECT_EQ(0, aeron_uri_string_builder_close(&request_channel));
    EXPECT_EQ(0, aeron_uri_string_builder_close(&response_channel));

    EXPECT_EQ(0, aeron_archive_context_close(m_ctx));
    delete[] buffer;
}

TEST_F(AeronCArchiveIdTest, shouldNotApplyDefaultParametersToRequestAndResponseChannelsIfTheyAreSetExplicitly)
{
    ASSERT_EQ_ERR(0, aeron_archive_context_init(&m_ctx));

    aeron_archive_context_set_control_request_channel(m_ctx, "aeron:udp?endpoint=localhost:8080|term-length=64k|mtu=1408|sparse=true|session-id=0|ttl=3|interface=127.0.0.1");
    aeron_archive_context_set_control_response_channel(m_ctx, "aeron:ipc?term-length=128k|mtu=4096|sparse=true|alias=response");
    aeron_t aeron = {};
    aeron.conductor.control_protocol_version = 0;
    const size_t buffer_capacity = 128 + AERON_RB_TRAILER_LENGTH;
    auto *buffer = new uint8_t[buffer_capacity];
    ASSERT_EQ_ERR(0, aeron_mpsc_rb_init(&aeron.conductor.to_driver_buffer, buffer, buffer_capacity));
    aeron_archive_context_set_aeron(m_ctx, &aeron);
    aeron_archive_context_set_error_handler(m_ctx, error_handler, nullptr);
    aeron_archive_context_set_control_term_buffer_length(m_ctx, 256 * 1024);
    aeron_archive_context_set_control_mtu_length(m_ctx, 2048);
    aeron_archive_context_set_control_term_buffer_sparse(m_ctx, false);
    ASSERT_EQ_ERR(0, aeron_archive_context_conclude(m_ctx));

    aeron_uri_string_builder_t request_channel;
    EXPECT_EQ(0, aeron_uri_string_builder_init_on_string(
        &request_channel,
        aeron_archive_context_get_control_request_channel(m_ctx)));
    EXPECT_STREQ("64k", aeron_uri_string_builder_get(&request_channel, AERON_URI_TERM_LENGTH_KEY));
    EXPECT_STREQ("1408", aeron_uri_string_builder_get(&request_channel, AERON_URI_MTU_LENGTH_KEY));
    EXPECT_STREQ("true", aeron_uri_string_builder_get(&request_channel, AERON_URI_SPARSE_TERM_KEY));
    EXPECT_STREQ("3", aeron_uri_string_builder_get(&request_channel, AERON_UDP_CHANNEL_TTL_KEY));
    EXPECT_STREQ("127.0.0.1", aeron_uri_string_builder_get(&request_channel, AERON_UDP_CHANNEL_INTERFACE_KEY));
    EXPECT_STREQ("udp", aeron_uri_string_builder_get(&request_channel, AERON_URI_STRING_BUILDER_MEDIA_KEY));
    const auto session_id = aeron_uri_string_builder_get(&request_channel, AERON_URI_SESSION_ID_KEY);
    EXPECT_NE(nullptr, session_id);
    EXPECT_STRNE("", session_id);

    aeron_uri_string_builder_t response_channel;
    EXPECT_EQ(0, aeron_uri_string_builder_init_on_string(
        &response_channel,
        aeron_archive_context_get_control_response_channel(m_ctx)));
    EXPECT_STREQ("128k", aeron_uri_string_builder_get(&response_channel, AERON_URI_TERM_LENGTH_KEY));
    EXPECT_STREQ("4096", aeron_uri_string_builder_get(&response_channel, AERON_URI_MTU_LENGTH_KEY));
    EXPECT_STREQ("true", aeron_uri_string_builder_get(&response_channel, AERON_URI_SPARSE_TERM_KEY));
    EXPECT_STREQ("response", aeron_uri_string_builder_get(&response_channel, AERON_URI_ALIAS_KEY));
    EXPECT_STREQ("ipc", aeron_uri_string_builder_get(&response_channel, AERON_URI_STRING_BUILDER_MEDIA_KEY));
    EXPECT_STRNE("", aeron_uri_string_builder_get(&response_channel, AERON_URI_SESSION_ID_KEY));

    EXPECT_STREQ(session_id, aeron_uri_string_builder_get(&response_channel, AERON_URI_SESSION_ID_KEY));

    EXPECT_EQ(0, aeron_uri_string_builder_close(&request_channel));
    EXPECT_EQ(0, aeron_uri_string_builder_close(&response_channel));

    EXPECT_EQ(0, aeron_archive_context_close(m_ctx));
    delete[] buffer;
}

TEST_F(AeronCArchiveIdTest, shouldNotSetSessionIdOnControlRequestAndReponseChannelsIfControlModeResponseIsUsed)
{
    ASSERT_EQ_ERR(0, aeron_archive_context_init(&m_ctx));

    aeron_archive_context_set_control_request_channel(m_ctx, "aeron:udp?endpoint=localhost:8080");
    aeron_archive_context_set_control_response_channel(m_ctx, "aeron:udp?control=localhost:9090|control-mode=response");
    aeron_t aeron = {};
    aeron.conductor.control_protocol_version = 0;
    const size_t buffer_capacity = 128 + AERON_RB_TRAILER_LENGTH;
    auto *buffer = new uint8_t[buffer_capacity];
    ASSERT_EQ_ERR(0, aeron_mpsc_rb_init(&aeron.conductor.to_driver_buffer, buffer, buffer_capacity));
    aeron_archive_context_set_aeron(m_ctx, &aeron);
    aeron_archive_context_set_error_handler(m_ctx, error_handler, nullptr);
    aeron_archive_context_set_control_term_buffer_length(m_ctx, 256 * 1024);
    aeron_archive_context_set_control_mtu_length(m_ctx, 2048);
    aeron_archive_context_set_control_term_buffer_sparse(m_ctx, false);
    ASSERT_EQ_ERR(0, aeron_archive_context_conclude(m_ctx));

    aeron_uri_string_builder_t request_channel;
    EXPECT_EQ(0, aeron_uri_string_builder_init_on_string(
        &request_channel,
        aeron_archive_context_get_control_request_channel(m_ctx)));
    EXPECT_STREQ("localhost:8080", aeron_uri_string_builder_get(&request_channel, AERON_UDP_CHANNEL_ENDPOINT_KEY));
    EXPECT_EQ(nullptr, aeron_uri_string_builder_get(&request_channel, AERON_URI_SESSION_ID_KEY));

    aeron_uri_string_builder_t response_channel;
    EXPECT_EQ(0, aeron_uri_string_builder_init_on_string(
        &response_channel,
        aeron_archive_context_get_control_response_channel(m_ctx)));
    EXPECT_EQ(nullptr, aeron_uri_string_builder_get(&response_channel, AERON_UDP_CHANNEL_ENDPOINT_KEY));
    EXPECT_STREQ("localhost:9090", aeron_uri_string_builder_get(&response_channel, AERON_UDP_CHANNEL_CONTROL_KEY));
    EXPECT_STREQ(AERON_UDP_CHANNEL_CONTROL_MODE_RESPONSE_VALUE, aeron_uri_string_builder_get(&response_channel, AERON_UDP_CHANNEL_CONTROL_MODE_KEY));
    EXPECT_EQ(nullptr, aeron_uri_string_builder_get(&response_channel, AERON_URI_SESSION_ID_KEY));
    EXPECT_EQ(0, aeron_uri_string_builder_close(&request_channel));
    EXPECT_EQ(0, aeron_uri_string_builder_close(&response_channel));

    EXPECT_EQ(0, aeron_archive_context_close(m_ctx));
    delete[] buffer;
}

TEST_F(AeronCArchiveIdTest, shouldDuplicateContext)
{
    ASSERT_EQ_ERR(0, aeron_archive_context_init(&m_ctx));

    aeron_archive_context_set_control_request_channel(m_ctx, "aeron:udp?endpoint=localhost:8080");
    aeron_archive_context_set_control_request_stream_id(m_ctx, 42);
    aeron_archive_context_set_control_response_channel(m_ctx, "aeron:udp?endpoint=localhost:0");
    aeron_archive_context_set_control_response_stream_id(m_ctx, -5);
    aeron_archive_context_set_recording_events_channel(m_ctx, nullptr);
    aeron_archive_context_set_recording_events_stream_id(m_ctx, 777);
    aeron_archive_context_set_control_term_buffer_length(m_ctx, 256 * 1024);
    aeron_archive_context_set_control_mtu_length(m_ctx, 2048);
    aeron_archive_context_set_control_term_buffer_sparse(m_ctx, false);
    aeron_archive_context_set_message_timeout_ns(m_ctx, 1000000000);
    aeron_t aeron = {};
    aeron.conductor.control_protocol_version = 0;
    const size_t buffer_capacity = 128 + AERON_RB_TRAILER_LENGTH;
    auto *buffer = new uint8_t[buffer_capacity];
    ASSERT_EQ_ERR(0, aeron_mpsc_rb_init(&aeron.conductor.to_driver_buffer, buffer, buffer_capacity));
    aeron_archive_context_set_aeron(m_ctx, &aeron);
    aeron_archive_context_set_error_handler(m_ctx, error_handler, nullptr);
    aeron_archive_context_set_idle_strategy(m_ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns);

    aeron_archive_context_t *copy_ctx;
    ASSERT_EQ_ERR(0, aeron_archive_context_duplicate(&copy_ctx, m_ctx));

    EXPECT_NE(nullptr, copy_ctx);
    EXPECT_NE(m_ctx, copy_ctx);
    EXPECT_EQ(m_ctx->aeron, copy_ctx->aeron);
    EXPECT_EQ(m_ctx->owns_aeron_client, copy_ctx->owns_aeron_client);
    EXPECT_NE(m_ctx->control_request_channel, copy_ctx->control_request_channel);
    EXPECT_STREQ(m_ctx->control_request_channel, copy_ctx->control_request_channel);
    EXPECT_EQ(m_ctx->control_request_channel_length, copy_ctx->control_request_channel_length);
    EXPECT_NE(m_ctx->control_response_channel, copy_ctx->control_response_channel);
    EXPECT_STREQ(m_ctx->control_response_channel, copy_ctx->control_response_channel);
    EXPECT_EQ(m_ctx->control_response_channel_length, copy_ctx->control_response_channel_length);
    EXPECT_EQ(m_ctx->recording_events_channel, copy_ctx->recording_events_channel);
    EXPECT_EQ(m_ctx->recording_events_channel_length, copy_ctx->recording_events_channel_length);
    EXPECT_EQ(m_ctx->message_timeout_ns, copy_ctx->message_timeout_ns);
    EXPECT_EQ(m_ctx->control_term_buffer_sparse, copy_ctx->control_term_buffer_sparse);
    EXPECT_EQ(m_ctx->control_term_buffer_length, copy_ctx->control_term_buffer_length);
    EXPECT_EQ(m_ctx->control_mtu_length, copy_ctx->control_mtu_length);
    EXPECT_EQ(m_ctx->error_handler, copy_ctx->error_handler);
    EXPECT_EQ(m_ctx->error_handler_clientd, copy_ctx->error_handler_clientd);
    EXPECT_EQ(m_ctx->idle_strategy_func, copy_ctx->idle_strategy_func);
    EXPECT_EQ(m_ctx->idle_strategy_state, copy_ctx->idle_strategy_state);
    EXPECT_EQ(m_ctx->delegating_invoker_func, copy_ctx->delegating_invoker_func);
    EXPECT_EQ(m_ctx->delegating_invoker_func_clientd, copy_ctx->delegating_invoker_func_clientd);
    EXPECT_EQ(m_ctx->on_recording_signal, copy_ctx->on_recording_signal);
    EXPECT_EQ(m_ctx->on_recording_signal_clientd, copy_ctx->on_recording_signal_clientd);

    EXPECT_EQ(0, aeron_archive_context_close(m_ctx));
    EXPECT_EQ(0, aeron_archive_context_close(copy_ctx));
    delete[] buffer;
}

TEST_F(AeronCArchiveIdTest, shouldResolveArchiveId)
{
    std::int64_t archiveId = 0x4236483BEEF;
    DoSetUp(archiveId);

    connect();

    aeron_subscription_t *subscription = aeron_archive_get_control_response_subscription(m_archive);
    EXPECT_TRUE(aeron_subscription_is_connected(subscription));
    EXPECT_EQ(archiveId, aeron_archive_get_archive_id(m_archive));

    DoTearDown();
}

TEST_F(AeronCArchiveTest, shouldConnectToArchiveWithResponseChannels)
{
    ASSERT_EQ_ERR(0, aeron_archive_context_init(&m_ctx));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_request_channel(m_ctx, "aeron:udp?endpoint=localhost:8010"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_response_channel(
        m_ctx,
        "aeron:udp?control-mode=response|control=localhost:10002"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_idle_strategy(
        m_ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
        m_ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds_clientd));
    ASSERT_EQ_ERR(0, aeron_archive_connect(&m_archive, m_ctx));

    aeron_subscription_t *subscription = aeron_archive_get_control_response_subscription(m_archive);
    EXPECT_TRUE(aeron_subscription_is_connected(subscription));
}

TEST_P(AeronCArchiveParamTest, shouldReplayWithResponseChannel)
{
    bool tryStop = GetParam();

    size_t message_count = 1000;
    const char *response_channel = "aeron:udp?control-mode=response|control=localhost:10002";

    ASSERT_EQ_ERR(0, aeron_archive_context_init(&m_ctx));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_request_channel(m_ctx, "aeron:udp?endpoint=localhost:8010"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_response_channel(
        m_ctx,
        response_channel));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_idle_strategy(
        m_ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
        m_ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds_clientd));
    ASSERT_EQ_ERR(0, aeron_archive_connect(&m_archive, m_ctx));

    m_aeron = aeron_archive_context_get_aeron(m_ctx);

    int64_t recording_id, stop_position, halfway_position;

    recordData(tryStop, &recording_id, &stop_position, &halfway_position, message_count);

    int64_t position = 0L;
    int64_t length = stop_position - position;

    aeron_archive_replay_params_t replay_params;
    aeron_archive_replay_params_init(&replay_params);

    replay_params.position = position;
    replay_params.length = length;
    replay_params.file_io_max_length = 4096;

    aeron_subscription_t *subscription;

    ASSERT_EQ_ERR(0, aeron_archive_replay(
        &subscription,
        m_archive,
        recording_id,
        response_channel,
        m_replayStreamId,
        &replay_params));

    consumeMessages(subscription, message_count);

    aeron_image_t *image = aeron_subscription_image_at_index(subscription, 0);
    EXPECT_EQ(stop_position, aeron_image_position(image));
}

TEST_P(AeronCArchiveParamTest, shouldBoundedReplayWithResponseChannel)
{
    bool tryStop = GetParam();

    size_t message_count = 1000;
    const char *response_channel = "aeron:udp?control-mode=response|control=localhost:10002";
    const std::int64_t key = 1234567890;

    ASSERT_EQ_ERR(0, aeron_archive_context_init(&m_ctx));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_request_channel(m_ctx, "aeron:udp?endpoint=localhost:8010"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_response_channel(
        m_ctx,
        response_channel));
    ASSERT_EQ_ERR(0,
        aeron_archive_context_set_idle_strategy(m_ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
        m_ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds_clientd));
    ASSERT_EQ_ERR(0, aeron_archive_connect(&m_archive, m_ctx));

    m_aeron = aeron_archive_context_get_aeron(m_ctx);

    int64_t recording_id, stop_position, halfway_position;

    recordData(tryStop, &recording_id, &stop_position, &halfway_position, message_count);

    const char *counter_name = "test bounded counter";
    aeron_async_add_counter_t *async_add_counter;
    ASSERT_EQ_ERR(0, aeron_async_add_counter(
        &async_add_counter,
        m_aeron,
        10001,
        (const uint8_t *)&key,
        sizeof(key),
        counter_name,
        strlen(counter_name)));

    aeron_counter_t *counter = nullptr;
    aeron_async_add_counter_poll(&counter, async_add_counter);
    while (nullptr == counter)
    {
        idle();
        aeron_async_add_counter_poll(&counter, async_add_counter);
    }

    aeron_counter_set_release(aeron_counter_addr(counter), halfway_position);

    int64_t position = 0L;
    int64_t length = stop_position - position;

    aeron_counter_constants_t counter_constants;
    aeron_counter_constants(counter, &counter_constants);

    aeron_archive_replay_params_t replay_params;
    aeron_archive_replay_params_init(&replay_params);

    replay_params.position = position;
    replay_params.length = length;
    replay_params.file_io_max_length = 4096;
    replay_params.bounding_limit_counter_id = counter_constants.counter_id;

    aeron_subscription_t *subscription;

    ASSERT_EQ_ERR(0, aeron_archive_replay(
        &subscription,
        m_archive,
        recording_id,
        response_channel,
        m_replayStreamId,
        &replay_params));

    consumeMessages(subscription, message_count / 2);

    aeron_image_t *image = aeron_subscription_image_at_index(subscription, 0);
    EXPECT_EQ(halfway_position, aeron_image_position(image));
}

TEST_P(AeronCArchiveParamTest, shouldStartReplayWithResponseChannel)
{
    bool tryStop = GetParam();

    size_t message_count = 1000;
    const char *response_channel = "aeron:udp?control-mode=response|control=localhost:10003";

    ASSERT_EQ_ERR(0, aeron_archive_context_init(&m_ctx));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_request_channel(m_ctx, "aeron:udp?endpoint=localhost:8010"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_response_channel(
        m_ctx,
        response_channel));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_idle_strategy(
        m_ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
        m_ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds_clientd));
    ASSERT_EQ_ERR(0, aeron_archive_connect(&m_archive, m_ctx));

    m_aeron = aeron_archive_context_get_aeron(m_ctx);

    int64_t recording_id, stop_position, halfway_position;

    recordData(tryStop, &recording_id, &stop_position, &halfway_position, message_count);

    aeron_subscription_t *subscription = addSubscription(response_channel, m_replayStreamId);

    int64_t position = 0L;
    int64_t length = stop_position - position;

    aeron_subscription_constants_t subscription_constants;
    aeron_subscription_constants(subscription, &subscription_constants);

    aeron_archive_replay_params_t replay_params;
    aeron_archive_replay_params_init(&replay_params);

    replay_params.position = position;
    replay_params.length = length;
    replay_params.file_io_max_length = 4096;
    replay_params.subscription_registration_id = subscription_constants.registration_id;

    ASSERT_EQ_ERR(0, aeron_archive_start_replay(
        nullptr,
        m_archive,
        recording_id,
        response_channel,
        m_replayStreamId,
        &replay_params));

    consumeMessages(subscription, message_count);

    aeron_image_t *image = aeron_subscription_image_at_index(subscription, 0);
    EXPECT_EQ(stop_position, aeron_image_position(image));
}

TEST_P(AeronCArchiveParamTest, shouldStartBoundedReplayWithResponseChannel)
{
    bool tryStop = GetParam();

    size_t message_count = 1000;
    const char *response_channel = "aeron:udp?control-mode=response|control=localhost:10002";
    const std::int64_t key = 1234567890;

    ASSERT_EQ_ERR(0, aeron_archive_context_init(&m_ctx));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_request_channel(m_ctx, "aeron:udp?endpoint=localhost:8010"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_response_channel(
        m_ctx,
        response_channel));
    ASSERT_EQ_ERR(0,
        aeron_archive_context_set_idle_strategy(m_ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
        m_ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds_clientd));
    ASSERT_EQ_ERR(0, aeron_archive_connect(&m_archive, m_ctx));

    m_aeron = aeron_archive_context_get_aeron(m_ctx);

    int64_t recording_id, stop_position, halfway_position;

    recordData(tryStop, &recording_id, &stop_position, &halfway_position, message_count);

    const char *counter_name = "test bounded counter";
    aeron_async_add_counter_t *async_add_counter;
    ASSERT_EQ_ERR(0, aeron_async_add_counter(
        &async_add_counter,
        m_aeron,
        10001,
        (const uint8_t *)&key,
        sizeof(key),
        counter_name,
        strlen(counter_name)));

    aeron_counter_t *counter = nullptr;
    aeron_async_add_counter_poll(&counter, async_add_counter);
    while (nullptr == counter)
    {
        idle();
        aeron_async_add_counter_poll(&counter, async_add_counter);
    }

    aeron_counter_set_release(aeron_counter_addr(counter), halfway_position);

    aeron_subscription_t *subscription = addSubscription(response_channel, m_replayStreamId);

    int64_t position = 0L;
    int64_t length = stop_position - position;

    aeron_counter_constants_t counter_constants;
    aeron_counter_constants(counter, &counter_constants);

    aeron_subscription_constants_t subscription_constants;
    aeron_subscription_constants(subscription, &subscription_constants);

    aeron_archive_replay_params_t replay_params;
    aeron_archive_replay_params_init(&replay_params);

    replay_params.position = position;
    replay_params.length = length;
    replay_params.file_io_max_length = 4096;
    replay_params.bounding_limit_counter_id = counter_constants.counter_id;
    replay_params.subscription_registration_id = subscription_constants.registration_id;

    ASSERT_EQ_ERR(0, aeron_archive_start_replay(
        nullptr,
        m_archive,
        recording_id,
        response_channel,
        m_replayStreamId,
        &replay_params));

    consumeMessages(subscription, message_count / 2);

    aeron_image_t *image = aeron_subscription_image_at_index(subscription, 0);
    EXPECT_EQ(halfway_position, aeron_image_position(image));
}

TEST_F(AeronCArchiveTest, shouldDisconnectAfterStopAllReplays)
{
    const char *response_channel = "aeron:udp?control-mode=response|control=localhost:10002";

    ASSERT_EQ_ERR(0, aeron_archive_context_init(&m_ctx));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_request_channel(m_ctx, "aeron:udp?endpoint=localhost:8010"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_response_channel(
        m_ctx,
        response_channel));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_idle_strategy(
        m_ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
        m_ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds_clientd));
    ASSERT_EQ_ERR(0, aeron_archive_connect(&m_archive, m_ctx));

    m_aeron = aeron_archive_context_get_aeron(m_ctx);

    addSubscription(m_recordingChannel, m_recordingStreamId);

    aeron_publication_t *publication;
    ASSERT_EQ_ERR(0, aeron_archive_add_recorded_publication(
        &publication,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId));

    int32_t session_id = aeron_publication_session_id(publication);

    setupCounters(session_id);

    offerMessages(publication);

    int64_t stop_position = aeron_publication_position(publication);

    waitUntilCaughtUp(stop_position);

    aeron_archive_replay_params_t replay_params;
    aeron_archive_replay_params_init(&replay_params);

    replay_params.position = 0L;
    replay_params.file_io_max_length = 4096;

    aeron_subscription_t *subscription;

    ASSERT_EQ_ERR(0, aeron_archive_replay(
        &subscription,
        m_archive,
        m_recording_id_from_counter,
        response_channel,
        m_replayStreamId,
        &replay_params));

    consumeMessages(subscription);

    aeron_image_t *image = aeron_subscription_image_at_index(subscription, 0);
    EXPECT_EQ(stop_position, aeron_image_position(image));

    ASSERT_EQ_ERR(0, aeron_archive_stop_all_replays(
        m_archive,
        m_recording_id_from_counter));

    while (aeron_subscription_is_connected(subscription))
    {
        idle();
    }
}

TEST_P(AeronCArchiveParamTest, shouldRecordAndExtend)
{
    bool tryStop = GetParam();

    connect();

    {
        aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);

        aeron_publication_t *publication;
        ASSERT_EQ_ERR(0, aeron_archive_add_recorded_publication(
            &publication,
            m_archive,
            m_recordingChannel.c_str(),
            m_recordingStreamId));

        int32_t session_id = aeron_publication_session_id(publication);

        setupCounters(session_id);

        offerMessages(publication);
        consumeMessages(subscription);

        int64_t stop_position = aeron_publication_position(publication);

        waitUntilCaughtUp(stop_position);

        ASSERT_EQ_ERR(0, aeron_archive_stop_recording_publication(m_archive, publication));

        ASSERT_EQ_ERR(0, aeron_subscription_close(subscription, nullptr, nullptr));
        ASSERT_EQ_ERR(0, aeron_publication_close(publication, nullptr, nullptr));
    }

    recording_descriptor_consumer_clientd_t clientd;

    int32_t count;
    ASSERT_EQ_ERR(0, aeron_archive_list_recording(
        &count,
        m_archive,
        m_recording_id_from_counter,
        recording_descriptor_consumer,
        &clientd));
    EXPECT_EQ(1, count);

    char recordingChannel2[AERON_URI_MAX_LENGTH];

    aeron_uri_string_builder_t builder;
    ASSERT_EQ_ERR(0, aeron_uri_string_builder_init_on_string(&builder, "aeron:udp?endpoint=localhost:3332"));
    ASSERT_EQ_ERR(0, aeron_uri_string_builder_set_initial_position(
        &builder,
        clientd.last_descriptor.stop_position,
        clientd.last_descriptor.initial_term_id,
        clientd.last_descriptor.term_buffer_length));
    ASSERT_EQ_ERR(0, aeron_uri_string_builder_sprint(&builder, recordingChannel2, sizeof(recordingChannel2)));
    ASSERT_EQ_ERR(0, aeron_uri_string_builder_close(&builder));

    {
        aeron_subscription_t *subscription = addSubscription(recordingChannel2, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(recordingChannel2, m_recordingStreamId);

        int32_t session_id = aeron_publication_session_id(publication);

        int64_t subscription_id;
        ASSERT_EQ_ERR(0, aeron_archive_extend_recording(
            &subscription_id,
            m_archive,
            m_recording_id_from_counter,
            recordingChannel2,
            m_recordingStreamId,
            AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
            false));

        setupCounters(session_id);

        offerMessages(publication);
        consumeMessages(subscription);

        int64_t stop_position = aeron_publication_position(publication);

        waitUntilCaughtUp(stop_position);

        if (tryStop)
        {
            bool stopped;
            ASSERT_EQ_ERR(0, aeron_archive_try_stop_recording_channel_and_stream(
                &stopped, m_archive, recordingChannel2, m_recordingStreamId));
            EXPECT_TRUE(stopped);
        }
        else
        {
            ASSERT_EQ_ERR(0, aeron_archive_stop_recording_channel_and_stream(
                m_archive, recordingChannel2, m_recordingStreamId));
        }

        ASSERT_EQ_ERR(0, aeron_subscription_close(subscription, nullptr, nullptr));
        ASSERT_EQ_ERR(0, aeron_publication_close(publication, nullptr, nullptr));
    }

    ASSERT_EQ_ERR(0, aeron_archive_list_recording(
        &count,
        m_archive,
        m_recording_id_from_counter,
        recording_descriptor_consumer,
        &clientd));
    EXPECT_EQ(1, count);

    aeron_archive_replay_params_t replay_params;
    aeron_archive_replay_params_init(&replay_params);

    replay_params.position = clientd.last_descriptor.start_position;
    replay_params.file_io_max_length = 4096;

    aeron_subscription_t *replay_subscription;

    ASSERT_EQ_ERR(0, aeron_archive_replay(
        &replay_subscription,
        m_archive,
        m_recording_id_from_counter,
        m_replayChannel.c_str(),
        m_replayStreamId,
        &replay_params));

    consumeMessages(replay_subscription, 20);

    aeron_image_t *image = aeron_subscription_image_at_index(replay_subscription, 0);
    ASSERT_EQ(clientd.last_descriptor.stop_position, aeron_image_position(image));
}

#define TERM_LENGTH AERON_LOGBUFFER_TERM_MIN_LENGTH
#define SEGMENT_LENGTH (TERM_LENGTH * 2)
#define MTU_LENGTH 1024

TEST_F(AeronCArchiveTest, shouldPurgeSegments)
{
    int32_t session_id;
    int64_t stop_position;

    recording_signal_consumer_clientd_t rsc_cd;

    connect(&rsc_cd);

    char publication_channel[AERON_URI_MAX_LENGTH];
    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_new(&builder);

        aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_ENDPOINT_KEY, "localhost:3333");
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_TERM_LENGTH_KEY, TERM_LENGTH);
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_MTU_LENGTH_KEY, MTU_LENGTH);

        aeron_uri_string_builder_sprint(&builder, publication_channel, sizeof(publication_channel));
        aeron_uri_string_builder_close(&builder);
    }

    aeron_publication_t *publication;
    ASSERT_EQ_ERR(0, aeron_archive_add_recorded_publication(
        &publication,
        m_archive,
        publication_channel,
        m_recordingStreamId));

    session_id = aeron_publication_session_id(publication);

    setupCounters(session_id);

    int64_t targetPosition = (SEGMENT_LENGTH * 3L) + 1;
    offerMessagesToPosition(publication, targetPosition);

    stop_position = aeron_publication_position(publication);

    waitUntilCaughtUp(stop_position);

    int64_t start_position = 0;
    int64_t segment_file_base_position = aeron_archive_segment_file_base_position(
        start_position,
        SEGMENT_LENGTH * 2L,
        TERM_LENGTH,
        SEGMENT_LENGTH);

    int64_t count;
    ASSERT_EQ_ERR(0, aeron_archive_purge_segments(
        &count,
        m_archive,
        m_recording_id_from_counter,
        segment_file_base_position));

    while (0 == rsc_cd.signals.count(AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_DELETE))
    {
        aeron_archive_poll_for_recording_signals(nullptr, m_archive);

        idle();
    }

    ASSERT_EQ(2, count);

    ASSERT_EQ_ERR(0, aeron_archive_get_start_position(&start_position, m_archive, m_recording_id_from_counter));
    ASSERT_EQ(start_position, segment_file_base_position);
}

TEST_F(AeronCArchiveTest, shouldDetachAndDeleteSegments)
{
    int32_t session_id;
    int64_t stop_position;

    recording_signal_consumer_clientd_t rsc_cd;

    connect(&rsc_cd);

    char publication_channel[AERON_URI_MAX_LENGTH];
    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_new(&builder);

        aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_ENDPOINT_KEY, "localhost:3333");
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_TERM_LENGTH_KEY, TERM_LENGTH);
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_MTU_LENGTH_KEY, MTU_LENGTH);

        aeron_uri_string_builder_sprint(&builder, publication_channel, sizeof(publication_channel));
        aeron_uri_string_builder_close(&builder);
    }

    aeron_publication_t *publication;
    ASSERT_EQ_ERR(0, aeron_archive_add_recorded_publication(
        &publication,
        m_archive,
        publication_channel,
        m_recordingStreamId));

    session_id = aeron_publication_session_id(publication);

    setupCounters(session_id);

    int64_t targetPosition = (SEGMENT_LENGTH * 4L) + 1;
    offerMessagesToPosition(publication, targetPosition);

    stop_position = aeron_publication_position(publication);

    waitUntilCaughtUp(stop_position);

    int64_t start_position = 0;
    int64_t segment_file_base_position = aeron_archive_segment_file_base_position(
        start_position,
        SEGMENT_LENGTH * 3L,
        TERM_LENGTH,
        SEGMENT_LENGTH);

    ASSERT_EQ_ERR(0, aeron_archive_detach_segments(
        m_archive,
        m_recording_id_from_counter,
        segment_file_base_position));

    int64_t count;
    ASSERT_EQ_ERR(0, aeron_archive_delete_detached_segments(
        &count,
        m_archive,
        m_recording_id_from_counter));

    while (0 == rsc_cd.signals.count(AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_DELETE))
    {
        aeron_archive_poll_for_recording_signals(nullptr, m_archive);

        idle();
    }

    ASSERT_EQ(3, count);

    ASSERT_EQ_ERR(0, aeron_archive_get_start_position(&start_position, m_archive, m_recording_id_from_counter));
    ASSERT_EQ(start_position, segment_file_base_position);
}

TEST_F(AeronCArchiveTest, shouldDetachAndReattachSegments)
{
    int32_t session_id;
    int64_t stop_position;

    recording_signal_consumer_clientd_t rsc_cd;

    connect(&rsc_cd);

    char publication_channel[AERON_URI_MAX_LENGTH];
    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_new(&builder);

        aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_ENDPOINT_KEY, "localhost:3333");
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_TERM_LENGTH_KEY, TERM_LENGTH);
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_MTU_LENGTH_KEY, MTU_LENGTH);

        aeron_uri_string_builder_sprint(&builder, publication_channel, sizeof(publication_channel));
        aeron_uri_string_builder_close(&builder);
    }

    aeron_publication_t *publication;
    ASSERT_EQ_ERR(0, aeron_archive_add_recorded_publication(
        &publication,
        m_archive,
        publication_channel,
        m_recordingStreamId));

    session_id = aeron_publication_session_id(publication);

    setupCounters(session_id);

    int64_t targetPosition = (SEGMENT_LENGTH * 5L) + 1;
    offerMessagesToPosition(publication, targetPosition);

    stop_position = aeron_publication_position(publication);

    waitUntilCaughtUp(stop_position);

    int64_t start_position = 0;
    int64_t segment_file_base_position = aeron_archive_segment_file_base_position(
        start_position,
        SEGMENT_LENGTH * 4L,
        TERM_LENGTH,
        SEGMENT_LENGTH);

    ASSERT_EQ_ERR(0, aeron_archive_detach_segments(
        m_archive,
        m_recording_id_from_counter,
        segment_file_base_position));

    ASSERT_EQ_ERR(0, aeron_archive_get_start_position(&start_position, m_archive, m_recording_id_from_counter));
    ASSERT_EQ(start_position, segment_file_base_position);

    int64_t count;
    ASSERT_EQ_ERR(0, aeron_archive_attach_segments(
        &count,
        m_archive,
        m_recording_id_from_counter));

    ASSERT_EQ(4, count);

    ASSERT_EQ_ERR(0, aeron_archive_get_start_position(&start_position, m_archive, m_recording_id_from_counter));
    ASSERT_EQ(start_position, 0);
}

TEST_F(AeronCArchiveTest, shouldSetAeronClientName)
{
    connect(
        nullptr,
        "aeron:udp?endpoint=localhost:8010",
        "aeron:udp?control=localhost:9090|control-mode=response");

    auto aeron = aeron_archive_context_get_aeron(m_ctx);
    const auto client_name = std::string(aeron_context_get_client_name(aeron->context));
    EXPECT_NE(std::string::npos, client_name.find("archive-client"));
}

TEST_F(AeronCArchiveTest, shouldSendClientInfoToArchive)
{
    connect(
        nullptr,
        "aeron:udp?endpoint=localhost:8010",
        "aeron:udp?control=localhost:9090|control-mode=response",
        "my client");

    m_aeron = aeron_archive_context_get_aeron(m_ctx);
    m_counters_reader = aeron_counters_reader(m_aeron);

    int64_t controlSessionId = aeron_archive_control_session_id(m_archive);

    struct counter_data
    {
        int32_t id;
        size_t key_length;
        size_t label_length;
        uint8_t key[AERON_COUNTER_MAX_KEY_LENGTH];
        char label[AERON_COUNTER_MAX_LABEL_LENGTH];
    };
    counter_data counter = {};

    static constexpr int32_t controlSessionTypeId = AERON_COUNTER_ARCHIVE_CONTROL_SESSION_TYPE_ID;

    aeron_counters_reader_foreach_counter(
        m_counters_reader,
[](int64_t value,
        int32_t id,
        int32_t type_id,
        const uint8_t *key,
        size_t key_length,
        const char *label,
        size_t label_length,
        void *clientd)
    {
        if (controlSessionTypeId == type_id)
        {
            auto *data = static_cast<counter_data *>(clientd);
            data->id = id;
            data->key_length = key_length;
            data->label_length = label_length;
            memcpy(data->key, key, key_length);
            memcpy(data->label, label, label_length);
        }
    },
    &counter);

    ASSERT_NE(AERON_NULL_COUNTER_ID, counter.id);
    ASSERT_GE(counter.key_length, 2 * sizeof(int64_t));

    int64_t actualArchiveId, actualControlSessionId;
    memcpy(&actualArchiveId, counter.key, sizeof(int64_t));
    ASSERT_EQ(aeron_archive_get_archive_id(m_archive), actualArchiveId);
    memcpy(&actualControlSessionId, counter.key + sizeof(int64_t), sizeof(int64_t));
    ASSERT_EQ(controlSessionId, actualControlSessionId);

    const auto label = std::string(counter.label);
    const auto expected = std::string("name=")
        .append(aeron_archive_context_get_client_name(m_ctx))
        .append(" version=").append(aeron_archive_client_version_text())
        .append(" commit=").append(aeron_archive_client_version_git_sha());
    EXPECT_NE(std::string::npos, label.find(expected));
    EXPECT_NE(std::string::npos, label.find(std::string("archiveId=").append(std::to_string(actualArchiveId))));
}

TEST_F(AeronCArchiveTest, shouldUpdateChannel)
{
    int32_t session_id;
    int64_t stop_position;

    recording_signal_consumer_clientd_t rsc_cd;

    connect(&rsc_cd);

    char publication_channel[AERON_URI_MAX_LENGTH];
    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_new(&builder);

        aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_ENDPOINT_KEY, "localhost:3333");
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_TERM_LENGTH_KEY, TERM_LENGTH);
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_MTU_LENGTH_KEY, MTU_LENGTH);

        aeron_uri_string_builder_sprint(&builder, publication_channel, sizeof(publication_channel));
        aeron_uri_string_builder_close(&builder);
    }

    aeron_publication_t *publication;
    ASSERT_EQ_ERR(0, aeron_archive_add_recorded_publication(
            &publication,
            m_archive,
            publication_channel,
            m_recordingStreamId));

    session_id = aeron_publication_session_id(publication);

    setupCounters(session_id);

    int64_t targetPosition = TERM_LENGTH;
    offerMessagesToPosition(publication, targetPosition);
    stop_position = aeron_publication_position(publication);
    waitUntilCaughtUp(stop_position);

    const char *newChannel = "aeron:ipc?alias=test42|rejoin=false|mtu=8k|term-length=1m|pub-wnd=512k|ttl=6";
    ASSERT_EQ_ERR(0, aeron_archive_update_channel(m_archive, m_recording_id_from_counter, newChannel));

    recording_descriptor_consumer_clientd_t clientd;
    clientd.verify_recording_id = true;
    clientd.recording_id = m_recording_id_from_counter;
    clientd.verify_stream_id = true;
    clientd.stream_id = m_recordingStreamId;
    clientd.original_channel = newChannel;

    int32_t count;
    ASSERT_EQ_ERR(0, aeron_archive_list_recording(
            &count,
            m_archive,
            m_recording_id_from_counter,
            recording_descriptor_consumer,
            &clientd));
    EXPECT_EQ(1, count);

    aeron_uri_t uri = {};
    EXPECT_EQ(0, aeron_uri_parse(strlen(clientd.last_descriptor_stripped_channel), clientd.last_descriptor_stripped_channel, &uri));

    EXPECT_STREQ(nullptr, aeron_uri_find_param_value(&uri.params.ipc.additional_params, AERON_URI_PUBLICATION_WINDOW_KEY));
    EXPECT_STREQ(nullptr, aeron_uri_find_param_value(&uri.params.ipc.additional_params, AERON_URI_TERM_LENGTH_KEY));
    EXPECT_STREQ(nullptr, aeron_uri_find_param_value(&uri.params.ipc.additional_params, AERON_URI_MTU_LENGTH_KEY));
    EXPECT_STREQ("test42", aeron_uri_find_param_value(&uri.params.ipc.additional_params, AERON_URI_ALIAS_KEY));
    EXPECT_STREQ("6", aeron_uri_find_param_value(&uri.params.ipc.additional_params, AERON_UDP_CHANNEL_TTL_KEY));
    EXPECT_STREQ("false", aeron_uri_find_param_value(&uri.params.ipc.additional_params, AERON_URI_REJOIN_KEY));
    aeron_uri_close(&uri);
}

class AeronCArchiveNoSetupTest : public AeronCArchiveTestBase, public testing::Test
{
};

TEST_F(AeronCArchiveNoSetupTest, shouldNotHangClosingReplayMergeWhenUsingInvoker)
{
    m_invoke_on_idle = true;

    DoSetUp(42, 0);

    const std::size_t termLength = 64 * 1024;
    const std::string message_prefix = "Message ";
    const std::size_t min_messages_per_term = termLength / (message_prefix.length() + AERON_DATA_HEADER_LENGTH);
    const char *control_endpoint = "localhost:23265";
    const char *recording_endpoint = "localhost:23266";
    const char *live_endpoint = "localhost:23267";
    const char *replay_endpoint = "localhost:0";

    char publication_channel[AERON_URI_MAX_LENGTH];
    char live_destination[AERON_URI_MAX_LENGTH];
    char replay_destination[AERON_URI_MAX_LENGTH];
    char recording_channel[AERON_URI_MAX_LENGTH];
    char subscription_channel[AERON_URI_MAX_LENGTH];

    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_new(&builder);

        aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_CONTROL_KEY, control_endpoint);
        aeron_uri_string_builder_put(
            &builder, AERON_UDP_CHANNEL_CONTROL_MODE_KEY, AERON_UDP_CHANNEL_CONTROL_MODE_DYNAMIC_VALUE);
        aeron_uri_string_builder_put(&builder, AERON_URI_FC_KEY, "tagged,g:99901/1,t:5s");
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_TERM_LENGTH_KEY, termLength);

        aeron_uri_string_builder_sprint(&builder, publication_channel, sizeof(publication_channel));
        aeron_uri_string_builder_close(&builder);
    }

    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_new(&builder);

        aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_ENDPOINT_KEY, live_endpoint);
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_CONTROL_KEY, control_endpoint);

        aeron_uri_string_builder_sprint(&builder, live_destination, sizeof(live_destination));
        aeron_uri_string_builder_close(&builder);
    }

    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_new(&builder);

        aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_ENDPOINT_KEY, replay_endpoint);

        aeron_uri_string_builder_sprint(&builder, replay_destination, sizeof(replay_destination));
        aeron_uri_string_builder_close(&builder);
    }

    const size_t initial_message_count = min_messages_per_term * 3;

    aeron_archive_context_t *ctx;
    aeron_archive_async_connect_t *async;

    aeron_context_t *aeron_ctx;

    ASSERT_EQ_ERR(0, aeron_archive_context_init(&ctx));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_request_channel(ctx, "aeron:udp?endpoint=localhost:8010"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_control_response_channel(ctx, "aeron:udp?endpoint=localhost:0"));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_idle_strategy(
        ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ_ERR(0, aeron_archive_context_set_credentials_supplier(
        ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds_clientd));

    ASSERT_EQ_ERR(0, aeron_context_init(&aeron_ctx));
    ASSERT_EQ_ERR(0, aeron_context_set_use_conductor_agent_invoker(aeron_ctx, true));
    ASSERT_EQ_ERR(0, aeron_context_set_dir(aeron_ctx, aeron_archive_context_get_aeron_directory_name(ctx)));
    ASSERT_EQ_ERR(0, aeron_init(&m_aeron, aeron_ctx));
    ASSERT_EQ_ERR(0, aeron_start(m_aeron));

    ASSERT_EQ_ERR(0, aeron_archive_context_set_aeron(ctx, m_aeron));
    ASSERT_EQ_ERR(0, aeron_archive_async_connect(&async, ctx));

    // the ctx passed into async_connect gets duplicated, so it should be safe to delete it now
    ASSERT_EQ_ERR(0, aeron_archive_context_close(ctx));

    ASSERT_EQ_ERR(0, aeron_archive_async_connect_poll(&m_archive, async));

    while (nullptr == m_archive)
    {
        idle();

        ASSERT_NE(-1, aeron_archive_async_connect_poll(&m_archive, async));
    }

    ctx = aeron_archive_get_archive_context(m_archive);
    ASSERT_FALSE(aeron_archive_context_get_owns_aeron_client(ctx));

    aeron_publication_t *publication = addPublication(publication_channel, m_recordingStreamId);

    int32_t session_id = aeron_publication_session_id(publication);

    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_new(&builder);

        aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
        aeron_uri_string_builder_put(&builder, AERON_URI_GTAG_KEY, "99901");
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_SESSION_ID_KEY, session_id);
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_ENDPOINT_KEY, recording_endpoint);
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_CONTROL_KEY, control_endpoint);

        aeron_uri_string_builder_sprint(&builder, recording_channel, sizeof(recording_channel));
        aeron_uri_string_builder_close(&builder);
    }

    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_new(&builder);

        aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
        aeron_uri_string_builder_put(
            &builder, AERON_UDP_CHANNEL_CONTROL_MODE_KEY, AERON_UDP_CHANNEL_CONTROL_MODE_MANUAL_VALUE);
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_SESSION_ID_KEY, session_id);

        aeron_uri_string_builder_sprint(&builder, subscription_channel, sizeof(subscription_channel));
        aeron_uri_string_builder_close(&builder);
    }

    ASSERT_EQ_ERR(0, aeron_archive_start_recording(
        nullptr,
        m_archive,
        recording_channel,
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_REMOTE,
        true));

    setupCounters(session_id);

    bool is_active;
    ASSERT_EQ_ERR(0, aeron_archive_recording_pos_is_active(
        &is_active,
        m_counters_reader,
        m_counter_id,
        m_recording_id_from_counter));
    EXPECT_TRUE(is_active);

    ASSERT_EQ_ERR(m_counter_id, aeron_archive_recording_pos_find_counter_id_by_recording_id(
        m_counters_reader,
        m_recording_id_from_counter));

    {
        size_t sib_len = AERON_COUNTER_MAX_LABEL_LENGTH;
        const char source_identity_buffer[AERON_COUNTER_MAX_LABEL_LENGTH] = { '\0' };

        ASSERT_EQ_ERR(0,
            aeron_archive_recording_pos_get_source_identity(
                m_counters_reader,
                m_counter_id,
                source_identity_buffer,
                &sib_len));
        EXPECT_STREQ("127.0.0.1:23265", source_identity_buffer);
    }

    offerMessages(publication, initial_message_count);

    waitUntilCaughtUp(aeron_publication_position(publication));

    fragment_handler_clientd_t clientd;
    clientd.received = 0;
    clientd.position = 0;

    aeron_subscription_t *subscription = addSubscription(subscription_channel, m_recordingStreamId);

    char replay_channel[AERON_URI_MAX_LENGTH];
    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_new(&builder);

        aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_SESSION_ID_KEY, session_id);

        aeron_uri_string_builder_sprint(&builder, replay_channel, sizeof(replay_channel));
        aeron_uri_string_builder_close(&builder);
    }

    aeron_archive_replay_merge_t *replay_merge;

    ASSERT_EQ_ERR(0, aeron_archive_replay_merge_init(
        &replay_merge,
        subscription,
        m_archive,
        replay_channel,
        replay_destination,
        live_destination,
        m_recording_id_from_counter,
        clientd.position,
        aeron_epoch_clock(),
        REPLAY_MERGE_PROGRESS_TIMEOUT_DEFAULT_MS));

    while (aeron_archive_replay_merge_poll(replay_merge, nullptr, nullptr, 123) >= 0)
    {
        idle();
    }
    ASSERT_THAT(aeron_errmsg(), testing::HasSubstr("max concurrent replays reached 0"));

    ASSERT_EQ_ERR(0, aeron_archive_replay_merge_close(replay_merge));

    DoTearDown();

    ASSERT_EQ_ERR(0, aeron_close(m_aeron));
    ASSERT_EQ_ERR(0, aeron_context_close(aeron_ctx));
}
