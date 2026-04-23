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

#include <chrono>
#include <thread>
#include <iostream>
#include <random>

#include <gtest/gtest.h>

#include "FragmentAssembler.h"
#include "client/archive/AeronArchive.h"
#include "client/archive/CredentialsSupplier.h"
#include "client/archive/RecordingPos.h"
#include "client/archive/PersistentSubscription.h"

#include "concurrent/SleepingIdleStrategy.h"

#include "TestArchive.h"
#include "TestMediaDriver.h"

static const std::chrono::duration<long, std::milli> IDLE_SLEEP_MS_1(1);

using namespace aeron;
using namespace aeron::archive::client;

static const std::string MDC_PUBLICATION_CHANNEL = "aeron:udp?control=localhost:2000|control-mode=dynamic|fc=max";
static const std::string MDC_SUBSCRIPTION_CHANNEL = "aeron:udp?control=localhost:2000";
static const std::string UNICAST_CHANNEL = "aeron:udp?endpoint=localhost:2000";
static const std::int32_t STREAM_ID = 1000;
static const std::int32_t REPLAY_STREAM_ID = -5;
static const std::int32_t ONE_KB = 992;
static const std::string CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8010";
static const std::string CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";

class PersistentSubscriptionWrapperTestBase
{
public:
    void DoSetUp()
    {
        std::string sourceArchiveDir = m_archiveDir + AERON_FILE_SEP + "source";
        m_archive = std::make_shared<TestArchive>(
            m_context.aeronDirectoryName(), sourceArchiveDir, std::cout,
            CONTROL_REQUEST_CHANNEL, "aeron:udp?endpoint=localhost:0", 42);

        setCredentials(m_context);
        m_context.controlRequestChannel(CONTROL_REQUEST_CHANNEL)
            .controlResponseChannel(CONTROL_RESPONSE_CHANNEL)
            .idleStrategy(m_idleStrategy);
    }

    void DoTearDown()
    {
    }

    static void setCredentials(AeronArchive::Context_t &context)
    {
        auto onEncodedCredentials = []() -> std::pair<const char *, std::uint32_t>
        {
            std::string credentials("admin:admin");
            char *arr = new char[credentials.length() + 1];
            std::memcpy(arr, credentials.data(), credentials.length());
            arr[credentials.length()] = '\0';
            return { arr, static_cast<std::uint32_t>(credentials.length()) };
        };

        context.credentialsSupplier(CredentialsSupplier(onEncodedCredentials));
    }

    struct RecordedPublication
    {
        std::shared_ptr<AeronArchive> archive;
        std::shared_ptr<ExclusivePublication> publication;
        std::int64_t recordingId;
        std::int32_t counterId;
    };

    RecordedPublication createRecordedPublication(
        const std::string &channel, std::int32_t streamId)
    {
        AeronArchive::Context_t pubCtx;
        pubCtx.controlRequestChannel(CONTROL_REQUEST_CHANNEL)
            .controlResponseChannel(CONTROL_RESPONSE_CHANNEL)
            .controlResponseStreamId(pubCtx.controlResponseStreamId() + 10)
            .idleStrategy(m_idleStrategy);
        setCredentials(pubCtx);

        std::shared_ptr<AeronArchive> archive = AeronArchive::connect(pubCtx);
        std::shared_ptr<ExclusivePublication> publication = archive->addRecordedExclusivePublication(channel, streamId);

        CountersReader &counters = archive->context().aeron()->countersReader();
        std::int32_t counterId = RecordingPos::findCounterIdBySessionId(counters, publication->sessionId());
        waitUntil(
            "recording counter found",
            [&] {
                counterId = RecordingPos::findCounterIdBySessionId(counters, publication->sessionId());
                return counterId != CountersReader::NULL_COUNTER_ID;
            });

        std::int64_t recordingId = RecordingPos::getRecordingId(counters, counterId);

        return { archive, publication, recordingId, counterId };
    }

    static void persistMessages(
        RecordedPublication &recordedPublication,
        const std::vector<std::vector<uint8_t>> &messages)
    {
        static const uint8_t empty_buffer = 0;
        int64_t position = 0;
        for (auto &msg : messages)
        {
            const uint8_t *buffer = msg.empty() ? &empty_buffer : msg.data();
            executeUntil(
                "offer message",
                [&] {
                    position = recordedPublication.publication->offer(
                        AtomicBuffer(const_cast<uint8_t *>(buffer), msg.size()),
                        0,
                        static_cast<util::index_t>(msg.size()));
                    return position > 0 ? 1 : 0;
                },
                [&] { return position > 0; });
        }

        CountersReader &counters = recordedPublication.archive->context().aeron()->countersReader();
        waitUntil(
            "recording persisted",
            [&] { return counters.getCounterValue(recordedPublication.counterId) >= position; });
    }

    static void offerMessages(
        RecordedPublication &recordedPublication,
        const std::vector<std::vector<uint8_t>> &messages)
    {
        static const uint8_t empty_buffer = 0;
        for (auto &msg : messages)
        {
            const uint8_t *buffer = msg.empty() ? &empty_buffer : msg.data();
            int64_t result = 0;
            executeUntil(
                "offer message",
                [&] {
                    result = recordedPublication.publication->offer(
                        AtomicBuffer(const_cast<uint8_t *>(buffer), msg.size()),
                        0,
                        static_cast<util::index_t>(msg.size()));
                    return result > 0 ? 1 : 0;
                },
                [&] { return result > 0; });
        }
    }

    static std::vector<std::vector<uint8_t>> generateMessages(int count, int size)
    {
        static std::random_device rd;
        static std::default_random_engine engine(rd());
        static std::uniform_int_distribution<unsigned short> dist(0, UINT8_MAX);

        std::vector<std::vector<uint8_t>> messages(count);
        for (int i = 0; i < count; i++)
        {
            messages[i].resize(size);
            for (int j = 0; j < size; j++)
            {
                messages[i][j] = dist(engine);
            }
        }
        return messages;
    }

    class MessageCapture
    {
    public:
        ControlledPollAction onFragment(
            AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            std::vector<uint8_t> msg(length);
            buffer.getBytes(offset, msg.data(), static_cast<std::size_t>(length));
            m_messages.push_back(msg);
            return ControlledPollAction::CONTINUE;
        }

        size_t count() const { return m_messages.size(); }

        const std::vector<std::vector<uint8_t>> &messages() const { return m_messages; }

        void clear() { m_messages.clear(); }

    private:
        std::vector<std::vector<uint8_t>> m_messages;
    };

    template<typename PredicateFn>
    static void waitUntil(
        const std::string &label,
        PredicateFn predicate)
    {
        executeUntil(label, [] { return 0; }, predicate);
    }

    template<typename ActionFn, typename PredicateFn>
    static void executeUntil(
        const std::string &label,
        ActionFn action,
        PredicateFn predicate)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (true)
        {
            if (std::chrono::steady_clock::now() >= deadline)
            {
                FAIL() << "timed out waiting for '" << label << "'";
            }

            const int result = action();

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

    void configurePersistentSubscriptionCtx(
        PersistentSubscription::Context &ctx,
        std::shared_ptr<AeronArchive::Context_t> archiveCtx,
        std::shared_ptr<Aeron> aeron,
        std::int64_t recordingId,
        const std::string &liveChannel = std::string("aeron:ipc"),
        std::int64_t startPosition = 0)
    {
        setCredentials(*archiveCtx);
        archiveCtx->controlRequestChannel(CONTROL_REQUEST_CHANNEL)
            .controlResponseChannel(CONTROL_RESPONSE_CHANNEL)
            .controlResponseStreamId(m_context.controlResponseStreamId() + 20);

        ctx.archiveContext(archiveCtx)
            .aeron(aeron)
            .recordingId(recordingId)
            .startPosition(startPosition)
            .liveChannel(liveChannel)
            .liveStreamId(STREAM_ID)
            .replayChannel("aeron:udp?endpoint=localhost:0")
            .replayStreamId(REPLAY_STREAM_ID);
    }

protected:
    std::shared_ptr<TestArchive> m_archive;
    const std::string m_archiveDir = ARCHIVE_DIR;
    aeron::archive::client::Context m_context;
    SleepingIdleStrategy m_idleStrategy = SleepingIdleStrategy(IDLE_SLEEP_MS_1);
};

class PersistentSubscriptionWrapperTest : public PersistentSubscriptionWrapperTestBase, public testing::Test
{
public:
    void SetUp() final { DoSetUp(); }
    void TearDown() final { DoTearDown(); }
};

// --- Transition behavior ---

TEST_F(PersistentSubscriptionWrapperTest, shouldCycleThroughMultipleLiveReplayTransitions)
{

    RecordedPublication recordedPublication = createRecordedPublication(MDC_PUBLICATION_CHANNEL, STREAM_ID);
    std::vector<std::vector<uint8_t>> initialMessages = generateMessages(5, ONE_KB);
    persistMessages(recordedPublication, initialMessages);

    std::shared_ptr<AeronArchive> archive = AeronArchive::connect(m_context);
    std::shared_ptr<Aeron> aeron = archive->context().aeron();

    int liveJoinedCount = 0;
    int liveLeftCount = 0;

    PersistentSubscription::Context persistentSubscriptionCtx;
    auto persistentSubscriptionArchiveCtx = std::make_shared<AeronArchive::Context_t>();
    configurePersistentSubscriptionCtx(persistentSubscriptionCtx, persistentSubscriptionArchiveCtx, aeron, recordedPublication.recordingId, MDC_SUBSCRIPTION_CHANNEL);
    persistentSubscriptionCtx.onLiveJoined([&liveJoinedCount]() { liveJoinedCount++; })
         .onLiveLeft([&liveLeftCount]() { liveLeftCount++; });

    MessageCapture capture;
    auto handler = [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
    { return capture.onFragment(buffer, offset, length, header); };

    std::shared_ptr<PersistentSubscription> persistentSubscription = PersistentSubscription::create(persistentSubscriptionCtx);

    auto poller = [&] { return persistentSubscription->controlledPoll(handler, 10); };

    executeUntil("becomes live", poller, [&] { return persistentSubscription->isLive(); });
    ASSERT_EQ(1, liveJoinedCount);
    ASSERT_EQ(initialMessages.size(), capture.count());

    std::vector<std::vector<uint8_t>> allMessages(initialMessages);

    // Run two drop-and-recover cycles
    for (int cycle = 0; cycle < 2; cycle++)
    {
        std::vector<std::vector<uint8_t>> liveMessages = generateMessages(5, ONE_KB);
        persistMessages(recordedPublication, liveMessages);

        executeUntil("receives live", poller,
            [&] { return capture.count() >= allMessages.size() + liveMessages.size(); });

        allMessages.insert(allMessages.end(), liveMessages.begin(), liveMessages.end());
        ASSERT_TRUE(persistentSubscription->isLive());

        // Launch a second driver with a fast subscriber to force the PS to fall behind
        const std::string aeronDir2 = aeron::Context::defaultAeronPath() + "_2";
        const std::string archiveDir2 = std::string(ARCHIVE_DIR) + AERON_FILE_SEP + "driver2";
        TestArchive archive2(aeronDir2, archiveDir2, std::cout,
            "aeron:udp?endpoint=localhost:8011", "aeron:udp?endpoint=localhost:0", 2);

        aeron::Context aeronCtx2;
        aeronCtx2.aeronDir(aeronDir2);
        std::shared_ptr<Aeron> aeron2 = Aeron::connect(aeronCtx2);
        
        std::int64_t fastSubId = aeron2->addSubscription(MDC_SUBSCRIPTION_CHANNEL, STREAM_ID);
        std::shared_ptr<Subscription> fastSub;
        waitUntil("fast subscription created",
            [&] { fastSub = aeron2->findSubscription(fastSubId); return fastSub != nullptr; });
        waitUntil("fast subscription has image",
            [&] { return fastSub->imageCount() > 0; });

        // Flood 64 large messages — fast subscriber drains them, PS falls behind
        std::vector<std::vector<uint8_t>> backPressureMessages = generateMessages(64, ONE_KB);
        offerMessages(recordedPublication, backPressureMessages);

        std::size_t fastCount = 0;
        fragment_handler_t fastHandler = [&](const AtomicBuffer &, util::index_t, util::index_t, const Header &)
        {
            fastCount++;
        };
        FragmentAssembler fastAssembler(fastHandler);
        executeUntil("fast sub receives 64",
            [&] { return static_cast<int>(fastSub->poll(fastAssembler.handler(), 1)); },
            [&] { return fastCount >= 64; });

        executeUntil("drops to replaying", poller, [&] { return persistentSubscription->isReplaying(); });
        ASSERT_EQ(cycle + 1, liveLeftCount);

        std::vector<std::vector<uint8_t>> messagesAfterRejoin = generateMessages(5, ONE_KB);
        persistMessages(recordedPublication, messagesAfterRejoin);

        allMessages.insert(allMessages.end(), backPressureMessages.begin(), backPressureMessages.end());
        allMessages.insert(allMessages.end(), messagesAfterRejoin.begin(), messagesAfterRejoin.end());

        executeUntil("recovers to live", poller,
            [&]
            {
                return capture.count() >= allMessages.size() && persistentSubscription->isLive();
            });

        ASSERT_EQ(cycle + 2, liveJoinedCount);
    }

    ASSERT_EQ(allMessages, capture.messages());
}

// Verifies the ABORT action is respected during replay.
TEST_F(PersistentSubscriptionWrapperTest, shouldRespectAbortActionDuringReplayAndLive)
{
    RecordedPublication recordedPublication = createRecordedPublication("aeron:ipc", STREAM_ID);
    std::vector<std::vector<uint8_t>> messages = generateMessages(3, 128);
    persistMessages(recordedPublication, messages);

    std::shared_ptr<AeronArchive> archive = AeronArchive::connect(m_context);
    std::shared_ptr<Aeron> aeron = archive->context().aeron();

    PersistentSubscription::Context persistentSubscriptionCtx;
    auto persistentSubscriptionArchiveCtx = std::make_shared<AeronArchive::Context_t>();
    configurePersistentSubscriptionCtx(persistentSubscriptionCtx, persistentSubscriptionArchiveCtx, aeron, recordedPublication.recordingId);

    MessageCapture capture;
    bool shouldAbort = true;

    auto abortingHandler = [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        -> ControlledPollAction
    {
        if (shouldAbort)
        {
            shouldAbort = false;
            return ControlledPollAction::ABORT;
        }
        shouldAbort = true;
        return capture.onFragment(buffer, offset, length, header);
    };

    std::shared_ptr<PersistentSubscription> persistentSubscription = PersistentSubscription::create(persistentSubscriptionCtx);

    executeUntil(
        "becomes live",
        [&] { return persistentSubscription->controlledPoll(abortingHandler, 1); },
        [&] { return persistentSubscription->isLive(); });

    ASSERT_EQ(messages, capture.messages());

    shouldAbort = false;
    std::vector<std::vector<uint8_t>> liveMessages = generateMessages(2, 128);
    offerMessages(recordedPublication, liveMessages);

    executeUntil(
        "receives live",
        [&] { return persistentSubscription->controlledPoll(
            [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
            { return capture.onFragment(buffer, offset, length, header); }, 10); },
        [&] { return capture.count() >= messages.size() + liveMessages.size(); });
}

// Context goes out of scope immediately after create — the PS must still function
// correctly through replay and live. This is the core ownership transfer test.
TEST_F(PersistentSubscriptionWrapperTest, shouldWorkAfterContextIsDestroyed)
{
    RecordedPublication recordedPublication = createRecordedPublication("aeron:ipc", STREAM_ID);
    std::vector<std::vector<uint8_t>> messages = generateMessages(3, 128);
    persistMessages(recordedPublication, messages);

    std::shared_ptr<AeronArchive> archive = AeronArchive::connect(m_context);
    std::shared_ptr<Aeron> aeron = archive->context().aeron();

    std::shared_ptr<PersistentSubscription> persistentSubscription;
    auto persistentSubscriptionArchiveCtx = std::make_shared<AeronArchive::Context_t>();

    {
        PersistentSubscription::Context persistentSubscriptionCtx;
        configurePersistentSubscriptionCtx(persistentSubscriptionCtx, persistentSubscriptionArchiveCtx, aeron, recordedPublication.recordingId);
        persistentSubscriptionCtx.onLiveJoined([]() {});  // set callbacks that reference nothing outside
        persistentSubscriptionCtx.onLiveLeft([]() {});
        persistentSubscriptionCtx.onError([](int, const std::string &) {});

        persistentSubscription = PersistentSubscription::create(persistentSubscriptionCtx);
        // persistentSubscriptionCtx destroyed here
    }

    // PS must still work with PersistentSubscription::Context destroyed
    MessageCapture capture;
    auto handler = [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
    { return capture.onFragment(buffer, offset, length, header); };

    auto poller = [&] { return persistentSubscription->controlledPoll(handler, 10); };

    executeUntil("becomes live", poller, [&] { return persistentSubscription->isLive(); });
    ASSERT_EQ(messages, capture.messages());

    std::vector<std::vector<uint8_t>> liveMessages = generateMessages(2, 128);
    offerMessages(recordedPublication, liveMessages);

    executeUntil("receives live", poller, [&] { return capture.count() == messages.size() + liveMessages.size(); });

    std::vector<std::vector<uint8_t>> allMessages;
    allMessages.insert(allMessages.end(), messages.begin(), messages.end());
    allMessages.insert(allMessages.end(), liveMessages.begin(), liveMessages.end());
    ASSERT_EQ(allMessages, capture.messages());
}

// Context goes out of scope before PS has connected to archive (async connection in progress).
TEST_F(PersistentSubscriptionWrapperTest, shouldWorkWhenContextDestroyedDuringAsyncConnect)
{
    RecordedPublication recordedPublication = createRecordedPublication("aeron:ipc", STREAM_ID);
    std::vector<std::vector<uint8_t>> messages = generateMessages(3, 128);
    persistMessages(recordedPublication, messages);

    std::shared_ptr<AeronArchive> archive = AeronArchive::connect(m_context);
    std::shared_ptr<Aeron> aeron = archive->context().aeron();

    std::shared_ptr<PersistentSubscription> persistentSubscription;
    auto persistentSubscriptionArchiveCtx = std::make_shared<AeronArchive::Context_t>();

    {
        PersistentSubscription::Context persistentSubscriptionCtx;
        configurePersistentSubscriptionCtx(persistentSubscriptionCtx, persistentSubscriptionArchiveCtx, aeron, recordedPublication.recordingId);

        persistentSubscription = PersistentSubscription::create(persistentSubscriptionCtx);
        // Do NOT poll — PS is still in AWAIT_ARCHIVE_CONNECTION
        // persistentSubscriptionCtx destroyed here while async connect is pending
    }

    MessageCapture capture;
    auto handler = [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
    { return capture.onFragment(buffer, offset, length, header); };

    auto poller = [&] { return persistentSubscription->controlledPoll(handler, 10); };

    executeUntil("becomes live", poller, [&] { return persistentSubscription->isLive(); });
    ASSERT_EQ(messages, capture.messages());
}

// Listener callbacks still fire after Context is destroyed.
TEST_F(PersistentSubscriptionWrapperTest, shouldFireListenerCallbacksAfterContextDestroyed)
{
    RecordedPublication recordedPublication = createRecordedPublication("aeron:ipc", STREAM_ID);
    std::vector<std::vector<uint8_t>> messages = generateMessages(3, 128);
    persistMessages(recordedPublication, messages);

    std::shared_ptr<AeronArchive> archive = AeronArchive::connect(m_context);
    std::shared_ptr<Aeron> aeron = archive->context().aeron();

    int liveJoinedCount = 0;
    std::shared_ptr<PersistentSubscription> persistentSubscription;
    auto persistentSubscriptionArchiveCtx = std::make_shared<AeronArchive::Context_t>();

    {
        PersistentSubscription::Context persistentSubscriptionCtx;
        configurePersistentSubscriptionCtx(persistentSubscriptionCtx, persistentSubscriptionArchiveCtx, aeron, recordedPublication.recordingId);
        persistentSubscriptionCtx.onLiveJoined([&liveJoinedCount]() { liveJoinedCount++; });

        persistentSubscription = PersistentSubscription::create(persistentSubscriptionCtx);
    }

    ASSERT_EQ(0, liveJoinedCount);

    MessageCapture capture;
    auto handler = [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
    { return capture.onFragment(buffer, offset, length, header); };

    auto poller = [&] { return persistentSubscription->controlledPoll(handler, 10); };

    // The onLiveJoined callback must fire even though Context is gone
    executeUntil("becomes live", poller, [&] { return persistentSubscription->isLive(); });
    ASSERT_EQ(1, liveJoinedCount);
}

// Error callback fires after Context is destroyed.
TEST_F(PersistentSubscriptionWrapperTest, shouldFireErrorCallbackAfterContextDestroyed)
{
    std::shared_ptr<AeronArchive> archive = AeronArchive::connect(m_context);
    std::shared_ptr<Aeron> aeron = archive->context().aeron();

    int errorCount = 0;
    std::shared_ptr<PersistentSubscription> persistentSubscription;
    auto persistentSubscriptionArchiveCtx = std::make_shared<AeronArchive::Context_t>();

    {
        PersistentSubscription::Context persistentSubscriptionCtx;
        configurePersistentSubscriptionCtx(persistentSubscriptionCtx, persistentSubscriptionArchiveCtx, aeron, 99999);  // non-existent recording
        persistentSubscriptionCtx.onError([&errorCount](int, const std::string &) { errorCount++; });

        persistentSubscription = PersistentSubscription::create(persistentSubscriptionCtx);
    }

    executeUntil(
        "has failed",
        [&] { return persistentSubscription->controlledPoll([](AtomicBuffer &, util::index_t, util::index_t, Header &)
            { return ControlledPollAction::CONTINUE; }, 1); },
        [&] { return persistentSubscription->hasFailed(); });

    ASSERT_EQ(1, errorCount);
}

// Credentials work after Context is destroyed (archive connection uses credentials
// from the transferred CallbackState, not the original Context).
TEST_F(PersistentSubscriptionWrapperTest, shouldAuthenticateAfterContextDestroyed)
{
    RecordedPublication recordedPublication = createRecordedPublication("aeron:ipc", STREAM_ID);
    std::vector<std::vector<uint8_t>> messages = generateMessages(3, 128);
    persistMessages(recordedPublication, messages);

    std::shared_ptr<AeronArchive> archive = AeronArchive::connect(m_context);
    std::shared_ptr<Aeron> aeron = archive->context().aeron();

    std::shared_ptr<PersistentSubscription> persistentSubscription;
    auto persistentSubscriptionArchiveCtx = std::make_shared<AeronArchive::Context_t>();

    {
        PersistentSubscription::Context persistentSubscriptionCtx;
        configurePersistentSubscriptionCtx(persistentSubscriptionCtx, persistentSubscriptionArchiveCtx, aeron, recordedPublication.recordingId);
        // Credentials are set by configurePersistentSubscriptionCtx on the archive context

        persistentSubscription = PersistentSubscription::create(persistentSubscriptionCtx);
        // Do NOT poll — archive connection hasn't started yet
        // persistentSubscriptionCtx destroyed here — credentials and archive context must still be available
    }

    MessageCapture capture;
    auto handler = [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
    { return capture.onFragment(buffer, offset, length, header); };

    auto poller = [&] { return persistentSubscription->controlledPoll(handler, 10); };

    // Connection (which needs credentials) happens during poll, after PersistentSubscription::Context is gone
    executeUntil("becomes live", poller, [&] { return persistentSubscription->isLive(); });
    ASSERT_EQ(messages, capture.messages());
}

// PersistentSubscription destroyed while callbacks are set but never invoked.
// Verifies clean destruction of CallbackState when callbacks are configured but the
// PS never reaches a state that triggers them.
TEST_F(PersistentSubscriptionWrapperTest, shouldCleanUpCallbackStateWhenNeverTriggered)
{
    RecordedPublication recordedPublication = createRecordedPublication("aeron:ipc", STREAM_ID);

    std::shared_ptr<AeronArchive> archive = AeronArchive::connect(m_context);
    std::shared_ptr<Aeron> aeron = archive->context().aeron();

    bool liveJoinedCalled = false;
    bool liveLeftCalled = false;
    bool errorCalled = false;

    {
        PersistentSubscription::Context persistentSubscriptionCtx;
        auto persistentSubscriptionArchiveCtx = std::make_shared<AeronArchive::Context_t>();
        configurePersistentSubscriptionCtx(persistentSubscriptionCtx, persistentSubscriptionArchiveCtx, aeron, recordedPublication.recordingId);
        persistentSubscriptionCtx.onLiveJoined([&]() { liveJoinedCalled = true; })
             .onLiveLeft([&]() { liveLeftCalled = true; })
             .onError([&](int, const std::string &) { errorCalled = true; });

        std::shared_ptr<PersistentSubscription> persistentSubscription = PersistentSubscription::create(persistentSubscriptionCtx);
        // Destroy PS and Context without ever polling
    }

    ASSERT_FALSE(liveJoinedCalled);
    ASSERT_FALSE(liveLeftCalled);
    ASSERT_FALSE(errorCalled);
}

// Verify that Context is inert after create() — setting properties after create
// should not affect the running PersistentSubscription.
TEST_F(PersistentSubscriptionWrapperTest, shouldNotBeAffectedByContextModificationAfterCreate)
{
    RecordedPublication recordedPublication = createRecordedPublication("aeron:ipc", STREAM_ID);
    std::vector<std::vector<uint8_t>> messages = generateMessages(3, 128);
    persistMessages(recordedPublication, messages);

    std::shared_ptr<AeronArchive> archive = AeronArchive::connect(m_context);
    std::shared_ptr<Aeron> aeron = archive->context().aeron();

    int liveJoinedCount = 0;

    PersistentSubscription::Context persistentSubscriptionCtx;
    auto persistentSubscriptionArchiveCtx = std::make_shared<AeronArchive::Context_t>();
    configurePersistentSubscriptionCtx(persistentSubscriptionCtx, persistentSubscriptionArchiveCtx, aeron, recordedPublication.recordingId);
    persistentSubscriptionCtx.onLiveJoined([&liveJoinedCount]() { liveJoinedCount++; });

    std::shared_ptr<PersistentSubscription> persistentSubscription = PersistentSubscription::create(persistentSubscriptionCtx);

    persistentSubscriptionCtx.onLiveJoined([&liveJoinedCount]() { liveJoinedCount += 10; });

    MessageCapture capture;
    auto handler = [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
    { return capture.onFragment(buffer, offset, length, header); };

    auto poller = [&] { return persistentSubscription->controlledPoll(handler, 10); };

    executeUntil("becomes live", poller, [&] { return persistentSubscription->isLive(); });
    ASSERT_EQ(1, liveJoinedCount);
    ASSERT_EQ(messages, capture.messages());
}
