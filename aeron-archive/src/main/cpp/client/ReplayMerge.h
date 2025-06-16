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
#ifndef AERON_ARCHIVE_REPLAY_MERGE_H
#define AERON_ARCHIVE_REPLAY_MERGE_H

#include "AeronArchive.h"

namespace aeron { namespace archive { namespace client
{

constexpr const std::int32_t REPLAY_MERGE_LIVE_ADD_MAX_WINDOW = 32 * 1024 * 1024;
constexpr const std::int32_t REPLAY_MERGE_REPLAY_REMOVE_THRESHOLD = 0;
constexpr const std::int64_t REPLAY_MERGE_PROGRESS_TIMEOUT_DEFAULT_MS = 5 * 1000;
constexpr const std::int64_t REPLAY_MERGE_INITIAL_GET_MAX_RECORDED_POSITION_BACKOFF_MS = 8;
constexpr const std::int64_t REPLAY_MERGE_GET_MAX_RECORDED_POSITION_BACKOFF_MAX_MS = 500;
constexpr const std::int64_t REPLAY_MERGE_ARCHIVE_POLL_INTERVAL_MS = 100;

/**
 * Replay a recorded stream from a starting position and merge with live stream to consume a full history of a stream.
 * <p>
 * Once constructed the either of #poll(FragmentHandler, int) or #doWork() interleaved with consumption
 * of the #image() should be called in a duty cycle loop until #isMerged() is true,
 * after which the ReplayMerge can go out of scope and continued usage can be made of the Image or its
 * parent Subscription. If an exception occurs or progress stops, the merge will fail and
 * #hasErrored() will be true.
 * <p>
 * If the endpoint on the replay destination uses a port of 0, then the OS will assign a port from the ephemeral
 * range and this will be added to the replay channel for instructing the archive.
 * <p>
 * NOTE: Merging is only supported with UDP streams.
 */
class ReplayMerge
{
public:
    /**
     * Create a ReplayMerge to manage the merging of a replayed stream and switching to live stream as
     * appropriate.
     *
     * @param subscription to use for the replay and live stream. Must be a multi-destination subscription.
     * @param archive to use for the replay.
     * @param replayChannel to use for the replay.
     * @param replayDestination to send the replay to and the destination added by the Subscription.
     * @param liveDestination for the live stream and the destination added by the Subscription.
     * @param recordingId for the replay.
     * @param startPosition for the replay.
     * @param epochClock to use for progress checks.
     * @param mergeProgressTimeoutMs to use for progress checks.
     */
    ReplayMerge(
        std::shared_ptr<Subscription> subscription,
        std::shared_ptr<AeronArchive> archive,
        const std::string &replayChannel,
        const std::string &replayDestination,
        const std::string &liveDestination,
        std::int64_t recordingId,
        std::int64_t startPosition,
        epoch_clock_t epochClock = aeron::currentTimeMillis,
        std::int64_t mergeProgressTimeoutMs = REPLAY_MERGE_PROGRESS_TIMEOUT_DEFAULT_MS);

    ~ReplayMerge();

    /**
     * Process the operation of the merge. Do not call the processing of fragments on the subscription.
     *
     * @return indication of work done processing the merge.
     */
    inline int doWork()
    {
        int workCount = 0;
        const long long nowMs = m_epochClock();

        try
        {
            switch (m_state)
            {
                case State::RESOLVE_REPLAY_PORT:
                    workCount += resolveReplayPort(nowMs);
                    checkProgress(nowMs);
                    break;

                case State::GET_RECORDING_POSITION:
                    workCount += getRecordingPosition(nowMs);
                    checkProgress(nowMs);
                    break;

                case State::REPLAY:
                    workCount += replay(nowMs);
                    checkProgress(nowMs);
                    break;

                case State::CATCHUP:
                    workCount += catchup(nowMs);
                    checkProgress(nowMs);
                    break;

                case State::ATTEMPT_LIVE_JOIN:
                    workCount += attemptLiveJoin(nowMs);
                    checkProgress(nowMs);
                    break;

                default:
                    break;
            }
        }
        catch (std::exception &ex)
        {
            state(State::FAILED);
            throw;
        }

        return workCount;
    }

    /**
     * Poll the Image used for the merging replay and live stream. The ReplayMerge#doWork method
     * will be called before the poll so that processing of the merge can be done.
     *
     * @param fragmentHandler to call for fragments
     * @param fragmentLimit for poll call
     * @return number of fragments processed.
     */
    template<typename F>
    inline int poll(F &&fragmentHandler, int fragmentLimit)
    {
        doWork();
        return nullptr == m_image ? 0 : m_image->poll(fragmentHandler, fragmentLimit);
    }

    /**
     * The Image used for the replay and live stream.
     *
     * @return the Image used for the replay and live stream.
     */
    inline std::shared_ptr<Image> image()
    {
        return m_image;
    }

    /**
     * Is the live stream merged and the replay stopped?
     *
     * @return true if live stream is merged and the replay stopped or false if not.
     */
    inline bool isMerged() const
    {
        return m_state == State::MERGED;
    }

    /**
     * Has the replay merge failed due to an error?
     *
     * @return true if replay merge has failed due to an error.
     */
    inline bool hasFailed() const
    {
        return m_state == State::FAILED;
    }

    /**
     * Is the live destination added to the subscription?
     *
     * @return true if live destination added or false if not.
     */
    inline bool isLiveAdded() const
    {
        return m_isLiveAdded;
    }

private:
    enum State : std::int8_t
    {
        RESOLVE_REPLAY_PORT,
        GET_RECORDING_POSITION,
        REPLAY,
        CATCHUP,
        ATTEMPT_LIVE_JOIN,
        MERGED,
        FAILED,
        CLOSED
    };

    const std::shared_ptr<Subscription> m_subscription;
    const std::shared_ptr<AeronArchive> m_archive;
    const std::string m_replayDestination;
    const std::string m_liveDestination;
    const std::int64_t m_recordingId;
    const std::int64_t m_startPosition;
    const long long m_mergeProgressTimeoutMs;

    State m_state = GET_RECORDING_POSITION;
    std::string m_replayEndpoint;
    std::shared_ptr<ChannelUri> m_replayChannelUri = nullptr;
    std::shared_ptr<Image> m_image = nullptr;
    epoch_clock_t m_epochClock;
    std::int64_t m_activeCorrelationId = aeron::NULL_VALUE;
    std::int64_t m_nextTargetPosition = aeron::NULL_VALUE;
    std::int64_t m_replaySessionId = aeron::NULL_VALUE;
    std::int64_t m_positionOfLastProgress = aeron::NULL_VALUE;
    long long m_timeOfLastProgressMs = 0;
    long long m_timeOfNextGetMaxRecordedPositionMs;
    long long m_getMaxRecordedPositionBackoffMs = REPLAY_MERGE_INITIAL_GET_MAX_RECORDED_POSITION_BACKOFF_MS;
    long long m_timeOfLastScheduledArchivePollMs = 0;
    bool m_isLiveAdded = false;
    bool m_isReplayActive = false;

    inline void state(State state)
    {
        m_state = state;
    }

    inline bool shouldAddLiveDestination(std::int64_t position)
    {
        return !m_isLiveAdded &&
            (m_nextTargetPosition - position) <=
                std::min(m_image->termBufferLength() / 4, REPLAY_MERGE_LIVE_ADD_MAX_WINDOW);
    }

    inline bool shouldStopAndRemoveReplay(std::int64_t position)
    {
        return m_isLiveAdded &&
            (m_nextTargetPosition - position) <= REPLAY_MERGE_REPLAY_REMOVE_THRESHOLD &&
                m_image->activeTransportCount() >= 2;
    }

    int resolveReplayPort(long long nowMs);

    int getRecordingPosition(long long nowMs);

    int replay(long long nowMs);

    int catchup(long long nowMs);

    int attemptLiveJoin(long long nowMs);

    bool callGetMaxRecordedPosition(long long nowMs);

    void stopReplay();

    void checkProgress(long long nowMs);

    static bool pollForResponse(AeronArchive &archive, std::int64_t correlationId);
};

}}}

#endif //AERON_ARCHIVE_REPLAY_MERGE_H
