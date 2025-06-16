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

#include "ReplayMerge.h"

using namespace aeron;
using namespace aeron::archive::client;

ReplayMerge::ReplayMerge(
    std::shared_ptr<Subscription> subscription,
    std::shared_ptr<AeronArchive> archive,
    const std::string &replayChannel,
    const std::string &replayDestination,
    const std::string &liveDestination,
    std::int64_t recordingId,
    std::int64_t startPosition,
    epoch_clock_t epochClock,
    std::int64_t mergeProgressTimeoutMs) :
    m_subscription(std::move(subscription)),
    m_archive(std::move(archive)),
    m_replayDestination(replayDestination),
    m_liveDestination(liveDestination),
    m_recordingId(recordingId),
    m_startPosition(startPosition),
    m_mergeProgressTimeoutMs(mergeProgressTimeoutMs),
    m_epochClock(std::move(epochClock))
{
    if (m_subscription->channel().compare(0, 9, IPC_CHANNEL) == 0 ||
        replayChannel.compare(0, 9, IPC_CHANNEL) == 0 ||
        replayDestination.compare(0, 9, IPC_CHANNEL) == 0 ||
        liveDestination.compare(0, 9, IPC_CHANNEL) == 0)
    {
        throw util::IllegalArgumentException("IPC merging is not supported", SOURCEINFO);
    }

    if (m_subscription->channel().find("control-mode=manual") == std::string::npos)
    {
        throw util::IllegalArgumentException(
            "Subscription URI must have 'control-mode=manual' uri=" + m_subscription->channel(), SOURCEINFO);
    }

    m_replayChannelUri = ChannelUri::parse(replayChannel);
    m_replayChannelUri->put(LINGER_PARAM_NAME, "0");
    m_replayChannelUri->put(EOS_PARAM_NAME, "false");

    m_replayEndpoint = ChannelUri::parse(m_replayDestination)->get(ENDPOINT_PARAM_NAME);
    if (aeron::util::endsWith(m_replayEndpoint, std::string(":0")))
    {
        m_state = RESOLVE_REPLAY_PORT;
    }
    else
    {
        m_replayChannelUri->put(ENDPOINT_PARAM_NAME, m_replayEndpoint);
        m_state = GET_RECORDING_POSITION;
    }

    m_subscription->addDestination(m_replayDestination);
    m_timeOfLastProgressMs = m_timeOfNextGetMaxRecordedPositionMs = m_epochClock();
}

ReplayMerge::~ReplayMerge()
{
    if (State::CLOSED != m_state)
    {
        if (!m_archive->context().aeron()->isClosed())
        {
            if (State::MERGED != m_state)
            {
                m_subscription->removeDestination(m_replayDestination);
            }

            if (m_isReplayActive && m_archive->archiveProxy().publication()->isConnected())
            {
                stopReplay();
            }
        }

        state(State::CLOSED);
    }
}

int ReplayMerge::resolveReplayPort(long long nowMs)
{
    int workCount = 0;

    const std::string resolvedEndpoint = m_subscription->resolvedEndpoint();
    if (!resolvedEndpoint.empty())
    {
        std::size_t i = resolvedEndpoint.find_last_of(':');

        m_replayChannelUri->put(
            ENDPOINT_PARAM_NAME,
            m_replayEndpoint.substr(0, m_replayEndpoint.length() - 2) + resolvedEndpoint.substr(i));

        m_timeOfLastProgressMs = nowMs;
        state(GET_RECORDING_POSITION);
        workCount += 1;
    }

    return workCount;
}

int ReplayMerge::getRecordingPosition(long long nowMs)
{
    int workCount = 0;

    if (aeron::NULL_VALUE == m_activeCorrelationId)
    {
        if (callGetMaxRecordedPosition(nowMs))
        {
            m_timeOfLastProgressMs = nowMs;
            workCount += 1;
        }
    }
    else if (pollForResponse(*m_archive, m_activeCorrelationId))
    {
        m_nextTargetPosition = m_archive->controlResponsePoller().relevantId();
        m_activeCorrelationId = aeron::NULL_VALUE;

        if (NULL_POSITION != m_nextTargetPosition)
        {
            m_timeOfLastProgressMs = nowMs;
            state(State::REPLAY);
        }

        workCount += 1;
    }

    return workCount;
}

int ReplayMerge::replay(long long nowMs)
{
    int workCount = 0;

    if (aeron::NULL_VALUE == m_activeCorrelationId)
    {
        const std::int64_t correlationId = m_archive->context().aeron()->nextCorrelationId();

        if (m_archive->archiveProxy().replay(
            m_recordingId,
            m_startPosition,
            std::numeric_limits<std::int64_t>::max(),
            m_replayChannelUri->toString(),
            m_subscription->streamId(),
            correlationId,
            m_archive->controlSessionId()))
        {
            m_timeOfLastProgressMs = nowMs;
            m_activeCorrelationId = correlationId;
            workCount += 1;
        }
    }
    else if (pollForResponse(*m_archive, m_activeCorrelationId))
    {
        m_isReplayActive = true;
        m_replaySessionId = m_archive->controlResponsePoller().relevantId();
        m_timeOfLastProgressMs = nowMs;
        m_activeCorrelationId = aeron::NULL_VALUE;

        // reset getRecordingPosition backoff when moving to CATCHUP state
        m_getMaxRecordedPositionBackoffMs = REPLAY_MERGE_INITIAL_GET_MAX_RECORDED_POSITION_BACKOFF_MS;
        m_timeOfNextGetMaxRecordedPositionMs = nowMs;

        state(State::CATCHUP);
        workCount += 1;
    }

    return workCount;
}

int ReplayMerge::catchup(long long nowMs)
{
    int workCount = 0;

    if (nullptr == m_image && m_subscription->isConnected())
    {
        m_timeOfLastProgressMs = nowMs;
        m_image = m_subscription->imageBySessionId(static_cast<std::int32_t>(m_replaySessionId));
        m_positionOfLastProgress = m_image ? m_image->position() : aeron::NULL_VALUE;
    }

    if (nullptr != m_image)
    {
        int64_t position = m_image->position();
        if (position >= m_nextTargetPosition)
        {
            m_timeOfLastProgressMs = nowMs;
            m_positionOfLastProgress = position;
            m_activeCorrelationId = aeron::NULL_VALUE;
            state(State::ATTEMPT_LIVE_JOIN);
            workCount += 1;
        }
        else if (position > m_positionOfLastProgress)
        {
            m_timeOfLastProgressMs = nowMs;
            m_positionOfLastProgress = position;
        }
        else if (m_image->isClosed())
        {
            throw TimeoutException("ReplayMerge Image closed unexpectedly", SOURCEINFO);
        }
    }

    return workCount;
}

int ReplayMerge::attemptLiveJoin(long long nowMs)
{
    int workCount = 0;

    if (aeron::NULL_VALUE == m_activeCorrelationId)
    {
        if (callGetMaxRecordedPosition(nowMs))
        {
            m_timeOfLastProgressMs = nowMs;
            workCount += 1;
        }
    }
    else if (pollForResponse(*m_archive, m_activeCorrelationId))
    {
        m_nextTargetPosition = m_archive->controlResponsePoller().relevantId();
        m_activeCorrelationId = aeron::NULL_VALUE;

        if (NULL_POSITION != m_nextTargetPosition)
        {
            State nextState = State::CATCHUP;

            if (nullptr != m_image)
            {
                const std::int64_t position = m_image->position();

                if (shouldAddLiveDestination(position))
                {
                    m_subscription->addDestination(m_liveDestination);
                    m_timeOfLastProgressMs = nowMs;
                    m_positionOfLastProgress = position;
                    m_isLiveAdded = true;
                }
                else if (shouldStopAndRemoveReplay(position))
                {
                    m_subscription->removeDestination(m_replayDestination);
                    stopReplay();
                    m_timeOfLastProgressMs = nowMs;
                    m_positionOfLastProgress = position;
                    nextState = State::MERGED;
                }
            }

            state(nextState);
        }

        workCount += 1;
    }

    return workCount;
}

bool ReplayMerge::callGetMaxRecordedPosition(long long nowMs)
{
    if (nowMs < m_timeOfNextGetMaxRecordedPositionMs)
    {
        return false;
    }

    const std::int64_t correlationId = m_archive->context().aeron()->nextCorrelationId();

    const bool result = m_archive->archiveProxy().getMaxRecordedPosition(m_recordingId, correlationId, m_archive->controlSessionId());

    if (result)
    {
        m_activeCorrelationId = correlationId;
    }

    // increase backoff regardless of result
    m_getMaxRecordedPositionBackoffMs *= 2;
    if (m_getMaxRecordedPositionBackoffMs > REPLAY_MERGE_GET_MAX_RECORDED_POSITION_BACKOFF_MAX_MS)
    {
        m_getMaxRecordedPositionBackoffMs = REPLAY_MERGE_GET_MAX_RECORDED_POSITION_BACKOFF_MAX_MS;
    }
    m_timeOfNextGetMaxRecordedPositionMs = nowMs + m_getMaxRecordedPositionBackoffMs;

    return result;
}

void ReplayMerge::stopReplay()
{
    const std::int64_t correlationId = m_archive->context().aeron()->nextCorrelationId();

    if (m_archive->archiveProxy().stopReplay(m_replaySessionId, correlationId, m_archive->controlSessionId()))
    {
        m_isReplayActive = false;
    }
}

void ReplayMerge::checkProgress(long long nowMs)
{
    if (nowMs > (m_timeOfLastProgressMs + m_mergeProgressTimeoutMs))
    {
        throw TimeoutException("ReplayMerge no progress: state=" + std::to_string(m_state), SOURCEINFO);
    }

    if (aeron::NULL_VALUE == m_activeCorrelationId &&
        (nowMs > (m_timeOfLastScheduledArchivePollMs + REPLAY_MERGE_ARCHIVE_POLL_INTERVAL_MS)))
    {
        m_timeOfLastScheduledArchivePollMs = nowMs;
        pollForResponse(*m_archive, aeron::NULL_VALUE);
    }
}

bool ReplayMerge::pollForResponse(AeronArchive &archive, std::int64_t correlationId)
{
    ControlResponsePoller &poller = archive.controlResponsePoller();

    const int poll_count = poller.poll();
    if (poller.isPollComplete())
    {
        if (poller.controlSessionId() == archive.controlSessionId())
        {
            if (poller.isCodeError())
            {
                throw ArchiveException(
                    static_cast<std::int32_t>(poller.relevantId()),
                    poller.correlationId(),
                    "archive response for correlationId=" + std::to_string(poller.correlationId()) +
                    ", error: " + poller.errorMessage(),
                    SOURCEINFO);
            }

            return poller.correlationId() == correlationId;
        }
    }
    else if (poll_count == 0 && !poller.subscription()->isConnected())
    {
        throw ArchiveException("archive is not connected", SOURCEINFO);
    }

    return false;
}
