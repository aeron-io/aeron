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
#ifndef AERON_ARCHIVE_WRAPPER_REPLAY_MERGE_H
#define AERON_ARCHIVE_WRAPPER_REPLAY_MERGE_H

#include "AeronArchive.h"

namespace aeron { namespace archive { namespace client
{

class ReplayMerge
{
public:

    ReplayMerge(
        const std::shared_ptr<Subscription> &subscription,
        const std::shared_ptr<AeronArchive> &archive,
        const std::string &replayChannel,
        const std::string &replayDestination,
        const std::string &liveDestination,
        std::int64_t recordingId,
        std::int64_t startPosition,
        const epoch_clock_t &epochClock = aeron::currentTimeMillis,
        std::int64_t mergeProgressTimeoutMs = REPLAY_MERGE_PROGRESS_TIMEOUT_DEFAULT_MS) :
        m_subscription(subscription),
        m_archive(archive),
        m_replay_merge_t(nullptr),
        m_image(nullptr)
    {
        if (aeron_archive_replay_merge_init(
            &m_replay_merge_t,
            subscription->subscription(),
            archive->m_aeron_archive_t,
            replayChannel.c_str(),
            replayDestination.c_str(),
            liveDestination.c_str(),
            recordingId,
            startPosition,
            epochClock(),
            mergeProgressTimeoutMs) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    ~ReplayMerge()
    {
        m_image = nullptr;
        aeron_archive_replay_merge_close(m_replay_merge_t);
        m_replay_merge_t = nullptr;
    }

    ReplayMerge(const ReplayMerge&) = delete;
    ReplayMerge& operator=(const ReplayMerge&) = delete;

    ReplayMerge(ReplayMerge &&other) noexcept :
        m_subscription(std::move(other.m_subscription)), m_archive(std::move(other.m_archive)), m_replay_merge_t(other.m_replay_merge_t), m_image(std::move(other.m_image))
    {
        other.m_subscription = nullptr;
        other.m_archive = nullptr;
        other.m_replay_merge_t = nullptr;
        other.m_image = nullptr;
    }

    ReplayMerge& operator=(ReplayMerge &&other) noexcept
    {
        if (this == &other)
        {
            return *this;
        }

        if (nullptr != m_replay_merge_t)
        {
            m_image = nullptr;
            aeron_archive_replay_merge_close(m_replay_merge_t);
        }

        m_subscription = std::move(other.m_subscription);
        m_archive = std::move(other.m_archive);
        m_replay_merge_t = other.m_replay_merge_t;
        m_image = std::move(other.m_image);

        other.m_subscription = nullptr;
        other.m_archive = nullptr;
        other.m_replay_merge_t = nullptr;
        other.m_image = nullptr;

        return *this;
    }

    inline int doWork()
    {
        int count;

        if (aeron_archive_replay_merge_do_work(
            &count,
            m_replay_merge_t) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    template<typename F>
    inline int poll(F &&fragmentHandler, int fragmentLimit)
    {
        using handler_type = typename std::remove_reference<F>::type;

        int rc = aeron_archive_replay_merge_poll(
            m_replay_merge_t,
            doPoll<handler_type>,
            const_cast<void *>(reinterpret_cast<const void *>(&fragmentHandler)),
            fragmentLimit);

        if (rc < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return rc;
    }

    inline std::shared_ptr<Image> image()
    {
        if (nullptr == m_image)
        {
            aeron_image_t *image = aeron_archive_replay_merge_image(m_replay_merge_t);

            if (nullptr == image)
            {
                return nullptr;
            }

            m_image = std::make_shared<Image>(m_subscription->subscription(), image);
        }

        return m_image;
    }

    inline bool isMerged() const
    {
        return aeron_archive_replay_merge_is_merged(m_replay_merge_t);
    }

    inline bool hasFailed() const
    {
        return aeron_archive_replay_merge_has_failed(m_replay_merge_t);
    }

    inline bool isLiveAdded() const
    {
        return aeron_archive_replay_merge_is_live_added(m_replay_merge_t);
    }

private:
    std::shared_ptr<Subscription> m_subscription;
    std::shared_ptr<AeronArchive> m_archive;
    aeron_archive_replay_merge_t *m_replay_merge_t;

    std::shared_ptr<Image> m_image;
};

}}}

#endif //AERON_ARCHIVE_WRAPPER_REPLAY_MERGE_H
