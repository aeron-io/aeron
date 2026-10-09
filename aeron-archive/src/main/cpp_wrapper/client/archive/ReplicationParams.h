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
#ifndef AERON_ARCHIVE_WRAPPER_REPLICATION_PARAMS_H
#define AERON_ARCHIVE_WRAPPER_REPLICATION_PARAMS_H

#include "AeronArchive.h"

namespace aeron { namespace archive { namespace client
{

class ReplicationParams
{
public:
    friend class AeronArchive;

    ReplicationParams()
    {
        aeron_archive_replication_params_init(&m_params);
        liveDestination("");
        replicationChannel("");
    }

    std::int64_t stopPosition() const
    {
        return m_params.stop_position;
    }

    ReplicationParams& stopPosition(std::int64_t stopPosition)
    {
        m_params.stop_position = stopPosition;
        return *this;
    }

    std::int64_t dstRecordingId() const
    {
        return m_params.dst_recording_id;
    }

    ReplicationParams &dstRecordingId(std::int64_t dstRecordingId)
    {
        m_params.dst_recording_id = dstRecordingId;
        return *this;
    }

    const std::string &liveDestination() const
    {
        return m_liveDestination;
    }

    ReplicationParams &liveDestination(const std::string &liveDestination)
    {
        m_liveDestination = liveDestination;
        m_params.live_destination = m_liveDestination.c_str();
        return *this;
    }

    const std::string &replicationChannel() const
    {
        return m_replicationChannel;
    }

    ReplicationParams &replicationChannel(const std::string &replicationChannel)
    {
        m_replicationChannel = replicationChannel;
        m_params.replication_channel = m_replicationChannel.c_str();
        return *this;
    }

    std::int64_t channelTagId() const
    {
        return m_params.channel_tag_id;
    }

    ReplicationParams &channelTagId(std::int64_t channelTagId)
    {
        m_params.channel_tag_id = channelTagId;
        return *this;
    }

    std::int64_t subscriptionTagId() const
    {
        return m_params.subscription_tag_id;
    }

    ReplicationParams &subscriptionTagId(std::int64_t subscriptionTagId)
    {
        m_params.subscription_tag_id = subscriptionTagId;
        return *this;
    }

    std::int32_t fileIoMaxLength() const
    {
        return m_params.file_io_max_length;
    }

    ReplicationParams &fileIoMaxLength(std::int32_t fileIoMaxLength)
    {
        m_params.file_io_max_length = fileIoMaxLength;
        return *this;
    }

    std::int32_t replicationSessionId() const
    {
        return m_params.replication_session_id;
    }

    ReplicationParams &replicationSessionId(std::int32_t replicationSessionId)
    {
        m_params.replication_session_id = replicationSessionId;
        return *this;
    }

    std::pair<const char *, std::uint32_t> encodedCredentials() const
    {
        return m_encodedCredentials;
    }

    ReplicationParams &encodedCredentials(std::pair<const char *, std::uint32_t> encodedCredentials)
    {
        m_encodedCredentials = encodedCredentials;
        m_encoded_credentials_t = { m_encodedCredentials.first, m_encodedCredentials.second };
        m_params.encoded_credentials = &m_encoded_credentials_t;
        return *this;
    }

private:
    aeron_archive_replication_params_t m_params;

    std::string m_liveDestination;
    std::string m_replicationChannel;

    std::pair<const char *, std::uint32_t> m_encodedCredentials = {};
    aeron_archive_encoded_credentials_t m_encoded_credentials_t = {};

};

}}}

#endif //AERON_ARCHIVE_WRAPPER_REPLICATION_PARAMS_H
