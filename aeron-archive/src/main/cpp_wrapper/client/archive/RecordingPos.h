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
#ifndef AERON_ARCHIVE_WRAPPER_RECORDING_POS_H
#define AERON_ARCHIVE_WRAPPER_RECORDING_POS_H

#include "AeronArchive.h"

namespace aeron { namespace archive { namespace client
{

namespace RecordingPos
{

inline static std::int32_t findCounterIdByRecordingId(
    CountersReader &countersReader, std::int64_t recordingId, std::int64_t archiveId)
{
    return aeron_archive_recording_pos_find_counter_id_by_recording_id(countersReader.countersReader(), recordingId, archiveId);
}

inline static std::int32_t findCounterIdBySessionId(
    CountersReader &countersReader, std::int32_t sessionId, std::int64_t archiveId)
{
    return aeron_archive_recording_pos_find_counter_id_by_session_id(countersReader.countersReader(), sessionId, archiveId);
}

inline static std::int64_t getRecordingId(CountersReader &countersReader, std::int32_t counterId)
{
    return aeron_archive_recording_pos_get_recording_id(countersReader.countersReader(), counterId);
}

inline static std::string getSourceIdentity(CountersReader &countersReader, std::int32_t counterId)
{
    const size_t initial_sib_len = AERON_COUNTER_MAX_LABEL_LENGTH;
    char source_identity_buffer[initial_sib_len] = { '\0' };
    size_t sib_len = initial_sib_len;

    if (aeron_archive_recording_pos_get_source_identity(
        countersReader.countersReader(),
        counterId,
        source_identity_buffer,
        &sib_len) < 0)
    {
        ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
    }

    return { source_identity_buffer, sib_len };
}

inline static bool isActive(CountersReader &countersReader, std::int32_t counterId, std::int64_t recordingId)
{
    bool isActive;

    if (aeron_archive_recording_pos_is_active(&isActive, countersReader.countersReader(), counterId, recordingId) < 0)
    {
        ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
    }

    return isActive;
}

}

}}}

#endif //AERON_ARCHIVE_WRAPPER_RECORDING_POS_H
