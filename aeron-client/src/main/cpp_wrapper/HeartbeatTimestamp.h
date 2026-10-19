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

#ifndef AERON_HEARTBEAT_TIMESTAMP_H
#define AERON_HEARTBEAT_TIMESTAMP_H

#include "concurrent/CountersReader.h"

namespace aeron
{

/**
 * The heartbeat as a timestamp for an entity to indicate liveness.
 * <p>
 * Key has the following layout:
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                       Registration ID                         |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 * </pre>
 */
namespace HeartbeatTimestamp
{

/// Counter type id of a client heartbeat timestamp
static constexpr std::int32_t CLIENT_HEARTBEAT_TYPE_ID = 11;

#pragma pack(push)
#pragma pack(4)
struct HeartbeatTimestampKeyDefn
{
    std::int64_t registrationId;
};
#pragma pack(pop)

/**
 * Find the active counter id for an entity with a type id and registration id.
 *
 * @param countersReader to search within.
 * @param counterTypeId  to match against.
 * @param registrationId for the active entity.
 * @return the counter id if found otherwise #NULL_COUNTER_ID.
 */
inline static std::int32_t findCounterIdByRegistrationId(
    CountersReader &countersReader, std::int32_t counterTypeId, std::int64_t registrationId)
{
    AtomicBuffer buffer = countersReader.metaDataBuffer();

    for (std::int32_t i = 0, maxId = countersReader.maxCounterId(); i <= maxId; i++)
    {
        if (countersReader.getCounterState(i) == CountersReader::RECORD_ALLOCATED)
        {
            const util::index_t recordOffset = CountersReader::metadataOffset(i);
            auto key = buffer.overlayStruct<HeartbeatTimestampKeyDefn>(
                recordOffset + CountersReader::KEY_OFFSET);

            if (registrationId == key.registrationId &&
                buffer.getInt32(recordOffset + CountersReader::TYPE_ID_OFFSET) == counterTypeId)
            {
                return i;
            }
        }
    }

    return CountersReader::NULL_COUNTER_ID;
}

/**
 * Is the counter still active.
 *
 * @param countersReader to search within.
 * @param counterId      to search for.
 * @param counterTypeId  to match for counter type.
 * @param registrationId to match the entity key.
 * @return true if the counter is still active otherwise false.
 */
inline static bool isActive(
    CountersReader &countersReader, std::int32_t counterId, std::int32_t counterTypeId, std::int64_t registrationId)
{
    // getCounterTypeId validates the counter id before the metadata is read
    if (countersReader.getCounterTypeId(counterId) != counterTypeId)
    {
        return false;
    }

    AtomicBuffer buffer = countersReader.metaDataBuffer();
    const util::index_t recordOffset = CountersReader::metadataOffset(counterId);
    auto key = buffer.overlayStruct<HeartbeatTimestampKeyDefn>(recordOffset + CountersReader::KEY_OFFSET);

    return
        registrationId == key.registrationId &&
        countersReader.getCounterState(counterId) == CountersReader::RECORD_ALLOCATED;
}

}
}
#endif //AERON_HEARTBEAT_TIMESTAMP_H
