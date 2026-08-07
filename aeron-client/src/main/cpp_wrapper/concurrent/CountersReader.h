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
#ifndef AERON_COUNTERS_READER_H
#define AERON_COUNTERS_READER_H

#include <functional>

#include "util/Exceptions.h"
#include "util/BitUtil.h"
#include "concurrent/AtomicBuffer.h"

#include "aeronc.h"

namespace aeron { namespace concurrent {

/**
 * Reads the counters metadata and values buffers.
 *
 * This class is threadsafe.
 *
 * <b>Values Buffer</b>
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                        Counter Value                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                       Registration Id                         |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                          Owner Id                             |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                     104 bytes of padding                     ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                   Repeats to end of buffer                   ...
 *  |                                                               |
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 *
 * <b>Meta Data Buffer</b>
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                        Record State                           |
 *  +---------------------------------------------------------------+
 *  |                          Type Id                              |
 *  +---------------------------------------------------------------+
 *  |                  Free-for-reuse Deadline (ms)                 |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                      112 bytes for key                       ...
 * ...                                                              |
 *  +-+-------------------------------------------------------------+
 *  |R|                      Label Length                           |
 *  +-+-------------------------------------------------------------+
 *  |                  380 bytes of Label in ASCII                 ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                   Repeats to end of buffer                   ...
 *  |                                                               |
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */

typedef std::function<void(std::int32_t, std::int32_t, const AtomicBuffer&, const std::string&)> on_counters_metadata_t;

using namespace aeron::util;

class CountersReader
{

public:
    inline explicit CountersReader(aeron_counters_reader_t *countersReader) :
        m_countersReader(countersReader), m_buffers{}
    {
        aeron_counters_reader_get_buffers(m_countersReader, &m_buffers);
    }

    template <typename F>
    void forEach(F &&onCountersMetadata) const
    {
        using handler_type = typename std::remove_reference<F>::type;
        handler_type &handler = onCountersMetadata;
        void *handler_ptr = const_cast<void *>(reinterpret_cast<const void *>(&handler));

        aeron_counters_reader_foreach_counter(m_countersReader, forEachCounter<handler_type>, handler_ptr);
    }

    inline std::int32_t findByTypeIdAndRegistrationId(
        const std::int32_t typeId, const std::int64_t registrationId) const
    {
        std::int32_t id = NULL_COUNTER_ID;

        forEach(
            [&](std::int32_t counterId, std::int32_t counterTypeId, const AtomicBuffer &keyBuffer, const std::string &label)
            {
                if (NULL_COUNTER_ID == id && typeId == counterTypeId && registrationId == getCounterRegistrationId(counterId))
                {
                    id = counterId;
                }
            });

        return id;
    }

    inline std::int32_t findByRegistrationId(const std::int64_t registrationId) const
    {
        std::int32_t id = NULL_COUNTER_ID;

        forEach(
            [&](std::int32_t counterId, std::int32_t counterTypeId, const AtomicBuffer &keyBuffer, const std::string &label)
            {
                if (NULL_COUNTER_ID == id && registrationId == getCounterRegistrationId(counterId))
                {
                    id = counterId;
                }
            });

        return id;
    }

    inline std::int32_t maxCounterId() const
    {
        return aeron_counters_reader_max_counter_id(m_countersReader);
    }

    inline std::int64_t getCounterValue(std::int32_t id) const
    {
        validateCounterId(id);
        std::int64_t *counter_addr = getCounterAddress(id);
        return *counter_addr;
    }

    /// @cond HIDDEN_SYMBOLS
    inline std::int64_t *getCounterAddress(std::int32_t id) const
    {
        return aeron_counters_reader_addr(m_countersReader, id);
    }
    /// @endcond

    inline std::int64_t getCounterRegistrationId(std::int32_t id) const
    {
        validateCounterId(id);

        std::int64_t registrationId;
        if (aeron_counters_reader_counter_registration_id(m_countersReader, id, &registrationId) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return registrationId;
    }

    inline std::int64_t getCounterOwnerId(std::int32_t id) const
    {
        validateCounterId(id);

        std::int64_t ownerId;
        if (aeron_counters_reader_counter_owner_id(m_countersReader, id, &ownerId) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return ownerId;
    }

    inline std::int32_t getCounterState(std::int32_t id) const
    {
        std::int32_t state;
        if (aeron_counters_reader_counter_state(m_countersReader, id, &state) < 0)
        {
            throw util::IllegalArgumentException(
                "counter id " + std::to_string(id) +
                " out of range: maxCounterId=" + std::to_string(maxCounterId()),
                SOURCEINFO, EINVAL);
        }

        return state;
    }

    inline std::int32_t getCounterTypeId(std::int32_t id) const
    {
        std::int32_t typeId;
        if (aeron_counters_reader_counter_type_id(m_countersReader, id, &typeId) < 0)
        {
            throw util::IllegalArgumentException(
                "counter id " + std::to_string(id) +
                " out of range: maxCounterId=" + std::to_string(maxCounterId()),
                SOURCEINFO, EINVAL);
        }

        return typeId;
    }

    inline std::int64_t getFreeForReuseDeadline(std::int32_t id) const
    {
        std::int64_t deadline;
        if (aeron_counters_reader_free_for_reuse_deadline_ms(m_countersReader, id, &deadline))
        {
            throw util::IllegalArgumentException(
                "counter id " + std::to_string(id) +
                " out of range: maxCounterId=" + std::to_string(maxCounterId()),
                SOURCEINFO, EINVAL);
        }

        return deadline;
    }

    inline std::string getCounterLabel(std::int32_t id) const
    {
        char buffer[AERON_COUNTER_MAX_LABEL_LENGTH];
        int length = aeron_counters_reader_counter_label(m_countersReader, id, buffer, sizeof(buffer));
        if (length < 0)
        {
            throw util::IllegalArgumentException(
                "counter id " + std::to_string(id) +
                " out of range: maxCounterId=" + std::to_string(maxCounterId()),
                SOURCEINFO, EINVAL);
        }

        return { buffer, static_cast<std::size_t>(length) };
    }

    inline static util::index_t counterOffset(std::int32_t counterId)
    {
        return AERON_COUNTER_OFFSET(counterId);
    }

    inline static util::index_t metadataOffset(std::int32_t counterId)
    {
        return AERON_COUNTER_METADATA_OFFSET(counterId);
    }

    inline AtomicBuffer valuesBuffer() const
    {
        return { m_buffers.values, m_buffers.values_length };
    }

    inline AtomicBuffer metaDataBuffer() const
    {
        return { m_buffers.metadata, m_buffers.metadata_length };
    }

    static constexpr std::int32_t NULL_COUNTER_ID = AERON_NULL_COUNTER_ID;

    static constexpr std::int32_t RECORD_UNUSED = AERON_COUNTER_RECORD_UNUSED;
    static constexpr std::int32_t RECORD_ALLOCATED = AERON_COUNTER_RECORD_ALLOCATED;
    static constexpr std::int32_t RECORD_RECLAIMED = AERON_COUNTER_RECORD_RECLAIMED;

    static constexpr std::int64_t DEFAULT_REGISTRATION_ID = AERON_COUNTER_REGISTRATION_ID_DEFAULT;
    static constexpr std::int64_t NOT_FREE_TO_REUSE = AERON_COUNTER_NOT_FREE_TO_REUSE;

    static constexpr util::index_t COUNTER_LENGTH = AERON_COUNTER_VALUE_LENGTH;
    static constexpr util::index_t REGISTRATION_ID_OFFSET = AERON_COUNTER_REGISTRATION_ID_OFFSET;

    static constexpr util::index_t METADATA_LENGTH = AERON_COUNTER_METADATA_LENGTH;
    static constexpr util::index_t TYPE_ID_OFFSET = AERON_COUNTER_TYPE_ID_OFFSET;
    static constexpr util::index_t FREE_FOR_REUSE_DEADLINE_OFFSET = AERON_COUNTER_FREE_FOR_REUSE_DEADLINE_OFFSET;
    static constexpr util::index_t KEY_OFFSET = AERON_COUNTER_KEY_OFFSET;
    static constexpr util::index_t LABEL_LENGTH_OFFSET = AERON_COUNTER_LABEL_LENGTH_OFFSET;

    static constexpr std::int32_t MAX_LABEL_LENGTH = AERON_COUNTER_MAX_LABEL_LENGTH;
    static constexpr std::int32_t MAX_KEY_LENGTH = AERON_COUNTER_MAX_KEY_LENGTH;

    inline aeron_counters_reader_t *countersReader() const
    {
        return m_countersReader;
    }

protected:
    aeron_counters_reader_t *m_countersReader;
    aeron_counters_reader_buffers_t m_buffers;

    void validateCounterId(std::int32_t counterId) const
    {
        if (counterId < 0 || counterId > maxCounterId())
        {
            throw util::IllegalArgumentException(
                "counter id " + std::to_string(counterId) +
                " out of range: maxCounterId=" + std::to_string(maxCounterId()),
                SOURCEINFO, EINVAL);
        }
    }

    template<typename H>
    static void forEachCounter(
        std::int64_t /* value */,
        std::int32_t id,
        std::int32_t typeId,
        const std::uint8_t *key,
        std::size_t key_length,
        const char *label,
        std::size_t label_length,
        void *clientd)
    {
        H &handler = *reinterpret_cast<H *>(clientd);
        AtomicBuffer keyBuffer = { const_cast<std::uint8_t *>(key), key_length };
        std::string labelStr = { const_cast<char *>(label), label_length };

        handler(id, typeId, keyBuffer, labelStr);
    }
};

}}

#endif
