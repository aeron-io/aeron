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

#ifndef AERON_CONCURRENT_LOGBUFFER_HEADER_H
#define AERON_CONCURRENT_LOGBUFFER_HEADER_H

#include "util/Index.h"
#include "util/BitUtil.h"
#include "util/Exceptions.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/logbuffer/DataFrameHeader.h"

#include "aeronc.h"

namespace aeron { namespace concurrent { namespace logbuffer {

using namespace aeron::util;

/**
 * Represents the header of the data frame for accessing meta data fields.
 */
class Header
{
public:
    explicit Header(aeron_header_t *header) : m_header(header)
    {
        if (nullptr == m_header)
        {
            // Values are read lazily, see values(), so a null header is rejected up front rather than
            // on first use, keeping the accessors free of null checks.
            throw IllegalArgumentException("header must not be null", SOURCEINFO, EINVAL);
        }
    }

    /**
     * Get the initial term id this stream started at.
     *
     * @return the initial term id this stream started at.
     */
    inline std::int32_t initialTermId() const
    {
        return values().initial_term_id;
    }

    /**
     * The total length of the frame including the header.
     *
     * @return the total length of the frame including the header.
     */
    inline std::int32_t frameLength() const
    {
        return values().frame.frame_length;
    }

    /**
     * The session ID to which the frame belongs.
     *
     * @return the session ID to which the frame belongs.
     */
    inline std::int32_t sessionId() const
    {
        return values().frame.session_id;
    }

    /**
     * The stream ID to which the frame belongs.
     *
     * @return the stream ID to which the frame belongs.
     */
    inline std::int32_t streamId() const
    {
        return values().frame.stream_id;
    }

    /**
     * The term ID to which the frame belongs.
     *
     * @return the term ID to which the frame belongs.
     */
    inline std::int32_t termId() const
    {
        return values().frame.term_id;
    }

    /**
     * The offset in the term at which the frame begins. This will be the same as {@link #offset()}
     *
     * @return the offset in the term at which the frame begins.
     */
    inline std::int32_t termOffset() const
    {
        return values().frame.term_offset;
    }

    /**
     * The type of the the frame which should always be {@link DataFrameHeader::HDR_TYPE_DATA}
     *
     * @return type of the the frame which should always be {@link DataFrameHeader::HDR_TYPE_DATA}
     */
    inline std::uint16_t type() const
    {
        // C and Java API declare this as int16_t.
        return static_cast<std::uint16_t>(values().frame.type);
    }

    /**
     * The flags for this frame. Valid flags are {@link DataFrameHeader::BEGIN_FLAG}
     * and {@link DataFrameHeader::END_FLAG}. A convenience flag {@link DataFrameHeader::BEGIN_AND_END_FLAGS}
     * can be used for both flags.
     *
     * @return the flags for this frame.
     */
    inline std::uint8_t flags() const
    {
        return values().frame.flags;
    }

    /**
     * Get the current position to which the Image has advanced on reading this message.
     *
     * @return the current position to which the Image has advanced on reading this message.
     */
    inline std::int64_t position() const
    {
        return aeron_header_position(m_header);
    }

    /**
     * The number of times to left shift the term count to multiply by term length.
     *
     * @return number of times to left shift the term count to multiply by term length.
     */
    inline std::int32_t positionBitsToShift() const
    {
        return static_cast<std::int32_t>(values().position_bits_to_shift);
    }

    /**
     * Get the value stored in the reserve space at the end of a data frame header.
     *
     * @return the value stored in the reserve space at the end of a data frame header.
     */
    inline std::int64_t reservedValue() const
    {
        return values().frame.reserved_value;
    }

    /**
     * Get a pointer to the context associated with this message. Only valid during poll handling. Is normally a
     * pointer to an Image instance.
     *
     * @return a pointer to the context associated with this message.
     */
    inline void *context() const
    {
        return aeron_header_context(m_header);
    }

    aeron_header_t *hdr()
    {
        return m_header;
    }

private:
    // The frame values are copied out of the log buffer on first use rather than on construction,
    // because a fragment handler that only needs the payload never reads them. The copy is taken once
    // and is then stable for the lifetime of this object, which, like the aeron_header_t it wraps, is
    // the duration of the poll callback.
    inline const aeron_header_values_t &values() const
    {
        if (AERON_COND_EXPECT(!m_valuesLoaded, false))
        {
            // Cannot fail: the constructor has already rejected a null header.
            aeron_header_values(m_header, &m_headerValues);
            m_valuesLoaded = true;
        }

        return m_headerValues;
    }

    aeron_header_t *m_header = nullptr;
    mutable aeron_header_values_t m_headerValues = {};
    mutable bool m_valuesLoaded = false;
};

}}}

#endif
