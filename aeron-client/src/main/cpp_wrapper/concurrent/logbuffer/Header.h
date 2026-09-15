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

extern "C"
{
#include "protocol/aeron_udp_protocol.h"
}

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
        return aeron_header_initial_term_id(m_header);
    }

    /**
     * The total length of the frame including the header.
     *
     * @return the total length of the frame including the header.
     */
    inline std::int32_t frameLength() const
    {
        return m_header->frame->frame_header.frame_length;
    }

    /**
     * The session ID to which the frame belongs.
     *
     * @return the session ID to which the frame belongs.
     */
    inline std::int32_t sessionId() const
    {
        return m_header->frame->session_id;
    }

    /**
     * The stream ID to which the frame belongs.
     *
     * @return the stream ID to which the frame belongs.
     */
    inline std::int32_t streamId() const
    {
        return m_header->frame->stream_id;
    }

    /**
     * The term ID to which the frame belongs.
     *
     * @return the term ID to which the frame belongs.
     */
    inline std::int32_t termId() const
    {
        return m_header->frame->term_id;
    }

    /**
     * The offset in the term at which the frame begins. This will be the same as {@link #offset()}
     *
     * @return the offset in the term at which the frame begins.
     */
    inline std::int32_t termOffset() const
    {
        return m_header->frame->term_offset;
    }

    /**
     * The type of the the frame which should always be {@link DataFrameHeader::HDR_TYPE_DATA}
     *
     * @return type of the the frame which should always be {@link DataFrameHeader::HDR_TYPE_DATA}
     */
    inline std::uint16_t type() const
    {
        // C and Java API declare this as int16_t.
        return static_cast<std::uint16_t>(m_header->frame->frame_header.type);
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
        return m_header->frame->frame_header.flags;
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
        return static_cast<std::int32_t>(aeron_header_position_bits_to_shift(m_header));
    }

    /**
     * Get the value stored in the reserve space at the end of a data frame header.
     *
     * @return the value stored in the reserve space at the end of a data frame header.
     */
    inline std::int64_t reservedValue() const
    {
        return m_header->frame->reserved_value;
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
    aeron_header_t *m_header = nullptr;
};

}}}

#endif
