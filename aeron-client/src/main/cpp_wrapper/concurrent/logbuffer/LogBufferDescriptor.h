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

#ifndef AERON_CONCURRENT_LOGBUFFER_DESCRIPTOR_H
#define AERON_CONCURRENT_LOGBUFFER_DESCRIPTOR_H

#include "util/Index.h"
#include "util/StringUtil.h"
#include "util/BitUtil.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/logbuffer/FrameDescriptor.h"
#include "concurrent/logbuffer/DataFrameHeader.h"

namespace aeron { namespace concurrent { namespace logbuffer { namespace LogBufferDescriptor
{

const std::int32_t TERM_MIN_LENGTH = 64 * 1024;
const std::int32_t TERM_MAX_LENGTH = 1024 * 1024 * 1024;

#if defined(__GNUC__) || _MSC_VER >= 1900
static constexpr const int PARTITION_COUNT = 3;
#else
// Visual Studio 2013 doesn't like constexpr without an update
// https://msdn.microsoft.com/en-us/library/vstudio/hh567368.aspx
// https://www.microsoft.com/en-us/download/details.aspx?id=41151
static const int PARTITION_COUNT = 3;
#endif

/*
 * Layout description for log buffers which contains partitions of terms with associated term meta data,
 * plus ending with overall log meta data.
 *
 * <pre>
 *  +----------------------------+
 *  |           Term 0           |
 *  +----------------------------+
 *  |           Term 1           |
 *  +----------------------------+
 *  |           Term 2           |
 *  +----------------------------+
 *  |        Log Meta Data       |
 *  +----------------------------+
 * </pre>
 */

const util::index_t LOG_META_DATA_SECTION_INDEX = PARTITION_COUNT;

/**
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                       Tail Counter 0                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                       Tail Counter 1                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                       Tail Counter 2                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                      Active Term Count                        |
 *  +---------------------------------------------------------------+
 *  |                      Cache Line Padding                      ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                    End of Stream Position                     |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        Is Connected                           |
 *  +---------------------------------------------------------------+
 *  |                    Active Transport Count                     |
 *  +---------------------------------------------------------------+
 *  |                      Cache Line Padding                      ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                 Registration / Correlation ID                 |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        Initial Term Id                        |
 *  +---------------------------------------------------------------+
 *  |                  Default Frame Header Length                  |
 *  +---------------------------------------------------------------+
 *  |                          MTU Length                           |
 *  +---------------------------------------------------------------+
 *  |                         Term Length                           |
 *  +---------------------------------------------------------------+
 *  |                          Page Size                            |
 *  +---------------------------------------------------------------+
 *  |                      Cache Line Padding                      ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                    Default Frame Header                      ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */

inline void checkTermLength(std::int32_t termLength)
{
    if (termLength < TERM_MIN_LENGTH)
    {
        throw util::IllegalStateException(
            "term length less than min size of " + std::to_string(TERM_MIN_LENGTH) +
            ", length=" + std::to_string(termLength), SOURCEINFO, EINVAL);
    }

    if (termLength > TERM_MAX_LENGTH)
    {
        throw util::IllegalStateException(
            "term length greater than max size of " + std::to_string(TERM_MAX_LENGTH) +
            ", length=" + std::to_string(termLength), SOURCEINFO, EINVAL);
    }

    if (!util::BitUtil::isPowerOfTwo(termLength))
    {
        throw util::IllegalStateException(
            "term length not a power of 2, length=" + std::to_string(termLength), SOURCEINFO, EINVAL);
    }
}

inline std::int32_t computeTermCount(std::int32_t termId, std::int32_t initialTermId) noexcept
{
    const std::int64_t difference = static_cast<std::int64_t>(termId) - static_cast<std::int64_t>(initialTermId);
    return static_cast<std::int32_t>(difference & 0xFFFFFFFF);
}

inline std::int64_t computePosition(
    std::int32_t activeTermId,
    std::int32_t termOffset,
    std::int32_t positionBitsToShift,
    std::int32_t initialTermId) noexcept
{
    const auto termCount = static_cast<std::int64_t>(computeTermCount(activeTermId, initialTermId));
    return (termCount << positionBitsToShift) + termOffset;
}

/**
 * Compute frame length for a message that is fragmented into chunks of {@code maxPayloadSize}.
 *
 * @param length of the message.
 * @param maxPayloadSize fragment size without the header.
 * @return message length after fragmentation.
 */
inline static util::index_t computeFragmentedFrameLength(
    const util::index_t length,
    const util::index_t maxPayloadLength)
{
    const int numMaxPayloads = length / maxPayloadLength;
    const util::index_t remainingPayload = length % maxPayloadLength;
    const util::index_t lastFrameLength = remainingPayload > 0 ?
        util::BitUtil::align(remainingPayload + DataFrameHeader::LENGTH, FrameDescriptor::FRAME_ALIGNMENT) : 0;

    return (numMaxPayloads * (maxPayloadLength + DataFrameHeader::LENGTH)) + lastFrameLength;
}

}

}}}

#endif
