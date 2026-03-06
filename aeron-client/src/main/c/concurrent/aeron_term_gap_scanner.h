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

#ifndef AERON_TERM_GAP_SCANNER_H
#define AERON_TERM_GAP_SCANNER_H

#include "protocol/aeron_udp_protocol.h"
#include "util/aeron_bitutil.h"
#include "aeron_logbuffer_descriptor.h"

inline int32_t aeron_term_gap_scanner_scan_for_gap(
    const uint8_t *buffer,
    int32_t offset,
    int32_t limit_offset,
    int32_t *gap_length)
{
    do
    {
        aeron_frame_header_t *hdr = (aeron_frame_header_t *)(buffer + offset);
        int32_t frame_length;

        AERON_GET_ACQUIRE(frame_length, hdr->frame_length);
        if (frame_length == 0)
        {
            break;
        }

        if (frame_length < 0)
        {
            frame_length = -frame_length;
        }

        offset += AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    } while (offset < limit_offset);

    const int32_t gap_begin_offset = offset;
    if (gap_begin_offset < limit_offset)
    {
        offset = gap_begin_offset + AERON_DATA_HEADER_LENGTH;
        while (offset < limit_offset)
        {
            aeron_frame_header_t *hdr = (aeron_frame_header_t *)(buffer + offset);
            int32_t frame_length;
            AERON_GET_ACQUIRE(frame_length, hdr->frame_length);

            if (0 != frame_length)
            {
                break;
            }
            offset += AERON_DATA_HEADER_LENGTH;
        }

        const int32_t next_frame_offset = offset;

        // we loop again to make sure this is indeed the next frame header

        offset = gap_begin_offset + AERON_DATA_HEADER_LENGTH;
        while (offset < limit_offset)
        {
            aeron_frame_header_t *hdr = (aeron_frame_header_t *)(buffer + offset);
            int32_t frame_length;
            AERON_GET_ACQUIRE(frame_length, hdr->frame_length);

            if (0 != frame_length)
            {
                break;
            }
            offset += AERON_DATA_HEADER_LENGTH;
        }

        if (next_frame_offset != offset)
        {
            // we failed to find next_frame_offset due to concurrent changes
            // will try next time
            return -1;
        }

        *gap_length = next_frame_offset - gap_begin_offset;
    }

    return gap_begin_offset;
}

#endif // AERON_TERM_GAP_SCANNER_H
