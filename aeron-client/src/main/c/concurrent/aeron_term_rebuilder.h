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

#ifndef AERON_TERM_REBUILDER_H
#define AERON_TERM_REBUILDER_H

#include <string.h>
#include "protocol/aeron_udp_protocol.h"
#include "aeron_atomic.h"
#include "aeron_logbuffer_descriptor.h"

inline void aeron_term_rebuilder_insert(uint8_t *dest, const uint8_t *src, size_t length)
{
    for (size_t offset = 0; offset < length;)
    {
        aeron_data_header_t *src_hdr = (aeron_data_header_t *)(src + offset);
        aeron_data_header_t *dest_hdr = (aeron_data_header_t *)(dest + offset);
        aeron_data_header_as_longs_t *src_hdr_as_longs = (aeron_data_header_as_longs_t *)(src + offset);
        aeron_data_header_as_longs_t *dest_hdr_as_longs = (aeron_data_header_as_longs_t *)(dest + offset);

        int32_t frame_length = src_hdr->frame_header.frame_length;

        if (0 == dest_hdr->frame_header.frame_length)
        {
            AERON_SET_RELEASE(dest_hdr->frame_header.frame_length, (-(int32_t)frame_length));
            aeron_release();

            memcpy(dest + offset + AERON_DATA_HEADER_LENGTH, src + offset + AERON_DATA_HEADER_LENGTH, frame_length - AERON_DATA_HEADER_LENGTH);

            dest_hdr_as_longs->hdr[3] = src_hdr_as_longs->hdr[3];
            dest_hdr_as_longs->hdr[2] = src_hdr_as_longs->hdr[2];
            dest_hdr_as_longs->hdr[1] = src_hdr_as_longs->hdr[1];

            AERON_SET_RELEASE(dest_hdr_as_longs->hdr[0], src_hdr_as_longs->hdr[0]);
        }

        offset += AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    }
}

#endif //AERON_TERM_REBUILDER_H
