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

#include "protocol/aeron_udp_protocol.h"
#include "aeron_timestamps.h"
#include "aeron_udp_channel.h"

void aeron_timestamps_set_timestamp(
    struct timespec *timestamp,
    int32_t offset,
    uint8_t *frame,
    size_t frame_length)
{
    aeron_data_header_t *data_header = (aeron_data_header_t *)frame;

    if (frame_length >= sizeof(aeron_data_header_t) &&
        AERON_HDR_TYPE_DATA == data_header->frame_header.type &&
        AERON_DATA_HEADER_BEGIN_FLAG & data_header->frame_header.flags &&
        0 != data_header->frame_header.frame_length)
    {
        size_t body_length = frame_length - sizeof(aeron_data_header_t);
        uint8_t *body_buffer = frame + sizeof(aeron_data_header_t);
        int64_t timestamp_ns = (INT64_C(1000) * 1000 * 1000 * timestamp->tv_sec) + timestamp->tv_nsec;

        if (AERON_UDP_CHANNEL_RESERVED_VALUE_OFFSET == offset)
        {
            data_header->reserved_value = timestamp_ns;
        }
        else if (0 <= offset &&
            offset <= (int32_t)(body_length - sizeof(timestamp_ns)) &&
            offset <= (int32_t)((data_header->frame_header.frame_length - sizeof(*data_header)) - sizeof(timestamp_ns)))
        {
            memcpy(body_buffer + (size_t)offset, &timestamp_ns, sizeof(timestamp_ns));
        }
    }
}
