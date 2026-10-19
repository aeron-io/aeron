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
    struct timespec *timestamp, int32_t timestamp_offset, aeron_data_header_t *data_header)
{
    int64_t timestamp_ns = INT64_C(1000000000) * timestamp->tv_sec + timestamp->tv_nsec;

    if (AERON_UDP_CHANNEL_RESERVED_VALUE_OFFSET == timestamp_offset)
    {
        data_header->reserved_value = timestamp_ns;
    }
    else
    {
        int32_t body_length = data_header->frame_header.frame_length - (int32_t)AERON_DATA_HEADER_LENGTH;
        if (0 <= timestamp_offset && timestamp_offset <= body_length - (int32_t)sizeof(timestamp_ns))
        {
            memcpy(
                (uint8_t *)data_header + AERON_DATA_HEADER_LENGTH + (size_t)timestamp_offset,
                &timestamp_ns,
                sizeof(timestamp_ns));
        }
    }
}
