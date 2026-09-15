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

#include "ControlledFragmentAssemblerTestFixture.h"

extern "C"
{
#include "aeron_image.h"
}

void initHeader(std::uint8_t *buffer, size_t length, Header **header)
{
    auto *aeronHeader = new aeron_header_internal_t{};
    aeronHeader->base.frame = reinterpret_cast<aeron_data_header_t *>(buffer);
    aeronHeader->fragmented_frame_length = NULL_VALUE;
    aeronHeader->initial_term_id = INITIAL_TERM_ID;
    aeronHeader->position_bits_to_shift = POSITION_BITS_TO_SHIFT;
    aeronHeader->context = (void*)"test context";

    *header = new Header{&aeronHeader->base};
}

void freeHeader(Header *header)
{
    delete AERON_HEADER_INTERNAL(header->hdr());
    delete header;
}

void fillHeader(Header &header, std::int32_t termOffset, std::uint8_t flags, std::int32_t payloadLength)
{
    aeron_data_header_t *frame = header.hdr()->frame;

    frame->frame_header.frame_length = DataFrameHeader::LENGTH + payloadLength;
    frame->frame_header.version = DataFrameHeader::CURRENT_VERSION;
    frame->frame_header.flags = flags;
    frame->frame_header.type = DataFrameHeader::HDR_TYPE_DATA;
    frame->term_offset = termOffset;
    frame->session_id = SESSION_ID;
    frame->stream_id = STREAM_ID;
    frame->term_id = ACTIVE_TERM_ID;
}

INSTANTIATE_TEST_SUITE_P(
    ControlledFragmentAssemblerParameterisedTest,
    ControlledFragmentAssemblerParameterisedTest,
    testing::Values(std::make_tuple(initHeader, freeHeader, fillHeader)));