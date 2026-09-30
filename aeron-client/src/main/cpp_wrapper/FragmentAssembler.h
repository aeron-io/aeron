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

#ifndef AERON_FRAGMENT_ASSEMBLER_H
#define AERON_FRAGMENT_ASSEMBLER_H

#include <unordered_map>

#include "Aeron.h"

namespace aeron
{

static constexpr std::size_t DEFAULT_FRAGMENT_ASSEMBLY_BUFFER_LENGTH = 4096;

/**
 * A handler that sits in a chain-of-responsibility pattern that reassembles fragmented messages
 * so that the next handler in the chain only sees whole messages.
 * <p>
 * Unfragmented messages are delegated without copy. Fragmented messages are copied to a temporary
 * buffer for reassembly before delegation.
 * <p>
 * The Header passed to the delegate on assembling a message will be that of the last fragment.
 * <p>
 * Session based buffers will be allocated and grown as necessary based on the length of messages to be assembled.
 * When sessions go inactive see {@link on_unavailable_image_t}, it is possible to free the buffer by calling
 * {@link #deleteSessionBuffer(std::int32_t)}.
 */
class FragmentAssembler
{
public:

    /**
     * Construct an adapter to reassembly message fragments and delegate on only whole messages.
     *
     * @param delegate            onto which whole messages are forwarded.
     * @param initialBufferLength to be used for each session.
     */
    explicit FragmentAssembler(
        const fragment_handler_t &delegate, std::size_t initialBufferLength = DEFAULT_FRAGMENT_ASSEMBLY_BUFFER_LENGTH) :
        m_delegate(delegate)
    {
        aeron_fragment_assembler_create(&m_fragment_assembler, handlerCallback, reinterpret_cast<void *>(this));
    }

    ~FragmentAssembler()
    {
        aeron_fragment_assembler_delete(m_fragment_assembler);
    }

    FragmentAssembler(FragmentAssembler& other) = delete;
    FragmentAssembler(FragmentAssembler&& other) = delete;
    FragmentAssembler& operator=(FragmentAssembler& other) = delete;
    FragmentAssembler& operator=(FragmentAssembler&& other) = delete;

    /**
     * Compose a fragment_handler_t that calls the this FragmentAssembler instance for reassembly. Suitable for
     * passing to Subscription::poll(fragment_handler_t, int).
     *
     * @return fragment_handler_t composed with the FragmentAssembler instance
     */
    fragment_handler_t handler()
    {
        return [this](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            this->onFragment(buffer, offset, length, header);
        };
    }

    /**
     * Free an existing session buffer to reduce memory pressure when an Image goes inactive or no more
     * large messages are expected.
     *
     * @param sessionId to have its buffer freed
     * @return true if a buffer has been freed otherwise false.
     */
    bool deleteSessionBuffer(std::int32_t sessionId)
    {
        return aeron_fragment_assembler_delete_session_buffer(m_fragment_assembler, sessionId);
    }

private:
    aeron_fragment_assembler_t *m_fragment_assembler = nullptr;
    fragment_handler_t m_delegate;

    static void handlerCallback(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        auto *assembler = reinterpret_cast<FragmentAssembler *>(clientd);
        Header headerWrapper{header};
        AtomicBuffer buffer1{const_cast<uint8_t *>(buffer), length};
        assembler->m_delegate(buffer1, 0, (util::index_t)length, headerWrapper);
    }

    inline void onFragment(AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
    {
        aeron_fragment_assembler_handler(m_fragment_assembler, buffer.buffer() + offset, length, header.hdr());
    }
};

}

#endif
