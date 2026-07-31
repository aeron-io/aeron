/*
 * Copyright 2014-2026 Real Logic Limited.
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
package io.aeron.logging;

import io.aeron.CommonContext;
import org.agrona.DirectBuffer;

/**
 * Base interface for renderers of commands and protocol messages.
 */
public interface BinaryRenderer
{
    /**
     * Whether {@link BinaryRenderer} implementations should render extra bytes as a pretty hex/ASCII dump.
     */
    boolean RENDER_EXTRA_CONTENT = Boolean.getBoolean(CommonContext.EVENT_LOG_RENDER_EXTRA_BYTES_PROP_NAME);

    /**
     * {@return the message type ids that this renderer supports}
     */
    int[] supportingMsgTypeIds();

    /**
     * Append this message to the supplied string builder.
     *
     * @param sb        to append to.
     * @param msgTypeId of the message.
     * @param buffer    containing the message.
     * @param offset    of the message in the buffer.
     * @param length    of the message.
     */
    void append(StringBuilder sb, int msgTypeId, DirectBuffer buffer, int offset, int length);
}
