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
package io.aeron.agent;

import org.agrona.MutableDirectBuffer;

/**
 * Interface that describes a logger for a given Aeron component.
 */
public interface ModuleLogger
{
    /**
     * The type code to distinguish this logger when encoding/decoding messages.
     *
     * @return the type code for this logger.
     */
    int typeCode();

    /**
     * Decode a message on the reader side.
     *
     * @param buffer      containing the message.
     * @param offset      in the buffer to the message.
     * @param eventCodeId of the event to be decoded.
     * @param builder     to render the message to.
     */
    void decode(MutableDirectBuffer buffer, int offset, int eventCodeId, StringBuilder builder);

    /**
     * Reset the logger and its configuration. Typically called when stopping/disabling logging.
     */
    void reset();

    /**
     * Get version information for this logger. Typically, will use version and commit info, e.g.
     * {@code version=1.49.0 commit=4b09b14043}.
     *
     * @return version information, e.g. {@code version=1.49.0 commit=4b09b14043}.
     */
    String version();
}
