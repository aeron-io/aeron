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
package io.aeron.driver.buffer;

import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

/**
 * Represents the collection of raw metadata and associated term buffers for publication and image logs.
 */
public interface RawLog extends AutoCloseable
{
    /**
     * The length of each term in bytes.
     *
     * @return the length of each term in bytes.
     */
    int termLength();

    /**
     * An array of term buffer partitions.
     *
     * @return an array of the term buffer partitions.
     */
    UnsafeBuffer[] termBuffers();

    /**
     * The meta data storage for the overall log.
     *
     * @return the meta data storage for the overall log.
     */
    UnsafeBuffer metaData();

    /**
     * Slice the underlying buffer to provide an array of term buffers in order.
     *
     * @return slices of the underlying buffer to provide an array of term buffers in order.
     */
    ByteBuffer[] sliceTerms();

    /**
     * Get the fully qualified file name for the log file.
     *
     * @return the fully qualified file name for the log file.
     */
    String fileName();

    /**
     * Free the mapped buffers and delete the file.
     *
     * @return true if successful or false if it should be reattempted.
     */
    boolean free();

    /**
     * Close the resource regardless of if {@link #free()} has succeeded or not.
     */
    @Override
    void close();
}
