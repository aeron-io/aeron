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
package io.aeron;

import io.aeron.exceptions.AeronException;
import org.agrona.DirectBuffer;
import org.agrona.SemanticVersion;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

import static org.agrona.BitUtil.*;

/**
 * Description of the command and control file used between driver and clients.
 * <p>
 * File Layout
 * <pre>
 *  +-----------------------------+
 *  |          Meta Data          |
 *  +-----------------------------+
 *  |      to-driver Buffer       |
 *  +-----------------------------+
 *  |      to-clients Buffer      |
 *  +-----------------------------+
 *  |   Counters Metadata Buffer  |
 *  +-----------------------------+
 *  |    Counters Values Buffer   |
 *  +-----------------------------+
 *  |          Error Log          |
 *  +-----------------------------+
 * </pre>
 * <p>
 * Metadata Layout {@link #CNC_VERSION}
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                      Aeron CnC Version                        |
 *  +---------------------------------------------------------------+
 *  |                   to-driver buffer length                     |
 *  +---------------------------------------------------------------+
 *  |                  to-clients buffer length                     |
 *  +---------------------------------------------------------------+
 *  |               Counters Metadata buffer length                 |
 *  +---------------------------------------------------------------+
 *  |                Counters Values buffer length                  |
 *  +---------------------------------------------------------------+
 *  |                   Error Log buffer length                     |
 *  +---------------------------------------------------------------+
 *  |                   Client Liveness Timeout                     |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                    Driver Start Timestamp                     |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                         Driver PID                            |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        File page size                         |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class CncFileDescriptor
{
    /**
     * Name used for CnC file in the Aeron directory.
     */
    public static final String CNC_FILE = "cnc.dat";

    /**
     * Version of the CnC file using semantic versioning ({@link SemanticVersion}) stored as an 32-bit integer.
     */
    public static final int CNC_VERSION = SemanticVersion.compose(0, 2, 0);

    /**
     * Offset at which the version field can be found.
     */
    public static final int CNC_VERSION_FIELD_OFFSET;

    /**
     * Offset at which the length field can be found for the command ring buffer to the driver.
     */
    public static final int TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET;

    /**
     * Offset at which the length field can be found for the broadcast buffer to the clients can be found.
     */
    public static final int TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET;

    /**
     * Offset at which the length field can be found for counter metadata, e.g. labels, can be found.
     */
    public static final int COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET;

    /**
     * Offset at which the length field can be found for the counters values can be found.
     */
    public static final int COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET;

    /**
     * Offset at which the client liveness timeout value can be found.
     */
    public static final int CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET;

    /**
     * Offset at which the length field can be found for buffer containing the error log can be found.
     */
    public static final int ERROR_LOG_BUFFER_LENGTH_FIELD_OFFSET;

    /**
     * Offset at which the start timestamp value for the driver can be found.
     */
    public static final int START_TIMESTAMP_FIELD_OFFSET;

    /**
     * Offset at which the PID value for the driver can be found.
     */
    public static final int PID_FIELD_OFFSET;

    /**
     * Offset at which the file page size value for the driver can be found.
     */
    public static final int FILE_PAGE_SIZE_FIELD_OFFSET;

    static
    {
        CNC_VERSION_FIELD_OFFSET = 0;
        TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET = CNC_VERSION_FIELD_OFFSET + SIZE_OF_INT;
        TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET = TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET + SIZE_OF_INT;
        COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET = TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET + SIZE_OF_INT;
        COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET = COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET + SIZE_OF_INT;
        ERROR_LOG_BUFFER_LENGTH_FIELD_OFFSET = COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET + SIZE_OF_INT;
        CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET = ERROR_LOG_BUFFER_LENGTH_FIELD_OFFSET + SIZE_OF_INT;
        START_TIMESTAMP_FIELD_OFFSET = CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET + SIZE_OF_LONG;
        PID_FIELD_OFFSET = START_TIMESTAMP_FIELD_OFFSET + SIZE_OF_LONG;
        FILE_PAGE_SIZE_FIELD_OFFSET = PID_FIELD_OFFSET + SIZE_OF_LONG;
    }

    /**
     * Length of the metadata header for the CnC file.
     */
    public static final int META_DATA_LENGTH = CACHE_LINE_LENGTH * 2;

    /**
     * The offset of the first byte past the metadata header which is aligned on a cache-line boundary.
     */
    public static final int END_OF_METADATA_OFFSET = META_DATA_LENGTH;

    /**
     * Compute the length of the cnc file and return it.
     *
     * @param totalLengthOfBuffers in bytes.
     * @param alignment            for file length to adhere to.
     * @return cnc file length in bytes.
     */
    public static int computeCncFileLength(final int totalLengthOfBuffers, final int alignment)
    {
        return align(META_DATA_LENGTH + totalLengthOfBuffers, alignment);
    }

    /**
     * Offset in the buffer at which the version field exists.
     *
     * @param baseOffset for the start of the metadata.
     * @return offset in the buffer at which the version field exists.
     */
    public static int cncVersionOffset(final int baseOffset)
    {
        return baseOffset + CNC_VERSION_FIELD_OFFSET;
    }

    /**
     * Offset in the buffer at which the to driver buffer length field exists.
     *
     * @param baseOffset for the start of the metadata.
     * @return offset in the buffer at which the to driver buffer length field exists.
     */
    public static int toDriverBufferLengthOffset(final int baseOffset)
    {
        return baseOffset + TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET;
    }

    /**
     * Offset in the buffer at which the to clients buffer length field exists.
     *
     * @param baseOffset for the start of the metadata.
     * @return offset in the buffer at which the to clients buffer length field exists.
     */
    public static int toClientsBufferLengthOffset(final int baseOffset)
    {
        return baseOffset + TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET;
    }

    /**
     * Offset in the buffer at which the counter metadata buffer length field exists.
     *
     * @param baseOffset for the start of the metadata.
     * @return offset in the buffer at which the counter metadata buffer length field exists.
     */
    public static int countersMetaDataBufferLengthOffset(final int baseOffset)
    {
        return baseOffset + COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET;
    }

    /**
     * Offset in the buffer at which the counter value buffer length field exists.
     *
     * @param baseOffset for the start of the metadata.
     * @return offset in the buffer at which the counter value buffer length field exists.
     */
    public static int countersValuesBufferLengthOffset(final int baseOffset)
    {
        return baseOffset + COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET;
    }

    /**
     * Offset in the buffer at which the client liveness timeout field exists.
     *
     * @param baseOffset for the start of the metadata.
     * @return offset in the buffer at which the client liveness timeout field exists.
     */
    public static int clientLivenessTimeoutOffset(final int baseOffset)
    {
        return baseOffset + CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET;
    }

    /**
     * Offset in the buffer at which the error buffer length field exists.
     *
     * @param baseOffset for the start of the metadata.
     * @return offset in the buffer at which the error buffer length field exists.
     */
    public static int errorLogBufferLengthOffset(final int baseOffset)
    {
        return baseOffset + ERROR_LOG_BUFFER_LENGTH_FIELD_OFFSET;
    }

    /**
     * Offset in the buffer at which the driver start time timestamp field exists.
     *
     * @param baseOffset for the start of the metadata.
     * @return offset in the buffer at which the driver start time timestamp field exists.
     */
    public static int startTimestampOffset(final int baseOffset)
    {
        return baseOffset + START_TIMESTAMP_FIELD_OFFSET;
    }

    /**
     * Offset in the buffer at which the driver process PID field exists.
     *
     * @param baseOffset for the start of the metadata.
     * @return offset in the buffer at which the driver process PID field exists.
     */
    public static int pidOffset(final int baseOffset)
    {
        return baseOffset + PID_FIELD_OFFSET;
    }

    /**
     * Fill the CnC file with metadata to define its sections.
     *
     * @param cncMetaDataBuffer           that wraps the metadata section of the CnC file.
     * @param toDriverBufferLength        for sending commands to the driver.
     * @param toClientsBufferLength       for broadcasting events to the clients.
     * @param counterMetaDataBufferLength buffer length for counters metadata.
     * @param counterValuesBufferLength   buffer length for counter values.
     * @param clientLivenessTimeoutNs     timeout value in nanoseconds for client liveness and inter-service interval.
     * @param errorLogBufferLength        for recording the distinct error log.
     * @param startTimestampMs            epoch at which the driver started.
     * @param pid                         for the process hosting the driver.
     * @param filePageSize                for alignment of all files.
     */
    public static void fillMetaData(
        final UnsafeBuffer cncMetaDataBuffer,
        final int toDriverBufferLength,
        final int toClientsBufferLength,
        final int counterMetaDataBufferLength,
        final int counterValuesBufferLength,
        final long clientLivenessTimeoutNs,
        final int errorLogBufferLength,
        final long startTimestampMs,
        final long pid,
        final int filePageSize)
    {
        cncMetaDataBuffer.putInt(TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET, toDriverBufferLength);
        cncMetaDataBuffer.putInt(TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET, toClientsBufferLength);
        cncMetaDataBuffer.putInt(COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET, counterMetaDataBufferLength);
        cncMetaDataBuffer.putInt(COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET, counterValuesBufferLength);
        cncMetaDataBuffer.putInt(ERROR_LOG_BUFFER_LENGTH_FIELD_OFFSET, errorLogBufferLength);
        cncMetaDataBuffer.putLong(CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET, clientLivenessTimeoutNs);
        cncMetaDataBuffer.putLong(START_TIMESTAMP_FIELD_OFFSET, startTimestampMs);
        cncMetaDataBuffer.putLong(PID_FIELD_OFFSET, pid);
        cncMetaDataBuffer.putInt(FILE_PAGE_SIZE_FIELD_OFFSET, filePageSize);
    }

    /**
     * Signal that the CnC file is ready for use by client by writing the version into the CnC file.
     *
     * @param cncMetaDataBuffer for the CnC file.
     */
    public static void signalCncReady(final UnsafeBuffer cncMetaDataBuffer)
    {
        cncMetaDataBuffer.putIntVolatile(CNC_VERSION_FIELD_OFFSET, CNC_VERSION);
    }

    /**
     * Create the buffer which wraps the area in the CnC file for the metadata about the CnC file itself.
     *
     * @param buffer for the CnC file.
     * @return the buffer which wraps the area in the CnC file for the metadata about the CnC file itself.
     */
    public static UnsafeBuffer createMetaDataBuffer(final ByteBuffer buffer)
    {
        return new UnsafeBuffer(buffer, 0, META_DATA_LENGTH);
    }

    /**
     * Create the buffer which wraps the area in the CnC file for the command buffer from clients to the driver.
     *
     * @param buffer         for the CnC file.
     * @param metaDataBuffer within the CnC file.
     * @return a buffer which wraps the section in the CnC file for the command buffer from clients to the driver.
     */
    public static UnsafeBuffer createToDriverBuffer(final ByteBuffer buffer, final DirectBuffer metaDataBuffer)
    {
        return new UnsafeBuffer(
            buffer, META_DATA_LENGTH, metaDataBuffer.getInt(TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET));
    }

    /**
     * Create the buffer which wraps the section in the CnC file for the broadcast buffer from the driver to clients.
     *
     * @param buffer         for the CnC file.
     * @param metaDataBuffer within the CnC file.
     * @return a buffer which wraps the section in the CnC file for the broadcast buffer from the driver to clients.
     */
    public static UnsafeBuffer createToClientsBuffer(final ByteBuffer buffer, final DirectBuffer metaDataBuffer)
    {
        final int offset = META_DATA_LENGTH + metaDataBuffer.getInt(TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET);

        return new UnsafeBuffer(buffer, offset, metaDataBuffer.getInt(TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET));
    }

    /**
     * Create the buffer which wraps the section in the CnC file for the counter's metadata.
     *
     * @param buffer         for the CnC file.
     * @param metaDataBuffer within the CnC file.
     * @return a buffer which wraps the section in the CnC file for the counter's metadata.
     */
    public static UnsafeBuffer createCountersMetaDataBuffer(final ByteBuffer buffer, final DirectBuffer metaDataBuffer)
    {
        final int offset = META_DATA_LENGTH +
            metaDataBuffer.getInt(TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET) +
            metaDataBuffer.getInt(TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET);

        return new UnsafeBuffer(buffer, offset, metaDataBuffer.getInt(COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET));
    }

    /**
     * Create the buffer which wraps the section in the CnC file for the counter values.
     *
     * @param buffer         for the CnC file.
     * @param metaDataBuffer within the CnC file.
     * @return a buffer which wraps the section in the CnC file for the counter values.
     */
    public static UnsafeBuffer createCountersValuesBuffer(final ByteBuffer buffer, final DirectBuffer metaDataBuffer)
    {
        final int offset = META_DATA_LENGTH +
            metaDataBuffer.getInt(TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET) +
            metaDataBuffer.getInt(TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET) +
            metaDataBuffer.getInt(COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET);

        return new UnsafeBuffer(buffer, offset, metaDataBuffer.getInt(COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET));
    }

    /**
     * Create the buffer which wraps the section in the CnC file for the error log.
     *
     * @param buffer         for the CnC file.
     * @param metaDataBuffer within the CnC file.
     * @return a buffer which wraps the section in the CnC file for the error log.
     */
    public static UnsafeBuffer createErrorLogBuffer(final ByteBuffer buffer, final DirectBuffer metaDataBuffer)
    {
        final int offset = META_DATA_LENGTH +
            metaDataBuffer.getInt(TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET) +
            metaDataBuffer.getInt(TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET) +
            metaDataBuffer.getInt(COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET) +
            metaDataBuffer.getInt(COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET);

        return new UnsafeBuffer(buffer, offset, metaDataBuffer.getInt(ERROR_LOG_BUFFER_LENGTH_FIELD_OFFSET));
    }

    /**
     * Get the timeout in nanoseconds for tracking client liveness and inter-service timeout.
     *
     * @param metaDataBuffer for the CnC file.
     * @return the timeout in milliseconds for tracking client liveness.
     */
    public static long clientLivenessTimeoutNs(final DirectBuffer metaDataBuffer)
    {
        return metaDataBuffer.getLong(CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET);
    }

    /**
     * Get the start timestamp in milliseconds for the media driver.
     *
     * @param metaDataBuffer for the CnC file.
     * @return the start timestamp in milliseconds for the media driver.
     */
    public static long startTimestampMs(final DirectBuffer metaDataBuffer)
    {
        return metaDataBuffer.getLong(START_TIMESTAMP_FIELD_OFFSET);
    }

    /**
     * Get the process PID hosting the driver.
     *
     * @param metaDataBuffer for the CnC file.
     * @return the process PID hosting the driver.
     */
    public static long pid(final DirectBuffer metaDataBuffer)
    {
        return metaDataBuffer.getLong(PID_FIELD_OFFSET);
    }

    /**
     * Get the file page size.
     *
     * @param metaDataBuffer for the CnC file.
     * @return the file page size.
     */
    public static int filePageSize(final DirectBuffer metaDataBuffer)
    {
        return metaDataBuffer.getInt(FILE_PAGE_SIZE_FIELD_OFFSET);
    }

    /**
     * Check the version of the CnC file is compatible with application.
     *
     * @param cncVersion of the CnC file.
     * @throws AeronException if the major versions are not compatible.
     */
    public static void checkVersion(final int cncVersion)
    {
        if (SemanticVersion.major(CNC_VERSION) != SemanticVersion.major(cncVersion))
        {
            throw new AeronException("CnC version not compatible:" +
                " app=" + SemanticVersion.toString(CNC_VERSION) +
                " file=" + SemanticVersion.toString(cncVersion));
        }
    }

    /**
     * Is the provided length for the CnC file sufficient given what is stored in the metadata.
     *
     * @param metaDataBuffer for the CnC file.
     * @param cncFileLength  to check if it is sufficient based on what is stored in the metadata.
     * @return true is the length is correct otherwise false.
     */
    public static boolean isCncFileLengthSufficient(final DirectBuffer metaDataBuffer, final int cncFileLength)
    {
        final int metadataRequiredLength =
            META_DATA_LENGTH +
            metaDataBuffer.getInt(TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET) +
            metaDataBuffer.getInt(TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET) +
            metaDataBuffer.getInt(COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET) +
            metaDataBuffer.getInt(COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET) +
            metaDataBuffer.getInt(ERROR_LOG_BUFFER_LENGTH_FIELD_OFFSET);

        return cncFileLength >= metadataRequiredLength;
    }

    /**
     * Determines if this path name matches the cnc file name pattern.
     *
     * @param path    to examine.
     * @param ignored only needed for bi-predicate signature matching.
     * @return true if the name matches.
     */
    public static boolean isCncFile(final Path path, final BasicFileAttributes ignored)
    {
        return path.getFileName().toString().equals(CNC_FILE);
    }
}
