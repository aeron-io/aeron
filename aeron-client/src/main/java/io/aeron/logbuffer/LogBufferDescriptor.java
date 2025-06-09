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
package io.aeron.logbuffer;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.agrona.BitUtil.*;

/**
 * Layout description for log buffers which contains partitions of terms with associated term metadata,
 * plus ending with overall log metadata.
 * <pre>
 *  +----------------------------+
 *  |           Term 0           |
 *  +----------------------------+
 *  |           Term 1           |
 *  +----------------------------+
 *  |           Term 2           |
 *  +----------------------------+
 *  |        Log Meta Data       |
 *  +----------------------------+
 * </pre>
 */
public class LogBufferDescriptor
{
    private static final int PADDING_SIZE = 64;

    /**
     * The number of partitions the log is divided into.
     */
    public static final int PARTITION_COUNT = 3;

    /**
     * Section index for which buffer contains the log metadata.
     */
    public static final int LOG_META_DATA_SECTION_INDEX = PARTITION_COUNT;

    /**
     * Minimum buffer length for a log term.
     */
    public static final int TERM_MIN_LENGTH = 64 * 1024;

    /**
     * Maximum buffer length for a log term.
     */
    public static final int TERM_MAX_LENGTH = 1024 * 1024 * 1024;

    /**
     * Minimum page size.
     */
    public static final int PAGE_MIN_SIZE = 4 * 1024;

    /**
     * Maximum page size.
     */
    public static final int PAGE_MAX_SIZE = 1024 * 1024 * 1024;


    // *******************************
    // *** Log Meta Data Constants ***
    // *******************************

    /**
     * Offset within the metadata where the tail values are stored.
     */
    public static final int TERM_TAIL_COUNTERS_OFFSET;

    /**
     * Offset within the log metadata where the active partition index is stored.
     */
    public static final int LOG_ACTIVE_TERM_COUNT_OFFSET;

    /**
     * Offset within the log metadata where the position of the End of Stream is stored.
     */
    public static final int LOG_END_OF_STREAM_POSITION_OFFSET;

    /**
     * Offset within the log metadata where whether the log is connected or not is stored.
     */
    public static final int LOG_IS_CONNECTED_OFFSET;

    /**
     * Offset within the log metadata where the count of active transports is stored.
     */
    public static final int LOG_ACTIVE_TRANSPORT_COUNT;

    /**
     * Offset within the log metadata where the active term id is stored.
     */
    public static final int LOG_INITIAL_TERM_ID_OFFSET;

    /**
     * Offset within the log metadata which the length field for the frame header is stored.
     */
    public static final int LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET;

    /**
     * Offset within the log metadata which the MTU length is stored.
     */
    public static final int LOG_MTU_LENGTH_OFFSET;

    /**
     * Offset within the log metadata which the correlation id is stored.
     */
    public static final int LOG_CORRELATION_ID_OFFSET;

    /**
     * Offset within the log metadata which the term length is stored.
     */
    public static final int LOG_TERM_LENGTH_OFFSET;

    /**
     * Offset within the log metadata which the page size is stored.
     */
    public static final int LOG_PAGE_SIZE_OFFSET;

    /**
     * Offset at which the default frame headers begin.
     */
    public static final int LOG_DEFAULT_FRAME_HEADER_OFFSET;

    /**
     * Maximum length of a frame header.
     */
    public static final int LOG_DEFAULT_FRAME_HEADER_MAX_LENGTH = PADDING_SIZE * 2;

    /**
     * Offset within the log metadata where the sparse property is stored.
     */
    public static final int LOG_SPARSE_OFFSET;

    /**
     * Offset within the log metadata where the tether property is stored.
     */
    public static final int LOG_TETHER_OFFSET;

    /**
     * Offset within the log metadata where the 'publication revoked' status is indicated.
     */
    public static final int LOG_IS_PUBLICATION_REVOKED_OFFSET;

    /**
     * Offset within the log metadata where the rejoin property is stored.
     */
    public static final int LOG_REJOIN_OFFSET;

    /**
     * Offset within the log metadata where the reliable property is stored.
     */
    public static final int LOG_RELIABLE_OFFSET;

    /**
     * Offset within the log metadata where the socket receive buffer length is stored.
     */
    public static final int LOG_SOCKET_RCVBUF_LENGTH_OFFSET;

    /**
     * Offset within the log metadata where the OS default length for the socket receive buffer is stored.
     */
    public static final int LOG_OS_DEFAULT_SOCKET_RCVBUF_LENGTH_OFFSET;

    /**
     * Offset within the log metadata where the OS maximum length for the socket receive buffer is stored.
     */
    public static final int LOG_OS_MAX_SOCKET_RCVBUF_LENGTH_OFFSET;

    /**
     * Offset within the log metadata where the socket send buffer length is stored.
     */
    public static final int LOG_SOCKET_SNDBUF_LENGTH_OFFSET;

    /**
     * Offset within the log metadata where the OS default length for the socket send buffer is stored.
     */
    public static final int LOG_OS_DEFAULT_SOCKET_SNDBUF_LENGTH_OFFSET;

    /**
     * Offset within the log metadata where the OS maximum length for the socket send buffer is stored.
     */
    public static final int LOG_OS_MAX_SOCKET_SNDBUF_LENGTH_OFFSET;

    /**
     * Offset within the log metadata where the receiver window length is stored.
     */
    public static final int LOG_RECEIVER_WINDOW_LENGTH_OFFSET;

    /**
     * Offset within the log metadata where the publication window length is stored.
     */
    public static final int LOG_PUBLICATION_WINDOW_LENGTH_OFFSET;

    /**
     * Offset within the log metadata where the untethered window limit timeout ns is stored.
     */
    public static final int LOG_UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS_OFFSET;

    /**
     * Offset within the log metadata where the untethered linger timeout ns is stored.
     */
    public static final int LOG_UNTETHERED_LINGER_TIMEOUT_NS_OFFSET;

    /**
     * Offset within the log metadata where the untethered resting timeout ns is stored.
     */
    public static final int LOG_UNTETHERED_RESTING_TIMEOUT_NS_OFFSET;

    /**
     * Offset within the log metadata where the max resend is stored.
     */
    public static final int LOG_MAX_RESEND_OFFSET;

    /**
     * Offset within the log metadata where the linger timeout ns is stored.
     */
    public static final int LOG_LINGER_TIMEOUT_NS_OFFSET;

    /**
     * Offset within the log metadata where the signal-eos is stored.
     */
    public static final int LOG_SIGNAL_EOS_OFFSET;

    /**
     * Offset within the log metadata where the spies-simulate-connection is stored.
     */
    public static final int LOG_SPIES_SIMULATE_CONNECTION_OFFSET;

    /**
     * Offset within the log metadata where the group is stored.
     */
    public static final int LOG_GROUP_OFFSET;

    /**
     * Offset within the log metadata where the entity tag is stored.
     */
    public static final int LOG_ENTITY_TAG_OFFSET;

    /**
     * Offset within the log metadata where the response correlation id is stored.
     */
    public static final int LOG_RESPONSE_CORRELATION_ID_OFFSET;

    /**
     * Offset within the log metadata where is-response is stored.
     */
    public static final int LOG_IS_RESPONSE_OFFSET;


    /**
     * Total length of the log metadata buffer in bytes.
     * <pre>
     *   0                   1                   2                   3
     *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |                       Tail Counter 0                          |
     *  |                                                               |
     *  +---------------------------------------------------------------+
     *  |                       Tail Counter 1                          |
     *  |                                                               |
     *  +---------------------------------------------------------------+
     *  |                       Tail Counter 2                          |
     *  |                                                               |
     *  +---------------------------------------------------------------+
     *  |                      Active Term Count                        |
     *  +---------------------------------------------------------------+
     *  |                     Cache Line Padding                       ...
     * ...                                                              |
     *  +---------------------------------------------------------------+
     *  |                    End of Stream Position                     |
     *  |                                                               |
     *  +---------------------------------------------------------------+
     *  |                        Is Connected                           |
     *  +---------------------------------------------------------------+
     *  |                    Active Transport Count                     |
     *  +---------------------------------------------------------------+
     *  |                      Cache Line Padding                      ...
     * ...                                                              |
     *  +---------------------------------------------------------------+
     *  |                 Registration / Correlation ID                 |
     *  |                                                               |
     *  +---------------------------------------------------------------+
     *  |                        Initial Term Id                        |
     *  +---------------------------------------------------------------+
     *  |                  Default Frame Header Length                  |
     *  +---------------------------------------------------------------+
     *  |                          MTU Length                           |
     *  +---------------------------------------------------------------+
     *  |                         Term Length                           |
     *  +---------------------------------------------------------------+
     *  |                          Page Size                            |
     *  +---------------------------------------------------------------+
     *  |                    Publication Window Length                  |
     *  +---------------------------------------------------------------+
     *  |                      Receiver Window Length                   |
     *  +---------------------------------------------------------------+
     *  |                    Socket Send Buffer Length                  |
     *  +---------------------------------------------------------------+
     *  |               OS Default Socket Send Buffer Length            |
     *  +---------------------------------------------------------------+
     *  |                OS Max Socket Send Buffer Length               |
     *  +---------------------------------------------------------------+
     *  |                  Socket Receive Buffer Length                 |
     *  +---------------------------------------------------------------+
     *  |              OS Default Socket Receive Buffer Length          |
     *  +---------------------------------------------------------------+
     *  |               OS Max Socket Receive Buffer Length             |
     *  +---------------------------------------------------------------+
     *  |                        Maximum Resend                         |
     *  +---------------------------------------------------------------+
     *  |                           Entity tag                          |
     *  |                                                               |
     *  +---------------------------------------------------------------+
     *  |                    Response correlation id                    |
     *  |                                                               |
     *  +---------------------------------------------------------------+
     *  |                     Default Frame Header                     ...
     * ...                                                              |
     *  +---------------------------------------------------------------+
     *  |                        Linger Timeout (ns)                    |
     *  |                                                               |
     *  +---------------------------------------------------------------+
     *  |               Untethered Window Limit Timeout (ns)            |
     *  |                                                               |
     *  +---------------------------------------------------------------+
     *  |                 Untethered Resting Timeout (ns)               |
     *  |                                                               |
     *  +---------------------------------------------------------------+
     *  |                            Group                              |
     *  +---------------------------------------------------------------+
     *  |                          Is response                          |
     *  +---------------------------------------------------------------+
     *  |                            Rejoin                             |
     *  +---------------------------------------------------------------+
     *  |                           Reliable                            |
     *  +---------------------------------------------------------------+
     *  |                            Sparse                             |
     *  +---------------------------------------------------------------+
     *  |                         Signal EOS                            |
     *  +---------------------------------------------------------------+
     *  |                 Spies Simulate Connection                     |
     *  +---------------------------------------------------------------+
     *  |                          Tether                               |
     *  +---------------------------------------------------------------+
     *  |                     Is publication revoked                    |
     *  +---------------------------------------------------------------+
     *  |                         Alignment gap                         |
     *  +---------------------------------------------------------------+
     *  |                  Untethered Linger Timeout (ns)               |
     *  |                                                               |
     *  +---------------------------------------------------------------+
     * </pre>
     */
    public static final int LOG_META_DATA_LENGTH;

    static
    {
        TERM_TAIL_COUNTERS_OFFSET = 0;
        LOG_ACTIVE_TERM_COUNT_OFFSET = TERM_TAIL_COUNTERS_OFFSET + (SIZE_OF_LONG * PARTITION_COUNT);

        LOG_END_OF_STREAM_POSITION_OFFSET = PADDING_SIZE * 2;
        LOG_IS_CONNECTED_OFFSET = LOG_END_OF_STREAM_POSITION_OFFSET + SIZE_OF_LONG;
        LOG_ACTIVE_TRANSPORT_COUNT = LOG_IS_CONNECTED_OFFSET + SIZE_OF_INT;

        LOG_CORRELATION_ID_OFFSET = PADDING_SIZE * 4;
        LOG_INITIAL_TERM_ID_OFFSET = LOG_CORRELATION_ID_OFFSET + SIZE_OF_LONG;
        LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET = LOG_INITIAL_TERM_ID_OFFSET + SIZE_OF_INT;
        LOG_MTU_LENGTH_OFFSET = LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET + SIZE_OF_INT;
        LOG_TERM_LENGTH_OFFSET = LOG_MTU_LENGTH_OFFSET + SIZE_OF_INT;
        LOG_PAGE_SIZE_OFFSET = LOG_TERM_LENGTH_OFFSET + SIZE_OF_INT;

        LOG_PUBLICATION_WINDOW_LENGTH_OFFSET = LOG_PAGE_SIZE_OFFSET + SIZE_OF_INT;
        LOG_RECEIVER_WINDOW_LENGTH_OFFSET = LOG_PUBLICATION_WINDOW_LENGTH_OFFSET + SIZE_OF_INT;
        LOG_SOCKET_SNDBUF_LENGTH_OFFSET = LOG_RECEIVER_WINDOW_LENGTH_OFFSET + SIZE_OF_INT;
        LOG_OS_DEFAULT_SOCKET_SNDBUF_LENGTH_OFFSET = LOG_SOCKET_SNDBUF_LENGTH_OFFSET + SIZE_OF_INT;
        LOG_OS_MAX_SOCKET_SNDBUF_LENGTH_OFFSET = LOG_OS_DEFAULT_SOCKET_SNDBUF_LENGTH_OFFSET + SIZE_OF_INT;
        LOG_SOCKET_RCVBUF_LENGTH_OFFSET = LOG_OS_MAX_SOCKET_SNDBUF_LENGTH_OFFSET + SIZE_OF_INT;
        LOG_OS_DEFAULT_SOCKET_RCVBUF_LENGTH_OFFSET = LOG_SOCKET_RCVBUF_LENGTH_OFFSET + SIZE_OF_INT;
        LOG_OS_MAX_SOCKET_RCVBUF_LENGTH_OFFSET = LOG_OS_DEFAULT_SOCKET_RCVBUF_LENGTH_OFFSET + SIZE_OF_INT;
        LOG_MAX_RESEND_OFFSET = LOG_OS_MAX_SOCKET_RCVBUF_LENGTH_OFFSET + SIZE_OF_INT;

        LOG_DEFAULT_FRAME_HEADER_OFFSET = PADDING_SIZE * 5;
        LOG_ENTITY_TAG_OFFSET = LOG_DEFAULT_FRAME_HEADER_OFFSET + LOG_DEFAULT_FRAME_HEADER_MAX_LENGTH;
        LOG_RESPONSE_CORRELATION_ID_OFFSET = LOG_ENTITY_TAG_OFFSET + SIZE_OF_LONG;
        LOG_LINGER_TIMEOUT_NS_OFFSET = LOG_RESPONSE_CORRELATION_ID_OFFSET + SIZE_OF_LONG;
        LOG_UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS_OFFSET = LOG_LINGER_TIMEOUT_NS_OFFSET + SIZE_OF_LONG;
        LOG_UNTETHERED_RESTING_TIMEOUT_NS_OFFSET = LOG_UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS_OFFSET + SIZE_OF_LONG;
        LOG_GROUP_OFFSET = LOG_UNTETHERED_RESTING_TIMEOUT_NS_OFFSET + SIZE_OF_LONG;
        LOG_IS_RESPONSE_OFFSET = LOG_GROUP_OFFSET + SIZE_OF_BYTE;
        LOG_REJOIN_OFFSET = LOG_IS_RESPONSE_OFFSET + SIZE_OF_BYTE;
        LOG_RELIABLE_OFFSET = LOG_REJOIN_OFFSET + SIZE_OF_BYTE;
        LOG_SPARSE_OFFSET = LOG_RELIABLE_OFFSET + SIZE_OF_BYTE;
        LOG_SIGNAL_EOS_OFFSET = LOG_SPARSE_OFFSET + SIZE_OF_BYTE;
        LOG_SPIES_SIMULATE_CONNECTION_OFFSET = LOG_SIGNAL_EOS_OFFSET + SIZE_OF_BYTE;
        LOG_TETHER_OFFSET = LOG_SPIES_SIMULATE_CONNECTION_OFFSET + SIZE_OF_BYTE;
        LOG_IS_PUBLICATION_REVOKED_OFFSET = LOG_TETHER_OFFSET + SIZE_OF_BYTE;
        LOG_UNTETHERED_LINGER_TIMEOUT_NS_OFFSET = LOG_IS_PUBLICATION_REVOKED_OFFSET + SIZE_OF_INT;

        LOG_META_DATA_LENGTH = PAGE_MIN_SIZE;
    }

    /**
     * Check that term length is valid and alignment is valid.
     *
     * @param termLength to be checked.
     * @throws IllegalStateException if the length is not as expected.
     */
    public static void checkTermLength(final int termLength)
    {
        if (termLength < TERM_MIN_LENGTH)
        {
            throw new IllegalStateException(
                "Term length less than min length of " + TERM_MIN_LENGTH + ": length=" + termLength);
        }

        if (termLength > TERM_MAX_LENGTH)
        {
            throw new IllegalStateException(
                "Term length more than max length of " + TERM_MAX_LENGTH + ": length=" + termLength);
        }

        if (!BitUtil.isPowerOfTwo(termLength))
        {
            throw new IllegalStateException("Term length not a power of 2: length=" + termLength);
        }
    }

    /**
     * Check that page size is valid and alignment is valid.
     *
     * @param pageSize to be checked.
     * @throws IllegalStateException if the size is not as expected.
     */
    public static void checkPageSize(final int pageSize)
    {
        if (pageSize < PAGE_MIN_SIZE)
        {
            throw new IllegalStateException(
                "Page size less than min size of " + PAGE_MIN_SIZE + ": page size=" + pageSize);
        }

        if (pageSize > PAGE_MAX_SIZE)
        {
            throw new IllegalStateException(
                "Page size more than max size of " + PAGE_MAX_SIZE + ": page size=" + pageSize);
        }

        if (!BitUtil.isPowerOfTwo(pageSize))
        {
            throw new IllegalStateException("Page size not a power of 2: page size=" + pageSize);
        }
    }

    /**
     * Get the value of the initial Term id used for this log.
     *
     * @param metadataBuffer containing the meta data.
     * @return the value of the initial Term id used for this log.
     */
    public static int initialTermId(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getInt(LOG_INITIAL_TERM_ID_OFFSET);
    }

    /**
     * Set the initial term at which this log begins. Initial should be randomised so that stream does not get
     * reused accidentally.
     *
     * @param metadataBuffer containing the meta data.
     * @param initialTermId  value to be set.
     */
    public static void initialTermId(final UnsafeBuffer metadataBuffer, final int initialTermId)
    {
        metadataBuffer.putInt(LOG_INITIAL_TERM_ID_OFFSET, initialTermId);
    }

    /**
     * Get the value of the MTU length used for this log.
     *
     * @param metadataBuffer containing the meta data.
     * @return the value of the MTU length used for this log.
     */
    public static int mtuLength(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getInt(LOG_MTU_LENGTH_OFFSET);
    }

    /**
     * Set the MTU length used for this log.
     *
     * @param metadataBuffer containing the meta data.
     * @param mtuLength      value to be set.
     */
    public static void mtuLength(final UnsafeBuffer metadataBuffer, final int mtuLength)
    {
        metadataBuffer.putInt(LOG_MTU_LENGTH_OFFSET, mtuLength);
    }

    /**
     * Get the value of the Term Length used for this log.
     *
     * @param metadataBuffer containing the meta data.
     * @return the value of the term length used for this log.
     */
    public static int termLength(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getInt(LOG_TERM_LENGTH_OFFSET);
    }

    /**
     * Set the term length used for this log.
     *
     * @param metadataBuffer containing the meta data.
     * @param termLength     value to be set.
     */
    public static void termLength(final UnsafeBuffer metadataBuffer, final int termLength)
    {
        metadataBuffer.putInt(LOG_TERM_LENGTH_OFFSET, termLength);
    }

    /**
     * Get the value of the page size used for this log.
     *
     * @param metadataBuffer containing the meta data.
     * @return the value of the page size used for this log.
     */
    public static int pageSize(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getInt(LOG_PAGE_SIZE_OFFSET);
    }

    /**
     * Set the page size used for this log.
     *
     * @param metadataBuffer containing the meta data.
     * @param pageSize       value to be set.
     */
    public static void pageSize(final UnsafeBuffer metadataBuffer, final int pageSize)
    {
        metadataBuffer.putInt(LOG_PAGE_SIZE_OFFSET, pageSize);
    }

    /**
     * Get the value of the correlation ID for this log relating to the command which created it.
     *
     * @param metadataBuffer containing the meta data.
     * @return the value of the correlation ID used for this log.
     */
    public static long correlationId(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getLong(LOG_CORRELATION_ID_OFFSET);
    }

    /**
     * Set the correlation ID used for this log relating to the command which created it.
     *
     * @param metadataBuffer containing the meta data.
     * @param id             value to be set.
     */
    public static void correlationId(final UnsafeBuffer metadataBuffer, final long id)
    {
        metadataBuffer.putLong(LOG_CORRELATION_ID_OFFSET, id);
    }

    /**
     * Get whether the log is considered connected or not by the driver.
     *
     * @param metadataBuffer containing the meta data.
     * @return whether the log is considered connected or not by the driver.
     */
    public static boolean isConnected(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getIntVolatile(LOG_IS_CONNECTED_OFFSET) == 1;
    }

    /**
     * Set whether the log is considered connected or not by the driver.
     *
     * @param metadataBuffer containing the meta data.
     * @param isConnected    or not.
     */
    public static void isConnected(final UnsafeBuffer metadataBuffer, final boolean isConnected)
    {
        metadataBuffer.putIntRelease(LOG_IS_CONNECTED_OFFSET, isConnected ? 1 : 0);
    }

    /**
     * Get the count of active transports for the Image.
     *
     * @param metadataBuffer containing the meta data.
     * @return count of active transports.
     */
    public static int activeTransportCount(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getIntVolatile(LOG_ACTIVE_TRANSPORT_COUNT);
    }

    /**
     * Set the number of active transports for the Image.
     *
     * @param metadataBuffer           containing the meta data.
     * @param numberOfActiveTransports value to be set.
     */
    public static void activeTransportCount(final UnsafeBuffer metadataBuffer, final int numberOfActiveTransports)
    {
        metadataBuffer.putIntRelease(LOG_ACTIVE_TRANSPORT_COUNT, numberOfActiveTransports);
    }

    /**
     * Get the value of the end of stream position.
     *
     * @param metadataBuffer containing the meta data.
     * @return the value of end of stream position
     */
    public static long endOfStreamPosition(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getLongVolatile(LOG_END_OF_STREAM_POSITION_OFFSET);
    }

    /**
     * Set the value of the end of stream position.
     *
     * @param metadataBuffer containing the meta data.
     * @param position       value of the end of stream position.
     */
    public static void endOfStreamPosition(final UnsafeBuffer metadataBuffer, final long position)
    {
        metadataBuffer.putLongRelease(LOG_END_OF_STREAM_POSITION_OFFSET, position);
    }

    /**
     * Get the value of the active term count used by the producer of this log. Consumers may have a different
     * active term count if they are running behind. The read is done with volatile semantics.
     *
     * @param metadataBuffer containing the meta data.
     * @return the value of the active term count used by the producer of this log.
     */
    public static int activeTermCount(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getIntVolatile(LOG_ACTIVE_TERM_COUNT_OFFSET);
    }

    /**
     * Set the value of the current active term count for the producer using memory release semantics.
     *
     * @param metadataBuffer containing the meta data.
     * @param termCount      value of the active term count used by the producer of this log.
     */
    public static void activeTermCountOrdered(final UnsafeBuffer metadataBuffer, final int termCount)
    {
        metadataBuffer.putIntRelease(LOG_ACTIVE_TERM_COUNT_OFFSET, termCount);
    }

    /**
     * Compare and set the value of the current active term count.
     *
     * @param metadataBuffer    containing the meta data.
     * @param expectedTermCount value of the active term count expected in the log.
     * @param updateTermCount   value of the active term count to be updated in the log.
     * @return true if successful otherwise false.
     */
    public static boolean casActiveTermCount(
        final UnsafeBuffer metadataBuffer, final int expectedTermCount, final int updateTermCount)
    {
        return metadataBuffer.compareAndSetInt(LOG_ACTIVE_TERM_COUNT_OFFSET, expectedTermCount, updateTermCount);
    }

    /**
     * Set the value of the current active partition index for the producer.
     *
     * @param metadataBuffer containing the meta data.
     * @param termCount      value of the active term count used by the producer of this log.
     */
    public static void activeTermCount(final UnsafeBuffer metadataBuffer, final int termCount)
    {
        metadataBuffer.putInt(LOG_ACTIVE_TERM_COUNT_OFFSET, termCount);
    }

    /**
     * Rotate to the next partition in sequence for the term id.
     *
     * @param currentIndex partition index.
     * @return the next partition index.
     */
    public static int nextPartitionIndex(final int currentIndex)
    {
        return (currentIndex + 1) % PARTITION_COUNT;
    }

    /**
     * Determine the partition index to be used given the initial term and active term ids.
     *
     * @param initialTermId at which the log buffer usage began.
     * @param activeTermId  that is in current usage.
     * @return the index of which buffer should be used.
     */
    public static int indexByTerm(final int initialTermId, final int activeTermId)
    {
        return (activeTermId - initialTermId) % PARTITION_COUNT;
    }

    /**
     * Determine the partition index based on number of terms that have passed.
     *
     * @param termCount for the number of terms that have passed.
     * @return the partition index for the term count.
     */
    public static int indexByTermCount(final long termCount)
    {
        return (int)(termCount % PARTITION_COUNT);
    }

    /**
     * Determine the partition index given a stream position.
     *
     * @param position            in the stream in bytes.
     * @param positionBitsToShift number of times to left shift the term count to multiply by term length.
     * @return the partition index for the position.
     */
    public static int indexByPosition(final long position, final int positionBitsToShift)
    {
        return (int)((position >>> positionBitsToShift) % PARTITION_COUNT);
    }

    /**
     * Compute the current position in absolute number of bytes.
     *
     * @param activeTermId        active term id.
     * @param termOffset          in the term.
     * @param positionBitsToShift number of times to left shift the term count to multiply by term length.
     * @param initialTermId       the initial term id that this stream started on.
     * @return the absolute position in bytes.
     */
    public static long computePosition(
        final int activeTermId, final int termOffset, final int positionBitsToShift, final int initialTermId)
    {
        final long termCount = activeTermId - initialTermId; // copes with negative activeTermId on rollover

        return (termCount << positionBitsToShift) + termOffset;
    }

    /**
     * Compute the current position in absolute number of bytes for the beginning of a term.
     *
     * @param activeTermId        active term id.
     * @param positionBitsToShift number of times to left shift the term count to multiply by term length.
     * @param initialTermId       the initial term id that this stream started on.
     * @return the absolute position in bytes.
     */
    public static long computeTermBeginPosition(
        final int activeTermId, final int positionBitsToShift, final int initialTermId)
    {
        final long termCount = activeTermId - initialTermId; // copes with negative activeTermId on rollover

        return termCount << positionBitsToShift;
    }

    /**
     * Compute the term id from a position.
     *
     * @param position            to calculate from
     * @param positionBitsToShift number of times to left shift the term count to multiply by term length.
     * @param initialTermId       the initial term id that this stream started on.
     * @return the term id according to the position.
     */
    public static int computeTermIdFromPosition(
        final long position, final int positionBitsToShift, final int initialTermId)
    {
        return (int)(position >>> positionBitsToShift) + initialTermId;
    }

    /**
     * Compute the total length of a log file given the term length.
     * <p>
     * Assumes {@link #TERM_MAX_LENGTH} is 1 GB and that filePageSize is 1 GB or less and a power of 2.
     *
     * @param termLength   on which to base the calculation.
     * @param filePageSize to use for log.
     * @return the total length of the log file.
     */
    public static long computeLogLength(final int termLength, final int filePageSize)
    {
        return align((PARTITION_COUNT * (long)termLength) + LOG_META_DATA_LENGTH, filePageSize);
    }

    /**
     * Store the default frame header to the log metadata buffer.
     *
     * @param metadataBuffer into which the default headers should be stored.
     * @param defaultHeader  to be stored.
     * @throws IllegalArgumentException if the defaultHeader larger than {@link #LOG_DEFAULT_FRAME_HEADER_MAX_LENGTH}.
     */
    public static void storeDefaultFrameHeader(final UnsafeBuffer metadataBuffer, final DirectBuffer defaultHeader)
    {
        if (defaultHeader.capacity() != HEADER_LENGTH)
        {
            throw new IllegalArgumentException(
                "Default header length not equal to HEADER_LENGTH: length=" + defaultHeader.capacity());
        }

        metadataBuffer.putInt(LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET, HEADER_LENGTH);
        metadataBuffer.putBytes(LOG_DEFAULT_FRAME_HEADER_OFFSET, defaultHeader, 0, HEADER_LENGTH);
    }

    /**
     * Get a wrapper around the default frame header from the log metadata.
     *
     * @param metadataBuffer containing the raw bytes for the default frame header.
     * @return a buffer wrapping the raw bytes.
     */
    public static UnsafeBuffer defaultFrameHeader(final UnsafeBuffer metadataBuffer)
    {
        return new UnsafeBuffer(metadataBuffer, LOG_DEFAULT_FRAME_HEADER_OFFSET, HEADER_LENGTH);
    }

    /**
     * Apply the default header for a message in a term.
     *
     * @param metadataBuffer containing the default headers.
     * @param termBuffer     to which the default header should be applied.
     * @param termOffset     at which the default should be applied.
     */
    public static void applyDefaultHeader(
        final UnsafeBuffer metadataBuffer, final UnsafeBuffer termBuffer, final int termOffset)
    {
        termBuffer.putBytes(termOffset, metadataBuffer, LOG_DEFAULT_FRAME_HEADER_OFFSET, HEADER_LENGTH);
    }

    /**
     * Rotate the log and update the tail counter for the new term.
     * <p>
     * This method is safe for concurrent use.
     *
     * @param metadataBuffer for the log.
     * @param termCount      from which to rotate.
     * @param termId         to be used in the default headers.
     * @return true if log was rotated.
     */
    public static boolean rotateLog(final UnsafeBuffer metadataBuffer, final int termCount, final int termId)
    {
        final int nextTermId = termId + 1;
        final int nextTermCount = termCount + 1;
        final int nextIndex = indexByTermCount(nextTermCount);
        final int expectedTermId = nextTermId - PARTITION_COUNT;

        long rawTail;
        do
        {
            rawTail = rawTailVolatile(metadataBuffer, nextIndex);
            if (expectedTermId != termId(rawTail))
            {
                break;
            }
        }
        while (!casRawTail(metadataBuffer, nextIndex, rawTail, packTail(nextTermId, 0)));

        return casActiveTermCount(metadataBuffer, termCount, nextTermCount);
    }

    /**
     * Set the initial value for the termId in the upper bits of the tail counter.
     *
     * @param metadataBuffer contain the tail counter.
     * @param partitionIndex to be initialised.
     * @param termId         to be set.
     */
    public static void initialiseTailWithTermId(
        final UnsafeBuffer metadataBuffer, final int partitionIndex, final int termId)
    {
        metadataBuffer.putLong(TERM_TAIL_COUNTERS_OFFSET + (partitionIndex * SIZE_OF_LONG), packTail(termId, 0));
    }

    /**
     * Get the termId from a packed raw tail value.
     *
     * @param rawTail containing the termId.
     * @return the termId from a packed raw tail value.
     */
    public static int termId(final long rawTail)
    {
        return (int)(rawTail >> 32);
    }

    /**
     * Read the termOffset from a packed raw tail value.
     *
     * @param rawTail    containing the termOffset.
     * @param termLength that the offset cannot exceed.
     * @return the termOffset value.
     */
    public static int termOffset(final long rawTail, final long termLength)
    {
        final long tail = rawTail & 0xFFFF_FFFFL;

        return (int)Math.min(tail, termLength);
    }

    /**
     * The termOffset as a result of the append operation.
     *
     * @param result into which the termOffset value has been packed.
     * @return the termOffset after the append operation.
     */
    public static int termOffset(final long result)
    {
        return (int)result;
    }

    /**
     * Pack a termId and termOffset into a raw tail value.
     *
     * @param termId     to be packed.
     * @param termOffset to be packed.
     * @return the packed value.
     */
    public static long packTail(final int termId, final int termOffset)
    {
        return ((long)termId << 32) | termOffset;
    }

    /**
     * Set the raw value of the tail for the given partition.
     *
     * @param metadataBuffer containing the tail counters.
     * @param partitionIndex for the tail counter.
     * @param rawTail        to be stored.
     */
    public static void rawTail(final UnsafeBuffer metadataBuffer, final int partitionIndex, final long rawTail)
    {
        metadataBuffer.putLong(TERM_TAIL_COUNTERS_OFFSET + (SIZE_OF_LONG * partitionIndex), rawTail);
    }

    /**
     * Get the raw value of the tail for the given partition.
     *
     * @param metadataBuffer containing the tail counters.
     * @param partitionIndex for the tail counter.
     * @return the raw value of the tail for the current active partition.
     */
    public static long rawTail(final UnsafeBuffer metadataBuffer, final int partitionIndex)
    {
        return metadataBuffer.getLong(TERM_TAIL_COUNTERS_OFFSET + (SIZE_OF_LONG * partitionIndex));
    }

    /**
     * Set the raw value of the tail for the given partition.
     *
     * @param metadataBuffer containing the tail counters.
     * @param partitionIndex for the tail counter.
     * @param rawTail        to be stored.
     */
    public static void rawTailVolatile(final UnsafeBuffer metadataBuffer, final int partitionIndex, final long rawTail)
    {
        metadataBuffer.putLongVolatile(TERM_TAIL_COUNTERS_OFFSET + (SIZE_OF_LONG * partitionIndex), rawTail);
    }

    /**
     * Get the raw value of the tail for the given partition.
     *
     * @param metadataBuffer containing the tail counters.
     * @param partitionIndex for the tail counter.
     * @return the raw value of the tail for the current active partition.
     */
    public static long rawTailVolatile(final UnsafeBuffer metadataBuffer, final int partitionIndex)
    {
        return metadataBuffer.getLongVolatile(TERM_TAIL_COUNTERS_OFFSET + (SIZE_OF_LONG * partitionIndex));
    }

    /**
     * Get the raw value of the tail for the current active partition.
     *
     * @param metadataBuffer containing the tail counters.
     * @return the raw value of the tail for the current active partition.
     */
    public static long rawTailVolatile(final UnsafeBuffer metadataBuffer)
    {
        final int partitionIndex = indexByTermCount(activeTermCount(metadataBuffer));
        return metadataBuffer.getLongVolatile(TERM_TAIL_COUNTERS_OFFSET + (SIZE_OF_LONG * partitionIndex));
    }

    /**
     * Compare and set the raw value of the tail for the given partition.
     *
     * @param metadataBuffer  containing the tail counters.
     * @param partitionIndex  for the tail counter.
     * @param expectedRawTail expected current value.
     * @param updateRawTail   to be applied.
     * @return true if the update was successful otherwise false.
     */
    public static boolean casRawTail(
        final UnsafeBuffer metadataBuffer,
        final int partitionIndex,
        final long expectedRawTail,
        final long updateRawTail)
    {
        final int index = TERM_TAIL_COUNTERS_OFFSET + (SIZE_OF_LONG * partitionIndex);
        return metadataBuffer.compareAndSetLong(index, expectedRawTail, updateRawTail);
    }

    /**
     * Get the number of bits to shift when dividing or multiplying by the term buffer length.
     *
     * @param termBufferLength to compute the number of bits to shift for.
     * @return the number of bits to shift to divide or multiply by the term buffer length.
     */
    public static int positionBitsToShift(final int termBufferLength)
    {
        return switch (termBufferLength)
        {
            case 64 * 1024 -> 16;
            case 128 * 1024 -> 17;
            case 256 * 1024 -> 18;
            case 512 * 1024 -> 19;
            case 1024 * 1024 -> 20;
            case 2 * 1024 * 1024 -> 21;
            case 4 * 1024 * 1024 -> 22;
            case 8 * 1024 * 1024 -> 23;
            case 16 * 1024 * 1024 -> 24;
            case 32 * 1024 * 1024 -> 25;
            case 64 * 1024 * 1024 -> 26;
            case 128 * 1024 * 1024 -> 27;
            case 256 * 1024 * 1024 -> 28;
            case 512 * 1024 * 1024 -> 29;
            case 1024 * 1024 * 1024 -> 30;
            default -> throw new IllegalArgumentException("invalid term buffer length: " + termBufferLength);
        };
    }

    /**
     * Compute frame length for a message that is fragmented into chunks of {@code maxPayloadSize}.
     *
     * @param length         of the message.
     * @param maxPayloadSize fragment size without the header.
     * @return message length after fragmentation.
     */
    public static int computeFragmentedFrameLength(final int length, final int maxPayloadSize)
    {
        final int numMaxPayloads = length / maxPayloadSize;
        final int remainingPayload = length % maxPayloadSize;
        final int lastFrameLength =
            remainingPayload > 0 ? align(remainingPayload + HEADER_LENGTH, FRAME_ALIGNMENT) : 0;

        return (numMaxPayloads * (maxPayloadSize + HEADER_LENGTH)) + lastFrameLength;
    }

    /**
     * Compute frame length for a message that has been reassembled from chunks of {@code maxPayloadSize}.
     *
     * @param length         of the message.
     * @param maxPayloadSize fragment size without the header.
     * @return message length after fragmentation.
     */
    public static int computeAssembledFrameLength(final int length, final int maxPayloadSize)
    {
        final int numMaxPayloads = length / maxPayloadSize;
        final int remainingPayload = length % maxPayloadSize;

        return HEADER_LENGTH + (numMaxPayloads * maxPayloadSize) + remainingPayload;
    }

    /**
     * Get whether the log is sparse from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return true if the log is sparse, otherwise false.
     */
    public static boolean sparse(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getByte(LOG_SPARSE_OFFSET) == 1;
    }

    /**
     * Set whether the log is sparse in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          true if the log is sparse, otherwise false.
     */
    public static void sparse(final UnsafeBuffer metadataBuffer, final boolean value)
    {
        metadataBuffer.putByte(LOG_SPARSE_OFFSET, (byte)(value ? 1 : 0));
    }

    /**
     * Get whether the log is tethered from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return true if the log is tethered, otherwise false.
     */
    public static boolean tether(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getByte(LOG_TETHER_OFFSET) == 1;
    }

    /**
     * Set whether the log is tethered in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          true if the log is tethered, otherwise false.
     */
    public static void tether(final UnsafeBuffer metadataBuffer, final boolean value)
    {
        metadataBuffer.putByte(LOG_TETHER_OFFSET, (byte)(value ? 1 : 0));
    }

    /**
     * Get whether the log's publication was revoked.
     *
     * @param metadataBuffer containing the meta data.
     * @return true if the log's publication was revoked, otherwise false.
     */
    public static boolean isPublicationRevoked(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getByte(LOG_IS_PUBLICATION_REVOKED_OFFSET) == 1;
    }

    /**
     * Set whether the log's publication was revoked.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          true if the log's publication was revoked, otherwise false.
     */
    public static void isPublicationRevoked(final UnsafeBuffer metadataBuffer, final boolean value)
    {
        metadataBuffer.putByte(LOG_IS_PUBLICATION_REVOKED_OFFSET, (byte)(value ? 1 : 0));
    }

    /**
     * Get whether the log is group from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return true if the log is group, otherwise false.
     */
    public static boolean group(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getByte(LOG_GROUP_OFFSET) == 1;
    }

    /**
     * Set whether the log is group in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          true if the log is group, otherwise false.
     */
    public static void group(final UnsafeBuffer metadataBuffer, final boolean value)
    {
        metadataBuffer.putByte(LOG_GROUP_OFFSET, (byte)(value ? 1 : 0));
    }

    /**
     * Get whether the log is response from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return true if the log is group, otherwise false.
     */
    public static boolean isResponse(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getByte(LOG_IS_RESPONSE_OFFSET) == 1;
    }

    /**
     * Set whether the log is response in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          true if the log is group, otherwise false.
     */
    public static void isResponse(final UnsafeBuffer metadataBuffer, final boolean value)
    {
        metadataBuffer.putByte(LOG_IS_RESPONSE_OFFSET, (byte)(value ? 1 : 0));
    }


    /**
     * Get whether the log is rejoining from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return true if the log is rejoining, otherwise false.
     */
    public static boolean rejoin(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getByte(LOG_REJOIN_OFFSET) == 1;
    }

    /**
     * Set whether the log is rejoining in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          true if the log is rejoining, otherwise false.
     */
    public static void rejoin(final UnsafeBuffer metadataBuffer, final boolean value)
    {
        metadataBuffer.putByte(LOG_REJOIN_OFFSET, (byte)(value ? 1 : 0));
    }

    /**
     * Get whether the log is reliable from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return true if the log is reliable, otherwise false.
     */
    public static boolean reliable(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getByte(LOG_RELIABLE_OFFSET) == 1;
    }

    /**
     * Set whether the log is reliable in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          true if the log is reliable, otherwise false.
     */
    public static void reliable(final UnsafeBuffer metadataBuffer, final boolean value)
    {
        metadataBuffer.putByte(LOG_RELIABLE_OFFSET, (byte)(value ? 1 : 0));
    }

    /**
     * Get the socket receive buffer length from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return the socket receive buffer length.
     */
    public static int socketRcvbufLength(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getInt(LOG_SOCKET_RCVBUF_LENGTH_OFFSET);
    }

    /**
     * Set the socket receive buffer length in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          the socket receive buffer length to set.
     */
    public static void socketRcvbufLength(final UnsafeBuffer metadataBuffer, final int value)
    {
        metadataBuffer.putInt(LOG_SOCKET_RCVBUF_LENGTH_OFFSET, value);
    }

    /**
     * Get the default length in bytes for the socket receive buffer as per OS configuration from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return the default length in bytes for the socket receive buffer.
     */
    public static int osDefaultSocketRcvbufLength(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getInt(LOG_OS_DEFAULT_SOCKET_RCVBUF_LENGTH_OFFSET);
    }

    /**
     * Set the default length for the socket receive buffer as per OS configuration in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          the default length in bytes for the socket receive buffer.
     */
    public static void osDefaultSocketRcvbufLength(final UnsafeBuffer metadataBuffer, final int value)
    {
        metadataBuffer.putInt(LOG_OS_DEFAULT_SOCKET_RCVBUF_LENGTH_OFFSET, value);
    }

    /**
     * Get the maximum length in bytes for the socket receive buffer as per OS configuration from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return the maximum allowed length in bytes for the socket receive buffer.
     */
    public static int osMaxSocketRcvbufLength(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getInt(LOG_OS_MAX_SOCKET_RCVBUF_LENGTH_OFFSET);
    }

    /**
     * Set the maximum allowed length in bytes for the socket receive buffer as per OS configuration in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          the maximum allowed length in bytes for the socket receive buffer.
     */
    public static void osMaxSocketRcvbufLength(final UnsafeBuffer metadataBuffer, final int value)
    {
        metadataBuffer.putInt(LOG_OS_MAX_SOCKET_RCVBUF_LENGTH_OFFSET, value);
    }

    /**
     * Get the socket send buffer length from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return the socket send buffer length.
     */
    public static int socketSndbufLength(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getInt(LOG_SOCKET_SNDBUF_LENGTH_OFFSET);
    }

    /**
     * Set the socket send buffer length in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          the socket send buffer length to set.
     */
    public static void socketSndbufLength(final UnsafeBuffer metadataBuffer, final int value)
    {
        metadataBuffer.putInt(LOG_SOCKET_SNDBUF_LENGTH_OFFSET, value);
    }

    /**
     * Get the default length in bytes for the socket send buffer as per OS configuration from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return the default length in bytes for the socket send buffer.
     */
    public static int osDefaultSocketSndbufLength(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getInt(LOG_OS_DEFAULT_SOCKET_SNDBUF_LENGTH_OFFSET);
    }

    /**
     * Set the default length for the socket send buffer as per OS configuration in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          the default length in bytes for the socket send buffer.
     */
    public static void osDefaultSocketSndbufLength(final UnsafeBuffer metadataBuffer, final int value)
    {
        metadataBuffer.putInt(LOG_OS_DEFAULT_SOCKET_SNDBUF_LENGTH_OFFSET, value);
    }

    /**
     * Get the maximum length in bytes for the socket send buffer as per OS configuration from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return the maximum allowed length in bytes for the socket send buffer.
     */
    public static int osMaxSocketSndbufLength(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getInt(LOG_OS_MAX_SOCKET_SNDBUF_LENGTH_OFFSET);
    }

    /**
     * Set the maximum allowed length in bytes for the socket send buffer as per OS configuration in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          the maximum allowed length in bytes for the socket send buffer.
     */
    public static void osMaxSocketSndbufLength(final UnsafeBuffer metadataBuffer, final int value)
    {
        metadataBuffer.putInt(LOG_OS_MAX_SOCKET_SNDBUF_LENGTH_OFFSET, value);
    }

    /**
     * Get the receiver window length from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return the receiver window length.
     */
    public static int receiverWindowLength(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getInt(LOG_RECEIVER_WINDOW_LENGTH_OFFSET);
    }

    /**
     * Set the receiver window length in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          the receiver window length to set.
     */
    public static void receiverWindowLength(final UnsafeBuffer metadataBuffer, final int value)
    {
        metadataBuffer.putInt(LOG_RECEIVER_WINDOW_LENGTH_OFFSET, value);
    }

    /**
     * Get the publication window length from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return the publication window length.
     */
    public static int publicationWindowLength(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getInt(LOG_PUBLICATION_WINDOW_LENGTH_OFFSET);
    }

    /**
     * Set the publication window length in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          the publication window length to set.
     */
    public static void publicationWindowLength(final UnsafeBuffer metadataBuffer, final int value)
    {
        metadataBuffer.putInt(LOG_PUBLICATION_WINDOW_LENGTH_OFFSET, value);
    }

    /**
     * Get the untethered window limit timeout in nanoseconds from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return the untethered window limit timeout in nanoseconds.
     */
    public static long untetheredWindowLimitTimeoutNs(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getLong(LOG_UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS_OFFSET);
    }

    /**
     * Set the untethered window limit timeout in nanoseconds in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          the untethered window limit timeout to set.
     */
    public static void untetheredWindowLimitTimeoutNs(final UnsafeBuffer metadataBuffer, final long value)
    {
        metadataBuffer.putLong(LOG_UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS_OFFSET, value);
    }

    /**
     * Get the untethered linger timeout in nanoseconds from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return the untethered window limit timeout in nanoseconds.
     */
    public static long untetheredLingerTimeoutNs(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getLong(LOG_UNTETHERED_LINGER_TIMEOUT_NS_OFFSET);
    }

    /**
     * Set the untethered linger timeout in nanoseconds in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          the untethered linger timeout to set.
     */
    public static void untetheredLingerTimeoutNs(final UnsafeBuffer metadataBuffer, final long value)
    {
        metadataBuffer.putLong(LOG_UNTETHERED_LINGER_TIMEOUT_NS_OFFSET, value);
    }

    /**
     * Get the untethered resting timeout in nanoseconds from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return the untethered resting timeout in nanoseconds.
     */
    public static long untetheredRestingTimeoutNs(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getLong(LOG_UNTETHERED_RESTING_TIMEOUT_NS_OFFSET);
    }

    /**
     * Set the untethered resting timeout in nanoseconds in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          the untethered resting timeout to set.
     */
    public static void untetheredRestingTimeoutNs(final UnsafeBuffer metadataBuffer, final long value)
    {
        metadataBuffer.putLong(LOG_UNTETHERED_RESTING_TIMEOUT_NS_OFFSET, value);
    }

    /**
     * Get the maximum resend count from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return the maximum resend count.
     */
    public static int maxResend(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getInt(LOG_MAX_RESEND_OFFSET);
    }

    /**
     * Set the maximum resend count in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          the maximum resend count to set.
     */
    public static void maxResend(final UnsafeBuffer metadataBuffer, final int value)
    {
        metadataBuffer.putInt(LOG_MAX_RESEND_OFFSET, value);
    }

    /**
     * Get the linger timeout in nanoseconds from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return the linger timeout in nanoseconds.
     */
    public static long lingerTimeoutNs(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getLong(LOG_LINGER_TIMEOUT_NS_OFFSET);
    }

    /**
     * Set the linger timeout in nanoseconds in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          the linger timeout to set.
     */
    public static void lingerTimeoutNs(final UnsafeBuffer metadataBuffer, final long value)
    {
        metadataBuffer.putLong(LOG_LINGER_TIMEOUT_NS_OFFSET, value);
    }

    /**
     * Get the entity tag  from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return the entity tag in nanoseconds.
     */
    public static long entityTag(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getLong(LOG_ENTITY_TAG_OFFSET);
    }

    /**
     * Set the entity tag in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          the entity tag to set.
     */
    public static void entityTag(final UnsafeBuffer metadataBuffer, final long value)
    {
        metadataBuffer.putLong(LOG_ENTITY_TAG_OFFSET, value);
    }

    /**
     * Get the response correlation id  from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return the entity tag in nanoseconds.
     */
    public static long responseCorrelationId(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getLong(LOG_RESPONSE_CORRELATION_ID_OFFSET);
    }

    /**
     * Set the response correlation id in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          the resonse correlation id to set.
     */
    public static void responseCorrelationId(final UnsafeBuffer metadataBuffer, final long value)
    {
        metadataBuffer.putLong(LOG_RESPONSE_CORRELATION_ID_OFFSET, value);
    }

    /**
     * Get whether the signal EOS is enabled from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return true if signal EOS is enabled, otherwise false.
     */
    public static boolean signalEos(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getByte(LOG_SIGNAL_EOS_OFFSET) == 1;
    }

    /**
     * Set whether the signal EOS is enabled in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          true if signal EOS is enabled, otherwise false.
     */
    public static void signalEos(final UnsafeBuffer metadataBuffer, final boolean value)
    {
        metadataBuffer.putByte(LOG_SIGNAL_EOS_OFFSET, (byte)(value ? 1 : 0));
    }

    /**
     * Get whether spies simulate connection from the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @return true if spies simulate connection, otherwise false.
     */
    public static boolean spiesSimulateConnection(final UnsafeBuffer metadataBuffer)
    {
        return metadataBuffer.getByte(LOG_SPIES_SIMULATE_CONNECTION_OFFSET) == 1;
    }

    /**
     * Set whether spies simulate connection in the metadata.
     *
     * @param metadataBuffer containing the meta data.
     * @param value          true if spies simulate connection, otherwise false.
     */
    public static void spiesSimulateConnection(final UnsafeBuffer metadataBuffer, final boolean value)
    {
        metadataBuffer.putByte(LOG_SPIES_SIMULATE_CONNECTION_OFFSET, (byte)(value ? 1 : 0));
    }
}
