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

import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

import static io.aeron.logbuffer.LogBufferDescriptor.LOG_META_DATA_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.LOG_META_DATA_SECTION_INDEX;
import static io.aeron.logbuffer.LogBufferDescriptor.PARTITION_COUNT;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MAX_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.checkPageSize;
import static io.aeron.logbuffer.LogBufferDescriptor.checkTermLength;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SPARSE;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Takes a log file name and maps the file into memory and wraps it with {@link UnsafeBuffer}s as appropriate.
 *
 * @see io.aeron.logbuffer.LogBufferDescriptor
 */
public final class LogBuffers implements AutoCloseable
{
    private static final EnumSet<StandardOpenOption> FILE_OPTIONS = EnumSet.of(READ, WRITE, SPARSE);

    private long lingerDeadlineNs = Long.MAX_VALUE;
    private int refCount;
    private final int termLength;
    private final FileChannel fileChannel;
    private final ByteBuffer[] termBuffers = new ByteBuffer[PARTITION_COUNT];
    private final UnsafeBuffer logMetaDataBuffer;
    private final MappedByteBuffer[] mappedByteBuffers;

    /**
     * Construct the log buffers for a given log file.
     *
     * @param logFileName to be mapped.
     */
    public LogBuffers(final String logFileName)
    {
        int termLength = 0;
        FileChannel fileChannel = null;
        UnsafeBuffer logMetaDataBuffer = null;
        MappedByteBuffer[] mappedByteBuffers = null;

        try
        {
            fileChannel = FileChannel.open(Paths.get(logFileName), FILE_OPTIONS);
            final long logLength = fileChannel.size();
            if (logLength < LOG_META_DATA_LENGTH)
            {
                throw new IllegalStateException(
                    "Log file length less than min length of " + LOG_META_DATA_LENGTH + ": length=" + logLength);
            }

            if (logLength < Integer.MAX_VALUE)
            {
                final MappedByteBuffer mappedBuffer = fileChannel.map(READ_WRITE, 0, logLength);
                mappedBuffer.order(ByteOrder.LITTLE_ENDIAN);
                mappedByteBuffers = new MappedByteBuffer[]{ mappedBuffer };

                logMetaDataBuffer = new UnsafeBuffer(
                    mappedBuffer, (int)(logLength - LOG_META_DATA_LENGTH), LOG_META_DATA_LENGTH);

                termLength = LogBufferDescriptor.termLength(logMetaDataBuffer);
                final int pageSize = LogBufferDescriptor.pageSize(logMetaDataBuffer);

                checkTermLength(termLength);
                checkPageSize(pageSize);

                for (int i = 0; i < PARTITION_COUNT; i++)
                {
                    final int offset = i * termLength;
                    mappedBuffer.limit(offset + termLength).position(offset);

                    termBuffers[i] = mappedBuffer.slice();
                }
            }
            else
            {
                mappedByteBuffers = new MappedByteBuffer[PARTITION_COUNT + 1];

                final int assumedTermLength = TERM_MAX_LENGTH;

                final long metaDataSectionOffset = assumedTermLength * (long)PARTITION_COUNT;
                final long metaDataMappingLength = logLength - metaDataSectionOffset;

                final MappedByteBuffer metaDataMappedBuffer = fileChannel.map(
                    READ_WRITE, metaDataSectionOffset, metaDataMappingLength);
                metaDataMappedBuffer.order(ByteOrder.LITTLE_ENDIAN);

                mappedByteBuffers[LOG_META_DATA_SECTION_INDEX] = metaDataMappedBuffer;

                logMetaDataBuffer = new UnsafeBuffer(
                    metaDataMappedBuffer,
                    (int)metaDataMappingLength - LOG_META_DATA_LENGTH,
                    LOG_META_DATA_LENGTH);

                final int metaDataTermLength = LogBufferDescriptor.termLength(logMetaDataBuffer);
                final int pageSize = LogBufferDescriptor.pageSize(logMetaDataBuffer);

                checkPageSize(pageSize);
                if (metaDataTermLength != assumedTermLength)
                {
                    throw new IllegalStateException(
                        "assumed term length " + assumedTermLength +
                        " does not match metadata: termLength=" + metaDataTermLength);
                }

                termLength = assumedTermLength;

                for (int i = 0; i < PARTITION_COUNT; i++)
                {
                    final long position = assumedTermLength * (long)i;
                    final MappedByteBuffer mappedBuffer = fileChannel.map(READ_WRITE, position, assumedTermLength);
                    mappedBuffer.order(ByteOrder.LITTLE_ENDIAN);
                    mappedByteBuffers[i] = mappedBuffer;
                    termBuffers[i] = mappedBuffer;
                }
            }
        }
        catch (final Exception ex)
        {
            close(fileChannel, logMetaDataBuffer, mappedByteBuffers);
            LangUtil.rethrowUnchecked(ex);
        }

        this.termLength = termLength;
        this.fileChannel = fileChannel;
        this.logMetaDataBuffer = logMetaDataBuffer;
        this.mappedByteBuffers = mappedByteBuffers;
    }

    /**
     * Duplicate the underlying {@link ByteBuffer}s and wrap them for thread local access.
     *
     * @return duplicates of the wrapped underlying {@link ByteBuffer}s.
     */
    public UnsafeBuffer[] duplicateTermBuffers()
    {
        final UnsafeBuffer[] buffers = new UnsafeBuffer[PARTITION_COUNT];

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            buffers[i] = new UnsafeBuffer(termBuffers[i].duplicate().order(ByteOrder.LITTLE_ENDIAN));
        }

        return buffers;
    }

    /**
     * Get the buffer which holds the log metadata.
     *
     * @return the buffer which holds the log metadata.
     */
    public UnsafeBuffer metaDataBuffer()
    {
        return logMetaDataBuffer;
    }

    /**
     * The {@link FileChannel} for the mapped log.
     *
     * @return the {@link FileChannel} for the mapped log.
     */
    public FileChannel fileChannel()
    {
        return fileChannel;
    }

    /**
     * Pre touch memory pages, so they are faulted in to be available before access.
     */
    public void preTouch()
    {
        final int value = 0;
        final int pageSize = LogBufferDescriptor.pageSize(logMetaDataBuffer);
        final UnsafeBuffer atomicBuffer = new UnsafeBuffer();

        for (final MappedByteBuffer buffer : mappedByteBuffers)
        {
            atomicBuffer.wrap(buffer);

            for (int i = 0, length = atomicBuffer.capacity(); i < length; i += pageSize)
            {
                atomicBuffer.compareAndSetInt(i, value, value);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        close(fileChannel, logMetaDataBuffer, mappedByteBuffers);
    }

    /**
     * The length of the term buffer in each log partition.
     *
     * @return length of the term buffer in each log partition.
     */
    public int termLength()
    {
        return termLength;
    }

    /**
     * Increment reference count.
     *
     * @return current reference count after increment.
     */
    public int incRef()
    {
        return ++refCount;
    }

    /**
     * Decrement reference count.
     *
     * @return current reference counter after decrement.
     */
    public int decRef()
    {
        return --refCount;
    }

    /**
     * Set the deadline for how long to linger around once unreferenced.
     *
     * @param timeNs the deadline for how long to linger around once unreferenced.
     */
    public void lingerDeadlineNs(final long timeNs)
    {
        lingerDeadlineNs = timeNs;
    }

    /**
     * The deadline for how long to linger around once unreferenced.
     *
     * @return the deadline for how long to linger around once unreferenced.
     */
    public long lingerDeadlineNs()
    {
        return lingerDeadlineNs;
    }

    private static void close(
        final FileChannel fileChannel, final UnsafeBuffer logMetaDataBuffer, final MappedByteBuffer[] mappedByteBuffers)
    {
        Throwable error = null;
        try
        {
            CloseHelper.close(fileChannel);
        }
        catch (final Exception ex)
        {
            error = ex;
        }

        if (logMetaDataBuffer != null)
        {
            logMetaDataBuffer.wrap(0, 0);
        }

        if (null != mappedByteBuffers)
        {
            for (int i = 0, length = mappedByteBuffers.length; i < length; i++)
            {
                final MappedByteBuffer mappedByteBuffer = mappedByteBuffers[i];
                mappedByteBuffers[i] = null;
                BufferUtil.free(mappedByteBuffer);
            }
        }

        if (error != null)
        {
            LangUtil.rethrowUnchecked(error);
        }
    }
}
