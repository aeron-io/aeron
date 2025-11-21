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

import io.aeron.exceptions.AeronException;
import org.agrona.BufferUtil;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.*;
import static org.agrona.BitUtil.align;

/**
 * Encapsulates responsibility for mapping the files into memory used by the log partitions.
 */
class MappedRawLog implements RawLog
{
    private static final int ONE_GIG = 1 << 30;
    private static final EnumSet<StandardOpenOption> FILE_OPTIONS = EnumSet.of(CREATE_NEW, READ, WRITE);
    private static final EnumSet<StandardOpenOption> SPARSE_FILE_OPTIONS = EnumSet.of(CREATE_NEW, READ, WRITE, SPARSE);

    private final int termLength;
    private final long logLength;
    private final UnsafeBuffer[] termBuffers = new UnsafeBuffer[PARTITION_COUNT];
    private final UnsafeBuffer logMetaDataBuffer;
    private final ErrorHandler errorHandler;
    private final AtomicCounter mappedBytesCounter;
    private File logFile;
    private MappedByteBuffer[] mappedBuffers;

    MappedRawLog(
        final File location,
        final boolean useSparseFiles,
        final long logLength,
        final int termLength,
        final int filePageSize,
        final ErrorHandler errorHandler,
        final AtomicCounter mappedBytesCounter)
    {
        this.termLength = termLength;
        this.errorHandler = errorHandler;
        this.logFile = location;
        this.logLength = logLength;
        this.mappedBytesCounter = mappedBytesCounter;

        final EnumSet<StandardOpenOption> options = useSparseFiles ? SPARSE_FILE_OPTIONS : FILE_OPTIONS;
        try
        {
            try (FileChannel logChannel = FileChannel.open(logFile.toPath(), options))
            {
                logChannel.truncate(logLength); // set file size like the C driver does
                if (logLength <= Integer.MAX_VALUE)
                {
                    final MappedByteBuffer mappedBuffer = logChannel.map(READ_WRITE, 0, logLength);
                    mappedBuffer.order(ByteOrder.LITTLE_ENDIAN);
                    mappedBuffers = new MappedByteBuffer[]{ mappedBuffer };

                    for (int i = 0; i < PARTITION_COUNT; i++)
                    {
                        termBuffers[i] = new UnsafeBuffer(mappedBuffer, i * termLength, termLength);
                    }

                    logMetaDataBuffer = new UnsafeBuffer(
                        mappedBuffer, (int)(logLength - LOG_META_DATA_LENGTH), LOG_META_DATA_LENGTH);
                }
                else
                {
                    mappedBuffers = new MappedByteBuffer[PARTITION_COUNT + 1];

                    for (int i = 0; i < PARTITION_COUNT; i++)
                    {
                        final MappedByteBuffer buffer = logChannel.map(READ_WRITE, termLength * (long)i, termLength);
                        buffer.order(ByteOrder.LITTLE_ENDIAN);
                        mappedBuffers[i] = buffer;
                        termBuffers[i] = new UnsafeBuffer(buffer, 0, termLength);
                    }

                    final int metaDataMappingLength = align(LOG_META_DATA_LENGTH, filePageSize);
                    final long metaDataSectionOffset = termLength * (long)PARTITION_COUNT;

                    final MappedByteBuffer metaDataMappedBuffer = logChannel.map(
                        READ_WRITE, metaDataSectionOffset, metaDataMappingLength);
                    metaDataMappedBuffer.order(ByteOrder.LITTLE_ENDIAN);

                    mappedBuffers[LOG_META_DATA_SECTION_INDEX] = metaDataMappedBuffer;
                    logMetaDataBuffer = new UnsafeBuffer(
                        metaDataMappedBuffer,
                        metaDataMappingLength - LOG_META_DATA_LENGTH,
                        LOG_META_DATA_LENGTH);
                }

                if (!useSparseFiles)
                {
                    preTouchPages(termBuffers, termLength, filePageSize);
                }

                mappedBytesCounter.getAndAddRelease(logLength);
            }
        }
        catch (final IOException ex)
        {
            IoUtil.delete(logFile, true);
            throw new UncheckedIOException(ex);
        }
    }

    public int termLength()
    {
        return termLength;
    }

    public boolean free()
    {
        final MappedByteBuffer[] mappedBuffers = this.mappedBuffers;
        if (null != mappedBuffers)
        {
            this.mappedBuffers = null;
            for (int i = 0; i < mappedBuffers.length; i++)
            {
                BufferUtil.free(mappedBuffers[i]);
            }

            mappedBytesCounter.getAndAddRelease(-logLength);

            logMetaDataBuffer.wrap(0, 0);
            for (int i = 0; i < termBuffers.length; i++)
            {
                termBuffers[i].wrap(0, 0);
            }
        }

        if (null != logFile)
        {
            if (!logFile.delete() && logFile.exists())
            {
                return false;
            }

            logFile = null;
        }

        return true;
    }

    public void close()
    {
        if (!free())
        {
            errorHandler.onError(new AeronException("unable to delete " + logFile, AeronException.Category.WARN));
        }
    }

    public UnsafeBuffer[] termBuffers()
    {
        return termBuffers;
    }

    public UnsafeBuffer metaData()
    {
        return logMetaDataBuffer;
    }

    public ByteBuffer[] sliceTerms()
    {
        final ByteBuffer[] terms = new ByteBuffer[PARTITION_COUNT];

        if (termLength < ONE_GIG)
        {
            final MappedByteBuffer buffer = mappedBuffers[0];
            for (int i = 0; i < PARTITION_COUNT; i++)
            {
                buffer.limit((termLength * i) + termLength).position(termLength * i);
                terms[i] = buffer.slice();
            }
        }
        else
        {
            for (int i = 0; i < PARTITION_COUNT; i++)
            {
                terms[i] = mappedBuffers[i].duplicate();
            }
        }

        return terms;
    }

    public String fileName()
    {
        return logFile.getAbsolutePath();
    }

    private static void preTouchPages(final UnsafeBuffer[] buffers, final int length, final int pageSize)
    {
        for (final UnsafeBuffer buffer : buffers)
        {
            for (long i = 0; i < length; i += pageSize)
            {
                buffer.putByte((int)i, (byte)0);
            }
        }
    }
}
