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
package io.aeron.archive;

import io.aeron.CommonContext;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.mark.MarkFileHeaderDecoder;
import io.aeron.archive.codecs.mark.MarkFileHeaderEncoder;
import io.aeron.archive.codecs.mark.MessageHeaderDecoder;
import io.aeron.archive.codecs.mark.MessageHeaderEncoder;
import io.aeron.archive.codecs.mark.VarAsciiEncodingEncoder;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.MarkFile;
import org.agrona.SemanticVersion;
import org.agrona.SystemUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.PrintStream;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.Archive.Configuration.LIVENESS_TIMEOUT_MS;

/**
 * Used to mark the presence of a running {@link Archive} in a directory to guard it.
 */
public class ArchiveMarkFile implements AutoCloseable
{
    /**
     * Major version for the archive files stored on disk. A change to this requires migration.
     */
    public static final int MAJOR_VERSION = 3;

    /**
     * Minor version for the archive files stored on disk. A change to this indicates new features.
     */
    public static final int MINOR_VERSION = 1;

    /**
     * Patch version for the archive files stored on disk. A change to this indicates feature parity bug fixes.
     */
    public static final int PATCH_VERSION = 0;

    /**
     * Combined semantic version for the stored files.
     *
     * @see SemanticVersion
     */
    public static final int SEMANTIC_VERSION = SemanticVersion.compose(MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION);

    /**
     * Header length for the {@link MarkFile} containing the metadata.
     */
    public static final int HEADER_LENGTH = 8 * 1024;

    /**
     * Name for the archive {@link MarkFile} stored in the {@link Archive.Configuration#ARCHIVE_DIR_PROP_NAME}.
     */
    public static final String FILENAME = "archive-mark.dat";

    /**
     * Name for a file contain a link to the directory containing the {@link MarkFile}.
     */
    public static final String LINK_FILENAME = "archive-mark.lnk";
    private static final UnsafeBuffer EMPTY_BUFFER = new UnsafeBuffer();

    private static final int HEADER_OFFSET = MessageHeaderDecoder.ENCODED_LENGTH;

    private final MarkFileHeaderDecoder headerDecoder = new MarkFileHeaderDecoder();
    private final MarkFileHeaderEncoder headerEncoder = new MarkFileHeaderEncoder();
    private final MarkFile markFile;
    private final UnsafeBuffer buffer;
    private final UnsafeBuffer errorBuffer;

    ArchiveMarkFile(final Archive.Context ctx)
    {
        this(
            new File(ctx.markFileDir(), FILENAME),
            alignedTotalFileLength(ctx),
            ctx.errorBufferLength(),
            ctx.epochClock(),
            LIVENESS_TIMEOUT_MS);

        encode(ctx);
    }

    ArchiveMarkFile(
        final File file,
        final int totalFileLength,
        final int errorBufferLength,
        final EpochClock epochClock,
        final long timeoutMs)
    {
        final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();

        if (file.exists())
        {
            final int currentHeaderOffset = headerOffset(file);
            final MarkFile existingMarkFile = new MarkFile(
                file,
                true,
                currentHeaderOffset + MarkFileHeaderDecoder.versionEncodingOffset(),
                currentHeaderOffset + MarkFileHeaderDecoder.activityTimestampEncodingOffset(),
                totalFileLength,
                timeoutMs,
                epochClock,
                (version) -> validateVersion(file, version),
                null);

            final UnsafeBuffer existingBuffer = existingMarkFile.buffer();

            if (0 != currentHeaderOffset)
            {
                headerDecoder.wrapAndApplyHeader(existingBuffer, 0, messageHeaderDecoder);
            }
            else
            {
                headerDecoder.wrap(
                    existingBuffer, 0, MarkFileHeaderDecoder.BLOCK_LENGTH, MarkFileHeaderDecoder.SCHEMA_VERSION);
            }

            final int existingErrorBufferLength = headerDecoder.errorBufferLength();
            if (existingErrorBufferLength > 0)
            {
                final UnsafeBuffer existingErrorBuffer = new UnsafeBuffer(
                    existingBuffer, headerDecoder.headerLength(), existingErrorBufferLength);

                saveExistingErrors(file, existingErrorBuffer, CommonContext.fallbackLogger());
                existingErrorBuffer.setMemory(0, existingErrorBufferLength, (byte)0);
            }

            if (0 != currentHeaderOffset)
            {
                markFile = existingMarkFile;
                buffer = existingBuffer;
            }
            else
            {
                headerDecoder.wrap(EMPTY_BUFFER, 0, 0, 0);
                CloseHelper.close(existingMarkFile);

                markFile = new MarkFile(
                    file,
                    true,
                    HEADER_OFFSET + MarkFileHeaderDecoder.versionEncodingOffset(),
                    HEADER_OFFSET + MarkFileHeaderDecoder.activityTimestampEncodingOffset(),
                    totalFileLength,
                    timeoutMs,
                    epochClock,
                    (version) -> {},
                    null);
                buffer = markFile.buffer();
                buffer.setMemory(0, buffer.capacity(), (byte)0);
            }
        }
        else
        {
            markFile = new MarkFile(
                file,
                false,
                HEADER_OFFSET + MarkFileHeaderDecoder.versionEncodingOffset(),
                HEADER_OFFSET + MarkFileHeaderDecoder.activityTimestampEncodingOffset(),
                totalFileLength,
                timeoutMs,
                epochClock,
                (version) -> {},
                null);
            buffer = markFile.buffer();
        }

        headerEncoder
            .wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
            .pid(SystemUtil.getPid());

        headerDecoder.wrapAndApplyHeader(buffer, 0, messageHeaderDecoder);

        errorBuffer = new UnsafeBuffer(buffer, HEADER_LENGTH, errorBufferLength);
    }

    /**
     * Construct the {@link MarkFile} based on an existing directory containing a mark file of a running archive.
     *
     * @param directory  containing the archive files.
     * @param filename   for the mark file.
     * @param epochClock to be used for checking liveness.
     * @param timeoutMs  after which the opening will be aborted if no archive starts.
     * @param logger     to detail any discoveries.
     */
    public ArchiveMarkFile(
        final File directory,
        final String filename,
        final EpochClock epochClock,
        final long timeoutMs,
        final Consumer<String> logger)
    {
        this(
            directory,
            filename,
            epochClock,
            timeoutMs,
            (version) -> validateVersion(new File(directory, filename), version),
            logger);
    }

    /**
     * Open an existing {@link MarkFile} or create a new one to be used in migration.
     *
     * @param directory    containing the archive files.
     * @param filename     for the mark file.
     * @param epochClock   to be used for checking liveness.
     * @param timeoutMs    after which the opening will be aborted if no archive starts.
     * @param versionCheck for confirming the correct version.
     * @param logger       to detail any discoveries.
     */
    public ArchiveMarkFile(
        final File directory,
        final String filename,
        final EpochClock epochClock,
        final long timeoutMs,
        final IntConsumer versionCheck,
        final Consumer<String> logger)
    {
        this(openExistingMarkFile(directory, filename, epochClock, timeoutMs, versionCheck, logger));
    }

    ArchiveMarkFile(final MarkFile markFile)
    {
        this.markFile = markFile;

        buffer = markFile.buffer();

        if (0 != headerOffset(buffer))
        {
            headerEncoder.wrap(buffer, HEADER_OFFSET);
            headerDecoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderDecoder());
        }
        else
        {
            headerDecoder.wrap(buffer, 0, MarkFileHeaderDecoder.BLOCK_LENGTH, MarkFileHeaderDecoder.SCHEMA_VERSION);

            // determine the actual sbe schema version used
            final int actingBlockLength = 128;
            final int actingVersion = headerDecoder.headerLength() > 0 ? 1 : 0;
            headerDecoder.wrap(buffer, 0, actingBlockLength, actingVersion);
            headerEncoder.wrap(buffer, 0);
        }

        errorBuffer = headerDecoder.headerLength() > 0 ?
            new UnsafeBuffer(buffer, headerDecoder.headerLength(), headerDecoder.errorBufferLength()) :
            new UnsafeBuffer(buffer, 0, 0);
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        if (!markFile.isClosed())
        {
            headerEncoder.wrap(EMPTY_BUFFER, 0);
            headerDecoder.wrap(EMPTY_BUFFER, 0, 0, 0);
            errorBuffer.wrap(0, 0);
            CloseHelper.close(markFile);
        }
    }

    /**
     * Check if the {@link MarkFile} is closed.
     *
     * @return true if the {@link MarkFile} is closed.
     */
    public boolean isClosed()
    {
        return markFile.isClosed();
    }

    /**
     * Get archive id that is stored in the mark file.
     *
     * @return archive id or {@link io.aeron.Aeron#NULL_VALUE} if mark file does not contain this information.
     * @since 1.47.0
     */
    public long archiveId()
    {
        return markFile.isClosed() ? NULL_VALUE : headerDecoder.archiveId();
    }

    /**
     * Signal the archive has concluded successfully and ready to start.
     *
     * @deprecated Use {@link #signalReady(long)} instead.
     */
    @Deprecated(forRemoval = true)
    public void signalReady()
    {
        if (!markFile.isClosed())
        {
            markFile.signalReady(SEMANTIC_VERSION);
        }
    }

    /**
     * Signal the archive has concluded successfully and ready to start.
     *
     * @param activityTimestamp to be used.
     */
    public void signalReady(final long activityTimestamp)
    {
        if (!markFile.isClosed())
        {
            markFile.timestampRelease(activityTimestamp);
            markFile.signalReady(SEMANTIC_VERSION);
            force();
        }
    }

    /**
     * Signal the archive has terminated cleanly.
     */
    public void signalTerminated()
    {
        signalReady(NULL_VALUE);
    }

    /**
     * Update the activity timestamp as a proof of life.
     *
     * @param nowMs activity timestamp as a proof of life.
     */
    public void updateActivityTimestamp(final long nowMs)
    {
        if (!markFile.isClosed())
        {
            markFile.timestampRelease(nowMs);
        }
    }

    /**
     * Read the activity timestamp of the archive with volatile semantics.
     *
     * @return the activity timestamp of the archive with volatile semantics.
     */
    public long activityTimestampVolatile()
    {
        return markFile.isClosed() ? NULL_VALUE : markFile.timestampVolatile();
    }

    /**
     * The encoder for writing the {@link MarkFile} header.
     *
     * @return the encoder for writing the {@link MarkFile} header.
     */
    public MarkFileHeaderEncoder encoder()
    {
        return headerEncoder;
    }

    /**
     * The decoder for reading the {@link MarkFile} header.
     *
     * @return the decoder for reading the {@link MarkFile} header.
     */
    public MarkFileHeaderDecoder decoder()
    {
        return headerDecoder;
    }

    /**
     * The direct buffer which wraps the region of the {@link MarkFile} which contains the error log.
     *
     * @return the direct buffer which wraps the region of the {@link MarkFile} which contains the error log.
     */
    public AtomicBuffer errorBuffer()
    {
        return errorBuffer;
    }

    /**
     * Save the existing errors from a {@link MarkFile} to a {@link PrintStream} for logging.
     *
     * @param markFile    which contains the error buffer.
     * @param errorBuffer which wraps the error log.
     * @param logger      to which the existing errors will be printed.
     */
    public static void saveExistingErrors(final File markFile, final AtomicBuffer errorBuffer, final PrintStream logger)
    {
        CommonContext.saveExistingErrors(markFile, errorBuffer, logger, "archive");
    }

    /**
     * Determine if the path matches the archive mark file name.
     *
     * @param path       to match.
     * @param attributes ignored, only needed for BiPredicate signature matching.
     * @return true if the filename matches.
     */
    public static boolean isArchiveMarkFile(final Path path, final BasicFileAttributes attributes)
    {
        return FILENAME.equals(path.getFileName().toString());
    }

    /**
     * Get the parent directory containing the mark file.
     *
     * @return parent directory of the mark file.
     * @see MarkFile#parentDirectory()
     */
    public File parentDirectory()
    {
        return markFile.parentDirectory();
    }

    /**
     * Forces any changes made to the mark file's content to be written to the storage device containing the mapped
     * file.
     *
     * @since 1.44.0
     */
    public void force()
    {
        if (!markFile.isClosed())
        {
            markFile.mappedByteBuffer().force();
        }
    }

    private static int alignedTotalFileLength(final Archive.Context ctx)
    {
        final int headerLength =
            HEADER_OFFSET +
            MarkFileHeaderEncoder.BLOCK_LENGTH +
            (4 * VarAsciiEncodingEncoder.lengthEncodingLength()) +
            (null != ctx.controlChannel() ? ctx.controlChannel().length() : 0) +
            ctx.localControlChannel().length() +
            (null != ctx.recordingEventsChannel() ? ctx.recordingEventsChannel().length() : 0) +
            ctx.aeronDirectoryName().length();

        if (headerLength > HEADER_LENGTH)
        {
            throw new ArchiveException(
                "ArchiveMarkFile headerLength=" + headerLength + " > headerLengthCapacity=" + HEADER_LENGTH);
        }

        final int filePageSize = null != ctx.aeron() ? ctx.aeron().context().filePageSize() :
            CommonContext.driverFilePageSize(
                new File(ctx.aeronDirectoryName()), ctx.epochClock(), new CommonContext().driverTimeoutMs());

        LogBufferDescriptor.checkPageSize(filePageSize);

        return BitUtil.align(HEADER_LENGTH + ctx.errorBufferLength(), filePageSize);
    }

    String aeronDirectory()
    {
        headerDecoder.sbeRewind();
        headerDecoder.skipControlChannel();
        headerDecoder.skipLocalControlChannel();
        headerDecoder.skipEventsChannel();
        return headerDecoder.aeronDirectory();
    }

    UnsafeBuffer buffer()
    {
        return buffer;
    }

    private void encode(final Archive.Context ctx)
    {
        headerEncoder
            .startTimestamp(ctx.epochClock().time())
            .controlStreamId(ctx.controlStreamId())
            .localControlStreamId(ctx.localControlStreamId())
            .eventsStreamId(ctx.recordingEventsStreamId())
            .headerLength(HEADER_LENGTH)
            .errorBufferLength(ctx.errorBufferLength())
            .archiveId(ctx.archiveId())
            .controlChannel(ctx.controlChannel())
            .localControlChannel(ctx.localControlChannel())
            .eventsChannel(ctx.recordingEventsChannel())
            .aeronDirectory(ctx.aeronDirectoryName());
    }

    private static void validateVersion(final File markFile, final int version)
    {
        if (SemanticVersion.major(version) != MAJOR_VERSION)
        {
            throw new IllegalArgumentException(
                "mark file (" + markFile.getAbsolutePath() + ") major version " + SemanticVersion.major(version) +
                " does not match software: " + MAJOR_VERSION);
        }
    }

    private static int headerOffset(final File file)
    {
        final MappedByteBuffer mappedByteBuffer = IoUtil.mapExistingFile(file, FILENAME);
        try
        {
            final UnsafeBuffer unsafeBuffer =
                new UnsafeBuffer(mappedByteBuffer, 0, HEADER_OFFSET);
            return headerOffset(unsafeBuffer);
        }
        finally
        {
            IoUtil.unmap(mappedByteBuffer);
        }
    }

    private static int headerOffset(final UnsafeBuffer headerBuffer)
    {
        final MessageHeaderDecoder decoder = new MessageHeaderDecoder();
        decoder.wrap(headerBuffer, 0);
        return MarkFileHeaderDecoder.TEMPLATE_ID == decoder.templateId() &&
            MarkFileHeaderDecoder.SCHEMA_ID == decoder.schemaId() ? HEADER_OFFSET : 0;
    }

    private static MarkFile openExistingMarkFile(
        final File directory,
        final String filename,
        final EpochClock epochClock,
        final long timeoutMs,
        final IntConsumer versionCheck,
        final Consumer<String> logger)
    {
        final int headerOffset = headerOffset(new File(directory, filename));
        return new MarkFile(
            directory,
            filename,
            headerOffset + MarkFileHeaderDecoder.versionEncodingOffset(),
            headerOffset + MarkFileHeaderDecoder.activityTimestampEncodingOffset(),
            timeoutMs,
            epochClock,
            versionCheck,
            logger);
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "ArchiveMarkFile{" +
            "markFile=" + markFile +
            '}';
    }
}
