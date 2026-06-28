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

import io.aeron.Aeron;
import io.aeron.CncFileDescriptor;
import io.aeron.CommonContext;
import io.aeron.archive.checksum.Checksum;
import io.aeron.archive.codecs.*;
import io.aeron.driver.Configuration;
import io.aeron.exceptions.AeronException;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import org.agrona.*;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.MutableBoolean;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.Stream;

import static io.aeron.archive.Archive.Configuration.CATALOG_FILE_NAME;
import static io.aeron.archive.Archive.Configuration.FILE_IO_MAX_LENGTH_DEFAULT;
import static io.aeron.archive.ArchiveTool.VerifyOption.APPLY_CHECKSUM;
import static io.aeron.archive.ArchiveTool.VerifyOption.VERIFY_ALL_SEGMENT_FILES;
import static io.aeron.archive.Catalog.*;
import static io.aeron.archive.MigrationUtils.fullVersionString;
import static io.aeron.archive.ReplaySession.isInvalidHeader;
import static io.aeron.archive.checksum.Checksums.newInstance;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.segmentFileBasePosition;
import static io.aeron.archive.codecs.RecordingState.*;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.logbuffer.LogBufferDescriptor.computeTermIdFromPosition;
import static io.aeron.logbuffer.LogBufferDescriptor.positionBitsToShift;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_DATA;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;
import static java.lang.Math.min;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.file.StandardOpenOption.*;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toMap;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.BitUtil.align;
import static org.agrona.BufferUtil.NATIVE_BYTE_ORDER;
import static org.agrona.concurrent.SystemEpochClock.INSTANCE;

/**
 * Tool for inspecting and performing administrative tasks on an {@link Archive} and its contents which is described
 * in a {@link Catalog}.
 */
public final class ArchiveTool
{
    /**
     * Allows user to confirm or reject an action.
     *
     * @param <T> type of the context data.
     */
    @FunctionalInterface
    public interface ActionConfirmation<T>
    {
        /**
         * Confirm or reject the action.
         *
         * @param context context data.
         * @return {@code true} confirms the action and {@code false} to reject the action.
         */
        boolean confirm(T context);
    }

    private ArchiveTool()
    {
    }

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    @SuppressWarnings("MethodLength")
    public static void main(final String[] args)
    {
        if (args.length == 0 || args.length > 6)
        {
            printHelp();
            System.exit(-1);
        }

        final File archiveDir = new File(args[0]);
        if (!archiveDir.exists())
        {
            System.err.println("ERR: Archive folder not found: " + archiveDir.getAbsolutePath());
            printHelp();
            System.exit(-1);
        }

        final PrintStream out = System.out;
        if (args.length > 1 && "describe-all".equals(args[1]))
        {
            describeAll(out, archiveDir);
        }
        else if (args.length == 2 && "describe".equals(args[1]))
        {
            describe(out, archiveDir);
        }
        else if (args.length == 3 && "describe".equals(args[1]))
        {
            describeRecording(out, archiveDir, Long.parseLong(args[2]));
        }
        else if (args.length >= 2 && "dump".equals(args[1]))
        {
            dump(
                out,
                archiveDir,
                args.length >= 3 ? Long.parseLong(args[2]) : Long.MAX_VALUE,
                ArchiveTool::continueOnFrameLimit);
        }
        else if (args.length == 2 && "errors".equals(args[1]))
        {
            printErrors(out, archiveDir);
        }
        else if (args.length == 2 && "pid".equals(args[1]))
        {
            out.println(pid(archiveDir));
        }
        else if (args.length >= 2 && "verify".equals(args[1]))
        {
            final boolean hasErrors;

            if (args.length == 2)
            {
                hasErrors = !verify(
                    out,
                    archiveDir,
                    emptySet(),
                    null,
                    ArchiveTool::truncateOnPageStraddle);
            }
            else if (args.length == 3)
            {
                if (VERIFY_ALL_SEGMENT_FILES == VerifyOption.byFlag(args[2]))
                {
                    hasErrors = !verify(
                        out,
                        archiveDir,
                        EnumSet.of(VERIFY_ALL_SEGMENT_FILES),
                        null,
                        ArchiveTool::truncateOnPageStraddle);
                }
                else
                {
                    hasErrors = !verifyRecording(
                        out,
                        archiveDir,
                        Long.parseLong(args[2]),
                        emptySet(),
                        null,
                        ArchiveTool::truncateOnPageStraddle);
                }
            }
            else if (args.length == 4)
            {
                if (APPLY_CHECKSUM == VerifyOption.byFlag(args[2]))
                {
                    hasErrors = !verify(
                        out,
                        archiveDir,
                        EnumSet.of(APPLY_CHECKSUM),
                        validateChecksumClass(args[3]),
                        ArchiveTool::truncateOnPageStraddle);
                }
                else
                {
                    hasErrors = !verifyRecording(
                        out,
                        archiveDir,
                        Long.parseLong(args[2]),
                        EnumSet.of(VERIFY_ALL_SEGMENT_FILES),
                        null,
                        ArchiveTool::truncateOnPageStraddle);
                }
            }
            else if (args.length == 5)
            {
                if (VERIFY_ALL_SEGMENT_FILES == VerifyOption.byFlag(args[2]))
                {
                    hasErrors = !verify(
                        out,
                        archiveDir,
                        EnumSet.allOf(VerifyOption.class),
                        validateChecksumClass(args[4]),
                        ArchiveTool::truncateOnPageStraddle);
                }
                else
                {
                    hasErrors = !verifyRecording(
                        out,
                        archiveDir,
                        Long.parseLong(args[2]),
                        EnumSet.of(APPLY_CHECKSUM),
                        validateChecksumClass(args[4]),
                        ArchiveTool::truncateOnPageStraddle);
                }
            }
            else
            {
                hasErrors = !verifyRecording(
                    out,
                    archiveDir,
                    Long.parseLong(args[2]),
                    EnumSet.allOf(VerifyOption.class),
                    validateChecksumClass(args[5]),
                    ArchiveTool::truncateOnPageStraddle);
            }

            if (hasErrors)
            {
                System.exit(-1);
            }
        }
        else if (args.length >= 3 && "checksum".equals(args[1]))
        {
            if (args.length == 3)
            {
                checksum(out, archiveDir, false, args[2]);
            }
            else
            {
                if ("-a".equals(args[3]))
                {
                    checksum(out, archiveDir, true, args[2]);
                }
                else
                {
                    checksumRecording(
                        out,
                        archiveDir,
                        Long.parseLong(args[3]),
                        args.length > 4 && "-a".equals(args[4]),
                        args[2]);
                }
            }
        }
        else if (args.length == 2 && "count-entries".equals(args[1]))
        {
            out.println(entryCount(archiveDir));
        }
        else if (args.length == 2 && "max-entries".equals(args[1]))
        {
            out.println("max-entries is deprecated please use capacity.");
        }
        else if (args.length == 3 && "max-entries".equals(args[1]))
        {
            out.println("max-entries is deprecated please use capacity.");
        }
        else if (args.length == 2 && "migrate".equals(args[1]))
        {
            out.print("WARNING: please ensure archive is not running and that a backup has been taken of " +
                "the archive directory before attempting migration(s).");

            if (readContinueAnswer("Continue? (y/n)"))
            {
                migrate(out, archiveDir);
            }
        }
        else if (args.length == 2 && "compact".equals(args[1]))
        {
            out.print("WARNING: Compacting the Catalog is non-recoverable operation! All non-ACTIVE recordings" +
                " will be deleted along with the corresponding segment files.");

            if (readContinueAnswer("Continue? (y/n)"))
            {
                compact(out, archiveDir);
            }
        }
        else if (args.length >= 2 && "delete-orphaned-segments".equals(args[1]))
        {
            if (args.length == 2)
            {
                out.print("WARNING: All orphaned segment files will be deleted.");
                if (readContinueAnswer("Continue? (y/n)"))
                {
                    deleteOrphanedSegments(out, archiveDir);
                }
            }
            else
            {
                final long recordingId = Long.parseLong(args[2]);
                out.print("WARNING: All orphaned segment files owned by the RecordingId[");
                out.print(recordingId);
                out.print("] will be deleted.");
                if (readContinueAnswer("Continue? (y/n)"))
                {
                    deleteOrphanedSegments(out, archiveDir, recordingId);
                }
            }
        }
        else if (args.length == 3 && "mark-valid".equals(args[1]))
        {
            markRecordingValid(
                out,
                archiveDir,
                Long.parseLong(args[2]));
        }
        else if (args.length == 3 && "mark-invalid".equals(args[1]))
        {
            markRecordingInvalid(
                out,
                archiveDir,
                Long.parseLong(args[2]));
        }
        else
        {
            System.err.println("ERR: Invalid command");
            printHelp();
            System.exit(-1);
        }
    }

    /**
     * Get the maximum number of entries supported by a {@link Catalog}.
     *
     * @param archiveDir containing the {@link Catalog}.
     * @return the maximum number of entries supported by a {@link Catalog}.
     * @see #capacity(File)
     * @deprecated Use {@link #capacity(File)} instead.
     */
    @Deprecated
    public static int maxEntries(final File archiveDir)
    {
        try (Catalog catalog = openCatalogReadOnly(archiveDir, INSTANCE))
        {
            return (int)(catalog.capacity() / DEFAULT_RECORD_LENGTH);
        }
    }

    /**
     * Set the maximum number of entries supported by a {@link Catalog}.
     *
     * @param archiveDir    containing the {@link Catalog}.
     * @param newMaxEntries value to set.
     * @return the maximum number of entries supported by a {@link Catalog} after update.
     * @see #capacity(File, long)
     * @deprecated Use {@link #capacity(File, long)} instead.
     */
    @Deprecated
    public static int maxEntries(final File archiveDir, final long newMaxEntries)
    {
        final long newCapacity = capacity(archiveDir, newMaxEntries * DEFAULT_RECORD_LENGTH);
        return (int)(newCapacity / DEFAULT_RECORD_LENGTH);
    }

    /**
     * Get the capacity in bytes of the {@link Catalog}.
     *
     * @param archiveDir containing the {@link Catalog}.
     * @return capacity in bytes of the {@link Catalog}, i.e. size of the {@link Catalog} file.
     */
    public static long capacity(final File archiveDir)
    {
        try (Catalog catalog = openCatalogReadOnly(archiveDir, INSTANCE))
        {
            return catalog.capacity();
        }
    }

    /**
     * Set the capacity in bytes of the {@link Catalog}. If new capacity is smaller than current {@link Catalog}
     * capacity then this method is a no op.
     *
     * @param archiveDir  containing the {@link Catalog}.
     * @param newCapacity value to set.
     * @return the capacity of the {@link Catalog} after update.
     */
    public static long capacity(final File archiveDir, final long newCapacity)
    {
        try (Catalog catalog = openCatalogReadWrite(archiveDir, INSTANCE, newCapacity, null, null))
        {
            return catalog.capacity();
        }
    }

    /**
     * Describe the metadata for all entries in the {@link Catalog}.
     * This will include entries that have been invalidated.
     *
     * @param out        to which the entries will be printed.
     * @param archiveDir containing the {@link Catalog}.
     */
    public static void describeAll(final PrintStream out, final File archiveDir)
    {
        try (Catalog catalog = openCatalogReadOnly(archiveDir, INSTANCE);
            ArchiveMarkFile markFile = openMarkFile(archiveDir, out::println))
        {
            printMarkInformation(markFile, out);
            out.println("Catalog capacity in bytes: " + catalog.capacity());
            catalog.forEach((recordingDescriptorOffset, he, hd, e, d) -> out.println(d + "|" + hd.state()));
        }
    }

    /**
     * Describe the metadata for all valid entries in the {@link Catalog}.
     * This will not include entries that have been invalidated.
     *
     * @param out        to which the entries will be printed.
     * @param archiveDir containing the {@link Catalog}.
     */
    public static void describe(final PrintStream out, final File archiveDir)
    {
        try (Catalog catalog = openCatalogReadOnly(archiveDir, INSTANCE);
            ArchiveMarkFile markFile = openMarkFile(archiveDir, out::println))
        {
            printMarkInformation(markFile, out);
            out.println("Catalog capacity in bytes: " + catalog.capacity());
            catalog.forEach((recordingDescriptorOffset, he, hd, e, d) ->
            {
                if (hd.state() == VALID)
                {
                    out.println(d);
                }
            });
        }
    }

    /**
     * Describe the metadata for an entry in the {@link Catalog} identified by recording id.
     *
     * @param out         to which the entry will be printed.
     * @param archiveDir  containing the {@link Catalog}.
     * @param recordingId to identify the entry.
     */
    public static void describeRecording(final PrintStream out, final File archiveDir, final long recordingId)
    {
        try (Catalog catalog = openCatalogReadWrite(archiveDir, INSTANCE, MIN_CAPACITY, null, null))
        {
            final MutableBoolean found = new MutableBoolean(false);
            catalog.forEach((recordingDescriptorOffset, headerEnc, headerDec, encoder, decoder) ->
            {
                if (decoder.recordingId() == recordingId)
                {
                    found.set(true);
                    out.println(decoder);
                }
            });

            if (!found.get())
            {
                throw new AeronException("no recording found with recordingId: " + recordingId);
            }
        }
    }

    /**
     * Count of the number of entries in the {@link Catalog}.
     *
     * @param archiveDir containing the {@link Catalog}.
     * @return the number of entries in the {@link Catalog}.
     */
    public static int entryCount(final File archiveDir)
    {
        try (Catalog catalog = openCatalogReadOnly(archiveDir, INSTANCE))
        {
            return catalog.entryCount();
        }
    }

    /**
     * Get the pid of the process for the {@link Archive}.
     *
     * @param archiveDir containing the {@link org.agrona.MarkFile}.
     * @return the pid of the process for the {@link Archive}.
     */
    public static long pid(final File archiveDir)
    {
        try (ArchiveMarkFile markFile = openMarkFile(archiveDir, null))
        {
            return markFile.decoder().pid();
        }
    }

    /**
     * Print the errors in the {@link org.agrona.MarkFile}.
     *
     * @param out        stream to which the errors will be printed.
     * @param archiveDir containing the {@link org.agrona.MarkFile}.
     */
    public static void printErrors(final PrintStream out, final File archiveDir)
    {
        try (ArchiveMarkFile markFile = openMarkFile(archiveDir, null))
        {
            printErrors(out, markFile);
        }
    }

    /**
     * Dump the contents of an {@link Archive} so it can be inspected or debugged. Each recording will have its
     * contents dumped up to fragmentCountLimit before requesting to continue.
     *
     * @param out                               to which the contents will be printed.
     * @param archiveDir                        containing the {@link Archive}.
     * @param fragmentCountLimit                limit of data fragments to print from each recording before continue.
     * @param confirmActionOnFragmentCountLimit confirm continue to dump at limit.
     */
    public static void dump(
        final PrintStream out,
        final File archiveDir,
        final long fragmentCountLimit,
        final ActionConfirmation<Long> confirmActionOnFragmentCountLimit)
    {
        if (fragmentCountLimit <= 0)
        {
            throw new IllegalArgumentException("Invalid fragmentCountLimit=" + fragmentCountLimit);
        }

        try (Catalog catalog = openCatalogReadOnly(archiveDir, INSTANCE);
            ArchiveMarkFile markFile = openMarkFile(archiveDir, out::println))
        {
            printMarkInformation(markFile, out);
            out.println("Catalog capacity in bytes: " + catalog.capacity());

            out.println();
            out.println("Dumping up to " + fragmentCountLimit + " fragments per recording");

            final SimpleFragmentHandler fragmentHandler = (buffer, offset, length, frameType, flags, reservedValue) ->
            {
                out.println("data at offset [" + offset + "] with length = " + length);
                if (HDR_TYPE_PAD == frameType)
                {
                    out.println("PADDING FRAME");
                }
                else if (HDR_TYPE_DATA == frameType)
                {
                    if ((flags & UNFRAGMENTED) != UNFRAGMENTED)
                    {
                        String suffix = (flags & BEGIN_FRAG_FLAG) == BEGIN_FRAG_FLAG ? "BEGIN_FRAGMENT" : "";
                        suffix += (flags & END_FRAG_FLAG) == END_FRAG_FLAG ? "END_FRAGMENT" : "";
                        out.println("Fragmented frame. " + suffix);
                    }
                    out.println(PrintBufferUtil.prettyHexDump(buffer, offset, length));
                }
                else
                {
                    out.println("Unexpected frame type " + frameType);
                }
            };

            catalog.forEach(
                (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) -> dump(
                out,
                archiveDir,
                catalog,
                fragmentCountLimit,
                confirmActionOnFragmentCountLimit,
                headerDecoder,
                descriptorDecoder,
                fragmentHandler));
        }
    }

    /**
     * Set of options for {@link #verify(PrintStream, File, Set, String, ActionConfirmation)} and
     * {@link #verifyRecording(PrintStream, File, long, Set, String, ActionConfirmation)} methods.
     */
    public enum VerifyOption
    {
        /**
         * Enables verification for all segment files of a given recording.
         * By default, only last segment file is verify.
         */
        VERIFY_ALL_SEGMENT_FILES("-a"),

        /**
         * Perform checksum for each data frame within a segment file being verified.
         */
        APPLY_CHECKSUM("-checksum");

        private final String flag;

        VerifyOption(final String flag)
        {
            this.flag = flag;
        }

        private static final Map<String, VerifyOption> BY_FLAG = Stream.of(values())
            .collect(toMap((opt) -> opt.flag, (opt) -> opt));

        /**
         * Lookup the {@link VerifyOption} by string format of the command line flag.
         *
         * @param flag command line option.
         * @return the {@link VerifyOption} which matches the flag or null if not found.
         */
        public static VerifyOption byFlag(final String flag)
        {
            return BY_FLAG.get(flag);
        }
    }

    /**
     * Verify descriptors in the catalog, checking recording files availability and contents.
     * <p>
     * Faulty entries are marked as unusable.
     *
     * @param out                    stream to print results and errors to.
     * @param archiveDir             that contains {@link org.agrona.MarkFile}, {@link Catalog}, and recordings.
     * @param options                set of options that control verification behavior.
     * @param truncateOnPageStraddle action to perform if last fragment in the max segment file straddles the page
     *                               boundary, i.e. if {@code true} the file will be truncated (last fragment
     *                               will be deleted), if {@code false} the fragment is considered complete.
     * @param checksumClassName      (optional) fully qualified class name of the {@link Checksum} implementation.
     * @return {@code true} if no errors have been encountered, {@code false} otherwise (faulty entries are marked
     * as unusable if {@code false} is returned)
     */
    public static boolean verify(
        final PrintStream out,
        final File archiveDir,
        final Set<VerifyOption> options,
        final String checksumClassName,
        final ActionConfirmation<File> truncateOnPageStraddle)
    {
        final Checksum checksum = createChecksum(options, checksumClassName);
        return verify(out, archiveDir, options, checksum, INSTANCE, truncateOnPageStraddle);
    }

    /**
     * Verify descriptor in the catalog according to recordingId, checking recording files availability and contents.
     * <p>
     * Faulty entries are marked as unusable.
     *
     * @param out                    stream to print results and errors to.
     * @param archiveDir             that contains {@link org.agrona.MarkFile}, {@link Catalog}, and recordings.
     * @param recordingId            to verify.
     * @param options                set of options that control verification behavior.
     * @param truncateOnPageStraddle action to perform if last fragment in the max segment file straddles the page
     *                               boundary, i.e. if {@code true} the file will be truncated (last fragment
     *                               will be deleted), if {@code false} the fragment if considered complete.
     * @param checksumClassName      (optional) fully qualified class name of the {@link Checksum} implementation.
     * @return {@code true} if no errors have been encountered, {@code false} otherwise (the recording is marked
     * as unusable if {@code false} is returned)
     * @throws AeronException if there is no recording with {@code recordingId} in the archive
     **/
    public static boolean verifyRecording(
        final PrintStream out,
        final File archiveDir,
        final long recordingId,
        final Set<VerifyOption> options,
        final String checksumClassName,
        final ActionConfirmation<File> truncateOnPageStraddle)
    {
        return verifyRecording(
            out,
            archiveDir,
            recordingId,
            options,
            createChecksum(options, checksumClassName),
            INSTANCE,
            truncateOnPageStraddle);
    }

    /**
     * Compute and persist CRC-32(c) checksums for every fragment of a segment file for all recordings in the catalog.
     *
     * @param out               stream to print results and errors to.
     * @param archiveDir        that contains {@link org.agrona.MarkFile}, {@link Catalog}, and recordings.
     * @param allFiles          should compute checksums for all segment file or only for the last one.
     * @param checksumClassName fully qualified class name of the {@link Checksum} implementation.
     */
    public static void checksum(
        final PrintStream out, final File archiveDir, final boolean allFiles, final String checksumClassName)
    {
        checksum(out, archiveDir, allFiles, newInstance(validateChecksumClass(checksumClassName)), INSTANCE);
    }

    /**
     * Compute and persist CRC-32(c) checksums for every fragment of a segment file(s) for a given recording.
     *
     * @param out               stream to print results and errors to.
     * @param archiveDir        that contains {@link org.agrona.MarkFile}, {@link Catalog}, and recordings.
     * @param recordingId       to compute checksums for.
     * @param allFiles          should compute checksums for all segment file or only for the last one.
     * @param checksumClassName fully qualified class name of the {@link Checksum} implementation.
     * @throws AeronException if there is no recording with {@code recordingId} in the archive
     */
    public static void checksumRecording(
        final PrintStream out,
        final File archiveDir,
        final long recordingId,
        final boolean allFiles,
        final String checksumClassName)
    {
        checksumRecording(
            out, archiveDir, recordingId, allFiles, newInstance(validateChecksumClass(checksumClassName)), INSTANCE);
    }

    /**
     * Mark a recording valid by marking it as {@link RecordingState#VALID} in the catalog.
     *
     * @param out         stream to print results and errors to.
     * @param archiveDir  that contains {@link org.agrona.MarkFile}, {@link Catalog}, and recordings.
     * @param recordingId to revive
     * @throws AeronException if there is no recording with {@code recordingId} in the archive
     */
    public static void markRecordingValid(final PrintStream out, final File archiveDir, final long recordingId)
    {
        changeRecordingState(out, archiveDir, recordingId, INVALID, VALID);
    }

    /**
     * Mark a recording invalid by marking it as {@link RecordingState#INVALID} in the catalog.
     *
     * @param out         stream to print results and errors to.
     * @param archiveDir  that contains {@link org.agrona.MarkFile}, {@link Catalog}, and recordings.
     * @param recordingId to invalidate
     * @throws AeronException if there is no recording with {@code recordingId} in the archive
     */
    public static void markRecordingInvalid(final PrintStream out, final File archiveDir, final long recordingId)
    {
        changeRecordingState(out, archiveDir, recordingId, VALID, INVALID);
    }

    /**
     * Migrate previous archive {@link org.agrona.MarkFile}, {@link Catalog}, and recordings from previous version
     * to the latest version.
     *
     * @param out        output stream to print results and errors to.
     * @param archiveDir that contains MarkFile, Catalog and recordings.
     */
    public static void migrate(final PrintStream out, final File archiveDir)
    {
        final EpochClock epochClock = INSTANCE;
        try
        {
            final int markFileVersion;
            final IntConsumer noVersionCheck = (version) -> {};
            try (ArchiveMarkFile markFile = openMarkFileReadWrite(archiveDir, epochClock);
                Catalog catalog = openCatalogReadWrite(archiveDir, epochClock, MIN_CAPACITY, null, noVersionCheck))
            {
                markFileVersion = markFile.decoder().version();
                out.println("MarkFile version=" + fullVersionString(markFileVersion));
                out.println("Catalog version=" + fullVersionString(catalog.version()));
                out.println("Latest version=" + fullVersionString(ArchiveMarkFile.SEMANTIC_VERSION));
            }

            final List<ArchiveMigrationStep> steps = ArchiveMigrationPlanner.createPlan(markFileVersion);
            for (final ArchiveMigrationStep step : steps)
            {
                try (ArchiveMarkFile markFile = openMarkFileReadWrite(archiveDir, epochClock);
                    Catalog catalog = openCatalogReadWrite(archiveDir, epochClock, MIN_CAPACITY, null, noVersionCheck))
                {
                    out.println("Migration step " + step.toString());
                    step.migrate(out, markFile, catalog, archiveDir);
                }
            }
        }
        catch (final Exception ex)
        {
            ex.printStackTrace(out);
        }
    }

    /**
     * Compact Catalog file by removing all recordings in state {@link io.aeron.archive.codecs.RecordingState#INVALID}
     * and delete the corresponding segment files.
     *
     * @param out        stream to print results and errors to.
     * @param archiveDir that contains {@link MarkFile}, {@link Catalog}, and recordings.
     */
    public static void compact(final PrintStream out, final File archiveDir)
    {
        compact(out, archiveDir, INSTANCE);
    }

    /**
     * Delete orphaned recording segments that have been detached for all recordings, i.e. outside the start and stop
     * recording range, but are not deleted.
     *
     * @param out        stream to print results and errors to.
     * @param archiveDir that contains {@link MarkFile}, {@link Catalog}, and recordings.
     */
    public static void deleteOrphanedSegments(final PrintStream out, final File archiveDir)
    {
        deleteOrphanedSegments(out, archiveDir, INSTANCE, NULL_RECORD_ID);
    }

    /**
     * Delete orphaned recording segments that have been detached, i.e. outside the start and stop recording range,
     * but are not deleted.
     *
     * @param out               stream to print results and errors to.
     * @param archiveDir        that contains {@link MarkFile}, {@link Catalog}, and recordings.
     * @param targetRecordingId optional recordingId to delete orphaned segments for a specific recording.
     *                          If {@link Aeron#NULL_VALUE}, delete orphaned segments for all recordings.
     */
    public static void deleteOrphanedSegments(
        final PrintStream out,
        final File archiveDir,
        final long targetRecordingId)
    {
        deleteOrphanedSegments(out, archiveDir, INSTANCE, targetRecordingId);
    }

    static void deleteOrphanedSegments(
        final PrintStream out,
        final File archiveDir,
        final EpochClock epochClock,
        final long targetRecordingId)
    {
        try (Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
        {
            final Long2ObjectHashMap<List<String>> segmentFilesByRecordingId = indexSegmentFiles(archiveDir);

            final MutableBoolean found = new MutableBoolean(false);
            catalog.forEach((recordingDescriptorOffset,
                headerEncoder,
                headerDecoder,
                descriptorEncoder,
                descriptorDecoder) ->
            {
                final long recordingId = descriptorDecoder.recordingId();
                if (NULL_RECORD_ID == targetRecordingId || targetRecordingId == recordingId)
                {
                    found.set(true);
                    final List<String> files = segmentFilesByRecordingId.getOrDefault(recordingId, emptyList());
                    deleteOrphanedSegmentFiles(out, archiveDir, descriptorDecoder, files);
                }
            });

            if (NULL_RECORD_ID != targetRecordingId && !found.get())
            {
                throw new AeronException("no recording found with recordingId: " + targetRecordingId);
            }
        }
    }

    static void compact(final PrintStream out, final File archiveDir, final EpochClock epochClock)
    {
        final File compactFile = new File(archiveDir, CATALOG_FILE_NAME + ".compact");
        try
        {
            final MutableInteger offset = new MutableInteger(CatalogHeaderEncoder.BLOCK_LENGTH);
            final MutableInteger deletedRecords = new MutableInteger();
            final MutableInteger reclaimedBytes = new MutableInteger();

            final Path compactFilePath = compactFile.toPath();
            try (FileChannel channel = FileChannel.open(compactFilePath, READ, WRITE, CREATE_NEW);
                Catalog catalog = openCatalogReadOnly(archiveDir, epochClock))
            {
                final MappedByteBuffer mappedByteBuffer = channel.map(READ_WRITE, 0, catalog.capacity());
                mappedByteBuffer.order(CatalogHeaderEncoder.BYTE_ORDER);
                try
                {
                    final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(mappedByteBuffer);

                    new CatalogHeaderEncoder()
                        .wrap(unsafeBuffer, 0)
                        .version(catalog.version())
                        .length(CatalogHeaderEncoder.BLOCK_LENGTH)
                        .nextRecordingId(catalog.nextRecordingId())
                        .alignment(catalog.alignment());

                    final Long2ObjectHashMap<List<String>> segmentFilesByRecordingId = indexSegmentFiles(archiveDir);

                    catalog.forEach(
                        (recordingDescriptorOffset,
                        headerEncoder,
                        headerDecoder,
                        descriptorEncoder,
                        descriptorDecoder) ->
                        {
                            final int frameLength = headerDecoder.encodedLength() + headerDecoder.length();
                            if (VALID != headerDecoder.state())
                            {
                                deletedRecords.increment();
                                reclaimedBytes.addAndGet(frameLength);

                                final List<String> segmentFiles = segmentFilesByRecordingId.getOrDefault(
                                    descriptorDecoder.recordingId(), emptyList());

                                for (final String segmentFile : segmentFiles)
                                {
                                    IoUtil.deleteIfExists(new File(archiveDir, segmentFile));
                                }
                            }
                            else
                            {
                                final int index = offset.getAndAdd(frameLength);
                                unsafeBuffer.putBytes(index, headerDecoder.buffer(), 0, frameLength);
                            }
                        });
                }
                finally
                {
                    BufferUtil.free(mappedByteBuffer);
                }

                channel.truncate(offset.get()); // trim to size
            }

            out.println("Compaction result: deleted " + deletedRecords.get() + " records and reclaimed " +
                reclaimedBytes.get() + " bytes");

            final Path catalogFilePath = compactFilePath.resolveSibling(CATALOG_FILE_NAME);
            Files.delete(catalogFilePath);
            Files.move(compactFilePath, catalogFilePath);
        }
        catch (final IOException ex)
        {
            ex.printStackTrace(out);
        }
        finally
        {
            IoUtil.deleteIfExists(compactFile);
        }
    }

    static boolean verify(
        final PrintStream out,
        final File archiveDir,
        final Set<VerifyOption> options,
        final Checksum checksum,
        final EpochClock epochClock,
        final ActionConfirmation<File> truncateOnPageStraddle)
    {
        try (Catalog catalog = openCatalogReadWrite(archiveDir, epochClock, MIN_CAPACITY, checksum, null))
        {
            final MutableInteger errorCount = new MutableInteger();
            catalog.forEach(createVerifyEntryProcessor(
                out, archiveDir, options, catalog, checksum, epochClock, errorCount, truncateOnPageStraddle));

            return errorCount.get() == 0;
        }
    }

    static boolean verifyRecording(
        final PrintStream out,
        final File archiveDir,
        final long recordingId,
        final Set<VerifyOption> options,
        final Checksum checksum,
        final EpochClock epochClock,
        final ActionConfirmation<File> truncateOnPageStraddle)
    {
        try (Catalog catalog = openCatalogReadWrite(archiveDir, epochClock, MIN_CAPACITY, checksum, null))
        {
            final MutableBoolean foundRecording = new MutableBoolean();
            final MutableInteger errorCount = new MutableInteger();
            final CatalogEntryProcessor delegate = createVerifyEntryProcessor(
                out, archiveDir, options, catalog, checksum, epochClock, errorCount, truncateOnPageStraddle);
            catalog.forEach((recordingDescriptorOffset, he, hd, encoder, decoder) ->
            {
                if (decoder.recordingId() == recordingId)
                {
                    foundRecording.set(true);
                    delegate.accept(recordingDescriptorOffset, he, hd, encoder, decoder);
                }
            });

            if (!foundRecording.get())
            {
                throw new AeronException("no recording found with recordingId: " + recordingId);
            }

            return errorCount.get() == 0;
        }
    }

    static Catalog openCatalogReadOnly(final File archiveDir, final EpochClock epochClock)
    {
        return new Catalog(archiveDir, epochClock);
    }

    static Catalog openCatalogReadWrite(
        final File archiveDir,
        final EpochClock epochClock,
        final long capacity,
        final Checksum checksum,
        final IntConsumer versionCheck)
    {
        return new Catalog(archiveDir, epochClock, capacity, true, checksum, versionCheck);
    }

    private static void changeRecordingState(
        final PrintStream out,
        final File archiveDir,
        final long recordingId,
        final RecordingState expectedState,
        final RecordingState targetState)
    {
        try (Catalog catalog = openCatalogReadWrite(archiveDir, INSTANCE, MIN_CAPACITY, null, null))
        {
            final MutableBoolean found = new MutableBoolean(false);
            catalog.forEach((recordingDescriptorOffset, he, hDecoder, descriptorEncoder, descriptorDecoder) ->
            {
                if (descriptorDecoder.recordingId() == recordingId)
                {
                    found.set(true);
                    final RecordingState currentState = hDecoder.state();
                    if (targetState != currentState)
                    {
                        if (expectedState != currentState)
                        {
                            throw new AeronException("(recordingId=" + recordingId + ") state transition " +
                                currentState + " -> " + targetState + " is not allowed");
                        }
                        he.state(targetState);
                        out.println("(recordingId=" + recordingId + ") changed state to " + targetState);
                    }
                }
            });

            if (!found.get())
            {
                throw new AeronException("no recording found with recordingId: " + recordingId);
            }
        }
    }

    private static String validateChecksumClass(final String checksumClassName)
    {
        final String className = null == checksumClassName ? null : checksumClassName.trim();
        if (Strings.isEmpty(className))
        {
            throw new IllegalArgumentException("Checksum class name must be specified!");
        }

        return className;
    }

    private static Checksum createChecksum(final Set<VerifyOption> options, final String checksumClassName)
    {
        if (null == checksumClassName)
        {
            if (options.contains(APPLY_CHECKSUM))
            {
                throw new IllegalArgumentException(
                    "Checksum class name is required when " + APPLY_CHECKSUM + " option is specified!");
            }

            return null;
        }

        return newInstance(checksumClassName);
    }

    private static CatalogEntryProcessor createVerifyEntryProcessor(
        final PrintStream out,
        final File archiveDir,
        final Set<VerifyOption> options,
        final Catalog catalog,
        final Checksum checksum,
        final EpochClock epochClock,
        final MutableInteger errorCount,
        final ActionConfirmation<File> truncateOnPageStraddle)
    {
        final ByteBuffer buffer = BufferUtil.allocateDirectAligned(FILE_IO_MAX_LENGTH_DEFAULT, CACHE_LINE_LENGTH);
        buffer.order(LITTLE_ENDIAN);
        final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight(buffer);

        final Long2ObjectHashMap<List<String>> segmentFilesByRecordingId = indexSegmentFiles(archiveDir);

        return (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
            verifyRecording(
                out,
                archiveDir,
                segmentFilesByRecordingId,
                options,
                catalog,
                checksum,
                epochClock,
                errorCount,
                truncateOnPageStraddle,
                headerFlyweight,
                recordingDescriptorOffset,
                headerEncoder,
                headerDecoder,
                descriptorEncoder,
                descriptorDecoder);
    }

    private static boolean truncateOnPageStraddle(final File maxSegmentFile)
    {
        return readContinueAnswer(String.format(
            "Last fragment in segment file: %s straddles a page boundary,%n" +
            "i.e. it is not possible to verify if it was written correctly.%n%n" +
            "Please choose the corrective action: (y) to truncate the file or (n) to do nothing",
            maxSegmentFile.getAbsolutePath()));
    }

    private static boolean continueOnFrameLimit(final Long frameLimit)
    {
        return readContinueAnswer("Specified frame limit " + frameLimit + " reached. Continue? (y/n)");
    }

    private static boolean readContinueAnswer(final String msg)
    {
        System.out.println();
        System.out.print(msg + ": ");
        final String answer = new Scanner(System.in).nextLine();

        return answer.isEmpty() || answer.equalsIgnoreCase("y") || answer.equalsIgnoreCase("yes");
    }

    private static ArchiveMarkFile openMarkFile(final File archiveDir, final Consumer<String> logger)
    {
        return new ArchiveMarkFile(
            resolveMarkFileDir(archiveDir), ArchiveMarkFile.FILENAME, INSTANCE, TimeUnit.SECONDS.toMillis(5), logger);
    }

    private static ArchiveMarkFile openMarkFileReadWrite(final File archiveDir, final EpochClock epochClock)
    {
        return new ArchiveMarkFile(
            resolveMarkFileDir(archiveDir),
            ArchiveMarkFile.FILENAME,
            epochClock,
            TimeUnit.SECONDS.toMillis(5),
            (version) -> {},
            null);
    }

    private static File resolveMarkFileDir(final File archiveDir)
    {
        final File linkFile = new File(archiveDir, ArchiveMarkFile.LINK_FILENAME);
        final File markFileDir;
        if (linkFile.exists())
        {
            try
            {
                final byte[] bytes = Files.readAllBytes(linkFile.toPath());
                final String markFileDirPath = new String(bytes, US_ASCII).trim();
                markFileDir = new File(markFileDirPath);
            }
            catch (final IOException ex)
            {
                throw new RuntimeException("failed to read link file=" + linkFile, ex);
            }
        }
        else
        {
            markFileDir = archiveDir;
        }

        return markFileDir;
    }

    private static void dump(
        final PrintStream out,
        final File archiveDir,
        final Catalog catalog,
        final long fragmentCountLimit,
        final ActionConfirmation<Long> continueActionOnFragmentLimit,
        final RecordingDescriptorHeaderDecoder header,
        final RecordingDescriptorDecoder descriptor,
        final SimpleFragmentHandler fragmentHandler)
    {
        final long stopPosition = descriptor.stopPosition();
        final long streamLength = stopPosition - descriptor.startPosition();

        out.println();
        out.println();
        out.println("Recording " + descriptor.recordingId());
        out.println("  channel: " + descriptor.strippedChannel());
        out.println("  streamId: " + descriptor.streamId());
        out.println("  stream length: " + (NULL_POSITION == stopPosition ? NULL_POSITION : streamLength));
        out.println(header);
        out.println(descriptor);

        if (0 == streamLength)
        {
            out.println("Recording is empty");
            return;
        }

        try (RecordingReader reader = new RecordingReader(
            catalog.recordingSummary(descriptor.recordingId(), new RecordingSummary()),
            archiveDir,
            descriptor.startPosition(),
            NULL_POSITION))
        {
            boolean isActive = true;
            long fragmentCount = fragmentCountLimit;
            do
            {
                out.println();
                out.print("Frame at position [" + reader.replayPosition() + "] ");

                if (reader.poll(fragmentHandler, 1) > 0)
                {
                    if (--fragmentCount == 0)
                    {
                        fragmentCount = fragmentCountLimit;
                        if (NULL_POSITION != stopPosition)
                        {
                            out.println(
                                streamLength - reader.replayPosition() + " bytes of (from " + streamLength +
                                ") remaining in recording id " + descriptor.recordingId());
                        }

                        isActive = continueActionOnFragmentLimit.confirm(fragmentCountLimit);
                    }
                }
            }
            while (isActive && !reader.isDone());
        }
    }

    private static void printMarkInformation(final ArchiveMarkFile markFile, final PrintStream out)
    {
        out.format("%1$tH:%1$tM:%1$tS (start: %2$tF %2$tH:%2$tM:%2$tS, activity: %3$tF %3$tH:%3$tM:%3$tS)%n",
            new Date(), new Date(markFile.decoder().startTimestamp()),
            new Date(markFile.activityTimestampVolatile()));
        out.println(markFile.decoder());
    }

    @SuppressWarnings("MethodLength")
    private static void verifyRecording(
        final PrintStream out,
        final File archiveDir,
        final Long2ObjectHashMap<List<String>> segmentFileByRecordingId,
        final Set<VerifyOption> options,
        final Catalog catalog,
        final Checksum checksum,
        final EpochClock epochClock,
        final MutableInteger errorCount,
        final ActionConfirmation<File> truncateOnPageStraddle,
        final DataHeaderFlyweight headerFlyweight,
        final int recordingDescriptorOffset,
        final RecordingDescriptorHeaderEncoder headerEncoder,
        final RecordingDescriptorHeaderDecoder headerDecoder,
        final RecordingDescriptorEncoder encoder,
        final RecordingDescriptorDecoder decoder)
    {
        final long recordingId = decoder.recordingId();
        final RecordingState state = headerDecoder.state();
        if (VALID != state && INVALID != state)
        {
            out.println("(recordingId=" + recordingId + ") skipping: " + state);
            return;
        }

        final long startPosition = decoder.startPosition();
        final long stopPosition = decoder.stopPosition();
        if (isPositionInvariantViolated(out, recordingId, startPosition, stopPosition))
        {
            errorCount.increment();
            headerEncoder.state(INVALID);
            return;
        }

        final int segmentLength = decoder.segmentFileLength();
        final int termLength = decoder.termBufferLength();
        final List<String> segmentFiles = segmentFileByRecordingId.getOrDefault(recordingId, emptyList());
        final String maxSegmentFile;
        final long computedStopPosition;

        try
        {
            maxSegmentFile = findSegmentFileWithHighestPosition(segmentFiles);
            if (maxSegmentFile != null)
            {
                final long maxSegmentPosition = parseSegmentFilePosition(maxSegmentFile) + (segmentLength - 1);
                if (startPosition > maxSegmentPosition || stopPosition > maxSegmentPosition)
                {
                    out.println("(recordingId=" + recordingId + ") ERR: Invariant violation: startPosition=" +
                        startPosition + " and/or stopPosition=" + stopPosition + " exceed max segment file position=" +
                        maxSegmentPosition);
                    errorCount.increment();
                    headerEncoder.state(INVALID);
                    return;
                }
            }

            computedStopPosition = computeStopPosition(
                archiveDir,
                maxSegmentFile,
                startPosition,
                termLength,
                segmentLength,
                checksum,
                headerFlyweight,
                truncateOnPageStraddle::confirm);
        }
        catch (final Exception ex)
        {
            final String message = ex.getMessage();
            out.println("(recordingId=" + recordingId + ") ERR: " + (null != message ? message : ex.toString()));
            errorCount.increment();
            headerEncoder.state(INVALID);
            return;
        }

        final boolean applyChecksum = options.contains(APPLY_CHECKSUM);
        if (applyChecksum)
        {
            final int recordingDescriptorChecksum = catalog.computeRecordingDescriptorChecksum(
                recordingDescriptorOffset, headerDecoder.length());
            if (recordingDescriptorChecksum != headerDecoder.checksum())
            {
                out.println("(recordingId=" + recordingId + ") ERR: invalid Catalog checksum: expected=" +
                    recordingDescriptorChecksum + ", actual=" + headerDecoder.checksum());
                errorCount.increment();
                headerEncoder.state(INVALID);
                return;
            }
        }

        if (null != maxSegmentFile)
        {
            final int streamId = decoder.streamId();
            if (options.contains(VERIFY_ALL_SEGMENT_FILES))
            {
                for (final String filename : segmentFiles)
                {
                    if (isInvalidSegmentFile(
                        out,
                        archiveDir,
                        recordingId,
                        filename,
                        startPosition,
                        termLength,
                        segmentLength,
                        streamId,
                        decoder.initialTermId(),
                        applyChecksum,
                        checksum,
                        headerFlyweight))
                    {
                        errorCount.increment();
                        headerEncoder.state(INVALID);
                        return;
                    }
                }
            }
            else if (isInvalidSegmentFile(
                out,
                archiveDir,
                recordingId,
                maxSegmentFile,
                startPosition,
                termLength,
                segmentLength,
                streamId,
                decoder.initialTermId(),
                applyChecksum,
                checksum,
                headerFlyweight))
            {
                errorCount.increment();
                headerEncoder.state(INVALID);
                return;
            }
        }

        if (computedStopPosition != stopPosition)
        {
            encoder.stopPosition(computedStopPosition);
            encoder.stopTimestamp(epochClock.time());
        }

        if (INVALID == state && !segmentFiles.isEmpty())
        {
            headerEncoder.state(VALID);
        }
        out.println("(recordingId=" + recordingId + ") OK");
    }

    private static boolean isPositionInvariantViolated(
        final PrintStream out, final long recordingId, final long startPosition, final long stopPosition)
    {
        if (startPosition < 0)
        {
            out.println("(recordingId=" + recordingId + ") ERR: Negative startPosition=" + startPosition);
            return true;
        }
        else if (isNotFrameAligned(startPosition))
        {
            out.println("(recordingId=" + recordingId + ") ERR: Non-aligned startPosition=" + startPosition);
            return true;
        }
        else if (stopPosition != NULL_POSITION)
        {
            if (stopPosition < startPosition)
            {
                out.println("(recordingId=" + recordingId + ") ERR: Invariant violation stopPosition=" +
                    stopPosition + " is before startPosition=" + startPosition);
                return true;
            }
            else if (isNotFrameAligned(stopPosition))
            {
                out.println("(recordingId=" + recordingId + ") ERR: Non-aligned stopPosition=" + stopPosition);
                return true;
            }
        }

        return false;
    }

    private static boolean isNotFrameAligned(final long position)
    {
        return 0 != (position & (FRAME_ALIGNMENT - 1));
    }

    private static boolean isInvalidSegmentFile(
        final PrintStream out,
        final File archiveDir,
        final long recordingId,
        final String fileName,
        final long startPosition,
        final int termLength,
        final int segmentLength,
        final int streamId,
        final int initialTermId,
        final boolean applyChecksum,
        final Checksum checksum,
        final DataHeaderFlyweight headerFlyweight)
    {
        final File file = new File(archiveDir, fileName);
        try (FileChannel channel = FileChannel.open(file.toPath(), READ))
        {
            final long offsetLimit = min(segmentLength, channel.size());
            final int positionBitsToShift = positionBitsToShift(termLength);
            final long startTermOffset = startPosition & (termLength - 1);
            final long startTermBasePosition = startPosition - startTermOffset;
            final long segmentFileBasePosition = parseSegmentFilePosition(fileName);

            final ByteBuffer byteBuffer = headerFlyweight.byteBuffer();
            final long bufferAddress = headerFlyweight.addressOffset();

            long fileOffset = segmentFileBasePosition == startTermBasePosition ? startTermOffset : 0;
            long position = segmentFileBasePosition + fileOffset;
            do
            {
                byteBuffer.clear().limit(HEADER_LENGTH);
                if (HEADER_LENGTH != channel.read(byteBuffer, fileOffset))
                {
                    out.println("(recordingId=" + recordingId + ", file=" + file +
                        ") ERR: failed to read fragment header");
                    return true;
                }

                final int frameLength = headerFlyweight.frameLength();
                if (0 == frameLength)
                {
                    break;
                }

                final int termId = computeTermIdFromPosition(position, positionBitsToShift, initialTermId);
                final int termOffset = (int)(position & (termLength - 1));
                if (isInvalidHeader(headerFlyweight, streamId, termId, termOffset))
                {
                    out.println("(recordingId=" + recordingId + ", file=" + file + ") ERR: fragment " +
                        "termOffset=" + headerFlyweight.termOffset() + " (expected=" + termOffset + "), " +
                        "termId=" + headerFlyweight.termId() + " (expected=" + termId + "), " +
                        "streamId=" + headerFlyweight.streamId() + " (expected=" + streamId + ")");
                    return true;
                }
                final int frameType = frameType(headerFlyweight, 0);
                final int sessionId = frameSessionId(headerFlyweight, 0);

                final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
                final int dataLength = alignedFrameLength - HEADER_LENGTH;
                byteBuffer.clear().limit(dataLength);
                if (dataLength != channel.read(byteBuffer, fileOffset + HEADER_LENGTH))
                {
                    out.println("(recordingId=" + recordingId + ", file=" + file + ") ERR: failed to read " +
                        dataLength + " byte(s) of data at offset " + (fileOffset + HEADER_LENGTH));
                    return true;
                }

                if (applyChecksum && HDR_TYPE_DATA == frameType)
                {
                    final int computedChecksum = checksum.compute(bufferAddress, 0, dataLength);
                    if (computedChecksum != sessionId)
                    {
                        out.println("(recordingId=" + recordingId + ", file=" + file + ") ERR: checksum failed " +
                            "recorded=" + sessionId + " (expected=" + computedChecksum + ")");
                        return true;
                    }
                }

                fileOffset += alignedFrameLength;
                position += alignedFrameLength;
            }
            while (fileOffset < offsetLimit);
        }
        catch (final IOException ex)
        {
            out.println("(recordingId=" + recordingId + ", file=" + file + ") ERR: failed to verify file");
            ex.printStackTrace(out);
            return true;
        }

        return false;
    }

    private static void printErrors(final PrintStream out, final ArchiveMarkFile markFile)
    {
        out.println("Archive error log:");
        CommonContext.printErrorLog(markFile.errorBuffer(), out);

        final String aeronDirectory = markFile.aeronDirectory();

        out.println();
        out.println("Aeron driver error log (directory: " + aeronDirectory + "):");
        final File cncFile = new File(aeronDirectory, CncFileDescriptor.CNC_FILE);

        final MappedByteBuffer cncByteBuffer = IoUtil.mapExistingFile(cncFile, FileChannel.MapMode.READ_ONLY, "cnc");
        final DirectBuffer cncMetaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);
        final int cncVersion = cncMetaDataBuffer.getInt(CncFileDescriptor.cncVersionOffset(0));

        CncFileDescriptor.checkVersion(cncVersion);
        CommonContext.printErrorLog(CncFileDescriptor.createErrorLogBuffer(cncByteBuffer, cncMetaDataBuffer), out);
    }

    static void checksumRecording(
        final PrintStream out,
        final File archiveDir,
        final long recordingId,
        final boolean allFiles,
        final Checksum checksum,
        final EpochClock epochClock)
    {
        try (Catalog catalog = openCatalogReadWrite(archiveDir, epochClock, MIN_CAPACITY, checksum, null))
        {
            final Long2ObjectHashMap<List<String>> segmentFilesByRecordingId = indexSegmentFiles(archiveDir);

            final CatalogEntryProcessor catalogEntryProcessor =
                (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                {
                    final ByteBuffer buffer = ByteBuffer.allocateDirect(
                        align(descriptorDecoder.mtuLength(), CACHE_LINE_LENGTH));
                    buffer.order(LITTLE_ENDIAN);
                    catalog.updateChecksum(recordingDescriptorOffset);
                    checksum(buffer, out, archiveDir, segmentFilesByRecordingId, allFiles, checksum, descriptorDecoder);
                };

            if (!catalog.forEntry(recordingId, catalogEntryProcessor))
            {
                throw new AeronException("no recording found with recordingId: " + recordingId);
            }
        }
    }

    private static void checksum(
        final ByteBuffer buffer,
        final PrintStream out,
        final File archiveDir,
        final Long2ObjectHashMap<List<String>> segmentFilesByRecordingId,
        final boolean allFiles,
        final Checksum checksum,
        final RecordingDescriptorDecoder descriptorDecoder)
    {
        final long recordingId = descriptorDecoder.recordingId();
        final long startPosition = descriptorDecoder.startPosition();
        final int termLength = descriptorDecoder.termBufferLength();
        final List<String> segmentFiles = segmentFilesByRecordingId.getOrDefault(recordingId, emptyList());

        if (allFiles)
        {
            for (final String fileName : segmentFiles)
            {
                checksumSegmentFile(
                    buffer, out, archiveDir, checksum, recordingId, fileName, startPosition, termLength);
            }
        }
        else
        {
            final String lastFile = findSegmentFileWithHighestPosition(segmentFiles);
            checksumSegmentFile(buffer, out, archiveDir, checksum, recordingId, lastFile, startPosition, termLength);
        }
    }

    private static void checksumSegmentFile(
        final ByteBuffer buffer,
        final PrintStream out,
        final File archiveDir,
        final Checksum checksum,
        final long recordingId,
        final String fileName,
        final long startPosition,
        final int termLength)
    {
        final File file = new File(archiveDir, fileName);
        final long startTermOffset = startPosition & (termLength - 1);
        final long startTermBasePosition = startPosition - startTermOffset;
        final long segmentFileBasePosition = parseSegmentFilePosition(fileName);

        try (FileChannel channel = FileChannel.open(file.toPath(), READ, WRITE))
        {
            final HeaderFlyweight headerFlyweight = new HeaderFlyweight(buffer);
            final long bufferAddress = headerFlyweight.addressOffset();
            final long size = channel.size();
            long fileOffset = segmentFileBasePosition == startTermBasePosition ? startTermOffset : 0;

            while (fileOffset < size)
            {
                buffer.clear().limit(HEADER_LENGTH);
                if (HEADER_LENGTH != channel.read(buffer, fileOffset))
                {
                    out.println("(recordingId=" + recordingId + ", file=" + file +
                        ") ERR: failed to read fragment header");
                    return;
                }

                final int frameLength = headerFlyweight.frameLength();
                if (0 == frameLength)
                {
                    break;
                }

                final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
                if (HDR_TYPE_DATA == frameType(headerFlyweight, 0))
                {
                    final int dataLength = alignedLength - HEADER_LENGTH;
                    buffer.clear().limit(dataLength);
                    if (dataLength != channel.read(buffer, fileOffset + HEADER_LENGTH))
                    {
                        out.println("(recordingId=" + recordingId + ", file=" + file + ") ERR: failed to read " +
                            dataLength + " byte(s) of data at offset " + (fileOffset + HEADER_LENGTH));
                        return;
                    }

                    int checksumResult = checksum.compute(bufferAddress, 0, dataLength);
                    if (NATIVE_BYTE_ORDER != LITTLE_ENDIAN)
                    {
                        checksumResult = Integer.reverseBytes(checksumResult);
                    }

                    buffer.clear();
                    buffer.putInt(checksumResult).flip();
                    channel.write(buffer, fileOffset + SESSION_ID_FIELD_OFFSET);
                }

                fileOffset += alignedLength;
            }
        }
        catch (final Exception ex)
        {
            out.println("(recordingId=" + recordingId + ", file=" + file + ") ERR: failed to checksum");
            ex.printStackTrace(out);
        }
    }

    static void checksum(
        final PrintStream out,
        final File archiveDir,
        final boolean allFiles,
        final Checksum checksum,
        final EpochClock epochClock)
    {
        try (Catalog catalog = openCatalogReadWrite(archiveDir, epochClock, MIN_CAPACITY, checksum, null))
        {
            final ByteBuffer buffer = ByteBuffer.allocateDirect(
                align(Configuration.MAX_UDP_PAYLOAD_LENGTH, CACHE_LINE_LENGTH));
            buffer.order(LITTLE_ENDIAN);
            final Long2ObjectHashMap<List<String>> segmentFilesByRecordingId = indexSegmentFiles(archiveDir);

            catalog.forEach(
                (recordingDescriptorOffset, headerEncoder, headerDecoder, descriptorEncoder, descriptorDecoder) ->
                {
                    try
                    {
                        catalog.updateChecksum(recordingDescriptorOffset);
                        checksum(
                            buffer, out, archiveDir, segmentFilesByRecordingId, allFiles, checksum, descriptorDecoder);
                    }
                    catch (final Exception ex)
                    {
                        out.println(
                            "(recordingId=" + descriptorDecoder.recordingId() + ") ERR: failed to compute checksums");
                        out.println(ex);
                    }
                });
        }
    }

    private static void deleteOrphanedSegmentFiles(
        final PrintStream out,
        final File archiveDir,
        final RecordingDescriptorDecoder descriptorDecoder,
        final List<String> segmentFiles)
    {
        final long minBaseOffset = segmentFileBasePosition(
            descriptorDecoder.startPosition(),
            descriptorDecoder.startPosition(),
            descriptorDecoder.termBufferLength(),
            descriptorDecoder.segmentFileLength());

        final long maxBaseOffset = NULL_POSITION == descriptorDecoder.stopPosition() ? NULL_POSITION :
            segmentFileBasePosition(
            descriptorDecoder.startPosition(),
            descriptorDecoder.stopPosition(),
            descriptorDecoder.termBufferLength(),
            descriptorDecoder.segmentFileLength());

        for (final String segmentFile : segmentFiles)
        {
            boolean delete;
            try
            {
                final long offset = parseSegmentFilePosition(segmentFile);
                delete = offset < minBaseOffset || NULL_POSITION != maxBaseOffset && offset > maxBaseOffset;
            }
            catch (final RuntimeException ex)
            {
                delete = true; // invalid file name
            }

            if (delete)
            {
                try
                {
                    Files.deleteIfExists(archiveDir.toPath().resolve(segmentFile));
                }
                catch (final IOException ex)
                {
                    ex.printStackTrace(out);
                }
            }
        }
    }

    private static void printHelp()
    {
        System.out.format(
            "Usage: <archive-dir> <command> (items in square brackets are optional)%n%n" +
            "  capacity [capacity in bytes]: gets or increases catalog capacity.%n%n" +
            "  checksum className [recordingId] [-a]: computes and persists checksums.%n" +
            "     checksums are computed using the specified Checksum implementation%n" +
            "     (e.g. io.aeron.archive.checksum.Crc32).%n" +
            "     Only the last segment file of each recording is processed by default,%n" +
            "     unless flag '-a' is specified in which case all of the segment files are processed.%n%n" +
            "  compact: compacts Catalog file by removing entries in non-valid state and deleting the%n" +
            "     corresponding segment files.%n%n" +
            "  count-entries: queries the number of `VALID` recording entries in the catalog.%n%n" +
            "  delete-orphaned-segments [recordingId]: deletes orphaned recording segments that have been detached,%n" +
            "     If recordingId is specified, only delete orphaned segments for that recording.%n" +
            "     i.e. outside the start and stop recording range, but are not deleted.%n%n" +
            "  describe: prints out descriptors for all valid recordings in the catalog.%n%n" +
            "  describe recordingId: prints out descriptor for the specified recording entry in the catalog.%n%n" +
            "  describe-all: prints out descriptors for all recordings in the catalog.%n%n" +
            "  dump [data fragment limit per recording]: prints descriptor(s)%n" +
            "     in the catalog and associated recorded data.%n%n" +
            "  errors: prints errors for the archive and media driver.%n%n" +
            "  max-entries [number of entries]: *** DEPRECATED: use `capacity` instead. ***%n%n" +
            "  migrate: migrates archive MarkFile, Catalog, and recordings to the latest version.%n%n" +
            "  pid: prints just PID of archive.%n%n" +
            "  verify [recordingId] [-a] [-checksum className]: verifies descriptor(s) in the catalog%n" +
            "     checking recording files availability and contents. Only the last segment file is%n" +
            "     verified unless flag '-a' is specified, i.e. meaning verify all segment files.%n" +
            "     To perform checksum for each data frame specify the '-checksum' flag together with%n" +
            "     the Checksum implementation class name (e.g. io.aeron.archive.checksum.Crc32).%n" +
            "     Faulty entries are marked as `INVALID`.%n%n" +
            "  mark-invalid <recordingId>: marks a recording as invalid. %n%n" +
            "  mark-valid <recordingId>: marks a previously invalidated recording as valid. %n%n");
        System.out.flush();
    }
}
