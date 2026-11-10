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
import io.aeron.ChannelUriStringBuilder;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.CRC32;

import static io.aeron.archive.ArchiveSystemTests.CATALOG_CAPACITY;
import static io.aeron.archive.ArchiveSystemTests.awaitSignal;
import static io.aeron.archive.ArchiveSystemTests.injectRecordingSignalConsumer;
import static io.aeron.archive.ArchiveSystemTests.offerToPosition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class MigrateSegmentsTest
{
    private static final String REPLAY_CHANNEL = "aeron:ipc";
    private static final int REPLAY_STREAM_ID = 1034;
    private static final int TERM_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int SEGMENT_LENGTH = TERM_LENGTH * 2;
    private static final int STREAM_ID = 1033;
    private static final int MTU_LENGTH = 1024;
    private static final int FRAGMENT_LIMIT = 10;

    private final Set<AutoCloseable> openPublications = new HashSet<>();
    private TestMediaDriver driver;
    private Archive archive;
    private Aeron aeron;
    private AeronArchive aeronArchive;
    private TestRecordingSignalConsumer signalConsumer;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @BeforeEach
    void before(@TempDir final Path tempDir)
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .publicationTermBufferLength(TERM_LENGTH)
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .spiesSimulateConnection(true)
            .dirDeleteOnStart(true);

        final Archive.Context archiveCtx = TestContexts.localhostArchive()
            .catalogCapacity(CATALOG_CAPACITY)
            .segmentFileLength(SEGMENT_LENGTH)
            .deleteArchiveOnStart(true)
            .archiveDir(tempDir.resolve("archive").toFile())
            .fileSyncLevel(0)
            .threadingMode(ArchiveThreadingMode.SHARED);


        driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());
        archive = Archive.launch(archiveCtx);
        systemTestWatcher.dataCollector().add(archiveCtx.archiveDir());

        aeron = Aeron.connect();

        aeronArchive = AeronArchive.connect(
            TestContexts.localhostAeronArchive()
                .aeron(aeron));

        signalConsumer = injectRecordingSignalConsumer(aeronArchive);
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(openPublications);
        CloseHelper.closeAll(aeronArchive, aeron, archive, driver);
    }

    @ParameterizedTest
    @MethodSource("validCases")
    void shouldMigrateSegments(final TestCaseParams testCase)
    {
        final RecordingParams src = testCase.source();
        final RecordingParams dst = testCase.destination();

        final long srcRecordingId = arrangeRecording(src);
        final long dstRecordingId = arrangeRecording(dst);

        final boolean isPrepend = src.recordedPosition() <= dst.startPosition();
        final CRC32 originalChecksum = new CRC32();
        if (isPrepend)
        {
            calculateChecksum(originalChecksum, srcRecordingId, src.startPosition(), src.recordedPosition());
            calculateChecksum(originalChecksum, dstRecordingId, dst.startPosition(), dst.recordedPosition());
        }
        else
        {
            calculateChecksum(originalChecksum, dstRecordingId, dst.startPosition(), dst.recordedPosition());
            calculateChecksum(originalChecksum, srcRecordingId, src.startPosition(), src.recordedPosition());
        }

        signalConsumer.reset();
        final long migratedSegmentCount = aeronArchive.migrateSegments(srcRecordingId, dstRecordingId);
        awaitSignal(aeronArchive, signalConsumer, RecordingSignal.DELETE);
        assertEquals(srcRecordingId, signalConsumer.recordingId);

        final CRC32 migratedChecksum = new CRC32();
        calculateChecksum(migratedChecksum, dstRecordingId,
            Math.min(src.startPosition(), dst.startPosition()),
            Math.max(src.recordedPosition(), dst.recordedPosition()));

        assertEquals(testCase.expectedMigratedSegmentCount, migratedSegmentCount);

        assertEquals(src.expectedStartPosition(), aeronArchive.getStartPosition(srcRecordingId));
        assertEquals(src.expectedStopPosition(), aeronArchive.getStopPosition(srcRecordingId));
        assertEquals(dst.expectedStartPosition(), aeronArchive.getStartPosition(dstRecordingId));
        assertEquals(dst.expectedStopPosition(), aeronArchive.getStopPosition(dstRecordingId));

        final File archiveDir = archive.context().archiveDir();

        final String[] segmentFiles = archiveDir.list(
            (dir, name) -> name.endsWith(".rec") || name.endsWith(".rec.del"));
        assertThat(segmentFiles, arrayContainingInAnyOrder(testCase.expectedSegments(dstRecordingId)));

        assertEquals(originalChecksum.getValue(), migratedChecksum.getValue());

        assertCanExtend(srcRecordingId, src);
        if (dst.state() != RecordingState.LIVE)
        {
            assertCanExtend(dstRecordingId, dst);
        }
    }

    @ParameterizedTest
    @MethodSource("invalidCases")
    void migrateSegmentsShouldThrow(final FailureCaseParams testCase)
    {
        final RecordingParams src = testCase.source();
        final RecordingParams dst = testCase.destination();

        final long srcRecordingId = arrangeRecording(src);
        final long dstRecordingId = arrangeRecording(dst);

        final File archiveDir = archive.context().archiveDir();
        testCase.archiveDirPerturbation().perturb(archiveDir, srcRecordingId, dstRecordingId);

        final ArchiveException ex = assertThrows(ArchiveException.class,
            () -> aeronArchive.migrateSegments(srcRecordingId, dstRecordingId));
        final String subErrorMessage = ex.getMessage()
            .replaceFirst(".* error: ", "")
            .replace(archiveDir.toString(), "${ARCHIVE_DIR}")
            .replace("\\", "/");
        assertEquals(testCase.expectedErrorMessage(), subErrorMessage);
    }

    static TestCaseParams[] validCases()
    {
        return new TestCaseParams[]
            {
                prependSegmentAlignedStream(FollowingSegmentAction.TRUNCATE),
                prependSegmentAlignedStream(FollowingSegmentAction.ASSERT_EXISTS),
                prependSegmentAlignedStream(FollowingSegmentAction.RENAME_FOR_DELETION),
                prependStreamThatDoesNotStartAtSegmentBoundary(),
                prependToLiveStream(),
                appendSegmentAlignedStream(FollowingSegmentAction.TRUNCATE),
                appendSegmentAlignedStream(FollowingSegmentAction.ASSERT_EXISTS),
                appendSegmentAlignedStream(FollowingSegmentAction.RENAME_FOR_DELETION),
            };
    }

    static FailureCaseParams[] invalidCases()
    {
        return new FailureCaseParams[]
            {
                differentStreamIds(),
                differentInitialTermIds(),
                differentMtuLengths(),
                differentTermBufferLengths(),
                prependLiveStream(),
                prependWithGap(),
                prependWithMissingSegmentFile(),
                prependOverPreexistingSegmentFile(),
                seamAtNonSegmentButTermBoundary(),
                seamAtNonSegmentAndNonTermBoundary(),
                appendWithGap(),
                appendToNonTruncatedStream(),
                appendToLiveStream(),
                appendWithMissingSegmentFile(),
                appendOverPreexistingSegmentFile()
            };
    }

    private void assertCanExtend(final long recordingId, final RecordingParams recordingParams)
    {
        final long extendPosition = recordingParams.expectedStopPosition();

        final StreamParams streamParams = recordingParams.stream();
        final String channelUri = new ChannelUriStringBuilder()
            .media("udp")
            .mtu(streamParams.mtuLength())
            .endpoint(streamParams.endpoint())
            .initialPosition(extendPosition, streamParams.initialTermId(), streamParams.termLength())
            .build();

        final long subscriptionId = aeronArchive.extendRecording(
            recordingId, channelUri, streamParams.streamId(), SourceLocation.LOCAL);

        try (Publication publication = aeron.addExclusivePublication(channelUri, streamParams.streamId()))
        {
            final CountersReader counters = aeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());

            offerToPosition(publication, "ext-message-", extendPosition + SEGMENT_LENGTH + 1L);
            Tests.awaitPosition(counters, counterId, publication.position());
        }

        signalConsumer.reset();
        aeronArchive.stopRecording(subscriptionId);
        awaitSignal(aeronArchive, signalConsumer, recordingId, RecordingSignal.STOP);
    }

    private void calculateChecksum(
        final CRC32 checksum,
        final long recordingId,
        final long startPosition,
        final long endPosition)
    {
        final long length = endPosition - startPosition;
        try (Subscription replay = aeronArchive.replay(
            recordingId, startPosition, length, REPLAY_CHANNEL, REPLAY_STREAM_ID))
        {
            while (replay.hasNoImages())
            {
                Tests.yield();
            }

            final Image image = replay.imageAtIndex(0);

            final FragmentAssembler fragmentAssembler = new FragmentAssembler(
                (buffer, offset, msgLength, header) ->
                {
                    final byte[] data = new byte[msgLength];
                    buffer.getBytes(offset, data);
                    checksum.update(data);
                });

            while (!image.isEndOfStream() && image.position() < endPosition)
            {
                image.poll(fragmentAssembler, FRAGMENT_LIMIT);

                if (image.isClosed())
                {
                    fail("Replay image closed unexpectedly.");
                }
            }
        }
    }

    private long arrangeRecording(final RecordingParams recordingParams)
    {
        final StreamParams streamParams = recordingParams.stream();

        final String channelUri = new ChannelUriStringBuilder()
            .media("udp")
            .mtu(streamParams.mtuLength())
            .endpoint(streamParams.endpoint())
            .initialPosition(recordingParams.startPosition(), streamParams.initialTermId(), streamParams.termLength())
            .build();

        final Publication publication = aeron.addExclusivePublication(channelUri, streamParams.streamId());
        openPublications.add(publication);
        final long subscriptionId = aeronArchive.startRecording(
            channelUri, streamParams.streamId(), SourceLocation.LOCAL, false);
        final CountersReader counters = aeron.countersReader();
        final int counterId =
            Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
        final long recordingId = RecordingPos.getRecordingId(counters, counterId);

        offerToPosition(publication, "src-message-", recordingParams.recordedPosition());
        Tests.awaitPosition(counters, counterId, publication.position());

        if (recordingParams.state() == RecordingState.LIVE)
        {
            if (recordingParams.followingSegment() != FollowingSegmentAction.LEAVE_ALONE)
            {
                fail("cannot alter following segment of live recordings");
            }
        }
        else
        {
            signalConsumer.reset();
            aeronArchive.stopRecording(subscriptionId);
            awaitSignal(aeronArchive, signalConsumer, recordingId, RecordingSignal.STOP);
            openPublications.remove(publication);
            publication.close();
        }

        final File archiveDir = archive.context().archiveDir();

        switch (recordingParams.followingSegment())
        {
            case TRUNCATE:
                signalConsumer.reset();
                aeronArchive.truncateRecording(recordingId, recordingParams.recordedPosition());
                awaitSignal(aeronArchive, signalConsumer, recordingId, RecordingSignal.DELETE);
                break;

            case ASSERT_EXISTS:
                assertEmptyFollowingSegmentExists(recordingParams, recordingId, archiveDir);
                break;

            case RENAME_FOR_DELETION:
                final File segmentFile = assertEmptyFollowingSegmentExists(recordingParams, recordingId, archiveDir);
                final String normalFileName = Archive.segmentFileName(recordingId, recordingParams.recordedPosition());
                final String deletingFileName = normalFileName + ".del";
                final File segmentFileForDeletion = new File(archiveDir, deletingFileName);

                if (!segmentFile.renameTo(segmentFileForDeletion))
                {
                    fail("failed to rename segment file.");
                }
                break;

            case LEAVE_ALONE:
                break;

            default:
                fail("unsupported kind of following segment.");
                break;
        }

        return recordingId;
    }

    private File assertEmptyFollowingSegmentExists(
        final RecordingParams recordingParams,
        final long recordingId,
        final File archiveDir)
    {
        if (recordingParams.recordedPosition() % SEGMENT_LENGTH != 0)
        {
            fail("following empty segment only possible when recording stops on a segment boundary");
        }

        final String fileName = Archive.segmentFileName(recordingId, recordingParams.recordedPosition());
        final File segmentFile = new File(archiveDir, fileName);
        if (!segmentFile.exists())
        {
            fail("expected empty following segment file to exist");
        }

        return segmentFile;
    }

    private static TestCaseParams prependSegmentAlignedStream(final FollowingSegmentAction followingSrcSegment)
    {
        final TestCaseParams test = new TestCaseParams();

        test.source()
            .startPosition(0)
            .recordedPosition(2 * SEGMENT_LENGTH)
            .expectedStartPosition(0)
            .expectedStopPosition(0)
            .followingSegment(followingSrcSegment)
            .stream().endpoint("localhost:3333");

        test.destination()
            .startPosition(2 * SEGMENT_LENGTH)
            .recordedPosition(4 * SEGMENT_LENGTH)
            .expectedStartPosition(0)
            .expectedStopPosition(4 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.ASSERT_EXISTS)
            .stream().endpoint("localhost:3334");

        test.expectedMigratedSegmentCount(2);

        final SegmentFileExpectation segmentFileExpectation = (dstRecordingId) -> new String[]
        {
            Archive.segmentFileName(dstRecordingId, 0),
            Archive.segmentFileName(dstRecordingId, SEGMENT_LENGTH),
            Archive.segmentFileName(dstRecordingId, SEGMENT_LENGTH * 2),
            Archive.segmentFileName(dstRecordingId, SEGMENT_LENGTH * 3),
            Archive.segmentFileName(dstRecordingId, SEGMENT_LENGTH * 4)
        };
        test.expectedSegments(segmentFileExpectation);

        return test;
    }

    private static TestCaseParams appendSegmentAlignedStream(final FollowingSegmentAction followingSrcSegment)
    {
        final TestCaseParams test = new TestCaseParams();

        test.source()
            .startPosition(2 * SEGMENT_LENGTH)
            .recordedPosition(4 * SEGMENT_LENGTH)
            .expectedStartPosition(2 * SEGMENT_LENGTH)
            .expectedStopPosition(2 * SEGMENT_LENGTH)
            .followingSegment(followingSrcSegment)
            .stream().endpoint("localhost:3333");

        test.destination()
            .startPosition(0)
            .recordedPosition(2 * SEGMENT_LENGTH)
            .expectedStartPosition(0)
            .expectedStopPosition(4 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.TRUNCATE)
            .stream().endpoint("localhost:3334");

        test.expectedMigratedSegmentCount(2);

        final SegmentFileExpectation segmentFileExpectation = (dstRecordingId) -> new String[]
        {
            Archive.segmentFileName(dstRecordingId, 0),
            Archive.segmentFileName(dstRecordingId, SEGMENT_LENGTH),
            Archive.segmentFileName(dstRecordingId, SEGMENT_LENGTH * 2),
            Archive.segmentFileName(dstRecordingId, SEGMENT_LENGTH * 3)
        };
        test.expectedSegments(segmentFileExpectation);

        return test;
    }

    private static TestCaseParams prependToLiveStream()
    {
        final TestCaseParams test = new TestCaseParams();

        test.source()
            .startPosition(0)
            .recordedPosition(2 * SEGMENT_LENGTH)
            .expectedStartPosition(0)
            .expectedStopPosition(0)
            .followingSegment(FollowingSegmentAction.ASSERT_EXISTS)
            .stream().endpoint("localhost:3333");

        test.destination()
            .startPosition(2 * SEGMENT_LENGTH)
            .recordedPosition(4 * SEGMENT_LENGTH)
            .expectedStartPosition(0)
            .expectedStopPosition(-1L)
            .followingSegment(FollowingSegmentAction.LEAVE_ALONE)
            .state(RecordingState.LIVE)
            .stream().endpoint("localhost:3334");

        test.expectedMigratedSegmentCount(2);

        final SegmentFileExpectation segmentFileExpectation = (dstRecordingId) -> new String[]
        {
            Archive.segmentFileName(dstRecordingId, 0),
            Archive.segmentFileName(dstRecordingId, SEGMENT_LENGTH),
            Archive.segmentFileName(dstRecordingId, SEGMENT_LENGTH * 2),
            Archive.segmentFileName(dstRecordingId, SEGMENT_LENGTH * 3),
            Archive.segmentFileName(dstRecordingId, SEGMENT_LENGTH * 4)
        };
        test.expectedSegments(segmentFileExpectation);

        return test;
    }

    private static TestCaseParams prependStreamThatDoesNotStartAtSegmentBoundary()
    {
        final TestCaseParams test = new TestCaseParams();

        final int srcStartPosition = SEGMENT_LENGTH + 256;

        test.source()
            .startPosition(srcStartPosition)
            .recordedPosition(2 * SEGMENT_LENGTH)
            .expectedStartPosition(srcStartPosition)
            .expectedStopPosition(srcStartPosition)
            .followingSegment(FollowingSegmentAction.ASSERT_EXISTS)
            .stream().endpoint("localhost:3333");

        test.destination()
            .startPosition(2 * SEGMENT_LENGTH)
            .recordedPosition(4 * SEGMENT_LENGTH)
            .expectedStartPosition(srcStartPosition)
            .expectedStopPosition(4 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.ASSERT_EXISTS)
            .stream().endpoint("localhost:3334");

        test.expectedMigratedSegmentCount(1);

        final SegmentFileExpectation segmentFileExpectation = (dstRecordingId) -> new String[]
        {
            Archive.segmentFileName(dstRecordingId, SEGMENT_LENGTH),
            Archive.segmentFileName(dstRecordingId, SEGMENT_LENGTH * 2),
            Archive.segmentFileName(dstRecordingId, SEGMENT_LENGTH * 3),
            Archive.segmentFileName(dstRecordingId, SEGMENT_LENGTH * 4)
        };
        test.expectedSegments(segmentFileExpectation);

        return test;
    }

    private static FailureCaseParams differentStreamIds()
    {
        final FailureCaseParams test = createValidPrependParams();
        test.source().stream().streamId(42);
        test.destination().stream().streamId(1337);
        test.expectedErrorMessage("invalid migrate: srcStreamId=42 dstStreamId=1337");
        return test;
    }

    private static FailureCaseParams differentMtuLengths()
    {
        final FailureCaseParams test = createValidPrependParams();
        test.source().stream().mtuLength(MTU_LENGTH);
        test.destination().stream().mtuLength(MTU_LENGTH * 2);
        test.expectedErrorMessage("invalid migrate: srcMtuLength=1024 dstMtuLength=2048");
        return test;
    }

    private static FailureCaseParams differentInitialTermIds()
    {
        final FailureCaseParams test = createValidPrependParams();
        test.source().stream().initialTermId(42);
        test.destination().stream().initialTermId(1337);
        test.expectedErrorMessage("invalid migrate: srcInitialTermId=42 dstInitialTermId=1337");
        return test;
    }

    private static FailureCaseParams differentTermBufferLengths()
    {
        final FailureCaseParams test = createValidPrependParams();
        test.source().stream().termLength(TERM_LENGTH);
        test.destination().stream().termLength(TERM_LENGTH * 2);
        test.expectedErrorMessage(
            "invalid migrate:" +
            " srcTermBufferLength=" + TERM_LENGTH +
            " dstTermBufferLength=" + TERM_LENGTH * 2);
        return test;
    }

    private static FailureCaseParams prependLiveStream()
    {
        final FailureCaseParams test = createValidPrependParams();
        test.source().state(RecordingState.LIVE);
        test.expectedErrorMessage("recording 0 is still active");
        return test;
    }

    private static FailureCaseParams createValidPrependParams()
    {
        final FailureCaseParams test = new FailureCaseParams();

        test.source()
            .startPosition(0)
            .recordedPosition(2 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.LEAVE_ALONE)
            .stream().endpoint("localhost:3333");

        test.destination()
            .startPosition(2 * SEGMENT_LENGTH)
            .recordedPosition(4 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.LEAVE_ALONE)
            .stream().endpoint("localhost:3334");

        return test;
    }

    private static FailureCaseParams prependWithGap()
    {
        final FailureCaseParams test = new FailureCaseParams();

        test.source()
            .startPosition(0)
            .recordedPosition(2 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.LEAVE_ALONE)
            .stream().endpoint("localhost:3333");

        test.destination()
            .startPosition(3 * SEGMENT_LENGTH)
            .recordedPosition(4 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.LEAVE_ALONE)
            .stream().endpoint("localhost:3334");

        test.expectedErrorMessage("invalid migrate: src and dst are not contiguous" +
            " srcStartPosition=0 srcStopPosition=262144" +
            " dstStartPosition=393216 dstStopPosition=524288");

        return test;
    }

    private static FailureCaseParams seamAtNonSegmentButTermBoundary()
    {
        final FailureCaseParams test = new FailureCaseParams();

        test.source()
            .startPosition(0)
            .recordedPosition(TERM_LENGTH)
            .followingSegment(FollowingSegmentAction.TRUNCATE)
            .stream().endpoint("localhost:3333");

        test.destination()
            .startPosition(TERM_LENGTH)
            .recordedPosition(2 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.LEAVE_ALONE)
            .stream().endpoint("localhost:3334");

        test.expectedErrorMessage("invalid migrate: join position is not on segment boundary" +
            " of src recording seamPosition=65536" +
            " startPosition=0 stopPosition=65536" +
            " termBufferLength=65536 segmentFileLength=131072");

        return test;
    }

    private static FailureCaseParams seamAtNonSegmentAndNonTermBoundary()
    {
        final FailureCaseParams test = new FailureCaseParams();

        test.source()
            .startPosition(0)
            .recordedPosition(1024)
            .followingSegment(FollowingSegmentAction.TRUNCATE)
            .stream().endpoint("localhost:3333");

        test.destination()
            .startPosition(1024)
            .recordedPosition(2 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.LEAVE_ALONE)
            .stream().endpoint("localhost:3334");

        test.expectedErrorMessage("invalid migrate: join position is not on segment boundary" +
            " of src recording seamPosition=1024" +
            " startPosition=0 stopPosition=1024" +
            " termBufferLength=65536 segmentFileLength=131072");

        return test;
    }

    private static FailureCaseParams appendWithGap()
    {
        final FailureCaseParams test = new FailureCaseParams();

        test.source()
            .startPosition(2 * SEGMENT_LENGTH)
            .recordedPosition(3 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.TRUNCATE)
            .stream().endpoint("localhost:3333");

        test.destination()
            .startPosition(0)
            .recordedPosition(SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.TRUNCATE)
            .stream().endpoint("localhost:3334");

        test.expectedErrorMessage("invalid migrate: src and dst are not contiguous" +
            " srcStartPosition=262144 srcStopPosition=393216" +
            " dstStartPosition=0 dstStopPosition=131072");

        return test;
    }

    private static FailureCaseParams appendToNonTruncatedStream()
    {
        final FailureCaseParams test = new FailureCaseParams();

        test.source()
            .startPosition(2 * SEGMENT_LENGTH)
            .recordedPosition(3 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.TRUNCATE)
            .stream().endpoint("localhost:3333");

        test.destination()
            .startPosition(0)
            .recordedPosition(2 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.LEAVE_ALONE)
            .stream().endpoint("localhost:3334");

        test.expectedErrorMessage("preexisting dst segment file ${ARCHIVE_DIR}/1-262144.rec");

        return test;
    }

    private static FailureCaseParams appendToLiveStream()
    {
        final FailureCaseParams test = new FailureCaseParams();

        test.source()
            .startPosition(2 * SEGMENT_LENGTH)
            .recordedPosition(3 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.TRUNCATE)
            .stream().endpoint("localhost:3333");

        test.destination()
            .startPosition(0)
            .recordedPosition(2 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.LEAVE_ALONE)
            .state(RecordingState.LIVE)
            .stream().endpoint("localhost:3334");

        test.expectedErrorMessage("invalid migrate: src and dst are not contiguous" +
            " srcStartPosition=262144 srcStopPosition=393216" +
            " dstStartPosition=0 dstStopPosition=-1");

        return test;
    }

    private static FailureCaseParams prependWithMissingSegmentFile()
    {
        final FailureCaseParams test = new FailureCaseParams();

        test.source()
            .startPosition(0)
            .recordedPosition(3 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.TRUNCATE)
            .stream().endpoint("localhost:3333");

        test.destination()
            .startPosition(3 * SEGMENT_LENGTH)
            .recordedPosition(5 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.LEAVE_ALONE)
            .stream().endpoint("localhost:3334");

        test.archiveDirPerturbation(new DeleteSrcSegmentPerturbation(SEGMENT_LENGTH));

        test.expectedErrorMessage("missing src segment file ${ARCHIVE_DIR}/0-131072.rec");

        return test;
    }

    private static FailureCaseParams prependOverPreexistingSegmentFile()
    {
        final FailureCaseParams test = new FailureCaseParams();

        test.source()
            .startPosition(0)
            .recordedPosition(3 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.TRUNCATE)
            .stream().endpoint("localhost:3333");

        test.destination()
            .startPosition(3 * SEGMENT_LENGTH)
            .recordedPosition(5 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.LEAVE_ALONE)
            .stream().endpoint("localhost:3334");

        test.archiveDirPerturbation(new AddDstSegmentPerturbation(SEGMENT_LENGTH));

        test.expectedErrorMessage("preexisting dst segment file ${ARCHIVE_DIR}/1-131072.rec");

        return test;
    }

    private static FailureCaseParams appendWithMissingSegmentFile()
    {
        final FailureCaseParams test = new FailureCaseParams();

        test.source()
            .startPosition(2 * SEGMENT_LENGTH)
            .recordedPosition(5 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.TRUNCATE)
            .stream().endpoint("localhost:3333");

        test.destination()
            .startPosition(0)
            .recordedPosition(2 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.TRUNCATE)
            .stream().endpoint("localhost:3334");

        test.archiveDirPerturbation(new DeleteSrcSegmentPerturbation(3 * SEGMENT_LENGTH));

        test.expectedErrorMessage("missing src segment file ${ARCHIVE_DIR}/0-393216.rec");

        return test;
    }


    private static FailureCaseParams appendOverPreexistingSegmentFile()
    {
        final FailureCaseParams test = new FailureCaseParams();

        test.source()
            .startPosition(2 * SEGMENT_LENGTH)
            .recordedPosition(5 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.TRUNCATE)
            .stream().endpoint("localhost:3333");

        test.destination()
            .startPosition(0)
            .recordedPosition(2 * SEGMENT_LENGTH)
            .followingSegment(FollowingSegmentAction.TRUNCATE)
            .stream().endpoint("localhost:3334");

        test.archiveDirPerturbation(new AddDstSegmentPerturbation(3 * SEGMENT_LENGTH));

        test.expectedErrorMessage("preexisting dst segment file ${ARCHIVE_DIR}/1-393216.rec");

        return test;
    }

    @SuppressWarnings("UnusedReturnValue")
    private static final class StreamParams
    {
        private String endpoint;
        private int initialTermId = 1337;
        private int mtuLength = MTU_LENGTH;
        private int termLength = TERM_LENGTH;
        private int streamId = STREAM_ID;

        public String endpoint()
        {
            return endpoint;
        }

        public StreamParams endpoint(final String endpoint)
        {
            this.endpoint = endpoint;
            return this;
        }

        public int initialTermId()
        {
            return initialTermId;
        }

        public StreamParams initialTermId(final int initialTermId)
        {
            this.initialTermId = initialTermId;
            return this;
        }

        public int mtuLength()
        {
            return mtuLength;
        }

        public StreamParams mtuLength(final int mtuLength)
        {
            this.mtuLength = mtuLength;
            return this;
        }

        public int termLength()
        {
            return termLength;
        }

        public StreamParams termLength(final int termLength)
        {
            this.termLength = termLength;
            return this;
        }

        public int streamId()
        {
            return streamId;
        }

        public StreamParams streamId(final int streamId)
        {
            this.streamId = streamId;
            return this;
        }

        @Override
        public String toString()
        {
            return "{" +
                "endpoint='" + endpoint + '\'' +
                ", initialTermId=" + initialTermId +
                ", mtuLength=" + mtuLength +
                ", termLength=" + termLength +
                ", streamId=" + streamId +
                '}';
        }
    }

    private enum FollowingSegmentAction
    {
        TRUNCATE,
        ASSERT_EXISTS,
        RENAME_FOR_DELETION,
        LEAVE_ALONE
    }

    private enum RecordingState
    {
        LIVE,
        STOPPED
    }

    @SuppressWarnings("UnusedReturnValue")
    private static final class RecordingParams
    {
        private final StreamParams stream = new StreamParams();
        private long startPosition;
        private long recordedPosition;
        private FollowingSegmentAction followingSegment = FollowingSegmentAction.TRUNCATE;
        private long expectedStartPosition;
        private long expectedStopPosition;
        private RecordingState state = RecordingState.STOPPED;

        public StreamParams stream()
        {
            return stream;
        }

        public long startPosition()
        {
            return startPosition;
        }

        public RecordingParams startPosition(final long startPosition)
        {
            this.startPosition = startPosition;
            return this;
        }

        public long recordedPosition()
        {
            return recordedPosition;
        }

        public RecordingParams recordedPosition(final long recordedPosition)
        {
            this.recordedPosition = recordedPosition;
            return this;
        }

        public FollowingSegmentAction followingSegment()
        {
            return followingSegment;
        }

        public RecordingParams followingSegment(final FollowingSegmentAction followingSegment)
        {
            this.followingSegment = followingSegment;
            return this;
        }

        public RecordingState state()
        {
            return state;
        }

        public RecordingParams state(final RecordingState state)
        {
            this.state = state;
            return this;
        }

        public long expectedStartPosition()
        {
            return expectedStartPosition;
        }

        public RecordingParams expectedStartPosition(final long expectedStartPosition)
        {
            this.expectedStartPosition = expectedStartPosition;
            return this;
        }

        public long expectedStopPosition()
        {
            return expectedStopPosition;
        }

        public RecordingParams expectedStopPosition(final long expectedStopPosition)
        {
            this.expectedStopPosition = expectedStopPosition;
            return this;
        }

        @Override
        public String toString()
        {
            return "{" +
                "startPosition=" + startPosition +
                ", recordedPosition=" + recordedPosition +
                ", followingSegment=" + followingSegment +
                ", state=" + state +
                '}';
        }
    }

    private interface SegmentFileExpectation
    {
        String[] getExpectedFiles(long dstRecordingId);
    }

    @SuppressWarnings("UnusedReturnValue")
    private static final class TestCaseParams
    {
        private final RecordingParams source = new RecordingParams();
        private final RecordingParams destination = new RecordingParams();
        private long expectedMigratedSegmentCount;
        private SegmentFileExpectation expectedSegments;

        public RecordingParams source()
        {
            return source;
        }

        public RecordingParams destination()
        {
            return destination;
        }

        public TestCaseParams expectedMigratedSegmentCount(final long expectedMigratedSegmentCount)
        {
            this.expectedMigratedSegmentCount = expectedMigratedSegmentCount;
            return this;
        }

        public TestCaseParams expectedSegments(final SegmentFileExpectation expectedSegments)
        {
            this.expectedSegments = expectedSegments;
            return this;
        }

        public String[] expectedSegments(final long dstRecordingId)
        {
            return expectedSegments.getExpectedFiles(dstRecordingId);
        }

        @Override
        public String toString()
        {
            return "{" +
                "source=" + source +
                ", destination=" + destination +
                '}';
        }
    }

    private interface ArchiveDirPerturbation
    {
        void perturb(
            File archiveDir,
            long srcRecordingId,
            long dstRecordingId);
    }

    private static final class NullPerturbation implements ArchiveDirPerturbation
    {
        private static final NullPerturbation INSTANCE = new NullPerturbation();

        private NullPerturbation()
        {
        }

        @Override
        public void perturb(final File archiveDir, final long srcRecordingId, final long dstRecordingId)
        {
        }

        @Override
        public String toString()
        {
            return "None";
        }
    }

    private static final class DeleteSrcSegmentPerturbation implements ArchiveDirPerturbation
    {
        private final long segmentBasePosition;

        private DeleteSrcSegmentPerturbation(final long segmentBasePosition)
        {
            this.segmentBasePosition = segmentBasePosition;
        }

        @Override
        public void perturb(final File archiveDir, final long srcRecordingId, final long dstRecordingId)
        {
            final String fileName = Archive.segmentFileName(srcRecordingId, segmentBasePosition);
            final File segmentFile = new File(archiveDir, fileName);
            assertTrue(segmentFile.exists());
            assertTrue(segmentFile.delete());
        }

        @Override
        public String toString()
        {
            return "DeleteSrcSegment(" + segmentBasePosition + ")";
        }
    }

    private static final class AddDstSegmentPerturbation implements ArchiveDirPerturbation
    {
        private final long segmentBasePosition;

        private AddDstSegmentPerturbation(final long segmentBasePosition)
        {
            this.segmentBasePosition = segmentBasePosition;
        }

        @Override
        public void perturb(final File archiveDir, final long srcRecordingId, final long dstRecordingId)
        {
            final String fileName = Archive.segmentFileName(dstRecordingId, segmentBasePosition);
            final File segmentFile = new File(archiveDir, fileName);
            assertFalse(segmentFile.exists());
            try
            {
                Files.write(segmentFile.toPath(), new byte[] {0x1, 0x2, 0x3}, StandardOpenOption.CREATE_NEW);
            }
            catch (final IOException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        @Override
        public String toString()
        {
            return "AddDstSegment(" + segmentBasePosition + ")";
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    private static final class FailureCaseParams
    {
        private final RecordingParams source = new RecordingParams();
        private final RecordingParams destination = new RecordingParams();
        private ArchiveDirPerturbation archiveDirPerturbation = NullPerturbation.INSTANCE;
        private String expectedErrorMessage;

        public RecordingParams source()
        {
            return source;
        }

        public RecordingParams destination()
        {
            return destination;
        }

        public String expectedErrorMessage()
        {
            return expectedErrorMessage;
        }

        public FailureCaseParams expectedErrorMessage(final String expectedErrorMessage)
        {
            this.expectedErrorMessage = expectedErrorMessage;
            return this;
        }

        public ArchiveDirPerturbation archiveDirPerturbation()
        {
            return archiveDirPerturbation;
        }

        public FailureCaseParams archiveDirPerturbation(final ArchiveDirPerturbation archiveDirPerturbation)
        {
            this.archiveDirPerturbation = archiveDirPerturbation;
            return this;
        }

        @Override
        public String toString()
        {
            return "{" +
                "source=" + source +
                ", destination=" + destination +
                ", archiveDirPerturbation=" + archiveDirPerturbation +
                '}';
        }
    }
}
