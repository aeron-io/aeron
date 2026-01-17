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
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.ReplicationParams;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.samples.archive.RecordingDescriptor;
import io.aeron.samples.archive.RecordingDescriptorCollector;
import io.aeron.samples.archive.SampleAuthenticatorSupplier;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.CommonContext.generateRandomDirName;
import static io.aeron.archive.ArchiveSystemTests.CATALOG_CAPACITY;
import static io.aeron.archive.ArchiveSystemTests.TERM_LENGTH;
import static io.aeron.archive.ArchiveSystemTests.awaitSignal;
import static io.aeron.archive.ArchiveSystemTests.consume;
import static io.aeron.archive.ArchiveSystemTests.injectRecordingSignalConsumer;
import static io.aeron.archive.ArchiveSystemTests.offer;
import static io.aeron.archive.ArchiveSystemTests.resetAndAwaitSignal;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.codecs.RecordingSignal.DELETE;
import static io.aeron.archive.codecs.RecordingSignal.EXTEND;
import static io.aeron.archive.codecs.RecordingSignal.MERGE;
import static io.aeron.archive.codecs.RecordingSignal.REPLICATE;
import static io.aeron.archive.codecs.RecordingSignal.REPLICATE_END;
import static io.aeron.archive.codecs.RecordingSignal.START;
import static io.aeron.archive.codecs.RecordingSignal.STOP;
import static io.aeron.archive.codecs.RecordingSignal.SYNC;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class ReplicateRecordingTest
{
    public static final String SRC_RESPONSE_CHANNEL = "aeron:udp?control-mode=response|control=localhost:10000";
    private static final int SRC_CONTROL_STREAM_ID = AeronArchive.Configuration.CONTROL_STREAM_ID_DEFAULT;
    private static final String SRC_CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8090";
    private static final String INVALID_SRC_CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:18090";
    private static final String SRC_CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final String DST_CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8095";
    private static final String DST_CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final String SRC_REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final String DST_REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:20000";
    private static final String REPLAY_CHANNEL = "aeron:udp?endpoint=localhost:6666";
    private static final int REPLAY_STREAM_ID = 101;
    private static final long TIMER_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(15);

    private static final int LIVE_STREAM_ID = 1033;
    private static final String LIVE_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .controlMode("dynamic")
        .controlEndpoint("localhost:8100")
        .termLength(TERM_LENGTH)
        .build();
    private TestMediaDriver srcDriver;
    private Archive.Context srcArchiveCtx;
    private Archive srcArchive;
    private TestMediaDriver dstDriver;
    private Archive dstArchive;
    private AeronArchive srcAeronArchive;
    private AeronArchive dstAeronArchive;
    private TestRecordingSignalConsumer srcRecordingSignalConsumer;
    private TestRecordingSignalConsumer dstRecordingSignalConsumer;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();
    private AeronArchive.Context srcAeronArchiveCtx;

    @BeforeEach
    void before()
    {
        final String srcAeronDirectoryName = generateRandomDirName();
        final String dstAeronDirectoryName = generateRandomDirName();

        final MediaDriver.Context srcContext = new MediaDriver.Context()
            .aeronDirectoryName(srcAeronDirectoryName)
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .spiesSimulateConnection(true)
            .timerIntervalNs(TIMER_INTERVAL_NS)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true);

        srcArchiveCtx = new Archive.Context()
            .catalogCapacity(CATALOG_CAPACITY)
            .aeronDirectoryName(srcAeronDirectoryName)
            .controlChannel(SRC_CONTROL_REQUEST_CHANNEL)
            .archiveClientContext(new AeronArchive.Context().controlResponseChannel(SRC_CONTROL_RESPONSE_CHANNEL))
            .recordingEventsEnabled(false)
            .replicationChannel(SRC_REPLICATION_CHANNEL)
            .deleteArchiveOnStart(true)
            .archiveDir(new File(SystemUtil.tmpDirName(), "src-archive"))
            .fileSyncLevel(0)
            .threadingMode(ArchiveThreadingMode.SHARED);

        final MediaDriver.Context dstContext = new MediaDriver.Context()
            .aeronDirectoryName(dstAeronDirectoryName)
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .spiesSimulateConnection(true)
            .timerIntervalNs(TIMER_INTERVAL_NS)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true);

        final Archive.Context dstArchiveCtx = new Archive.Context()
            .catalogCapacity(CATALOG_CAPACITY)
            .aeronDirectoryName(dstAeronDirectoryName)
            .controlChannel(DST_CONTROL_REQUEST_CHANNEL)
            .archiveClientContext(new AeronArchive.Context().controlResponseChannel(DST_CONTROL_RESPONSE_CHANNEL))
            .recordingEventsEnabled(false)
            .replicationChannel(DST_REPLICATION_CHANNEL)
            .deleteArchiveOnStart(true)
            .archiveDir(new File(SystemUtil.tmpDirName(), "dst-archive"))
            .fileSyncLevel(0)
            .threadingMode(ArchiveThreadingMode.SHARED);

        srcDriver = TestMediaDriver.launch(srcContext, systemTestWatcher);
        systemTestWatcher.dataCollector().add(srcContext.aeronDirectory());
        srcArchive = Archive.launch(srcArchiveCtx.clone());
        systemTestWatcher.dataCollector().add(srcArchiveCtx.archiveDir());
        dstDriver = TestMediaDriver.launch(dstContext, systemTestWatcher);
        systemTestWatcher.dataCollector().add(dstContext.aeronDirectory());
        dstArchive = Archive.launch(dstArchiveCtx);
        systemTestWatcher.dataCollector().add(dstArchiveCtx.archiveDir());

        srcAeronArchiveCtx = new AeronArchive.Context()
            .idleStrategy(YieldingIdleStrategy.INSTANCE)
            .controlRequestChannel(SRC_CONTROL_REQUEST_CHANNEL)
            .controlResponseChannel(SRC_CONTROL_RESPONSE_CHANNEL)
            .aeronDirectoryName(srcAeronDirectoryName);

        srcAeronArchive = AeronArchive.connect(srcAeronArchiveCtx.clone());

        dstAeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .idleStrategy(YieldingIdleStrategy.INSTANCE)
                .controlRequestChannel(DST_CONTROL_REQUEST_CHANNEL)
                .controlResponseChannel(DST_CONTROL_RESPONSE_CHANNEL)
                .aeronDirectoryName(dstAeronDirectoryName));

        srcRecordingSignalConsumer = injectRecordingSignalConsumer(srcAeronArchive);
        dstRecordingSignalConsumer = injectRecordingSignalConsumer(dstAeronArchive);
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(
            srcAeronArchive,
            dstAeronArchive,
            srcArchive,
            dstArchive,
            dstDriver,
            srcDriver);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldThrowExceptionWhenDstRecordingIdUnknown(final boolean useParams)
    {
        final long unknownId = 7L;
        try
        {
            if (useParams)
            {
                dstAeronArchive.replicate(
                    NULL_VALUE,
                    SRC_CONTROL_STREAM_ID,
                    SRC_CONTROL_REQUEST_CHANNEL,
                    new ReplicationParams().dstRecordingId(unknownId));
            }
            else
            {
                dstAeronArchive.replicate(
                    NULL_VALUE, unknownId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);
            }
        }
        catch (final ArchiveException ex)
        {
            assertEquals(ArchiveException.UNKNOWN_RECORDING, ex.errorCode());
            assertThat(ex.getMessage(), endsWith(Long.toString(unknownId)));
            return;
        }

        fail("expected archive exception");
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldThrowExceptionWhenSrcRecordingIdUnknown(final boolean useParams)
    {
        final long unknownId = 7L;
        if (useParams)
        {
            dstAeronArchive.replicate(
                unknownId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, new ReplicationParams());
        }
        else
        {
            dstAeronArchive.replicate(
                unknownId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);
        }

        String errorResponse;
        while (null == (errorResponse = dstAeronArchive.pollForErrorResponse()))
        {
            Thread.yield();
        }
        assertEquals("unknown src recording id " + unknownId, errorResponse);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldReplicateStoppedRecording(final boolean useParams)
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

        final Aeron srcAeron = srcAeronArchive.context().aeron();
        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            final CountersReader counters = srcAeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), srcAeronArchive.archiveId());
            srcRecordingId = RecordingPos.getRecordingId(counters, counterId);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(counters, counterId, publication.position());
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, STOP);

        dstRecordingSignalConsumer.reset();
        if (useParams)
        {
            dstAeronArchive.replicate(
                srcRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, new ReplicationParams());
        }
        else
        {
            dstAeronArchive.replicate(
                srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);
        }

        awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, REPLICATE);
        final long dstRecordingId = dstRecordingSignalConsumer.recordingId;
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, EXTEND);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, SYNC);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, REPLICATE_END);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, STOP);
    }

    @Test
    @InterruptAfter(10)
    void shouldThrowExceptionLiveDestinationUsedWithResponseChannels()
    {
        final long unknownId = 7L;
        final ReplicationParams replicationParams = new ReplicationParams()
            .replicationChannel(SRC_RESPONSE_CHANNEL)
            .liveDestination(LIVE_CHANNEL);

        final Executable replication = () -> dstAeronArchive.replicate(
            unknownId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, replicationParams);
        final ArchiveException archiveException = assertThrows(ArchiveException.class, replication);
        assertThat(
            archiveException.getMessage(), containsString("response channels can't be used with live destinations"));
    }

    @Test
    @InterruptAfter(10)
    void shouldThrowExceptionTagsUsedWithResponseChannels()
    {
        final long unknownId = 7L;
        final ReplicationParams replicationParams = new ReplicationParams()
            .replicationChannel(SRC_RESPONSE_CHANNEL)
            .channelTagId(1).subscriptionTagId(2);

        final Executable replication = () -> dstAeronArchive.replicate(
            unknownId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, replicationParams);
        final ArchiveException archiveException = assertThrows(ArchiveException.class, replication);
        assertThat(
            archiveException.getMessage(), containsString("response channels can't be used with tagged replication"));
    }

    @Test
    @InterruptAfter(10)
    void shouldReplicateStoppedRecordingWithResponseChannel()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

        final Aeron srcAeron = srcAeronArchive.context().aeron();
        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            final CountersReader counters = srcAeron.countersReader();
            final int counterId = Tests.awaitRecordingCounterId(
                counters, publication.sessionId(), srcAeronArchive.archiveId());
            srcRecordingId = RecordingPos.getRecordingId(counters, counterId);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(counters, counterId, publication.position());
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, STOP);

        dstRecordingSignalConsumer.reset();
        final ReplicationParams replicationParams = new ReplicationParams()
            .replicationChannel(SRC_RESPONSE_CHANNEL)
            .srcResponseChannel(SRC_RESPONSE_CHANNEL);

        dstAeronArchive.replicate(
            srcRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, replicationParams);

        awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, REPLICATE);
        final long dstRecordingId = dstRecordingSignalConsumer.recordingId;
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, EXTEND);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, SYNC);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, REPLICATE_END);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, STOP);
    }

    @Test
    @InterruptAfter(10)
    void shouldReplicateRecordingWithCustomSessionId()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final int specifiedSessionId = 100_024;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

        final Aeron srcAeron = srcAeronArchive.context().aeron();
        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            final CountersReader counters = srcAeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), srcAeronArchive.archiveId());
            srcRecordingId = RecordingPos.getRecordingId(counters, counterId);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(counters, counterId, publication.position());
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, STOP);

        dstRecordingSignalConsumer.reset();
        dstAeronArchive.replicate(
            srcRecordingId,
            SRC_CONTROL_STREAM_ID,
            SRC_CONTROL_REQUEST_CHANNEL,
            new ReplicationParams().replicationSessionId(specifiedSessionId));

        awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, REPLICATE);
        final long dstRecordingId = dstRecordingSignalConsumer.recordingId;
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, EXTEND);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, SYNC);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, REPLICATE_END);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, STOP);


        final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(1);
        assertEquals(1, dstAeronArchive.listRecording(dstRecordingId, collector.reset()));
        final RecordingDescriptor dstRecording = collector.descriptors().get(0).retain();

        assertEquals(specifiedSessionId, dstRecording.sessionId());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldReplicateWithOlderVersion(final boolean useParams)
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

        final Aeron srcAeron = srcAeronArchive.context().aeron();
        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            final CountersReader counters = srcAeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), srcAeronArchive.archiveId());
            srcRecordingId = RecordingPos.getRecordingId(counters, counterId);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(counters, counterId, publication.position());
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, STOP);

        dstRecordingSignalConsumer.reset();
        if (useParams)
        {
            dstAeronArchive.replicate(
                srcRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, new ReplicationParams());
        }
        else
        {
            dstAeronArchive.replicate(
                srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);
        }

        awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, REPLICATE);
        final long dstRecordingId = dstRecordingSignalConsumer.recordingId;
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, EXTEND);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, SYNC);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, REPLICATE_END);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, STOP);
    }

    @Test
    @InterruptAfter(10)
    void shouldReplicateStoppedRecordingsConcurrently()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long[] srcRecordingIds = new long[2];
        long position = 0;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);
        final Aeron srcAeron = srcAeronArchive.context().aeron();

        for (int i = 0; i < 2; i++)
        {
            try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
            {
                final CountersReader counters = srcAeron.countersReader();
                final int counterId =
                    Tests.awaitRecordingCounterId(counters, publication.sessionId(), srcAeronArchive.archiveId());
                srcRecordingIds[i] = RecordingPos.getRecordingId(counters, counterId);

                offer(publication, messageCount, messagePrefix);
                position = publication.position();
                Tests.awaitPosition(counters, counterId, position);
            }
            awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingIds[i], STOP);
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);

        for (int i = 0; i < 2; i++)
        {
            dstRecordingSignalConsumer.reset();
            dstAeronArchive.archiveProxy().replicate(
                srcRecordingIds[i],
                SRC_CONTROL_STREAM_ID,
                SRC_CONTROL_REQUEST_CHANNEL,
                new ReplicationParams(),
                dstAeronArchive.context().aeron().nextCorrelationId(),
                dstAeronArchive.controlSessionId());
        }

        int stopCount = 0;
        while (stopCount < 2)
        {
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, STOP);
            stopCount++;
        }

        assertEquals(dstAeronArchive.getStopPosition(0), position);
        assertEquals(dstAeronArchive.getStopPosition(1), position);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldReplicateLiveWithoutMergingRecording(final boolean useParams)
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);
        final Aeron srcAeron = srcAeronArchive.context().aeron();

        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            final CountersReader srcCounters = srcAeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(srcCounters, publication.sessionId(), srcAeronArchive.archiveId());
            srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(srcCounters, counterId, publication.position());

            dstRecordingSignalConsumer.reset();
            final long replicationId;
            if (useParams)
            {
                replicationId = dstAeronArchive.replicate(
                    srcRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, new ReplicationParams());
            }
            else
            {
                replicationId = dstAeronArchive.replicate(
                    srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);
            }

            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, REPLICATE);
            final long dstRecordingId = dstRecordingSignalConsumer.recordingId;
            resetAndAwaitSignal(
                dstAeronArchive,
                dstRecordingSignalConsumer,
                dstRecordingId,
                EXTEND);

            final CountersReader dstCounters = dstAeronArchive.context().aeron().countersReader();
            final int dstCounterId =
                RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId, dstAeronArchive.archiveId());
            Tests.awaitPosition(dstCounters, dstCounterId, publication.position());

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(dstCounters, dstCounterId, publication.position());

            dstRecordingSignalConsumer.reset();
            dstAeronArchive.stopReplication(replicationId);
            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, STOP);
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, STOP);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldReplicateLiveRecordingAndStopAtSpecifiedPosition(final boolean useParams)
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);
        final Aeron srcAeron = srcAeronArchive.context().aeron();

        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            final CountersReader srcCounters = srcAeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(srcCounters, publication.sessionId(), srcAeronArchive.archiveId());
            srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);

            offer(publication, messageCount, messagePrefix);
            final long firstPosition = publication.position();
            Tests.awaitPosition(srcCounters, counterId, firstPosition);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(srcCounters, counterId, publication.position());

            dstRecordingSignalConsumer.reset();
            if (useParams)
            {
                dstAeronArchive.replicate(
                    srcRecordingId,
                    SRC_CONTROL_STREAM_ID,
                    SRC_CONTROL_REQUEST_CHANNEL,
                    new ReplicationParams().stopPosition(firstPosition));
            }
            else
            {
                dstAeronArchive.replicate(
                    srcRecordingId,
                    NULL_VALUE,
                    firstPosition,
                    SRC_CONTROL_STREAM_ID,
                    SRC_CONTROL_REQUEST_CHANNEL,
                    null,
                    null);
            }

            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, REPLICATE);
            final long dstRecordingId = dstRecordingSignalConsumer.recordingId;
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, EXTEND);
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, STOP);

            offer(publication, messageCount, messagePrefix);
            final int srcCounterId =
                RecordingPos.findCounterIdByRecording(srcCounters, srcRecordingId, srcAeronArchive.archiveId());
            Tests.awaitPosition(srcCounters, srcCounterId, publication.position());

            assertTrue(firstPosition < publication.position());
            long dstStopPosition;
            while (NULL_POSITION == (dstStopPosition = dstAeronArchive.getStopPosition(dstRecordingId)))
            {
                Tests.yield();
            }
            assertEquals(firstPosition, dstStopPosition);
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, STOP);
    }

    @Test
    @InterruptAfter(10)
    void shouldReplicateMoreThanOnce()
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);
        final Aeron srcAeron = srcAeronArchive.context().aeron();
        final ReplicationParams replicationParams = new ReplicationParams();

        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            final CountersReader srcCounters = srcAeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(srcCounters, publication.sessionId(), srcAeronArchive.archiveId());
            srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(srcCounters, counterId, publication.position());

            dstRecordingSignalConsumer.reset();
            replicationParams
                .reset()
                .replicationSessionId((int)dstAeronArchive.context().aeron().nextCorrelationId());

            long replicationId = dstAeronArchive.replicate(
                srcRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, replicationParams);

            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, REPLICATE);
            final long dstRecordingId = dstRecordingSignalConsumer.recordingId;
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, EXTEND);

            final CountersReader dstCounters = dstArchive.context().aeron().countersReader();
            int dstCounterId =
                RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId, dstAeronArchive.archiveId());
            Tests.awaitPosition(dstCounters, dstCounterId, publication.position());

            dstRecordingSignalConsumer.reset();
            dstAeronArchive.stopReplication(replicationId);
            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, STOP);

            dstRecordingSignalConsumer.reset();

            replicationParams
                .reset()
                .replicationSessionId((int)dstAeronArchive.context().aeron().nextCorrelationId())
                .dstRecordingId(dstRecordingId);
            replicationId = dstAeronArchive.replicate(
                srcRecordingId,
                SRC_CONTROL_STREAM_ID,
                SRC_CONTROL_REQUEST_CHANNEL,
                replicationParams);

            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, EXTEND);

            dstCounterId =
                RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId, dstAeronArchive.archiveId());

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(dstCounters, dstCounterId, publication.position());

            dstRecordingSignalConsumer.reset();
            dstAeronArchive.stopReplication(replicationId);
            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, STOP);
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, STOP);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldHandleMultipleConcurrentReplicationsToSeveralArchives(
        final boolean useResponseChannels, final @TempDir Path tempDir)
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);
        final Aeron srcAeron = srcAeronArchive.context().aeron();
        final ReplicationParams replicationParams = new ReplicationParams()
            .replicationSessionId(42);
        if (useResponseChannels)
        {
            replicationParams
                .replicationChannel("aeron:udp?control-mode=response|control=localhost:3000")
                .srcResponseChannel(SRC_RESPONSE_CHANNEL);
        }
        else
        {
            replicationParams.replicationChannel("aeron:udp?endpoint=localhost:0");
        }

        try (TestMediaDriver driver = TestMediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .threadingMode(ThreadingMode.SHARED), systemTestWatcher);
            Archive archive = Archive.launch(srcArchiveCtx.clone()
                .aeronDirectoryName(driver.aeronDirectoryName())
                .archiveDir(tempDir.resolve("archive").toFile())
                .archiveClientContext(new AeronArchive.Context()
                    .controlResponseChannel("aeron:udp?endpoint=localhost:0").controlResponseStreamId(222))
                .controlChannel("aeron:udp?endpoint=127.0.0.1:8099")
                .controlStreamId(110)
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .archiveId(Long.MAX_VALUE));
            AeronArchive aeronArchive = AeronArchive.connect(new AeronArchive.Context()
                .aeronDirectoryName(driver.aeronDirectoryName())
                .controlRequestChannel(archive.context().controlChannel())
                .controlRequestStreamId(archive.context().controlStreamId())
                .controlResponseStreamId(120)
                .controlResponseChannel("aeron:udp?control-mode=response|control=127.0.0.1:30000")))
        {
            final TestRecordingSignalConsumer recordingSignalConsumer =
                injectRecordingSignalConsumer(aeronArchive);

            try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
            {
                final CountersReader srcCounters = srcAeron.countersReader();
                final int counterId =
                    Tests.awaitRecordingCounterId(srcCounters, publication.sessionId(), srcAeronArchive.archiveId());
                srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);

                offer(publication, messageCount, messagePrefix);
                Tests.awaitPosition(srcCounters, counterId, publication.position());

                dstRecordingSignalConsumer.reset();
                final long replicationId1 = dstAeronArchive.replicate(
                    srcRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, replicationParams);

                recordingSignalConsumer.reset();
                final long replicationId2 = aeronArchive.replicate(
                    srcRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, replicationParams);

                awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, REPLICATE);
                final long dstRecordingId1 = dstRecordingSignalConsumer.recordingId;
                resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId1, EXTEND);

                awaitSignal(aeronArchive, recordingSignalConsumer, REPLICATE);
                final long dstRecordingId2 = recordingSignalConsumer.recordingId;
                resetAndAwaitSignal(aeronArchive, recordingSignalConsumer, dstRecordingId2, EXTEND);

                final CountersReader counters1 = dstArchive.context().aeron().countersReader();
                final int counterId1 =
                    RecordingPos.findCounterIdByRecording(counters1, dstRecordingId1, dstAeronArchive.archiveId());
                Tests.awaitPosition(counters1, counterId1, publication.position());

                final CountersReader counters2 = aeronArchive.context().aeron().countersReader();
                final int counterId2 =
                    RecordingPos.findCounterIdByRecording(counters2, dstRecordingId2, aeronArchive.archiveId());
                Tests.awaitPosition(counters2, counterId2, publication.position());

                offer(publication, messageCount, messagePrefix);
                Tests.awaitPosition(counters1, counterId1, publication.position());
                Tests.awaitPosition(counters2, counterId2, publication.position());

                dstRecordingSignalConsumer.reset();
                dstAeronArchive.stopReplication(replicationId1);

                recordingSignalConsumer.reset();
                aeronArchive.stopReplication(replicationId2);

                awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId1, STOP);
                awaitSignal(aeronArchive, recordingSignalConsumer, dstRecordingId1, STOP);
            }
        }
        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, STOP);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldReplicateSyncedRecording(final boolean useParams)
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);
        final Aeron srcAeron = srcAeronArchive.context().aeron();

        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            final CountersReader srcCounters = srcAeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(srcCounters, publication.sessionId(), srcAeronArchive.archiveId());
            srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(srcCounters, counterId, publication.position());

            srcRecordingSignalConsumer.reset();
            srcAeronArchive.stopRecording(subscriptionId);
            awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, STOP);

            dstRecordingSignalConsumer.reset();
            if (useParams)
            {
                dstAeronArchive.replicate(
                    srcRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, new ReplicationParams());
            }
            else
            {
                dstAeronArchive.replicate(
                    srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);
            }
            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, REPLICATE);
            final long dstRecordingId = dstRecordingSignalConsumer.recordingId;
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, EXTEND);


            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, SYNC);
            resetAndAwaitSignal(
                dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, REPLICATE_END);
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, STOP);

            dstRecordingSignalConsumer.reset();
            if (useParams)
            {
                dstAeronArchive.replicate(
                    srcRecordingId,
                    SRC_CONTROL_STREAM_ID,
                    SRC_CONTROL_REQUEST_CHANNEL,
                    new ReplicationParams().dstRecordingId(dstRecordingId));
            }
            else
            {
                dstAeronArchive.replicate(
                    srcRecordingId, dstRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);
            }
            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, SYNC);
            resetAndAwaitSignal(
                dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, REPLICATE_END);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldReplicateLiveRecordingAndMerge(final boolean useParams)
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);
        final Aeron srcAeron = srcAeronArchive.context().aeron();

        srcRecordingSignalConsumer.reset();
        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, START);
            final long signaledRecordingId = srcRecordingSignalConsumer.recordingId;
            final CountersReader srcCounters = srcAeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(srcCounters, publication.sessionId(), srcAeronArchive.archiveId());
            srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);
            assertEquals(srcRecordingId, signaledRecordingId);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(srcCounters, counterId, publication.position());

            dstRecordingSignalConsumer.reset();
            if (useParams)
            {
                dstAeronArchive.replicate(
                    srcRecordingId,
                    SRC_CONTROL_STREAM_ID,
                    SRC_CONTROL_REQUEST_CHANNEL,
                    new ReplicationParams().liveDestination(LIVE_CHANNEL));
            }
            else
            {
                dstAeronArchive.replicate(
                    srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, LIVE_CHANNEL);
            }

            offer(publication, messageCount, messagePrefix);

            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, REPLICATE);
            final long dstRecordingId = dstRecordingSignalConsumer.recordingId;
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, EXTEND);
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, MERGE);

            final CountersReader dstCounters = dstArchive.context().aeron().countersReader();
            final int dstCounterId =
                RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId, dstAeronArchive.archiveId());

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(dstCounters, dstCounterId, publication.position());
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, STOP);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldReplicateLiveRecordingAndMergeBeforeDataFlows(final boolean useParams)
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;
        final long dstRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);
        final Aeron srcAeron = srcAeronArchive.context().aeron();

        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            final CountersReader srcCounters = srcAeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(srcCounters, publication.sessionId(), srcAeronArchive.archiveId());
            srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);

            dstRecordingSignalConsumer.reset();
            if (useParams)
            {
                dstAeronArchive.replicate(
                    srcRecordingId,
                    SRC_CONTROL_STREAM_ID,
                    SRC_CONTROL_REQUEST_CHANNEL,
                    new ReplicationParams().liveDestination(LIVE_CHANNEL));
            }
            else
            {
                dstAeronArchive.replicate(
                    srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, LIVE_CHANNEL);
            }

            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, REPLICATE);
            dstRecordingId = dstRecordingSignalConsumer.recordingId;
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, EXTEND);
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, MERGE);
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, REPLICATE_END);

            final CountersReader dstCounters = dstArchive.context().aeron().countersReader();
            final int dstCounterId =
                RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId, dstAeronArchive.archiveId());

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(dstCounters, dstCounterId, publication.position());
        }

        dstRecordingSignalConsumer.reset();
        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, STOP);
        assertEquals(0, dstAeronArchive.pollForRecordingSignals());
        assertEquals(NULL_VALUE, dstRecordingSignalConsumer.recordingId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldReplicateLiveRecordingAndMergeWhileFollowingWithTaggedSubscription(final boolean useParams)
    {
        final Aeron srcAeron = srcAeronArchive.context().aeron();
        final Aeron dstAeron = dstAeronArchive.context().aeron();
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;
        final long dstRecordingId;
        final long channelTagId = dstAeron.nextCorrelationId();
        final long subscriptionTagId = dstAeron.nextCorrelationId();
        final String taggedChannel =
            "aeron:udp?control-mode=manual|rejoin=false|tags=" + channelTagId + "," + subscriptionTagId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID);
            Subscription taggedSubscription = dstAeron.addSubscription(taggedChannel, LIVE_STREAM_ID))
        {
            final CountersReader srcCounters = srcAeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(srcCounters, publication.sessionId(), srcAeronArchive.archiveId());
            srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(srcCounters, counterId, publication.position());

            dstRecordingSignalConsumer.reset();
            if (useParams)
            {
                final ReplicationParams replicationParams = new ReplicationParams()
                    .liveDestination(LIVE_CHANNEL)
                    .channelTagId(channelTagId)
                    .subscriptionTagId(subscriptionTagId);

                dstAeronArchive.replicate(
                    srcRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, replicationParams);
            }
            else
            {
                dstAeronArchive.taggedReplicate(
                    srcRecordingId,
                    NULL_VALUE,
                    channelTagId,
                    subscriptionTagId,
                    SRC_CONTROL_STREAM_ID,
                    SRC_CONTROL_REQUEST_CHANNEL,
                    LIVE_CHANNEL);
            }

            consume(taggedSubscription, messageCount, messagePrefix);

            offer(publication, messageCount, messagePrefix);
            consume(taggedSubscription, messageCount, messagePrefix);

            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, REPLICATE);
            dstRecordingId = dstRecordingSignalConsumer.recordingId;
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, EXTEND);
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, MERGE);
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, REPLICATE_END);

            final CountersReader dstCounters = dstAeron.countersReader();
            final int dstCounterId =
                RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId, dstAeronArchive.archiveId());

            offer(publication, messageCount, messagePrefix);
            consume(taggedSubscription, messageCount, messagePrefix);
            Tests.awaitPosition(dstCounters, dstCounterId, publication.position());

            final Image image = taggedSubscription.imageBySessionId(publication.sessionId());
            assertEquals(publication.position(), image.position());
        }

        dstRecordingSignalConsumer.reset();
        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, STOP);
        assertEquals(0, dstAeronArchive.pollForRecordingSignals());
        assertEquals(NULL_VALUE, dstRecordingSignalConsumer.recordingId);
    }

    @Test
    @InterruptAfter(10)
    public void shouldReplicateStoppedRecordingWithFileIoMaxLength()
    {
        final String messagePrefix = "Message-Prefix-";
        final int fileIoMaxLength = 1500;
        final int longMessagePadding = (2 * fileIoMaxLength) + 1000;

        final StringBuilder longMessagePrefix = new StringBuilder();
        longMessagePrefix.append(messagePrefix);
        while (longMessagePrefix.length() < longMessagePadding)
        {
            longMessagePrefix.append('X');
        }
        longMessagePrefix.append('-');

        final int messageCount = 10;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);
        final Aeron srcAeron = srcAeronArchive.context().aeron();

        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            final CountersReader counters = srcAeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), srcAeronArchive.archiveId());
            srcRecordingId = RecordingPos.getRecordingId(counters, counterId);

            offer(publication, messageCount, longMessagePrefix.toString());
            Tests.awaitPosition(counters, counterId, publication.position());
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, STOP);

        dstRecordingSignalConsumer.reset();
        dstAeronArchive.replicate(
            srcRecordingId,
            SRC_CONTROL_STREAM_ID,
            SRC_CONTROL_REQUEST_CHANNEL,
            new ReplicationParams().fileIoMaxLength(fileIoMaxLength));

        awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, REPLICATE);
        final long dstRecordingId = dstRecordingSignalConsumer.recordingId;
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, EXTEND);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, SYNC);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, REPLICATE_END);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, STOP);

        assertNotEquals(NULL_VALUE, dstRecordingId);

        validateRecordingAreEqual(srcRecordingId, dstRecordingId);
    }

    @Test
    @InterruptAfter(10)
    public void shouldErrorReplicateIfFileIoMaxLengthIsLessThanMtu()
    {
        final String messagePrefix = "Message-Prefix-";
        final int fileIoMaxLength = 1500;
        final int longMessagePadding = (2 * fileIoMaxLength) + 1000;

        final StringBuilder longMessagePrefix = new StringBuilder();
        longMessagePrefix.append(messagePrefix);
        while (longMessagePrefix.length() < longMessagePadding)
        {
            longMessagePrefix.append('X');
        }
        longMessagePrefix.append('-');

        final int messageCount = 10;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);
        final Aeron srcAeron = srcAeronArchive.context().aeron();

        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            final CountersReader counters = srcAeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), srcAeronArchive.archiveId());
            srcRecordingId = RecordingPos.getRecordingId(counters, counterId);

            offer(publication, messageCount, longMessagePrefix.toString());
            Tests.awaitPosition(counters, counterId, publication.position());
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, STOP);

        final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(10);
        final int i = srcAeronArchive.listRecording(srcRecordingId, collector.reset());
        assertEquals(1, i);

        final RecordingDescriptor descriptor = collector.descriptors().get(0);
        final int mtu = descriptor.mtuLength();

        dstAeronArchive.replicate(
            srcRecordingId,
            SRC_CONTROL_STREAM_ID,
            SRC_CONTROL_REQUEST_CHANNEL,
            new ReplicationParams().fileIoMaxLength(mtu - 1));

        String error;
        while (null == (error = dstAeronArchive.pollForErrorResponse()))
        {
            Tests.yield();
        }

        assertThat(error, Matchers.containsString("mtuLength"));
        assertThat(error, Matchers.containsString("fileIoMaxLength"));
    }

    @ParameterizedTest
    @InterruptAfter(10)
    @CsvSource({
        "aeron:ipc?alias=src-recording|mtu=1344|init-term-id=777|term-id=1111112|term-offset=4096|" +
            "term-length=512K, aeron:udp?alias=OTHER|endpoint=localhost:8108|term-length=1G, 5, 1",
        "aeron:udp?alias=OTHER|endpoint=localhost:8108|term-length=1G, aeron:ipc?alias=dst-recording|mtu=1344|" +
            "init-term-id=1111111|term-id=1111112|term-offset=4096|term-length=512K, 3, 10",
        "aeron:udp?endpoint=localhost:8108|mtu=1344|init-term-id=11|term-id=15|term-offset=1024|term-length=512K, " +
            "aeron:udp?endpoint=localhost:8109|mtu=1376|init-term-id=222|term-id=333|term-offset=96|term-length=256M" +
            ", 7, 4",
        "aeron:ipc?alias=src, aeron:udp?alias=dst|endpoint=localhost:8080, 21, 21",
        "aeron:udp?alias=src|endpoint=localhost:8080|init-term-id=3|term-id=5|term-length=64K|term-offset=64, " +
            "aeron:ipc?alias=dst|init-term-id=11|term-id=13|term-length=64K|term-offset=2752, 42, 19"
    })
    public void shouldReplicateStoppedRecordingOverAnExistingTruncatedRecordingReplacingAllParameters(
        final String srcChannel, final String dstChannel, final int srcMessageCount, final int dstMessageCount)
    {
        final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(1);
        final int srcStreamId = 3333;
        final long srcRecordingId = createStoppedRecording(
            srcAeronArchive,
            srcRecordingSignalConsumer,
            srcChannel,
            srcStreamId,
            "src recording data", srcMessageCount);

        int dstStreamId = 555;
        long dstRecordingId;
        while (srcRecordingId >= (dstRecordingId = createStoppedRecording(
            dstAeronArchive, dstRecordingSignalConsumer, "aeron:ipc?term-length=64K", dstStreamId, "temp", 1)))
        {
            dstRecordingSignalConsumer.reset();
            dstAeronArchive.truncateRecording(dstRecordingId, 0);
            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, DELETE);
            dstStreamId++;
        }

        dstRecordingId = createStoppedRecording(
            dstAeronArchive, dstRecordingSignalConsumer, dstChannel, dstStreamId, "destination 42", dstMessageCount);
        assertNotEquals(srcRecordingId, dstRecordingId);
        assertNotEquals(srcStreamId, dstStreamId);

        assertEquals(1, srcAeronArchive.listRecording(srcRecordingId, collector.reset()));
        final RecordingDescriptor srcRecording = collector.descriptors().get(0).retain();
        assertEquals(1, dstAeronArchive.listRecording(dstRecordingId, collector.reset()));
        final RecordingDescriptor dstRecording = collector.descriptors().get(0).retain();
        assertNotEquals(srcRecording.startTimestamp(), dstRecording.startTimestamp());
        assertNotEquals(srcRecording.stopTimestamp(), dstRecording.stopTimestamp());
        assertNotEquals(srcRecording.controlSessionId(), dstRecording.controlSessionId());
        assertNotEquals(srcRecording.sessionId(), dstRecording.sessionId());
        assertNotEquals(srcRecording.streamId(), dstRecording.streamId());
        assertNotEquals(srcRecording.strippedChannel(), dstRecording.strippedChannel());
        assertNotEquals(srcRecording.originalChannel(), dstRecording.originalChannel());
        assertEquals(srcRecording.sourceIdentity(), dstRecording.sourceIdentity());

        dstRecordingSignalConsumer.reset();
        dstAeronArchive.truncateRecording(dstRecordingId, dstRecording.startPosition());
        awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, DELETE);

        dstRecordingSignalConsumer.reset();
        dstAeronArchive.replicate(
            srcRecordingId, dstRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);

        awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, EXTEND);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, SYNC);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, REPLICATE_END);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, STOP);

        assertEquals(1, dstAeronArchive.listRecording(dstRecordingId, collector.reset()));
        final RecordingDescriptor replicatedRecording = collector.descriptors().get(0).retain();
        assertEquals(srcRecording.startTimestamp(), replicatedRecording.startTimestamp());
        assertEquals(srcRecording.startPosition(), replicatedRecording.startPosition());
        assertEquals(srcRecording.stopPosition(), replicatedRecording.stopPosition());
        assertEquals(srcRecording.initialTermId(), replicatedRecording.initialTermId());
        assertEquals(srcRecording.segmentFileLength(), replicatedRecording.segmentFileLength());
        assertEquals(srcRecording.termBufferLength(), replicatedRecording.termBufferLength());
        assertEquals(srcRecording.mtuLength(), replicatedRecording.mtuLength());
        assertEquals(srcRecording.sessionId(), replicatedRecording.sessionId());
        assertEquals(srcRecording.streamId(), replicatedRecording.streamId());
        assertEquals(srcRecording.strippedChannel(), replicatedRecording.strippedChannel());
        assertEquals(srcRecording.originalChannel(), replicatedRecording.originalChannel());
        assertEquals(srcRecording.sourceIdentity(), replicatedRecording.sourceIdentity());
        assertEquals(dstRecording.controlSessionId(), replicatedRecording.controlSessionId());
        // extend recording will overwrite the stopTimestamp
        assertNotEquals(srcRecording.stopTimestamp(), replicatedRecording.stopTimestamp());
        assertNotEquals(dstRecording.stopTimestamp(), replicatedRecording.stopTimestamp());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldReplicateLiveRecordingAndMergeIntoADestinationIpcOnlyArchive(final boolean useParams)
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;

        CloseHelper.closeAll(dstAeronArchive, dstArchive, dstDriver);

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

        dstDriver = TestMediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName(generateRandomDirName())
                .termBufferSparseFile(true)
                .threadingMode(ThreadingMode.SHARED)
                .spiesSimulateConnection(true)
                .timerIntervalNs(TIMER_INTERVAL_NS)
                .dirDeleteOnStart(true),
            systemTestWatcher);
        systemTestWatcher.dataCollector().add(dstDriver.context().aeronDirectory());

        dstArchive = Archive.launch(new Archive.Context()
            .catalogCapacity(CATALOG_CAPACITY)
            .aeronDirectoryName(dstDriver.aeronDirectoryName())
            .controlChannel(null)
            .controlChannelEnabled(false)
            .localControlChannel(IPC_CHANNEL)
            .localControlStreamId(4444)
            .archiveClientContext(
                new AeronArchive.Context().controlResponseChannel("aeron:udp?endpoint=localhost:0"))
            .recordingEventsEnabled(false)
            .replicationChannel("aeron:udp?endpoint=localhost:0")
            .deleteArchiveOnStart(true)
            .archiveDir(new File(SystemUtil.tmpDirName(), "ipc-archive"))
            .fileSyncLevel(0)
            .threadingMode(ArchiveThreadingMode.SHARED));
        systemTestWatcher.dataCollector().add(dstArchive.context().archiveDir());

        dstAeronArchive = AeronArchive.connect(new AeronArchive.Context()
            .controlRequestChannel(dstArchive.context().localControlChannel())
            .controlRequestStreamId(dstArchive.context().localControlStreamId())
            .controlResponseChannel(IPC_CHANNEL)
            .controlResponseStreamId(5555)
            .idleStrategy(YieldingIdleStrategy.INSTANCE)
            .aeronDirectoryName(dstDriver.aeronDirectoryName()));

        dstRecordingSignalConsumer = injectRecordingSignalConsumer(dstAeronArchive);
        final Aeron srcAeron = srcAeronArchive.context().aeron();

        srcRecordingSignalConsumer.reset();
        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, START);
            final long signaledRecordingId = srcRecordingSignalConsumer.recordingId;
            final CountersReader srcCounters = srcAeron.countersReader();
            final int counterId =
                Tests.awaitRecordingCounterId(srcCounters, publication.sessionId(), srcAeronArchive.archiveId());
            srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);
            assertEquals(srcRecordingId, signaledRecordingId);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(srcCounters, counterId, publication.position());

            dstRecordingSignalConsumer.reset();
            if (useParams)
            {
                dstAeronArchive.replicate(
                    srcRecordingId,
                    SRC_CONTROL_STREAM_ID,
                    SRC_CONTROL_REQUEST_CHANNEL,
                    new ReplicationParams().liveDestination(LIVE_CHANNEL));
            }
            else
            {
                dstAeronArchive.replicate(
                    srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, LIVE_CHANNEL);
            }

            offer(publication, messageCount, messagePrefix);

            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, REPLICATE);
            final long dstRecordingId = dstRecordingSignalConsumer.recordingId;
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, EXTEND);
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, MERGE);

            final CountersReader dstCounters = dstArchive.context().aeron().countersReader();
            final int dstCounterId =
                RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId, dstArchive.context().archiveId());

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(dstCounters, dstCounterId, publication.position());
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, STOP);
    }

    @Test
    @SlowTest
    @InterruptAfter(30)
    void shouldHandleInvalidSrcEndpoint()
    {
        try
        {
            dstAeronArchive.replicate(
                NULL_VALUE,
                SRC_CONTROL_STREAM_ID,
                INVALID_SRC_CONTROL_REQUEST_CHANNEL,
                new ReplicationParams());

            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, REPLICATE);
        }
        catch (final ArchiveException ex)
        {
            assertEquals(ArchiveException.REPLICATION_CONNECTION_FAILURE, ex.errorCode());
            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, REPLICATE_END);

            return;
        }

        fail("expected archive exception");
    }

    @Test
    @InterruptAfter(10)
    void shouldReplicateWithAuthentication()
    {
        final Archive.Context authArchiveCtx = srcArchiveCtx
            .clone()
            .controlChannel("aeron:udp?endpoint=localhost:8098")
            .archiveDir(new File(SystemUtil.tmpDirName(), "auth-archive"))
            .authenticatorSupplier(new SampleAuthenticatorSupplier());
        final AeronArchive.Context authAeronArchiveCtx = srcAeronArchiveCtx
            .clone()
            .credentialsSupplier(TestCluster.SIMPLE_CREDENTIALS_SUPPLIER)
            .controlRequestChannel(authArchiveCtx.controlChannel());

        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;

        try (Archive archive = Archive.launch(authArchiveCtx);
            AeronArchive aeronArchive = AeronArchive.connect(authAeronArchiveCtx))
        {
            Objects.requireNonNull(archive);
            final TestRecordingSignalConsumer authSignalConsumer = injectRecordingSignalConsumer(aeronArchive);

            final long subscriptionId = aeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

            final Aeron srcAeron = aeronArchive.context().aeron();
            try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
            {
                final CountersReader counters = srcAeron.countersReader();
                final int counterId =
                    Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
                srcRecordingId = RecordingPos.getRecordingId(counters, counterId);

                offer(publication, messageCount, messagePrefix);
                Tests.awaitPosition(counters, counterId, publication.position());
            }

            authSignalConsumer.reset();
            aeronArchive.stopRecording(subscriptionId);
            awaitSignal(aeronArchive, authSignalConsumer, srcRecordingId, STOP);

            dstRecordingSignalConsumer.reset();
            final ReplicationParams replicationParams = new ReplicationParams()
                .encodedCredentials(TestCluster.SIMPLE_CREDENTIALS_SUPPLIER.encodedCredentials());
            dstAeronArchive.replicate(
                srcRecordingId,
                authAeronArchiveCtx.controlRequestStreamId(),
                authAeronArchiveCtx.controlRequestChannel(),
                replicationParams);

            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, REPLICATE);
            final long dstRecordingId = dstRecordingSignalConsumer.recordingId;
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, EXTEND);
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, REPLICATE_END);
            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, STOP);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldFailReplicationWithUsefulErrorWithChallengeResponseAuthentication()
    {
        final Archive.Context authArchiveCtx = srcArchiveCtx
            .clone()
            .controlChannel("aeron:udp?endpoint=localhost:8098")
            .archiveDir(new File(SystemUtil.tmpDirName(), "auth-archive"))
            .authenticatorSupplier(new SampleAuthenticatorSupplier());
        final AeronArchive.Context authAeronArchiveCtx = srcAeronArchiveCtx
            .clone()
            .credentialsSupplier(TestCluster.CHALLENGE_RESPONSE_CREDENTIALS_SUPPLIER)
            .controlRequestChannel(authArchiveCtx.controlChannel());

        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;

        try (Archive archive = Archive.launch(authArchiveCtx);
            AeronArchive aeronArchive = AeronArchive.connect(authAeronArchiveCtx))
        {
            Objects.requireNonNull(archive);
            final TestRecordingSignalConsumer authSignalConsumer = injectRecordingSignalConsumer(aeronArchive);

            final long subscriptionId = aeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

            final Aeron srcAeron = aeronArchive.context().aeron();
            try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
            {
                final CountersReader counters = srcAeron.countersReader();
                final int counterId =
                    Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
                srcRecordingId = RecordingPos.getRecordingId(counters, counterId);

                offer(publication, messageCount, messagePrefix);
                Tests.awaitPosition(counters, counterId, publication.position());
            }

            authSignalConsumer.reset();
            aeronArchive.stopRecording(subscriptionId);
            awaitSignal(aeronArchive, authSignalConsumer, srcRecordingId, STOP);

            dstRecordingSignalConsumer.reset();
            final ReplicationParams replicationParams = new ReplicationParams()
                .encodedCredentials(TestCluster.CHALLENGE_RESPONSE_CREDENTIALS_SUPPLIER.encodedCredentials());
            dstAeronArchive.replicate(
                srcRecordingId,
                authAeronArchiveCtx.controlRequestStreamId(),
                authAeronArchiveCtx.controlRequestChannel(),
                replicationParams);

            final ArchiveException archiveException = assertThrows(
                ArchiveException.class,
                () -> awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, REPLICATE));
            assertThat(
                archiveException.getMessage(),
                containsString("Replication does not support challenge/response authentication"));
        }
    }

    private void readRecordingIntoBuffer(final long srcRecordingId, final ExpandableArrayBuffer srcRecordingData)
    {
        final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(10);
        final int i = srcAeronArchive.listRecording(srcRecordingId, collector.reset());
        assertEquals(1, i);
        final RecordingDescriptor descriptor = collector.descriptors().get(0);
        final long length = descriptor.stopPosition() - descriptor.startPosition();

        try (Subscription replay = srcAeronArchive.replay(
            srcRecordingId, descriptor.startPosition(), length, REPLAY_CHANNEL, REPLAY_STREAM_ID))
        {
            final MutableInteger position = new MutableInteger(0);
            Tests.awaitConnected(replay);

            // Assumes session specific subscription used for replay.
            final Image image = replay.imageAtIndex(0);
            final FragmentHandler fragmentHandler =
                (buffer, offset, len, header) ->
                {
                    srcRecordingData.putBytes(position.get(), buffer, offset, len);
                    position.addAndGet(len);
                };

            while (!image.isEndOfStream())
            {
                image.poll(fragmentHandler, 10);
            }
        }
    }

    private long createStoppedRecording(
        final AeronArchive aeronArchive,
        final TestRecordingSignalConsumer recordingSignalConsumer,
        final String channel,
        final int streamId,
        final String payload,
        final int messageCount)
    {
        try (ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(channel, streamId))
        {
            long recordingId = Long.MIN_VALUE;
            try
            {
                final CountersReader counters = aeronArchive.context().aeron().countersReader();
                final int counterId =
                    Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
                recordingId = RecordingPos.getRecordingId(counters, counterId);

                offer(publication, messageCount, payload);
                Tests.awaitPosition(counters, counterId, publication.position());
                return recordingId;
            }
            finally
            {
                recordingSignalConsumer.reset();
                aeronArchive.stopRecording(publication);
                awaitSignal(aeronArchive, recordingSignalConsumer, recordingId, STOP);
            }
        }
    }

    private void validateRecordingAreEqual(final long srcRecordingId, final long dstRecordingId)
    {
        final ExpandableArrayBuffer srcRecordingData = new ExpandableArrayBuffer();
        readRecordingIntoBuffer(srcRecordingId, srcRecordingData);

        final ExpandableArrayBuffer dstRecordingData = new ExpandableArrayBuffer();
        readRecordingIntoBuffer(dstRecordingId, dstRecordingData);

        assertEquals(srcRecordingData, dstRecordingData);
    }
}
