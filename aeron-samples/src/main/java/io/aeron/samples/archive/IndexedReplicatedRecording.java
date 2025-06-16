/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.samples.archive;

import io.aeron.*;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.SystemUtil;
import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.nio.ByteOrder;
import java.util.Random;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.getAeronDirectoryName;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Example of how to create a recorded stream that is replicated and indexed. Indexers run in parallel to the recording
 * for greater throughput and lower latency.
 * <p>
 * The index allows for the lookup of message start position in a recording based on message index plus a basic
 * time series for when a message was published.
 * <p>
 * The secondary (destination) archive launches after the primary (source) archive and replicates from the sources
 * the history of the stream it missed then when it catches up to live it will merge with the live stream and stop
 * the replay replication from the source.
 */
public class IndexedReplicatedRecording implements AutoCloseable
{
    static final int MESSAGE_INDEX_OFFSET = 0;
    static final int TIMESTAMP_OFFSET = SIZE_OF_LONG;
    static final int HEADER_LENGTH = TIMESTAMP_OFFSET + SIZE_OF_LONG;
    static final int MESSAGE_BURST_COUNT = 10_000;

    private static final int TERM_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final long CATALOG_CAPACITY = 64 * 1024;
    private static final int SRC_CONTROL_STREAM_ID = AeronArchive.Configuration.CONTROL_STREAM_ID_DEFAULT;
    private static final String SRC_CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8090";
    private static final String SRC_CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final String DST_CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8095";
    private static final String DST_CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final String SRC_REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final String DST_REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";

    private static final int LIVE_STREAM_ID = 1033;
    private static final String LIVE_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .controlMode(CommonContext.MDC_CONTROL_MODE_DYNAMIC)
        .controlEndpoint("localhost:8100")
        .termLength(TERM_LENGTH)
        .build();

    private static final int INDEX_STREAM_ID = 1097;
    private static final String INDEX_CHANNEL = new ChannelUriStringBuilder()
        .media("ipc")
        .termLength(TERM_LENGTH)
        .build();

    private final ArchivingMediaDriver srcArchivingMediaDriver;
    private final ArchivingMediaDriver dstArchivingMediaDriver;
    private final Aeron srcAeron;
    private final Aeron dstAeron;
    private final AeronArchive srcAeronArchive;
    private final AeronArchive dstAeronArchive;

    IndexedReplicatedRecording()
    {
        final String srcAeronDirectoryName = getAeronDirectoryName() + "-src";
        System.out.println("srcAeronDirectoryName=" + srcAeronDirectoryName);
        final String dstAeronDirectoryName = getAeronDirectoryName() + "-dst";
        System.out.println("dstAeronDirectoryName=" + dstAeronDirectoryName);

        final File srcArchiveDir = new File(SystemUtil.tmpDirName(), "src-archive");
        System.out.println("srcArchiveDir=" + srcArchiveDir);
        srcArchivingMediaDriver = ArchivingMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(srcAeronDirectoryName)
                .termBufferSparseFile(true)
                .threadingMode(ThreadingMode.SHARED)
                .errorHandler(Throwable::printStackTrace)
                .spiesSimulateConnection(true)
                .dirDeleteOnShutdown(true)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .catalogCapacity(CATALOG_CAPACITY)
                .controlChannel(SRC_CONTROL_REQUEST_CHANNEL)
                .archiveClientContext(new AeronArchive.Context().controlResponseChannel(SRC_CONTROL_RESPONSE_CHANNEL))
                .replicationChannel(SRC_REPLICATION_CHANNEL)
                .deleteArchiveOnStart(true)
                .archiveDir(srcArchiveDir)
                .fileSyncLevel(0)
                .threadingMode(ArchiveThreadingMode.SHARED));

        final File dstArchiveDir = new File(SystemUtil.tmpDirName(), "dst-archive");
        System.out.println("dstArchiveDir=" + dstArchiveDir);
        dstArchivingMediaDriver = ArchivingMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(dstAeronDirectoryName)
                .termBufferSparseFile(true)
                .threadingMode(ThreadingMode.SHARED)
                .errorHandler(Throwable::printStackTrace)
                .spiesSimulateConnection(true)
                .dirDeleteOnShutdown(true)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .catalogCapacity(CATALOG_CAPACITY)
                .controlChannel(DST_CONTROL_REQUEST_CHANNEL)
                .archiveClientContext(new AeronArchive.Context().controlResponseChannel(DST_CONTROL_RESPONSE_CHANNEL))
                .replicationChannel(DST_REPLICATION_CHANNEL)
                .deleteArchiveOnStart(true)
                .archiveDir(dstArchiveDir)
                .fileSyncLevel(0)
                .threadingMode(ArchiveThreadingMode.SHARED));

        srcAeron = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(srcAeronDirectoryName));

        dstAeron = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(dstAeronDirectoryName));

        srcAeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .idleStrategy(YieldingIdleStrategy.INSTANCE)
                .controlRequestChannel(SRC_CONTROL_REQUEST_CHANNEL)
                .controlResponseChannel(SRC_CONTROL_RESPONSE_CHANNEL)
                .aeron(srcAeron));

        dstAeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .idleStrategy(YieldingIdleStrategy.INSTANCE)
                .controlRequestChannel(DST_CONTROL_REQUEST_CHANNEL)
                .controlResponseChannel(DST_CONTROL_RESPONSE_CHANNEL)
                .aeron(dstAeron));
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.closeAll(
            srcAeronArchive,
            dstAeronArchive,
            srcAeron,
            dstAeron,
            srcArchivingMediaDriver,
            dstArchivingMediaDriver);

        srcArchivingMediaDriver.archive().context().deleteDirectory();
        dstArchivingMediaDriver.archive().context().deleteDirectory();
    }

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     * @throws InterruptedException if the thread is interrupted.
     */
    public static void main(final String[] args) throws InterruptedException
    {
        try (IndexedReplicatedRecording test = new IndexedReplicatedRecording())
        {
            final Publication publication = test.srcAeron.addExclusivePublication(LIVE_CHANNEL, LIVE_STREAM_ID);
            final String sessionSpecificLiveChannel = LIVE_CHANNEL + "|session-id=" + publication.sessionId();
            final Sequencer sequencer = new Sequencer(MESSAGE_BURST_COUNT, publication);

            final Indexer primaryIndexer = new Indexer(
                test.srcAeron.addSubscription(CommonContext.SPY_PREFIX + sessionSpecificLiveChannel, LIVE_STREAM_ID),
                test.srcAeron.addExclusivePublication(INDEX_CHANNEL, INDEX_STREAM_ID),
                publication.sessionId());
            test.srcAeronArchive.startRecording(INDEX_CHANNEL, INDEX_STREAM_ID, SourceLocation.LOCAL, true);
            final Thread primaryIndexerThread = Indexer.start(primaryIndexer);

            final long srcRecordingSubscriptionId = test.srcAeronArchive.startRecording(
                sessionSpecificLiveChannel, LIVE_STREAM_ID, SourceLocation.LOCAL, true);
            final CountersReader srcCounters = test.srcAeron.countersReader();
            final int srcCounterId =
                awaitRecordingCounterId(srcCounters, publication.sessionId(), test.srcAeronArchive.archiveId());
            final long srcRecordingId = RecordingPos.getRecordingId(srcCounters, srcCounterId);

            sequencer.sendBurst();

            final long channelTagId = test.dstAeron.nextCorrelationId();
            final long subscriptionTagId = test.dstAeron.nextCorrelationId();
            final String taggedChannel =
                "aeron:udp?control-mode=manual|rejoin=false|tags=" + channelTagId + "," + subscriptionTagId;

            final Indexer secondaryIndexer = new Indexer(
                test.dstAeron.addSubscription(taggedChannel, LIVE_STREAM_ID),
                test.dstAeron.addExclusivePublication(INDEX_CHANNEL, INDEX_STREAM_ID),
                publication.sessionId());
            test.dstAeronArchive.startRecording(INDEX_CHANNEL, INDEX_STREAM_ID, SourceLocation.LOCAL, true);
            final Thread secondaryIndexerThread = Indexer.start(secondaryIndexer);

            final long replicationId = test.dstAeronArchive.taggedReplicate(
                srcRecordingId,
                NULL_VALUE,
                channelTagId,
                subscriptionTagId,
                SRC_CONTROL_STREAM_ID,
                SRC_CONTROL_REQUEST_CHANNEL,
                LIVE_CHANNEL);

            sequencer.sendBurst();
            sequencer.sendBurst();

            final long position = publication.position();
            awaitPosition(srcCounters, srcCounterId, position);
            primaryIndexer.awaitPosition(position);

            final CountersReader dstCounters = test.dstAeron.countersReader();
            final int dstCounterId =
                awaitRecordingCounterId(dstCounters, publication.sessionId(), test.srcAeronArchive.archiveId());
            awaitPosition(dstCounters, dstCounterId, position);
            secondaryIndexer.awaitPosition(position);

            primaryIndexerThread.interrupt();
            primaryIndexerThread.join();
            primaryIndexer.close();

            secondaryIndexerThread.interrupt();
            secondaryIndexerThread.join();
            secondaryIndexer.close();

            test.dstAeronArchive.stopReplication(replicationId);
            test.srcAeronArchive.stopRecording(srcRecordingSubscriptionId);

            assertEquals("index", sequencer.nextMessageIndex(), primaryIndexer.nextMessageIndex());
            assertEquals("index", sequencer.nextMessageIndex(), secondaryIndexer.nextMessageIndex());

            assertEquals("positions", primaryIndexer.messagePositions(), secondaryIndexer.messagePositions());
            assertEquals("timestamps", primaryIndexer.timestamps(), secondaryIndexer.timestamps());
            assertEquals(
                "timestamp positions", primaryIndexer.timestampPositions(), secondaryIndexer.timestampPositions());
        }
    }

    static int awaitRecordingCounterId(final CountersReader counters, final int sessionId, final long archiveId)
        throws InterruptedException
    {
        int counterId;
        while (NULL_VALUE == (counterId = RecordingPos.findCounterIdBySession(counters, sessionId, archiveId)))
        {
            Thread.yield();
            if (Thread.interrupted())
            {
                throw new InterruptedException();
            }
        }

        return counterId;
    }

    static void awaitPosition(final CountersReader counters, final int counterId, final long position)
        throws InterruptedException
    {
        while (counters.getCounterValue(counterId) < position)
        {
            if (counters.getCounterState(counterId) != CountersReader.RECORD_ALLOCATED)
            {
                throw new IllegalStateException("count not active: " + counterId);
            }

            Thread.yield();
            if (Thread.interrupted())
            {
                throw new InterruptedException();
            }
        }
    }

    static void assertEquals(final String type, final long srcValue, final long dstValue)
    {
        if (srcValue != dstValue)
        {
            throw new IllegalStateException(type + " not equal: srcValue=" + srcValue + " dstValue=" + dstValue);
        }
    }

    static void assertEquals(final String type, final LongArrayList srcList, final LongArrayList dstList)
    {
        final int srcSize = srcList.size();
        final int dstSize = dstList.size();
        if (srcSize != dstSize)
        {
            throw new IllegalStateException(type + " not equal: srcList.size=" + srcSize + " dstList.size=" + dstSize);
        }

        for (int i = 0; i < srcSize; i++)
        {
            final long srcVal = srcList.getLong(i);
            final long dstVal = srcList.getLong(i);
            if (srcVal != dstVal)
            {
                throw new IllegalStateException(
                    type + " [" + i + "] not equal: srcVal=" + srcVal + " dstVal=" + dstVal);
            }
        }
    }

    static class Sequencer implements AutoCloseable
    {
        private static final int MAX_MESSAGE_LENGTH = 1000;

        private long nextMessageIndex;
        private final int burstLength;
        private final Publication publication;
        private final Random random = new Random();
        private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[HEADER_LENGTH + MAX_MESSAGE_LENGTH]);

        Sequencer(final int burstLength, final Publication publication)
        {
            this.burstLength = burstLength;
            this.publication = publication;
            buffer.setMemory(HEADER_LENGTH, MAX_MESSAGE_LENGTH, (byte)'X');
        }

        public void close()
        {
            CloseHelper.close(publication);
        }

        long nextMessageIndex()
        {
            return nextMessageIndex;
        }

        void sendBurst() throws InterruptedException
        {
            for (int i = 0; i < burstLength; i++)
            {
                appendMessage();
            }
        }

        private void appendMessage() throws InterruptedException
        {
            final int variableLength = random.nextInt(MAX_MESSAGE_LENGTH);
            buffer.putLong(MESSAGE_INDEX_OFFSET, nextMessageIndex, ByteOrder.LITTLE_ENDIAN);
            buffer.putLong(TIMESTAMP_OFFSET, System.currentTimeMillis(), ByteOrder.LITTLE_ENDIAN);

            while (publication.offer(buffer, 0, HEADER_LENGTH + variableLength) < 0)
            {
                Thread.yield();
                if (Thread.interrupted())
                {
                    throw new InterruptedException();
                }
            }

            ++nextMessageIndex;
        }
    }

    static class Indexer implements AutoCloseable, Runnable, ControlledFragmentHandler
    {
        private static final int FRAGMENT_LIMIT = 10;
        private static final int INDEX_BUFFER_CAPACITY = 1024;
        private static final int BATCH_SIZE = 126; // Fits 1k payload - message index, timestamp, positions...

        private final int sessionId;
        private int nextMessageIndex = 0;
        private int batchIndex = 0;
        private long lastMessagePosition = Aeron.NULL_VALUE;
        private final Subscription subscription;
        private final Publication publication;
        private Image image;
        private final LongArrayList messagePositions = new LongArrayList();
        private final LongArrayList timestamps = new LongArrayList();
        private final LongArrayList timestampPositions = new LongArrayList();
        private final UnsafeBuffer indexBuffer = new UnsafeBuffer(new byte[INDEX_BUFFER_CAPACITY]);

        static Thread start(final Indexer indexer)
        {
            final Thread thread = new Thread(indexer);
            thread.setName("indexer");
            thread.setDaemon(true);
            thread.start();

            return thread;
        }

        Indexer(final Subscription subscription, final Publication publication, final int sessionId)
        {
            this.subscription = subscription;
            this.publication = publication;
            this.sessionId = sessionId;
        }

        public void close()
        {
            CloseHelper.close(subscription);
        }

        long position()
        {
            if (null == image)
            {
                return Aeron.NULL_VALUE;
            }

            return image.position();
        }

        void awaitPosition(final long position)
        {
            while (position() < position)
            {
                Thread.yield();
            }
        }

        long nextMessageIndex()
        {
            return nextMessageIndex;
        }

        LongArrayList messagePositions()
        {
            return messagePositions;
        }

        LongArrayList timestamps()
        {
            return timestamps;
        }

        LongArrayList timestampPositions()
        {
            return timestampPositions;
        }

        public void run()
        {
            while (!subscription.isConnected() || !publication.isConnected())
            {
                try
                {
                    Thread.sleep(1);
                }
                catch (final InterruptedException ignore)
                {
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            final Image image = subscription.imageBySessionId(sessionId);
            this.image = image;
            if (null == image)
            {
                throw new IllegalStateException("session not found");
            }

            lastMessagePosition = image.joinPosition();

            final IdleStrategy idleStrategy = YieldingIdleStrategy.INSTANCE;
            while (true)
            {
                final int fragments = image.controlledPoll(this, FRAGMENT_LIMIT);
                if (0 == fragments)
                {
                    if (Thread.interrupted() || image.isClosed())
                    {
                        return;
                    }
                }

                idleStrategy.idle(fragments);
            }
        }

        public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            final long currentPosition = lastMessagePosition;
            final long index = buffer.getLong(offset + MESSAGE_INDEX_OFFSET, ByteOrder.LITTLE_ENDIAN);
            if (index != nextMessageIndex)
            {
                throw new IllegalStateException("invalid index: expected=" + nextMessageIndex + " actual=" + index);
            }

            if (0 == batchIndex)
            {
                final long timestamp = buffer.getLong(offset + TIMESTAMP_OFFSET, ByteOrder.LITTLE_ENDIAN);
                timestamps.addLong(timestamp);
                timestampPositions.addLong(currentPosition);
                indexBuffer.putLong(MESSAGE_INDEX_OFFSET, nextMessageIndex, ByteOrder.LITTLE_ENDIAN);
                indexBuffer.putLong(TIMESTAMP_OFFSET, timestamp, ByteOrder.LITTLE_ENDIAN);
            }

            final int positionOffset = HEADER_LENGTH + (batchIndex * SIZE_OF_LONG);
            indexBuffer.putLong(positionOffset, currentPosition, ByteOrder.LITTLE_ENDIAN);

            if (++batchIndex >= BATCH_SIZE)
            {
                if (publication.offer(indexBuffer, 0, INDEX_BUFFER_CAPACITY) <= 0)
                {
                    --batchIndex;
                    return Action.ABORT;
                }

                batchIndex = 0;
            }

            messagePositions.addLong(currentPosition);
            lastMessagePosition = header.position();
            ++nextMessageIndex;

            return Action.CONTINUE;
        }
    }
}
