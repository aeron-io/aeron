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
package io.aeron.samples.archive;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ReplayMerge;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.samples.SampleConfiguration;
import io.aeron.samples.SamplesUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.util.concurrent.atomic.AtomicBoolean;
import static io.aeron.samples.SamplesUtil.findLatestRecording;

/**
 * This is an Aeron subscriber utilising {@link io.aeron.archive.client.ReplayMerge}.
 * <p>
 * The application uses {@code ReplayMerge} to replay from a recording, before joining the live stream.
 * It uses the default channel for live, and the default stream ID for both live and replay.
 * These defaults can be overwritten by setting their corresponding Java system properties
 * at the command line, for example:
 * export JVM_OPTS="-Daeron.sample.channel=aeron:udp?endpoint=localhost:5555 -Daeron.sample.streamId=20"
 */
public class ReplayMergeSubscriber
{
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final String LIVE_DESTINATION = SampleConfiguration.CHANNEL;
    private static final String REPLAY_DESTINATION = "aeron:udp?endpoint=localhost:0";
    private static final String MDS_CHANNEL = "aeron:udp?control-mode=manual";
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final boolean EMBEDDED_MEDIA_DRIVER = SampleConfiguration.EMBEDDED_MEDIA_DRIVER;

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    @SuppressWarnings("try")
    public static void main(final String[] args)
    {
        System.out.println("Subscribing to live " + LIVE_DESTINATION + ", and replay " + REPLAY_DESTINATION +
            " on stream id " + STREAM_ID);

        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicBoolean isLive = new AtomicBoolean(false);
        final IdleStrategy idleStrategy = new YieldingIdleStrategy();
        final FragmentHandler fragmentHandler = new FragmentAssembler(
            (final DirectBuffer buffer, final int offset, final int length, final Header header) ->
            {
                final String msg = buffer.getStringWithoutLengthAscii(offset, length);
                final String streamState = isLive.get() ? "live" : "replay";

                System.out.printf("Message to %s stream %d from session %d (%d@%d) <<%s>>%n",
                    streamState, STREAM_ID, header.sessionId(), length, offset, msg);
            });

        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier(() -> running.set(false));
            MediaDriver driver = EMBEDDED_MEDIA_DRIVER ?
                MediaDriver.launchEmbedded(new MediaDriver.Context().terminationHook(barrier::signalAll)) : null)
        {
            final Aeron.Context ctx = new Aeron.Context()
                .availableImageHandler(SamplesUtil::printAvailableImage)
                .unavailableImageHandler(SamplesUtil::printUnavailableImage);

            if (EMBEDDED_MEDIA_DRIVER)
            {
                ctx.aeronDirectoryName(driver.aeronDirectoryName());
            }

            final AeronArchive.Context aeronArchiveCtx = new AeronArchive.Context();

            // Create Aeron and AeronArchive instances using the configured Context.
            try (Aeron aeron = Aeron.connect(ctx);
                AeronArchive aeronArchive = AeronArchive.connect(aeronArchiveCtx.aeron(aeron)))
            {
                if (null == aeronArchive)
                {
                    System.out.println("Could not connect to aeron archive.");
                    return;
                }

                final RecordingDescriptor descriptor = findLatestRecording(aeronArchive, LIVE_DESTINATION, STREAM_ID);

                if (descriptor == null)
                {
                    System.out.println("No recordings found for channel " + LIVE_DESTINATION);
                    return;
                }
                final String replayChannel = CommonContext.UDP_CHANNEL + "?session-id=" + descriptor.sessionId();
                final String subscriptionChannel = MDS_CHANNEL + "|session-id=" + descriptor.sessionId();

                // Create a Multi-Destination Subscription on the Aeron instance for the ReplayMerge instance, then
                // create a ReplayMerge instance.
                try (Subscription subscription = aeron.addSubscription(subscriptionChannel, STREAM_ID);
                    ReplayMerge replayMerge = new ReplayMerge(
                        subscription,
                        aeronArchive,
                        replayChannel,
                        REPLAY_DESTINATION,
                        LIVE_DESTINATION,
                        descriptor.recordingId(),
                        descriptor.startPosition()))
                {
                    while (running.get())
                    {
                        if (replayMerge.hasFailed())
                        {
                            throw new IllegalStateException("ReplayMerge has failed, " + replayMerge);
                        }

                        if (replayMerge.isMerged() && !isLive.get())
                        {
                            System.out.println("===========");
                            System.out.println("ReplayMerge has joined live stream.");
                            System.out.println("===========");
                            isLive.set(true);
                        }

                        final int fragments = replayMerge.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);

                        idleStrategy.idle(fragments);
                    }
                    System.out.println("Shutting down...");
                }
            }
        }
    }
}
