/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.PersistentSubscription;
import io.aeron.archive.client.PersistentSubscriptionListener;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.samples.SampleConfiguration;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.archive.client.PersistentSubscription.FROM_START;
import static io.aeron.samples.SampleConfiguration.FRAGMENT_COUNT_LIMIT;
import static io.aeron.samples.SamplesUtil.findLatestRecording;

/**
 * This is an Aeron subscriber utilising {@link io.aeron.archive.client.PersistentSubscription}.
 * <p>
 * The application uses {@code PersistentSubscription} to replay messages from a recording before joining the live
 * stream on startup and whenever it disconnects.
 * <p>
 * The default values for configuration options are defined in {@link SampleConfiguration} or here, and can be
 * overridden by setting their corresponding properties via the command-line; e.g.:
 * {@code -Daeron.sample.channel=aeron:udp?endpoint=localhost:5555 -Daeron.sample.streamId=20}
 * <p>
 * For details on how to use this sample, see the Persistent Subscription section in
 * {@code aeron-samples/scripts/archive/README.md}.
 */
public class PersistentSubscriber
{
    private static final String LIVE_CHANNEL = SampleConfiguration.CHANNEL;
    private static final int LIVE_STREAM_ID = SampleConfiguration.STREAM_ID;

    private static final String REPLAY_CHANNEL =
        System.getProperty("aeron.sample.replay.channel", "aeron:udp?endpoint=localhost:0");
    private static final int REPLAY_STREAM_ID = Integer.getInteger("aeron.sample.replay.streamId", 5000);

    private static final long START_POSITION = Long.getLong("aeron.sample.startPosition", FROM_START);

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    @SuppressWarnings("try")
    public static void main(final String[] args)
    {
        System.out.println("Subscribing to live on channel " + LIVE_CHANNEL + " and stream id " + LIVE_STREAM_ID +
                           ", to replays on channel " + REPLAY_CHANNEL + " and stream id " + REPLAY_STREAM_ID +
                           ", starting from position " + START_POSITION);

        final AtomicBoolean running = new AtomicBoolean(true);
        final IdleStrategy idleStrategy = new YieldingIdleStrategy();
        final SamplePersistentSubscriptionListener listener = new SamplePersistentSubscriptionListener();
        final FragmentHandler fragmentHandler =
            (final DirectBuffer buffer, final int offset, final int length, final Header header) ->
            {
                final String msg = buffer.getStringWithoutLengthAscii(offset, length);
                final String streamState = listener.isLive() ? "live" : "replay";

                System.out.printf("Message to %s stream %d from session %d of length %d at position %d <<%s>>%n",
                    streamState, header.streamId(), header.sessionId(), length, header.position(), msg);
            };

        final AeronArchive.Context archiveCtx = new AeronArchive.Context();

        try (ShutdownSignalBarrier ignore = new ShutdownSignalBarrier(() -> running.set(false));
            AeronArchive aeronArchive = AeronArchive.connect(archiveCtx.clone()))
        {
            final RecordingDescriptor descriptor = findLatestRecording(aeronArchive, LIVE_CHANNEL, LIVE_STREAM_ID);

            if (null == descriptor)
            {
                System.out.println("No recordings found for channel " + LIVE_CHANNEL +
                                   " and stream id " + LIVE_STREAM_ID);
                return;
            }

            final PersistentSubscription.Context ctx = new PersistentSubscription.Context()
                .aeron(aeronArchive.context().aeron())
                .aeronArchiveContext(archiveCtx.clone())
                .listener(listener)
                .startPosition(START_POSITION)
                .recordingId(descriptor.recordingId())
                .liveChannel(LIVE_CHANNEL)
                .liveStreamId(LIVE_STREAM_ID)
                .replayChannel(REPLAY_CHANNEL)
                .replayStreamId(REPLAY_STREAM_ID);

            try (PersistentSubscription subscription = PersistentSubscription.create(ctx))
            {
                while (running.get())
                {
                    final int workCount = subscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);

                    if (subscription.hasFailed())
                    {
                        break;
                    }

                    idleStrategy.idle(workCount);
                }

                System.out.println("Shutting down...");
            }
        }
    }

    private static final class SamplePersistentSubscriptionListener implements PersistentSubscriptionListener
    {
        private boolean live;

        public void onLiveJoined()
        {
            live = true;

            System.out.println("===========");
            System.out.println("PersistentSubscription has joined live stream.");
            System.out.println("===========");
        }

        public void onLiveLeft()
        {
            live = false;

            System.out.println("===========");
            System.out.println("PersistentSubscription has left live stream.");
            System.out.println("===========");
        }

        public void onError(final Exception e)
        {
            System.err.println("Persistent subscription error:");
            e.printStackTrace(System.err);
        }

        public boolean isLive()
        {
            return live;
        }
    }
}
