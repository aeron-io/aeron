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
import static io.aeron.samples.SamplesUtil.findLatestRecording;

/**
 *  This is an Aeron subscriber utilising {@link io.aeron.archive.client.PersistentSubscription}.
 *  <p>
 *  The application uses {@code PersistentSubscription} to replay messages from a recording before joining the live
 *  stream.
 * The default values for channel and stream ID are defined in {@link SampleConfiguration} and can be
 * overridden by setting their corresponding properties via the command-line; e.g.:
 * {@code -Daeron.sample.channel=aeron:udp?endpoint=localhost:5555 -Daeron.sample.streamId=20}
 *
 * For details on how to use this sample, see the Persistent Subscription section in the Archive samples README.
 */
public class PersistentSubscriber
{
    private static final int LIVE_STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final String LIVE_CHANNEL = SampleConfiguration.CHANNEL;

    private static final String REPLAY_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final int REPLAY_STREAM_ID = SampleConfiguration.STREAM_ID + 1;

    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    @SuppressWarnings("try")
    public static void main(final String[] args)
    {
        System.out.println("Subscribing to live " + LIVE_CHANNEL + " on stream id " + LIVE_STREAM_ID +
            ", and replay " + REPLAY_CHANNEL + " on stream id " + REPLAY_STREAM_ID);

        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicBoolean isLive = new AtomicBoolean(false);
        final IdleStrategy idleStrategy = new YieldingIdleStrategy();
        final FragmentHandler fragmentHandler = (
            (final DirectBuffer buffer, final int offset, final int length, final Header header) ->
            {
                final String msg = buffer.getStringWithoutLengthAscii(offset, length);
                final String streamState = isLive.get() ? "live" : "replay";

                System.out.printf("Message to %s stream %d from session %d (%d@%d) <<%s>>%n",
                    streamState, LIVE_STREAM_ID, header.sessionId(), length, offset, msg);
            });

        final AeronArchive.Context archiveCtx = new AeronArchive.Context()
            .controlResponseStreamId(AeronArchive.Configuration.controlResponseStreamId() + 2);

        try (ShutdownSignalBarrier ignore = new ShutdownSignalBarrier(() -> running.set(false));
            AeronArchive aeronArchive = AeronArchive.connect(archiveCtx.clone()))
        {
            if (null == aeronArchive)
            {
                System.out.println("Could not connect to aeron archive.");
                return;
            }

            final RecordingDescriptor descriptor = findLatestRecording(aeronArchive, LIVE_CHANNEL, LIVE_STREAM_ID);

            if (null == descriptor)
            {
                System.out.println("No recordings found for channel " + LIVE_CHANNEL);
                return;
            }

            final PersistentSubscription.Context ctx = new PersistentSubscription.Context()
                .aeron(aeronArchive.context().aeron())
                .aeronArchiveContext(archiveCtx.clone())
                .listener(new SamplePersistentSubscriptionListener())
                .startPosition(PersistentSubscription.FROM_START)
                .recordingId(descriptor.recordingId())
                .liveChannel(LIVE_CHANNEL)
                .liveStreamId(LIVE_STREAM_ID)
                .replayChannel(REPLAY_CHANNEL)
                .replayStreamId(REPLAY_STREAM_ID);

            try (PersistentSubscription subscription = PersistentSubscription.create(ctx))
            {
                while (running.get())
                {
                    if (subscription.hasFailed())
                    {
                        throw new IllegalStateException("PersistentSubscription has failed");
                    }

                    if (subscription.isReplaying())
                    {
                        isLive.set(false);
                    }

                    if (subscription.isLive())
                    {
                        isLive.set(true);
                    }

                    final int fragmentsRead = subscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);
                    idleStrategy.idle(fragmentsRead);
                }

                System.out.println("Shutting down...");
            }
        }
    }

    private static final class SamplePersistentSubscriptionListener implements PersistentSubscriptionListener
    {
        public void onLiveJoined()
        {
            System.out.println("===========");
            System.out.println("PersistentSubscription has joined live stream.");
            System.out.println("===========");
        }

        public void onLiveLeft()
        {
            System.out.println("===========");
            System.out.println("PersistentSubscription has left live stream.");
            System.out.println("===========");
        }

        public void onError(final Exception e)
        {
            System.err.println("Persistent subscription error: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
}