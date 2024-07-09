/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron.samples;

import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.protocol.HeaderFlyweight;
import org.agrona.concurrent.IdleStrategy;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Utility functions for the samples.
 */
public class SamplesUtil
{
    /**
     * Return a reusable, parametrised event loop that calls a default {@link IdleStrategy} when no messages
     * are received.
     *
     * @param fragmentHandler to be called back for each message.
     * @param limit           passed to {@link Subscription#poll(FragmentHandler, int)}.
     * @param running         indication for loop.
     * @return loop function.
     */
    public static Consumer<Subscription> subscriberLoop(
        final FragmentHandler fragmentHandler, final int limit, final AtomicBoolean running)
    {
        return subscriberLoop(fragmentHandler, limit, running, SampleConfiguration.newIdleStrategy());
    }

    /**
     * Return a reusable, parametrised event loop that calls and idler when no messages are received.
     *
     * @param fragmentHandler to be called back for each message.
     * @param limit           passed to {@link Subscription#poll(FragmentHandler, int)}.
     * @param running         indication for loop.
     * @param idleStrategy    to use for loop.
     * @return loop function.
     */
    public static Consumer<Subscription> subscriberLoop(
        final FragmentHandler fragmentHandler,
        final int limit,
        final AtomicBoolean running,
        final IdleStrategy idleStrategy)
    {
        return
            (subscription) ->
            {
                final FragmentAssembler assembler = new FragmentAssembler(fragmentHandler);
                while (running.get())
                {
                    final int fragmentsRead = subscription.poll(assembler, limit);
                    idleStrategy.idle(fragmentsRead);
                }
            };
    }

    /**
     * Return a reusable, parametrised {@link FragmentHandler} that prints to stdout.
     *
     * @param streamId to show when printing.
     * @return subscription data handler function that prints the message contents.
     */
    public static FragmentHandler printAsciiMessage(final int streamId)
    {
        return (buffer, offset, length, header) ->
        {
            final String msg = buffer.getStringWithoutLengthAscii(offset, length);
            System.out.printf(
                "Message to stream %d from session %d (%d@%d) <<%s>>%n",
                streamId, header.sessionId(), length, offset, msg);
        };
    }

    /**
     * Return a reusable, parametrised {@link FragmentHandler} that calls into a
     * {@link RateReporter}.
     *
     * @param reporter for the rate.
     * @return {@link FragmentHandler} that records the rate information.
     */
    public static FragmentHandler rateReporterHandler(final RateReporter reporter)
    {
        return (buffer, offset, length, header) -> reporter.onMessage(length);
    }

    /**
     * Generic error handler that just prints message to stdout.
     *
     * @param channel   for the error.
     * @param streamId  for the error.
     * @param sessionId for the error, if it has a source.
     * @param message   indicating what the error was.
     * @param cause     of the error.
     */
    public static void printError(
        final String channel,
        final int streamId,
        final int sessionId,
        final String message,
        final HeaderFlyweight cause)
    {
        System.out.println(message);
    }

    /**
     * Print the rates to stdout.
     *
     * @param messagesPerSec being reported.
     * @param bytesPerSec    being reported.
     * @param totalMessages  being reported.
     * @param totalBytes     being reported.
     */
    public static void printRate(
        final double messagesPerSec,
        final double bytesPerSec,
        final long totalMessages,
        final long totalBytes)
    {
        System.out.printf(
            "%.04g msgs/sec, %.04g payload bytes/sec, totals %d messages %d MB%n",
            messagesPerSec, bytesPerSec, totalMessages, totalBytes / (1024 * 1024));
    }

    /**
     * Print the information for an available image to stdout.
     *
     * @param image that has been created.
     */
    public static void printAvailableImage(final Image image)
    {
        final Subscription subscription = image.subscription();
        System.out.printf(
            "Available image on %s streamId=%d sessionId=%d mtu=%d term-length=%d from %s%n",
            subscription.channel(), subscription.streamId(), image.sessionId(), image.mtuLength(),
            image.termBufferLength(), image.sourceIdentity());
    }

    /**
     * Print the information for an unavailable image to stdout.
     *
     * @param image that has gone inactive.
     */
    public static void printUnavailableImage(final Image image)
    {
        final Subscription subscription = image.subscription();
        System.out.printf(
            "Unavailable image on %s streamId=%d sessionId=%d%n",
            subscription.channel(), subscription.streamId(), image.sessionId());
    }
}
