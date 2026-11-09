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
package io.aeron.test.driver;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ReceiveChannelEndpointSupplier;
import io.aeron.driver.StaticDelayGenerator;
import io.aeron.driver.ext.*;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.status.CountersManager;

public final class JavaTestMediaDriver implements TestMediaDriver
{
    private final MediaDriver mediaDriver;

    private JavaTestMediaDriver(final MediaDriver mediaDriver)
    {
        this.mediaDriver = mediaDriver;
    }

    @Override
    public void close()
    {
        mediaDriver.close();
    }

    @Override
    public void cleanup()
    {
    }

    public static JavaTestMediaDriver launch(final MediaDriver.Context context)
    {
        final MediaDriver mediaDriver = MediaDriver.launch(context);
        return new JavaTestMediaDriver(mediaDriver);
    }

    @Override
    public MediaDriver.Context context()
    {
        return mediaDriver.context();
    }

    @Override
    public String aeronDirectoryName()
    {
        return mediaDriver.aeronDirectoryName();
    }

    @Override
    public AgentInvoker sharedAgentInvoker()
    {
        return mediaDriver.sharedAgentInvoker();
    }

    @Override
    public CountersManager counters()
    {
        return mediaDriver.context().countersManager();
    }

    private static void enableLossOnReceive(
        final MediaDriver.Context context,
        final LossGenerator dataLossGenerator,
        final LossGenerator controlLossGenerator)
    {
        final ReceiveChannelEndpointSupplier endpointSupplier =
            (udpChannel, dispatcher, statusIndicator, ctx) ->
            {
                return new DebugReceiveChannelEndpoint(
                    udpChannel, dispatcher, statusIndicator, ctx,
                    dataLossGenerator == null ? (address, buffer, length) -> false : dataLossGenerator,
                    controlLossGenerator == null ? (address, buffer, length) -> false : controlLossGenerator);
            };

        context.receiveChannelEndpointSupplier(endpointSupplier);
    }

    public static void enableRandomLossOnReceive(
        final MediaDriver.Context context,
        final double rate,
        final long seed,
        final boolean loseDataMessages,
        final boolean loseControlMessages)
    {
        enableLossOnReceive(
            context,
            loseDataMessages ? DebugChannelEndpointConfiguration.lossGeneratorSupplier(rate, seed) : null,
            loseControlMessages ? DebugChannelEndpointConfiguration.lossGeneratorSupplier(rate, seed) : null);
    }

    public static void enableFixedLossOnReceive(
        final MediaDriver.Context context,
        final int termId,
        final int termOffset,
        final int length)
    {
        enableLossOnReceive(context, new FixedLossGenerator(termId, termOffset, length), null);
    }

    public static void enableMultiGapLossOnReceive(
        final MediaDriver.Context context,
        final int termId,
        final int gapRadix,
        final int gapLength,
        final int totalGaps)
    {
        enableLossOnReceive(context, new MultiGapLossGenerator(termId, gapRadix, gapLength, totalGaps), null);
    }

    public static void dontCoalesceNaksOnReceiverByDefault(final MediaDriver.Context context)
    {
        context.unicastFeedbackDelayGenerator(new StaticDelayGenerator(0, 0));
    }
}
