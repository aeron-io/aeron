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
package io.aeron.driver;

import io.aeron.CommonContext;
import io.aeron.driver.ext.CubicCongestionControl;
import io.aeron.driver.media.UdpChannel;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.status.CountersManager;

import java.net.InetSocketAddress;

/**
 * Supplier of congestion control algorithms which is aware of channel URI params otherwise defaults to
 * {@link StaticWindowCongestionControl}.
 */
public class DefaultCongestionControlSupplier implements CongestionControlSupplier
{
    /**
     * Default constructor.
     */
    public DefaultCongestionControlSupplier()
    {
    }

    /**
     * {@inheritDoc}
     */
    public CongestionControl newInstance(
        final long registrationId,
        final UdpChannel udpChannel,
        final int streamId,
        final int sessionId,
        final int termLength,
        final int senderMtuLength,
        final InetSocketAddress controlAddress,
        final InetSocketAddress sourceAddress,
        final NanoClock nanoClock,
        final MediaDriver.Context context,
        final CountersManager countersManager)
    {
        final String ccStr = udpChannel.channelUri().get(CommonContext.CONGESTION_CONTROL_PARAM_NAME);

        if (null == ccStr || StaticWindowCongestionControl.CC_PARAM_VALUE.equals(ccStr))
        {
            return new StaticWindowCongestionControl(
                registrationId,
                udpChannel,
                streamId,
                sessionId,
                termLength,
                senderMtuLength,
                controlAddress,
                sourceAddress,
                nanoClock,
                context,
                countersManager);
        }
        else if (CubicCongestionControl.CC_PARAM_VALUE.equals(ccStr))
        {
            return new CubicCongestionControl(
                registrationId,
                udpChannel,
                streamId,
                sessionId,
                termLength,
                senderMtuLength,
                controlAddress,
                sourceAddress,
                nanoClock,
                context,
                countersManager);
        }

        throw new IllegalArgumentException("unsupported congestion control : cc=" + ccStr);
    }
}
