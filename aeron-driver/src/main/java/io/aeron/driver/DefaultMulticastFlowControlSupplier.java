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
import io.aeron.driver.media.UdpChannel;
import org.agrona.LangUtil;

import static io.aeron.driver.Configuration.MULTICAST_FLOW_CONTROL_STRATEGY;

/**
 * Default supplier of {@link FlowControl} strategies for multicast streams which supports defining the strategy in
 * the channel URI as a priority over {@link Configuration#MULTICAST_FLOW_CONTROL_STRATEGY_PROP_NAME}.
 */
public class DefaultMulticastFlowControlSupplier implements FlowControlSupplier
{
    /**
     * {@inheritDoc}
     */
    public FlowControl newInstance(final UdpChannel udpChannel, final int streamId, final long registrationId)
    {
        final String fcStr = udpChannel.channelUri().get(CommonContext.FLOW_CONTROL_PARAM_NAME);
        if (null != fcStr)
        {
            final int delimiter = fcStr.indexOf(',');
            final String strategyStr = -1 == delimiter ? fcStr : fcStr.substring(0, delimiter);

            switch (strategyStr)
            {
                case MaxMulticastFlowControl.FC_PARAM_VALUE:
                    return new MaxMulticastFlowControl();

                case MinMulticastFlowControl.FC_PARAM_VALUE:
                    return new MinMulticastFlowControl();

                case TaggedMulticastFlowControl.FC_PARAM_VALUE:
                    return new TaggedMulticastFlowControl();

                default:
                    throw new IllegalArgumentException("unsupported multicast flow control strategy: fc=" + fcStr);
            }
        }

        if (MaxMulticastFlowControl.class.getName().equals(MULTICAST_FLOW_CONTROL_STRATEGY))
        {
            return new MaxMulticastFlowControl();
        }
        else if (MinMulticastFlowControl.class.getName().equals(MULTICAST_FLOW_CONTROL_STRATEGY))
        {
            return new MinMulticastFlowControl();
        }
        else if (TaggedMulticastFlowControl.class.getName().equals(MULTICAST_FLOW_CONTROL_STRATEGY))
        {
            return new TaggedMulticastFlowControl();
        }

        FlowControl flowControl = null;
        try
        {
            flowControl = (FlowControl)Class.forName(MULTICAST_FLOW_CONTROL_STRATEGY)
                .getConstructor()
                .newInstance();
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return flowControl;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "DefaultMulticastFlowControlSupplier{flowControlClass=" +
            MULTICAST_FLOW_CONTROL_STRATEGY + "}";
    }
}
