/*
 * Copyright 2014-2026 Real Logic Limited.
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
package io.aeron.agent;

import java.util.List;

/**
 * Decoder for converting generic CBOR logging callbacks to the typed interface.
 */
public class CborClusterEventDecode implements LoggerEventCallback
{
    private final LogElectionStateChangeFlyweight logElectionStateChangeFlyweight;
    private LoggerEventCallback currentDecoder = null;

    /**
     * Construct a decoder with the supplied loggers.
     *
     * @param loggers to be called when logging events arrive.
     */
    public CborClusterEventDecode(final List<ClusterEventLogger> loggers)
    {
        this.logElectionStateChangeFlyweight = new LogElectionStateChangeFlyweight(loggers);
    }

    /**
     * {@inheritDoc}
     */
    public void onHeader(final int eventType, final int eventCode, final long timestamp)
    {
        if (EventCodeType.CLUSTER.getTypeCode() != eventCode)
        {
            currentDecoder = null;
            return;
        }

        final ClusterEventCode clusterEventCode = ClusterEventCode.fromEventCodeId(eventCode);
        switch (clusterEventCode)
        {
            case ELECTION_STATE_CHANGE:
            {
                logElectionStateChangeFlyweight.reset();
                currentDecoder = logElectionStateChangeFlyweight;
            }
        }

        if (null == currentDecoder)
        {
            return;
        }

        currentDecoder.onHeader(eventType, eventCode, timestamp);
    }

    /**
     * {@inheritDoc}
     */
    public void onValue(final CharSequence name, final CharSequence value)
    {
        if (null == currentDecoder)
        {
            return;
        }

        currentDecoder.onValue(name, value);
    }

    /**
     * {@inheritDoc}
     */
    public void onValue(final CharSequence name, final long value)
    {
        if (null == currentDecoder)
        {
            return;
        }

        currentDecoder.onValue(name, value);
    }

    /**
     * {@inheritDoc}
     */
    public void onFooter(final boolean truncated)
    {
        if (null == currentDecoder)
        {
            return;
        }

        currentDecoder.onFooter(truncated);
        currentDecoder = null;
    }
}
