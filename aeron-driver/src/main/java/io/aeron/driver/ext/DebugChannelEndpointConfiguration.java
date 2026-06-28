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
package io.aeron.driver.ext;

import io.aeron.config.Config;
import io.aeron.config.DefaultType;

import static java.lang.Long.getLong;
import static java.lang.System.getProperty;

/**
 * Configuration options to be applied when {@link DebugSendChannelEndpoint} and {@link DebugReceiveChannelEndpoint}
 * are load.
 */
public final class DebugChannelEndpointConfiguration
{
    private DebugChannelEndpointConfiguration()
    {
    }

    /**
     * Property name for receiver inbound data loss rate.
     */
    @Config(defaultType = DefaultType.DOUBLE, defaultDouble = 0.0, hasContext = false, existsInC = false)
    public static final String RECEIVE_DATA_LOSS_RATE_PROP_NAME = "aeron.debug.receive.data.loss.rate";

    /**
     * Property name for receiver inbound data loss seed.
     */
    @Config(defaultType = DefaultType.LONG, defaultLong = -1, hasContext = false, existsInC = false)
    public static final String RECEIVE_DATA_LOSS_SEED_PROP_NAME = "aeron.debug.receive.data.loss.seed";

    /**
     * Property name for receiver outbound control loss rate.
     */
    @Config(defaultType = DefaultType.DOUBLE, defaultDouble = 0.0, hasContext = false, existsInC = false)
    public static final String RECEIVE_CONTROL_LOSS_RATE_PROP_NAME = "aeron.debug.receive.control.loss.rate";

    /**
     * Property name for receiver outbound control loss seed.
     */
    @Config(defaultType = DefaultType.LONG, defaultLong = -1, hasContext = false, existsInC = false)
    public static final String RECEIVE_CONTROL_LOSS_SEED_PROP_NAME = "aeron.debug.receive.control.loss.seed";

    /**
     * Property name for sender outbound data loss rate.
     */
    @Config(defaultType = DefaultType.DOUBLE, defaultDouble = 0.0, hasContext = false, existsInC = false)
    public static final String SEND_DATA_LOSS_RATE_PROP_NAME = "aeron.debug.send.data.loss.rate";

    /**
     * Property name for sender outbound data loss seed.
     */
    @Config(defaultType = DefaultType.LONG, defaultLong = -1, hasContext = false, existsInC = false)
    public static final String SEND_DATA_LOSS_SEED_PROP_NAME = "aeron.debug.send.data.loss.seed";

    /**
     * Property name for sender inbound control loss rate.
     */
    @Config(defaultType = DefaultType.DOUBLE, defaultDouble = 0.0, hasContext = false, existsInC = false)
    public static final String SEND_CONTROL_LOSS_RATE_PROP_NAME = "aeron.debug.send.control.loss.rate";

    /**
     * Property name for sender inbound control loss seed.
     */
    @Config(defaultType = DefaultType.LONG, defaultLong = -1, hasContext = false, existsInC = false)
    public static final String SEND_CONTROL_LOSS_SEED_PROP_NAME = "aeron.debug.send.control.loss.seed";

    private static final long RECEIVE_DATA_LOSS_SEED = getLong(RECEIVE_DATA_LOSS_SEED_PROP_NAME, -1);

    private static final double RECEIVE_DATA_LOSS_RATE =
        Double.parseDouble(getProperty(RECEIVE_DATA_LOSS_RATE_PROP_NAME, "0.0"));

    private static final long RECEIVE_CONTROL_LOSS_SEED = getLong(RECEIVE_CONTROL_LOSS_SEED_PROP_NAME, -1);

    private static final double RECEIVE_CONTROL_LOSS_RATE =
        Double.parseDouble(getProperty(RECEIVE_CONTROL_LOSS_RATE_PROP_NAME, "0.0"));

    private static final long SEND_DATA_LOSS_SEED = getLong(SEND_DATA_LOSS_SEED_PROP_NAME, -1);

    private static final double SEND_DATA_LOSS_RATE =
        Double.parseDouble(getProperty(SEND_DATA_LOSS_RATE_PROP_NAME, "0.0"));

    private static final long SEND_CONTROL_LOSS_SEED = getLong(SEND_CONTROL_LOSS_SEED_PROP_NAME, -1);

    private static final double SEND_CONTROL_LOSS_RATE =
        Double.parseDouble(getProperty(SEND_CONTROL_LOSS_RATE_PROP_NAME, "0.0"));

    /**
     * Supplier of {@link LossGenerator}s with provided loss rate and seed for randomisation.
     *
     * @param lossRate to generate at.
     * @param lossSeed to initialise.
     * @return a {@link LossGenerator}s with provided loss rate and seed.
     */
    public static LossGenerator lossGeneratorSupplier(final double lossRate, final long lossSeed)
    {
        if (0 == lossRate)
        {
            return (address, buffer, length) -> false;
        }

        return new RandomLossGenerator(lossRate, lossSeed);
    }

    /**
     * The supplier of {@link LossGenerator}s for the receiving end of a data stream.
     *
     * @return the supplier of {@link LossGenerator}s for the receiving end of a data stream.
     */
    public static LossGenerator receiveDataLossGeneratorSupplier()
    {
        return lossGeneratorSupplier(RECEIVE_DATA_LOSS_RATE, RECEIVE_DATA_LOSS_SEED);
    }

    /**
     * The supplier of {@link LossGenerator}s for the receiving end of a control stream.
     *
     * @return the supplier of {@link LossGenerator}s for the receiving end of a control stream.
     */
    public static LossGenerator receiveControlLossGeneratorSupplier()
    {
        return lossGeneratorSupplier(RECEIVE_CONTROL_LOSS_RATE, RECEIVE_CONTROL_LOSS_SEED);
    }

    /**
     * The supplier of {@link LossGenerator}s for the send end of a data stream.
     *
     * @return the supplier of {@link LossGenerator}s for the send end of a data stream.
     */
    public static LossGenerator sendDataLossGeneratorSupplier()
    {
        return lossGeneratorSupplier(SEND_DATA_LOSS_RATE, SEND_DATA_LOSS_SEED);
    }

    /**
     * The supplier of {@link LossGenerator}s for the send end of a control stream.
     *
     * @return the supplier of {@link LossGenerator}s for the send end of a control stream.
     */
    public static LossGenerator sendControlLossGeneratorSupplier()
    {
        return lossGeneratorSupplier(SEND_CONTROL_LOSS_RATE, SEND_CONTROL_LOSS_SEED);
    }
}
