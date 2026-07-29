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
package io.aeron.driver.logging;

import io.aeron.logging.BinaryRenderer;
import org.agrona.DirectBuffer;

import java.util.Arrays;

/**
 * Binary renderer for the Aeron network protocol messages.
 */
public class DriverProtocolBinaryRenderer implements BinaryRenderer
{
    private static final int[] MSG_TYPE_ID = {
        DriverEventCode.FRAME_IN.toEventCodeId(),
        DriverEventCode.FRAME_OUT.toEventCodeId()
    };

    /**
     * Default constructor.
     */
    public DriverProtocolBinaryRenderer()
    {
    }

    /**
     * {@inheritDoc}
     */
    public int[] supportingMsgTypeIds()
    {
        return Arrays.copyOf(MSG_TYPE_ID, 0);
    }

    /**
     * {@inheritDoc}
     */
    public void append(
        final StringBuilder sb,
        final int msgTypeId,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
    }
}
