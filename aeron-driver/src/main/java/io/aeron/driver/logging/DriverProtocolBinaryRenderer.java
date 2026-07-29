package io.aeron.driver.logging;

import io.aeron.logging.BinaryRenderer;
import org.agrona.DirectBuffer;

public class DriverProtocolBinaryRenderer implements BinaryRenderer
{
    public static final int[] MSG_TYPE_ID = {
        DriverEventCode.FRAME_IN.toEventCodeId(),
        DriverEventCode.FRAME_OUT.toEventCodeId()
    };

    public int[] supportingMsgTypeIds()
    {
        return MSG_TYPE_ID;
    }

    public void append(
        final StringBuilder sb,
        final int msgTypeId,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {

    }
}
