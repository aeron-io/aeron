package io.aeron.agent;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;

import java.util.List;

public class CborDecode implements MessageHandler
{
    private final List<LoggerEventCallback> loggers;

    public CborDecode(final List<LoggerEventCallback> loggers)
    {
        this.loggers = loggers;
    }

    public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
    {
        final int typeByte = (0xFF) & buffer.getByte(index);
        final int majorType = ((0b111_00000) & typeByte) >>> 5;

        switch(majorType)
        {

        }
    }
}
