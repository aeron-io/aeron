package io.aeron.agent;

import org.agrona.MutableDirectBuffer;

public class EncodingState
{
    private boolean reachedLimit = false;
    private MutableDirectBuffer buffer;
    private int offset;
    private int length;

    public void reset(final MutableDirectBuffer buffer, final int offset, final int length)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.length = length;
        reachedLimit = false;
    }

    public boolean isReachedLimit()
    {
        return reachedLimit;
    }

    public void reachedLimit(final boolean value)
    {
        reachedLimit = value;
    }

    public int offset()
    {
        return offset;
    }

    public void offset(final int offset)
    {
        this.offset = offset;
    }

    public int length()
    {
        return length;
    }

    public int remaining()
    {
        return length - offset;
    }

    public void incrementOffset(final int length)
    {
        offset += length;
    }

    public MutableDirectBuffer buffer()
    {
        return buffer;
    }
}
