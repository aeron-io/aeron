package io.aeron.agent;

import org.agrona.DirectBuffer;

public class DecodingState
{
    private DirectBuffer buffer;
    private int position;
    private int length;
    private boolean terminated = false;

    public DecodingState(final DirectBuffer buffer, final int initialPosition, final int length)
    {
        this.buffer = buffer;
        this.position = initialPosition;
        this.length = length;
    }

    public void incrementPosition(int increment)
    {
        this.position += increment;
        if (this.position > this.length)
        {
            throw new CborDecode.InvalidMessage("Terminated prematurely");
        }
    }

    public int position()
    {
        return position;
    }

    public DirectBuffer buffer()
    {
        return this.buffer;
    }

    public boolean isTerminated()
    {
        return terminated;
    }

    public void terminate()
    {
        this.terminated = true;
    }
}
