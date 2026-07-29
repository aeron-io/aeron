package io.aeron.logging;

import org.agrona.DirectBuffer;
import org.agrona.collections.IntArrayList;

public interface BinaryRenderer
{
    int[] supportingMsgTypeIds();

    void append(StringBuilder sb, int msgTypeId, DirectBuffer buffer, int offset, int length);
}
