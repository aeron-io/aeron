package io.aeron.agent;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.cbor.CBORFactory;
import tools.jackson.dataformat.cbor.CBORMapper;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CborUtilTest
{
    @Test
    void shouldEncodeMessage()
    {
        final int offset = 0;
        final int length = 1024;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);

        final int memberId = 2;
        final long timestamp = 12643263L;

        final EncodingState encodingState = new EncodingState();
        encodingState.reset(buffer, offset, length);

        CborUtil.encodeHeader(encodingState, ClusterEventCode.ELECTION_STATE_CHANGE, timestamp);
        CborUtil.encode(encodingState, "memberId", memberId);
        CborUtil.encodeFooter(encodingState);

        final ObjectMapper cborObjectMapper = new ObjectMapper(new CBORFactory());

        final byte[] data = new byte[encodingState.offset()];
        encodingState.buffer().getBytes(0, data);

        final Map<String, Object> stringObjectMap = cborObjectMapper.readValue(
            data,
            new TypeReference<>()
            {
            });

        assertEquals(memberId, (Integer)stringObjectMap.get("memberId"));

        System.out.println(stringObjectMap);
    }
}