package io.aeron.agent;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.cbor.CBORFactory;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CborUtilTest
{
    @ParameterizedTest
    @ValueSource(ints = {
        2, 25, 0x7F, 0x100,
        0x1234, 0x7FFF, 0x10000,
        0x12345678, 0x7FFFFFFF, 0x80000000,
        -2, -25, -0xFF,
        -0x1234, -0xFFFF,
        -0x12345678, -0xFFFFFFFE})
    void shouldEncodeMessage(final int memberId)
    {
        final int offset = 0;
        final int length = 1024;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);

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