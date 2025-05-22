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
package io.aeron.cluster;

import org.agrona.SemanticVersion;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SessionEventCodecCompatibilityTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);

    @Test
    void readingVersion12UsingVersion5Codec()
    {
        final long clusterSessionId = -4623823;
        final long correlationId = 3583456348756843L;
        final long leadershipTermId = 10_000_000_000L;
        final int leaderMemberId = 2;
        final io.aeron.cluster.codecs.v12.EventCode code = io.aeron.cluster.codecs.v12.EventCode.REDIRECT;
        final int version = SemanticVersion.compose(3, 47, 123);
        final String detail = "some very detailed message";
        final io.aeron.cluster.codecs.v12.SessionEventEncoder encoderV12 =
            new io.aeron.cluster.codecs.v12.SessionEventEncoder();
        encoderV12
            .wrapAndApplyHeader(buffer, 0, new io.aeron.cluster.codecs.v12.MessageHeaderEncoder())
            .clusterSessionId(clusterSessionId)
            .correlationId(correlationId)
            .leadershipTermId(leadershipTermId)
            .leaderMemberId(leaderMemberId)
            .code(code)
            .version(version)
            .detail(detail);

        final io.aeron.cluster.codecs.v5.SessionEventDecoder decoderV5 =
            new io.aeron.cluster.codecs.v5.SessionEventDecoder();
        final io.aeron.cluster.codecs.v5.MessageHeaderDecoder headerDecoderV5 =
            new io.aeron.cluster.codecs.v5.MessageHeaderDecoder();
        decoderV5.wrapAndApplyHeader(buffer, 0, headerDecoderV5);

        assertEquals(
            io.aeron.cluster.codecs.v5.SessionEventDecoder.SCHEMA_ID,
            io.aeron.cluster.codecs.v12.SessionEventEncoder.SCHEMA_ID);
        assertThat(
            io.aeron.cluster.codecs.v12.SessionEventEncoder.SCHEMA_VERSION,
            greaterThan(io.aeron.cluster.codecs.v5.SessionEventDecoder.SCHEMA_VERSION));
        assertThat(
            io.aeron.cluster.codecs.v12.SessionEventEncoder.BLOCK_LENGTH,
            greaterThan(io.aeron.cluster.codecs.v5.SessionEventDecoder.BLOCK_LENGTH));
        assertEquals(io.aeron.cluster.codecs.v5.SessionEventDecoder.SCHEMA_ID, headerDecoderV5.sbeSchemaId());
        assertEquals(io.aeron.cluster.codecs.v5.SessionEventDecoder.SCHEMA_VERSION, headerDecoderV5.sbeSchemaVersion());
        assertEquals(io.aeron.cluster.codecs.v12.SessionEventEncoder.SCHEMA_VERSION, headerDecoderV5.version());
        assertEquals(io.aeron.cluster.codecs.v12.SessionEventEncoder.BLOCK_LENGTH, headerDecoderV5.blockLength());

        assertEquals(clusterSessionId, decoderV5.clusterSessionId());
        assertEquals(correlationId, decoderV5.correlationId());
        assertEquals(leadershipTermId, decoderV5.leadershipTermId());
        assertEquals(leaderMemberId, decoderV5.leaderMemberId());
        assertEquals(code.value(), decoderV5.code().value());
        assertEquals(detail, decoderV5.detail());
    }
}
