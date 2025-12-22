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
package io.aeron.cluster.client;

import io.aeron.Subscription;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.ControlResponseEncoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SessionMessageHeaderEncoder;
import io.aeron.logbuffer.Header;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.BREAK;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class EgressPollerTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
    private final Header header = new Header(42, 16);
    private final Subscription subscription = mock(Subscription.class);
    private final EgressPoller egressPoller = new EgressPoller(subscription, 10);

    @Test
    void shouldIgnoreUnknownMessageSchema()
    {
        final int offset = 64;
        final ControlResponseEncoder controlResponseEncoder = new ControlResponseEncoder();
        final io.aeron.archive.codecs.MessageHeaderEncoder messageHeaderEncoder =
            new io.aeron.archive.codecs.MessageHeaderEncoder();
        controlResponseEncoder
            .wrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
            .correlationId(42)
            .code(ControlResponseCode.ERROR)
            .errorMessage("test");

        assertEquals(
            CONTINUE,
            egressPoller.onFragment(
                buffer, offset, messageHeaderEncoder.encodedLength() + controlResponseEncoder.encodedLength(), header));
        assertFalse(egressPoller.isPollComplete());
    }

    @Test
    void shouldHandleSessionMessage()
    {
        final int offset = 16;
        final SessionMessageHeaderEncoder encoder = new SessionMessageHeaderEncoder();
        final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        final long clusterSessionId = 7777;
        final long leadershipTermId = 5;
        encoder
            .wrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
            .clusterSessionId(clusterSessionId)
            .leadershipTermId(leadershipTermId);

        assertEquals(
            BREAK,
            egressPoller.onFragment(
                buffer, offset, messageHeaderEncoder.encodedLength() + encoder.encodedLength(), header));
        assertTrue(egressPoller.isPollComplete());
        assertEquals(clusterSessionId, egressPoller.clusterSessionId());
        assertEquals(leadershipTermId, egressPoller.leadershipTermId());

        assertEquals(
            ABORT,
            egressPoller.onFragment(
                buffer, offset, messageHeaderEncoder.encodedLength() + encoder.encodedLength(), header));
    }
}
