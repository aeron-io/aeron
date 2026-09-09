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

import io.aeron.Subscription;
import io.aeron.cluster.codecs.CommitPositionEncoder;
import io.aeron.cluster.codecs.LeadershipConfirmAckEncoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ConsensusAdapterTest
{
    private final ConsensusModuleAgent agent = mock(ConsensusModuleAgent.class);
    private final ConsensusAdapter adapter = new ConsensusAdapter(mock(Subscription.class), agent);
    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

    @ParameterizedTest
    @ValueSource(ints = { 16, 17 })
    void shouldDecodeCommitPositionWithoutConfirmationFromOlderSchemas(final int version)
    {
        final MessageHeaderEncoder header = new MessageHeaderEncoder();
        new CommitPositionEncoder().wrapAndApplyHeader(buffer, 0, header)
            .leadershipTermId(42).logPosition(100).leaderMemberId(1).confirmationCounter(7);
        // Older messages end before the new field. The bytes beyond the message must not become a counter.
        header.version(version).blockLength(20);

        adapter.onFragment(buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + 20, null);

        verify(agent).onCommitPosition(42, 100, 1, ConsensusModuleAgent.NULL_CONFIRMATION_COUNTER);
    }

    @Test
    void shouldDecodeCommitPositionWithConfirmationFromTheCurrentSchema()
    {
        final CommitPositionEncoder encoder = new CommitPositionEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
            .leadershipTermId(42).logPosition(100).leaderMemberId(1).confirmationCounter(7);

        adapter.onFragment(buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength(), null);

        verify(agent).onCommitPosition(42, 100, 1, 7);
    }

    @Test
    void shouldDecodeLeadershipConfirmationAcknowledgements()
    {
        final LeadershipConfirmAckEncoder encoder = new LeadershipConfirmAckEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
            .leadershipTermId(42).followerMemberId(2).confirmationCounter(7);

        adapter.onFragment(buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength(), null);

        verify(agent).onLeadershipConfirmAck(42, 2, 7);
    }
}
