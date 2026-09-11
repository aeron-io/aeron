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

import io.aeron.Image;
import io.aeron.ExclusivePublication;
import io.aeron.test.Tests;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CompactConfirmationTest
{
    @Test
    void shouldCoalesceAndInvalidateTokensOnElection()
    {
        final CompactConfirmation state = new CompactConfirmation();
        state.onElectionComplete();
        final long token = state.request();
        assertEquals(token, state.request());
        state.broadcast();
        assertEquals(token + 1, state.round());
        state.broadcast();
        assertEquals(token + 1, state.round());
        assertTrue(state.valid(token));
        state.onCommit(12);
        state.onAck(false);
        state.onElectionComplete();
        assertFalse(state.valid(token));
        assertFalse(state.pending());
        assertFalse(state.priority());
        assertFalse(state.requested());
        assertFalse(state.valid(-1));
        assertFalse(state.valid(state.round() + 1));
    }

    @Test
    void shouldFailClosedOnExhaustion()
    {
        final CompactConfirmation state = new CompactConfirmation();
        Tests.setField(state, "round", Long.MAX_VALUE);
        state.request();
        assertThrows(ArithmeticException.class, state::broadcast);
        assertThrows(ArithmeticException.class, state::onElectionComplete);
    }

    @ParameterizedTest
    @ValueSource(longs = { 2147483647L, 4294967295L, 17179869184L, 9223372036854775806L })
    void shouldRejectOldCachedAndDelayedEchoesWithoutSerialArithmetic(final long round)
    {
        final CompactConfirmation.Peer peer = new CompactConfirmation.Peer();
        peer.onSent(42, 1);
        peer.onAck(42, 1);
        peer.onSent(42, round + 1);
        assertFalse(peer.confirmed(42, round));
        peer.onAck(42, 1);
        assertFalse(peer.confirmed(42, round));
        peer.onAck(42, round + 1);
        assertTrue(peer.confirmed(42, round));
        peer.onAck(42, 1);
        assertTrue(peer.confirmed(42, round));
        assertFalse(peer.confirmed(43, round));
    }

    @Test
    void shouldRejectUnsentAndWrongTermEchoes()
    {
        final CompactConfirmation.Peer peer = new CompactConfirmation.Peer();
        peer.onAck(42, 10);
        assertFalse(peer.confirmed(42, 9));
        peer.onSent(42, 10);
        peer.onAck(41, 10);
        peer.onAck(42, 11);
        peer.onAck(42, -1);
        assertFalse(peer.confirmed(42, 9));
        peer.onAck(42, 10);
        assertTrue(peer.confirmed(42, 9));
        peer.onSent(43, 12);
        assertFalse(peer.confirmed(43, 9));
    }

    @Test
    void shouldRepeatAnnouncementsOnlyForChangedConnectionsAndRecoverWithoutFencing()
    {
        final CompactConfirmation.Peer peer = new CompactConfirmation.Peer();
        final ExclusivePublication publication = mock(ExclusivePublication.class);
        final Image first = mock(Image.class);
        peer.onImage(first, true);
        assertFalse(peer.ready(publication));
        peer.announced(publication);
        assertTrue(peer.ready(publication));
        peer.onImage(first, true);
        assertTrue(peer.ready(publication));
        peer.onImage(mock(Image.class), true);
        assertFalse(peer.ready(publication));
        peer.announced(publication);
        assertTrue(peer.ready(publication));
        assertFalse(peer.ready(mock(ExclusivePublication.class)));
        peer.onImage(mock(Image.class), false);
        peer.announced(publication);
        assertFalse(peer.ready(publication));
    }

    @Test
    void shouldCoalesceFollowerEchoesAndRetainPriorityOnBackpressure()
    {
        final CompactConfirmation state = new CompactConfirmation();
        state.onCommit(1L << 34);
        state.onAck(false);
        state.onCommit(7);
        assertEquals(1L << 34, state.followerRound());
        assertTrue(state.priority());
        state.onCommit((1L << 34) + 1);
        state.onAck(true);
        state.onCommit(1L << 34);
        assertFalse(state.pending());
        assertFalse(state.priority());
    }
}
