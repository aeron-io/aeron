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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LegacyConfirmationTest
{
    @ParameterizedTest
    @CsvSource({ "1,0,true", "0,0,false", "0,1,false", "-2147483647,2147483647,true",
        "2147483647,-2147483647,false", "1,-2147483648,true", "-2147483648,1,false" })
    void shouldPreserveLegacyEchoOrdering(final int round, final int previous, final boolean newer)
    {
        assertEquals(newer, LegacyConfirmation.isNewerRound(round, previous));
    }

    @Test
    void shouldCoalesceLegacyEchoesAndResetPendingWorkOnElection()
    {
        final LegacyConfirmation state = new LegacyConfirmation();
        state.onCommitPosition(7);
        state.onAckBackPressured();
        state.onCommitPosition(6);
        state.onCommitPosition(LegacyConfirmation.NULL_COUNTER);
        assertEquals(7, state.followerRound());
        assertTrue(state.isAckPending());
        assertTrue(state.hasAckPriority());
        state.onCommitPosition(8);
        assertEquals(8, state.followerRound());
        state.onAckSent();
        assertFalse(state.isAckPending());
        assertFalse(state.hasAckPriority());
        state.onCommitPosition(8);
        assertFalse(state.isAckPending());
        state.onCommitPosition(9);
        state.onAckBackPressured();
        state.onElectionComplete();
        assertFalse(state.isAckPending());
        assertFalse(state.hasAckPriority());
        assertEquals(LegacyConfirmation.NULL_COUNTER, state.followerRound());
        state.onCommitPosition(1);
        assertEquals(1, state.followerRound());
        assertTrue(state.isAckPending());
    }
}
