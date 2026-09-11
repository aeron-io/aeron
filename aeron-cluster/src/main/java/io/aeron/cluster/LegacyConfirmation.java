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

/**
 * Follower echoes for leaders using the v18, 32-bit confirmation protocol. New leaders never count these
 * acknowledgements towards a read quorum; their full-width round state belongs to {@link CompactConfirmation}.
 * Serial ordering here preserves legacy echo behavior, not the safety of a legacy leader's read API.
 */
final class LegacyConfirmation
{
    // The SBE null value also identifies pre-v18 CommitPosition messages with no confirmation counter.
    static final int NULL_COUNTER = Integer.MIN_VALUE;

    private int followerRound = NULL_COUNTER;
    private boolean ackPending;
    private boolean ackPriority;

    void onElectionComplete()
    {
        followerRound = NULL_COUNTER;
        ackPending = false;
        ackPriority = false;
    }

    void onCommitPosition(final int round)
    {
        if (isNewerRound(round, followerRound))
        {
            followerRound = round;
            ackPending = true;
        }
    }

    int followerRound()
    {
        return followerRound;
    }

    boolean isAckPending()
    {
        return ackPending;
    }

    boolean hasAckPriority()
    {
        return ackPriority;
    }

    void onAckSent()
    {
        ackPending = false;
        ackPriority = false;
    }

    void onAckBackPressured()
    {
        // A retry goes first next cycle; success restores append-position priority to prevent reverse starvation.
        ackPriority = true;
    }

    private static boolean isAfter(final int round, final int previous)
    {
        return 0 < (round - previous);
    }

    static boolean isNewerRound(final int round, final int previous)
    {
        return NULL_COUNTER != round && (NULL_COUNTER == previous || isAfter(round, previous));
    }
}
