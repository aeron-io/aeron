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

import io.aeron.Aeron;
import io.aeron.driver.Configuration;
import org.junit.jupiter.api.Test;

import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;

class LogPublisherFragmentationTrackerTest
{
    private static final long MAX_PAYLOAD_LENGTH = Configuration.MTU_LENGTH_DEFAULT - HEADER_LENGTH;
    private final LogPublisherFragmentationTracker tracker = new LogPublisherFragmentationTracker();

    @Test
    void shouldNotTrackUnfragmentedMessage()
    {
        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, MAX_PAYLOAD_LENGTH, 0, MAX_PAYLOAD_LENGTH);
        assertEquals(0, tracker.fragmentedMessageBounds().size());
    }

    @Test
    void shouldTrackFragmentedMessage()
    {
        final long startPosition = 0;
        final long endPosition = MAX_PAYLOAD_LENGTH * 2;
        final long length = endPosition - startPosition;

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, length, startPosition, endPosition);
        assertEquals(2, tracker.fragmentedMessageBounds().size());
        assertEquals(endPosition, tracker.fragmentedMessageBounds().pollLong());
        assertEquals(startPosition, tracker.fragmentedMessageBounds().pollLong());
    }

    @Test
    void shouldTrackMultipleFragmentedMessages()
    {
        final long startPositionOne = 0;
        final long endPositionOne = MAX_PAYLOAD_LENGTH * 2;
        final long lengthOne = endPositionOne - startPositionOne;
        final long startPositionTwo = endPositionOne;
        final long endPositionTwo = startPositionTwo + MAX_PAYLOAD_LENGTH;
        final long lengthTwo = endPositionTwo - startPositionTwo;
        final long startPositionThree = endPositionTwo;
        final long endPositionThree = startPositionThree + MAX_PAYLOAD_LENGTH * 3;
        final long lengthThree = endPositionThree - startPositionThree;

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthOne, startPositionOne, endPositionOne);
        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthTwo, startPositionTwo, endPositionTwo);
        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthThree, startPositionThree, endPositionThree);

        assertEquals(4, tracker.fragmentedMessageBounds().size());
        assertEquals(endPositionOne, tracker.fragmentedMessageBounds().pollLong());
        assertEquals(startPositionOne, tracker.fragmentedMessageBounds().pollLong());
        assertEquals(endPositionThree, tracker.fragmentedMessageBounds().pollLong());
        assertEquals(startPositionThree, tracker.fragmentedMessageBounds().pollLong());
    }

    @Test
    void shouldSweepASingleMessage()
    {
        final long startPosition = 432998L;
        final long endPosition = startPosition + MAX_PAYLOAD_LENGTH * 2;
        final long length = endPosition - startPosition;

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, length, startPosition, endPosition);
        tracker.sweepCommittedEntriesTo(0L);
        assertEquals(2, tracker.fragmentedMessageBounds().size());
        tracker.fragmentedMessageBounds().clear();

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, length, startPosition, endPosition);
        tracker.sweepCommittedEntriesTo(startPosition);
        assertEquals(2, tracker.fragmentedMessageBounds().size());
        tracker.fragmentedMessageBounds().clear();

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, length, startPosition, endPosition);
        tracker.sweepCommittedEntriesTo(startPosition + MAX_PAYLOAD_LENGTH);
        assertEquals(2, tracker.fragmentedMessageBounds().size());
        tracker.fragmentedMessageBounds().clear();

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, length, startPosition, endPosition);
        tracker.sweepCommittedEntriesTo(endPosition);
        assertEquals(0, tracker.fragmentedMessageBounds().size());
        tracker.fragmentedMessageBounds().clear();

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, length, startPosition, endPosition);
        tracker.sweepCommittedEntriesTo(endPosition + MAX_PAYLOAD_LENGTH);
        assertEquals(0, tracker.fragmentedMessageBounds().size());
        tracker.fragmentedMessageBounds().clear();
    }

    @Test
    void shouldDetermineLogAdapterRebuildPositionBaseCases()
    {
        final long startPosition = 398742L;
        final long endPosition = startPosition + MAX_PAYLOAD_LENGTH * 2;
        final long length = endPosition - startPosition;

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, length, startPosition, endPosition);
        tracker.storePositionToRebuildLogAdapter(5218L);
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildStartPosition());
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildEndPosition());
        tracker.fragmentedMessageBounds().clear();
        tracker.onLogReplayComplete();

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, length, startPosition, endPosition);
        tracker.storePositionToRebuildLogAdapter(startPosition);
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildStartPosition());
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildEndPosition());
        tracker.fragmentedMessageBounds().clear();
        tracker.onLogReplayComplete();

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, length, startPosition, endPosition);
        tracker.storePositionToRebuildLogAdapter(startPosition + MAX_PAYLOAD_LENGTH);
        assertEquals(startPosition, tracker.logAdapterRebuildStartPosition());
        assertEquals(startPosition + MAX_PAYLOAD_LENGTH, tracker.logAdapterRebuildEndPosition());
        tracker.fragmentedMessageBounds().clear();
        tracker.onLogReplayComplete();

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, length, startPosition, endPosition);
        tracker.storePositionToRebuildLogAdapter(endPosition);
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildStartPosition());
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildEndPosition());
        tracker.fragmentedMessageBounds().clear();
        tracker.onLogReplayComplete();

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, length, startPosition, endPosition);
        tracker.storePositionToRebuildLogAdapter(endPosition + MAX_PAYLOAD_LENGTH);
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildStartPosition());
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildEndPosition());
        tracker.fragmentedMessageBounds().clear();
        tracker.onLogReplayComplete();
    }

    @Test
    void shouldDetermineLogAdapterRebuildPositionTwoMessages()
    {
        final long startPositionOne = 398742L;
        final long endPositionOne = startPositionOne + MAX_PAYLOAD_LENGTH * 2;
        final long lengthOne = endPositionOne - startPositionOne;
        final long startPositionTwo = 321691328L;
        final long endPositionTwo = startPositionTwo + MAX_PAYLOAD_LENGTH * 3;
        final long lengthTwo = endPositionTwo - startPositionTwo;

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthOne, startPositionOne, endPositionOne);
        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthTwo, startPositionTwo, endPositionTwo);
        tracker.storePositionToRebuildLogAdapter(76L);
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildStartPosition());
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildEndPosition());
        tracker.fragmentedMessageBounds().clear();
        tracker.onLogReplayComplete();

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthOne, startPositionOne, endPositionOne);
        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthTwo, startPositionTwo, endPositionTwo);
        tracker.storePositionToRebuildLogAdapter(startPositionOne);
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildStartPosition());
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildEndPosition());
        tracker.fragmentedMessageBounds().clear();
        tracker.onLogReplayComplete();

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthOne, startPositionOne, endPositionOne);
        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthTwo, startPositionTwo, endPositionTwo);
        tracker.storePositionToRebuildLogAdapter(startPositionOne + MAX_PAYLOAD_LENGTH);
        assertEquals(startPositionOne, tracker.logAdapterRebuildStartPosition());
        assertEquals(startPositionOne + MAX_PAYLOAD_LENGTH, tracker.logAdapterRebuildEndPosition());
        tracker.fragmentedMessageBounds().clear();
        tracker.onLogReplayComplete();

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthOne, startPositionOne, endPositionOne);
        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthTwo, startPositionTwo, endPositionTwo);
        tracker.storePositionToRebuildLogAdapter(endPositionOne);
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildStartPosition());
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildEndPosition());
        tracker.fragmentedMessageBounds().clear();
        tracker.onLogReplayComplete();

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthOne, startPositionOne, endPositionOne);
        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthTwo, startPositionTwo, endPositionTwo);
        tracker.storePositionToRebuildLogAdapter(endPositionOne + ((startPositionTwo - endPositionOne) / 2));
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildStartPosition());
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildEndPosition());
        tracker.fragmentedMessageBounds().clear();
        tracker.onLogReplayComplete();

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthOne, startPositionOne, endPositionOne);
        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthTwo, startPositionTwo, endPositionTwo);
        tracker.storePositionToRebuildLogAdapter(startPositionTwo);
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildStartPosition());
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildEndPosition());
        tracker.fragmentedMessageBounds().clear();
        tracker.onLogReplayComplete();

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthOne, startPositionOne, endPositionOne);
        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthTwo, startPositionTwo, endPositionTwo);
        tracker.storePositionToRebuildLogAdapter(startPositionTwo + MAX_PAYLOAD_LENGTH);
        assertEquals(startPositionTwo, tracker.logAdapterRebuildStartPosition());
        assertEquals(startPositionTwo + MAX_PAYLOAD_LENGTH, tracker.logAdapterRebuildEndPosition());
        tracker.fragmentedMessageBounds().clear();
        tracker.onLogReplayComplete();

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthOne, startPositionOne, endPositionOne);
        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthTwo, startPositionTwo, endPositionTwo);
        tracker.storePositionToRebuildLogAdapter(endPositionTwo);
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildStartPosition());
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildEndPosition());
        tracker.fragmentedMessageBounds().clear();
        tracker.onLogReplayComplete();

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthOne, startPositionOne, endPositionOne);
        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthTwo, startPositionTwo, endPositionTwo);
        tracker.storePositionToRebuildLogAdapter(endPositionTwo + MAX_PAYLOAD_LENGTH);
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildStartPosition());
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildEndPosition());
        tracker.fragmentedMessageBounds().clear();
        tracker.onLogReplayComplete();
    }

    @Test
    void shouldDetermineLogAdapterRebuildPositionTwoContiguousMessages()
    {
        final long startPositionOne = 1372894L;
        final long endPositionOne = startPositionOne + MAX_PAYLOAD_LENGTH * 2;
        final long lengthOne = endPositionOne - startPositionOne;
        final long startPositionTwo = endPositionOne;
        final long endPositionTwo = startPositionTwo + MAX_PAYLOAD_LENGTH * 3;
        final long lengthTwo = endPositionTwo - startPositionTwo;

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthOne, startPositionOne, endPositionOne);
        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthTwo, startPositionTwo, endPositionTwo);
        tracker.storePositionToRebuildLogAdapter(startPositionTwo);
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildStartPosition());
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildEndPosition());
        tracker.fragmentedMessageBounds().clear();
        tracker.onLogReplayComplete();
    }

    @Test
    void shouldUpdateLogAdapterBuildPositionIfNoReplayRequired()
    {
        final long startPositionOne = 356743;
        final long endPositionOne = startPositionOne + MAX_PAYLOAD_LENGTH * 2;
        final long lengthOne = endPositionOne - startPositionOne;

        final long initialPosition = startPositionOne + MAX_PAYLOAD_LENGTH;

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthOne, startPositionOne, endPositionOne);
        tracker.storePositionToRebuildLogAdapter(initialPosition);
        assertEquals(startPositionOne, tracker.logAdapterRebuildStartPosition());
        assertEquals(initialPosition, tracker.logAdapterRebuildEndPosition());

        tracker.onLogReplayComplete();
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildStartPosition());
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildEndPosition());
    }

    @Test
    void shouldUpdateLogAdapterBuildPositionAsReplayAdvances()
    {
        final long startPositionOne = 912374L;
        final long endPositionOne = startPositionOne + MAX_PAYLOAD_LENGTH * 2;
        final long lengthOne = endPositionOne - startPositionOne;

        final long commitPosition = startPositionOne + MAX_PAYLOAD_LENGTH;

        tracker.trackFragmentedMessage(MAX_PAYLOAD_LENGTH, lengthOne, startPositionOne, endPositionOne);
        tracker.storePositionToRebuildLogAdapter(commitPosition);
        assertEquals(startPositionOne, tracker.logAdapterRebuildStartPosition());
        assertEquals(commitPosition, tracker.logAdapterRebuildEndPosition());

        tracker.onLogReplay(startPositionOne + 10);
        assertEquals(startPositionOne + 10, tracker.logAdapterRebuildStartPosition());
        assertEquals(commitPosition, tracker.logAdapterRebuildEndPosition());

        tracker.onLogReplay(startPositionOne + 26);
        assertEquals(startPositionOne + 26, tracker.logAdapterRebuildStartPosition());
        assertEquals(commitPosition, tracker.logAdapterRebuildEndPosition());

        tracker.onLogReplay(commitPosition);
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildStartPosition());
        assertEquals(Aeron.NULL_VALUE, tracker.logAdapterRebuildStartPosition());
    }
}