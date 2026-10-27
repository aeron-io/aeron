/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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

import io.aeron.ExclusivePublication;
import io.aeron.Image;

/**
 * Full-width, process-local generations. Election completion invalidates all earlier tokens; wire messages
 * additionally carry the full leadership term. Tokens belong to this control instance and must not be persisted
 * or transferred. The leader captures a token before advancing its round; only strictly later echoes count.
 * Cached and delayed echoes retain their original identity, including across any number of 32-bit boundaries.
 * Overflow fails closed. There is no per-read allocation or sequence window.
 */
final class CompactConfirmation
{
    private long round;
    // Lowest token valid in this leadership epoch; round identities remain monotonic across elections.
    private long firstRound;
    private boolean roundRequested;
    private long followerRound = -1;
    private boolean ackPending;
    private boolean retryAckFirst;

    void onElectionComplete()
    {
        firstRound = round = Math.incrementExact(round);
        roundRequested = false;
        followerRound = -1;
        ackPending = false;
        retryAckFirst = false;
    }

    long request()
    {
        roundRequested = true;
        return round;
    }

    boolean roundRequested()
    {
        return roundRequested;
    }

    void advanceRequestedRound()
    {
        if (roundRequested)
        {
            round = Math.incrementExact(round);
            roundRequested = false;
        }
    }

    long round()
    {
        return round;
    }

    boolean valid(final long token)
    {
        return token >= firstRound && token <= round && token >= 0;
    }

    void onCommit(final long receivedRound)
    {
        if (receivedRound > followerRound)
        {
            followerRound = receivedRound;
            ackPending = true;
        }
    }

    // A local offer can be lost when the receiving Image is replaced.
    void retryAckAfterRecovery()
    {
        if (followerRound >= 0)
        {
            ackPending = true;
        }
    }

    long followerRound()
    {
        return followerRound;
    }

    boolean ackPending()
    {
        return ackPending;
    }

    boolean retryAckFirst()
    {
        return retryAckFirst;
    }

    // A failed follower echo gets priority next cycle; success restores append-position priority.
    void onAckOffer(final boolean sent)
    {
        ackPending = !sent;
        retryAckFirst = !sent;
    }

    /**
     * Connection capability and successful confirmation sends for one member. A replacement publication must
     * announce its identity before compact traffic. Reconnects do not reuse round identities or fence a peer.
     * An echo can only confirm a round successfully sent to this member in the same full leadership term.
     */
    static final class Peer
    {
        private Image image;
        private boolean capable;
        private boolean announce = true;
        private ExclusivePublication announcedPublication;
        private long sentTerm = -1;
        private long sentRound = -1;
        private long confirmedRound = -1;

        void onImage(final Image currentImage, final boolean supportsCompact)
        {
            if (image != currentImage)
            {
                image = currentImage;
                announce = true;
            }
            capable = supportsCompact;
        }

        boolean observesImage(final Image currentImage)
        {
            return image == currentImage;
        }

        void requestAnnouncement()
        {
            announce = true;
        }

        boolean needsAnnouncement(final ExclusivePublication publication)
        {
            return announce || publication != announcedPublication;
        }

        void announced(final ExclusivePublication publication)
        {
            announcedPublication = publication;
            announce = false;
        }

        boolean hasBoundImage()
        {
            return capable && null != image && !image.isClosed();
        }

        boolean ready(final ExclusivePublication publication)
        {
            return hasBoundImage() && !needsAnnouncement(publication);
        }

        void resetConfirmation()
        {
            sentTerm = -1;
            sentRound = -1;
            confirmedRound = -1;
        }

        void onSent(final long term, final long round)
        {
            if (sentTerm != term)
            {
                sentTerm = term;
                confirmedRound = -1;
            }
            sentRound = round;
        }

        void onAckReceived(final long term, final long round)
        {
            if (term == sentTerm && round >= 0 && round <= sentRound && round > confirmedRound)
            {
                confirmedRound = round;
            }
        }

        boolean needsRound(final long term, final long round)
        {
            return sentTerm != term || sentRound < round;
        }

        boolean confirmed(final long term, final long token)
        {
            return sentTerm == term && confirmedRound > token;
        }
    }
}
