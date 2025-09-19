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
package io.aeron.driver;

import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Tracking and handling of retransmit request, NAKs, for senders, and receivers.
 * <p>
 * When configured for multicast, a max number of retransmits is permitted by
 * {@link Configuration#MAX_RESEND_DEFAULT}. Additional received NAKs will be ignored if this maximum is reached.
 * When configured for unicast, a single outstanding retransmit is permitted, and additional received NAKs
 * will be ignored iff they overlap the current retransmit - otherwise the previous retransmit is assumed to have
 * 'worked' and the new NAK will take its place.
 */
public final class RetransmitHandler
{
    private final RetransmitAction[] retransmitActionPool;
    private final NanoClock nanoClock;
    private final FeedbackDelayGenerator delayGenerator;
    private final FeedbackDelayGenerator lingerTimeoutGenerator;
    private final AtomicCounter invalidPackets;
    private final boolean hasGroupSemantics;
    private final AtomicCounter retransmitOverflowCounter;

    private int activeRetransmitCount = 0;

    /**
     * Create a handler for the dealing with the reception of frame request a frame to be retransmitted.
     *
     * @param nanoClock                 used to determine time.
     * @param invalidPackets            for recording invalid packets.
     * @param delayGenerator            to use for delay determination.
     * @param lingerTimeoutGenerator    to use for linger timeout.
     * @param hasGroupSemantics         indicates multicast/MDC semantics.
     * @param maxRetransmits            max retransmits for when group semantics is enabled
     * @param retransmitOverflowCounter counter to track overflows.
     */
    public RetransmitHandler(
        final NanoClock nanoClock,
        final AtomicCounter invalidPackets,
        final FeedbackDelayGenerator delayGenerator,
        final FeedbackDelayGenerator lingerTimeoutGenerator,
        final boolean hasGroupSemantics,
        final int maxRetransmits,
        final AtomicCounter retransmitOverflowCounter)
    {
        this.nanoClock = nanoClock;
        this.invalidPackets = invalidPackets;
        this.delayGenerator = delayGenerator;
        this.lingerTimeoutGenerator = lingerTimeoutGenerator;
        this.hasGroupSemantics = hasGroupSemantics;
        this.retransmitOverflowCounter = retransmitOverflowCounter;

        final int actualMaxRetransmits = this.hasGroupSemantics ? maxRetransmits : 1;

        retransmitActionPool = new RetransmitAction[actualMaxRetransmits];
        for (int i = 0; i < actualMaxRetransmits; i++)
        {
            retransmitActionPool[i] = new RetransmitAction();
        }
    }

    /**
     * Called on reception of a NAK to start retransmit handling.
     *
     * @param termId           from the NAK and the term id of the buffer to retransmit from.
     * @param termOffset       from the NAK and the offset of the data to retransmit.
     * @param length           of the missing data.
     * @param termLength       of the term buffer.
     * @param mtuLength        for the publication.
     * @param flowControl      for the publication (to clamp the retransmission length).
     * @param retransmitSender to call if an immediate retransmit is required.
     */
    public void onNak(
        final int termId,
        final int termOffset,
        final int length,
        final int termLength,
        final int mtuLength,
        final FlowControl flowControl,
        final RetransmitSender retransmitSender)
    {
        if (!isInvalid(termOffset, termLength, length) && 0 != length)
        {
            final int retransmitLength = flowControl.maxRetransmissionLength(termOffset, length, termLength, mtuLength);
            final RetransmitAction action = scanForAvailableRetransmit(termId, termOffset, retransmitLength);
            if (null != action)
            {
                action.termId = termId;
                action.termOffset = termOffset;
                action.length = retransmitLength;

                final long delay = delayGenerator.generateDelayNs();
                if (0 == delay)
                {
                    retransmitSender.resend(termId, termOffset, action.length);
                    action.linger(lingerTimeoutGenerator.generateDelayNs(), nanoClock.nanoTime());
                }
                else
                {
                    action.delay(delay, nanoClock.nanoTime());
                }
            }
        }
    }

    /**
     * Called to indicate a retransmission is received that may obviate the need to send one ourselves.
     * <p>
     * NOTE: Currently only called from unit tests. Would be used for retransmitting from receivers for NAK suppression.
     *
     * @param termId     of the data.
     * @param termOffset of the data.
     */
    public void onRetransmitReceived(final int termId, final int termOffset)
    {
        final RetransmitAction action = scanForExistingRetransmit(termId, termOffset);

        if (null != action && RetransmitAction.State.DELAYED == action.state)
        {
            removeRetransmit(action);
        }
    }

    /**
     * Called to process any outstanding timeouts.
     *
     * @param nowNs            time in nanoseconds.
     * @param retransmitSender to call on retransmissions.
     */
    public void processTimeouts(final long nowNs, final RetransmitSender retransmitSender)
    {
        if (activeRetransmitCount > 0)
        {
            for (final RetransmitAction action : retransmitActionPool)
            {
                if (RetransmitAction.State.DELAYED == action.state && (action.expiryNs - nowNs < 0))
                {
                    retransmitSender.resend(action.termId, action.termOffset, action.length);
                    action.linger(lingerTimeoutGenerator.generateDelayNs(), nanoClock.nanoTime());
                }
                else if (RetransmitAction.State.LINGERING == action.state && (action.expiryNs - nowNs < 0))
                {
                    removeRetransmit(action);
                }
            }
        }
    }

    private boolean isInvalid(final int termOffset, final int termLength, final int length)
    {
        final boolean isInvalid = (termOffset > (termLength - DataHeaderFlyweight.HEADER_LENGTH)) || (termOffset < 0) ||
            (length < 0);

        if (isInvalid)
        {
            invalidPackets.increment();
        }

        return isInvalid;
    }

    private RetransmitAction scanForAvailableRetransmit(final int termId, final int termOffset, final int length)
    {
        if (0 == activeRetransmitCount)
        {
            return addRetransmit(retransmitActionPool[0]);
        }

        RetransmitAction availableAction = null;
        for (final RetransmitAction action : retransmitActionPool)
        {
            switch (action.state)
            {
                case INACTIVE:
                    if (null == availableAction)
                    {
                        availableAction = action;
                    }
                    break;

                case DELAYED:
                case LINGERING:
                    if (action.termId == termId &&
                        action.termOffset <= termOffset && termOffset < action.termOffset + action.length)
                    {
                        return null;
                    }

                    if (!hasGroupSemantics)
                    {
                        // this is unicast, and the NAK does NOT overlap the previous one, so just reuse it
                        availableAction = action;
                    }
                    break;
            }
        }

        if (hasGroupSemantics)
        {
            if (null != availableAction)
            {
                return addRetransmit(availableAction);
            }

            retransmitOverflowCounter.increment();
        }

        return availableAction;
    }

    private RetransmitAction scanForExistingRetransmit(final int termId, final int termOffset)
    {
        if (0 == activeRetransmitCount)
        {
            return null;
        }

        for (final RetransmitAction action : retransmitActionPool)
        {
            switch (action.state)
            {
                case DELAYED:
                case LINGERING:
                    if (action.termId == termId && action.termOffset == termOffset)
                    {
                        return action;
                    }
                    break;

                default:
                    break;
            }
        }

        return null;
    }

    private RetransmitAction addRetransmit(final RetransmitAction retransmitAction)
    {
        ++activeRetransmitCount;
        return retransmitAction;
    }

    private void removeRetransmit(final RetransmitAction action)
    {
        --activeRetransmitCount;
        action.cancel();
    }

    static final class RetransmitAction
    {
        @SuppressWarnings("JavadocVariable")
        enum State
        {
            DELAYED,
            LINGERING,
            INACTIVE
        }

        long expiryNs;
        int termId;
        int termOffset;
        int length;
        State state = State.INACTIVE;

        void delay(final long delayNs, final long nowNs)
        {
            state = State.DELAYED;
            expiryNs = nowNs + delayNs;
        }

        void linger(final long timeoutNs, final long nowNs)
        {
            state = State.LINGERING;
            expiryNs = nowNs + timeoutNs;
        }

        void cancel()
        {
            state = State.INACTIVE;
        }
    }
}
