/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aeron.archive.client;

import io.aeron.exceptions.AeronException;

/**
 * Exception raised when using a {@link PersistentSubscription}.
 */
public final class PersistentSubscriptionException extends AeronException
{
    private static final long serialVersionUID = -6953277621087533657L;

    /**
     * The reason for this exception.
     */
    private final Reason reason;

    /**
     * Reason a {@link PersistentSubscriptionException} occurred.
     */
    public enum Reason
    {
        /**
         * A generic reason in case no specific reason is available.
         */
        GENERIC,
        /**
         * No recording exists with the specified recording id.
         */
        RECORDING_NOT_FOUND,
        /**
         * The requested live stream id does not match the stream id for the recording.
         */
        STREAM_ID_MISMATCH,
        /**
         * The requested start position is not available for the specified recording.
         */
        INVALID_START_POSITION
    }

    /**
     * Persistent Subscription exception with a detailed message and provided reason.
     *
     * @param reason  for the error.
     * @param message providing detail on the error.
     */
    public PersistentSubscriptionException(final Reason reason, final String message)
    {
        super(message);
        this.reason = reason;
    }

    /**
     * Returns the reason indicating the type of error that caused the exception.
     *
     * @return reason
     */
    public Reason reason()
    {
        return reason;
    }
}
