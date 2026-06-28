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
package io.aeron.driver.exceptions;

import io.aeron.ErrorCode;
import io.aeron.exceptions.ControlProtocolException;

/**
 * Indicates the subscription is unknown to the driver when a request to remove it has been received.
 */
public class UnknownSubscriptionException extends ControlProtocolException
{
    private static final long serialVersionUID = 8934006869519172332L;

    /**
     * Exception with provided message and {@link io.aeron.exceptions.AeronException.Category#ERROR}.
     *
     * @param message to detail the exception.
     */
    public UnknownSubscriptionException(final String message)
    {
        super(ErrorCode.UNKNOWN_SUBSCRIPTION, message);
    }
}
