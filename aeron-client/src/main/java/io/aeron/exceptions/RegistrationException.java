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
package io.aeron.exceptions;

import io.aeron.*;

/**
 * Caused when an error occurs during addition, modification, or release of client resources such as
 * {@link Publication}s, {@link Subscription}s, or {@link Counter}s.
 */
public class RegistrationException extends AeronException
{
    private static final long serialVersionUID = 9076689715024983035L;

    /**
     * The correlation id of the command to register the resource action.
     */
    private final long correlationId;

    /**
     * Value of the {@link #errorCode()} encoded as an int.
     */
    private final int errorCodeValue;

    /**
     * The {@link ErrorCode} for the specific exception.
     */
    private final ErrorCode errorCode;

    /**
     * Construct an exception to represent an error which occurred during registration of a resource such as a
     * Publication, Subscription, or Counter.
     *
     * @param correlationId  of the command to register the resource.
     * @param errorCodeValue in case the {@link ErrorCode} is unknown to the client version.
     * @param errorCode      indicating type of error experienced by the media driver.
     * @param msg            proving more detail.
     */
    public RegistrationException(
        final long correlationId, final int errorCodeValue, final ErrorCode errorCode, final String msg)
    {
        super(
            stripCategoryName(msg) + ", errorCodeValue=" + errorCodeValue,
            ControlProtocolException.errorToCategory(errorCode));
        this.correlationId = correlationId;
        this.errorCode = errorCode;
        this.errorCodeValue = errorCodeValue;
    }

    /**
     * Construct a from another to be nested, useful when exceptions are reported asynchronously and need to be
     * rethrown from other places in the code.
     *
     * @param cause original RegistrationException to be stored as the exception cause.
     */
    public RegistrationException(final RegistrationException cause)
    {
        super(stripCategoryName(cause.getMessage()), cause, cause.category());
        this.correlationId = cause.correlationId;
        this.errorCode = cause.errorCode;
        this.errorCodeValue = cause.errorCodeValue;
    }

    /**
     * Get the correlation id of the command to register the resource action.
     *
     * @return the correlation id of the command to register the resource action.
     */
    public long correlationId()
    {
        return correlationId;
    }

    /**
     * Get the {@link ErrorCode} for the specific exception.
     *
     * @return the {@link ErrorCode} for the specific exception.
     */
    public ErrorCode errorCode()
    {
        return errorCode;
    }

    /**
     * Value of the {@link #errorCode()} encoded as an int. This can provide additional information when a
     * {@link ErrorCode#UNKNOWN_CODE_VALUE} is returned.
     *
     * @return value of the errorCode encoded as an int.
     */
    public int errorCodeValue()
    {
        return errorCodeValue;
    }

    private static String stripCategoryName(final String msg)
    {
        if (null != msg && msg.length() > 7)
        {
            if (msg.startsWith("ERROR - ") || msg.startsWith("FATAL - "))
            {
                return msg.substring(8);
            }

            if (msg.startsWith("WARN - "))
            {
                return msg.substring(7);
            }
        }

        return msg;
    }
}
