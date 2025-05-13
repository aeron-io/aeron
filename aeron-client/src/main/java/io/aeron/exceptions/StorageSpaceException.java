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

import java.io.IOException;

/**
 * A request to allocate a resource (e.g. log buffer) failed due to insufficient storage space available.
 */
public class StorageSpaceException extends AeronException
{
    private static final long serialVersionUID = -552384561600482276L;

    /**
     * Construct the exception for the with detailed message.
     *
     * @param message detail for the exception.
     */
    public StorageSpaceException(final String message)
    {
        super(message);
    }

    /**
     * Check if given exception denotes an out of disc space error, i.e. which on Linux is represented by error code
     * {@code ENOSPC(28)} and on Windows by error code  {@code ERROR_DISK_FULL(112)}.
     *
     * @param error to check.
     * @return {@code true} if cause is {@link java.io.IOException} with a specific error.
     */
    public static boolean isStorageSpaceError(final Throwable error)
    {
        Throwable cause = error;
        while (null != cause)
        {
            if (cause instanceof StorageSpaceException)
            {
                return true;
            }
            else if (cause instanceof IOException)
            {
                final String msg = cause.getMessage();
                if ("No space left on device".equals(msg) || "There is not enough space on the disk".equals(msg))
                {
                    return true;
                }
            }
            cause = cause.getCause();
        }
        return false;
    }
}
