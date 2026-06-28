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
package io.aeron;

import org.agrona.ErrorHandler;
import org.agrona.LangUtil;

/**
 * Error handler that will rethrow a {@link Throwable} as an unchecked exception.
 */
public final class RethrowingErrorHandler implements ErrorHandler
{
    /**
     * Default constructor.
     */
    public RethrowingErrorHandler()
    {
    }

    /**
     * Singleton instance to avoid allocation.
     */
    public static final RethrowingErrorHandler INSTANCE = new RethrowingErrorHandler();

    /**
     * {@inheritDoc}
     */
    public void onError(final Throwable ex)
    {
        LangUtil.rethrowUnchecked(ex);
    }
}
