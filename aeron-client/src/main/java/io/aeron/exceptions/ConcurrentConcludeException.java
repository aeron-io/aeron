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

/**
 * Conclude has been called concurrently on a Context. The caller that receives this should not close the
 * concluded context as it will be owned by another caller.
 */
public class ConcurrentConcludeException extends AeronException
{
    /**
     * Create a default ConcurrentConcludeException.
     */
    public ConcurrentConcludeException()
    {
    }

    private static final long serialVersionUID = 684839776662091577L;
}
