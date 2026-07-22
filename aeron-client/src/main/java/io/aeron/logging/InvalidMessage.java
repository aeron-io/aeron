/*
 * Copyright 2014-2026 Real Logic Limited.
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
package io.aeron.logging;

import io.aeron.exceptions.AeronException;

import java.io.Serial;

/**
 * Exception when an invalid message is received within the event reader.
 */
public class InvalidMessage extends AeronException
{
    @Serial
    private static final long serialVersionUID = 2520658673245952222L;

    InvalidMessage(final String message)
    {
        super(message);
    }
}
