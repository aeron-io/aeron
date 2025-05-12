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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;

import static io.aeron.exceptions.StorageSpaceException.isStorageSpaceError;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageSpaceExceptionTest
{
    @Test
    void isStorageSpaceErrorReturnsFalseIfNull()
    {
        assertFalse(isStorageSpaceError(null));
    }

    @Test
    void isStorageSpaceErrorReturnsFalseIfNotIOException()
    {
        assertFalse(isStorageSpaceError(new IllegalArgumentException("No space left on device")));
    }

    @Test
    void isStorageSpaceErrorReturnsFalseIfWrongMessage()
    {
        assertFalse(isStorageSpaceError(new IllegalArgumentException(
            "Es steht nicht genug Speicherplatz auf dem Datenträger zur Verfügung")));
    }

    @ParameterizedTest
    @MethodSource("errors")
    void isStorageSpaceErrorReturnsTrueWhenIOExceptionWithAParticularMessage(final Throwable exception)
    {
        assertTrue(isStorageSpaceError(exception));
    }

    private static List<Throwable> errors()
    {
        return Arrays.asList(
            new StorageSpaceException("test"),
            new AeronException(new StorageSpaceException("test2")),
            new IOException("No space left on device") /* Linux */,
            new IOException("There is not enough space on the disk") /* Windows */,
            new IOException(
                "something else", new UncheckedIOException(null, new IOException("No space left on device"))),
            new AeronException(new IllegalArgumentException(new IOException("There is not enough space on the disk"))));
    }
}
