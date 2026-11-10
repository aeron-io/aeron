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
package io.aeron.archive.checksum;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ChecksumsTest
{
    @Test
    void crc32()
    {
        final Checksum instance = Checksums.crc32();
        assertNotNull(instance);
        assertSame(instance, Checksums.crc32());
    }

    @Test
    void crc32c()
    {
        final Checksum instance = Checksums.crc32c();
        assertNotNull(instance);
        assertSame(instance, Checksums.crc32c());
    }

    @ParameterizedTest
    @ValueSource(strings = { "CRC-32", "io.aeron.archive.checksum.Crc32", "org.agrona.checksum.Crc32" })
    void newInstanceReturnsSameInstanceOfCrc32(final String alias)
    {
        assertSame(Checksums.crc32(), Checksums.newInstance(alias));
    }

    @ParameterizedTest
    @ValueSource(strings = { "CRC-32C", "io.aeron.archive.checksum.Crc32c", "org.agrona.checksum.Crc32c" })
    void newInstanceReturnsSameInstanceOfCrc32c(final String alias)
    {
        assertSame(Checksums.crc32c(), Checksums.newInstance(alias));
    }

    @ParameterizedTest
    @NullAndEmptySource
    void newInstanceThrowsNullPointerExceptionIfClassNameIsNull(final String alias)
    {
        assertThrows(IllegalArgumentException.class, () -> Checksums.newInstance(alias));
    }

    @Test
    void newInstanceThrowsIllegalArgumentExceptionIfClassIsNotFound()
    {
        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class, () -> Checksums.newInstance("a.b.c.MissingClass"));
        assertEquals(ClassNotFoundException.class, exception.getCause().getClass());
    }

    @Test
    void newInstanceThrowsIllegalArgumentExceptionIfInstanceCannotBeCreated()
    {
        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class, () -> Checksums.newInstance(TimeUnit.class.getName()));
        assertEquals(NoSuchMethodException.class, exception.getCause().getClass());
    }

    @Test
    void newInstanceThrowsClassCastExceptionIfCreatedInstanceDoesNotImplementChecksumInterface()
    {
        assertThrows(ClassCastException.class, () -> Checksums.newInstance(Object.class.getName()));
    }

    @Test
    void newInstanceCreatesANewInstanceOfTheSpecifiedClass()
    {
        final Checksum instance1 = Checksums.newInstance(TestChecksum.class.getName());
        final Checksum instance2 = Checksums.newInstance(TestChecksum.class.getName());
        assertInstanceOf(TestChecksum.class, instance1);
        assertInstanceOf(TestChecksum.class, instance2);
        assertNotSame(instance1, instance2);
    }

    static class TestChecksum implements Checksum
    {
        @Override
        public int compute(final long address, final int offset, final int length)
        {
            return 0;
        }
    }
}
