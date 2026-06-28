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

import org.agrona.Strings;
import org.agrona.checksum.Crc32;
import org.agrona.checksum.Crc32c;

/**
 * Factory and common methods for working with {@link Checksum} instances.
 */
public final class Checksums
{
    private static final Checksum CRC_32 = Crc32.INSTANCE::compute;
    private static final Checksum CRC_32C = Crc32c.INSTANCE::compute;

    private Checksums()
    {
    }

    /**
     * Returns an instance of {@link Checksum} that computes CRC-32 checksums.
     *
     * @return CRC-32 implementation.
     */
    public static Checksum crc32()
    {
        return CRC_32;
    }

    /**
     * Returns an instance of {@link Checksum} that computes CRC-32C checksums.
     *
     * @return CRC-32C implementation.
     */
    public static Checksum crc32c()
    {
        return CRC_32C;
    }

    /**
     * Factory method to create an instance of the {@link Checksum} interface.
     *
     * @param className fully qualified class name or an alias of the {@link Checksum} implementation.
     * @return a {@link Checksum} instance.
     * @throws IllegalArgumentException if {@code className == null} or empty.
     * @throws IllegalStateException    if an attempt was made to acquire CRC-32C while running on JDK 8.
     * @throws IllegalArgumentException if an error occurs while creating a {@link Checksum} instance.
     * @throws ClassCastException       if created instance does not implement the {@link Checksum} interface.
     * @see #crc32c()
     */
    public static Checksum newInstance(final String className)
    {
        if (Strings.isEmpty(className))
        {
            throw new IllegalArgumentException("className is empty");
        }

        return switch (className)
        {
            case "CRC-32":
            case "io.aeron.archive.checksum.Crc32":
            case "org.agrona.checksum.Crc32":
                yield crc32();
            case "CRC-32C":
            case "io.aeron.archive.checksum.Crc32c":
            case "org.agrona.checksum.Crc32c":
                yield crc32c();
            default:
            {
                try
                {
                    final Class<?> klass = Class.forName(className);
                    final Object instance = klass.getDeclaredConstructor().newInstance();
                    yield (Checksum)instance;
                }
                catch (final ReflectiveOperationException ex)
                {
                    throw new IllegalArgumentException(
                        "failed to create Checksum instance for class: " + className, ex);
                }
            }
        };
    }
}
