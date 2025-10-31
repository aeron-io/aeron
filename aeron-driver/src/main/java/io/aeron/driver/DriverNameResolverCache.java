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
package io.aeron.driver;

import io.aeron.protocol.ResolutionEntryFlyweight;
import org.agrona.collections.ArrayListUtil;
import org.agrona.concurrent.status.AtomicCounter;

import java.util.ArrayList;
import java.util.Arrays;

final class DriverNameResolverCache
{
    private final ArrayList<CacheEntry> entries = new ArrayList<>();
    private final long timeoutMs;

    DriverNameResolverCache(final long timeoutMs)
    {
        this.timeoutMs = timeoutMs;
    }

    CacheEntry lookup(final String name, final byte type)
    {
        for (final CacheEntry entry : entries)
        {
            if (type == entry.type && fullLengthMatch(entry.name, name))
            {
                return entry;
            }
        }

        return null;
    }

    void addOrUpdateEntry(
        final byte[] name,
        final int nameLength,
        final long nowMs,
        final byte type,
        final byte[] address,
        final int port,
        final AtomicCounter cacheEntriesCounter)
    {
        CacheEntry entry = findEntryIndexByNameAndType(name, nameLength, type);
        final int addressLength = ResolutionEntryFlyweight.addressLength(type);

        if (null == entry)
        {
            entry = new CacheEntry(
                Arrays.copyOf(name, nameLength),
                type,
                nowMs,
                nowMs + timeoutMs,
                Arrays.copyOf(address, addressLength),
                port);
            entries.add(entry);
            cacheEntriesCounter.setRelease(entries.size());
        }
        else
        {
            entry.timeOfLastActivityMs = nowMs;
            entry.deadlineMs = nowMs + timeoutMs;

            if (port != entry.port || !fullLengthMatch(entry.address, address, addressLength))
            {
                entry.address = Arrays.copyOf(address, addressLength);
                entry.port = port;
            }
        }
    }

    int timeoutOldEntries(final long nowMs, final AtomicCounter cacheEntriesCounter)
    {
        int workCount = 0;

        final ArrayList<CacheEntry> listOfEntries = this.entries;
        for (int lastIndex = listOfEntries.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final CacheEntry entry = listOfEntries.get(i);

            if (entry.deadlineMs - nowMs < 0)
            {
                ArrayListUtil.fastUnorderedRemove(listOfEntries, i, lastIndex--);
                cacheEntriesCounter.setRelease(listOfEntries.size());
                workCount++;
            }
        }

        return workCount;
    }

    Iterator resetIterator()
    {
        iterator.cache = this;
        iterator.index = -1;

        return iterator;
    }

    static final class Iterator
    {
        int index = -1;
        DriverNameResolverCache cache;

        boolean hasNext()
        {
            return (index + 1) < cache.entries.size();
        }

        CacheEntry next()
        {
            return cache.entries.get(++index);
        }

        void rewindNext()
        {
            --index;
        }
    }

    private final Iterator iterator = new Iterator();

    static boolean fullLengthMatch(final byte[] expected, final byte[] actual, final int actualLength)
    {
        return actualLength == expected.length &&
            Arrays.equals(expected, 0, actualLength, actual, 0, actualLength);
    }

    static boolean fullLengthMatch(final byte[] expected, final String actual)
    {
        final int length = actual.length();
        if (length != expected.length)
        {
            return false;
        }

        for (int i = 0; i < length; i++)
        {
            if (expected[i] != actual.charAt(i))
            {
                return false;
            }
        }

        return true;
    }

    private CacheEntry findEntryIndexByNameAndType(final byte[] name, final int nameLength, final byte type)
    {
        for (final CacheEntry entry : entries)
        {
            if (type == entry.type && fullLengthMatch(entry.name, name, nameLength))
            {
                return entry;
            }
        }

        return null;
    }

    static final class CacheEntry
    {
        long deadlineMs;
        long timeOfLastActivityMs;
        int port;
        final byte type;
        final byte[] name;
        byte[] address;

        CacheEntry(
            final byte[] name,
            final byte type,
            final long timeOfLastActivityMs,
            final long deadlineMs,
            final byte[] address,
            final int port)
        {
            this.name = name;
            this.type = type;
            this.timeOfLastActivityMs = timeOfLastActivityMs;
            this.deadlineMs = deadlineMs;
            this.address = address;
            this.port = port;
        }
    }
}
