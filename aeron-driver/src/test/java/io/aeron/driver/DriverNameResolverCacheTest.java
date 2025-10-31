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

import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.Arrays;

import static io.aeron.protocol.ResolutionEntryFlyweight.ADDRESS_LENGTH_IP6;
import static io.aeron.protocol.ResolutionEntryFlyweight.RES_TYPE_NAME_TO_IP4_MD;
import static io.aeron.protocol.ResolutionEntryFlyweight.RES_TYPE_NAME_TO_IP6_MD;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

class DriverNameResolverCacheTest
{
    private final AtomicCounter cacheEntriesCounter = mock(AtomicCounter.class);
    private final byte[] name = new byte[32];
    private final byte[] address = new byte[ADDRESS_LENGTH_IP6];

    @Test
    void shouldAddNewEntries()
    {
        final DriverNameResolverCache cache = new DriverNameResolverCache(1000);

        name[0] = 'A';
        Arrays.fill(address, (byte)0);
        cache.addOrUpdateEntry(name, 1, 1, RES_TYPE_NAME_TO_IP4_MD, address, 8050, cacheEntriesCounter);

        Arrays.fill(name, (byte)'A');
        Arrays.fill(address, (byte)1);
        cache.addOrUpdateEntry(name, 2, 2, RES_TYPE_NAME_TO_IP4_MD, address, 8050, cacheEntriesCounter);

        Arrays.fill(name, (byte)'B');
        Arrays.fill(address, (byte)2);
        cache.addOrUpdateEntry(name, 1, 3, RES_TYPE_NAME_TO_IP4_MD, address, 8052, cacheEntriesCounter);

        Arrays.fill(name, (byte)'A');
        Arrays.fill(address, (byte)127);
        cache.addOrUpdateEntry(name, 2, 4, RES_TYPE_NAME_TO_IP4_MD, address, 9090, cacheEntriesCounter);

        Arrays.fill(name, (byte)'B');
        Arrays.fill(address, (byte)5);
        cache.addOrUpdateEntry(name, 1, 5, RES_TYPE_NAME_TO_IP6_MD, address, 8050, cacheEntriesCounter);

        final DriverNameResolverCache.Iterator iterator = cache.resetIterator();
        assertCacheEntry(
            iterator.next(), new byte[]{ 'A' }, RES_TYPE_NAME_TO_IP4_MD, 8050, new byte[]{ 0, 0, 0, 0 }, 1, 1001);

        assertCacheEntry(
            iterator.next(),
            new byte[]{ 'A', 'A' },
            RES_TYPE_NAME_TO_IP4_MD,
            9090,
            new byte[]{ 127, 127, 127, 127 },
            4,
            1004);

        assertCacheEntry(
            iterator.next(), new byte[]{ 'B' }, RES_TYPE_NAME_TO_IP4_MD, 8052, new byte[]{ 2, 2, 2, 2 }, 3, 1003);

        assertCacheEntry(
            iterator.next(),
            new byte[]{ 'B' },
            RES_TYPE_NAME_TO_IP6_MD,
            8050,
            new byte[]{ 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5 },
            5,
            1005);

        assertFalse(iterator.hasNext());

        final InOrder inOrder = inOrder(cacheEntriesCounter);
        inOrder.verify(cacheEntriesCounter).setRelease(1);
        inOrder.verify(cacheEntriesCounter).setRelease(2);
        inOrder.verify(cacheEntriesCounter).setRelease(3);
        inOrder.verify(cacheEntriesCounter).setRelease(4);
    }

    @Test
    void shouldLookupByTypeAndName()
    {
        final DriverNameResolverCache cache = new DriverNameResolverCache(5000);

        Arrays.fill(name, (byte)'A');
        Arrays.fill(address, (byte)0);
        cache.addOrUpdateEntry(name, 1, 1, RES_TYPE_NAME_TO_IP4_MD, address, 5555, cacheEntriesCounter);

        Arrays.fill(address, (byte)1);
        cache.addOrUpdateEntry(name, 2, 2, RES_TYPE_NAME_TO_IP4_MD, address, 6666, cacheEntriesCounter);

        Arrays.fill(address, (byte)2);
        cache.addOrUpdateEntry(name, 1, 3, RES_TYPE_NAME_TO_IP6_MD, address, 7777, cacheEntriesCounter);

        Arrays.fill(name, (byte)'B');
        Arrays.fill(address, (byte)1);
        cache.addOrUpdateEntry(name, 2, 4, RES_TYPE_NAME_TO_IP4_MD, address, 8888, cacheEntriesCounter);

        Arrays.fill(name, (byte)'C');
        Arrays.fill(address, (byte)0);
        cache.addOrUpdateEntry(name, 1, 5, RES_TYPE_NAME_TO_IP6_MD, address, 9999, cacheEntriesCounter);

        DriverNameResolverCache.CacheEntry entry = cache.lookup("A", RES_TYPE_NAME_TO_IP4_MD);
        assertCacheEntry(
            entry,
            new byte[]{ 'A' },
            RES_TYPE_NAME_TO_IP4_MD,
            5555,
            new byte[]{ 0, 0, 0, 0 },
            1,
            5001);

        entry = cache.lookup("A", RES_TYPE_NAME_TO_IP6_MD);
        assertCacheEntry(
            entry,
            new byte[]{ 'A' },
            RES_TYPE_NAME_TO_IP6_MD,
            7777,
            new byte[]{ 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2 },
            3,
            5003);

        // case-sensitive match
        assertNull(cache.lookup("a", RES_TYPE_NAME_TO_IP4_MD));

        // wrong type
        assertNull(cache.lookup("AA", RES_TYPE_NAME_TO_IP6_MD));

        // only full matches allowed
        assertNull(cache.lookup("B", RES_TYPE_NAME_TO_IP4_MD));

        entry = cache.lookup("BB", RES_TYPE_NAME_TO_IP4_MD);
        assertCacheEntry(
            entry,
            new byte[]{ 'B', 'B' },
            RES_TYPE_NAME_TO_IP4_MD,
            8888,
            new byte[]{ 1, 1, 1, 1 },
            4,
            5004);
    }

    private static void assertCacheEntry(
        final DriverNameResolverCache.CacheEntry entry,
        final byte[] name,
        final byte type,
        final int port,
        final byte[] address,
        final long timeOfLastActivityMs,
        final long deadlineMs)
    {
        assertNotNull(entry);
        assertArrayEquals(name, entry.name);
        assertEquals(type, entry.type);
        assertEquals(port, entry.port);
        assertArrayEquals(address, entry.address);
        assertEquals(timeOfLastActivityMs, entry.timeOfLastActivityMs);
        assertEquals(deadlineMs, entry.deadlineMs);
    }
}
