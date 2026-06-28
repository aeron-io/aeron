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
package io.aeron.test.driver;

import io.aeron.CounterProvider;
import io.aeron.driver.DefaultNameResolver;
import io.aeron.driver.NameResolverAgent;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

import java.net.InetAddress;
import java.util.Map;
import java.util.Objects;

import static io.aeron.Aeron.NULL_VALUE;
import static org.agrona.BitUtil.SIZE_OF_INT;

public class RedirectingNameResolver implements NameResolverAgent
{
    public static final int DISABLE_RESOLUTION = -1;
    public static final int USE_INITIAL_RESOLUTION_HOST = 0;
    public static final int USE_RE_RESOLUTION_HOST = 1;
    public static final int NAME_ENTRY_COUNTER_TYPE_ID = 2001;
    public static final int EXPECTED_COLUMN_COUNT = 3;
    private static final String INVALID_HOSTNAME_SENTINEL = "forced-resolve-failure.invalid";

    private final Map<String, NameEntry> nameToEntryMap = new Object2ObjectHashMap<>();
    private final String csvConfiguration;

    public RedirectingNameResolver(final String csvConfiguration)
    {
        this.csvConfiguration = csvConfiguration;
        final String[] lines = csvConfiguration.split("\\|");
        for (final String line : lines)
        {
            final String[] params = line.split(",");
            if (EXPECTED_COLUMN_COUNT != params.length)
            {
                throw new IllegalArgumentException("Expected 3 elements per row");
            }

            final NameEntry nameEntry = new NameEntry(params[0], params[1], params[2]);
            nameToEntryMap.put(nameEntry.name, nameEntry);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void init(final CountersReader countersReader, final CounterProvider counterProvider)
    {
        countersReader.forEach((counterId, typeId, keyBuffer, label) ->
        {
            if (typeId == NAME_ENTRY_COUNTER_TYPE_ID && keyBuffer.capacity() > SIZE_OF_INT)
            {
                final String entryName = keyBuffer.getStringAscii(0);
                final NameEntry entry = nameToEntryMap.get(entryName);
                if (null != entry)
                {
                    entry.counter(new AtomicCounter(countersReader.valuesBuffer(), counterId));
                }
            }
        });

        final ExpandableArrayBuffer tmpBuffer = new ExpandableArrayBuffer();
        for (final NameEntry nameEntry : nameToEntryMap.values())
        {
            if (null == nameEntry.counter)
            {
                final int keyLength = tmpBuffer.putStringAscii(0, nameEntry.name);
                final int labelLength = tmpBuffer.putStringWithoutLengthAscii(keyLength, nameEntry.toString());

                final AtomicCounter atomicCounter = counterProvider.newCounter(
                    NAME_ENTRY_COUNTER_TYPE_ID,
                    tmpBuffer,
                    0,
                    keyLength,
                    tmpBuffer,
                    keyLength,
                    labelLength);
                nameEntry.counter(atomicCounter);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public String lookup(final String name, final String uriParamName, final boolean isReLookup)
    {
        return name.endsWith(":X") ? name.substring(0, name.length() - 2) : name;
    }

    /**
     * {@inheritDoc}
     */
    public InetAddress resolve(final String name, final String uriParamName, final boolean isReResolution)
    {
        final NameEntry nameEntry = nameToEntryMap.get(name);
        final String hostname = null != nameEntry ? nameEntry.redirectHost(name) : name;

        InetAddress resolvedAddress = null;
        if (!Objects.equals(INVALID_HOSTNAME_SENTINEL, hostname))
        {
            resolvedAddress = DefaultNameResolver.INSTANCE.resolve(hostname, uriParamName, isReResolution);
        }

        return resolvedAddress;
    }

    public String csvConfiguration()
    {
        return csvConfiguration;
    }

    public static boolean updateNameResolutionStatus(
        final CountersReader counters,
        final String hostname,
        final int operationValue)
    {
        final MutableInteger nameCounterId = new MutableInteger(NULL_VALUE);
        counters.forEach((counterId, typeId, keyBuffer, label) ->
        {
            if (typeId == NAME_ENTRY_COUNTER_TYPE_ID && hostname.equals(keyBuffer.getStringAscii(0)))
            {
                nameCounterId.set(counterId);
            }
        });

        final boolean counterFound = NULL_VALUE != nameCounterId.get();
        if (counterFound)
        {
            final AtomicCounter nameCounter = new AtomicCounter(counters.valuesBuffer(), nameCounterId.get());

            nameCounter.set(operationValue);
        }

        return counterFound;
    }

    static final class NameEntry
    {
        private final String name;
        private final String initialResolutionHost;
        private final String reResolutionHost;
        private AtomicCounter counter;

        NameEntry(final String name, final String initialResolutionHost, final String reResolutionHost)
        {
            this.name = name;
            this.initialResolutionHost = initialResolutionHost;
            this.reResolutionHost = reResolutionHost;
        }

        void counter(final AtomicCounter counter)
        {
            this.counter = counter;
        }

        String redirectHost(final String name)
        {
            final long operation = null != counter ? counter.get() : USE_INITIAL_RESOLUTION_HOST;
            if (DISABLE_RESOLUTION == operation)
            {
                return INVALID_HOSTNAME_SENTINEL;
            }
            else if (USE_INITIAL_RESOLUTION_HOST == operation)
            {
                return initialResolutionHost;
            }
            else if (USE_RE_RESOLUTION_HOST == operation)
            {
                return reResolutionHost;
            }
            else
            {
                return name;
            }
        }

        public String toString()
        {
            return "NameEntry{" +
                "name='" + name + '\'' +
                ", initialResolutionHost='" + initialResolutionHost + '\'' +
                ", reResolutionHost='" + reResolutionHost + "'}";
        }
    }
}
