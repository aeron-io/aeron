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

package io.aeron.topology;

import org.agrona.collections.Int2IntHashMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.agrona.AsciiEncoding.parseIntAscii;

class PerCpuIdReader
{
    private final Int2IntHashMap idCache = new Int2IntHashMap(-1);
    private final Path sysfsRoot;
    private final String idFileDirectory;

    PerCpuIdReader(final Path sysfsRoot, final String idFileDirectory)
    {
        this.sysfsRoot = sysfsRoot;
        this.idFileDirectory = idFileDirectory;
    }

    private int loadIdFile(final int cpu) throws IOException
    {
        final Path idPath = sysfsRoot.resolve("cpu%d".formatted(cpu)).resolve(idFileDirectory);
        final String id = Files.readString(idPath);
        return parseIntAscii(id, 0, id.length());
    }

    int loadId(final int cpu) throws IOException
    {
        final int id = idCache.get(cpu);
        if (idCache.missingValue() == id)
        {
            final int loadedId = this.loadIdFile(cpu);
            idCache.put(cpu, loadedId);
            return loadedId;
        }
        return id;
    }
}
