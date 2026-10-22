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
package io.aeron.build;

import org.gradle.api.file.FileSystemLocation;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Internal;
import org.gradle.process.CommandLineArgumentProvider;

import java.util.ArrayList;
import java.util.List;

/**
 * Passes files as absolute paths on the command line, resolving them only when the task runs. The files must be
 * declared as task inputs or outputs separately, so they are not tracked here.
 */
public final class FilePathArgumentProvider implements CommandLineArgumentProvider
{
    private final List<Provider<? extends FileSystemLocation>> files;

    /**
     * Create a new argument provider.
     *
     * @param files to pass, in order.
     */
    @SafeVarargs
    public FilePathArgumentProvider(final Provider<? extends FileSystemLocation>... files)
    {
        this.files = List.of(files);
    }

    /**
     * Files passed as arguments, in order.
     *
     * @return files passed as arguments.
     */
    @Internal
    public List<Provider<? extends FileSystemLocation>> getFiles()
    {
        return files;
    }

    /**
     * {@inheritDoc}
     */
    public Iterable<String> asArguments()
    {
        final List<String> args = new ArrayList<>(files.size());
        for (final Provider<? extends FileSystemLocation> file : files)
        {
            args.add(file.get().getAsFile().getAbsolutePath());
        }

        return args;
    }
}
