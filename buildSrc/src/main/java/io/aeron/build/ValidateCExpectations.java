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

import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.JavaExec;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.work.DisableCachingByDefault;

import java.util.List;

/**
 * Checks a module's C sources against what the {@code aeron-annotations} processors recorded about its Java sources,
 * by running one of the validation tools in {@code aeron-annotations}. The tool is chosen with
 * {@link #getMainClass() mainClass}, and the {@code aeron-annotations} jar must be put on the
 * {@link #getClasspath() classpath}.
 */
@DisableCachingByDefault(because = "Produces no output, it only checks the C sources")
public abstract class ValidateCExpectations extends JavaExec
{
    /**
     * Configuration options or counters recorded by the {@code aeron-annotations} processors when the module was
     * compiled.
     *
     * @return configuration options or counters recorded by the {@code aeron-annotations} processors.
     */
    @InputFile
    @PathSensitive(PathSensitivity.NONE)
    public abstract RegularFileProperty getInfoFile();

    /**
     * Directory of the C sources to check.
     *
     * @return directory of the C sources to check.
     */
    @InputDirectory
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract DirectoryProperty getSourceDir();

    /**
     * {@inheritDoc}
     */
    @TaskAction
    public void exec()
    {
        setArgs(List.of(
            getInfoFile().get().getAsFile().getAbsolutePath(),
            getSourceDir().get().getAsFile().getAbsolutePath()));

        super.exec();
    }
}
