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

import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.JavaExec;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import java.util.List;

/**
 * Generates a Markdown reference of a module's configuration options by running
 * {@code io.aeron.config.docgen.GenerateConfigDocTask}. The {@code aeron-annotations} jar must be put on the
 * {@link #getClasspath() classpath}.
 */
@CacheableTask
public abstract class GenerateConfigDoc extends JavaExec
{
    /**
     * Create new task instance.
     */
    public GenerateConfigDoc()
    {
        getMainClass().set("io.aeron.config.docgen.GenerateConfigDocTask");
    }

    /**
     * Configuration options recorded by the {@code aeron-annotations} processors when the module was compiled.
     *
     * @return configuration options recorded by the {@code aeron-annotations} processors.
     */
    @InputFile
    @PathSensitive(PathSensitivity.NONE)
    public abstract RegularFileProperty getConfigInfoFile();

    /**
     * Markdown file the reference is written to.
     *
     * @return Markdown file the reference is written to.
     */
    @OutputFile
    public abstract RegularFileProperty getDocFile();

    /**
     * {@inheritDoc}
     */
    @TaskAction
    public void exec()
    {
        setArgs(List.of(
            getConfigInfoFile().get().getAsFile().getAbsolutePath(),
            getDocFile().get().getAsFile().getAbsolutePath()));

        super.exec();
    }
}
