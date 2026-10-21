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

import org.gradle.api.file.Directory;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.RegularFile;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.JavaExec;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskContainer;


/**
 * Registers tasks that check a module's C sources against the configuration options and counters recorded by the
 * {@code aeron-annotations} processors. Created by the {@code aeron.config-info} plugin as {@code aeronConfigInfo}.
 */
public abstract class AeronConfigInfoExtension
{
    private final TaskContainer tasks;
    private final FileCollection toolClasspath;
    private final Provider<Directory> infoDir;
    private final Provider<Directory> cSourceDir;

    /**
     * Create the extension.
     *
     * @param tasks         container to register tasks in.
     * @param toolClasspath classpath of the {@code aeron-annotations} tools.
     * @param infoDir       directory the annotation processors write their {@code .dat} files to.
     * @param cSourceDir    directory of the module's C sources.
     */
    public AeronConfigInfoExtension(
        final TaskContainer tasks,
        final FileCollection toolClasspath,
        final Provider<Directory> infoDir,
        final Provider<Directory> cSourceDir)
    {
        this.tasks = tasks;
        this.toolClasspath = toolClasspath;
        this.infoDir = infoDir;
        this.cSourceDir = cSourceDir;
    }

    /**
     * Register {@code validateConfigExpectations}, which checks the C sources against the module's configuration
     * options.
     */
    public void validateConfigExpectations()
    {
        registerValidation(
            "validateConfigExpectations", "io.aeron.config.validation.ValidateConfigExpectationsTask", "config-info.dat");
    }

    /**
     * Register {@code validateCounterExpectations}, which checks the C sources against the module's counters.
     */
    public void validateCounterExpectations()
    {
        registerValidation(
            "validateCounterExpectations",
            "io.aeron.counter.validation.ValidateCounterExpectationsTask",
            "counter-info.dat");
    }

    private void registerValidation(final String taskName, final String mainClass, final String infoFileName)
    {
        final Provider<RegularFile> infoFile = infoDir.map((dir) -> dir.file(infoFileName));

        tasks.register(taskName, JavaExec.class, (task) ->
        {
            task.getInputs().file(infoFile).withPathSensitivity(PathSensitivity.RELATIVE);
            task.getInputs().dir(cSourceDir).withPathSensitivity(PathSensitivity.RELATIVE);
            task.getMainClass().set(mainClass);
            task.setClasspath(toolClasspath);
            task.getArgumentProviders().add(new FilePathArgumentProvider(infoFile, cSourceDir));
        });
    }
}
