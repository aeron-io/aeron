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

import org.gradle.api.DefaultTask;
import org.gradle.api.artifacts.component.ModuleComponentIdentifier;
import org.gradle.api.artifacts.result.ResolvedArtifactResult;
import org.gradle.api.file.ArchiveOperations;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.provider.SetProperty;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import javax.inject.Inject;

/**
 * Extracts the {@code element-list} and {@code package-list} files from javadoc jars, one directory per library named
 * after its module, for use with the Javadoc {@code -linkoffline} option.
 */
public abstract class ExtractJavadocLinksTask extends DefaultTask
{
    /**
     * Javadoc jars to extract the package lists from.
     *
     * @return javadoc jars to extract the package lists from.
     */
    @InputFiles
    @PathSensitive(PathSensitivity.NAME_ONLY)
    public abstract ConfigurableFileCollection getJavadocJars();

    /**
     * Resolved javadoc jar artifacts, used to find the module each jar belongs to.
     *
     * @return resolved javadoc jar artifacts.
     */
    @Internal
    public abstract SetProperty<ResolvedArtifactResult> getArtifacts();

    /**
     * Directory to extract into, with one subdirectory per module.
     *
     * @return directory to extract into.
     */
    @OutputDirectory
    public abstract DirectoryProperty getOutputDir();

    /**
     * Archive operations used to read the javadoc jars.
     *
     * @return archive operations.
     */
    @Inject
    protected abstract ArchiveOperations getArchiveOperations();

    /**
     * File system operations used to copy the package lists.
     *
     * @return file system operations.
     */
    @Inject
    protected abstract FileSystemOperations getFileSystemOperations();

    /**
     * Extract the package lists.
     */
    @TaskAction
    public void extract()
    {
        getFileSystemOperations().delete((spec) -> spec.delete(getOutputDir()));
        for (final ResolvedArtifactResult artifact : getArtifacts().get())
        {
            if (artifact.getId().getComponentIdentifier() instanceof ModuleComponentIdentifier)
            {
                final String module =
                    ((ModuleComponentIdentifier)artifact.getId().getComponentIdentifier()).getModule();
                getFileSystemOperations().copy((spec) ->
                {
                    spec.from(getArchiveOperations().zipTree(artifact.getFile()));
                    spec.into(getOutputDir().dir(module));
                    spec.include("element-list", "package-list");
                });
            }
        }
    }
}
