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

import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.JavaExec;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Generates codecs from SBE message schemas by running {@code uk.co.real_logic.sbe.SbeTool}. The SBE jar must be put
 * on the {@link #getClasspath() classpath}.
 */
public abstract class SbeGenerateTask extends JavaExec
{
    /**
     * Create new task instance.
     */
    public SbeGenerateTask()
    {
        getMainClass().set("uk.co.real_logic.sbe.SbeTool");
    }

    /**
     * Message schemas to generate codecs for, in the order passed to SBE.
     *
     * @return message schemas to generate codecs for.
     */
    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getSchemas();

    /**
     * XSD the message schemas are validated against.
     *
     * @return XSD the message schemas are validated against.
     */
    @InputFile
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract RegularFileProperty getValidationXsd();

    /**
     * Directory the generated codecs are written to.
     *
     * @return directory the generated codecs are written to.
     */
    @OutputDirectory
    public abstract DirectoryProperty getOutputDir();

    /**
     * Language to generate codecs in, e.g. {@code Java}, {@code C} or {@code Cpp}.
     *
     * @return language to generate codecs in.
     */
    @Input
    public abstract Property<String> getTargetLanguage();

    /**
     * Namespace (package) for the generated codecs, overriding the one in the schema.
     *
     * @return namespace for the generated codecs.
     */
    @Input
    @Optional
    public abstract Property<String> getTargetNamespace();

    /**
     * Schema version transform, e.g. {@code *:6} to generate codecs for version 6 of every schema.
     *
     * @return schema version transform.
     */
    @Input
    @Optional
    public abstract Property<String> getSchemaTransformVersion();

    /**
     * {@inheritDoc}
     */
    @TaskAction
    public void exec()
    {
        // Every value passed to SBE is derived from the annotated properties above, which are the task's inputs.
        systemProperty("sbe.output.dir", getOutputDir().get().getAsFile().getAbsolutePath());
        systemProperty("sbe.target.language", getTargetLanguage().get());
        systemProperty("sbe.validation.xsd", getValidationXsd().get().getAsFile().getAbsolutePath());
        systemProperty("sbe.validation.stop.on.error", "true");
        if (getTargetNamespace().isPresent())
        {
            systemProperty("sbe.target.namespace", getTargetNamespace().get());
        }
        if (getSchemaTransformVersion().isPresent())
        {
            systemProperty("sbe.schema.transform.version", getSchemaTransformVersion().get());
        }

        final List<String> schemaPaths = new ArrayList<>();
        for (final File schema : getSchemas().getFiles())
        {
            schemaPaths.add(schema.getAbsolutePath());
        }
        setArgs(schemaPaths);

        super.exec();
    }
}
