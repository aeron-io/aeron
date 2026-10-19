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

import org.asciidoctor.Asciidoctor;
import org.asciidoctor.Attributes;
import org.asciidoctor.Options;
import org.asciidoctor.SafeMode;
import org.asciidoctor.ast.Document;
import org.asciidoctor.extension.PreprocessorReader;
import org.asciidoctor.extension.Reader;
import org.asciidoctor.log.Severity;
import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Gradle task to prepare AsciiDoc files.
 */
public class AsciidoctorPreprocessTask extends DefaultTask
{
    private final String sampleBaseDir = getProject().getProjectDir().getAbsolutePath();

    private final String sampleSourceDir = sampleBaseDir + "/src/main/java";

    private final File source = new File(sampleBaseDir, "/src/docs/asciidoc");

    private final File target = new File(
        getProject().getLayout().getBuildDirectory().getAsFile().get(), "/asciidoc/asciidoc");

    // Has a slightly silly name to avoid name clashes in the build script.
    private String versionText;

    /**
     * Base directory containing the samples code.
     *
     * @return base directory for samples.
     */
    @Input
    public String getSampleBaseDir()
    {
        return sampleBaseDir;
    }

    /**
     * Directory containing the samples source code.
     *
     * @return sources within the directory for samples.
     */
    @Input
    public String getSampleSourceDir()
    {
        return sampleSourceDir;
    }

    /**
     * Directory containing the source files.
     *
     * @return source directory.
     */
    @InputDirectory
    public File getSource()
    {
        return source;
    }

    /**
     * Directory containing the target files.
     *
     * @return target directory.
     */
    @OutputDirectory
    public File getTarget()
    {
        return target;
    }

    /**
     * Returns the version string.
     *
     * @return version string.
     */
    @Input
    public String getVersionText()
    {
        return versionText;
    }

    /**
     * Sets the version string.
     *
     * @param versionText version string.
     */
    public void setVersionText(final String versionText)
    {
        this.versionText = versionText;
    }

    /**
     * Implementation of the task action.
     *
     * @throws Exception in case of errors.
     */
    @TaskAction
    public void preprocess() throws Exception
    {
        if (!target.exists() && !target.mkdirs())
        {
            throw new IOException("unable to create build directory");
        }

        final File[] asciidocFiles = AsciidocUtil.filterAsciidocFiles(source);

        System.out.println("Transforming from: " + source);
        System.out.println("Found files: " + Arrays.stream(asciidocFiles).map(File::getName).collect(joining(", ")));

        final Map<File, Integer> errors = new HashMap<>();

        for (final File asciidocFile : asciidocFiles)
        {
            final File outputFile = new File(target, asciidocFile.getName());

            final Asciidoctor asciidoctor = Asciidoctor.Factory.create();

            final int[] errorCount = { 0 };

            asciidoctor.registerLogHandler(
                (logRecord) ->
                {
                    if (logRecord.getSeverity() == Severity.ERROR || logRecord.getSeverity() == Severity.FATAL)
                    {
                        errorCount[0]++;
                    }
                });

            final Attributes attributes = Attributes.builder()
                .attribute("sampleBaseDir", requireNonNull(sampleBaseDir, "Must specify sampleBaseDir"))
                .attribute("sampleSourceDir", requireNonNull(sampleSourceDir, "Must specify sampleSourceDir"))
                .build();

            final Options options = Options.builder()
                .attributes(attributes)
                .safe(SafeMode.UNSAFE)
                .build();

            try (PrintStream output = new PrintStream(outputFile))
            {
                asciidoctor.javaExtensionRegistry().preprocessor(
                    new org.asciidoctor.extension.Preprocessor()
                    {
                        public Reader process(final Document document, final PreprocessorReader reader)
                        {
                            String line;
                            while (null != (line = reader.readLine()))
                            {
                                if (line.startsWith(":aeronVersion:"))
                                {
                                    output.println(":aeronVersion: " + versionText);
                                }
                                else
                                {
                                    output.println(line);
                                }
                            }

                            return reader;
                        }
                    });

                asciidoctor.loadFile(asciidocFile, options);

                if (0 < errorCount[0])
                {
                    errors.put(asciidocFile, errorCount[0]);
                }
            }
        }

        errors.forEach((key, value) -> System.out.println("file: " + key + ", error count: " + value));

        if (!errors.isEmpty())
        {
            throw new Exception("failed due to errors in parsing");
        }
    }
}
