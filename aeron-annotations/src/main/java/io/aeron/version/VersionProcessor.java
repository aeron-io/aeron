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
package io.aeron.version;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Messager;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedOptions;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Version processor.
 */
@SupportedAnnotationTypes("io.aeron.version.Versioned")
@SupportedOptions({"io.aeron.version", "io.aeron.gitsha"})
public class VersionProcessor extends AbstractProcessor
{
    private static final String VERSION_IMPL =
        "\n" +
        "    @Override\n" +
        "    public String toString()\n" +
        "    {\n" +
        "        return VERSION;\n" +
        "    }\n" +
        "\n" +
        "    public int majorVersion()\n" +
        "    {\n" +
        "        return MAJOR_VERSION;\n" +
        "    }\n" +
        "\n" +
        "    public int minorVersion()\n" +
        "    {\n" +
        "        return MINOR_VERSION;\n" +
        "    }\n" +
        "\n" +
        "    public int patchVersion()\n" +
        "    {\n" +
        "        return PATCH_VERSION;\n" +
        "    }\n" +
        "\n" +
        "    public String gitSha()\n" +
        "    {\n" +
        "        return GIT_SHA;\n" +
        "    }";

    /**
     * Default constructor.
     */
    public VersionProcessor()
    {
    }

    /**
     * {@inheritDoc}
     */
    public SourceVersion getSupportedSourceVersion()
    {
        return SourceVersion.latest();
    }

    /**
     * {@inheritDoc}
     */
    public boolean process(final Set<? extends TypeElement> annotations, final RoundEnvironment roundEnv)
    {
        final Messager messager = processingEnv.getMessager();
        final String versionString = processingEnv.getOptions().get("io.aeron.version");
        final String gitSha = processingEnv.getOptions().get("io.aeron.gitsha");

        for (final TypeElement annotation : annotations)
        {
            final Set<? extends Element> elementsAnnotatedWith = roundEnv.getElementsAnnotatedWith(annotation);
            for (final Element element : elementsAnnotatedWith)
            {
                final VersionInformation info = VersionInformation.parse(versionString, messager, element);
                if (null == info)
                {
                    continue;
                }

                final PackageElement pkg = processingEnv.getElementUtils().getPackageOf(element);
                final String packageName = pkg.getQualifiedName().toString();
                final String className = element.getSimpleName() + "Version";

                try
                {
                    final JavaFileObject sourceFile = processingEnv.getFiler().createSourceFile(
                        packageName + '.' + className, element);
                    try (PrintWriter out = new PrintWriter(sourceFile.openWriter()))
                    {
                        out.printf("package %s;%n", packageName);
                        out.println();
                        out.printf("public class %s%n implements io.aeron.version.Version%n", className);
                        out.printf("{%n");
                        out.printf("    public static final String VERSION = \"%s\";%n", versionString);
                        out.printf("    public static final int MAJOR_VERSION = %s;%n", info.major);
                        out.printf("    public static final int MINOR_VERSION = %s;%n", info.minor);
                        out.printf("    public static final int PATCH_VERSION = %s;%n", info.patch);
                        out.printf("    public static final String GIT_SHA = \"%s\";%n", gitSha);
                        out.println(VERSION_IMPL);
                        out.printf("}%n");
                    }
                }
                catch (final IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }

        return false;
    }

    private static final class VersionInformation
    {
        private static final Pattern VERSION_PATTERN = Pattern.compile("([0-9]+).([0-9]+).([0-9]+)(?:-.+)?");
        private final int major;
        private final int minor;
        private final int patch;

        private VersionInformation(final int major, final int minor, final int patch)
        {
            this.major = major;
            this.minor = minor;
            this.patch = patch;
        }

        /**
         * Parse the {@code io.aeron.version} option, reporting a compilation error when it is
         * absent or malformed, so the build fails with an actionable message.
         *
         * @param versionString the value supplied via {@code -Aio.aeron.version}, or {@code null} if it was not set.
         * @param messager      used to report errors back to the compiler.
         * @param element       the {@code @Versioned} element the error relates to.
         * @return the parsed version, or {@code null} if the option was missing or invalid.
         */
        static VersionInformation parse(final String versionString, final Messager messager, final Element element)
        {
            if (null == versionString)
            {
                messager.printMessage(
                    Diagnostic.Kind.ERROR,
                    "@Versioned requires the 'io.aeron.version' annotation processor option, but it was not set. " +
                    "Pass it to the compiler, for example: -Aio.aeron.version=1.2.3",
                    element);
                return null;
            }

            final Matcher matcher = VERSION_PATTERN.matcher(versionString);
            if (!matcher.matches())
            {
                messager.printMessage(
                    Diagnostic.Kind.ERROR,
                    "The 'io.aeron.version' value '" + versionString + "' is not a valid version; expected " +
                    "<major>.<minor>.<patch> optionally followed by '-<suffix>', for example: 1.2.3 or 1.2.3-SNAPSHOT",
                    element);
                return null;
            }

            return new VersionInformation(
                Integer.parseInt(matcher.group(1)),
                Integer.parseInt(matcher.group(2)),
                Integer.parseInt(matcher.group(3)));
        }
    }
}
