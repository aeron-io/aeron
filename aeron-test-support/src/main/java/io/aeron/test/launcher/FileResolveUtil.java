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
package io.aeron.test.launcher;

import java.io.File;
import java.util.Arrays;

public class FileResolveUtil
{
    public static File resolveProjectRoot()
    {
        final File workingDir = new File(System.getProperty("user.dir"));
        File parent = workingDir;

        do
        {
            final String[] versionTxtFile = parent.list((dir, name) -> "version.txt".equals(name));
            if (null != versionTxtFile && 1 == versionTxtFile.length)
            {
                return parent;
            }

            parent = workingDir.getParentFile();
        }
        while (null != parent);

        throw new RuntimeException("unable to find project root directory from: " + workingDir);
    }

    public static File resolveAeronAllJar()
    {
        return resolveAeronJar("aeron-all", false);
    }

    private static File resolveAeronJar(final String moduleName, final boolean allowMultipleFiles)
    {
        final File projectRoot = resolveProjectRoot();

        final File allBuildLibs = new File(projectRoot, moduleName + "/build/libs");
        if (!allBuildLibs.exists())
        {
            throw new RuntimeException("directory: " + allBuildLibs + " does not exist");
        }

        final File[] aeronAllJarFiles = allBuildLibs.listFiles(
            (dir, name) ->
            name.startsWith(moduleName + "-") &&
            name.endsWith(".jar") &&
            !name.endsWith("-sources.jar") &&
            !name.endsWith("-javadoc.jar"));

        if (null == aeronAllJarFiles || 0 == aeronAllJarFiles.length)
        {
            throw new RuntimeException("unable to find aeron jar files in directory: " + allBuildLibs);
        }

        if (!allowMultipleFiles && 1 != aeronAllJarFiles.length)
        {
            throw new RuntimeException(
                "multiple libs found, run './gradlew clean': " + Arrays.toString(aeronAllJarFiles));
        }

        return aeronAllJarFiles[0];
    }

    public static File resolveJavaBinary()
    {
        final File javaHome = new File(System.getProperty("java.home"));
        if (!javaHome.exists())
        {
            throw new RuntimeException("java.home: " + javaHome + " does not exist??");
        }

        final File javaBinary = new File(javaHome, "bin/java");
        if (!javaBinary.exists())
        {
            throw new RuntimeException("java binary: " + javaBinary + " does not exist??");
        }

        return javaBinary;
    }

    public static File resolveClusterScriptDir()
    {
        return new File(resolveProjectRoot(), "aeron-samples/scripts/cluster");
    }
}
