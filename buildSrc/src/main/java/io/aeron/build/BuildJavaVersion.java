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

import org.gradle.api.JavaVersion;

/**
 * Java version used to build and test Aeron: the {@code BUILD_JAVA_VERSION} environment variable if set, otherwise
 * the version of the JVM running Gradle.
 */
public final class BuildJavaVersion
{
    private BuildJavaVersion()
    {
    }

    /**
     * Version string as given, e.g. {@code 21}, {@code 21.0.2} or {@code 26-ea}.
     *
     * @return version string as given.
     */
    public static String raw()
    {
        final String version = System.getenv("BUILD_JAVA_VERSION");
        return null != version && !version.isEmpty() ? version : JavaVersion.current().getMajorVersion();
    }

    /**
     * Is the build Java version an early access release?
     *
     * @return true if the build Java version is an early access release.
     */
    public static boolean isEarlyAccess()
    {
        return raw().endsWith("-ea");
    }

    /**
     * Major version number of the build Java version.
     *
     * @return major version number of the build Java version.
     */
    public static int major()
    {
        String version = raw();

        if (version.indexOf('.') > 0)
        {
            version = version.substring(0, version.indexOf('.'));
        }

        if (version.indexOf('-') > 0)
        {
            version = version.substring(0, version.indexOf('-'));
        }

        return Integer.parseInt(version);
    }
}
