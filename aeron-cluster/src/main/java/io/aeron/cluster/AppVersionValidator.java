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
package io.aeron.cluster;

import org.agrona.SemanticVersion;

/**
 * Default {@link VersionValidator} used for determining AppVersion compatibility, which uses
 * {@link org.agrona.SemanticVersion} major version for checking compatibility.
 * <p>
 * A custom policy can be supplied by implementing {@link VersionValidator}.
 */
public final class AppVersionValidator implements VersionValidator
{
    private AppVersionValidator()
    {
    }

    /**
     * Singleton instance of {@link AppVersionValidator} version which can be used to avoid allocation.
     */
    public static final AppVersionValidator SEMANTIC_VERSIONING_VALIDATOR = new AppVersionValidator();

    /**
     * Check version compatibility between configured context appVersion and appVersion in
     * new leadership term or snapshot.
     *
     * @param contextAppVersion   configured appVersion value from context.
     * @param appVersionUnderTest to check against configured appVersion.
     * @return true for compatible or false for not compatible.
     */
    public boolean isVersionCompatible(final int contextAppVersion, final int appVersionUnderTest)
    {
        return SemanticVersion.major(contextAppVersion) == SemanticVersion.major(appVersionUnderTest);
    }
}
