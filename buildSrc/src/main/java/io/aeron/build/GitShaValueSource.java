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
import org.gradle.api.provider.ValueSource;
import org.gradle.api.provider.ValueSourceParameters;

/**
 * Computes the abbreviated git SHA of the working tree, suffixed with {@code +guilty} when there are uncommitted
 * changes. Exposed as a {@link ValueSource} so it is computed once per build and is compatible with the configuration
 * cache.
 */
public abstract class GitShaValueSource implements ValueSource<String, GitShaValueSource.Parameters>
{
    /**
     * Parameters for {@link GitShaValueSource}.
     */
    public interface Parameters extends ValueSourceParameters
    {
        /**
         * Directory from which to search for the git repository.
         *
         * @return directory from which to search for the git repository.
         */
        DirectoryProperty getProjectDir();
    }

    /**
     * {@inheritDoc}
     */
    public String obtain()
    {
        try
        {
            return GithubUtil.currentGitHash(getParameters().getProjectDir().get().getAsFile().getAbsolutePath());
        }
        catch (final Exception ex)
        {
            throw new IllegalStateException("failed to determine git SHA", ex);
        }
    }
}
