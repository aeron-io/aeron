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

import org.gradle.api.Project;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.provider.Provider;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

/**
 * Computes the git SHA of the working tree once per build and shares it between all projects. See
 * {@link GithubUtil#currentGitHash(String)} for the format.
 */
public abstract class GitShaService implements BuildService<GitShaService.Parameters>
{
    private String gitSha;

    /**
     * Parameters for {@link GitShaService}.
     */
    public interface Parameters extends BuildServiceParameters
    {
        /**
         * Directory from which to search for the git repository.
         *
         * @return directory from which to search for the git repository.
         */
        DirectoryProperty getProjectDir();
    }

    /**
     * Provider of the git SHA of the working tree, shared by every project in the build.
     *
     * @param project requesting the git SHA.
     * @return provider of the git SHA of the working tree.
     */
    public static Provider<String> gitSha(final Project project)
    {
        return project.getGradle().getSharedServices()
            .registerIfAbsent("aeronGitSha", GitShaService.class,
                spec -> spec.getParameters().getProjectDir().set(project.getLayout().getSettingsDirectory()))
            .map(GitShaService::gitSha);
    }

    /**
     * Git SHA of the working tree, computed on first use.
     *
     * @return git SHA of the working tree.
     */
    public synchronized String gitSha()
    {
        if (null == gitSha)
        {
            try
            {
                gitSha = GithubUtil.currentGitHash(getParameters().getProjectDir().get().getAsFile().getAbsolutePath());
            }
            catch (final Exception ex)
            {
                throw new IllegalStateException("failed to determine git SHA", ex);
            }
        }

        return gitSha;
    }
}
