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

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.eclipse.jgit.lib.ConfigConstants.CONFIG_KEY_URL;
import static org.eclipse.jgit.lib.ConfigConstants.CONFIG_REMOTE_SECTION;

/**
 * Gradle task to publish the tutorial documentation.
 */
public class TutorialPublishTask extends DefaultTask
{
    private String apiKey;

    private File source;

    private String remoteName;

    private final File cloneDirectory =
        new File(getProject().getLayout().getBuildDirectory().getAsFile().get(), "tmp/tutorialPublish");

    private final File baseGitDir = new File(getProject().getRootDir(), ".git");

    /**
     * Returns the GitHub API key to use for publishing.
     *
     * @return GitHub API key.
     */
    @Input
    public String getApiKey()
    {
        return apiKey;
    }

    /**
     * Returns the source directory.
     *
     * @return source directory.
     */
    @InputDirectory
    public File getSource()
    {
        return source;
    }

    /**
     * Gets the name of the remote repo.
     *
     * @return name of the remote repo.
     */
    @Input
    public String getRemoteName()
    {
        return remoteName;
    }

    /**
     * Sets GitHub API key.
     *
     * @param apiKey used for publishing.
     */
    public void setApiKey(final String apiKey)
    {
        this.apiKey = apiKey;
    }

    /**
     * Sets the source directory.
     *
     * @param source directory.
     */
    public void setSource(final File source)
    {
        this.source = source;
    }

    /**
     * Sets the name of the remote repo.
     *
     * @param remoteName of Git repository.
     */
    public void setRemoteName(final String remoteName)
    {
        this.remoteName = remoteName;
    }

    /**
     * Task action implementation.
     *
     * @throws Exception in case of errors.
     */
    @TaskAction
    public void publish() throws Exception
    {
        final String wikiUri = getWikiUri();
        final File directory = cloneDirectory;
        // Use Personal Access Token or GITHUB_TOKEN for workflows
        final CredentialsProvider credentialsProvider = new UsernamePasswordCredentialsProvider(apiKey, "");

        final Git git = Git.cloneRepository()
            .setURI(wikiUri)
            .setCredentialsProvider(credentialsProvider)
            .setDirectory(directory)
            .call();

        final File[] asciidocFiles = AsciidocUtil.filterAsciidocFiles(source);
        System.out.println("Publishing from: " + source);
        System.out.println("Found files: " + Arrays.stream(asciidocFiles).map(File::getName).collect(joining(", ")));

        for (final File asciidocFile : asciidocFiles)
        {
            Files.copy(
                asciidocFile.toPath(),
                new File(directory, asciidocFile.getName()).toPath(),
                StandardCopyOption.REPLACE_EXISTING);
        }

        git.add().addFilepattern(".").setUpdate(false).call();
        git.commit()
            .setSign(false)
            .setMessage("Update Docs")
            .call();

        System.out.println("Publishing to: " + wikiUri);

        git.push().setCredentialsProvider(credentialsProvider).call();
    }

    private String getWikiUri() throws IOException, URISyntaxException
    {
        if (!baseGitDir.exists() || !baseGitDir.isDirectory())
        {
            throw new IllegalStateException("unable to find valid git repository at: " + baseGitDir);
        }

        final Repository baseGitRepo = new FileRepositoryBuilder()
            .setGitDir(baseGitDir)
            .build();

        final String origin = baseGitRepo.getConfig().getString(
            CONFIG_REMOTE_SECTION,
            requireNonNull(remoteName, "'remoteName' must be set, use origin as a default"),
            CONFIG_KEY_URL);

        if (null == origin)
        {
            throw new IllegalStateException("unable to find origin URI");
        }

        return GithubUtil.getWikiUriFromOriginUri(origin);
    }
}
