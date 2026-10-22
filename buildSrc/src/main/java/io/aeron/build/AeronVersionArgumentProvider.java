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

import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Input;
import org.gradle.process.CommandLineArgumentProvider;

import java.util.List;

/**
 * Supplies the {@code io.aeron.version} and {@code io.aeron.gitsha} annotation processor options to the compiler,
 * declaring both values as task inputs so that compilation reruns when either changes.
 */
public final class AeronVersionArgumentProvider implements CommandLineArgumentProvider
{
    private final Provider<String> version;
    private final Provider<String> gitSha;

    /**
     * Create a new argument provider.
     *
     * @param version of Aeron being built.
     * @param gitSha  of the working tree being built.
     */
    public AeronVersionArgumentProvider(final Provider<String> version, final Provider<String> gitSha)
    {
        this.version = version;
        this.gitSha = gitSha;
    }

    /**
     * Version of Aeron being built.
     *
     * @return version of Aeron being built.
     */
    @Input
    public Provider<String> getVersion()
    {
        return version;
    }

    /**
     * Git SHA of the working tree being built.
     *
     * @return git SHA of the working tree being built.
     */
    @Input
    public Provider<String> getGitSha()
    {
        return gitSha;
    }

    /**
     * {@inheritDoc}
     */
    public Iterable<String> asArguments()
    {
        return List.of("-Aio.aeron.version=" + version.get(), "-Aio.aeron.gitsha=" + gitSha.get());
    }
}
