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

import org.gradle.api.DefaultTask;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodySubscribers;
import java.net.http.HttpResponse.ResponseInfo;
import java.time.Duration;
import java.util.Base64;

import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * This task performs manual steps to publish artifacts to Central Portal via OSSRH Staging API.
 */
public class SonatypeCentralPortalUploadRepositoryTask extends DefaultTask
{
    private static final String CENTRAL_PORTAL_OSSRH_API_URI = "https://ossrh-staging-api.central.sonatype.com";
    private static final Duration CONNECTION_TIMEOUT = Duration.ofSeconds(30);

    private final Property<String> portalUsername;
    private final Property<String> portalPassword;
    private final Property<String> groupId;
    private final Property<Boolean> snapshotRelease;

    /**
     * Create new task instance.
     */
    public SonatypeCentralPortalUploadRepositoryTask()
    {
        portalUsername = getProject().getObjects().property(String.class);
        portalPassword = getProject().getObjects().property(String.class);
        groupId = getProject().getObjects().property(String.class);
        snapshotRelease = getProject().getObjects().property(Boolean.class);
    }

    /**
     * Return property to set Central Portal username.
     *
     * @return Central Portal username.
     */
    @Input
    public Property<String> getPortalUsername()
    {
        return portalUsername;
    }

    /**
     * Return property to set Central Portal password.
     *
     * @return Central Portal password.
     */
    @Input
    public Property<String> getPortalPassword()
    {
        return portalPassword;
    }

    /**
     * Return property to set {@code groupId} of the project.
     *
     * @return {@code groupId} of the project.
     */
    @Input
    public Property<String> getGroupId()
    {
        return groupId;
    }

    /**
     * Return property to set snapshot release.
     *
     * @return {@code true} if snapshot release.
     */
    @Input
    public Property<Boolean> getSnapshotRelease()
    {
        return snapshotRelease;
    }

    /**
     * Publish staging repository to the Central Portal.
     */
    @TaskAction
    public void run() throws IOException, InterruptedException
    {
        if (!portalUsername.isPresent())
        {
            return; // release is not configured
        }

        if (snapshotRelease.get())
        {
            return; // snapshots are published directly
        }

        final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(CONNECTION_TIMEOUT)
            .build();

        final String userNameAndPassword = portalUsername.get() + ":" + portalPassword.get();
        final String bearer = new String(
            Base64.getEncoder().encode(userNameAndPassword.getBytes(US_ASCII)), US_ASCII);

        final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
            .header("Authorization", "Bearer " + bearer);

        final URI apiUri = URI.create(CENTRAL_PORTAL_OSSRH_API_URI);

        final String repositoryKey = findOpenRepository(apiUri, httpClient, requestBuilder);
        uploadRepositoryToPortal(apiUri, httpClient, requestBuilder, repositoryKey);
        dropRepository(apiUri, httpClient, requestBuilder, repositoryKey);
    }

    private String findOpenRepository(
        final URI apiUri,
        final HttpClient httpClient,
        final HttpRequest.Builder requestBuilder) throws IOException, InterruptedException
    {
        final HttpRequest request = requestBuilder
            .copy()
            .GET()
            .uri(apiUri.resolve("/manual/search/repositories?ip=client"))
            .build();
        final HttpResponse<String> response = httpClient.send(
            request, (ResponseInfo responseInfo) -> BodySubscribers.ofString(US_ASCII));

        if (200 != response.statusCode())
        {
            throw new IllegalStateException("Failed to query repositories: " +
                "status=" + response.statusCode() + ", response=" + response.body());
        }

        final JSONArray repositories = new JSONObject(response.body()).getJSONArray("repositories");
        if (repositories.isEmpty())
        {
            throw new IllegalStateException("No open repositories found!");
        }

        String repositoryKey = null;
        final String group = groupId.get();
        for (int i = 0; i < repositories.length(); i++)
        {
            final JSONObject repo = (JSONObject)repositories.get(i);
            if ("open".equals(repo.getString("state")))
            {
                final String key = repo.getString("key");
                if (key.contains(group))
                {
                    repositoryKey = key;
                    break;
                }
            }
        }

        if (null == repositoryKey)
        {
            throw new IllegalStateException("No open repositories found!");
        }
        return repositoryKey;
    }

    private static void uploadRepositoryToPortal(
        final URI apiUri,
        final HttpClient httpClient,
        final HttpRequest.Builder requestBuilder,
        final String repositoryKey) throws IOException, InterruptedException
    {
        final HttpRequest request = requestBuilder
            .copy()
            .POST(HttpRequest.BodyPublishers.noBody())
            .uri(apiUri.resolve("/manual/upload/repository/" + repositoryKey + "?publishing_type=automatic"))
            .build();
        final HttpResponse<String> response = httpClient.send(
            request, (ResponseInfo responseInfo) -> BodySubscribers.ofString(US_ASCII));

        if (200 != response.statusCode())
        {
            throw new IllegalStateException("Failed to upload repository: repository_key=" + repositoryKey +
                ", status=" + response.statusCode() + ", response=" + response.body());
        }
    }

    private static void dropRepository(
        final URI apiUri,
        final HttpClient httpClient,
        final HttpRequest.Builder requestBuilder,
        final String repositoryKey) throws IOException, InterruptedException
    {
        final HttpRequest request = requestBuilder
            .copy()
            .DELETE()
            .uri(apiUri.resolve("/manual/drop/repository/" + repositoryKey))
            .build();
        final HttpResponse<String> response = httpClient.send(
            request, (ResponseInfo responseInfo) -> BodySubscribers.ofString(US_ASCII));

        if (204 != response.statusCode())
        {
            throw new IllegalStateException("Failed to drop repository: repository_key=" + repositoryKey +
                ", status=" + response.statusCode() + ", response=" + response.body());
        }
    }
}
