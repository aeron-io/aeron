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
package io.aeron.security;

import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.Object2ObjectHashMap;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * An authenticator that works off a simple principal/credential pair constructed by a builder. It only supports simple
 * authentication, not challenge/response.
 */
public final class SimpleAuthenticator implements Authenticator
{
    private final Object2ObjectHashMap<Credentials, Principal> principalsByCredentialsMap =
        new Object2ObjectHashMap<>();

    private final Long2ObjectHashMap<Principal> authenticatedSessionIdToPrincipalMap = new Long2ObjectHashMap<>();

    private SimpleAuthenticator(final Builder builder)
    {
        for (final Principal principal : builder.principals)
        {
            principalsByCredentialsMap.put(principal.credentials, principal);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void onConnectRequest(final long sessionId, final byte[] encodedCredentials, final long nowMs)
    {
        final Principal principal = principalsByCredentialsMap.get(new Credentials(encodedCredentials));

        if (null != principal && principal.credentialsMatch(encodedCredentials))
        {
            authenticatedSessionIdToPrincipalMap.put(sessionId, principal);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void onChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long nowMs)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs)
    {
        final long sessionId = sessionProxy.sessionId();
        final Principal principal = authenticatedSessionIdToPrincipalMap.get(sessionId);

        if (null != principal)
        {
            if (sessionProxy.authenticate(principal.encodedPrincipal))
            {
                authenticatedSessionIdToPrincipalMap.remove(sessionId);
            }
        }
        else
        {
            sessionProxy.reject();
        }
    }

    /**
     * {@inheritDoc}
     */
    public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Builder to create instances of SimpleAuthenticator.
     */
    public static class Builder
    {
        private final ArrayList<Principal> principals = new ArrayList<>();

        /**
         * Default constructor.
         */
        public Builder()
        {
        }

        /**
         * Add a principal/credentials pair to the list supported by this authenticator. Note that the
         * {@link SimpleAuthenticator} keys the principals by the credentials, so the encoded credentials should
         * include the encoded principal. The associated {@link CredentialsSupplier} used on the client should handle
         * encoding these credentials correctly as well. For example:
         * <pre>
         * final SimpleAuthenticator simpleAuthenticator = new SimpleAuthenticator.Builder()
         *     .principal("user".getBytes(US_ASCII), "user:pass".getBytes(US_ASCII))
         *     .newInstance();
         * </pre>
         *
         * @param encodedPrincipal   principal encoded as a byte array.
         * @param encodedCredentials credentials encoded as a byte array.
         * @return this for a fluent API.
         */
        public Builder principal(final byte[] encodedPrincipal, final byte[] encodedCredentials)
        {
            principals.add(new Principal(encodedPrincipal, encodedCredentials));
            return this;
        }

        /**
         * Construct a new instance of the SimpleAuthenticator.
         *
         * @return a new SimpleAuthenticator instance.
         */
        public SimpleAuthenticator newInstance()
        {
            return new SimpleAuthenticator(this);
        }
    }

    private static final class Principal
    {
        private final byte[] encodedPrincipal;
        private final Credentials credentials;

        private Principal(final byte[] encodedPrincipal, final byte[] encodedCredentials)
        {
            this.encodedPrincipal = encodedPrincipal;
            this.credentials = new Credentials(encodedCredentials);
        }

        public boolean credentialsMatch(final byte[] encodedCredentials)
        {
            return Arrays.equals(credentials.encodedCredentials, encodedCredentials);
        }
    }

    private static final class Credentials
    {
        private final byte[] encodedCredentials;

        private Credentials(final byte[] encodedCredentials)
        {
            this.encodedCredentials = encodedCredentials;
        }

        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            final Credentials that = (Credentials)o;
            return Arrays.equals(encodedCredentials, that.encodedCredentials);
        }

        public int hashCode()
        {
            return Arrays.hashCode(encodedCredentials);
        }
    }
}
