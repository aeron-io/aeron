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

import org.agrona.collections.ArrayUtil;

/**
 * Default Authenticator which authenticates all connection requests immediately.
 */
public class DefaultAuthenticatorSupplier implements AuthenticatorSupplier
{
    /**
     * Singleton instance.
     */
    public static final DefaultAuthenticatorSupplier INSTANCE = new DefaultAuthenticatorSupplier();

    /**
     * The null encoded principal is an empty array of bytes.
     */
    public static final byte[] NULL_ENCODED_PRINCIPAL = ArrayUtil.EMPTY_BYTE_ARRAY;

    /**
     * Singleton instance which can be used to avoid allocation.
     */
    public static final Authenticator DEFAULT_AUTHENTICATOR = new DefaultAuthenticator();

    /**
     * Default constructor.
     */
    public DefaultAuthenticatorSupplier()
    {
    }

    /**
     * Gets the singleton instance {@link #DEFAULT_AUTHENTICATOR} which authenticates all connection requests
     * immediately.
     *
     * @return {@link #DEFAULT_AUTHENTICATOR} which authenticates all connection requests immediately.
     */
    public Authenticator get()
    {
        return DEFAULT_AUTHENTICATOR;
    }

    static final class DefaultAuthenticator implements Authenticator
    {
        public void onConnectRequest(final long sessionId, final byte[] encodedCredentials, final long nowMs)
        {
        }

        public void onChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long nowMs)
        {
        }

        public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs)
        {
            sessionProxy.authenticate(NULL_ENCODED_PRINCIPAL);
        }

        public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs)
        {
            sessionProxy.authenticate(NULL_ENCODED_PRINCIPAL);
        }
    }
}
