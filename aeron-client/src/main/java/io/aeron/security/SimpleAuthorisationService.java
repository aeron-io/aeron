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

import org.agrona.collections.BiInt2ObjectMap;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Object2ObjectHashMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Authorisation service that supports setting general and per-principal rules as well as scoping to protocol, action,
 * and type. Uses a fluent API to add authorisation rules.
 */
public final class SimpleAuthorisationService implements AuthorisationService
{
    private final AuthorisationService defaultAuthorisation;
    private final Object2ObjectHashMap<ByteArrayAsKey, Principal> principalByKeyMap = new Object2ObjectHashMap<>();
    private final Principal defaultPrincipal;

    private SimpleAuthorisationService(final Builder builder)
    {
        defaultAuthorisation = builder.defaultAuthorisation;
        principalByKeyMap.putAll(builder.principalByKeyMap);
        defaultPrincipal = builder.defaultPrincipal;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isAuthorised(
        final int protocolId,
        final int actionId,
        final Object type,
        final byte[] encodedPrincipal)
    {
        Boolean isAuthorised = isAuthorised(
            principalByKeyMap.get(new ByteArrayAsKey(encodedPrincipal)), protocolId, actionId, type);

        if (null != isAuthorised)
        {
            return isAuthorised;
        }

        isAuthorised = isAuthorised(defaultPrincipal, protocolId, actionId, type);
        if (null != isAuthorised)
        {
            return isAuthorised;
        }

        return defaultAuthorisation.isAuthorised(protocolId, actionId, type, encodedPrincipal);
    }

    private Boolean isAuthorised(
        final Principal principal,
        final int protocolId,
        final int actionId,
        final Object type)
    {
        if (null == principal)
        {
            return null;
        }

        return principal.isAuthorised(protocolId, actionId, type);
    }

    /**
     * Builder to create the authorisation service.
     */
    public static class Builder
    {
        private AuthorisationService defaultAuthorisation = AuthorisationService.DENY_ALL;
        private final Object2ObjectHashMap<ByteArrayAsKey, Principal> principalByKeyMap = new Object2ObjectHashMap<>();
        private final Principal defaultPrincipal = new Principal(new byte[0]);

        /**
         * Default constructor.
         */
        public Builder()
        {
        }

        /**
         * Set the default authorisation if the authorisation request does not match any of the supplied rules.
         *
         * @param defaultAuthorisation an authorisation service to fall back to.
         * @return this for a fluent API
         * @see AuthorisationService#ALLOW_ALL
         * @see AuthorisationService#DENY_ALL
         */
        public Builder defaultAuthorisation(final AuthorisationService defaultAuthorisation)
        {
            this.defaultAuthorisation = defaultAuthorisation;
            return this;
        }

        /**
         * Add rule for a specific principal that is scoped to a protocolId, actionId, and type.
         *
         * @param protocolId       <code>sbe:messageSchema@id</code> value that the rule applies to.
         * @param actionId         <code>sbe:message@id</code> value that the rule applies to.
         * @param type             A parameter of the message can be used to narrow the scope of the authorisation
         *                         rule. This is message dependent.
         * @param encodedPrincipal The principal encoded as byte array.
         * @param isAllowed        If the rule should allow or deny access.
         * @return this for a fluent API.
         */
        public Builder addPrincipalRule(
            final int protocolId,
            final int actionId,
            final Object type,
            final byte[] encodedPrincipal,
            final boolean isAllowed)
        {
            final Principal principal = principalByKeyMap.computeIfAbsent(
                new ByteArrayAsKey(encodedPrincipal), (key) -> new Principal(key.data));

            final BiInt2ObjectMap<Set<Object>> byTypeMap = isAllowed ? principal.byProtocolActionTypeAllowed :
                principal.byProtocolActionTypeDenied;

            byTypeMap.computeIfAbsent(protocolId, actionId, (a, b) -> new HashSet<>()).add(type);

            return this;
        }

        /**
         * Add rule for a specific principal that is scoped to a protocolId, and actionId.
         *
         * @param protocolId       <code>sbe:messageSchema@id</code> value that the rule applies to.
         * @param actionId         <code>sbe:message@id</code> value that the rule applies to.
         * @param encodedPrincipal The principal encoded as byte array.
         * @param isAllowed        If the rule should allow or deny access.
         * @return this for a fluent API.
         */
        public Builder addPrincipalRule(
            final int protocolId,
            final int actionId,
            final byte[] encodedPrincipal,
            final boolean isAllowed)
        {
            final Principal principal = principalByKeyMap.computeIfAbsent(
                new ByteArrayAsKey(encodedPrincipal), (key) -> new Principal(key.data));

            principal.byProtocolAction.put(protocolId, actionId, isAllowed);
            return this;
        }

        /**
         * Add rule for a specific principal that is scoped to a protocolId.
         *
         * @param protocolId       <code>sbe:messageSchema@id</code> value that the rule applies to.
         * @param encodedPrincipal The principal encoded as byte array.
         * @param isAllowed        If the rule should allow or deny access.
         * @return this for a fluent API.
         */
        public Builder addPrincipalRule(
            final int protocolId,
            final byte[] encodedPrincipal,
            final boolean isAllowed)
        {
            final Principal principal = principalByKeyMap.computeIfAbsent(
                new ByteArrayAsKey(encodedPrincipal), (key) -> new Principal(key.data));

            principal.byProtocol.put(protocolId, Boolean.valueOf(isAllowed));
            return this;
        }

        /**
         * Add rule for all principals that is scoped to a protocolId, actionId, and type.
         *
         * @param protocolId <code>sbe:messageSchema@id</code> value that the rule applies to.
         * @param actionId   <code>sbe:message@id</code> value that the rule applies to.
         * @param type       A parameter of the message can be used to narrow the scope of the authorisation
         *                   rule. This is message dependent.
         * @param isAllowed  If the rule should allow or deny access.
         * @return this for a fluent API.
         */
        public Builder addGeneralRule(
            final int protocolId,
            final int actionId,
            final Object type,
            final boolean isAllowed)
        {
            final BiInt2ObjectMap<Set<Object>> byTypeMap = isAllowed ? defaultPrincipal.byProtocolActionTypeAllowed :
                defaultPrincipal.byProtocolActionTypeDenied;
            byTypeMap.computeIfAbsent(protocolId, actionId, (a, b) -> new HashSet<>()).add(type);

            return this;
        }

        /**
         * Add rule for all principals that is scoped to a protocolId and actionId.
         *
         * @param protocolId <code>sbe:messageSchema@id</code> value that the rule applies to.
         * @param actionId   <code>sbe:message@id</code> value that the rule applies to.
         * @param isAllowed  If the rule should allow or deny access.
         * @return this for a fluent API.
         */
        public Builder addGeneralRule(final int protocolId, final int actionId, final boolean isAllowed)
        {
            defaultPrincipal.byProtocolAction.put(protocolId, actionId, isAllowed);
            return this;
        }

        /**
         * Add rule for all principals that is scoped to a protocolId.
         *
         * @param protocolId <code>sbe:messageSchema@id</code> value that the rule applies to.
         * @param isAllowed  If the rule should allow or deny access.
         * @return this for a fluent API.
         */
        public Builder addGeneralRule(final int protocolId, final boolean isAllowed)
        {
            defaultPrincipal.byProtocol.put(protocolId, (Boolean)isAllowed);
            return this;
        }

        /**
         * Create and instance of the {@link SimpleAuthorisationService}.
         *
         * @return new {@link SimpleAuthorisationService}.
         */
        public SimpleAuthorisationService newInstance()
        {
            return new SimpleAuthorisationService(this);
        }
    }

    private static final class Principal
    {
        private final Int2ObjectHashMap<Boolean> byProtocol = new Int2ObjectHashMap<>();
        private final BiInt2ObjectMap<Boolean> byProtocolAction = new BiInt2ObjectMap<>();
        private final BiInt2ObjectMap<Set<Object>> byProtocolActionTypeAllowed = new BiInt2ObjectMap<>();
        private final BiInt2ObjectMap<Set<Object>> byProtocolActionTypeDenied = new BiInt2ObjectMap<>();
        private final byte[] encodedPrincipal;

        private Principal(final byte[] encodedPrincipal)
        {
            this.encodedPrincipal = encodedPrincipal;
        }

        public Boolean isAuthorised(final int protocolId, final int actionId, final Object type)
        {
            final Set<Object> typesAllowed = byProtocolActionTypeAllowed.getOrDefault(
                protocolId, actionId, Collections.emptySet());
            if (typesAllowed.contains(type))
            {
                return Boolean.TRUE;
            }

            final Set<Object> typesDenied = byProtocolActionTypeDenied.getOrDefault(
                protocolId, actionId, Collections.emptySet());
            if (typesDenied.contains(type))
            {
                return Boolean.FALSE;
            }

            final Boolean authorised = byProtocolAction.get(protocolId, actionId);
            if (null != authorised)
            {
                return authorised;
            }

            return byProtocol.get(protocolId);
        }
    }

    private static final class ByteArrayAsKey
    {
        private final byte[] data;

        private ByteArrayAsKey(final byte[] data)
        {
            this.data = data;
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

            final ByteArrayAsKey that = (ByteArrayAsKey)o;
            return Arrays.equals(data, that.data);
        }

        public int hashCode()
        {
            return Arrays.hashCode(data);
        }
    }
}
