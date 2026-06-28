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
package io.aeron.driver;

import java.net.InetAddress;

/**
 * Interface to allow resolving a name to an {@link InetAddress}.
 */
public interface NameResolver
{
    /**
     * Resolve a name and return the most up to date {@link InetAddress} that represents the name.
     *
     * @param name           to resolve.
     * @param uriParamName   that the resolution is for.
     * @param isReResolution {@code true} if this is a re-resolution or {@code false} if initial resolution.
     * @return address for the name that most recently represents the name or null if not resolvable currently.
     * @see io.aeron.CommonContext#ENDPOINT_PARAM_NAME
     * @see io.aeron.CommonContext#MDC_CONTROL_PARAM_NAME
     */
    InetAddress resolve(String name, String uriParamName, boolean isReResolution);

    /**
     * Lookup the name and return a string of the form {@code name:port} that represents the endpoint or control param
     * of the URI.
     *
     * @param name         to lookup
     * @param uriParamName that the lookup is for.
     * @param isReLookup   {@code true} if this is a re-lookup or {@code true} if initial lookup.
     * @return string in {@code name:port} form.
     */
    default String lookup(final String name, final String uriParamName, final boolean isReLookup)
    {
        return name;
    }

    /**
     * Gets the name of the resolver, used for logging and debugging.
     *
     * @return name of the resolver, defaults to the qualified class name.
     */
    default String name()
    {
        return this.getClass().getName();
    }
}
