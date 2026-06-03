/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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

/**
 * Interface for determining AppVersion compatibility, so that a custom policy can be supplied via
 * {@link ConsensusModule.Context#appVersionValidator(VersionValidator)} and
 * {@link io.aeron.cluster.service.ClusteredServiceContainer.Context#appVersionValidator(VersionValidator)}.
 * <p>
 * The default implementation is {@link AppVersionValidator#SEMANTIC_VERSIONING_VALIDATOR} which uses
 * {@link org.agrona.SemanticVersion} major version for checking compatibility.
 */
@FunctionalInterface
public interface VersionValidator
{
    /**
     * Check version compatibility between configured context appVersion and appVersion in
     * new leadership term or snapshot.
     *
     * @param contextAppVersion   configured appVersion value from context.
     * @param appVersionUnderTest to check against configured appVersion.
     * @return true for compatible or false for not compatible.
     */
    boolean isVersionCompatible(int contextAppVersion, int appVersionUnderTest);
}
