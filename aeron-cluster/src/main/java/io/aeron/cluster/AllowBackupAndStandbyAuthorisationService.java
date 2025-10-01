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
package io.aeron.cluster;

import io.aeron.cluster.codecs.BackupQueryDecoder;
import io.aeron.cluster.codecs.HeartbeatRequestDecoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.StandbySnapshotDecoder;
import io.aeron.security.AuthorisationService;

/**
 * An {@link AuthorisationService} that allows the actions required by Cluster Backup
 * and Aeron Cluster Standby.
 */
public enum AllowBackupAndStandbyAuthorisationService implements AuthorisationService
{
    /**
     * As there is no instance state then this object can be used to save on allocation.
     */
    INSTANCE;

    /**
     * {@inheritDoc}
     */
    public boolean isAuthorised(
        final int protocolId,
        final int actionId,
        final Object type,
        final byte[] encodedPrincipal)
    {
        return MessageHeaderDecoder.SCHEMA_ID == protocolId &&
            (BackupQueryDecoder.TEMPLATE_ID == actionId || HeartbeatRequestDecoder.TEMPLATE_ID == actionId ||
                StandbySnapshotDecoder.TEMPLATE_ID == actionId);
    }
}
