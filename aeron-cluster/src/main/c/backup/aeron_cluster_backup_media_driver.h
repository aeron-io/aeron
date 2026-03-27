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

#ifndef AERON_CLUSTER_BACKUP_MEDIA_DRIVER_H
#define AERON_CLUSTER_BACKUP_MEDIA_DRIVER_H

#include "aeron_cluster_backup_agent.h"
#include "aeronc.h"
#include "aeron_archive.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Aggregate of Aeron client + Archive client + ClusterBackup agent.
 * Mirrors Java ClusterBackupMediaDriver — bundles the three components
 * needed to run a cluster backup node.
 *
 * The actual MediaDriver is expected to be running externally (e.g.,
 * Java ArchivingMediaDriver or an embedded C driver). This struct
 * manages the Aeron CLIENT connection, archive CLIENT, and backup agent.
 */
typedef struct aeron_cluster_backup_media_driver_stct
{
    aeron_context_t                *aeron_ctx;
    aeron_t                        *aeron;
    bool                            owns_aeron;

    aeron_archive_t                *archive;

    aeron_cluster_backup_agent_t   *backup_agent;
    aeron_cluster_backup_context_t *backup_ctx;

    bool is_closed;
}
aeron_cluster_backup_media_driver_t;

/**
 * Launch a ClusterBackupMediaDriver aggregate.
 *
 * @param driver      out: created driver handle.
 * @param backup_ctx  fully configured backup context (conclude will be called).
 * @param cluster_dir directory for recording log.
 * @return 0 on success, -1 on error.
 */
int aeron_cluster_backup_media_driver_launch(
    aeron_cluster_backup_media_driver_t **driver,
    aeron_cluster_backup_context_t *backup_ctx,
    const char *cluster_dir);

/**
 * Drive one iteration of the backup agent's duty cycle.
 */
int aeron_cluster_backup_media_driver_do_work(
    aeron_cluster_backup_media_driver_t *driver,
    int64_t now_ms);

/**
 * Close and free all resources.
 */
void aeron_cluster_backup_media_driver_close(
    aeron_cluster_backup_media_driver_t *driver);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_BACKUP_MEDIA_DRIVER_H */
