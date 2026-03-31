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

/* Forward declaration -- full definition in server/aeron_archiving_media_driver.h */
typedef struct aeron_archiving_media_driver_stct aeron_archiving_media_driver_t;

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Aggregate of ArchivingMediaDriver (in-process driver + archive server) +
 * Aeron client + Archive client + ClusterBackup agent.
 *
 * Mirrors Java ClusterBackupMediaDriver -- bundles the media driver, archive
 * server, and backup agent into a single process. The archiving media driver
 * is launched in-process (no external Java process needed).
 */
typedef struct aeron_cluster_backup_media_driver_stct
{
    aeron_archiving_media_driver_t *archiving_driver;

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
 * This launches an in-process C ArchivingMediaDriver (media driver + archive
 * server), then creates an Aeron client, archive client, and backup agent.
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
 * Drive one iteration of the archiving media driver and backup agent duty cycles.
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
