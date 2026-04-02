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

#ifndef AERON_ARCHIVING_MEDIA_DRIVER_H
#define AERON_ARCHIVING_MEDIA_DRIVER_H

/* Forward declarations for media driver types (from aeronmd.h). */
typedef struct aeron_driver_context_stct aeron_driver_context_t;
typedef struct aeron_driver_stct aeron_driver_t;

#include "aeron_archive_server.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * An aggregate that bundles a MediaDriver and an Archive server into one process.
 *
 * Corresponds to Java ArchivingMediaDriver. Launching creates both a media driver
 * and an archive server that share the same process. Closing shuts down both in
 * reverse order (archive first, then driver).
 */
typedef struct aeron_archiving_media_driver_stct
{
    aeron_driver_t               *driver;
    aeron_driver_context_t       *driver_ctx;
    aeron_archive_server_t       *archive;
    aeron_archive_server_context_t *archive_ctx;
    volatile bool                 running;
    void                         *archive_thread; /* aeron_thread_t */
}
aeron_archiving_media_driver_t;

/**
 * Launch an archiving media driver with the provided driver and archive contexts.
 *
 * The driver context and archive context are optional (NULL for defaults).
 * Both contexts will be initialised and concluded as needed.
 *
 * The archive will be configured to use the driver's aeron directory and
 * INVOKER threading mode so both run on the same thread.
 *
 * @param archiving_driver  out parameter for the launched instance.
 * @param driver_ctx        media driver context (may be NULL for defaults).
 * @param archive_ctx       archive server context (may be NULL for defaults).
 * @return 0 on success, -1 on failure.
 */
int aeron_archiving_media_driver_launch(
    aeron_archiving_media_driver_t **archiving_driver,
    aeron_driver_context_t *driver_ctx,
    aeron_archive_server_context_t *archive_ctx);

/**
 * Perform a single unit of work for both the driver and the archive.
 *
 * Call this in a loop from your main thread when using the combined
 * archiving media driver.
 *
 * @param archiving_driver  the archiving media driver.
 * @return total work done by both driver and archive, or -1 on error.
 */
int aeron_archiving_media_driver_do_work(aeron_archiving_media_driver_t *archiving_driver);

/**
 * Close the archiving media driver, shutting down the archive and then the driver.
 *
 * @param archiving_driver  the archiving media driver to close (may be NULL).
 * @return 0 on success, -1 on failure.
 */
int aeron_archiving_media_driver_close(aeron_archiving_media_driver_t *archiving_driver);

/**
 * Get the archive server from the archiving media driver.
 *
 * @param archiving_driver  the archiving media driver.
 * @return the archive server, or NULL.
 */
aeron_archive_server_t *aeron_archiving_media_driver_archive(
    aeron_archiving_media_driver_t *archiving_driver);

/**
 * Get the media driver from the archiving media driver.
 *
 * @param archiving_driver  the archiving media driver.
 * @return the media driver, or NULL.
 */
aeron_driver_t *aeron_archiving_media_driver_driver(
    aeron_archiving_media_driver_t *archiving_driver);

#ifdef __cplusplus
}
#endif

#endif /* AERON_ARCHIVING_MEDIA_DRIVER_H */
