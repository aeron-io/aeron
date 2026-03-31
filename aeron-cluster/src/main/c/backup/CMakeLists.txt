#
# Copyright 2014-2025 Real Logic Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set(SOURCE
    aeron_cluster_snapshot_replication.c
    aeron_cluster_standby_snapshot_replicator.c
    aeron_cluster_backup_agent.c
    aeron_cluster_backup_media_driver.c
)

set(HEADERS
    aeron_cluster_snapshot_replication.h
    aeron_cluster_standby_snapshot_replicator.h
    aeron_cluster_backup_agent.h
    aeron_cluster_backup_media_driver.h
)

add_library(aeron_cluster_c_backup SHARED ${SOURCE} ${HEADERS})
add_library(aeron::aeron_cluster_c_backup ALIAS aeron_cluster_c_backup)

add_dependencies(aeron_cluster_c_backup
    cluster_c_codecs
    aeron_archive_c_client
    aeron_archive_c_server
    aeron_driver
    aeron_cluster_c_consensus)

target_include_directories(aeron_cluster_c_backup
    PUBLIC  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
            "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>"
            "$<INSTALL_INTERFACE:include/aeron>"
    PRIVATE ${CLUSTER_C_CODEC_TARGET_DIR}
    PRIVATE "${AERON_C_ARCHIVE_SOURCE_PATH}/client"
    PRIVATE "${AERON_C_ARCHIVE_SOURCE_PATH}/server"
    PRIVATE "${AERON_DRIVER_SOURCE_PATH}")

target_link_libraries(aeron_cluster_c_backup
    aeron
    aeron_driver
    aeron_archive_c_client
    aeron_archive_c_server
    aeron_cluster_c_consensus
    aeron_cluster_c_client
    ${CMAKE_THREAD_LIBS_INIT})

add_library(aeron_cluster_c_backup_static STATIC ${SOURCE} ${HEADERS})
add_library(aeron::aeron_cluster_c_backup_static ALIAS aeron_cluster_c_backup_static)

add_dependencies(aeron_cluster_c_backup_static
    cluster_c_codecs
    aeron_archive_c_client_static
    aeron_archive_c_server_static
    aeron_driver_static
    aeron_cluster_c_consensus_static)

target_include_directories(aeron_cluster_c_backup_static
    PUBLIC  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
            "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>"
            "$<INSTALL_INTERFACE:include/aeron>"
    PRIVATE ${CLUSTER_C_CODEC_TARGET_DIR}
    PRIVATE "${AERON_C_ARCHIVE_SOURCE_PATH}/client"
    PRIVATE "${AERON_C_ARCHIVE_SOURCE_PATH}/server"
    PRIVATE "${AERON_DRIVER_SOURCE_PATH}")

target_link_libraries(aeron_cluster_c_backup_static
    aeron_static
    aeron_driver_static
    aeron_archive_c_client_static
    aeron_archive_c_server_static
    aeron_cluster_c_consensus_static
    aeron_cluster_c_client_static
    ${CMAKE_THREAD_LIBS_INIT})

if (AERON_INSTALL_TARGETS)
    install(
        TARGETS aeron_cluster_c_backup aeron_cluster_c_backup_static
        EXPORT  aeron-targets
        RUNTIME DESTINATION lib
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
endif ()
