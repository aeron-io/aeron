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

find_package(Java REQUIRED)

set(SOURCE
    # Phase 1 — foundations
    aeron_cluster_recording_log.c
    aeron_cluster_member.c
    aeron_cluster_timer_service.c
    # Phase 2 — publishers and adapters
    aeron_cluster_consensus_publisher.c
    aeron_cluster_egress_publisher.c
    aeron_cluster_log_publisher.c
    aeron_cluster_consensus_adapter.c
    aeron_cluster_ingress_adapter_cm.c
    # Phase 3 — session management
    aeron_cluster_cluster_session.c
    aeron_cluster_session_manager.c
    aeron_cluster_service_proxy_cm.c
    # Phase 4 — snapshot
    aeron_cluster_cm_snapshot_taker.c
    # Phase 5 — election + agent + context
    aeron_cluster_election.c
    aeron_consensus_module_agent.c
    aeron_cm_context.c
    aeron_cluster_mark_file.c
    aeron_cluster_pending_message_tracker.c
)

set(HEADERS
    aeron_consensus_module_configuration.h
    aeron_consensus_module_agent_fwd.h
    aeron_cluster_recording_log.h
    aeron_cluster_member.h
    aeron_cluster_timer_service.h
    aeron_cluster_consensus_publisher.h
    aeron_cluster_egress_publisher.h
    aeron_cluster_log_publisher.h
    aeron_cluster_consensus_adapter.h
    aeron_cluster_ingress_adapter_cm.h
    aeron_cluster_cluster_session.h
    aeron_cluster_session_manager.h
    aeron_cluster_service_proxy_cm.h
    aeron_cluster_cm_snapshot_taker.h
    aeron_cluster_election.h
    aeron_consensus_module_agent.h
    aeron_cm_context.h
)

add_library(aeron_cluster_c_consensus SHARED ${SOURCE} ${HEADERS})
add_library(aeron::aeron_cluster_c_consensus ALIAS aeron_cluster_c_consensus)

add_dependencies(aeron_cluster_c_consensus cluster_c_codecs aeron_archive_c_client)

target_include_directories(aeron_cluster_c_consensus
    PUBLIC  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
            "$<INSTALL_INTERFACE:include/aeron>"
    PRIVATE ${CLUSTER_C_CODEC_TARGET_DIR}
    PRIVATE "${AERON_C_ARCHIVE_SOURCE_PATH}/client")

target_link_libraries(aeron_cluster_c_consensus
    aeron
    aeron_archive_c_client
    ${CMAKE_THREAD_LIBS_INIT})

add_library(aeron_cluster_c_consensus_static STATIC ${SOURCE} ${HEADERS})
add_library(aeron::aeron_cluster_c_consensus_static ALIAS aeron_cluster_c_consensus_static)

add_dependencies(aeron_cluster_c_consensus_static cluster_c_codecs aeron_archive_c_client_static)

target_include_directories(aeron_cluster_c_consensus_static
    PUBLIC  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
            "$<INSTALL_INTERFACE:include/aeron>"
    PRIVATE ${CLUSTER_C_CODEC_TARGET_DIR}
    PRIVATE "${AERON_C_ARCHIVE_SOURCE_PATH}/client")

target_link_libraries(aeron_cluster_c_consensus_static
    aeron_static
    aeron_archive_c_client_static
    ${CMAKE_THREAD_LIBS_INIT})

if (AERON_INSTALL_TARGETS)
    install(
        TARGETS aeron_cluster_c_consensus aeron_cluster_c_consensus_static
        EXPORT  aeron-targets
        RUNTIME DESTINATION lib
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
endif ()
