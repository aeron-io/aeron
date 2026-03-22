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
    aeron_cluster_service_context.c
    aeron_cluster_client_session.c
    aeron_cluster_consensus_module_proxy.c
    aeron_cluster_service_adapter.c
    aeron_cluster_bounded_log_adapter.c
    aeron_cluster_service_snapshot_taker.c
    aeron_cluster_service_snapshot_loader.c
    aeron_clustered_service_agent.c
)

set(HEADERS
    aeron_cluster_service.h
    aeron_cluster_service_context.h
    aeron_cluster_client_session.h
    aeron_cluster_consensus_module_proxy.h
    aeron_cluster_service_adapter.h
    aeron_cluster_bounded_log_adapter.h
    aeron_cluster_service_snapshot_taker.h
    aeron_cluster_service_snapshot_loader.h
    aeron_clustered_service_agent.h
)

add_library(aeron_cluster_c_service SHARED ${SOURCE} ${HEADERS})
add_library(aeron::aeron_cluster_c_service ALIAS aeron_cluster_c_service)

# Service layer depends on the codec target (same codecs as client)
add_dependencies(aeron_cluster_c_service cluster_c_codecs)

target_include_directories(aeron_cluster_c_service
    PUBLIC  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
            "$<INSTALL_INTERFACE:include/aeron>"
    PRIVATE ${CLUSTER_C_CODEC_TARGET_DIR})

target_link_libraries(aeron_cluster_c_service
    aeron
    ${CMAKE_THREAD_LIBS_INIT})

add_library(aeron_cluster_c_service_static STATIC ${SOURCE} ${HEADERS})
add_library(aeron::aeron_cluster_c_service_static ALIAS aeron_cluster_c_service_static)

add_dependencies(aeron_cluster_c_service_static cluster_c_codecs)

target_include_directories(aeron_cluster_c_service_static
    PUBLIC  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
            "$<INSTALL_INTERFACE:include/aeron>"
    PRIVATE ${CLUSTER_C_CODEC_TARGET_DIR})

target_link_libraries(aeron_cluster_c_service_static
    aeron_static
    ${CMAKE_THREAD_LIBS_INIT})

if (AERON_INSTALL_TARGETS)
    install(
        TARGETS aeron_cluster_c_service aeron_cluster_c_service_static
        EXPORT  aeron-targets
        RUNTIME DESTINATION lib
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
endif ()
