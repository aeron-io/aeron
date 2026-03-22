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

if (MSVC AND "${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
    set(BUILD_SHARED_LIBS ON)
endif ()

set(CLIENT_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/client/cluster/AeronCluster.h
    ${CMAKE_CURRENT_SOURCE_DIR}/client/cluster/ClusterContext.h
    ${CMAKE_CURRENT_SOURCE_DIR}/client/cluster/EgressPoller.h
    ${CMAKE_CURRENT_SOURCE_DIR}/client/util/ClusterException.h
)

set(SERVICE_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/service/cluster/ClusteredService.h
    ${CMAKE_CURRENT_SOURCE_DIR}/service/cluster/ClusteredServiceContainer.h
)

set(CONSENSUS_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/consensus/ConsensusModule.h
    ${CMAKE_CURRENT_SOURCE_DIR}/consensus/ConsensusModuleContext.h
)

set(HEADERS ${CLIENT_HEADERS} ${SERVICE_HEADERS} ${CONSENSUS_HEADERS})

# Client wrapper (original)
add_library(aeron_cluster_wrapper INTERFACE)
add_library(aeron::aeron_cluster_wrapper ALIAS aeron_cluster_wrapper)

target_include_directories(aeron_cluster_wrapper
    INTERFACE
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
    "$<BUILD_INTERFACE:${AERON_C_CLUSTER_SOURCE_PATH}>"
    "$<BUILD_INTERFACE:${AERON_CLIENT_WRAPPER_SOURCE_PATH}>"
    "$<INSTALL_INTERFACE:include/wrapper>"
)

target_sources(aeron_cluster_wrapper INTERFACE "$<BUILD_INTERFACE:${CLIENT_HEADERS}>")

# Service wrapper
add_library(aeron_cluster_service_wrapper INTERFACE)
add_library(aeron::aeron_cluster_service_wrapper ALIAS aeron_cluster_service_wrapper)

target_include_directories(aeron_cluster_service_wrapper
    INTERFACE
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
    "$<BUILD_INTERFACE:${AERON_C_CLUSTER_SERVICE_SOURCE_PATH}>"
    "$<BUILD_INTERFACE:${AERON_C_CLUSTER_SOURCE_PATH}/client>"
    "$<BUILD_INTERFACE:${AERON_CLIENT_WRAPPER_SOURCE_PATH}>"
    "$<INSTALL_INTERFACE:include/wrapper>"
)

target_sources(aeron_cluster_service_wrapper INTERFACE "$<BUILD_INTERFACE:${SERVICE_HEADERS}>")
target_link_libraries(aeron_cluster_service_wrapper INTERFACE aeron_cluster_c_service)

# Consensus module wrapper
add_library(aeron_cluster_consensus_wrapper INTERFACE)
add_library(aeron::aeron_cluster_consensus_wrapper ALIAS aeron_cluster_consensus_wrapper)

target_include_directories(aeron_cluster_consensus_wrapper
    INTERFACE
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
    "$<BUILD_INTERFACE:${AERON_C_CLUSTER_CONSENSUS_SOURCE_PATH}>"
    "$<BUILD_INTERFACE:${AERON_CLIENT_WRAPPER_SOURCE_PATH}>"
    "$<INSTALL_INTERFACE:include/wrapper>"
)

target_sources(aeron_cluster_consensus_wrapper INTERFACE "$<BUILD_INTERFACE:${CONSENSUS_HEADERS}>")
target_link_libraries(aeron_cluster_consensus_wrapper INTERFACE aeron_cluster_c_consensus)

if (NOT WIN32)
    set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
    set(THREADS_PREFER_PTHREAD_FLAG TRUE)
endif ()

target_link_libraries(aeron_cluster_wrapper INTERFACE ${CMAKE_THREAD_LIBS_INIT})

if (AERON_INSTALL_TARGETS)
    install(
        TARGETS aeron_cluster_wrapper
        EXPORT aeron-targets)
    install(DIRECTORY ./ DESTINATION include/wrapper FILES_MATCHING PATTERN "*.h")
endif ()
