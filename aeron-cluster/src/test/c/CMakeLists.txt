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

add_definitions(-DCLUSTER_DIR="${CMAKE_CURRENT_BINARY_DIR}/cluster")

function(aeron_c_cluster_test name file)
    add_executable(${name} ${file})
    add_dependencies(${name} gmock aeron_cluster_c_client)
    target_include_directories(${name} PUBLIC ${AERON_C_CLUSTER_SOURCE_PATH}/client)
    target_include_directories(${name} PUBLIC ${CLUSTER_C_CODEC_TARGET_DIR})
    target_link_libraries(${name} aeron aeron_cluster_c_client gmock_main ${CMAKE_THREAD_LIBS_INIT})
    add_test(NAME ${name} COMMAND ${name})
    set_tests_properties(${name} PROPERTIES TIMEOUT 300)
    set_tests_properties(${name} PROPERTIES RUN_SERIAL TRUE)
endfunction()

function(aeron_c_cluster_integration_test name)
    add_executable(${name} ${ARGN})
    add_dependencies(${name} gmock aeron_cluster_c_consensus aeron_cluster_c_backup aeron_archive_c_server aeron_driver)
    target_include_directories(${name} PUBLIC
        ${AERON_C_CLUSTER_CONSENSUS_SOURCE_PATH}
        ${AERON_C_CLUSTER_SERVICE_SOURCE_PATH}
        "${AERON_C_CLUSTER_SOURCE_PATH}/backup"
        "${AERON_C_ARCHIVE_SOURCE_PATH}/client"
        "${AERON_C_ARCHIVE_SOURCE_PATH}/server"
        "${AERON_C_CLIENT_SOURCE_PATH}"
        "${AERON_DRIVER_SOURCE_PATH}"
        ${CLUSTER_C_CODEC_TARGET_DIR})
    target_link_libraries(${name}
        aeron
        aeron_driver
        aeron_archive_c_client
        aeron_archive_c_server
        aeron_cluster_c_client
        aeron_cluster_c_consensus
        aeron_cluster_c_service
        aeron_cluster_c_backup
        gmock_main
        ${CMAKE_THREAD_LIBS_INIT})
    add_test(NAME ${name} COMMAND ${name})
    set_tests_properties(${name} PROPERTIES TIMEOUT 120)
    set_tests_properties(${name} PROPERTIES RUN_SERIAL TRUE)
    set_tests_properties(${name} PROPERTIES LABELS "integration")
endfunction()

if (AERON_TESTS)
    aeron_c_cluster_integration_test(aeron_cluster_integration_test
        integration/aeron_cluster_integration_test.cpp
        integration/aeron_cluster_server_helper.cpp
        integration/aeron_test_clustered_media_driver.cpp)
    aeron_c_cluster_integration_test(aeron_cluster_echo_test
        integration/aeron_cluster_echo_test.cpp
        integration/aeron_cluster_server_helper.cpp
        integration/aeron_test_clustered_media_driver.cpp)
    aeron_c_cluster_integration_test(aeron_cluster_3node_test
        integration/aeron_cluster_three_node_test.cpp
        integration/aeron_test_clustered_media_driver.cpp)
endif ()
