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

add_definitions(-DAERON_ALL_JAR="${AERON_ALL_JAR}")
add_definitions(-DAERON_AGENT_JAR="${AERON_AGENT_JAR}")
add_definitions(-DJAVA_EXECUTABLE="${Java_JAVA_EXECUTABLE}")
add_definitions(-DCLUSTER_DIR="${CMAKE_CURRENT_BINARY_DIR}/cluster")
add_definitions(-DJAVA_MAJOR_VERSION=${Java_VERSION_MAJOR})
add_definitions(-DJAVA_MINOR_VERSION=${Java_VERSION_MINOR})

set(TEST_HEADERS)

include_directories(${AERON_CLUSTER_WRAPPER_SOURCE_PATH})

function(aeron_cluster_wrapper_test name file)
    set(wrapper_name "${name}W")
    add_executable(${wrapper_name} ${file} ${TEST_HEADERS})
    add_dependencies(${wrapper_name} aeron_cluster_c_client gmock)
    target_include_directories(${wrapper_name} PUBLIC
        ${AERON_CLIENT_WRAPPER_SOURCE_PATH}
        ${AERON_C_CLUSTER_SOURCE_PATH}/client
        ${CLUSTER_C_CODEC_TARGET_DIR})
    target_link_libraries(${wrapper_name} aeron_cluster_c_client aeron_cluster_wrapper gmock_main ${CMAKE_THREAD_LIBS_INIT})
    target_compile_definitions(${wrapper_name} PUBLIC "_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING" "AERON_CLUSTER_TESTING")
    add_test(NAME ${wrapper_name} COMMAND ${wrapper_name})
    set_tests_properties(${wrapper_name} PROPERTIES TIMEOUT 300)
    set_tests_properties(${wrapper_name} PROPERTIES RUN_SERIAL TRUE)
endfunction()

# Tests are added here as wrapper headers are implemented:
if (AERON_UNIT_TESTS)
    aeron_cluster_wrapper_test(clusterTest AeronClusterWrapperTest.cpp)
    aeron_cluster_wrapper_test(egressPollerTest EgressPollerTest.cpp)
endif ()

# Service-side wrapper tests need the service library
function(aeron_cluster_service_wrapper_test name file)
    set(wrapper_name "${name}W")
    add_executable(${wrapper_name} ${file} ${TEST_HEADERS})
    add_dependencies(${wrapper_name} aeron_cluster_c_service aeron_cluster_c_client gmock)
    target_include_directories(${wrapper_name} PUBLIC
        ${AERON_CLIENT_WRAPPER_SOURCE_PATH}
        ${AERON_CLUSTER_WRAPPER_SOURCE_PATH}
        ${AERON_C_CLUSTER_SOURCE_PATH}/client
        ${AERON_C_CLUSTER_SOURCE_PATH}/service
        ${AERON_C_CLUSTER_SOURCE_PATH}/consensus
        ${CMAKE_CURRENT_SOURCE_DIR}/../../main/cpp_wrapper
        ${CLUSTER_C_CODEC_TARGET_DIR})
    target_link_libraries(${wrapper_name}
        aeron_cluster_c_service aeron_cluster_c_client aeron_cluster_wrapper
        gmock_main ${CMAKE_THREAD_LIBS_INIT})
    target_compile_definitions(${wrapper_name} PUBLIC "_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING")
    add_test(NAME ${wrapper_name} COMMAND ${wrapper_name})
    set_tests_properties(${wrapper_name} PROPERTIES TIMEOUT 300)
endfunction()

if (AERON_UNIT_TESTS)
    aeron_cluster_service_wrapper_test(clusteredServiceContainerTest ClusteredServiceContainerTest.cpp)
endif ()
