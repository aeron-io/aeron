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

function(aeron_c_cluster_test name file)
    add_executable(${name} ${file})
    add_dependencies(${name} gmock aeron-all-jar aeron_cluster_c_client)
    target_include_directories(${name} PUBLIC ${AERON_C_CLUSTER_SOURCE_PATH}/client)
    target_include_directories(${name} PUBLIC ${CLUSTER_C_CODEC_TARGET_DIR})
    target_link_libraries(${name} aeron aeron_cluster_c_client gmock_main ${CMAKE_THREAD_LIBS_INIT})
    add_test(NAME ${name} COMMAND ${name})
    set_tests_properties(${name} PROPERTIES TIMEOUT 300)
    set_tests_properties(${name} PROPERTIES RUN_SERIAL TRUE)
endfunction()

# Tests are added here as source files are implemented:
# if (AERON_UNIT_TESTS)
#     aeron_c_cluster_test(aeron_cluster_test client/aeron_cluster_test.cpp)
# endif ()
