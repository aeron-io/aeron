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

find_package(Java 17 REQUIRED)

add_definitions(-DAERON_ALL_JAR="${AERON_ALL_JAR}")
add_definitions(-DAERON_AGENT_JAR="${AERON_AGENT_JAR}")
add_definitions(-DJAVA_EXECUTABLE="${Java_JAVA_EXECUTABLE}")
add_definitions(-DARCHIVE_DIR="${CMAKE_CURRENT_BINARY_DIR}/archive")
add_definitions(-DJAVA_MAJOR_VERSION=${Java_VERSION_MAJOR})
add_definitions(-DJAVA_MINOR_VERSION=${Java_VERSION_MINOR})

set(TEST_HEADERS)

include_directories(${AERON_ARCHIVE_WRAPPER_SOURCE_PATH})

function(aeron_archive_wrapper_test name file)
    set(wrapper_name "${name}W")
    add_executable(${wrapper_name} ${file} ${TEST_HEADERS})
    add_dependencies(${wrapper_name} aeron_archive_c_client gmock)
    target_include_directories(${wrapper_name} PUBLIC ${AERON_CLIENT_WRAPPER_SOURCE_PATH})
    target_link_libraries(${wrapper_name} aeron_archive_c_client aeron_archive_wrapper gmock_main ${CMAKE_THREAD_LIBS_INIT})
    target_compile_definitions(${wrapper_name} PUBLIC "_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING")
    add_test(NAME ${wrapper_name} COMMAND ${wrapper_name})
endfunction()

if (AERON_UNIT_TESTS)
    aeron_archive_wrapper_test(archiveTest AeronArchiveWrapperTest.cpp)
endif ()