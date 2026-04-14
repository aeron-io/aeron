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

function(aeron_c_archive_test name file)
        add_executable(${name} "TestArchive.h" ${file} ${SOURCE} ${HEADERS})
        add_dependencies(${name} gmock aeron-all-jar aeron_archive_c_client)
        target_include_directories(${name} PUBLIC ${AERON_C_ARCHIVE_SOURCE_PATH})
        target_include_directories(${name} PUBLIC ${ARCHIVE_C_CODEC_TARGET_DIR})
        target_link_libraries(${name} aeron aeron_archive_c_client gmock_main ${CMAKE_THREAD_LIBS_INIT})
        add_test(NAME ${name} COMMAND ${name})
        set_tests_properties(${name} PROPERTIES TIMEOUT 300)
        set_tests_properties(${name} PROPERTIES RUN_SERIAL TRUE)
endfunction()

function(aeron_c_archive_unit_test name file)
    add_executable(${name} ${file})
    add_dependencies(${name} gmock aeron_archive_c_client)
    target_include_directories(${name} PUBLIC ${AERON_C_ARCHIVE_SOURCE_PATH})
    target_include_directories(${name} PUBLIC ${ARCHIVE_C_CODEC_TARGET_DIR})
    target_include_directories(${name} PUBLIC ${AERON_C_CLIENT_SOURCE_PATH})
    target_link_libraries(${name} aeron aeron_archive_c_client gmock_main ${CMAKE_THREAD_LIBS_INIT})
    add_test(NAME ${name} COMMAND ${name})
endfunction()

if (AERON_UNIT_TESTS)
    aeron_c_archive_unit_test(aeron_archive_async_connect_leak_test client/aeron_archive_async_connect_leak_test.cpp)
    aeron_c_archive_test(aeron_archive_test client/aeron_archive_test.cpp)
endif ()
