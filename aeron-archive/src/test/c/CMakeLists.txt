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

set(AERON_TEST_SUPPORT_C_PATH "${CMAKE_SOURCE_DIR}/aeron-test-support/src/main/c")

add_definitions(-DAERON_ALL_JAR="${AERON_ALL_JAR}")
add_definitions(-DAERON_AGENT_JAR="${AERON_AGENT_JAR}")
add_definitions(-DJAVA_EXECUTABLE="${Java_JAVA_EXECUTABLE}")
add_definitions(-DARCHIVE_DIR="${CMAKE_CURRENT_BINARY_DIR}/archive")
add_definitions(-DJAVA_MAJOR_VERSION=${Java_VERSION_MAJOR})
add_definitions(-DJAVA_MINOR_VERSION=${Java_VERSION_MINOR})

set(TEST_HEADERS
    EmbeddedMediaDriver.h
    TestArchive.h
    client/ArchiveClientTestUtils.h)

function(aeron_c_archive_unit_test name file)
    add_executable(${name} ${file})
    add_dependencies(${name} gmock aeron_archive_c_client)
    target_include_directories(${name} PUBLIC ${AERON_C_ARCHIVE_SOURCE_PATH})
    target_include_directories(${name} PUBLIC ${ARCHIVE_C_CODEC_TARGET_DIR})
    target_include_directories(${name} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
    target_include_directories(${name} PUBLIC ${AERON_TEST_SUPPORT_C_PATH})
    target_link_libraries(${name} aeron aeron_driver aeron_archive_c_client gmock_main ${CMAKE_THREAD_LIBS_INIT})
    add_test(NAME ${name} COMMAND ${name})
    set_tests_properties(${name} PROPERTIES TIMEOUT 300)
    set_tests_properties(${name} PROPERTIES RUN_SERIAL TRUE)
endfunction()

function(aeron_c_archive_system_test name file)
    add_executable(${name} ${TEST_HEADERS} ${file})
    add_dependencies(${name} gmock aeron-all-jar aeron_archive_c_client)
    target_include_directories(${name} PUBLIC ${AERON_C_ARCHIVE_SOURCE_PATH})
    target_include_directories(${name} PUBLIC ${ARCHIVE_C_CODEC_TARGET_DIR})
    target_include_directories(${name} PUBLIC ${AERON_TEST_SUPPORT_C_PATH})
    target_include_directories(${name} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(${name} aeron aeron_driver aeron_archive_c_client gmock_main ${CMAKE_THREAD_LIBS_INIT})
    add_test(NAME ${name} COMMAND ${name})
    set_tests_properties(${name} PROPERTIES TIMEOUT 300)
    set_tests_properties(${name} PROPERTIES RUN_SERIAL TRUE)
endfunction()

if (AERON_UNIT_TESTS)
    aeron_c_archive_unit_test(aeron_archive_async_connect_leak_test client/aeron_archive_async_connect_leak_test.cpp)
    aeron_c_archive_unit_test(aeron_archive_persistent_subscription_context_test client/aeron_archive_persistent_subscription_context_test.cpp)
endif ()

if (AERON_SYSTEM_TESTS)
    aeron_c_archive_system_test(aeron_archive_async_client_test client/aeron_archive_async_client_test.cpp)
    aeron_c_archive_system_test(aeron_archive_persistent_subscription_test client/aeron_archive_persistent_subscription_test.cpp)
    aeron_c_archive_system_test(aeron_archive_persistent_subscription_resilience_test client/aeron_archive_persistent_subscription_resilience_test.cpp)
    target_sources(aeron_archive_persistent_subscription_resilience_test PRIVATE
        ${AERON_TEST_SUPPORT_C_PATH}/aeron_stream_id_loss_generator.c
        ${AERON_TEST_SUPPORT_C_PATH}/aeron_stream_id_frame_data_loss_generator.c
        ${AERON_TEST_SUPPORT_C_PATH}/aeron_frame_data_loss_generator.c)
    aeron_c_archive_system_test(aeron_archive_test client/aeron_archive_test.cpp)
endif ()
