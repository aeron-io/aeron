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

SET(HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/client/archive/AeronArchive.h
    ${CMAKE_CURRENT_SOURCE_DIR}/client/archive/ArchiveContext.h
    ${CMAKE_CURRENT_SOURCE_DIR}/client/archive/CredentialsSupplier.h
    ${CMAKE_CURRENT_SOURCE_DIR}/client/archive/RecordingPos.h
    ${CMAKE_CURRENT_SOURCE_DIR}/client/archive/ReplayMerge.h
    ${CMAKE_CURRENT_SOURCE_DIR}/client/archive/ReplayParams.h
    ${CMAKE_CURRENT_SOURCE_DIR}/client/archive/ReplicationParams.h
    ${CMAKE_CURRENT_SOURCE_DIR}/client/util/ArchiveExceptions.h
)

# header only library
add_library(aeron_archive_wrapper INTERFACE)
add_library(aeron::aeron_archive_wrapper ALIAS aeron_archive_wrapper)
target_include_directories(aeron_archive_wrapper
    INTERFACE
    "$<BUILD_INTERFACE:${AERON_C_ARCHIVE_SOURCE_PATH}>"
    "$<BUILD_INTERFACE:${AERON_ARCHIVE_WRAPPER_SOURCE_PATH}>"
    "$<INSTALL_INTERFACE:include/wrapper>"
)

target_sources(aeron_archive_wrapper INTERFACE "$<BUILD_INTERFACE:${HEADERS}>")

if (MSVC)
    string(REPLACE "/" "\\\\" NATIVE_PROJECT_SOURCE_DIR "${PROJECT_SOURCE_DIR}")
else ()
    set(NATIVE_PROJECT_SOURCE_DIR "${PROJECT_SOURCE_DIR}")
endif ()

if (NOT WIN32)
    set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
    set(THREADS_PREFER_PTHREAD_FLAG TRUE)
endif ()

target_link_libraries(aeron_archive_wrapper INTERFACE ${CMAKE_THREAD_LIBS_INIT})

if (AERON_INSTALL_TARGETS)
    install(
        TARGETS aeron_archive_wrapper
        EXPORT aeron-targets)
    install(DIRECTORY ./ DESTINATION include/wrapper FILES_MATCHING PATTERN "*.h")
endif ()
