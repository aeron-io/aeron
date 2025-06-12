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

include(CheckSymbolExists)

if (MSVC AND "${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
    set(AERON_LIB_WINSOCK_LIBS wsock32 ws2_32 Iphlpapi)
    set(WSAPOLL_PROTOTYPE_EXISTS True)
endif ()

if (MSVC)
    set(AERON_STATIC_LIB_LINK_OPTS "")
else()
    # Because dlsym() is used to load strategies.
    set(AERON_STATIC_LIB_LINK_OPTS "-rdynamic")
endif()

if (WSAPOLL_PROTOTYPE_EXISTS)
    add_definitions(-DHAVE_WSAPOLL)
endif ()

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
    set(CMAKE_REQUIRED_DEFINITIONS "-D_GNU_SOURCE")
    add_definitions(-D_DEFAULT_SOURCE)
endif ()

check_symbol_exists(fallocate "fcntl.h" FALLOCATE_PROTOTYPE_EXISTS)
check_symbol_exists(posix_fallocate "fcntl.h" POSIX_FALLOCATE_PROTOTYPE_EXISTS)
check_symbol_exists(F_PREALLOCATE "fcntl.h" F_PREALLOCATE_PROTOTYPE_EXISTS)

if (FALLOCATE_PROTOTYPE_EXISTS)
    add_definitions(-DHAVE_FALLOCATE)
endif ()

if (POSIX_FALLOCATE_PROTOTYPE_EXISTS)
    add_definitions(-DHAVE_POSIX_FALLOCATE)
endif ()

if (F_PREALLOCATE_PROTOTYPE_EXISTS)
    add_definitions(-DHAVE_F_PREALLOCATE)
endif ()

if (MSVC AND "${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
    set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)
    set(BUILD_SHARED_LIBS ON)
endif ()

find_package(Java REQUIRED)

set(CODEC_SCHEMA ${ARCHIVE_CODEC_SCHEMA_DIR}/aeron-archive-codecs.xml)
set(GENERATED_C_CODECS
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/archiveIdRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/attachSegmentsRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/authConnectRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/booleanType.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/boundedReplayRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/catalogHeader.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/challenge.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/challengeResponse.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/closeSessionRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/connectRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/controlResponse.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/controlResponseCode.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/deleteDetachedSegmentsRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/detachSegmentsRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/extendRecordingRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/extendRecordingRequest2.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/findLastMatchingRecordingRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/keepAliveRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/listRecordingRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/listRecordingSubscriptionsRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/listRecordingsForUriRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/listRecordingsRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/maxRecordedPositionRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/messageHeader.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/migrateSegmentsRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/purgeRecordingRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/purgeSegmentsRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/recordingDescriptor.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/recordingDescriptorHeader.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/recordingPositionRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/recordingProgress.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/recordingSignal.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/recordingSignalEvent.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/recordingStarted.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/recordingState.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/recordingStopped.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/recordingSubscriptionDescriptor.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/replayRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/replayTokenRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/replicateRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/replicateRequest2.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/sourceLocation.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/startPositionRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/startRecordingRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/startRecordingRequest2.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/stopAllReplaysRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/stopPositionRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/stopRecordingByIdentityRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/stopRecordingRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/stopRecordingSubscriptionRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/stopReplayRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/stopReplicationRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/taggedReplicateRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/truncateRecordingRequest.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/varAsciiEncoding.h
    ${ARCHIVE_CODEC_TARGET_DIR}/aeron_c_archive_client/varDataEncoding.h)

add_custom_command(OUTPUT ${GENERATED_C_CODECS}
    COMMAND ${CMAKE_COMMAND} -E env JAVA_HOME=$ENV{JAVA_HOME} BUILD_JAVA_HOME=$ENV{BUILD_JAVA_HOME} BUILD_JAVA_VERSION=$ENV{BUILD_JAVA_VERSION} ${GRADLE_WRAPPER} -Dcodec.target.dir=${ARCHIVE_C_CODEC_TARGET_DIR} :aeron-archive:generateCCodecs --no-daemon --console=plain -q
    DEPENDS ${CODEC_SCHEMA} aeron-all-jar
    WORKING_DIRECTORY ${ARCHIVE_CODEC_WORKING_DIR}
    COMMENT "Generating C Archive codecs")

add_custom_target(c_codecs DEPENDS ${GENERATED_C_CODECS})

SET(SOURCE
    client/aeron_archive_async_connect.c
    client/aeron_archive_client.c
    client/aeron_archive_configuration.c
    client/aeron_archive_context.c
    client/aeron_archive_control_response_poller.c
    client/aeron_archive_credentials_supplier.c
    client/aeron_archive_proxy.c
    client/aeron_archive_recording_descriptor_poller.c
    client/aeron_archive_recording_pos.c
    client/aeron_archive_recording_signal.c
    client/aeron_archive_recording_subscription_descriptor_poller.c
    client/aeron_archive_replay_merge.c
    client/aeron_archive_replay_params.c
    client/aeron_archive_replication_params.c
)

SET(HEADERS
    client/aeron_archive.h
    client/aeron_archive_async_connect.h
    client/aeron_archive_client.h
    client/aeron_archive_configuration.h
    client/aeron_archive_context.h
    client/aeron_archive_control_response_poller.h
    client/aeron_archive_credentials_supplier.h
    client/aeron_archive_proxy.h
    client/aeron_archive_recording_descriptor_poller.h
    client/aeron_archive_recording_signal.h
    client/aeron_archive_recording_subscription_descriptor_poller.h
    client/aeron_archive_replay_params.h
)

# shared library
add_library(aeron_archive_c_client SHARED ${SOURCE} ${HEADERS})
add_library(aeron::aeron_archive_c_client ALIAS aeron_archive_c_client)

add_dependencies(aeron_archive_c_client c_codecs)

target_include_directories(aeron_archive_c_client
    PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>" "$<INSTALL_INTERFACE:include/aeron>"
    PRIVATE ${ARCHIVE_CODEC_TARGET_DIR})

target_link_libraries(
    aeron_archive_c_client
    aeron
    ${CMAKE_THREAD_LIBS_INIT})

# static library
add_library(aeron_archive_c_client_static STATIC ${SOURCE} ${HEADERS})
add_library(aeron::aeron_archive_c_client_static ALIAS aeron_archive_c_client_static)

add_dependencies(aeron_archive_c_client_static c_codecs)

target_include_directories(aeron_archive_c_client_static
    PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>" "$<INSTALL_INTERFACE:include/aeron>"
    PRIVATE ${ARCHIVE_CODEC_TARGET_DIR})

target_link_libraries(
    aeron_archive_c_client_static
    aeron_static
    ${CMAKE_THREAD_LIBS_INIT})

if (NOT WIN32)
    set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
    set(THREADS_PREFER_PTHREAD_FLAG TRUE)
endif ()

if (AERON_INSTALL_TARGETS)
    install(
        TARGETS aeron_archive_c_client aeron_archive_c_client_static
        EXPORT aeron-targets
        RUNTIME DESTINATION lib
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
    install(DIRECTORY ./ DESTINATION include/aeron
        FILES_MATCHING PATTERN "aeron_archive.h"
        PATTERN "aeron_archive_context.h"
        PATTERN "aeron_archive_credentials_supplier.h")
endif ()
