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

set(CODEC_SCHEMA ${CLUSTER_CODEC_SCHEMA_DIR}/aeron-cluster-codecs.xml)

set(GENERATED_C_CODECS
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/addPassiveMember.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/adminRequest.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/adminRequestType.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/adminResponse.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/adminResponseCode.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/appendPosition.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/backupQuery.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/backupResponse.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/booleanType.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/cancelTimer.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/canvassPosition.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/catchupPosition.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/challenge.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/challengeResponse.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/changeType.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/clientSession.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/closeReason.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/closeSession.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/clusterAction.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/clusterActionRequest.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/clusterMembers.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/clusterMembersChange.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/clusterMembersExtendedResponse.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/clusterMembersQuery.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/clusterMembersResponse.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/clusterSession.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/clusterTimeUnit.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/commitPosition.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/consensusModule.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/eventCode.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/groupSizeEncoding.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/heartbeatRequest.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/heartbeatResponse.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/joinCluster.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/joinLog.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/membershipChangeEvent.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/messageHeader.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/newLeaderEvent.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/newLeadershipTerm.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/newLeadershipTermEvent.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/pendingMessageTracker.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/removeMember.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/requestServiceAck.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/requestVote.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/scheduleTimer.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/serviceAck.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/serviceTerminationPosition.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/sessionCloseEvent.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/sessionCloseRequest.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/sessionConnectRequest.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/sessionEvent.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/sessionKeepAlive.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/sessionMessageHeader.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/sessionOpenEvent.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/snapshotMark.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/snapshotMarker.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/snapshotRecordingQuery.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/snapshotRecordings.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/standbySnapshot.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/stopCatchup.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/terminationAck.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/terminationPosition.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/timer.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/timerEvent.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/varAsciiEncoding.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/varDataEncoding.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/vote.h)

add_custom_command(OUTPUT ${GENERATED_C_CODECS}
    COMMAND ${CMAKE_COMMAND} -E env JAVA_HOME=$ENV{JAVA_HOME} BUILD_JAVA_HOME=$ENV{BUILD_JAVA_HOME} BUILD_JAVA_VERSION=$ENV{BUILD_JAVA_VERSION} ${GRADLE_WRAPPER} -Dcodec.target.dir=${CLUSTER_C_CODEC_TARGET_DIR} :aeron-cluster:generateCCodecs --no-daemon --console=plain -q
    DEPENDS ${CODEC_SCHEMA} aeron-all-jar
    WORKING_DIRECTORY ${CLUSTER_CODEC_WORKING_DIR}
    COMMENT "Generating C Cluster codecs")

add_custom_target(cluster_c_codecs DEPENDS ${GENERATED_C_CODECS})

set(SOURCE
    client/aeron_cluster_async_connect.c
    client/aeron_cluster_client.c
    client/aeron_cluster_client_version.c
    client/aeron_cluster_configuration.c
    client/aeron_cluster_context.c
    client/aeron_cluster_credentials_supplier.c
    client/aeron_cluster_egress_poller.c
    client/aeron_cluster_ingress_proxy.c
)

set(HEADERS
    client/aeron_cluster.h
    client/aeron_cluster_async_connect.h
    client/aeron_cluster_client.h
    client/aeron_cluster_client_version.h
    client/aeron_cluster_configuration.h
    client/aeron_cluster_context.h
    client/aeron_cluster_credentials_supplier.h
    client/aeron_cluster_egress_poller.h
    client/aeron_cluster_ingress_proxy.h
)

add_library(aeron_cluster_c_client SHARED ${SOURCE} ${HEADERS})
add_library(aeron::aeron_cluster_c_client ALIAS aeron_cluster_c_client)

add_dependencies(aeron_cluster_c_client cluster_c_codecs)

target_include_directories(aeron_cluster_c_client
    PUBLIC  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
            "$<INSTALL_INTERFACE:include/aeron>"
    PRIVATE ${CLUSTER_C_CODEC_TARGET_DIR})

target_link_libraries(aeron_cluster_c_client
    aeron
    ${CMAKE_THREAD_LIBS_INIT})

add_library(aeron_cluster_c_client_static STATIC ${SOURCE} ${HEADERS})
add_library(aeron::aeron_cluster_c_client_static ALIAS aeron_cluster_c_client_static)

add_dependencies(aeron_cluster_c_client_static cluster_c_codecs)

target_include_directories(aeron_cluster_c_client_static
    PUBLIC  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
            "$<INSTALL_INTERFACE:include/aeron>"
    PRIVATE ${CLUSTER_C_CODEC_TARGET_DIR})

target_link_libraries(aeron_cluster_c_client_static
    aeron_static
    ${CMAKE_THREAD_LIBS_INIT})

if (AERON_INSTALL_TARGETS)
    install(
        TARGETS aeron_cluster_c_client aeron_cluster_c_client_static
        EXPORT  aeron-targets
        RUNTIME DESTINATION lib
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
    install(DIRECTORY ./ DESTINATION include/aeron FILES_MATCHING PATTERN "aeron_cluster.h")
endif ()
