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
    ${CMAKE_CURRENT_SOURCE_DIR}/Aeron.h
    ${CMAKE_CURRENT_SOURCE_DIR}/AeronCounters.h
    ${CMAKE_CURRENT_SOURCE_DIR}/BufferBuilder.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ChannelUri.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ChannelUriStringBuilder.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ClientConductor.h
    ${CMAKE_CURRENT_SOURCE_DIR}/CncFileDescriptor.h
    ${CMAKE_CURRENT_SOURCE_DIR}/CncFileReader.h
    ${CMAKE_CURRENT_SOURCE_DIR}/Context.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ControlledFragmentAssembler.h
    ${CMAKE_CURRENT_SOURCE_DIR}/Counter.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ExclusivePublication.h
    ${CMAKE_CURRENT_SOURCE_DIR}/FragmentAssembler.h
    ${CMAKE_CURRENT_SOURCE_DIR}/HeartbeatTimestamp.h
    ${CMAKE_CURRENT_SOURCE_DIR}/Image.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ImageControlledFragmentAssembler.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ImageFragmentAssembler.h
    ${CMAKE_CURRENT_SOURCE_DIR}/Publication.h
    ${CMAKE_CURRENT_SOURCE_DIR}/Subscription.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/AgentInvoker.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/AgentRunner.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/Atomic64.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/AtomicBuffer.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/AtomicCounter.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/BackOffIdleStrategy.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/BusySpinIdleStrategy.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/CountersReader.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/NoOpIdleStrategy.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/SleepingIdleStrategy.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/YieldingIdleStrategy.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/atomic/Atomic64_gcc_cpp11.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/atomic/Atomic64_gcc_x86_64.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/atomic/Atomic64_msvc.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/errors/DistinctErrorLog.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/errors/ErrorLogDescriptor.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/errors/ErrorLogReader.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/logbuffer/BufferClaim.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/logbuffer/DataFrameHeader.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/logbuffer/FrameDescriptor.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/logbuffer/Header.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/logbuffer/LogBufferDescriptor.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/logbuffer/TermReader.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/status/Position.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/status/ReadablePosition.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/status/StatusIndicatorReader.h
    ${CMAKE_CURRENT_SOURCE_DIR}/concurrent/status/UnsafeBufferPosition.h
    ${CMAKE_CURRENT_SOURCE_DIR}/status/PublicationErrorFrame.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/BitUtil.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/CommandOption.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/CommandOptionParser.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/Exceptions.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/Index.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/LangUtil.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/MacroUtil.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/MemoryMappedFile.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/Platform.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/ScopeUtils.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/StringUtil.h)

# header only library
add_library(aeron_client_wrapper INTERFACE)
add_library(aeron::aeron_client_wrapper ALIAS aeron_client_wrapper)
target_include_directories(aeron_client_wrapper
    INTERFACE "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>" "$<INSTALL_INTERFACE:include/wrapper>")

target_sources(aeron_client_wrapper INTERFACE "$<BUILD_INTERFACE:${HEADERS}>")

if (MSVC)
    string(REPLACE "/" "\\\\" NATIVE_PROJECT_SOURCE_DIR "${PROJECT_SOURCE_DIR}")
else ()
    set(NATIVE_PROJECT_SOURCE_DIR "${PROJECT_SOURCE_DIR}")
endif ()

if (NOT WIN32)
    set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
    set(THREADS_PREFER_PTHREAD_FLAG TRUE)
endif ()

target_link_libraries(aeron_client_wrapper INTERFACE ${CMAKE_THREAD_LIBS_INIT})

if (AERON_INSTALL_TARGETS)
    install(
        TARGETS aeron_client_wrapper
        EXPORT aeron-targets)
    install(DIRECTORY ./ DESTINATION include/wrapper FILES_MATCHING PATTERN "*.h")
endif ()
