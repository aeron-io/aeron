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
cmake_minimum_required(VERSION 3.30 FATAL_ERROR)
cmake_policy(VERSION 3.30)
cmake_policy(SET CMP0003 NEW)
cmake_policy(SET CMP0135 NEW)

if (${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})
    message(FATAL_ERROR "In-source builds not allowed. Please make a new directory (called a build directory)"
        "and run CMake from there. You may need to remove CMakeCache.txt.")
endif ()

string(TIMESTAMP DSTAMP "%Y%m%d")

include(FetchContent)

file(STRINGS version.txt AERON_VERSION_TXT LIMIT_COUNT 1 REGEX "^[0-9]+(\\.[0-9]+)+")
string(REGEX REPLACE "^([0-9]+(\\.[0-9]+)+).*$" "\\1" AERON_VERSION_FROM_FILE "${AERON_VERSION_TXT}")

if (${CMAKE_CURRENT_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_BINARY_DIR})
    message(FATAL_ERROR "In-source builds not allowed. Please make a new directory (called a build directory)"
        "and run CMake from there. You may need to remove CMakeCache.txt.")
endif ()

if (${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_SOURCE_DIR})
    set(STANDALONE_BUILD TRUE)
endif ()

option(BUILD_AERON_DRIVER "Build Aeron driver" ON)
option(BUILD_AERON_ARCHIVE_API "Build Aeron Archive API" ON)

option(C_WARNINGS_AS_ERRORS "Enable warnings as errors for C" OFF)
option(CXX_WARNINGS_AS_ERRORS "Enable warnings as errors for C++" OFF)
option(SANITISE_BUILD "Enable sanitise options" OFF)
option(COVERAGE_BUILD "Enable code coverage" OFF)
option(AERON_HIDE_DEPRECATION_MESSAGE "Hide C++ deprecation message" OFF)
option(AERON_TESTS "Enable tests" ${STANDALONE_BUILD})
option(AERON_UNIT_TESTS "Enable unit tests" ${STANDALONE_BUILD})
option(AERON_SYSTEM_TESTS "Enable system tests" ${STANDALONE_BUILD})
option(AERON_SLOW_SYSTEM_TESTS "Enable slow system tests" OFF)
option(AERON_BUILD_SAMPLES "Enable building the sample projects" ${STANDALONE_BUILD})
option(LINK_SAMPLES_CLIENT_SHARED "Enable shared linking for sample projects" OFF)
option(AERON_BUILD_DOCUMENTATION "Build Aeron documentation" ${STANDALONE_BUILD})
option(AERON_INSTALL_TARGETS "Enable installation step" ${STANDALONE_BUILD})

unset(STANDALONE_BUILD)

# CMAKE_OSX_ARCHITECTURES can be set to arm64 or x86_64 (or both) to make a specific architecture version. Requires
# CMake 3.21. Must be set before project(), etc.
#set(CMAKE_OSX_ARCHITECTURES x86_64)

project("aeron" VERSION "${AERON_VERSION_FROM_FILE}")

if (AERON_TESTS)
    enable_testing()
    include(CTest)
endif ()

# default built type is Release
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build" FORCE)
endif (NOT CMAKE_BUILD_TYPE)

##########################################################
# Platform flags, etc.

find_package(Threads)

##########################################################
# Doxygen for generating doc

if (AERON_BUILD_DOCUMENTATION)
    find_package(Doxygen)
endif ()

if (NOT DEFINED CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 11)
endif ()

if (NOT DEFINED CMAKE_CXX_EXTENSIONS)
    set(CMAKE_CXX_EXTENSIONS OFF)
endif ()

if (NOT DEFINED CMAKE_C_STANDARD)
    set(CMAKE_C_STANDARD 11)
endif ()

if (NOT DEFINED CMAKE_C_EXTENSIONS)
    set(CMAKE_C_EXTENSIONS OFF)
endif ()

execute_process(
    COMMAND git log -1 --format=%h --abbrev=10
    WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
    OUTPUT_VARIABLE AERON_VERSION_GITSHA
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
    COMMAND git diff-index --quiet HEAD --
    WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
    RESULT_VARIABLE AERON_GIT_DIFF_INDEX_RESULT
)

if (NOT AERON_GIT_DIFF_INDEX_RESULT EQUAL 0)
    set(AERON_VERSION_GITSHA "${AERON_VERSION_GITSHA}+guilty")
endif ()

add_definitions(-DAERON_VERSION_TXT="${AERON_VERSION_TXT}")
add_definitions(-DAERON_VERSION_MAJOR=${aeron_VERSION_MAJOR})
add_definitions(-DAERON_VERSION_MINOR=${aeron_VERSION_MINOR})
add_definitions(-DAERON_VERSION_PATCH=${aeron_VERSION_PATCH})
add_definitions(-DAERON_VERSION_GITSHA="${AERON_VERSION_GITSHA}")

if (AERON_HIDE_DEPRECATION_MESSAGE)
    add_compile_definitions("AERON_HIDE_DEPRECATION_MESSAGE")
endif (AERON_HIDE_DEPRECATION_MESSAGE)

# all UNIX-based platform compiler flags
if (UNIX)
    add_compile_options(-fstrict-aliasing -Wall -Wpedantic -Wextra -Wno-unused-parameter -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer)

    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "11.0")
        add_compile_options(-Wno-error=maybe-uninitialized)
    endif ()

    if (C_WARNINGS_AS_ERRORS)
        add_compile_options($<$<COMPILE_LANGUAGE:C>:-Werror>)
    endif (C_WARNINGS_AS_ERRORS)

    if (CXX_WARNINGS_AS_ERRORS)
        add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Werror>)
    endif (CXX_WARNINGS_AS_ERRORS)

    if (SANITISE_BUILD)
        if (NOT DEFINED APPLE)
            set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address -fsanitize-address-use-after-scope -fsanitize=leak -fsanitize=undefined -DAERON_SANITIZE_ENABLED")
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize-address-use-after-scope -fsanitize=leak -fsanitize=undefined -DAERON_SANITIZE_ENABLED")
        endif ()
    endif (SANITISE_BUILD)

    if (COVERAGE_BUILD)
        add_compile_options(-fno-inline --coverage)
        SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage")
    endif (COVERAGE_BUILD)
endif ()

# platform specific flags
if (APPLE)
    add_compile_options(-Wsign-compare)
    add_definitions(-DDarwin)
    add_compile_options(-Wno-deprecated-register)

    if (SANITISE_BUILD)
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address -fsanitize-address-use-after-scope -fsanitize=undefined -DAERON_SANITIZE_ENABLED")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize-address-use-after-scope -fsanitize=undefined -DAERON_SANITIZE_ENABLED")
    endif (SANITISE_BUILD)
elseif (CYGWIN)
    add_definitions(-DWIN32)
    if (AERON_TESTS)
        add_definitions(-DGTEST_HAS_PTHREAD)
    endif ()
    set(CMAKE_CXX_EXTENSIONS ON)
elseif (MSVC)
    add_definitions(-DWIN32)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
    add_definitions(-D_CRT_NONSTDC_NO_WARNINGS)
    add_definitions(-DNOMINMAX)

    if (${MSVC_VERSION} GREATER_EQUAL 1915)
        # Acknowledge that we understand MSVC resolved a byte alignment issue in this compiler
        add_definitions(-D_ENABLE_EXTENDED_ALIGNED_STORAGE)
    endif ()

    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd /Od /Zi /MP /wd4251 /Oy-")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /MT /MP /wd4251 /Oy-")
    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} /MTd /Oy-")
    set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} /MT /Oy-")

    unset(CMAKE_C_STANDARD) # To avoid `/std:c11` in the compiler options

    if (C_WARNINGS_AS_ERRORS)
        add_compile_options($<$<COMPILE_LANGUAGE:C>:/WX>)
    endif (C_WARNINGS_AS_ERRORS)

    if (CXX_WARNINGS_AS_ERRORS)
        add_compile_options($<$<COMPILE_LANGUAGE:CXX>:/WX>)
    endif (CXX_WARNINGS_AS_ERRORS)

    if (SANITISE_BUILD)
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /fsanitize=address /Zi /D AERON_SANITIZE_ENABLED")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /fsanitize=address /Zi /D AERON_SANITIZE_ENABLED")
    endif (SANITISE_BUILD)

endif ()

if (DEFINED AERON_COMPILER_OPTIMIZATION_LEVEL)
    if (MSVC)
        add_compile_options(/O${AERON_COMPILER_OPTIMIZATION_LEVEL})
    else ()
        add_compile_options(-O${AERON_COMPILER_OPTIMIZATION_LEVEL})
    endif ()
endif()

##########################################################
# Project variables, etc.

if (NOT DEFINED GRADLE_WRAPPER)
    if (MSVC)
        set(GRADLE_WRAPPER "gradlew.bat" CACHE INTERNAL "Location of the Gradle wrapper script")
    else ()
        set(GRADLE_WRAPPER "./gradlew" CACHE INTERNAL "Location of the Gradle wrapper script")
    endif ()
endif ()

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/binaries")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/lib")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/lib")

if (AERON_BUILD_SAMPLES)
    set(AERON_SAMPLES_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-samples/src/main/cpp")
    set(AERON_C_SAMPLES_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-samples/src/main/c")
endif ()

# client source
set(AERON_CLIENT_SOURCE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-client/src/main/cpp")
set(AERON_CLIENT_WRAPPER_SOURCE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-client/src/main/cpp_wrapper")
set(AERON_C_CLIENT_SOURCE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-client/src/main/c")

# driver source
set(AERON_DRIVER_SOURCE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-driver/src/main/c")

# archive source
set(AERON_ARCHIVE_SOURCE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-archive/src/main/cpp")
set(AERON_ARCHIVE_WRAPPER_SOURCE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-archive/src/main/cpp_wrapper")
set(AERON_C_ARCHIVE_SOURCE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-archive/src/main/c")

if (AERON_TESTS)
    # client tests
    set(AERON_CLIENT_TEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-client/src/test/cpp")
    set(AERON_CLIENT_WRAPPER_TEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-client/src/test/cpp_wrapper")
    set(AERON_C_CLIENT_TEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-client/src/test/c")

    # driver tests
    set(AERON_DRIVER_TEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-driver/src/test/c")

    # archive tests
    set(AERON_ARCHIVE_TEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-archive/src/test/cpp")
    set(AERON_ARCHIVE_WRAPPER_TEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-archive/src/test/cpp_wrapper")
    set(AERON_C_ARCHIVE_TEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-archive/src/test/c")

    set(AERON_SYSTEM_TEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-system-tests")
endif ()

if (AERON_BUILD_SAMPLES)
    # hdr_histogram
    include_directories(${HDRHISTOGRAM_SOURCE_DIR}/include)
endif ()

##########################################################

add_definitions(-D_FILE_OFFSET_BITS=64)

##########################################################
# gmock usage

if (AERON_TESTS)
    set(INSTALL_GMOCK OFF CACHE INTERNAL "Install Googletest's GMock?")
    set(INSTALL_GTEST OFF CACHE INTERNAL "Install Googletest's GTest?" )

    FetchContent_Declare(
        gmock
        URL ${CMAKE_CURRENT_SOURCE_DIR}/cppbuild/googletest-1.14.0.zip
        URL_MD5 b4911e882c51cba34bebfb5df500a650)

    FetchContent_MakeAvailable(gmock)
endif ()

##########################################################
# HdrHistogram usage - use MD5 as means to identify snapshot

if (AERON_BUILD_SAMPLES)
    set(HDR_LOG_REQUIRED "OFF" CACHE INTERNAL "Set log required option")
    set(HDR_HISTOGRAM_BUILD_PROGRAMS "OFF" CACHE INTERNAL "Set build programs option")
    FetchContent_Declare(
        hdr_histogram
        URL ${CMAKE_CURRENT_SOURCE_DIR}/cppbuild/HdrHistogram_c-0.11.8.zip
        URL_MD5 e993a4a08ad084ea0a895e7b2502de99)

    FetchContent_MakeAvailable(hdr_histogram)
endif ()

add_subdirectory(${AERON_CLIENT_SOURCE_PATH})
add_subdirectory(${AERON_CLIENT_WRAPPER_SOURCE_PATH})
add_subdirectory(${AERON_C_CLIENT_SOURCE_PATH})

if (AERON_TESTS)
    add_subdirectory(${AERON_CLIENT_TEST_PATH})
    add_subdirectory(${AERON_CLIENT_WRAPPER_TEST_PATH})
    add_subdirectory(${AERON_C_CLIENT_TEST_PATH})
endif ()
if (AERON_BUILD_SAMPLES)
    add_subdirectory(${AERON_SAMPLES_PATH})
    add_subdirectory(${AERON_C_SAMPLES_PATH})
endif ()

if (BUILD_AERON_DRIVER)
    add_subdirectory(${AERON_DRIVER_SOURCE_PATH})
    if (AERON_TESTS)
        add_subdirectory(${AERON_DRIVER_TEST_PATH})
        add_subdirectory(${AERON_SYSTEM_TEST_PATH})
    endif ()
endif (BUILD_AERON_DRIVER)

if (BUILD_AERON_ARCHIVE_API)
    set(ARCHIVE_CODEC_TARGET_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
    set(ARCHIVE_C_CODEC_TARGET_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated/c")
    set(ARCHIVE_CODEC_SCHEMA_DIR "${CMAKE_CURRENT_SOURCE_DIR}/aeron-archive/src/main/resources/archive")
    set(ARCHIVE_CODEC_WORKING_DIR "${CMAKE_CURRENT_SOURCE_DIR}")

    add_subdirectory(${AERON_ARCHIVE_SOURCE_PATH})
    add_subdirectory(${AERON_ARCHIVE_WRAPPER_SOURCE_PATH})
    add_subdirectory(${AERON_C_ARCHIVE_SOURCE_PATH})
    set(AERON_ALL_JAR "${CMAKE_CURRENT_SOURCE_DIR}/aeron-all/build/libs/aeron-all-${AERON_VERSION_TXT}.jar")
    set(AERON_AGENT_JAR "${CMAKE_CURRENT_SOURCE_DIR}/aeron-agent/build/libs/aeron-agent-${AERON_VERSION_TXT}.jar")

    file(GLOB_RECURSE AERON_ALL_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}}/aeron-archive/src/main/java/*.java")

    add_custom_command(OUTPUT ${AERON_ALL_JAR}
        COMMAND ${CMAKE_COMMAND} -E env JAVA_HOME=$ENV{JAVA_HOME} BUILD_JAVA_HOME=$ENV{BUILD_JAVA_HOME} BUILD_JAVA_VERSION=$ENV{BUILD_JAVA_VERSION} ${GRADLE_WRAPPER} :aeron-all:clean :aeron-all:assemble --no-daemon -x javadoc --console=plain -q
        DEPENDS ${AERON_ALL_SOURCES}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Generating aeron-all jar")

    add_custom_target(aeron-all-jar
        DEPENDS ${AERON_ALL_JAR})

    if (AERON_TESTS)
        add_subdirectory(${AERON_ARCHIVE_TEST_PATH})
        add_subdirectory(${AERON_ARCHIVE_WRAPPER_TEST_PATH})
        add_subdirectory(${AERON_C_ARCHIVE_TEST_PATH})
    endif ()
endif (BUILD_AERON_ARCHIVE_API)
##########################################################
# doc target

if (AERON_BUILD_DOCUMENTATION AND DOXYGEN_FOUND)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/cppbuild/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)

    add_custom_target(
        doc
        ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen" VERBATIM)

    if (AERON_INSTALL_TARGETS)
        # install the doc if it has been built
        install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/doc DESTINATION share OPTIONAL)
    endif ()
endif ()

##########################################################
# export targets

if (AERON_INSTALL_TARGETS)
    include(CMakePackageConfigHelpers)

    install(
        EXPORT aeron-targets
        FILE aeron-targets.cmake
        NAMESPACE aeron::
        DESTINATION lib/cmake/aeron)

    write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/aeron-config-version.cmake"
        VERSION "${AERON_VERSION_FROM_FILE}"
        COMPATIBILITY AnyNewerVersion)

    configure_package_config_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/aeron-config.cmake.in
        "${CMAKE_CURRENT_BINARY_DIR}/aeron-config.cmake"
        INSTALL_DESTINATION lib/cmake/aeron)

    install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/aeron-config.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/aeron-config-version.cmake"
        DESTINATION lib/cmake/aeron)

    export(EXPORT aeron-targets
        FILE "${CMAKE_CURRENT_BINARY_DIR}/aeron-targets.cmake"
        NAMESPACE aeron::)
endif ()

##########################################################
# package target

set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
set(CPACK_PACKAGE_VERSION_MAJOR "${aeron_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${aeron_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${aeron_VERSION_PATCH}")

set(CPACK_GENERATOR "TGZ;STGZ")
include(CPack)

