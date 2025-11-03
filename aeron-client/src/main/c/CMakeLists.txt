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

include(CheckSymbolExists)

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
    set(CMAKE_REQUIRED_DEFINITIONS "-D_GNU_SOURCE")
    add_definitions(-D_DEFAULT_SOURCE)
endif ()

if (MSVC AND "${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
    set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)
    set(BUILD_SHARED_LIBS ON)
    set(AERON_LIB_WINSOCK_LIBS wsock32 ws2_32 Iphlpapi)
    set(WSAPOLL_PROTOTYPE_EXISTS True)
endif ()

if (NOT MSVC)
    message(STATUS "Looking up: /dev/urandom")
    execute_process(
        COMMAND ls /dev/urandom
        RESULT_VARIABLE DEV_URANDOM_EXISTS
        OUTPUT_QUIET
    )
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

check_include_file("bsd/stdlib.h" BSDSTDLIB_H_EXISTS)
find_library(LIBBSD_EXISTS NAMES bsd libbsd)

if (LIBBSD_EXISTS)
    set(CMAKE_REQUIRED_LIBRARIES "${CMAKE_REQUIRED_LIBRARIES} -lbsd")
endif ()

if (NOT BSDSTDLIB_H_EXISTS)
    check_symbol_exists(arc4random "stdlib.h" ARC4RANDOM_PROTOTYPE_EXISTS)
else ()
    add_definitions(-DHAVE_BSDSTDLIB_H)
    check_symbol_exists(arc4random "bsd/stdlib.h" ARC4RANDOM_PROTOTYPE_EXISTS)
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

if (ARC4RANDOM_PROTOTYPE_EXISTS)
    add_definitions(-DHAVE_ARC4RANDOM)
else ()
    message(WARNING "Could not find arc4random. If on Linux, is libbsd installed?")
endif ()

if (DEV_URANDOM_EXISTS EQUAL 0)
    message(STATUS "Found: /dev/urandom")
    add_definitions(-DHAVE_DEV_URANDOM)
endif ()

set(SOURCE
    collections/aeron_array_to_ptr_hash_map.c
    collections/aeron_bit_set.c
    collections/aeron_hashing.c
    collections/aeron_int64_counter_map.c
    collections/aeron_int64_to_ptr_hash_map.c
    collections/aeron_int64_to_tagged_ptr_hash_map.c
    collections/aeron_linked_queue.c
    collections/aeron_map.c
    collections/aeron_str_to_ptr_hash_map.c
    concurrent/aeron_atomic.c
    concurrent/aeron_blocking_linked_queue.c
    concurrent/aeron_broadcast_receiver.c
    concurrent/aeron_broadcast_transmitter.c
    concurrent/aeron_counters_manager.c
    concurrent/aeron_distinct_error_log.c
    concurrent/aeron_executor.c
    concurrent/aeron_logbuffer_descriptor.c
    concurrent/aeron_mpsc_concurrent_array_queue.c
    concurrent/aeron_mpsc_rb.c
    concurrent/aeron_spsc_concurrent_array_queue.c
    concurrent/aeron_spsc_rb.c
    concurrent/aeron_term_gap_filler.c
    concurrent/aeron_term_gap_scanner.c
    concurrent/aeron_term_rebuilder.c
    concurrent/aeron_term_scanner.c
    concurrent/aeron_term_unblocker.c
    concurrent/aeron_thread.c
    protocol/aeron_udp_protocol.c
    reports/aeron_loss_reporter.c
    status/aeron_local_sockaddr.c
    util/aeron_arrayutil.c
    util/aeron_bitutil.c
    util/aeron_clock.c
    util/aeron_deque.c
    util/aeron_dlopen.c
    util/aeron_env.c
    util/aeron_error.c
    util/aeron_fileutil.c
    util/aeron_http_util.c
    util/aeron_math.c
    util/aeron_netutil.c
    util/aeron_parse_util.c
    util/aeron_properties_util.c
    util/aeron_strutil.c
    util/aeron_symbol_table.c
    uri/aeron_uri.c
    uri/aeron_uri_string_builder.c
    aeron_agent.c
    aeron_alloc.c
    aeron_client.c
    aeron_client_conductor.c
    aeron_cnc.c
    aeron_cnc_file_descriptor.c
    aeron_context.c
    aeron_counter.c
    aeron_exclusive_publication.c
    aeron_fragment_assembler.c
    aeron_image.c
    aeron_log_buffer.c
    aeron_publication.c
    aeron_socket.c
    aeron_subscription.c
    aeron_windows.c
    aeronc.c
    aeron_version.c)

set(HEADERS
    collections/aeron_array_to_ptr_hash_map.h
    collections/aeron_bit_set.h
    collections/aeron_hashing.h
    collections/aeron_int64_counter_map.h
    collections/aeron_int64_to_ptr_hash_map.h
    collections/aeron_int64_to_tagged_ptr_hash_map.h
    collections/aeron_linked_queue.h
    collections/aeron_map.h
    collections/aeron_str_to_ptr_hash_map.h
    command/aeron_control_protocol.h
    concurrent/aeron_atomic.h
    concurrent/aeron_atomic64_gcc_x86_64.h
    concurrent/aeron_atomic64_msvc.h
    concurrent/aeron_atomic64_c11.h
    concurrent/aeron_blocking_linked_queue.h
    concurrent/aeron_broadcast_descriptor.h
    concurrent/aeron_broadcast_receiver.h
    concurrent/aeron_broadcast_transmitter.h
    concurrent/aeron_concurrent_array_queue.h
    concurrent/aeron_counters_manager.h
    concurrent/aeron_distinct_error_log.h
    concurrent/aeron_executor.h
    concurrent/aeron_logbuffer_descriptor.h
    concurrent/aeron_mpsc_concurrent_array_queue.h
    concurrent/aeron_mpsc_rb.h
    concurrent/aeron_rb.h
    concurrent/aeron_spsc_concurrent_array_queue.h
    concurrent/aeron_spsc_rb.h
    concurrent/aeron_term_gap_filler.h
    concurrent/aeron_term_gap_scanner.h
    concurrent/aeron_term_rebuilder.h
    concurrent/aeron_term_scanner.h
    concurrent/aeron_term_unblocker.h
    concurrent/aeron_thread.h
    protocol/aeron_udp_protocol.h
    reports/aeron_loss_reporter.h
    status/aeron_local_sockaddr.h
    util/aeron_arrayutil.h
    util/aeron_bitutil.h
    util/aeron_clock.h
    util/aeron_deque.h
    util/aeron_dlopen.h
    util/aeron_env.h
    util/aeron_error.h
    util/aeron_fileutil.h
    util/aeron_http_util.h
    util/aeron_math.h
    util/aeron_netutil.h
    util/aeron_parse_util.h
    util/aeron_platform.h
    util/aeron_properties_util.h
    util/aeron_strutil.h
    util/aeron_symbol_table.h
    uri/aeron_uri.h
    uri/aeron_uri_string_builder.h
    aeron_agent.h
    aeron_alloc.h
    aeron_client.h
    aeron_client_conductor.h
    aeron_cnc_file_descriptor.h
    aeron_common.h
    aeron_context.h
    aeron_counter.h
    aeron_counters.h
    aeron_exclusive_publication.h
    aeron_fragment_assembler.h
    aeron_image.h
    aeron_log_buffer.h
    aeron_publication.h
    aeron_socket.h
    aeron_subscription.h
    aeron_windows.h
    aeronc.h)

add_library(aeron SHARED ${SOURCE} ${HEADERS})
add_library(aeron::aeron ALIAS aeron)
target_include_directories(aeron
    PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>" "$<INSTALL_INTERFACE:include/aeron>")

add_library(aeron_static STATIC ${SOURCE} ${HEADERS})
add_library(aeron::aeron_static ALIAS aeron_static)
target_include_directories(aeron_static
    PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>" "$<INSTALL_INTERFACE:include/aeron>")

set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -DDISABLE_BOUNDS_CHECKS")

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
    set(AERON_LIB_M_LIBS m)

    if (LIBBSD_EXISTS)
        set(AERON_LIB_BSD_LIBS bsd)
    endif ()

    if ("${CMAKE_SYSTEM_PROCESSOR}" MATCHES "aarch64")
        set(AERON_LIB_ATOMIC_LIBS atomic)
    endif ()
endif ()

target_link_libraries(
    aeron
    ${CMAKE_DL_LIBS}
    ${AERON_LIB_M_LIBS}
    ${AERON_LIB_BSD_LIBS}
    ${AERON_LIB_ATOMIC_LIBS}
    ${CMAKE_THREAD_LIBS_INIT}
    ${AERON_LIB_WINSOCK_LIBS})

target_link_libraries(
    aeron_static INTERFACE
    ${CMAKE_DL_LIBS}
    ${AERON_LIB_M_LIBS}
    ${AERON_LIB_BSD_LIBS}
    ${AERON_LIB_ATOMIC_LIBS}
    ${CMAKE_THREAD_LIBS_INIT}
    ${AERON_LIB_WINSOCK_LIBS}
    ${AERON_STATIC_LIB_LINK_OPTS}
    )

if (AERON_INSTALL_TARGETS)
    install(
        TARGETS aeron aeron_static
        EXPORT aeron-targets
        RUNTIME DESTINATION lib
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
    install(DIRECTORY ./ DESTINATION include/aeron FILES_MATCHING PATTERN "*.h")
endif ()
