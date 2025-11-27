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
    set(AERON_LIB_WINSOCK_LIBS wsock32 ws2_32 Iphlpapi)
endif ()

include_directories(${AERON_C_CLIENT_SOURCE_PATH})

set(TEST_HEADERS
    aeron_client_test_utils.h)

function(aeron_c_client_test name file)
        add_executable(${name} ${file} ${TEST_HEADERS})
        add_dependencies(${name} gmock)
        target_link_libraries(${name} aeron gmock_main ${CMAKE_THREAD_LIBS_INIT} ${AERON_LIB_WINSOCK_LIBS})
        target_compile_definitions(${name} PUBLIC "_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING")
        add_test(NAME ${name} COMMAND ${name})
endfunction()

if (AERON_UNIT_TESTS)
    aeron_c_client_test(array_to_ptr_hash_map_test collections/aeron_array_to_ptr_hash_map_test.cpp)
    aeron_c_client_test(int64_to_ptr_hash_map_test collections/aeron_int64_to_ptr_hash_map_test.cpp)
    aeron_c_client_test(int64_counter_map_test collections/aeron_int64_counter_map_test.cpp)
    aeron_c_client_test(int64_to_tagged_ptr_hash_map_test collections/aeron_int64_to_tagged_ptr_hash_map_test.cpp)
    aeron_c_client_test(str_to_ptr_hash_map_test collections/aeron_str_to_ptr_hash_map_test.cpp)
    aeron_c_client_test(bit_set_test collections/aeron_bit_set_test.cpp)
    aeron_c_client_test(atomic_test concurrent/aeron_atomic_test.cpp)
    aeron_c_client_test(aeron_thread_test concurrent/aeron_thread_test.cpp)
    aeron_c_client_test(spsc_rb_test concurrent/aeron_spsc_rb_test.cpp)
    aeron_c_client_test(mpsc_rb_test concurrent/aeron_mpsc_rb_test.cpp)
    aeron_c_client_test(broadcast_receiver_test concurrent/aeron_broadcast_receiver_test.cpp)
    aeron_c_client_test(broadcast_transmitter_test concurrent/aeron_broadcast_transmitter_test.cpp)

    aeron_c_client_test(distinct_error_log_test concurrent/aeron_distinct_error_log_test.cpp)
    set_tests_properties(distinct_error_log_test PROPERTIES TIMEOUT 30)
    set_tests_properties(distinct_error_log_test PROPERTIES RUN_SERIAL TRUE)

    aeron_c_client_test(spsc_concurrent_array_queue_test concurrent/aeron_spsc_concurrent_array_queue_test.cpp)
    aeron_c_client_test(mpsc_concurrent_array_queue_test concurrent/aeron_mpsc_concurrent_array_queue_test.cpp)
    aeron_c_client_test(linked_queue_test collections/aeron_linked_queue_test.cpp)
    aeron_c_client_test(blocking_linked_queue_test concurrent/aeron_blocking_linked_queue_test.cpp)
    aeron_c_client_test(executor_test concurrent/aeron_executor_test.cpp)
    aeron_c_client_test(counters_test concurrent/aeron_counters_test.cpp)
    aeron_c_client_test(client_conductor_test aeron_client_conductor_test.cpp)
    aeron_c_client_test(publication_test aeron_publication_test.cpp)
    aeron_c_client_test(subscription_test aeron_subscription_test.cpp)
    aeron_c_client_test(image_test aeron_image_test.cpp)
    aeron_c_client_test(fragment_assembler_test aeron_fragment_assembler_test.cpp)
    aeron_c_client_test(image_fragment_assembler_test aeron_image_fragment_assembler_test.cpp)
    aeron_c_client_test(controlled_fragment_assembler_test aeron_controlled_fragment_assembler_test.cpp)
    aeron_c_client_test(controlled_image_fragment_assembler_test aeron_controlled_image_fragment_assembler_test.cpp)
    aeron_c_client_test(aeron_fileutil_test util/aeron_fileutil_test.cpp)
    aeron_c_client_test(aeron_uri_test aeron_uri_test.cpp)
    aeron_c_client_test(version_test aeron_version_test.cpp)
    aeron_c_client_test(bitutil_test util/aeron_bitutil_test.cpp)
    aeron_c_client_test(deque_test util/aeron_deque_test.cpp)
    aeron_c_client_test(fileutil_test util/aeron_fileutil_test.cpp)
    aeron_c_client_test(math_test util/aeron_math_test.cpp)
    aeron_c_client_test(strutil_test util/aeron_strutil_test.cpp)
    aeron_c_client_test(error_test util/aeron_error_test.cpp)
    aeron_c_client_test(netutil_test util/aeron_netutil_test.cpp)
    aeron_c_client_test(symbol_table_test util/aeron_symbol_table_test.cpp)
    aeron_c_client_test(http_util_test util/aeron_httputil_test.cpp)
endif ()
