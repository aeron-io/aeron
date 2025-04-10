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

if (MSVC AND "${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
    set(AERON_LIB_WINSOCK_LIBS wsock32 ws2_32 Iphlpapi)
endif ()

include_directories(${AERON_DRIVER_SOURCE_PATH})

set(TEST_HEADERS
    aeron_driver_conductor_test.h
    aeron_receiver_test.h
    EmbeddedMediaDriver.h
    aeron_test_udp_bindings.h
    aeron_test_base.h)

set(CMAKE_EXTRA_INCLUDE_FILES sys/socket.h)
check_type_size("struct mmsghdr" STRUCT_MMSGHDR_TYPE_EXISTS)
set(CMAKE_EXTRA_INCLUDE_FILES)

function(aeron_driver_test name file)
    add_executable(${name} ${file} ${TEST_HEADERS})
    add_dependencies(${name} gmock)
    target_link_libraries(${name} aeron_driver gmock_main ${CMAKE_THREAD_LIBS_INIT} ${AERON_LIB_WINSOCK_LIBS})
    target_compile_definitions(${name} PUBLIC "_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING")
    if (STRUCT_MMSGHDR_TYPE_EXISTS)
        target_compile_definitions(${name} PUBLIC -DHAVE_STRUCT_MMSGHDR)
    endif ()
    add_test(NAME ${name} COMMAND ${name})
endfunction()

if (AERON_UNIT_TESTS)
    aeron_driver_test(counters_manager_test aeron_counters_manager_test.cpp)
    aeron_driver_test(driver_conductor_ipc_test aeron_driver_conductor_ipc_test.cpp)
    aeron_driver_test(driver_conductor_network_test aeron_driver_conductor_network_test.cpp)
    aeron_driver_test(driver_conductor_pub_sub_test aeron_driver_conductor_pub_sub_test.cpp)
    aeron_driver_test(driver_conductor_spy_test aeron_driver_conductor_spy_test.cpp)
    aeron_driver_test(driver_conductor_counter_test aeron_driver_conductor_counter_test.cpp)
    aeron_driver_test(driver_conductor_clock_test aeron_driver_conductor_clock_test.cpp)
    aeron_driver_test(driver_conductor_config_test aeron_driver_conductor_config_test.cpp)
    aeron_driver_test(driver_conductor_subscribable_test aeron_driver_conductor_subscribable_test.cpp)
    aeron_driver_test(driver_uri_test aeron_driver_uri_test.cpp)
    aeron_driver_test(udp_channel_test aeron_udp_channel_test.cpp)
    aeron_driver_test(term_scanner_test aeron_term_scanner_test.cpp)
    aeron_driver_test(loss_detector_test aeron_loss_detector_test.cpp)
    aeron_driver_test(retransmit_handler_test aeron_retransmit_handler_test.cpp)
    aeron_driver_test(loss_reporter_test aeron_loss_reporter_test.cpp)
    aeron_driver_test(logbuffer_unblocker aeron_logbuffer_unblocker_test.cpp)
    aeron_driver_test(term_gap_filler_test aeron_term_gap_filler_test.cpp)
    aeron_driver_test(parse_util_test aeron_parse_util_test.cpp)
    aeron_driver_test(properties_test aeron_properties_test.cpp)
    aeron_driver_test(driver_configuration_test aeron_driver_configuration_test.cpp)
    aeron_driver_test(driver_agent_test agent/aeron_driver_agent_test.cpp)
    aeron_driver_test(udp_channel_transport_loss_test media/aeron_udp_channel_transport_loss_test.cpp)
    aeron_driver_test(udp_channel_transport_multi_gap_loss_test media/aeron_udp_channel_transport_multi_gap_loss_test.cpp)
    aeron_driver_test(flow_control_test aeron_flow_control_test.cpp)
    aeron_driver_test(name_resolver_test aeron_name_resolver_test.cpp)
    aeron_driver_test(name_resolver_cache_test aeron_name_resolver_cache_test.cpp)
    aeron_driver_test(data_packet_dispatcher_test aeron_data_packet_dispatcher_test.cpp)
    aeron_driver_test(publication_image_test aeron_publication_image_test.cpp)
    aeron_driver_test(port_manager_test aeron_port_manager_test.cpp)
    aeron_driver_test(term_cleaner_test aeron_term_cleaner_test.cpp)
    set_tests_properties(publication_image_test PROPERTIES RUN_SERIAL TRUE)

    aeron_driver_test(c_terminate_test aeron_c_terminate_test.cpp)
    set_tests_properties(c_terminate_test PROPERTIES TIMEOUT 60)
    set_tests_properties(c_terminate_test PROPERTIES RUN_SERIAL TRUE)

    aeron_driver_test(c_system_test aeron_c_system_test.cpp)
    set_tests_properties(c_system_test PROPERTIES TIMEOUT 120)
    set_tests_properties(c_system_test PROPERTIES RUN_SERIAL TRUE)

    aeron_driver_test(c_multi_destination_test aeron_c_multi_destination_test.cpp)
    set_tests_properties(c_multi_destination_test PROPERTIES TIMEOUT 60)
    set_tests_properties(c_multi_destination_test PROPERTIES RUN_SERIAL TRUE)

    aeron_driver_test(c_local_addresses_test aeron_c_local_addresses_test.cpp)
    set_tests_properties(c_local_addresses_test PROPERTIES TIMEOUT 60)
    set_tests_properties(c_local_addresses_test PROPERTIES RUN_SERIAL TRUE)

    aeron_driver_test(c_errors_test aeron_errors_test.cpp)
    set_tests_properties(c_errors_test PROPERTIES TIMEOUT 60)
    set_tests_properties(c_errors_test PROPERTIES RUN_SERIAL TRUE)

    aeron_driver_test(c_cnc_test aeron_c_cnc_test.cpp)
    set_tests_properties(c_cnc_test PROPERTIES TIMEOUT 60)
    set_tests_properties(c_cnc_test PROPERTIES RUN_SERIAL TRUE)

    aeron_driver_test(congestion_control_test aeron_congestion_control_test.cpp)

    aeron_driver_test(ipc_publication_test aeron_ipc_publication_test.cpp)
    aeron_driver_test(network_publication_test aeron_network_publication_test.cpp)

    aeron_driver_test(timestamps_test_recvmmsg aeron_timestamps_test.cpp)
    set_tests_properties(timestamps_test_recvmmsg PROPERTIES ENVIRONMENT "AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND=2;AERON_RECEIVER_IO_VECTOR_CAPACITY=3;AERON_SENDER_IO_VECTOR_CAPACITY=4")

    aeron_driver_test(timestamps_test_recvmsg aeron_timestamps_test.cpp)
    set_tests_properties(timestamps_test_recvmsg PROPERTIES ENVIRONMENT "AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND=1;AERON_RECEIVER_IO_VECTOR_CAPACITY=1;AERON_SENDER_IO_VECTOR_CAPACITY=1")

    aeron_driver_test(position_test aeron_position_test.cpp)
    aeron_driver_test(driver_context_config_test aeron_driver_context_config_test.cpp)

    if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
        add_executable(aeronmd_signal_test aeronmd_signal_test.cpp)
        add_dependencies(aeronmd_signal_test gmock aeronmd)
        target_compile_definitions(aeronmd_signal_test PUBLIC "_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING")
        target_link_libraries(aeronmd_signal_test aeron gmock_main ${CMAKE_THREAD_LIBS_INIT} ${AERON_LIB_WINSOCK_LIBS})
        add_test(NAME aeronmd_signal_test COMMAND aeronmd_signal_test $<TARGET_FILE:aeronmd>)
    endif ()

endif ()