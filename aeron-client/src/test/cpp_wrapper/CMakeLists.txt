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

set(TEST_HEADERS
    EmbeddedMediaDriver.h
    TestUtil.h)

include_directories(${AERON_CLIENT_WRAPPER_SOURCE_PATH})

function(aeron_client_native_test name file)
    add_executable(${name} ${file} ${TEST_HEADERS})
    add_dependencies(${name} gmock)
    target_link_libraries(${name} aeron_driver gmock_main ${CMAKE_THREAD_LIBS_INIT})
    target_compile_definitions(${name} PUBLIC "_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING")
    add_test(NAME ${name} COMMAND ${name})
endfunction()

function(aeron_client_wrapper_test name file)
    set(wrapper_name "${name}W")
    add_executable(${wrapper_name} ${file} ${TEST_HEADERS})
    add_dependencies(${wrapper_name} gmock)
    target_link_libraries(${wrapper_name} aeron_client_wrapper aeron_driver gmock_main ${CMAKE_THREAD_LIBS_INIT})
    target_compile_definitions(${wrapper_name} PUBLIC "_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING")
    add_test(NAME ${wrapper_name} COMMAND ${wrapper_name})
endfunction()

if (AERON_UNIT_TESTS)
    aeron_client_wrapper_test(systemTest SystemTest.cpp)
    aeron_client_wrapper_test(wrapperSystemTest WrapperSystemTest.cpp)
    aeron_client_native_test(systemTest SystemTest.cpp)

    aeron_client_wrapper_test(pubSubTest PubSubTest.cpp)
    aeron_client_native_test(pubSubTest PubSubTest.cpp)

    aeron_client_wrapper_test(countersTest CountersTest.cpp)
    aeron_client_native_test(countersTest CountersTest.cpp)

    aeron_client_wrapper_test(imagePollTest ImageTest.cpp)
    aeron_client_native_test(imagePollTest ImageTest.cpp)

    aeron_client_wrapper_test(localAddresses LocalAddressesTest.cpp)
    aeron_client_native_test(localAddresses LocalAddressesTest.cpp)

    aeron_client_wrapper_test(multiDestinationCast MultiDestinationTest.cpp)
    aeron_client_native_test(multiDestinationCast MultiDestinationTest.cpp)

    aeron_client_wrapper_test(multiDestinationByIdTest MultiDestinationByIdTest.cpp)

    aeron_client_wrapper_test(livenessTimeoutTest LivenessTimeoutTest.cpp)
    aeron_client_native_test(livenessTimeoutTest LivenessTimeoutTest.cpp)

    aeron_client_wrapper_test(responseChannelsTest ResponseChannelsTest.cpp)
    aeron_client_native_test(responseChannelsTest ResponseChannelsTest.cpp)

    aeron_client_wrapper_test(exceptionsTest ExceptionsTest.cpp)
    aeron_client_wrapper_test(channelUriStringBuilderTest ChannelUriStringBuilderTest.cpp)

    aeron_client_wrapper_test(fragmentAssemblerTest FragmentAssemblerTest.cpp)
    aeron_client_wrapper_test(imageFragmentAssemblerTest ImageFragmentAssemblerTest.cpp)
    aeron_client_wrapper_test(controlledFragmentAssemblerTest ControlledFragmentAssemblerTest.cpp)
    aeron_client_wrapper_test(imageControlledFragmentAssemblerTest ImageControlledFragmentAssemblerTest.cpp)
    aeron_client_wrapper_test(rejectImageTest RejectImageTest.cpp)
    aeron_client_wrapper_test(publicationRevokeTest PublicationRevokeTest.cpp)

endif ()