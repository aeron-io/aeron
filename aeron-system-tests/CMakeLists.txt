if (AERON_SYSTEM_TESTS)
    add_test(
        NAME java_system_tests_c_media_driver
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMAND ${CMAKE_COMMAND}
                -E env
                JAVA_HOME=${JAVA_HOME}
                BUILD_JAVA_HOME=${BUILD_JAVA_HOME}
                BUILD_JAVA_VERSION=${BUILD_JAVA_VERSION}
                ${GRADLE_WRAPPER}
                -Daeron.test.system.aeronmd.path=$<TARGET_FILE:aeronmd>
                :aeron-system-tests:cleanTest
                :aeron-system-tests:test
                --no-daemon
                --console=plain)
    set_tests_properties(java_system_tests_c_media_driver PROPERTIES RUN_SERIAL TRUE)
endif ()

if (AERON_SLOW_SYSTEM_TESTS)
    add_test(
        NAME java_slow_system_tests_c_media_driver
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMAND ${CMAKE_COMMAND}
                -E env
                JAVA_HOME=${JAVA_HOME}
                BUILD_JAVA_HOME=${BUILD_JAVA_HOME}
                BUILD_JAVA_VERSION=${BUILD_JAVA_VERSION}
                ${GRADLE_WRAPPER}
                -Daeron.test.system.aeronmd.path=$<TARGET_FILE:aeronmd>
                :aeron-system-tests:cleanSlowTest
                :aeron-system-tests:slowTest
                --no-daemon
                --console=plain)
    set_tests_properties(java_slow_system_tests_c_media_driver PROPERTIES TIMEOUT 3600)
    set_tests_properties(java_slow_system_tests_c_media_driver PROPERTIES RUN_SERIAL TRUE)
endif ()
