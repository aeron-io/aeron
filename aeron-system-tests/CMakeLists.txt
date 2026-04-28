find_package(Java 17 REQUIRED)

if (NOT DEFINED JAVA_HOME)
    set(CACHE{JAVA_HOME} TYPE PATH FORCE VALUE $ENV{JAVA_HOME})
endif ()

if (NOT DEFINED BUILD_JAVA_HOME)
    set(CACHE{BUILD_JAVA_HOME} TYPE PATH FORCE VALUE $ENV{BUILD_JAVA_HOME})
endif ()

if (NOT DEFINED BUILD_JAVA_VERSION)
    set(CACHE{BUILD_JAVA_VERSION} TYPE STRING FORCE VALUE $ENV{BUILD_JAVA_VERSION})
endif ()

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
    if (SANITISE_BUILD)
        # Sanitizer-instrumented runs are several times slower; double the budget.
        set_tests_properties(java_system_tests_c_media_driver PROPERTIES TIMEOUT 3000)
    else ()
        set_tests_properties(java_system_tests_c_media_driver PROPERTIES TIMEOUT 1500)
    endif ()
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
    if (SANITISE_BUILD)
        # Sanitizer-instrumented runs are several times slower; double the budget.
        set_tests_properties(java_slow_system_tests_c_media_driver PROPERTIES TIMEOUT 7200)
    else ()
        set_tests_properties(java_slow_system_tests_c_media_driver PROPERTIES TIMEOUT 3600)
    endif ()
    set_tests_properties(java_slow_system_tests_c_media_driver PROPERTIES RUN_SERIAL TRUE)
endif ()
