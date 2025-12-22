/*
 * Copyright 2025 Adaptive Financial Consulting Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.driver.media;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class NamedInterfaceTest
{
    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {
        " ",
        "eth0",
        "eth0:0",
        "{eth0",
        "{eth0}:",
        "{eth0}0",
        "{eth0}:-1000",
        "{eth0};1000",
        "{eth0}:1000:2000",
        "{eth0}:65536",
        "{}",
    })
    void shouldThrowIfParseInputIsInvalid(final String invalid)
    {
        assertThrows(IllegalArgumentException.class, () -> NamedInterface.parse(invalid));
    }

    static Stream<Arguments> validInputs()
    {
        return Stream.of(
            arguments("{eth0}", "eth0", 0),
            arguments("{eth0}:0", "eth0", 0),
            arguments("{eth0}:1234", "eth0", 1234),
            arguments("{eth0:0}:0", "eth0:0", 0),
            arguments("{eth0:1}:2000", "eth0:1", 2000),
            arguments("{eth0.100}:80", "eth0.100", 80),
            arguments("{x}:5678", "x", 5678),
            arguments("{lo}:65535", "lo", 65535),
            arguments("{Ethernet 1}", "Ethernet 1", 0),
            arguments("{ethernet_32768}", "ethernet_32768", 0),
            arguments("{foo{bar-baz-12345}}:9999", "foo{bar-baz-12345}", 9999)
        );
    }

    @ParameterizedTest
    @MethodSource("validInputs")
    void shouldParseValidInputs(final String str, final String name, final int port)
    {
        final NamedInterface namedInterface = NamedInterface.parse(str);
        assertEquals(name, namedInterface.name());
        assertEquals(port, namedInterface.port());
    }
}
