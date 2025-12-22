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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UnresolvedInterfaceTest
{
    @Test
    void shouldParseValidInterfaceSpecifications() throws Exception
    {
        assertInstanceOf(InterfaceSearchAddress.class, UnresolvedInterface.parse("localhost"));
        assertInstanceOf(InterfaceSearchAddress.class, UnresolvedInterface.parse("10.0.0.1"));
        assertInstanceOf(InterfaceSearchAddress.class, UnresolvedInterface.parse("[2001:db8::1]"));
        assertInstanceOf(NamedInterface.class, UnresolvedInterface.parse("{lo}"));
    }

    @Test
    void shouldThrowOnInvalidInterfaceSpecifications()
    {
        assertThrows(IllegalArgumentException.class, () -> UnresolvedInterface.parse(null));
        assertThrows(IllegalArgumentException.class, () -> UnresolvedInterface.parse(""));
    }
}
