/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron;

import org.agrona.SystemUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static io.aeron.PropertiesUtil.getBoolean;
import static io.aeron.PropertiesUtil.getDurationInNanos;
import static io.aeron.PropertiesUtil.getInteger;
import static io.aeron.PropertiesUtil.getLong;
import static io.aeron.PropertiesUtil.getProperty;
import static io.aeron.PropertiesUtil.getSizeAsInt;
import static io.aeron.PropertiesUtil.getSizeAsLong;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PropertiesUtilTest
{
    private static final String NAME = "io.aeron.test.properties.util";

    private final Properties properties = new Properties();

    @AfterEach
    void tearDown()
    {
        System.clearProperty(NAME);
    }

    @Test
    void getPropertyReturnsNullWhenUnset()
    {
        assertNull(getProperty(properties, NAME));
        assertEquals("fallback", getProperty(properties, NAME, "fallback"));
    }

    @Test
    void getPropertyTreatsNullSentinelAsNull()
    {
        properties.setProperty(NAME, SystemUtil.NULL_PROPERTY_VALUE);

        assertNull(getProperty(properties, NAME));
        assertNull(getProperty(properties, NAME, "fallback"));
    }

    @Test
    void getBooleanMatchesBooleanGetBoolean()
    {
        assertFalse(getBoolean(properties, NAME));

        properties.setProperty(NAME, "TrUe");
        assertTrue(getBoolean(properties, NAME));

        properties.setProperty(NAME, "yes");
        assertFalse(getBoolean(properties, NAME));
    }

    @Test
    void getIntegerDecodesAndFallsBackLikeIntegerGetInteger()
    {
        assertEquals(42, getInteger(properties, NAME, 42));

        properties.setProperty(NAME, "0x10");
        assertEquals(16, getInteger(properties, NAME, 42));

        properties.setProperty(NAME, "not a number");
        assertEquals(42, getInteger(properties, NAME, 42));

        System.setProperty(NAME, "7");
        assertEquals(Integer.getInteger(NAME, 42), getInteger(System.getProperties(), NAME, 42));
    }

    @Test
    void getLongDecodesAndFallsBackLikeLongGetLong()
    {
        assertEquals(42L, getLong(properties, NAME, 42L));
        assertNull(getLong(properties, NAME, (Long)null));

        properties.setProperty(NAME, "0x10");
        assertEquals(16L, getLong(properties, NAME, 42L));

        properties.setProperty(NAME, "not a number");
        assertEquals(42L, getLong(properties, NAME, 42L));

        System.setProperty(NAME, "7");
        assertEquals(Long.getLong(NAME, 42L), getLong(System.getProperties(), NAME, 42L));
    }

    @Test
    void getSizeMatchesSystemUtil()
    {
        assertEquals(64, getSizeAsInt(properties, NAME, 64));
        assertEquals(64L, getSizeAsLong(properties, NAME, 64L));

        properties.setProperty(NAME, "1m");
        System.setProperty(NAME, "1m");

        assertEquals(SystemUtil.getSizeAsInt(NAME, 64), getSizeAsInt(properties, NAME, 64));
        assertEquals(SystemUtil.getSizeAsLong(NAME, 64L), getSizeAsLong(properties, NAME, 64L));
    }

    @Test
    void getSizeAsIntRejectsValuesLargerThanIntegerMaxValue()
    {
        properties.setProperty(NAME, "4g");

        assertThrows(NumberFormatException.class, () -> getSizeAsInt(properties, NAME, 64));
    }

    @Test
    void getDurationInNanosMatchesSystemUtil()
    {
        assertEquals(64L, getDurationInNanos(properties, NAME, 64L));

        properties.setProperty(NAME, "500ms");
        System.setProperty(NAME, "500ms");

        assertEquals(SystemUtil.getDurationInNanos(NAME, 64L), getDurationInNanos(properties, NAME, 64L));
    }

    @Test
    void suppliedPropertiesAreIndependentOfSystemProperties()
    {
        System.setProperty(NAME, "1000");
        properties.setProperty(NAME, "2000");

        assertEquals(2000, getInteger(properties, NAME, 0));
        assertEquals(1000, getInteger(System.getProperties(), NAME, 0));
        assertEquals(0, getInteger(new Properties(), NAME, 0));
    }
}
