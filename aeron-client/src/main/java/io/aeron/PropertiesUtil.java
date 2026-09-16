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

import java.util.Properties;

/**
 * Utilities for reading typed values from supplied {@link Properties} instance.
 *
 * @see SystemUtil
 */
public final class PropertiesUtil
{
    private PropertiesUtil()
    {
    }

    /**
     * Get the value of a property with the exception that if the value is {@link SystemUtil#NULL_PROPERTY_VALUE} then
     * return {@code null}.
     *
     * @param properties   to look up the value in.
     * @param propertyName to get the value for.
     * @return the value of the property with the exception that if the value is
     * {@link SystemUtil#NULL_PROPERTY_VALUE} then return {@code null}.
     * @see SystemUtil#getProperty(String)
     */
    public static String getProperty(final Properties properties, final String propertyName)
    {
        final String propertyValue = properties.getProperty(propertyName);

        return SystemUtil.NULL_PROPERTY_VALUE.equals(propertyValue) ? null : propertyValue;
    }

    /**
     * Get the value of a property with the exception that if the value is {@link SystemUtil#NULL_PROPERTY_VALUE} then
     * return {@code null}, otherwise if the value is not set then return the default value.
     *
     * @param properties   to look up the value in.
     * @param propertyName to get the value for.
     * @param defaultValue to use if the property is not set.
     * @return the value of the property with the exception that if the value is
     * {@link SystemUtil#NULL_PROPERTY_VALUE} then return {@code null}, otherwise if the value is not set then return
     * the default value.
     * @see SystemUtil#getProperty(String, String)
     */
    public static String getProperty(
        final Properties properties, final String propertyName, final String defaultValue)
    {
        final String propertyValue = properties.getProperty(propertyName);
        if (SystemUtil.NULL_PROPERTY_VALUE.equals(propertyValue))
        {
            return null;
        }

        return null == propertyValue ? defaultValue : propertyValue;
    }

    /**
     * Get a boolean value from a property. The result is {@code true} if and only if the value is equal, ignoring
     * case, to the string {@code "true"}.
     *
     * @param properties   to look up the value in.
     * @param propertyName to get the value for.
     * @return the boolean value.
     * @see Boolean#getBoolean(String)
     */
    public static boolean getBoolean(final Properties properties, final String propertyName)
    {
        return Boolean.parseBoolean(properties.getProperty(propertyName));
    }

    /**
     * Get an int value from a property. The value is decoded as per {@link Integer#decode(String)} so hexadecimal,
     * octal, and decimal representations are all supported. The default value is used if the property is not present
     * or cannot be decoded.
     *
     * @param properties   to look up the value in.
     * @param propertyName to get the value for.
     * @param defaultValue to be used if the property is not present or cannot be decoded.
     * @return the int value.
     * @see Integer#getInteger(String, int)
     */
    public static int getInteger(final Properties properties, final String propertyName, final int defaultValue)
    {
        final String propertyValue = properties.getProperty(propertyName);
        if (null != propertyValue)
        {
            try
            {
                return Integer.decode(propertyValue);
            }
            catch (final NumberFormatException ignore)
            {
                // fall through to the default value, as per Integer.getInteger(String, int)
            }
        }

        return defaultValue;
    }

    /**
     * Get a long value from a property. The value is decoded as per {@link Long#decode(String)} so hexadecimal,
     * octal, and decimal representations are all supported. The default value is used if the property is not present
     * or cannot be decoded.
     *
     * @param properties   to look up the value in.
     * @param propertyName to get the value for.
     * @param defaultValue to be used if the property is not present or cannot be decoded.
     * @return the long value.
     * @see Long#getLong(String, long)
     */
    public static long getLong(final Properties properties, final String propertyName, final long defaultValue)
    {
        final String propertyValue = properties.getProperty(propertyName);
        if (null != propertyValue)
        {
            try
            {
                return Long.decode(propertyValue);
            }
            catch (final NumberFormatException ignore)
            {
                // fall through to the default value, as per Long.getLong(String, long)
            }
        }

        return defaultValue;
    }

    /**
     * Get a long value from a property, allowing a {@code null} default value to represent an unset property. The
     * value is decoded as per {@link Long#decode(String)} so hexadecimal, octal, and decimal representations are all
     * supported. The default value is used if the property is not present or cannot be decoded.
     *
     * @param properties   to look up the value in.
     * @param propertyName to get the value for.
     * @param defaultValue to be used if the property is not present or cannot be decoded, may be {@code null}.
     * @return the {@link Long} value which may be {@code null}.
     * @see Long#getLong(String, Long)
     */
    public static Long getLong(final Properties properties, final String propertyName, final Long defaultValue)
    {
        final String propertyValue = properties.getProperty(propertyName);
        if (null != propertyValue)
        {
            try
            {
                return Long.decode(propertyValue);
            }
            catch (final NumberFormatException ignore)
            {
                // fall through to the default value, as per Long.getLong(String, Long)
            }
        }

        return defaultValue;
    }

    /**
     * Get a size value as an int from a property. Supports a 'g', 'm', and 'k' suffix to indicate gigabytes,
     * megabytes, or kilobytes respectively.
     *
     * @param properties   to look up the value in.
     * @param propertyName to look up.
     * @param defaultValue to be applied if the property is not set.
     * @return the int value.
     * @throws NumberFormatException if the value is out of range or mal-formatted.
     * @see SystemUtil#getSizeAsInt(String, int)
     */
    public static int getSizeAsInt(final Properties properties, final String propertyName, final int defaultValue)
    {
        final String propertyValue = properties.getProperty(propertyName);
        if (null != propertyValue)
        {
            final long value = SystemUtil.parseSize(propertyName, propertyValue);
            if (value < 0 || value > Integer.MAX_VALUE)
            {
                throw new NumberFormatException(
                    propertyName + " must positive and less than Integer.MAX_VALUE: " + value);
            }

            return (int)value;
        }

        return defaultValue;
    }

    /**
     * Get a size value as a long from a property. Supports a 'g', 'm', and 'k' suffix to indicate gigabytes,
     * megabytes, or kilobytes respectively.
     *
     * @param properties   to look up the value in.
     * @param propertyName to look up.
     * @param defaultValue to be applied if the property is not set.
     * @return the long value.
     * @throws NumberFormatException if the value is out of range or mal-formatted.
     * @see SystemUtil#getSizeAsLong(String, long)
     */
    public static long getSizeAsLong(final Properties properties, final String propertyName, final long defaultValue)
    {
        final String propertyValue = properties.getProperty(propertyName);
        if (null != propertyValue)
        {
            return SystemUtil.parseSize(propertyName, propertyValue);
        }

        return defaultValue;
    }

    /**
     * Get a time duration in nanoseconds from a property with an optional suffix of 's', 'ms', 'us', or 'ns' to
     * indicate seconds, milliseconds, microseconds, or nanoseconds respectively.
     * <p>
     * If the resulting duration is greater than {@link Long#MAX_VALUE} then {@link Long#MAX_VALUE} is used.
     *
     * @param properties   to look up the value in.
     * @param propertyName associated with the duration value.
     * @param defaultValue to be used if the property is not present.
     * @return the long value.
     * @throws NumberFormatException if the value is negative or malformed.
     * @see SystemUtil#getDurationInNanos(String, long)
     */
    public static long getDurationInNanos(
        final Properties properties, final String propertyName, final long defaultValue)
    {
        final String propertyValue = properties.getProperty(propertyName);
        if (null != propertyValue)
        {
            return SystemUtil.parseDuration(propertyName, propertyValue);
        }

        return defaultValue;
    }
}
