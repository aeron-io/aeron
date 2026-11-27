/*
 * Copyright 2014-2026 Real Logic Limited.
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
import java.util.function.Supplier;

public final class ContextProperties
{
    private static final ThreadLocal<Properties> BOUND_PROPERTIES = new ThreadLocal<>();

    private ContextProperties()
    {
    }

    /**
     * Create a context which takes its default values from the supplied properties rather than the system properties.
     *
     * @param properties      to configure the context from, or {@code null} to use the system properties.
     * @param contextSupplier to create the context, e.g. {@code MediaDriver.Context::new}.
     * @param <T>             type of the context.
     * @return the new context.
     */
    public static <T> T newContext(final Properties properties, final Supplier<T> contextSupplier)
    {
        final Properties previous = bind(properties);
        try
        {
            return contextSupplier.get();
        }
        finally
        {
            bind(previous);
        }
    }

    /**
     * Run an action, e.g. concluding a context, with configuration read from the supplied properties on the current
     * thread.
     *
     * @param properties to read the configuration from, or {@code null} to use the system properties.
     * @param action     to run.
     */
    public static void run(final Properties properties, final Runnable action)
    {
        final Properties previous = bind(properties);
        try
        {
            action.run();
        }
        finally
        {
            bind(previous);
        }
    }

    /**
     * The properties configuration is currently read from on this thread, which a context keeps when it is created so
     * that it can use them again when it is concluded.
     *
     * @return the properties configuration is read from, or {@code null} if it is read from the system properties.
     */
    public static Properties current()
    {
        return BOUND_PROPERTIES.get();
    }

    /**
     * Get a property, as per {@link System#getProperty(String)}.
     *
     * @param propertyName to look up.
     * @return the value of the property, or {@code null} if it is not set.
     */
    public static String getProperty(final String propertyName)
    {
        final Properties properties = BOUND_PROPERTIES.get();

        return null == properties ? System.getProperty(propertyName) : properties.getProperty(propertyName);
    }

    /**
     * Get a property, as per {@link System#getProperty(String, String)}.
     *
     * @param propertyName to look up.
     * @param defaultValue to use if the property is not set.
     * @return the value of the property, or the default value if it is not set.
     */
    public static String getProperty(final String propertyName, final String defaultValue)
    {
        final String propertyValue = getProperty(propertyName);

        return null == propertyValue ? defaultValue : propertyValue;
    }

    /**
     * Get a property, as per {@link SystemUtil#getProperty(String, String)}, i.e. a value of
     * {@link SystemUtil#NULL_PROPERTY_VALUE} is returned as {@code null}.
     *
     * @param propertyName to look up.
     * @param defaultValue to use if the property is not set.
     * @return the value of the property, {@code null} if set to {@link SystemUtil#NULL_PROPERTY_VALUE}, or the
     * default value if it is not set.
     */
    public static String getNullableProperty(final String propertyName, final String defaultValue)
    {
        final String propertyValue = getProperty(propertyName);
        if (SystemUtil.NULL_PROPERTY_VALUE.equals(propertyValue))
        {
            return null;
        }

        return null == propertyValue ? defaultValue : propertyValue;
    }

    /**
     * Get a boolean property, as per {@link Boolean#getBoolean(String)}.
     *
     * @param propertyName to look up.
     * @return {@code true} if the property is set to {@code "true"} ignoring case, otherwise {@code false}.
     */
    public static boolean getBoolean(final String propertyName)
    {
        return Boolean.parseBoolean(getProperty(propertyName));
    }

    /**
     * Get an integer property, as per {@link Integer#getInteger(String, int)}.
     *
     * @param propertyName to look up.
     * @param defaultValue to use if the property is not set or is not a valid integer.
     * @return the value of the property, or the default value.
     */
    public static int getInteger(final String propertyName, final int defaultValue)
    {
        final String propertyValue = getProperty(propertyName);
        if (null != propertyValue)
        {
            try
            {
                return Integer.decode(propertyValue);
            }
            catch (final NumberFormatException ignore)
            {
                // fall back to the default value, as per Integer.getInteger
            }
        }

        return defaultValue;
    }

    /**
     * Get a long property, as per {@link Long#getLong(String, Long)}.
     *
     * @param propertyName to look up.
     * @param defaultValue to use if the property is not set or is not a valid long, which may be {@code null}.
     * @return the value of the property, or the default value.
     */
    public static Long getLong(final String propertyName, final Long defaultValue)
    {
        final String propertyValue = getProperty(propertyName);
        if (null != propertyValue)
        {
            try
            {
                return Long.decode(propertyValue);
            }
            catch (final NumberFormatException ignore)
            {
                // fall back to the default value, as per Long.getLong
            }
        }

        return defaultValue;
    }

    /**
     * Get a size property, as per {@link SystemUtil#getSizeAsInt(String, int)}.
     *
     * @param propertyName to look up.
     * @param defaultValue to use if the property is not set.
     * @return the value of the property, or the default value if it is not set.
     * @throws NumberFormatException if the value is not a valid size or is out of range for an int.
     */
    public static int getSizeAsInt(final String propertyName, final int defaultValue)
    {
        final String propertyValue = getProperty(propertyName);
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
     * Get a size property, as per {@link SystemUtil#getSizeAsLong(String, long)}.
     *
     * @param propertyName to look up.
     * @param defaultValue to use if the property is not set.
     * @return the value of the property, or the default value if it is not set.
     * @throws NumberFormatException if the value is not a valid size.
     */
    public static long getSizeAsLong(final String propertyName, final long defaultValue)
    {
        final String propertyValue = getProperty(propertyName);

        return null != propertyValue ? SystemUtil.parseSize(propertyName, propertyValue) : defaultValue;
    }

    /**
     * Get a duration property in nanoseconds, as per {@link SystemUtil#getDurationInNanos(String, long)}.
     *
     * @param propertyName to look up.
     * @param defaultValue to use if the property is not set.
     * @return the value of the property in nanoseconds, or the default value if it is not set.
     * @throws NumberFormatException if the value is not a valid duration.
     */
    public static long getDurationInNanos(final String propertyName, final long defaultValue)
    {
        final String propertyValue = getProperty(propertyName);

        return null != propertyValue ? SystemUtil.parseDuration(propertyName, propertyValue) : defaultValue;
    }

    private static Properties bind(final Properties properties)
    {
        final Properties previous = BOUND_PROPERTIES.get();
        if (null == properties)
        {
            BOUND_PROPERTIES.remove();
        }
        else
        {
            BOUND_PROPERTIES.set(properties);
        }

        return previous;
    }
}
