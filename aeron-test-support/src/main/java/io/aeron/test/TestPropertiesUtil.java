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

package io.aeron.test;

import java.util.Properties;

/**
 * Utility operations for handling system properties.
 */
public final class TestPropertiesUtil
{
    private TestPropertiesUtil()
    {
    }

    /**
     * Placeholder for a null value.
     */
    public static final String NULL_SENTINEL = "@null";

    /**
     * Back up the system properties in the supplied newProperties and immediately override
     * the currently loaded ones.
     *
     * @param backup        to store the backed up properties to.
     * @param newProperties to define which properties to back up and override.
     * @return the backup properties for a fluent API.
     */
    public static Properties backupAndOverrideSystemProperties(final Properties backup, final Properties newProperties)
    {
        backupSystemProperties(backup, newProperties);
        System.getProperties().putAll(newProperties);
        return backup;
    }

    /**
     * Back up the system properties specified in the supplied newProperties collection to the backup properties
     * collection. Will track <code>null</code> values with the {@link #NULL_SENTINEL}.
     *
     * @param backup        To store the backed up properties to.
     * @param newProperties To define which properties to back up.
     * @return              the backup properties for a fluent API.
     */
    public static Properties backupSystemProperties(final Properties backup, final Properties newProperties)
    {
        for (final String key : newProperties.stringPropertyNames())
        {
            final String value = System.getProperty(key);
            if (!backup.containsKey(key))
            {
                if (null == value)
                {
                    backup.setProperty(key, NULL_SENTINEL);
                }
                else
                {
                    backup.setProperty(key, System.getProperty(key));
                }
            }
        }

        return backup;
    }

    /**
     * Restore the supplied properties back to the system properties.  Will track <code>null</code> values with the
     * {@link #NULL_SENTINEL}.
     *
     * @param backup to restore to system properties.
     */
    public static void restoreSystemProperties(final Properties backup)
    {
        for (final String key : backup.stringPropertyNames())
        {
            final String value = backup.getProperty(key);
            if (NULL_SENTINEL.equals(value))
            {
                System.clearProperty(key);
            }
            else
            {
                System.setProperty(key, value);
            }
        }
    }
}
