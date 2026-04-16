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
package io.aeron.config;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Holds all metadata collected by {@link ConfigProcessor} for a single logical configuration
 * option, serialised to {@code config-info.dat} for use by downstream Gradle tasks.
 *
 * <p>A single {@code ConfigInfo} entry is built from up to three annotated Java elements that
 * share the same {@link #id}:</p>
 * <ul>
 *   <li>A {@code _PROP_NAME} field → populates {@link #propertyName},
 *       {@link #propertyNameFieldName}, {@link #propertyNameClassName}, and
 *       {@link #propertyNameDescription}.</li>
 *   <li>A {@code _DEFAULT} / {@code _DEFAULT_NS} field → populates {@link #defaultValue},
 *       {@link #defaultFieldName}, {@link #defaultClassName}, and
 *       {@link #defaultDescription}.</li>
 *   <li>A {@code Context} method → populates {@link #context} and
 *       {@link #contextDescription}.</li>
 * </ul>
 *
 * <p>After all elements are processed, {@link ConfigProcessor} derives the expected C driver
 * definitions ({@link ExpectedCConfig}) and runs a sanity check.  Use {@link Config#existsInC} /
 * {@link Config#existsInJava} to mark options that only exist on one side of the Java/C boundary.</p>
 *
 * <p>Each description field has a paired {@code *Clean} field that contains the same text with
 * Javadoc markup (inline tags, HTML) converted to Markdown, produced by
 * {@link io.aeron.utility.JavadocCleaner}.  The clean field is {@code null} when the cleaned
 * text is identical to the original (i.e. no markup was present).</p>
 */
public class ConfigInfo implements Serializable
{
    private static final long serialVersionUID = 6600224566064248728L;

    /**
     * Id.
     */
    public final String id;
    /**
     * List of expectations.
     */
    public final ExpectedConfig expectations;
    /**
     * Whether property was found.
     */
    public boolean foundPropertyName = false;
    /**
     * Whether default was found.
     */
    public boolean foundDefault = false;
    /**
     * Property description.
     */
    public String propertyNameDescription;
    /**
     * Property description with Javadoc markup converted to Markdown.
     */
    public String propertyNameDescriptionClean;
    /**
     * Property field name.
     */
    public String propertyNameFieldName;
    /**
     * Property class name.
     */
    public String propertyNameClassName;
    /**
     * Property name.
     */
    public String propertyName;
    /**
     * Default description.
     */
    public String defaultDescription;
    /**
     * Default description with Javadoc markup converted to Markdown.
     */
    public String defaultDescriptionClean;
    /**
     * Default field name.
     */
    public String defaultFieldName;
    /**
     * Default class name.
     */
    public String defaultClassName;
    /**
     * Default value.
     */
    public String defaultValue;
    /**
     * Default value string.
     */
    public String defaultValueString;
    /**
     * Default value type.
     */
    public DefaultType defaultValueType = DefaultType.UNDEFINED;
    /**
     * Default override type.
     */
    public String overrideDefaultValue;
    /**
     * Default override type value.
     */
    public DefaultType overrideDefaultValueType = DefaultType.UNDEFINED;
    /**
     * Uri param.
     */
    public String uriParam;
    /**
     * Whether property has context.
     */
    public boolean hasContext = true;
    /**
     * Context.
     */
    public String context;
    /**
     * Context description.
     */
    public String contextDescription;
    /**
     * Context description with Javadoc markup converted to Markdown.
     */
    public String contextDescriptionClean;
    /**
     * Is time value.
     */
    public Boolean isTimeValue;
    /**
     * Time unit.
     */
    public TimeUnit timeUnit;
    /**
     * Whether property is deprecated.
     */
    public boolean deprecated = false;
    /**
     * Whether this config option exists in Java.
     */
    public boolean existsInJava = true;

    /**
     * Construct the ConfigInfo with the unique id.
     *
     * @param id the unique identifier for this block o' config information.
     */
    public ConfigInfo(final String id)
    {
        this.id = id;
        expectations = new ExpectedConfig();
    }
}
