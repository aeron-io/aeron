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

/**
 * A handy class for storing expected C config info that can be serialized into json.
 */
public class ExpectedCConfig implements Serializable
{
    private static final long serialVersionUID = -4549394851227986144L;

    /**
     * Whether C config exists.
     */
    public boolean exists = true;
    /**
     * ENV var field name.
     */
    public String envVarFieldName;
    /**
     * ENV var name.
     */
    public String envVar;
    /**
     * Default field name.
     */
    public String defaultFieldName;
    /**
     * Default value.
     */
    public String defaultValue;
    /**
     * Default value type.
     */
    public DefaultType defaultValueType;
    /**
     * Whether default value validation should be skipped.
     */
    public boolean skipDefaultValidation = false;

    /**
     * Default constructor.
     */
    public ExpectedCConfig()
    {
    }
}
