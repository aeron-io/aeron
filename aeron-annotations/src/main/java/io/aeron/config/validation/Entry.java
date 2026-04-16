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
package io.aeron.config.validation;

import java.io.PrintStream;
import io.aeron.config.ConfigInfo;

class Entry
{
    private final ConfigInfo configInfo;
    final Validation envVarValidation;
    final Validation defaultValidation;

    Entry(final ConfigInfo configInfo)
    {
        this.configInfo = configInfo;
        this.envVarValidation = new Validation();
        this.defaultValidation = new Validation();
    }

    void printOn(final PrintStream out)
    {
        if (configInfo.expectations.c.exists)
        {
            out.println(configInfo.id);
            envVarValidation.printOn(out);
            defaultValidation.printOn(out);
        }
        else
        {
            out.println(configInfo.id + " -- SKIPPED");
        }
    }

    boolean printFailuresOn(final PrintStream out)
    {
        if (configInfo.expectations.c.exists && (!envVarValidation.isValid() || !defaultValidation.isValid()))
        {
            printOn(out);
            return true;
        }

        return false;
    }
}
