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
package io.aeron.driver;

import io.aeron.CommonContext;
import org.agrona.Strings;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.CompositeAgent;

class FixedNameCompositeAgent extends CompositeAgent
{
    private final String name;

    private String generateClassicName(final String classicName)
    {
        final String prefix = Strings.isEmpty(classicName) ? "" : classicName + " ";
        return prefix + super.roleName();
    }

    FixedNameCompositeAgent(final String newName, final String classicName, final Agent... agents)
    {
        super(agents);
        this.name = CommonContext.threadName(newName, generateClassicName(classicName));
    }

    public String roleName()
    {
        return name;
    }
}
