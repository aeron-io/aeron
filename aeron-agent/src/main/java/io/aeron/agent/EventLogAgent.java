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
package io.aeron.agent;

import java.lang.instrument.Instrumentation;

/**
 * A placeholder to prevent existing systems using the -javaagent configuration from breaking.
 */
public final class EventLogAgent
{
    private EventLogAgent()
    {
    }

    /**
     * Premain method to run before the main method of the application.
     *
     * @param agentArgs       which are ignored.
     * @param instrumentation for applying to the agent.
     */
    public static void premain(final String agentArgs, final Instrumentation instrumentation)
    {
        System.out.println(
            "WARN: The aeron-agent is no longer required for logging, please remove the -javaagent setting");
    }

    /**
     * Agent main method for dynamic attach.
     *
     * @param agentArgs       containing configuration options or command to stop.
     * @param instrumentation for applying to the agent.
     */
    public static void agentmain(final String agentArgs, final Instrumentation instrumentation)
    {
        System.out.println("WARN: The aeron-agent no longer support dynamic attach");
    }
}

