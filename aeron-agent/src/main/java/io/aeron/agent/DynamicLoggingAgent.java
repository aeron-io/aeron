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

import net.bytebuddy.agent.ByteBuddyAgent;
import org.agrona.PropertyAction;
import org.agrona.Strings;
import org.agrona.SystemUtil;

import java.io.File;
import java.nio.file.Paths;
import java.util.Map;

import static io.aeron.agent.ConfigOption.*;
import static java.lang.System.out;

/**
 * Attach/detach logging agent dynamically to a running process.
 */
public final class DynamicLoggingAgent
{
    private DynamicLoggingAgent()
    {
    }

    /**
     * Attach logging agent jar to a running process.
     *
     * @param args program arguments.
     */
    public static void main(final String[] args)
    {
        if (args.length < 3)
        {
            printHelp();
            System.exit(-1);
        }

        final File agentJar = Paths.get(args[0]).toAbsolutePath().toFile();
        if (!agentJar.exists())
        {
            throw new IllegalArgumentException(agentJar + " does not exist!");
        }

        final String processId = args[1];
        if (Strings.isEmpty(processId))
        {
            throw new IllegalArgumentException("no PID provided!");
        }

        final String command = args[2];
        switch (command)
        {
            case START_COMMAND:
            {
                for (int i = 3; i < args.length; i++)
                {
                    SystemUtil.loadPropertiesFile(PropertyAction.PRESERVE, args[i]);
                }

                final Map<String, String> configOptions = fromSystemProperties();
                final String agentArgs = buildAgentArgs(configOptions);
                attachAgent(START_COMMAND, agentJar, processId, agentArgs);
                out.println("Logging started.");
                break;
            }

            case STOP_COMMAND:
            {
                attachAgent(STOP_COMMAND, agentJar, processId, command);
                out.println("Logging stopped.");
                break;
            }

            default:
                throw new IllegalArgumentException("invalid command: " + command);
        }
    }

    private static void printHelp()
    {
        out.println("Usage: <agent-jar> <java-process-id> <command> [property files...]");
        out.println("  <agent-jar> - fully qualified path to the agent jar");
        out.println("  <java-process-id> - PID of the Java process to attach an agent to");
        out.println("  <command> - either '" + START_COMMAND + "' or '" + STOP_COMMAND + "'");
        out.println("  [property files...] - an optional list of property files to configure logging options");
        out.println("Note: logging options can be specified either via system properties or the property files.");
    }

    private static void attachAgent(
        final String command, final File agentJar, final String processId, final String agentArgs)
    {
        try
        {
            ByteBuddyAgent.attach(agentJar, processId, agentArgs);
        }
        catch (final Exception ex)
        {
            out.println("Command '" + command + "' failed, cause: " + getCause(ex));
            System.exit(-1);
        }
    }

    private static Throwable getCause(final Throwable t)
    {
        Throwable cause = t;
        while (null != cause.getCause())
        {
            cause = cause.getCause();
        }

        return cause;
    }
}
