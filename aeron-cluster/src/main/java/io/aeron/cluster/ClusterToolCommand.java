/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.aeron.cluster;

import static io.aeron.cluster.ClusterToolOperator.SUCCESS;

import java.io.File;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Map;
import java.util.function.ToIntBiFunction;
import java.util.function.ToIntFunction;

/**
 * A command to be used by ClusterTool.
 * The command is an immutable representation of what to run (the Action), and the description for that action
 * (used in help).
 */
public final class ClusterToolCommand
{
    /**
     * Convenience method to ignore failure exit status.
     *
     * @param actual actual action.
     * @return SUCCESS
     */
    public static Action ignoreFailures(final Action actual)
    {
        return (clusterDir, out, args) ->
        {
            actual.act(clusterDir, out, args);
            return SUCCESS;
        };
    }

    /**
     * Convenience method for actions that do not require extra args.
     *
     * @param actual actual action.
     * @return SUCCESS
     */
    public static Action action(final ToIntBiFunction<File, PrintStream> actual)
    {
        return (clusterDir, out, args) -> actual.applyAsInt(clusterDir, out);
    }

    /**
     * Print help for a tool with the specified commands.
     *
     * @param commands map of commands by name.
     * @param prefix   usage description prefix.
     */
    public static void printHelp(final Map<String, ClusterToolCommand> commands, final String prefix)
    {
        System.out.format("%s%n", prefix);
        final int indentValue = Collections.max(commands.keySet().stream().map(String::length).toList()) + 1;
        final String indentSpaces = new String(new char[indentValue + 2]).replace('\0', ' ');
        for (final Map.Entry<String, ClusterToolCommand> command : commands.entrySet())
        {
            final int indent = indentValue - command.getKey().length();
            final String description = command.getValue().describe().replaceAll(
                "%n", String.format("%n") + indentSpaces);
            System.out.printf("%" + indent + "s", " ");
            System.out.printf("%s: %s%n", command.getKey(), description);
        }
        System.out.flush();
    }

    private final Action action;
    private final String description;

    /**
     * Constructor.
     *
     * @param action      action to perform for command.
     * @param description description of command.
     */
    public ClusterToolCommand(final Action action, final String description)
    {
        this.action = action;
        this.description = description;
    }

    /**
     * Convenience method for actions that only require the cluster directory.
     *
     * @param actual actual action.
     * @return SUCCESS.
     */
    public static Action action(final ToIntFunction<File> actual)
    {
        return (clusterDir, out, args) -> actual.applyAsInt(clusterDir);
    }

    /**
     * Description of command.
     *
     * @return description of command.
     */
    public String describe()
    {
        return description;
    }

    /**
     * The actual action to perform when given the command.
     *
     * @return the actual action to perform when given the command.
     */
    public Action action()
    {
        return action;
    }

    /**
     * Functional interface of a cluster tool operator action used in {@link ClusterToolCommand}.
     */
    @FunctionalInterface
    public interface Action
    {
        /**
         * An action for an operator tool to control cluster.
         *
         * @param clusterDir local cluster directory.
         * @param out        Where to print the output.
         * @param args       args to tool.
         * @return exit value.
         */
        int act(File clusterDir, PrintStream out, String[] args);
    }
}
