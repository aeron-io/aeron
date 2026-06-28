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
package io.aeron.validation;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * A utility class for running 'grep'.
 */
public final class Grep
{
    /**
     * Execute a grep process.
     *
     * @param pattern the regex pattern passed to grep.
     * @param sourceDir the base directory where the search should begin.
     * @return a Grep object with the results of the action.
     */
    public static Grep execute(final String pattern, final String sourceDir)
    {
        final String commandString = "grep -r -n -E '^" + pattern + "' " + sourceDir;

        try
        {
            // TODO make grep location configurable??
            final Process process = new ProcessBuilder()
                .redirectErrorStream(true)
                .command(new String[] {"/usr/bin/grep", "-r", "-n", "-E", "^" + pattern, sourceDir})
                .start();

            final int exitCode = process.waitFor();

            try (
                InputStreamReader inputStreamReader = new InputStreamReader(process.getInputStream());
                BufferedReader reader = new BufferedReader(inputStreamReader))
            {
                return new Grep(commandString, exitCode, reader.lines().collect(Collectors.toList()));
            }
        }
        catch (final Exception e)
        {
            return new Grep(commandString, e);
        }
    }

    private final String commandString;

    private final int exitCode;

    private final List<String> lines;

    private final Exception e;

    private Grep(final String commandString, final Exception e)
    {
        this.commandString = commandString;
        this.exitCode = -1;
        this.lines = Collections.emptyList();
        this.e = e;
    }

    private Grep(final String commandString, final int exitCode, final List<String> lines)
    {
        this.commandString = commandString;
        this.exitCode = exitCode;
        this.lines = lines;
        this.e = null;
    }

    /**
     * Whether grep succeeded.
     *
     * @return whether grep succeeded.
     */
    public boolean success()
    {
        return success(true);
    }

    /**
     * Determine if the process completed successfully.
     *
     * @param expectOneLine many of the usages expect only a single line to be found.
     *                      if more than one are found, that counts as a failure
     * @return whether grep succeeded.
     */
    public boolean success(final boolean expectOneLine)
    {
        if (this.e != null)
        {
            return false;
        }

        if (this.exitCode != 0)
        {
            return false;
        }

        return !expectOneLine || this.lines.size() == 1;
    }

    /**
     * The command string that was executed.
     *
     * @return the command string that was executed.
     */
    public String getCommandString()
    {
        return this.commandString;
    }

    /**
     * The filename and line number of the first line of output.
     *
     * @return the filename and line number of the first line of output.
     */
    public String getFilenameAndLine()
    {
        return getFilenameAndLine(0);
    }

    /**
     * Get the file name and line, given the line number.
     *
     * @param lineNumber specify the line of output
     * @return the filename and line number of the specified line of output
     */
    public String getFilenameAndLine(final int lineNumber)
    {
        final String[] pieces = this.lines.get(lineNumber).split(":");
        return pieces[0] + ":" + pieces[1];
    }

    /**
     * The first line of output (minus the filename and line number).
     *
     * @return the first line of output (minus the filename and line number).
     */
    public String getOutput()
    {
        return getOutput(0);
    }

    /**
     * Get the output given the line number.
     *
     * @param lineNumber specify the line of output
     * @return the output of the specified line number (minus the filename and the line number)
     */
    public String getOutput(final int lineNumber)
    {
        return this.lines.get(lineNumber).split(":")[2];
    }

    /**
     * Apply the consumer to all the lines in the result.
     *
     * @param action a BiConsumer that consumes the filename/line number and output for each line of output
     */
    public void forEach(final BiConsumer<String, String> action)
    {
        for (int i = 0; i < lines.size(); i++)
        {
            action.accept(getFilenameAndLine(i), getOutput(i));
        }
    }
}
