/*
 * Copyright 2014-2024 Real Logic Limited.
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class IpTables
{
    public static boolean runIpTablesCmd(final boolean ignoreError, final String... command)
    {
        try
        {
            final Process start = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .start();

            final ByteArrayOutputStream commandOutput = new ByteArrayOutputStream();
            try (InputStream inputStream = start.getInputStream())
            {
                final byte[] block = new byte[4096];
                while (start.isAlive())
                {
                    final int read = inputStream.read(block);
                    if (0 < read)
                    {
                        commandOutput.write(block, 0, read);
                    }
                    Tests.yield();
                }
            }

            final boolean isSuccess = 0 == start.exitValue();
            if (!isSuccess && !ignoreError)
            {
                final String commandMsg = commandOutput.toString(StandardCharsets.UTF_8);
                throw new RuntimeException("Command: '" + String.join(" ", command) + "' failed - " + commandMsg);
            }

            return isSuccess;
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    public static void deleteChain(final String chainName)
    {
        runIpTablesCmd(false, "sudo", "iptables", "-X", chainName);
    }

    public static void removeFromInput(final String chainName)
    {
        boolean isSuccess;
        do
        {
            isSuccess = runIpTablesCmd(true, "sudo", "iptables", "-D", "INPUT", "-j", chainName);
        }
        while (isSuccess);
    }

    public static void addToInput(final String chainName)
    {
        runIpTablesCmd(true, "sudo", "iptables", "-A", "INPUT", "-j", chainName);
    }

    public static void makeNetworkPartition(
        final String chainName,
        final List<String> hostnames,
        final int toIsolateIndex)
    {
        final String toIsolateHostname = hostnames.get(toIsolateIndex);
        for (int i = 0; i < hostnames.size(); i++)
        {
            if (i != toIsolateIndex)
            {
                final String otherHostname = hostnames.get(i);
                runIpTablesCmd(
                    false, "sudo", "iptables", "-A", chainName,
                    "-d", toIsolateHostname,
                    "-s", otherHostname,
                    "-j", "DROP");
                runIpTablesCmd(
                    false, "sudo", "iptables", "-A", chainName,
                    "-d", otherHostname,
                    "-s", toIsolateHostname,
                    "-j", "DROP");
            }
        }

        runIpTablesCmd(false, "sudo", "iptables", "-A", chainName, "-j", "RETURN");
    }

    public static void createChain(final String chainName)
    {
        runIpTablesCmd(true, "sudo", "-n", "iptables", "-N", chainName);
    }

    public static void flushChain(final String chainName)
    {
        runIpTablesCmd(true, "sudo", "-n", "iptables", "-F", chainName);
    }

    public static void setupChain(final String chainName)
    {
        createChain(chainName);
        flushChain(chainName);
        addToInput(chainName);
    }

    public static void tearDownChain(final String chainName)
    {
        flushChain(chainName);
        removeFromInput(chainName);
        deleteChain(chainName);
    }
}
