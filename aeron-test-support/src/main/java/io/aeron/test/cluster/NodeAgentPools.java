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
package io.aeron.test.cluster;

import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.util.Arrays;

/**
 * Pools cluster-node component agents by TYPE across all nodes onto a small fixed number of
 * cooperative threads (one for every node's driver, one for every archive, one for every consensus module,
 * one for every clustered service), so an oversubscribed CI runner is not asked to schedule ~4 threads per
 * node.
 * <p>
 * Components that block on one another always live on DIFFERENT pools, so the blocking handshake is serviced
 * cross-thread and never deadlocks: a client connects to the driver (driver pool); a consensus module /
 * service connects to its archive (archive pool); and a consensus module blocks on its service during
 * election ({@code awaitServicesReady}) and recovery (the {@code ServiceAck} handshake), so the consensus
 * module and service occupy separate pools. Archive-to-archive replication is asynchronous, so all archives
 * may safely share one thread.
 */
final class NodeAgentPools implements AutoCloseable
{
    private final InvokerPool driverPool = new InvokerPool("test-driver-pool");
    private final InvokerPool archivePool = new InvokerPool("test-archive-pool");
    private final InvokerPool consensusModulePool = new InvokerPool("test-consensus-module-pool");
    private final InvokerPool servicePool = new InvokerPool("test-service-pool");
    private final AgentRunner driverRunner;
    private final AgentRunner archiveRunner;
    private final AgentRunner consensusModuleRunner;
    private final AgentRunner serviceRunner;

    NodeAgentPools(final ErrorHandler errorHandler)
    {
        driverRunner = new AgentRunner(new YieldingIdleStrategy(), errorHandler, null, driverPool);
        archiveRunner = new AgentRunner(new YieldingIdleStrategy(), errorHandler, null, archivePool);
        consensusModuleRunner = new AgentRunner(new YieldingIdleStrategy(), errorHandler, null, consensusModulePool);
        serviceRunner = new AgentRunner(new YieldingIdleStrategy(), errorHandler, null, servicePool);
        AgentRunner.startOnThread(driverRunner);
        AgentRunner.startOnThread(archiveRunner);
        AgentRunner.startOnThread(consensusModuleRunner);
        AgentRunner.startOnThread(serviceRunner);
    }

    void registerDriver(final AgentInvoker invoker)
    {
        driverPool.add(invoker);
    }

    void registerArchive(final AgentInvoker invoker)
    {
        archivePool.add(invoker);
    }

    void registerConsensusModule(final AgentInvoker invoker)
    {
        consensusModulePool.add(invoker);
    }

    void registerService(final AgentInvoker invoker)
    {
        servicePool.add(invoker);
    }

    void deregister(final AgentInvoker invoker)
    {
        if (null == invoker)
        {
            return;
        }

        driverPool.remove(invoker);
        archivePool.remove(invoker);
        consensusModulePool.remove(invoker);
        servicePool.remove(invoker);
    }

    public void close()
    {
        CloseHelper.closeAll(driverRunner, archiveRunner, consensusModuleRunner, serviceRunner);
    }

    private static final class InvokerPool implements Agent
    {
        private final String roleName;
        private volatile AgentInvoker[] invokers = new AgentInvoker[0];

        InvokerPool(final String roleName)
        {
            this.roleName = roleName;
        }

        synchronized void add(final AgentInvoker invoker)
        {
            final AgentInvoker[] current = invokers;
            final AgentInvoker[] next = Arrays.copyOf(current, current.length + 1);
            next[current.length] = invoker;
            invokers = next;
        }

        synchronized void remove(final AgentInvoker invoker)
        {
            final AgentInvoker[] current = invokers;
            int index = -1;
            for (int i = 0; i < current.length; i++)
            {
                if (current[i] == invoker)
                {
                    index = i;
                    break;
                }
            }

            if (index < 0)
            {
                return;
            }

            final AgentInvoker[] next = new AgentInvoker[current.length - 1];
            System.arraycopy(current, 0, next, 0, index);
            System.arraycopy(current, index + 1, next, index, current.length - index - 1);
            invokers = next;
        }

        // Synchronized on the same monitor as add/remove so that once remove() returns, the pool thread
        // cannot be mid-invocation of the removed invoker; the caller may then close it safely.
        public synchronized int doWork()
        {
            final AgentInvoker[] current = invokers;
            int workCount = 0;
            for (final AgentInvoker invoker : current)
            {
                workCount += invoker.invoke();
            }

            return workCount;
        }

        public String roleName()
        {
            return roleName;
        }
    }
}
