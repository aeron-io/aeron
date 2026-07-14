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

/**
 * MBean interface for a logging agent that stores events in memory and allows them to be periodically written
 * out. Useful within tests.
 */
public interface CollectingEventLogReaderAgentMBean
{
    /**
     * Enable or disable the collection of logs.
     *
     * @param isCollecting whether logs should be collected or not.
     */
    void setCollecting(boolean isCollecting);

    /**
     * Shows whether logs are being collected or not.
     *
     * @return true to indicate logs are being collected in memory.
     */
    boolean isCollecting();

    /**
     * Reset the internal positions within the collector discarding all previous logs. Should be called periodically,
     * so to avoid consuming all available memory.
     */
    void reset();

    /**
     * Start collecting messages and use the specified name as a title to start logging.
     *
     * @param name a value to be included as a message in the log to delineate events.
     */
    void startCollecting(String name);

    /**
     * Output the collected logs to file.
     *
     * @param filename of file to write to.
     */
    void writeToFile(String filename);
}
