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
package io.aeron.driver.reports;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static org.agrona.IoUtil.mapExistingFile;
import static org.agrona.IoUtil.mapNewFile;

/**
 * Utility functions for dealing with the Loss Report.
 */
public final class LossReportUtil
{
    /**
     * Name of the error report file in the Aeron directory.
     */
    public static final String LOSS_REPORT_FILE_NAME = "loss-report.dat";

    private LossReportUtil()
    {
    }

    /**
     * Create a new {@link File} object for the loss report.
     *
     * @param aeronDirectoryName in which the loss report should exist.
     * @return the new {@link File} for the loss report.
     */
    public static File file(final String aeronDirectoryName)
    {
        return new File(aeronDirectoryName, LOSS_REPORT_FILE_NAME);
    }

    /**
     * Map a new loss report in the Aeron directory for a given length.
     *
     * @param aeronDirectoryName in which to create the file.
     * @param reportFileLength   for the file.
     * @return the newly mapped buffer for the file.
     */
    public static MappedByteBuffer mapLossReport(final String aeronDirectoryName, final int reportFileLength)
    {
        return mapNewFile(file(aeronDirectoryName), reportFileLength, false);
    }

    /**
     * Map a new loss report in the Aeron directory for a given length.
     *
     * @param aeronDirectoryName containing the file
     * @return the read only mapped buffer for the file.
     */
    public static MappedByteBuffer mapLossReportReadOnly(final String aeronDirectoryName)
    {
        return mapExistingFile(file(aeronDirectoryName), FileChannel.MapMode.READ_ONLY, "Loss Report");
    }
}
