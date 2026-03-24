/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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
package io.aeron.cluster;

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import org.agrona.CloseHelper;
import org.agrona.collections.LongArrayList;
import org.agrona.collections.LongHashSet;

class RecordingLogValidator implements AutoCloseable
{
    private final int memberId;
    private final AeronArchive archive;
    private final RecordingLog recordingLog;
    private final LongHashSet unknownRecordingIds;

    private boolean isComplete = false;
    private LongArrayList recordingIds;

    RecordingLogValidator(
        final int memberId,
        final AeronArchive archive,
        final RecordingLog recordingLog)
    {
        this.memberId = memberId;
        this.archive = archive;
        this.recordingLog = recordingLog;
        this.unknownRecordingIds = new LongHashSet();
    }

    static RecordingLogValidator newInstance(
        final int memberId,
        final AeronArchive.Context archiveCtx,
        final RecordingLog recordingLog)
    {
        final AeronArchive archive = AeronArchive.connect(archiveCtx.clone().errorHandler(null));
        return new RecordingLogValidator(
            memberId,
            archive,
            recordingLog);
    }

    int poll()
    {
        if (null == recordingIds)
        {
            recordingIds = new LongArrayList();

            for (final RecordingLog.Entry entry : recordingLog.entries())
            {
                if (RecordingLog.ENTRY_TYPE_SNAPSHOT == entry.type && entry.isValid)
                {
                    recordingIds.addLong(entry.recordingId);
                }
            }
        }

        if (!recordingIds.isEmpty())
        {
            final long recordingId = recordingIds.removeAt(recordingIds.size() - 1);

            try
            {
                archive.getStartPosition(recordingId);
            }
            catch (final ArchiveException archiveException)
            {
                if (archiveException.errorCode() == ArchiveException.UNKNOWN_RECORDING)
                {
                    unknownRecordingIds.add(recordingId);
                }
            }
        }
        else
        {
            for (final RecordingLog.Entry entry : recordingLog.entries())
            {
                if (RecordingLog.ENTRY_TYPE_SNAPSHOT == entry.type &&
                    entry.isValid &&
                    unknownRecordingIds.contains(entry.recordingId))
                {
                    recordingLog.invalidateEntry(entry.entryIndex);

                    logSnapshotEntryInvalidation(
                        memberId, entry.entryIndex, entry.recordingId, entry.logPosition, entry.serviceId);
                }
            }

            isComplete = true;
        }

        return 1;
    }

    boolean isComplete()
    {
        return isComplete;
    }

    public void close()
    {
        CloseHelper.quietClose(archive);
    }

    private static void logSnapshotEntryInvalidation(
        final int memberId,
        final int entryIndex,
        final long recordingId,
        final long logPosition,
        final int serviceId)
    {
    }
}
