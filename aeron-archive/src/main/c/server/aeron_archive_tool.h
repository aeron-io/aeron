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

#ifndef AERON_ARCHIVE_TOOL_H
#define AERON_ARCHIVE_TOOL_H

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Verify options controlling the behavior of aeron_archive_tool_verify().
 */
typedef enum aeron_archive_tool_verify_option_en
{
    /** Only verify the last segment file of each recording (default). */
    AERON_ARCHIVE_TOOL_VERIFY_LAST_SEGMENT_ONLY = 0,

    /** Verify all segment files for each recording. */
    AERON_ARCHIVE_TOOL_VERIFY_ALL_SEGMENT_FILES = 1
}
aeron_archive_tool_verify_option_t;

/**
 * Describe (print) all valid recordings in the catalog.
 *
 * Opens the catalog read-only and prints metadata for each recording in VALID state.
 *
 * @param out         output stream for printing (e.g. stdout).
 * @param archive_dir the archive directory path.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_tool_describe(FILE *out, const char *archive_dir);

/**
 * Describe a single recording by recording_id.
 *
 * @param out          output stream for printing.
 * @param archive_dir  the archive directory path.
 * @param recording_id the recording id to describe.
 * @return 0 on success, -1 if not found or on failure.
 */
int aeron_archive_tool_describe_recording(FILE *out, const char *archive_dir, int64_t recording_id);

/**
 * Verify recording integrity by checking segment files.
 *
 * For each valid recording in the catalog, checks that segment files exist and that
 * frame headers within them are consistent (correct term id, term offset, stream id).
 *
 * @param out         output stream for printing results and errors.
 * @param archive_dir the archive directory path.
 * @param option      verification option controlling which segment files to check.
 * @return true if no errors were found, false otherwise.
 */
bool aeron_archive_tool_verify(
    FILE *out,
    const char *archive_dir,
    aeron_archive_tool_verify_option_t option);

/**
 * Verify a single recording by recording_id.
 *
 * @param out          output stream for printing results and errors.
 * @param archive_dir  the archive directory path.
 * @param recording_id the recording id to verify.
 * @param option       verification option controlling which segment files to check.
 * @return true if no errors were found, false otherwise.
 */
bool aeron_archive_tool_verify_recording(
    FILE *out,
    const char *archive_dir,
    int64_t recording_id,
    aeron_archive_tool_verify_option_t option);

/**
 * Count the number of valid recordings in the catalog.
 *
 * @param archive_dir the archive directory path.
 * @return the count of valid recordings, or -1 on error.
 */
int32_t aeron_archive_tool_count(const char *archive_dir);

/**
 * Get the maximum number of entries (capacity) supported by the catalog.
 *
 * Returns the catalog file capacity divided by the default record alignment.
 *
 * @param archive_dir the archive directory path.
 * @return the max entries capacity, or -1 on error.
 */
int64_t aeron_archive_tool_max_entries(const char *archive_dir);

/**
 * Read the PID of the archive process from the mark file.
 *
 * @param archive_dir the archive directory path.
 * @return the PID value, or -1 on error.
 */
int64_t aeron_archive_tool_pid(const char *archive_dir);

#ifdef __cplusplus
}
#endif

#endif /* AERON_ARCHIVE_TOOL_H */
