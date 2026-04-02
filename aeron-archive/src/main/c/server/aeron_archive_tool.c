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

#include "aeron_archive_tool.h"
#include "aeron_archive_catalog.h"
#include "aeron_archive_mark_file.h"
#include "aeron_archive_recording_writer.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>

#if defined(_MSC_VER)
#include <io.h>
#include <windows.h>
#define open _open
#define close _close
#define read _read
#define fstat _fstat
#define stat _stat
#define O_RDONLY _O_RDONLY

static int aeron_archive_tool_pread(int fd, void *buf, size_t count, int64_t offset)
{
    if (_lseeki64(fd, offset, SEEK_SET) < 0) { return -1; }
    return _read(fd, buf, (unsigned int)count);
}
#define pread(fd, buf, count, offset) aeron_archive_tool_pread(fd, buf, count, offset)
typedef int ssize_t;
#else
#include <dirent.h>
#include <unistd.h>
#endif

#include "aeron_common.h"
#include "util/aeron_error.h"
#include "util/aeron_bitutil.h"
#include "protocol/aeron_udp_protocol.h"
#include "concurrent/aeron_logbuffer_descriptor.h"

#define AERON_ARCHIVE_TOOL_NULL_POSITION (-1LL)

/* ---------------------------------------------------------------------------
 * Internal helpers
 * --------------------------------------------------------------------------- */

static const char *aeron_archive_tool_recording_state_name(aeron_archive_recording_state_t state)
{
    switch (state)
    {
        case AERON_ARCHIVE_RECORDING_VALID:
            return "VALID";
        case AERON_ARCHIVE_RECORDING_INVALID:
            return "INVALID";
        case AERON_ARCHIVE_RECORDING_DELETED:
            return "DELETED";
        default:
            return "UNKNOWN";
    }
}

static void aeron_archive_tool_print_descriptor(
    FILE *out,
    const aeron_archive_catalog_recording_descriptor_t *descriptor)
{
    fprintf(out,
        "recordingId=%" PRId64
        " state=%s"
        " startPosition=%" PRId64
        " stopPosition=%" PRId64
        " startTimestamp=%" PRId64
        " stopTimestamp=%" PRId64
        " initialTermId=%" PRId32
        " segmentFileLength=%" PRId32
        " termBufferLength=%" PRId32
        " mtuLength=%" PRId32
        " sessionId=%" PRId32
        " streamId=%" PRId32
        " strippedChannel=%.*s"
        " originalChannel=%.*s"
        " sourceIdentity=%.*s"
        "\n",
        descriptor->recording_id,
        aeron_archive_tool_recording_state_name(descriptor->state),
        descriptor->start_position,
        descriptor->stop_position,
        descriptor->start_timestamp,
        descriptor->stop_timestamp,
        descriptor->initial_term_id,
        descriptor->segment_file_length,
        descriptor->term_buffer_length,
        descriptor->mtu_length,
        descriptor->session_id,
        descriptor->stream_id,
        (int)descriptor->stripped_channel_length, descriptor->stripped_channel,
        (int)descriptor->original_channel_length, descriptor->original_channel,
        (int)descriptor->source_identity_length, descriptor->source_identity);
}

/**
 * Parse the segment base position from a segment file name of the form "<recordingId>-<position>.rec".
 * Returns the position, or -1 on parse failure.
 */
static int64_t aeron_archive_tool_parse_segment_file_position(const char *filename)
{
    const char *dash = strchr(filename, '-');
    if (NULL == dash)
    {
        return -1;
    }

    char *end = NULL;
    int64_t position = strtoll(dash + 1, &end, 10);
    if (NULL == end || *end != '.')
    {
        return -1;
    }

    return position;
}

/**
 * Check if a filename matches the segment file pattern: "<digits>-<digits>.rec"
 */
static bool aeron_archive_tool_is_segment_file(const char *filename)
{
    const char *suffix = strstr(filename, AERON_ARCHIVE_RECORDING_SEGMENT_SUFFIX);
    if (NULL == suffix)
    {
        return false;
    }

    /* Verify there is a dash and digits before the suffix */
    const char *dash = strchr(filename, '-');
    if (NULL == dash || dash >= suffix)
    {
        return false;
    }

    return true;
}

/**
 * Compute the number of leading zero bits for determining position bits to shift.
 */
static int32_t aeron_archive_tool_position_bits_to_shift(int32_t term_length)
{
    int32_t shift = 0;
    int32_t v = term_length;
    while (v > 1)
    {
        v >>= 1;
        shift++;
    }
    return shift;
}

/**
 * Compute the term id for a given position.
 */
static int32_t aeron_archive_tool_compute_term_id(
    int64_t position, int32_t position_bits_to_shift, int32_t initial_term_id)
{
    return (int32_t)(position >> position_bits_to_shift) + initial_term_id;
}

/**
 * Check if position is not frame aligned.
 */
static bool aeron_archive_tool_is_not_frame_aligned(int64_t position)
{
    return 0 != (position & (AERON_LOGBUFFER_FRAME_ALIGNMENT - 1));
}

/* ---------------------------------------------------------------------------
 * Segment file verification
 * --------------------------------------------------------------------------- */

/**
 * Verify a single segment file by reading frame headers and checking consistency.
 *
 * @return true if the segment file is valid, false if errors were found.
 */
static bool aeron_archive_tool_verify_segment_file(
    FILE *out,
    const char *archive_dir,
    int64_t recording_id,
    const char *segment_filename,
    int64_t start_position,
    int32_t term_length,
    int32_t segment_length,
    int32_t stream_id,
    int32_t initial_term_id)
{
    char filepath[AERON_MAX_PATH];
    snprintf(filepath, sizeof(filepath), "%s/%s", archive_dir, segment_filename);

    int fd = open(filepath, O_RDONLY);
    if (fd < 0)
    {
        fprintf(out, "(recordingId=%" PRId64 ", file=%s) ERR: failed to open segment file: %s\n",
            recording_id, segment_filename, strerror(errno));
        return false;
    }

    struct stat st;
    if (fstat(fd, &st) < 0)
    {
        fprintf(out, "(recordingId=%" PRId64 ", file=%s) ERR: fstat failed: %s\n",
            recording_id, segment_filename, strerror(errno));
        close(fd);
        return false;
    }

    const int64_t file_size = st.st_size;
    const int64_t offset_limit = file_size < segment_length ? file_size : segment_length;
    const int32_t position_bits_to_shift = aeron_archive_tool_position_bits_to_shift(term_length);
    const int64_t start_term_offset = start_position & (term_length - 1);
    const int64_t start_term_base_position = start_position - start_term_offset;
    const int64_t segment_file_base_position = aeron_archive_tool_parse_segment_file_position(segment_filename);

    int64_t file_offset = (segment_file_base_position == start_term_base_position) ? start_term_offset : 0;
    int64_t position = segment_file_base_position + file_offset;

    aeron_data_header_t header;
    bool valid = true;

    while (file_offset < offset_limit)
    {
        ssize_t bytes_read = pread(fd, &header, sizeof(header), (off_t)file_offset);
        if (bytes_read < (ssize_t)sizeof(aeron_frame_header_t))
        {
            fprintf(out, "(recordingId=%" PRId64 ", file=%s) ERR: failed to read fragment header at offset %" PRId64 "\n",
                recording_id, segment_filename, file_offset);
            valid = false;
            break;
        }

        const int32_t frame_length = header.frame_header.frame_length;
        if (0 == frame_length)
        {
            break;
        }

        if (frame_length < 0)
        {
            fprintf(out, "(recordingId=%" PRId64 ", file=%s) ERR: negative frame length %" PRId32 " at offset %" PRId64 "\n",
                recording_id, segment_filename, frame_length, file_offset);
            valid = false;
            break;
        }

        const int32_t expected_term_id = aeron_archive_tool_compute_term_id(
            position, position_bits_to_shift, initial_term_id);
        const int32_t expected_term_offset = (int32_t)(position & (term_length - 1));

        if (header.term_id != expected_term_id ||
            header.term_offset != expected_term_offset)
        {
            fprintf(out,
                "(recordingId=%" PRId64 ", file=%s) ERR: fragment "
                "termOffset=%" PRId32 " (expected=%" PRId32 "), "
                "termId=%" PRId32 " (expected=%" PRId32 ")\n",
                recording_id, segment_filename,
                header.term_offset, expected_term_offset,
                header.term_id, expected_term_id);
            valid = false;
            break;
        }

        if (header.stream_id != stream_id)
        {
            fprintf(out,
                "(recordingId=%" PRId64 ", file=%s) ERR: fragment "
                "streamId=%" PRId32 " (expected=%" PRId32 ")\n",
                recording_id, segment_filename,
                header.stream_id, stream_id);
            valid = false;
            break;
        }

        const int32_t aligned_frame_length = (int32_t)AERON_ALIGN(
            (uint32_t)frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);

        file_offset += aligned_frame_length;
        position += aligned_frame_length;
    }

    close(fd);
    return valid;
}

/**
 * Data structure for collecting segment files for a single recording during directory scan.
 */
typedef struct aeron_archive_tool_segment_files_stct
{
    char **filenames;
    int32_t count;
    int32_t capacity;
}
aeron_archive_tool_segment_files_t;

static int aeron_archive_tool_segment_files_init(aeron_archive_tool_segment_files_t *files)
{
    files->count = 0;
    files->capacity = 16;
    files->filenames = (char **)calloc((size_t)files->capacity, sizeof(char *));
    if (NULL == files->filenames)
    {
        return -1;
    }
    return 0;
}

static int aeron_archive_tool_segment_files_add(
    aeron_archive_tool_segment_files_t *files, const char *filename)
{
    if (files->count >= files->capacity)
    {
        int32_t new_capacity = files->capacity * 2;
        char **new_filenames = (char **)realloc(files->filenames, (size_t)new_capacity * sizeof(char *));
        if (NULL == new_filenames)
        {
            return -1;
        }
        memset(new_filenames + files->capacity, 0, (size_t)(new_capacity - files->capacity) * sizeof(char *));
        files->filenames = new_filenames;
        files->capacity = new_capacity;
    }

    files->filenames[files->count] = strdup(filename);
    if (NULL == files->filenames[files->count])
    {
        return -1;
    }
    files->count++;
    return 0;
}

static void aeron_archive_tool_segment_files_close(aeron_archive_tool_segment_files_t *files)
{
    if (NULL != files->filenames)
    {
        for (int32_t i = 0; i < files->count; i++)
        {
            free(files->filenames[i]);
        }
        free(files->filenames);
        files->filenames = NULL;
    }
    files->count = 0;
    files->capacity = 0;
}

/**
 * Find the segment file with the highest base position from the list.
 * Returns the filename, or NULL if list is empty.
 */
static const char *aeron_archive_tool_find_max_segment_file(
    const aeron_archive_tool_segment_files_t *files)
{
    if (files->count == 0)
    {
        return NULL;
    }

    const char *max_file = files->filenames[0];
    int64_t max_position = aeron_archive_tool_parse_segment_file_position(max_file);

    for (int32_t i = 1; i < files->count; i++)
    {
        int64_t pos = aeron_archive_tool_parse_segment_file_position(files->filenames[i]);
        if (pos > max_position)
        {
            max_position = pos;
            max_file = files->filenames[i];
        }
    }

    return max_file;
}

/**
 * Collect all segment files for a given recording_id from the archive directory.
 */
static int aeron_archive_tool_collect_segment_files(
    const char *archive_dir,
    int64_t recording_id,
    aeron_archive_tool_segment_files_t *files)
{
    char prefix[64];
    snprintf(prefix, sizeof(prefix), "%" PRId64 "-", recording_id);
    size_t prefix_len = strlen(prefix);

#if defined(_MSC_VER)
    char search_path[AERON_MAX_PATH];
    snprintf(search_path, sizeof(search_path), "%s\\*", archive_dir);

    WIN32_FIND_DATAA find_data;
    HANDLE hFind = FindFirstFileA(search_path, &find_data);
    if (INVALID_HANDLE_VALUE == hFind)
    {
        return -1;
    }

    do
    {
        if (strncmp(find_data.cFileName, prefix, prefix_len) == 0 &&
            aeron_archive_tool_is_segment_file(find_data.cFileName))
        {
            if (aeron_archive_tool_segment_files_add(files, find_data.cFileName) < 0)
            {
                FindClose(hFind);
                return -1;
            }
        }
    }
    while (FindNextFileA(hFind, &find_data));

    FindClose(hFind);
#else
    DIR *dir = opendir(archive_dir);
    if (NULL == dir)
    {
        return -1;
    }

    struct dirent *entry;
    while (NULL != (entry = readdir(dir)))
    {
        if (strncmp(entry->d_name, prefix, prefix_len) == 0 &&
            aeron_archive_tool_is_segment_file(entry->d_name))
        {
            if (aeron_archive_tool_segment_files_add(files, entry->d_name) < 0)
            {
                closedir(dir);
                return -1;
            }
        }
    }

    closedir(dir);
#endif
    return 0;
}

/* ---------------------------------------------------------------------------
 * Verify context for use with catalog for_each callback
 * --------------------------------------------------------------------------- */

typedef struct aeron_archive_tool_verify_context_stct
{
    FILE *out;
    const char *archive_dir;
    aeron_archive_tool_verify_option_t option;
    int32_t error_count;
}
aeron_archive_tool_verify_context_t;

static bool aeron_archive_tool_verify_position_invariants(
    FILE *out, int64_t recording_id, int64_t start_position, int64_t stop_position)
{
    if (start_position < 0)
    {
        fprintf(out, "(recordingId=%" PRId64 ") ERR: Negative startPosition=%" PRId64 "\n",
            recording_id, start_position);
        return false;
    }

    if (aeron_archive_tool_is_not_frame_aligned(start_position))
    {
        fprintf(out, "(recordingId=%" PRId64 ") ERR: Non-aligned startPosition=%" PRId64 "\n",
            recording_id, start_position);
        return false;
    }

    if (stop_position != AERON_ARCHIVE_TOOL_NULL_POSITION)
    {
        if (stop_position < start_position)
        {
            fprintf(out,
                "(recordingId=%" PRId64 ") ERR: Invariant violation stopPosition=%" PRId64
                " is before startPosition=%" PRId64 "\n",
                recording_id, stop_position, start_position);
            return false;
        }

        if (aeron_archive_tool_is_not_frame_aligned(stop_position))
        {
            fprintf(out, "(recordingId=%" PRId64 ") ERR: Non-aligned stopPosition=%" PRId64 "\n",
                recording_id, stop_position);
            return false;
        }
    }

    return true;
}

/**
 * Verify a single recording entry. Called from the catalog for_each iteration.
 */
static void aeron_archive_tool_verify_entry(
    const aeron_archive_catalog_recording_descriptor_t *descriptor,
    void *clientd)
{
    aeron_archive_tool_verify_context_t *ctx = (aeron_archive_tool_verify_context_t *)clientd;

    const int64_t recording_id = descriptor->recording_id;
    const aeron_archive_recording_state_t state = descriptor->state;

    if (AERON_ARCHIVE_RECORDING_VALID != state && AERON_ARCHIVE_RECORDING_INVALID != state)
    {
        fprintf(ctx->out, "(recordingId=%" PRId64 ") skipping: %s\n",
            recording_id, aeron_archive_tool_recording_state_name(state));
        return;
    }

    const int64_t start_position = descriptor->start_position;
    const int64_t stop_position = descriptor->stop_position;

    if (!aeron_archive_tool_verify_position_invariants(ctx->out, recording_id, start_position, stop_position))
    {
        ctx->error_count++;
        return;
    }

    aeron_archive_tool_segment_files_t segment_files;
    if (aeron_archive_tool_segment_files_init(&segment_files) < 0)
    {
        fprintf(ctx->out, "(recordingId=%" PRId64 ") ERR: failed to allocate segment file list\n", recording_id);
        ctx->error_count++;
        return;
    }

    if (aeron_archive_tool_collect_segment_files(ctx->archive_dir, recording_id, &segment_files) < 0)
    {
        fprintf(ctx->out, "(recordingId=%" PRId64 ") ERR: failed to scan segment files\n", recording_id);
        ctx->error_count++;
        aeron_archive_tool_segment_files_close(&segment_files);
        return;
    }

    const int32_t term_length = descriptor->term_buffer_length;
    const int32_t segment_length = descriptor->segment_file_length;
    const int32_t stream_id = descriptor->stream_id;
    const int32_t initial_term_id = descriptor->initial_term_id;

    if (ctx->option == AERON_ARCHIVE_TOOL_VERIFY_ALL_SEGMENT_FILES)
    {
        for (int32_t i = 0; i < segment_files.count; i++)
        {
            if (!aeron_archive_tool_verify_segment_file(
                ctx->out, ctx->archive_dir, recording_id, segment_files.filenames[i],
                start_position, term_length, segment_length, stream_id, initial_term_id))
            {
                ctx->error_count++;
                aeron_archive_tool_segment_files_close(&segment_files);
                return;
            }
        }
    }
    else
    {
        const char *max_segment = aeron_archive_tool_find_max_segment_file(&segment_files);
        if (NULL != max_segment)
        {
            if (!aeron_archive_tool_verify_segment_file(
                ctx->out, ctx->archive_dir, recording_id, max_segment,
                start_position, term_length, segment_length, stream_id, initial_term_id))
            {
                ctx->error_count++;
                aeron_archive_tool_segment_files_close(&segment_files);
                return;
            }
        }
    }

    fprintf(ctx->out, "(recordingId=%" PRId64 ") OK\n", recording_id);
    aeron_archive_tool_segment_files_close(&segment_files);
}

/* ---------------------------------------------------------------------------
 * Describe context for use with catalog for_each callback
 * --------------------------------------------------------------------------- */

typedef struct aeron_archive_tool_describe_context_stct
{
    FILE *out;
}
aeron_archive_tool_describe_context_t;

static void aeron_archive_tool_describe_entry(
    const aeron_archive_catalog_recording_descriptor_t *descriptor,
    void *clientd)
{
    aeron_archive_tool_describe_context_t *ctx = (aeron_archive_tool_describe_context_t *)clientd;
    aeron_archive_tool_print_descriptor(ctx->out, descriptor);
}

/* ---------------------------------------------------------------------------
 * Public API
 * --------------------------------------------------------------------------- */

int aeron_archive_tool_describe(FILE *out, const char *archive_dir)
{
    if (NULL == out || NULL == archive_dir)
    {
        return -1;
    }

    aeron_archive_catalog_t *catalog = NULL;
    if (aeron_archive_catalog_open(&catalog, archive_dir, false) < 0)
    {
        fprintf(out, "ERR: failed to open catalog in %s\n", archive_dir);
        return -1;
    }

    fprintf(out, "Catalog capacity: %zu bytes\n", catalog->capacity);

    aeron_archive_tool_describe_context_t ctx;
    ctx.out = out;

    aeron_archive_catalog_for_each(catalog, aeron_archive_tool_describe_entry, &ctx);

    aeron_archive_catalog_close(catalog);
    return 0;
}

int aeron_archive_tool_describe_recording(FILE *out, const char *archive_dir, int64_t recording_id)
{
    if (NULL == out || NULL == archive_dir)
    {
        return -1;
    }

    aeron_archive_catalog_t *catalog = NULL;
    if (aeron_archive_catalog_open(&catalog, archive_dir, false) < 0)
    {
        fprintf(out, "ERR: failed to open catalog in %s\n", archive_dir);
        return -1;
    }

    aeron_archive_catalog_recording_descriptor_t descriptor;
    if (aeron_archive_catalog_find_recording(catalog, recording_id, &descriptor) < 0)
    {
        fprintf(out, "ERR: no recording found with recordingId=%" PRId64 "\n", recording_id);
        aeron_archive_catalog_close(catalog);
        return -1;
    }

    aeron_archive_tool_print_descriptor(out, &descriptor);

    aeron_archive_catalog_close(catalog);
    return 0;
}

bool aeron_archive_tool_verify(
    FILE *out,
    const char *archive_dir,
    aeron_archive_tool_verify_option_t option)
{
    if (NULL == out || NULL == archive_dir)
    {
        return false;
    }

    aeron_archive_catalog_t *catalog = NULL;
    if (aeron_archive_catalog_open(&catalog, archive_dir, false) < 0)
    {
        fprintf(out, "ERR: failed to open catalog in %s\n", archive_dir);
        return false;
    }

    aeron_archive_tool_verify_context_t ctx;
    ctx.out = out;
    ctx.archive_dir = archive_dir;
    ctx.option = option;
    ctx.error_count = 0;

    aeron_archive_catalog_for_each(catalog, aeron_archive_tool_verify_entry, &ctx);

    aeron_archive_catalog_close(catalog);
    return ctx.error_count == 0;
}

bool aeron_archive_tool_verify_recording(
    FILE *out,
    const char *archive_dir,
    int64_t recording_id,
    aeron_archive_tool_verify_option_t option)
{
    if (NULL == out || NULL == archive_dir)
    {
        return false;
    }

    aeron_archive_catalog_t *catalog = NULL;
    if (aeron_archive_catalog_open(&catalog, archive_dir, false) < 0)
    {
        fprintf(out, "ERR: failed to open catalog in %s\n", archive_dir);
        return false;
    }

    aeron_archive_catalog_recording_descriptor_t descriptor;
    if (aeron_archive_catalog_find_recording(catalog, recording_id, &descriptor) < 0)
    {
        fprintf(out, "ERR: no recording found with recordingId=%" PRId64 "\n", recording_id);
        aeron_archive_catalog_close(catalog);
        return false;
    }

    aeron_archive_tool_verify_context_t ctx;
    ctx.out = out;
    ctx.archive_dir = archive_dir;
    ctx.option = option;
    ctx.error_count = 0;

    aeron_archive_tool_verify_entry(&descriptor, &ctx);

    aeron_archive_catalog_close(catalog);
    return ctx.error_count == 0;
}

int32_t aeron_archive_tool_count(const char *archive_dir)
{
    if (NULL == archive_dir)
    {
        return -1;
    }

    aeron_archive_catalog_t *catalog = NULL;
    if (aeron_archive_catalog_open(&catalog, archive_dir, false) < 0)
    {
        return -1;
    }

    int32_t count = aeron_archive_catalog_recording_count(catalog);

    aeron_archive_catalog_close(catalog);
    return count;
}

int64_t aeron_archive_tool_max_entries(const char *archive_dir)
{
    if (NULL == archive_dir)
    {
        return -1;
    }

    aeron_archive_catalog_t *catalog = NULL;
    if (aeron_archive_catalog_open(&catalog, archive_dir, false) < 0)
    {
        return -1;
    }

    int64_t max_entries = (int64_t)(catalog->capacity / (size_t)catalog->alignment);

    aeron_archive_catalog_close(catalog);
    return max_entries;
}

int64_t aeron_archive_tool_pid(const char *archive_dir)
{
    if (NULL == archive_dir)
    {
        return -1;
    }

    char mark_file_path[AERON_MAX_PATH];
    snprintf(mark_file_path, sizeof(mark_file_path), "%s/%s",
        archive_dir, AERON_ARCHIVE_MARK_FILE_FILENAME);

    int fd = open(mark_file_path, O_RDONLY);
    if (fd < 0)
    {
        /* Try the link file: read the path from archive-mark.lnk */
        char link_file_path[AERON_MAX_PATH];
        snprintf(link_file_path, sizeof(link_file_path), "%s/%s",
            archive_dir, AERON_ARCHIVE_MARK_FILE_LINK_FILENAME);

        int link_fd = open(link_file_path, O_RDONLY);
        if (link_fd < 0)
        {
            return -1;
        }

        char resolved_dir[AERON_MAX_PATH];
        ssize_t n = read(link_fd, resolved_dir, sizeof(resolved_dir) - 1);
        close(link_fd);

        if (n <= 0)
        {
            return -1;
        }
        resolved_dir[n] = '\0';

        /* Trim trailing whitespace */
        while (n > 0 && (resolved_dir[n - 1] == '\n' || resolved_dir[n - 1] == '\r' || resolved_dir[n - 1] == ' '))
        {
            resolved_dir[--n] = '\0';
        }

        int sn = snprintf(mark_file_path, sizeof(mark_file_path), "%s/%s",
            resolved_dir, AERON_ARCHIVE_MARK_FILE_FILENAME);
        if (sn < 0 || (size_t)sn >= sizeof(mark_file_path))
        {
            return -1;
        }

        fd = open(mark_file_path, O_RDONLY);
        if (fd < 0)
        {
            return -1;
        }
    }

    /* Read just enough of the header to get the PID field */
    uint8_t header[AERON_ARCHIVE_MARK_FILE_PID_OFFSET + (int)sizeof(int64_t)];
    ssize_t bytes_read = read(fd, header, sizeof(header));
    close(fd);

    if (bytes_read < (ssize_t)sizeof(header))
    {
        return -1;
    }

    int64_t pid;
    memcpy(&pid, header + AERON_ARCHIVE_MARK_FILE_PID_OFFSET, sizeof(pid));

    return pid;
}
