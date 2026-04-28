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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#if defined(__linux__) || defined(AERON_COMPILER_MSVC)
#define AERON_NATIVE_PRETOUCH
#endif

#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "aeron_platform.h"
#include "aeron_error.h"
#include "aeron_fileutil.h"

#ifdef _MSC_VER
#define AERON_FILE_SEP '\\'
#else
#define AERON_FILE_SEP '/'
#endif

#ifdef _MSC_VER
#define AERON_FILE_SEP_STR "\\"
#else
#define AERON_FILE_SEP_STR "/"
#endif

#if defined(AERON_COMPILER_MSVC)

#include <windows.h>
#include <winternl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <io.h>
#include <direct.h>

#include "aeron_alloc.h"

#define PROT_READ  1
#define PROT_WRITE 2
#define MAP_FAILED ((void *)-1)

#define MAP_SHARED 0x01
#define S_IRUSR _S_IREAD
#define S_IWUSR _S_IWRITE
#define S_IRGRP 0
#define S_IWGRP 0
#define S_IROTH 0
#define S_IWOTH 0

static int aeron_mmap(aeron_mapped_file_t *mapping, int fd, bool pre_touch)
{
    HANDLE hmap = CreateFileMapping((HANDLE)_get_osfhandle(fd), 0, PAGE_READWRITE, 0, 0, 0);

    if (!hmap)
    {
        AERON_SET_ERR_WIN(GetLastError(), "%s", "CreateFileMapping failed");
        return -1;
    }

    mapping->addr = MapViewOfFileEx(hmap, FILE_MAP_WRITE, 0, 0, mapping->length, NULL);

    if (!mapping->addr)
    {
        mapping->addr = MAP_FAILED;
        AERON_SET_ERR_WIN(GetLastError(), "%s", "MapViewOfFileEx failed");

        if (!CloseHandle(hmap))
        {
            AERON_APPEND_ERR("(%d) Failed to close file mapping handle", GetLastError());
        }
        _close(fd);
        return -1;
    }

    if (!CloseHandle(hmap))
    {
        AERON_SET_ERR_WIN(GetLastError(), "%s", "Failed to close file mapping handle");
        if (0 != aeron_unmap(mapping))
        {
            AERON_APPEND_ERR("(%d) Failed to unmap", GetLastError());
        }
        _close(fd);
        return -1;
    }

    _close(fd);

    if (pre_touch && MAP_FAILED != mapping->addr)
    {
        WIN32_MEMORY_RANGE_ENTRY entry;
        entry.NumberOfBytes = mapping->length;
        entry.VirtualAddress = mapping->addr;

        if (!PrefetchVirtualMemory(GetCurrentProcess(), 1, &entry, 0))
        {
            AERON_SET_ERR_WIN(GetLastError(), "%s", "PrefetchVirtualMemory failed");
            if (0 != aeron_unmap(mapping))
            {
                AERON_APPEND_ERR("(%d) Failed to unmap", GetLastError());
            }
            mapping->addr = MAP_FAILED;
        }
    }

    return MAP_FAILED == mapping->addr ? -1 : 0;
}

typedef NTSTATUS (NTAPI *aeron_pfn_NtCreateFile)(
    PHANDLE FileHandle,
    ACCESS_MASK DesiredAccess,
    POBJECT_ATTRIBUTES ObjectAttributes,
    PIO_STATUS_BLOCK IoStatusBlock,
    PLARGE_INTEGER AllocationSize,
    ULONG FileAttributes,
    ULONG ShareAccess,
    ULONG CreateDisposition,
    ULONG CreateOptions,
    PVOID EaBuffer,
    ULONG EaLength);

typedef ULONG (NTAPI *aeron_pfn_RtlNtStatusToDosError)(NTSTATUS Status);

// NtCreateFile CreateDisposition / CreateOptions values not present in winternl.h.
#define AERON_FILE_OPEN                        0x00000001UL
#define AERON_FILE_SYNCHRONOUS_IO_NONALERT     0x00000020UL
#define AERON_FILE_OPEN_FOR_BACKUP_INTENT      0x00004000UL
#define AERON_FILE_OPEN_REPARSE_POINT          0x00200000UL

#define AERON_STATUS_OBJECT_NAME_NOT_FOUND     ((NTSTATUS)0xC0000034L)
#define AERON_STATUS_OBJECT_PATH_NOT_FOUND     ((NTSTATUS)0xC000003AL)

// FileDispositionInfoEx and its flags may be missing from older Windows SDK
// headers; redeclare locally so the build doesn't depend on the SDK version.
#define AERON_FILE_DISPOSITION_INFO_EX_CLASS         21
#define AERON_FILE_DISPOSITION_FLAG_DELETE           0x00000001UL
#define AERON_FILE_DISPOSITION_FLAG_POSIX_SEMANTICS  0x00000002UL

typedef struct aeron_file_disposition_info_ex_stct
{
    DWORD Flags;
}
aeron_file_disposition_info_ex_t;

// NTFS caps filenames at 255 WCHAR (UCS-2); one extra slot for alignment/safety.
#define AERON_NT_MAX_NAME_WCHARS               256
// Size of the FileIdBothDirectoryInfo page returned per enumeration call.
#define AERON_NT_ENUM_BUFFER_BYTES             16384
// Initial capacity of the per-directory collected-names array.
#define AERON_NT_NAMES_INITIAL_CAPACITY        16

// Access mask used by every open-for-delete in this module: DELETE for the
// disposition call, READ_ATTRIBUTES for FileBasicInfo / FileId queries,
// WRITE_ATTRIBUTES for the READONLY clear, LIST_DIRECTORY for enumeration,
// and SYNCHRONIZE because we use synchronous I/O.
#define AERON_DELETE_ACCESS \
    (DELETE | FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES | FILE_LIST_DIRECTORY | SYNCHRONIZE)
// Full sharing so we don't fight with other handles to the same file.
#define AERON_DELETE_SHARE \
    (FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)

static inline bool aeron_is_dot_or_dotdot(const WCHAR *name, ULONG nchars)
{
    if (1 == nchars && L'.' == name[0])
    {
        return true;
    }
    if (2 == nchars && L'.' == name[0] && L'.' == name[1])
    {
        return true;
    }
    return false;
}

// FileId is captured when the filesystem supports it (NTFS / ReFS /
// most local filesystems) so we can verify after-open that the entry we got
// is the one we enumerated.
typedef struct aeron_nt_name_stct
{
    USHORT length_bytes;
    bool has_file_id;
    LARGE_INTEGER file_id;
    WCHAR name[AERON_NT_MAX_NAME_WCHARS];
}
aeron_nt_name_t;

static aeron_pfn_NtCreateFile aeron_p_NtCreateFile = NULL;
static aeron_pfn_RtlNtStatusToDosError aeron_p_RtlNtStatusToDosError = NULL;

static int aeron_load_nt(void)
{
    if (NULL != aeron_p_NtCreateFile && NULL != aeron_p_RtlNtStatusToDosError)
    {
        return 0;
    }

    HMODULE ntdll = GetModuleHandleW(L"ntdll.dll");
    if (NULL == ntdll)
    {
        AERON_APPEND_ERR_WIN(GetLastError(), "%s", "GetModuleHandle(ntdll.dll) failed");
        return -1;
    }

    aeron_p_NtCreateFile = (aeron_pfn_NtCreateFile)GetProcAddress(ntdll, "NtCreateFile");
    aeron_p_RtlNtStatusToDosError =
        (aeron_pfn_RtlNtStatusToDosError)GetProcAddress(ntdll, "RtlNtStatusToDosError");
    if (NULL == aeron_p_NtCreateFile || NULL == aeron_p_RtlNtStatusToDosError)
    {
        AERON_APPEND_ERR_WIN(GetLastError(), "%s", "GetProcAddress(NtCreateFile/RtlNtStatusToDosError) failed");
        return -1;
    }

    return 0;
}

// Enumerate `dir_handle` into a heap-allocated list of child names (. and .. skipped).
// On success, the caller owns *out_names and must aeron_free it.
//
// FileIdBothDirectoryInfo is preferred but unsupported on some file systems, so we
// fall back to FileFullDirectoryInfo if needed.
static int aeron_nt_collect_children(
    HANDLE dir_handle, const char *ctx, aeron_nt_name_t **out_names, size_t *out_count)
{
    aeron_nt_name_t *names = NULL;
    size_t capacity = AERON_NT_NAMES_INITIAL_CAPACITY;
    size_t count = 0;

    if (aeron_alloc((void **)&names, capacity * sizeof(aeron_nt_name_t)) < 0)
    {
        return -1;
    }

    FILE_INFO_BY_HANDLE_CLASS info_class = FileIdBothDirectoryInfo;
    char enum_buf[AERON_NT_ENUM_BUFFER_BYTES];
    while (true)
    {
        if (!GetFileInformationByHandleEx(dir_handle, info_class, enum_buf, sizeof(enum_buf)))
        {
            DWORD err = GetLastError();
            if (ERROR_NO_MORE_FILES == err)
            {
                break;
            }
            if (ERROR_INVALID_PARAMETER == err && FileIdBothDirectoryInfo == info_class && 0 == count)
            {
                info_class = FileFullDirectoryInfo;
                continue;
            }
            AERON_APPEND_ERR_WIN(err, "GetFileInformationByHandleEx(enumerate) failed under: %s", ctx);
            goto error;
        }

        const char *entry_bytes = enum_buf;
        while (true)
        {
            ULONG next_entry_offset;
            ULONG file_name_length;
            const WCHAR *file_name;
            LARGE_INTEGER file_id;
            bool has_file_id;
            if (FileIdBothDirectoryInfo == info_class)
            {
                const FILE_ID_BOTH_DIR_INFO *entry = (const FILE_ID_BOTH_DIR_INFO *)entry_bytes;
                next_entry_offset = entry->NextEntryOffset;
                file_name_length = entry->FileNameLength;
                file_name = entry->FileName;
                file_id = entry->FileId;
                has_file_id = true;
            }
            else
            {
                const FILE_FULL_DIR_INFO *entry = (const FILE_FULL_DIR_INFO *)entry_bytes;
                next_entry_offset = entry->NextEntryOffset;
                file_name_length = entry->FileNameLength;
                file_name = entry->FileName;
                file_id.QuadPart = 0;
                has_file_id = false;
            }

            if (!aeron_is_dot_or_dotdot(file_name, file_name_length / sizeof(WCHAR)))
            {
                if (file_name_length > (ULONG)sizeof(names[0].name))
                {
                    AERON_APPEND_ERR_WIN(EINVAL, "directory entry name too long under: %s", ctx);
                    goto error;
                }
                if (count == capacity)
                {
                    capacity *= 2;
                    // aeron_reallocf nulls *names on failure, so the error: path's
                    // aeron_free(NULL) is a safe no-op.
                    if (aeron_reallocf((void **)&names, capacity * sizeof(aeron_nt_name_t)) < 0)
                    {
                        goto error;
                    }
                }
                names[count].length_bytes = (USHORT)file_name_length;
                names[count].has_file_id = has_file_id;
                names[count].file_id = file_id;
                memcpy(names[count].name, file_name, file_name_length);
                count++;
            }
            if (0 == next_entry_offset)
            {
                break;
            }
            entry_bytes += next_entry_offset;
        }
    }

    *out_names = names;
    *out_count = count;
    return 0;

error:
    aeron_free(names);
    return -1;
}

// Mark the open handle for deletion. Prefer FileDispositionInfoEx with POSIX semantics
// (Windows 10 1709+ on NTFS) so the entry leaves the parent directory namespace
// synchronously even when a third party (AV, indexer, etc.) holds a FILE_SHARE_DELETE
// handle.
//
// Some file systems don't implement FileDispositionInfoEx, in which case we fall back
// to the legacy FILE_DISPOSITION_INFO.
static int aeron_set_disposition_delete(HANDLE h, const char *ctx)
{
    aeron_file_disposition_info_ex_t info_ex =
    {
        .Flags = AERON_FILE_DISPOSITION_FLAG_DELETE | AERON_FILE_DISPOSITION_FLAG_POSIX_SEMANTICS
    };
    if (SetFileInformationByHandle(
        h, (FILE_INFO_BY_HANDLE_CLASS)AERON_FILE_DISPOSITION_INFO_EX_CLASS, &info_ex, sizeof(info_ex)))
    {
        return 0;
    }

    const DWORD ex_err = GetLastError();
    if (ERROR_INVALID_PARAMETER == ex_err ||
        ERROR_INVALID_FUNCTION == ex_err ||
        ERROR_NOT_SUPPORTED == ex_err)
    {
        FILE_DISPOSITION_INFO info = { TRUE };
        if (SetFileInformationByHandle(h, FileDispositionInfo, &info, sizeof(info)))
        {
            return 0;
        }

        AERON_APPEND_ERR_WIN(GetLastError(),
            "SetFileInformationByHandle(FileDispositionInfo) failed (Ex returned %lu): %s",
            (unsigned long)ex_err, ctx);
        return -1;
    }

    AERON_APPEND_ERR_WIN(ex_err,
        "SetFileInformationByHandle(FileDispositionInfoEx) failed: %s", ctx);
    return -1;
}

// Result of opening a child entry by name relative to the parent directory handle.
//   OK    — *out_child is a valid handle the caller now owns.
//   RACED — entry vanished between enumeration and open; end state matches intent,
//           so caller should skip and continue.
//   ERROR — aeron_errmsg() is set; caller should propagate the failure.
typedef enum
{
    AERON_CHILD_OPEN_OK,
    AERON_CHILD_OPEN_RACED,
    AERON_CHILD_OPEN_ERROR
}
aeron_child_open_result_t;

// Open a child for delete and (when FileId is available) verify it's the same entry
// we enumerated. The parent handle anchors name resolution so absolute-path traversal
// isn't possible, and FILE_OPEN_REPARSE_POINT blocks symlink redirection — the FileId
// check closes the remaining hardlink / directory-swap window. Some file systems don't
// expose a FileId so the verification is silently skipped there.
static aeron_child_open_result_t aeron_open_child_for_delete(
    HANDLE parent, const aeron_nt_name_t *name, const char *ctx, HANDLE *out_child)
{
    UNICODE_STRING us;
    us.Length = name->length_bytes;
    us.MaximumLength = name->length_bytes;
    us.Buffer = (PWSTR)name->name;

    OBJECT_ATTRIBUTES oa;
    InitializeObjectAttributes(&oa, &us, OBJ_CASE_INSENSITIVE, parent, NULL);

    HANDLE child = NULL;
    IO_STATUS_BLOCK iosb = { 0 };
    NTSTATUS st = aeron_p_NtCreateFile(
        &child,
        AERON_DELETE_ACCESS,
        &oa,
        &iosb,
        NULL,
        0,
        AERON_DELETE_SHARE,
        AERON_FILE_OPEN,
        AERON_FILE_OPEN_FOR_BACKUP_INTENT | AERON_FILE_OPEN_REPARSE_POINT |
            AERON_FILE_SYNCHRONOUS_IO_NONALERT,
        NULL,
        0);

    if (!NT_SUCCESS(st))
    {
        if (AERON_STATUS_OBJECT_NAME_NOT_FOUND == st || AERON_STATUS_OBJECT_PATH_NOT_FOUND == st)
        {
            return AERON_CHILD_OPEN_RACED;
        }
        AERON_APPEND_ERR_WIN(aeron_p_RtlNtStatusToDosError(st),
            "NtCreateFile(delete) failed under: %s", ctx);
        return AERON_CHILD_OPEN_ERROR;
    }

    if (name->has_file_id)
    {
        BY_HANDLE_FILE_INFORMATION post_open;
        if (!GetFileInformationByHandle(child, &post_open))
        {
            AERON_APPEND_ERR_WIN(GetLastError(),
                "GetFileInformationByHandle (verify) failed under: %s", ctx);
            CloseHandle(child);
            return AERON_CHILD_OPEN_ERROR;
        }
        const ULONGLONG opened_id =
            ((ULONGLONG)post_open.nFileIndexHigh << 32) | post_open.nFileIndexLow;
        if (opened_id != (ULONGLONG)name->file_id.QuadPart)
        {
            aeron_set_errno(EAGAIN);
            AERON_APPEND_ERR(
                "directory entry replaced between enumerate and open under: %s", ctx);
            CloseHandle(child);
            return AERON_CHILD_OPEN_ERROR;
        }
    }

    *out_child = child;
    return AERON_CHILD_OPEN_OK;
}

// Recursively delete the entry referenced by `h`. Takes ownership of `h` and always
// closes it before returning, success or failure. Reparse points (symlinks, junctions,
// mount points) are treated as opaque leaves so we never traverse outside the subtree
// the caller asked us to delete.
static int aeron_recursively_delete_by_handle(HANDLE h, const char *ctx)
{
    FILE_BASIC_INFO basic;
    if (!GetFileInformationByHandleEx(h, FileBasicInfo, &basic, sizeof(basic)))
    {
        AERON_APPEND_ERR_WIN(GetLastError(), "GetFileInformationByHandleEx(Basic) failed: %s", ctx);
        CloseHandle(h);
        return -1;
    }

    const ULONG attrs = basic.FileAttributes;

    // Best-effort clear of READONLY so disposition delete can proceed; ignore failure here
    // since the disposition step will surface a clearer error later if the delete fails.
    if (attrs & FILE_ATTRIBUTE_READONLY)
    {
        FILE_BASIC_INFO clear = basic;
        clear.FileAttributes = attrs & ~FILE_ATTRIBUTE_READONLY;
        SetFileInformationByHandle(h, FileBasicInfo, &clear, sizeof(clear));
    }

    int rc = 0;
    if ((attrs & FILE_ATTRIBUTE_DIRECTORY) && !(attrs & FILE_ATTRIBUTE_REPARSE_POINT))
    {
        rc = aeron_delete_directory_contents(h, ctx);
    }

    // POSIX-semantics delete leaves the parent namespace synchronously even if a
    // FILE_SHARE_DELETE handle is held elsewhere. Skip on prior failure so the
    // "directory not empty" disposition error doesn't clobber the original cause.
    if (0 == rc && 0 != aeron_set_disposition_delete(h, ctx))
    {
        rc = -1;
    }
    CloseHandle(h);
    return rc;
}

// Enumerate `dir_handle` and recursively delete every entry inside it. Does not
// close `dir_handle` — the caller still owns it.
static int aeron_delete_directory_contents(HANDLE dir_handle, const char *ctx)
{
    if (0 != aeron_load_nt())
    {
        return -1;
    }

    aeron_nt_name_t *names = NULL;
    size_t count = 0;
    if (aeron_nt_collect_children(dir_handle, ctx, &names, &count) < 0)
    {
        return -1;
    }

    int rc = 0;
    for (size_t i = 0; i < count; i++)
    {
        HANDLE child;
        const aeron_child_open_result_t res = aeron_open_child_for_delete(dir_handle, &names[i], ctx, &child);
        if (AERON_CHILD_OPEN_RACED == res)
        {
            continue;
        }
        if (AERON_CHILD_OPEN_ERROR == res)
        {
            rc = -1;
            break;
        }
        if (0 != aeron_recursively_delete_by_handle(child, ctx))
        {
            rc = -1;
            break;
        }
    }

    aeron_free(names);
    return rc;
}

static int aeron_delete_path_internal(const char *path, DWORD extra_flags)
{
    HANDLE h = CreateFileA(
        path,
        AERON_DELETE_ACCESS,
        AERON_DELETE_SHARE,
        NULL,
        OPEN_EXISTING,
        FILE_FLAG_OPEN_REPARSE_POINT | extra_flags,
        NULL);

    if (INVALID_HANDLE_VALUE == h)
    {
        DWORD err = GetLastError();
        if (ERROR_FILE_NOT_FOUND == err || ERROR_PATH_NOT_FOUND == err)
        {
            return 0;
        }
        AERON_APPEND_ERR_WIN(err, "CreateFile(delete) failed: %s", path);
        return -1;
    }

    return aeron_recursively_delete_by_handle(h, path);
}

static int aeron_delete_file_internal(const char *path)
{
    return aeron_delete_path_internal(path, 0);
}

int aeron_unmap(aeron_mapped_file_t *mapped_file)
{
    if (NULL != mapped_file->addr)
    {
        return UnmapViewOfFile(mapped_file->addr) ? 0 : -1;
    }

    return 0;
}

int aeron_msync(void *addr, size_t length)
{
    if (NULL != addr && 0 == FlushViewOfFile(addr, length))
    {
        AERON_SET_ERR_WIN(GetLastError(), "%s", "FlushViewOfFile failed");
        return -1;
    }
    return 0;
}

int aeron_mkdir(const char *path, int permission)
{
    return _mkdir(path);
}

int64_t aeron_file_length(const char *path)
{
    WIN32_FILE_ATTRIBUTE_DATA fad;

    if (GetFileAttributesEx(path, GetFileExInfoStandard, &fad) == 0)
    {
        return -1;
    }

    ULARGE_INTEGER file_size;
    file_size.LowPart = fad.nFileSizeLow;
    file_size.HighPart = fad.nFileSizeHigh;

    return (int64_t)file_size.QuadPart;
}

uint64_t aeron_usable_fs_space(const char *path)
{
    ULARGE_INTEGER lpAvailableToCaller, lpTotalNumberOfBytes, lpTotalNumberOfFreeBytes;

    if (!GetDiskFreeSpaceEx(path, &lpAvailableToCaller, &lpTotalNumberOfBytes, &lpTotalNumberOfFreeBytes))
    {
        return 0;
    }

    return (uint64_t)lpAvailableToCaller.QuadPart;
}

int aeron_create_file(const char *path, size_t length, bool sparse_file)
{
    HANDLE hfile = CreateFile(
            path,
            FILE_GENERIC_READ | FILE_GENERIC_WRITE | DELETE,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            NULL,
            CREATE_NEW,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_POSIX_SEMANTICS,
            NULL);

    if (INVALID_HANDLE_VALUE == hfile)
    {
        AERON_SET_ERR_WIN(GetLastError(), "Failed to create file: %s", path);
        return -1;
    }

    if (sparse_file)
    {
        DWORD bytesReturned;
        if (!DeviceIoControl(hfile, FSCTL_SET_SPARSE, NULL, 0, NULL, 0, &bytesReturned, NULL))
        {
            AERON_SET_ERR_WIN(GetLastError(), "Failed to mark file as sparse: %s", path);
            goto error;
        }
    }

    LARGE_INTEGER file_size;
    file_size.QuadPart = (LONGLONG)length;

    if (!SetFilePointerEx(hfile, file_size, NULL, FILE_BEGIN) ||
        !SetEndOfFile(hfile))
    {
        AERON_SET_ERR_WIN(GetLastError(), "Failed to truncate file: %s", path);
        goto error;
    }

    int fd = _open_osfhandle((intptr_t)hfile, _O_RDWR);
    if (fd < 0)
    {
        AERON_SET_ERR_WIN(GetLastError(), "Failed to obtain file descriptor: %s", path);
        goto error;
    }

    return fd;

error:
    CloseHandle(hfile);
    if (-1 == aeron_delete_file_internal(path))
    {
        AERON_APPEND_ERR("(%d) Failed to remove file: %s", GetLastError(), path);
    }
    return -1;
}

int aeron_open_file_rw(const char *path)
{
    HANDLE hfile = CreateFile(
            path,
            FILE_GENERIC_READ | FILE_GENERIC_WRITE | DELETE,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            NULL,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_POSIX_SEMANTICS,
            NULL);

    if (INVALID_HANDLE_VALUE == hfile)
    {
        AERON_SET_ERR_WIN(GetLastError(), "Failed to open file: %s", path);
        return -1;
    }

    int fd = _open_osfhandle((intptr_t)hfile, _O_RDWR);
    if (fd < 0)
    {
        AERON_SET_ERR_WIN(GetLastError(), "Failed to obtain file descriptor: %s", path);
        return -1;
    }

    return fd;
}

int aeron_delete_directory(const char *dir)
{
    aeron_err_clear();
    return aeron_delete_path_internal(dir, FILE_FLAG_BACKUP_SEMANTICS);
}

int aeron_is_directory(const char *path)
{
    const DWORD attributes = GetFileAttributes(path);
    return INVALID_FILE_ATTRIBUTES != attributes && (attributes & FILE_ATTRIBUTE_DIRECTORY);
}

int aeron_delete_file(const char *path)
{
    aeron_err_clear();
    return aeron_delete_file_internal(path);
}

#else
#include <unistd.h>
#include <sys/mman.h>
#include <sys/statvfs.h>
#include <ftw.h>
#include <stdio.h>
#include <pwd.h>

static int aeron_mmap(aeron_mapped_file_t *mapping, int fd, bool pre_touch)
{
    int flags = MAP_SHARED;

#ifdef __linux__
    if (pre_touch)
    {
        flags = flags | MAP_POPULATE;
    }
#else
    (void)pre_touch;
#endif

    mapping->addr = mmap(NULL, mapping->length, PROT_READ | PROT_WRITE, flags, fd, 0);

    if (MAP_FAILED == mapping->addr)
    {
        AERON_SET_ERR(errno, "%s", "Failed to mmap");
        close(fd);
        return -1;
    }

    close(fd);
    return 0;
}

int aeron_unmap(aeron_mapped_file_t *mapped_file)
{
    if (NULL != mapped_file->addr)
    {
        return munmap(mapped_file->addr, mapped_file->length);
    }

    return 0;
}

int aeron_msync(void *addr, size_t length)
{
    if (NULL != addr && 0 != msync(addr, length, MS_SYNC | MS_INVALIDATE))
    {
        AERON_SET_ERR(errno, "%s", "msync failed");
        return -1;
    }
    return 0;
}

int aeron_delete_file(const char *path)
{
    return remove(path);
}

static int unlink_func(const char *path, const struct stat *sb, int type_flag, struct FTW *ftw)
{
    if (remove(path) != 0)
    {
        AERON_SET_ERR(errno, "could not remove %s", path);
        return -1;
    }

    return 0;
}

int aeron_delete_directory(const char *dirname)
{
    return nftw(dirname, unlink_func, 64, FTW_DEPTH | FTW_PHYS);
}

int aeron_is_directory(const char *dirname)
{
    struct stat sb;
    return stat(dirname, &sb) == 0 && S_ISDIR(sb.st_mode);
}

int64_t aeron_file_length(const char *path)
{
    struct stat sb;
    return stat(path, &sb) == 0 ? sb.st_size : -1;
}

uint64_t aeron_usable_fs_space(const char *path)
{
    struct statvfs vfs;
    uint64_t result = 0;

    if (statvfs(path, &vfs) == 0)
    {
        result = vfs.f_frsize * vfs.f_bavail;
    }

    return result;
}

int aeron_create_file(const char *path, size_t length, bool sparse_file)
{
    int fd = open(path, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
    if (fd < 0)
    {
        AERON_SET_ERR(errno, "Failed to create file: %s", path);
        return -1;
    }

    if (sparse_file)
    {
        if (0 != ftruncate(fd, (off_t)length))
        {
            AERON_SET_ERR(errno, "Failed to truncate file: %s", path);
            goto error;
        }
    }
    else
    {
#if HAVE_FALLOCATE
        if (0 != fallocate(fd, 0, 0, (off_t)length))
        {
            AERON_SET_ERR(errno, "Failed to allocate file space: %s", path);
            goto error;
        }
#elif HAVE_POSIX_FALLOCATE
        if (0 != posix_fallocate(fd, 0, (off_t)length))
        {
            AERON_SET_ERR(errno, "Failed to allocate file space: %s", path);
            goto error;
        }
#elif HAVE_F_PREALLOCATE
        fstore_t flags = {
            F_ALLOCATEALL,
            F_PEOFPOSMODE,
            0,
            (off_t)length,
            0};
        if (-1 == fcntl(fd, F_PREALLOCATE, &flags)) // changes physical file size
        {
            AERON_SET_ERR(errno, "Failed to allocate file space: %s", path);
            goto error;
        }

        if (0 != ftruncate(fd, (off_t)length)) // changes logical file size
        {
            AERON_SET_ERR(errno, "Failed to truncate file: %s", path);
            goto error;
        }
#else
        if (0 != ftruncate(fd, (off_t)length))
        {
            AERON_SET_ERR(errno, "Failed to truncate file: %s", path);
            goto error;
        }
#endif
    }

    return fd;

error:
    close(fd);
    if (-1 == remove(path))
    {
        AERON_APPEND_ERR("(%d) Failed to remove file", errno);
    }
    return -1;
}

int aeron_open_file_rw(const char *path)
{
    int fd = open(path, O_RDWR);
    if (-1 == fd)
    {
        AERON_SET_ERR(errno, "Failed to open file: %s", path);
        return -1;
    }
    return fd;
}
#endif

int aeron_mkdir_recursive(const char *pathname, int permission)
{
    if (aeron_mkdir(pathname, permission) == 0)
    {
        return 0;
    }

    if (errno != ENOENT)
    {
        AERON_SET_ERR(errno, "aeron_mkdir failed for %s", pathname);
        return -1;
    }

    char *_pathname = strdup(pathname);
    char *p;

    for (p = _pathname + strlen(_pathname) - 1; p != _pathname; p--)
    {
        if (*p == AERON_FILE_SEP)
        {
            *p = '\0';

            // _pathname is now the parent directory of the original pathname
            int rc = aeron_mkdir_recursive(_pathname, permission);

            free(_pathname);

            if (0 == rc)
            {
                // if rc is 0, then we were able to create the parent directory
                // so retry the original pathname
                return aeron_mkdir(pathname, permission);
            }
            else
            {
                AERON_APPEND_ERR("pathname=%s", pathname);
                return rc;
            }
        }
    }

    free(_pathname);

    AERON_SET_ERR(EINVAL, "aeron_mkdir_recursive failed to find parent directory in %s", pathname);

    return -1;
}

#include <inttypes.h>

#define AERON_BLOCK_SIZE (4 * 1024)

#ifndef AERON_NATIVE_PRETOUCH
static void aeron_touch_pages(volatile uint8_t *base, size_t length, size_t page_size)
{
    for (size_t i = 0; i < length; i += page_size)
    {
        volatile uint8_t *first_page_byte = base + i;
        *first_page_byte = 0;
    }
}
#endif

int aeron_map_new_file(aeron_mapped_file_t *mapped_file, const char *path, bool fill_with_zeroes)
{
    int fd = aeron_create_file(path, mapped_file->length, !fill_with_zeroes);
    if (-1 == fd)
    {
        return -1;
    }

    if (0 != aeron_mmap(mapped_file, fd, fill_with_zeroes))
    {
        AERON_APPEND_ERR("file: %s", path);
        if (-1 == remove(path))
        {
            AERON_APPEND_ERR("Failed to remove file: %s", path);
        }
        return -1;
    }

#ifndef AERON_NATIVE_PRETOUCH
    if (fill_with_zeroes)
    {
        aeron_touch_pages(mapped_file->addr, mapped_file->length, AERON_BLOCK_SIZE);
    }
#endif

    return 0;
}

int aeron_map_existing_file(aeron_mapped_file_t *mapped_file, const char *path)
{
    int fd = aeron_open_file_rw(path);
    if (fd < 0)
    {
        return -1;
    }

    const int64_t file_length = aeron_file_length(path);
    if (-1 == file_length)
    {
#if !defined(_MSC_VER)
        AERON_SET_ERR(errno, "Failed to determine the size of the file: %s", path);
        close(fd);
#else
        AERON_SET_ERR_WIN(GetLastError(), "Failed to determine the size of the file: %s", path);
        _close(fd);
#endif
        return -1;
    }

    mapped_file->length = (size_t)file_length;

    if (0 != aeron_mmap(mapped_file, fd, false))
    {
        AERON_APPEND_ERR("file: %s", path);
        return -1;
    }

    return 0;
}

uint64_t aeron_usable_fs_space_disabled(const char *path)
{
    return UINT64_MAX;
}

int aeron_ipc_publication_location(char *dst, size_t length, const char *aeron_dir, int64_t correlation_id)
{
    return snprintf(
        dst, length,
        "%s/" AERON_PUBLICATIONS_DIR "/%" PRId64 ".logbuffer",
        aeron_dir, correlation_id);
}

int aeron_network_publication_location(char *dst, size_t length, const char *aeron_dir, int64_t correlation_id)
{
    return snprintf(
        dst, length,
        "%s/" AERON_PUBLICATIONS_DIR "/%" PRId64 ".logbuffer",
        aeron_dir, correlation_id);
}

int aeron_publication_image_location(char *dst, size_t length, const char *aeron_dir, int64_t correlation_id)
{
    return snprintf(
        dst, length,
        "%s" AERON_FILE_SEP_STR AERON_IMAGES_DIR AERON_FILE_SEP_STR "%" PRId64 ".logbuffer",
        aeron_dir, correlation_id);
}

size_t aeron_temp_filename(char *filename, size_t length)
{
#if !defined(_MSC_VER)
    char rawname[] = "/tmp/aeron-c.XXXXXXX";
    int fd = mkstemp(rawname);
    close(fd);
    unlink(rawname);

    strncpy(filename, rawname, length);

    return strlen(filename);
#else
    char tmpdir[MAX_PATH + 1];
    char tmpfile[MAX_PATH];

    if (GetTempPath(MAX_PATH, &tmpdir[0]) > 0)
    {
        if (GetTempFileName(tmpdir, TEXT("aeron-c"), 101, &tmpfile[0]) != 0)
        {
            strncpy(filename, tmpfile, length);
            return strlen(filename);
        }
    }

    return 0;
#endif
}

int aeron_raw_log_map(
    aeron_mapped_raw_log_t *mapped_raw_log,
    const char *path,
    bool use_sparse_files,
    uint64_t term_length,
    uint64_t page_size)
{
    const uint64_t log_length = aeron_logbuffer_compute_log_length(term_length, page_size);

    int fd = aeron_create_file(path, (size_t)log_length, use_sparse_files);
    if (-1 == fd)
    {
        return -1;
    }

    mapped_raw_log->mapped_file.length = (size_t)log_length;
    mapped_raw_log->mapped_file.addr = NULL;

    if (0 != aeron_mmap(&mapped_raw_log->mapped_file, fd, !use_sparse_files))
    {
        AERON_APPEND_ERR("filename: %s", path);
        if (-1 == remove(path))
        {
            AERON_APPEND_ERR("Failed to remove raw log, filename: %s", path);
        }
        return -1;
    }

#ifndef AERON_NATIVE_PRETOUCH
    if (!use_sparse_files)
    {
        aeron_touch_pages(mapped_raw_log->mapped_file.addr, (size_t)log_length, (size_t)page_size);
    }
#endif

    for (size_t i = 0; i < AERON_LOGBUFFER_PARTITION_COUNT; i++)
    {
        mapped_raw_log->term_buffers[i].addr = (uint8_t *)mapped_raw_log->mapped_file.addr + (i * term_length);
        mapped_raw_log->term_buffers[i].length = (size_t)term_length;
    }

    mapped_raw_log->log_meta_data.addr =
        (uint8_t *)mapped_raw_log->mapped_file.addr + (log_length - AERON_LOGBUFFER_META_DATA_LENGTH);
    mapped_raw_log->log_meta_data.length = AERON_LOGBUFFER_META_DATA_LENGTH;
    mapped_raw_log->term_length = (size_t)term_length;

    return 0;
}

int aeron_raw_log_map_existing(aeron_mapped_raw_log_t *mapped_raw_log, const char *path, bool pre_touch)
{
    int fd = aeron_open_file_rw(path);
    if (fd < 0)
    {
        return -1;
    }

    const int64_t file_length = aeron_file_length(path);
    if (-1 == file_length)
    {
#if !defined(_MSC_VER)
        AERON_SET_ERR(errno, "Failed to determine the size of the existing raw log, filename: %s", path);
        close(fd);
#else
        AERON_SET_ERR_WIN(GetLastError(), "Failed to determine the size of the existing raw log, filename: %s", path);
        _close(fd);
#endif
        return -1;
    }

    mapped_raw_log->mapped_file.length = file_length;
    mapped_raw_log->mapped_file.addr = NULL;

    if (0 != aeron_mmap(&mapped_raw_log->mapped_file, fd, pre_touch))
    {
        AERON_APPEND_ERR("filename: %s", path);
        return -1;
    }

    mapped_raw_log->log_meta_data.addr =
        (uint8_t *)mapped_raw_log->mapped_file.addr +
        (mapped_raw_log->mapped_file.length - AERON_LOGBUFFER_META_DATA_LENGTH);
    mapped_raw_log->log_meta_data.length = AERON_LOGBUFFER_META_DATA_LENGTH;

    aeron_logbuffer_metadata_t *log_meta_data = (aeron_logbuffer_metadata_t *)mapped_raw_log->log_meta_data.addr;
    size_t term_length = (size_t)log_meta_data->term_length;
    size_t page_size = (size_t)log_meta_data->page_size;

    if (aeron_logbuffer_check_term_length(term_length) < 0 || aeron_logbuffer_check_page_size(page_size) < 0)
    {
        AERON_APPEND_ERR("Raw log metadata invalid, unmapping, filename: %s", path);
        aeron_unmap(&mapped_raw_log->mapped_file);
        return -1;
    }

    mapped_raw_log->term_length = term_length;

    for (size_t i = 0; i < AERON_LOGBUFFER_PARTITION_COUNT; i++)
    {
        mapped_raw_log->term_buffers[i].addr = (uint8_t *)mapped_raw_log->mapped_file.addr + (i * term_length);
        mapped_raw_log->term_buffers[i].length = term_length;
    }

#ifndef AERON_NATIVE_PRETOUCH
    if (pre_touch)
    {
        aeron_touch_pages(mapped_raw_log->mapped_file.addr, (size_t)file_length, (size_t)page_size);
    }
#endif

    return 0;
}

int aeron_raw_log_close(aeron_mapped_raw_log_t *mapped_raw_log, const char *filename)
{
    if (!aeron_raw_log_free(mapped_raw_log, filename))
    {
        AERON_SET_ERR(errno, "Failed to close raw log, filename: %s", filename);
        return -1;
    }

    return 0;
}

bool aeron_raw_log_free(aeron_mapped_raw_log_t *mapped_raw_log, const char *filename)
{
    if (NULL != mapped_raw_log->mapped_file.addr)
    {
        if (aeron_unmap(&mapped_raw_log->mapped_file) < 0)
        {
            return false;
        }

        mapped_raw_log->mapped_file.addr = NULL;
    }

    if (NULL != filename && mapped_raw_log->mapped_file.length > 0)
    {
        if (aeron_delete_file(filename) < 0 && aeron_file_length(filename) > 0)
        {
            return false;
        }

        mapped_raw_log->mapped_file.length = 0;
    }

    return true;
}

#if defined(__clang__)
    #pragma clang diagnostic push
    #pragma clang diagnostic ignored "-Wunused-function"
#endif

inline static const char *tmp_dir(void)
{
#if defined(_MSC_VER)
    static char buff[MAX_PATH + 1];

    if (GetTempPath(MAX_PATH, &buff[0]) > 0)
    {
        return buff;
    }

    return NULL;
#else
    const char *dir = "/tmp";
    const char *tmp_dir = getenv("TMPDIR");

    if (NULL != tmp_dir)
    {
        dir = tmp_dir;
    }

    return dir;
#endif
}

inline static bool has_file_separator_at_end(const char *path)
{
#if defined(_MSC_VER)
    const char last = path[strlen(path) - 1];
    return last == '\\' || last == '/';
#else
    return path[strlen(path) - 1] == '/';
#endif
}

#if defined(__clang__)
#pragma clang diagnostic pop
#endif

inline static const char *username(void)
{
#if (_MSC_VER)
    const char *username = getenv("USER");

    if (NULL == username)
    {
        username = getenv("USERNAME");
        if (NULL == username)
        {
             username = "default";
        }
    }

    return username;
#else
    static char static_buffer[16384];
    const char *username = getenv("USER");

    if (NULL == username)
    {
        uid_t uid = getuid(); // using uid instead of euid as that is what the JVM seems to do.
        struct passwd pw, *pw_result = NULL;

        int e = getpwuid_r(uid, &pw, static_buffer, sizeof(static_buffer), &pw_result);
        username = (0 == e && NULL != pw_result && NULL != pw_result->pw_name && '\0' != *(pw_result->pw_name)) ?
            pw_result->pw_name : "default";
    }

    return username;
#endif
}

int aeron_default_path(char *path, size_t path_length)
{
#if defined(__linux__)
    return snprintf(path, path_length, "/dev/shm/aeron-%s", username());
#elif defined(_MSC_VER)
    return snprintf(
        path, path_length, "%s%saeron-%s", tmp_dir(), has_file_separator_at_end(tmp_dir()) ? "" : "\\", username());
#else
    return snprintf(
        path, path_length, "%s%saeron-%s", tmp_dir(), has_file_separator_at_end(tmp_dir()) ? "" : "/", username());
#endif
}

int aeron_file_resolve(const char *parent, const char *child, char *buffer, size_t buffer_len)
{
    int result = snprintf(buffer, buffer_len, "%s%c%s", parent, AERON_FILE_SEP, child);
    buffer[buffer_len - 1] = '\0';

    if (result < 0)
    {
        AERON_SET_ERR(errno, "%s", "Failed to format resolved path");
        return -1;
    }
    else if ((int)buffer_len <= result)
    {
        AERON_SET_ERR(
            EINVAL,
            "Path name was truncated, required: %d, supplied: %d, result: %s",
            result,
            (int)buffer_len,
            buffer);
        return -1;
    }

    return result;
}
