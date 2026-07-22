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
#ifndef AERON_UTIL_MEMORY_MAPPED_FILE_H
#define AERON_UTIL_MEMORY_MAPPED_FILE_H

#include <cstdint>
#include <memory>
#include <string>
#include "Exceptions.h"
extern "C"
{
#include "util/aeron_fileutil.h"
}

#ifdef _WIN32
    #include <cstddef>
    typedef void * HANDLE;
#else
    #include <sys/types.h>
#endif

namespace aeron { namespace util
{

class  MemoryMappedFile
{
public:
    typedef std::shared_ptr<MemoryMappedFile> ptr_t;

    static ptr_t createNew(const char *filename, std::size_t offset, std::size_t length, bool preTouch)
    {
        aeron_mapped_file_t mapped_file = {};
        mapped_file.length = offset + length;
        if (aeron_map_new_file(&mapped_file, filename, false) < 0)
        {
            throw IOException(
                std::string("failed to map new file: ") + filename + " " + aeron_errmsg(), SOURCEINFO);
        }

        mapped_file.addr = static_cast<void *>(static_cast<std::uint8_t *>(mapped_file.addr) + offset);
        return ptr_t(new MemoryMappedFile(mapped_file, false, preTouch));
    }

    static ptr_t mapExisting(
        const char *filename, std::size_t offset, std::size_t length, bool readOnly = false, bool preTouch = false)
    {
        aeron_mapped_file_t mapped_file = {};
        mapped_file.length = offset + length;

        if (aeron_map_existing_file(&mapped_file, filename, readOnly) < 0)
        {
            throw IOException(
                std::string("failed to open existing file: ") + filename + " " + aeron_errmsg(), SOURCEINFO);
        }

        mapped_file.addr = static_cast<void *>(static_cast<std::uint8_t *>(mapped_file.addr) + offset);
        return ptr_t(new MemoryMappedFile(mapped_file, readOnly, preTouch));
    }

    static ptr_t mapExisting(const char *filename, bool readOnly = false, bool preTouch = false);

    inline static ptr_t mapExistingReadOnly(const char *filename)
    {
        return mapExisting(filename, 0, 0, true, false);
    }

    ~MemoryMappedFile()
    {
        aeron_unmap(&m_mappedFile);
    }

    std::uint8_t *getMemoryPtr() const
    {
        return reinterpret_cast<uint8_t *>(m_mappedFile.addr);
    }
    std::size_t getMemorySize() const
    {
        return m_mappedFile.length;
    }

    MemoryMappedFile(MemoryMappedFile const &) = delete;
    MemoryMappedFile& operator=(MemoryMappedFile const &) = delete;

    static std::size_t getPageSize() noexcept
    {
        return 0;
    }
    static std::int64_t getFileSize(const char *filename)
    {
        return aeron_file_length(filename);
    }

private:
    struct FileHandle
    {
#ifdef _WIN32
        HANDLE handle;
#else
        int handle = -1;
#endif
    };

    MemoryMappedFile(aeron_mapped_file_t mappedFile, bool readOnly, bool preTouch) : m_mappedFile(mappedFile)
    {}

    aeron_mapped_file_t m_mappedFile;

    static std::size_t m_page_size;
    static bool fill(FileHandle fd, std::size_t sz, std::uint8_t value);

#ifdef _WIN32
    HANDLE m_file = nullptr;
    HANDLE m_mapping = nullptr;
    void cleanUp();
#endif

};

}}

#endif
