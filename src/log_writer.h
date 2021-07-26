#include <memory>
#include "status.h"
#include "slice.h"
#include "log_format.h"

#ifndef LOG_WRITER_H
#define LOG_WRITER_H

namespace cowbpt {
    class WritableFile;

    namespace log {

    class Writer {
    private:
        typedef std::shared_ptr<WritableFile> WritableFilePtr;

    public:
    // Create a writer that will append data to "*dest".
    // "*dest" must be initially empty.
    // "*dest" must remain live while this Writer is in use.
    explicit Writer(WritableFilePtr dest);

    // Create a writer that will append data to "*dest".
    // "*dest" must have initial length "dest_length".
    // "*dest" must remain live while this Writer is in use.
    Writer(WritableFilePtr dest, uint64_t dest_length);

    Writer(const Writer&) = delete;
    Writer& operator=(const Writer&) = delete;

    ~Writer();

    Status AddRecord(const Slice& slice);

    private:
    Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

    WritableFilePtr dest_;
    int block_offset_;  // Current offset in block

    // crc32c values for all supported record types.  These are
    // pre-computed to reduce the overhead of computing the crc of the
    // record type stored in the header.
    uint32_t type_crc_[kMaxRecordType + 1];
    };

    }  // namespace log
}
#endif