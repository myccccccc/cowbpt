#ifndef STATUS_H
#define STATUS_H

#include <cassert>
#include "slice.h"

namespace cowbpt {

    class Status {
    public:
    // Create a success status.
    Status() : Status(kOk, "") {}
    ~Status() = default;

    Status(const Status& rhs) = default;
    Status& operator=(const Status& rhs) = default;

    Status(Status&& rhs) = default;
    Status& operator=(Status&& rhs)  = default;

    // Return a success status.
    static Status OK() { return Status(); }

    // Return error status of an appropriate type.
    static Status NotFound(const Slice& msg) {
        return Status(kNotFound, msg);
    }
    static Status Corruption(const Slice& msg) {
        return Status(kCorruption, msg);
    }
    static Status NotSupported(const Slice& msg) {
        return Status(kNotSupported, msg);
    }
    static Status InvalidArgument(const Slice& msg) {
        return Status(kInvalidArgument, msg);
    }
    static Status IOError(const Slice& msg) {
        return Status(kIOError, msg);
    }

    // Returns true iff the status indicates success.
    bool ok() const { return (_code == kOk); }

    // Returns true iff the status indicates a NotFound error.
    bool IsNotFound() const { return code() == kNotFound; }

    // Returns true iff the status indicates a Corruption error.
    bool IsCorruption() const { return code() == kCorruption; }

    // Returns true iff the status indicates an IOError.
    bool IsIOError() const { return code() == kIOError; }

    // Returns true iff the status indicates a NotSupportedError.
    bool IsNotSupportedError() const { return code() == kNotSupported; }

    // Returns true iff the status indicates an InvalidArgument.
    bool IsInvalidArgument() const { return code() == kInvalidArgument; }

    // Return a string representation of this status suitable for printing.
    // Returns the string "OK" for success.
    std::string string() const {
        std::string type;
        switch (code()) {
        case kOk:
            type = "OK";
            break;
        case kNotFound:
            type = "NotFound: ";
            break;
        case kCorruption:
            type = "Corruption: ";
            break;
        case kNotSupported:
            type = "Not implemented: ";
            break;
        case kInvalidArgument:
            type = "Invalid argument: ";
            break;
        case kIOError:
            type = "IO error: ";
            break;
        default:
            assert(false);
            break;
        }
        type.append(_msg.string());
        return type;
    }

    private:
    enum Code {
        kOk = 0,
        kNotFound = 1,
        kCorruption = 2,
        kNotSupported = 3,
        kInvalidArgument = 4,
        kIOError = 5
    };

    Code code() const {
        return _code;
    }

    Status(Code code, const Slice& msg) : _code(code), _msg(msg) {}

    Code _code;
    Slice _msg;
    };

}

#endif