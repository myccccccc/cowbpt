#include <string>
#include <memory>


#ifndef SLICE_H
#define SLICE_H

#define EMPTYSLICE Slice()

namespace cowbpt {
    class Slice {
    friend class SliceComparator;
    public:
        Slice(const Slice& s) = default;
        Slice(const char* c_string) : _s(std::make_shared<const std::string>(c_string)) {};
        Slice() : _s(std::make_shared<const std::string>()) {}; // empty slice
        bool empty() { return _s->empty(); }
        std::string string() {
            return *_s;
        }

    private:
        std::shared_ptr<const std::string> _s;
    };

    class SliceComparator {
    public:
        bool operator() (const Slice& x, const Slice& y) const {return _cmp(*(x._s), *(y._s));}
    private:
        std::less<std::string> _cmp;
    };
}

#endif