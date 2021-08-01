#include "slice.h"

#ifndef COMPARATOR_H
#define COMPARATOR_H

namespace cowbpt {
    class Comparator {
    public:
        // return true if x is less than y
        virtual bool operator() (const Slice& x, const Slice& y) const = 0;
    };

    class SliceComparator : public Comparator {
    public:
        bool operator() (const Slice& x, const Slice& y) const override {
            return _cmp(x.string(), y.string());
        }
    private:
        std::less<std::string> _cmp;
    };
}

#endif