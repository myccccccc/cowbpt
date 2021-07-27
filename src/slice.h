#include <string>
#include <memory>
#include <cassert>


#ifndef SLICE_H
#define SLICE_H

#define EMPTYSLICE Slice()

namespace cowbpt {
    class Slice {
    public:
        Slice(const Slice& s) = default;
        Slice(Slice&& s) = default;
        Slice& operator = (const Slice& s) = default;
        Slice& operator = (Slice&& s) = default;

        Slice(const char* c_string) : _s(std::make_shared<const std::string>(c_string)) {};
        Slice(const char* c_string, size_t n) : _s(std::make_shared<const std::string>(c_string, n)) {};

        Slice(std::string&& s): _s(std::make_shared<const std::string>(std::move(s))) {};
        Slice(const std::string& s): _s(std::make_shared<const std::string>(s)) {};

        Slice() : _s(std::make_shared<const std::string>()) {}; // empty slice

        bool empty() const { return _s->empty(); }

        const std::string string() const {
            return *_s;
        }
        std::shared_ptr<const std::string> stringPtr() const {
            return _s; 
        }

        size_t size() const {
            return _s->size();
        }
        
        //TODO: this api should be deleted and replced with stringPtr() instead
        const char* c_string() const {
            auto s = new std::string(*_s);
            return s->c_str(); // TODO: mem leak here
        }

        void remove_prefix(size_t n) {
            assert(n <= size());
            _s.reset(new std::string(_s->begin()+n, _s->end()));
        }
        
        void clear() {
            _s.reset(new std::string());
        }
        
    private:
        std::shared_ptr<const std::string> _s;
    };

}

#endif