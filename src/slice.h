#include <string>
#include <memory>


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
        
        const char* c_string() const {
            return _s->c_str();
        }
        
    private:
        std::shared_ptr<const std::string> _s;
    };

}

#endif