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

        Slice(const char* c_string) : _s(std::make_shared<const std::string>(c_string)), _pos(0), _len(_s->size()) {};
        Slice(const char* c_string, size_t n) : _s(std::make_shared<const std::string>(c_string, n)), _pos(0), _len(_s->size()) {};

        Slice(std::string&& s): _s(std::make_shared<const std::string>(std::move(s))), _pos(0), _len(_s->size()) {};
        Slice(const std::string& s): _s(std::make_shared<const std::string>(s)), _pos(0), _len(_s->size()) {};

        Slice() : _s(std::make_shared<const std::string>()), _pos(0), _len(_s->size()) {}; // empty slice

        Slice(const Slice& s, size_t len) : _s(s._s), _pos(s._pos), _len(len) { assert(len <= s.size()); }

        bool empty() const { 
            return _len == 0; 
        }

        const std::string string() const {
            return std::string(_s->begin()+_pos, _s->begin()+_pos+_len);
        }

        size_t size() const {
            return _len;
        }
        
        // dangerous api, the Slice instance calling this function, must out lives the returnd char* value
        // use this api very calfully
        const char* c_string() const {
            return _s->c_str() + _pos; 
        }

        void remove_prefix(size_t n) {
            assert(n <= _len);
            _pos += n;
            _len -= n;
        }
        
        void clear() {
            _s.reset(new std::string());
            _pos = 0;
            _len = 0;
        }
        
        char operator[](size_t n) const {
            assert(n < _len);
            return _s->c_str()[n + _pos];
        }
        
    private:
        std::shared_ptr<const std::string> _s;
        size_t _pos;
        size_t _len;
    };

}

#endif