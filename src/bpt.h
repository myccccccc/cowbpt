#include <memory>

#include "comparator.h"
#include "slice.h"
#include "node.h"

#ifndef BPT_H
#define BPT_H

namespace cowbpt {

    class Bpt {
    private:
        struct BptComparator {
        public:
            BptComparator(Comparator* user_comparator)
            : _user_comparator(user_comparator){

            }
        private:
            Comparator* _user_comparator;
        public:
            bool operator() (const Slice& x, const Slice& y) const {return _user_comparator->operator()(x, y);}
        };

        typedef std::shared_ptr<Node<BptComparator>> NodePtr;
        
    public:
        Bpt(Comparator* user_comparator);

        Bpt(const Bpt& ) = delete;
        Bpt& operator = (const Bpt& ) = delete;
        ~Bpt() = default;
        
        void put(const Slice& key, const Slice& value);
        Slice get(const Slice& key); // return an empty slice if key do not exist

        std::string dump();
    
    private:
        std::mutex _mutex; // _root is a shared pointer, need to be protected when it is being read and write currently;
        BptComparator _cmp;
        NodePtr _root;
    };

}

#endif