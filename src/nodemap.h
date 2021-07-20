#include <vector>
#include <algorithm>
#include <memory>

#ifndef NODEMAP_H
#define NODEMAP_H

namespace cowbpt {
    // key-value map inside the internal node or leaf node
    template <typename Key, typename Value, typename Comparator = std::less<Key>>
    class NodeMap {
    public:
        virtual ~NodeMap() = default;
        // return the size of the nodemap, not thread safe
        virtual size_t size() = 0;
        // if this is a leaf node update(alread exist) or insert(not exist) a key value pair
        // if this is an internal node insert a key value pair
        // not thread safe
        virtual void put(const Key& k, const Value& v) = 0; 
        // if this is an internal node map, get returns a pointer to the child node that may contains Key down below
        // if this is a leaf node map, get returns a pointer to the value if Key exists, otherwise nullptr
        // not thread safe 
        virtual Value get(const Key& k) = 0;
        // split half of this node's key-value pairs to another nodemap, 
        // k is set to be the first key at the right half split
        // not thread safe 
        virtual NodeMap* split(Key& k) = 0;
        // copy
        // not thread safe
        virtual NodeMap* copy() = 0;
        // TODO: delete
    };

    template <typename Key, typename Value, typename Comparator = std::less<Key>>
    class LeafNodeMap : public NodeMap<Key, Value, Comparator> {
    private:
        typedef std::vector<std::pair<Key, Value>> ArrayMap;
        LeafNodeMap(typename ArrayMap::iterator begin, typename ArrayMap::iterator end)
        : _v(begin, end) {
        }
    public:
        LeafNodeMap() = default;
        size_t size() override {
            return _v.size();
        }
        void put(const Key& k, const Value& v) override {
            auto offset = find_greater_or_equal(k);
            if (offset < _v.size() && !_cmp(k, _v[offset].first) && !_cmp(_v[offset].first, k)) {
                // this is an update
                _v[offset] = std::make_pair(k, v);
            } else {
                // this is an insertion
                _v.insert(_v.begin()+offset, std::make_pair(k, v));
            }
        }
        Value get(const Key& k) override {
            auto offset = find_greater_or_equal(k);
            if (offset < _v.size() && !_cmp(k, _v[offset].first) && !_cmp(_v[offset].first, k)) {
                return _v[offset].second;
            } else {
                return nullptr;
            }
        }; 
        NodeMap<Key, Value, Comparator>* split(Key& k) override {
            auto offset = _v.size() / 2;
            k = _v[offset].first;
            auto right_split_node = new LeafNodeMap(_v.begin() + offset, _v.end());
            _v.erase(_v.begin() + offset, _v.end());
            return right_split_node;
        }
        NodeMap<Key, Value, Comparator>* copy() override {
            return new LeafNodeMap(_v.begin(), _v.end());
        }
    private:
        // find the offset of _v where _v[offset] is greater or equal to k
        // return _v length if not found
        size_t find_greater_or_equal(const Key& k) {
            // TODO: use binary search
            size_t i = 0;
            for (; i < _v.size(); i++) {
                if (_cmp(_v[i].first, k)) {
                    continue;
                }
                break;
            }
            return i;
        }

        ArrayMap _v; // sorted by Key
        Comparator _cmp;
    };

    template <typename Key, typename Value, typename Comparator = std::less<Key>>
    class InternalNodeMap : public NodeMap<Key, Value, Comparator> {
    private:
        typedef std::vector<std::pair<Key, Value>> ArrayMap;
        InternalNodeMap(typename ArrayMap::iterator begin, typename ArrayMap::iterator end)
        : _v(begin, end) {
        }
    public:
        InternalNodeMap() = delete;
        InternalNodeMap(Value v1, 
                        const Key& k2, Value v2)
        : _v() {
            _v.push_back(std::make_pair(Key(), v1));
            _v.push_back(std::make_pair(k2, v2));
        }
        size_t size() override {
            return _v.size();
        }
        void put(const Key& k, const Value& v) override {
            auto offset = find_greater_or_equal(k);
            assert(size() == 1 || _cmp(k, _v[offset].first));
            // this is an insertion
            _v.insert(_v.begin()+offset, std::make_pair(k, v));
        }
        Value get(const Key& k) override {
            auto offset = find_greater_or_equal(k);
            if (size() > 1 && !_cmp(k, _v[offset].first) && !_cmp(_v[offset].first, k)) {
                return _v[offset].second;
            }
            return _v[offset-1].second;
        }
        NodeMap<Key, Value, Comparator>* split(Key& k) override {
            auto offset = _v.size() / 2;
            k = _v[offset].first;
            auto right_split_node = new InternalNodeMap(_v.begin() + offset, _v.end());
            _v.erase(_v.begin() + offset, _v.end());
            return right_split_node;
        }
        NodeMap<Key, Value, Comparator>* copy() override {
            return new InternalNodeMap(_v.begin(), _v.end());
        }
    private:
        // find the offset of _v where _v[offset]'s child node may contains Key down below
        size_t find_greater_or_equal(const Key& k) {
            assert(size() >= 1);
            // TODO: use binary search
            size_t i = 1;
            // we don't check i = 0 for internal node map, this is different from leaf node map
            // since in the leaf node _v[0] represent the exact key-value pair
            // however int internal node _v[0] represent any keys that are smaller than _v[1]
            // or the whole key space that belongs to this node if _v[1] does not exist
            for (; i < _v.size(); i++) {
                if (_cmp(_v[i].first, k)) {
                    continue;
                }
                break;
            }
            return i;
        }

        ArrayMap _v; // sorted by Key
        Comparator _cmp;
    };
}

#endif