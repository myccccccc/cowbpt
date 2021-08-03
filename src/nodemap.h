#include <deque>
#include <algorithm>
#include <memory>
#include <vector>

#ifndef NODEMAP_H
#define NODEMAP_H

namespace cowbpt {

    template <typename Key, typename Value, typename Comparator>
    class LeafNodeMap {
    private:
        typedef std::deque<std::pair<Key, Value>> ArrayMap;
        LeafNodeMap(Comparator cmp, typename ArrayMap::iterator begin, typename ArrayMap::iterator end)
        : _v(begin, end),
          _cmp(cmp) {
        }
    public:
        LeafNodeMap(Comparator cmp) 
        : _v(),
          _cmp(cmp) {
        }
        size_t size() {
            return _v.size();
        }
        void put(const Key& k, const Value& v) {
            auto offset = find_greater_or_equal(k);
            if (offset < _v.size() && !_cmp(k, _v[offset].first) && !_cmp(_v[offset].first, k)) {
                // this is an update
                _v[offset] = std::make_pair(k, v);
            } else {
                // this is an insertion
                _v.insert(_v.begin()+offset, std::make_pair(k, v));
            }
        }
        void erase(const Key& k) {
            auto offset = find_greater_or_equal(k);
            if (offset < _v.size() && !_cmp(k, _v[offset].first) && !_cmp(_v[offset].first, k)) {
                // found k
                _v.erase(_v.begin() + offset);
            }
        }
        Value get(const Key& k) {
            auto offset = find_greater_or_equal(k);
            if (offset < _v.size() && !_cmp(k, _v[offset].first) && !_cmp(_v[offset].first, k)) {
                return _v[offset].second;
            } else {
                return Value();
            }
        }; 
        LeafNodeMap<Key, Value, Comparator>* split(Key& k) {
            auto offset = _v.size() / 2;
            k = _v[offset].first;
            auto right_split_node = new LeafNodeMap(_cmp, _v.begin() + offset, _v.end());
            _v.erase(_v.begin() + offset, _v.end());
            return right_split_node;
        }
        LeafNodeMap<Key, Value, Comparator>* copy() {
            return new LeafNodeMap(_cmp, _v.begin(), _v.end());
        }
        std::pair<Key, Value> pop_first_leaf_node_value_and_second_key(Key& first_key) {
            assert(size() >= 2);
            auto p = _v.front();
            _v.pop_front();
            first_key = p.first;
            return std::make_pair(_v.front().first, p.second);
        }
        std::pair<Key, Value> pop_last_leaf_node_value_and_last_key() {
            assert(size() >= 2);
            auto p = _v.back();
            _v.pop_back();
            return p;
        }
        void append_right(LeafNodeMap<Key, Value, Comparator>* right) {
            _v.insert(_v.end(), right->_v.begin(), right->_v.end());
            right->_v.clear();
        }

        std::string dump() {
            std::string s;
            s.append("-----------------------------------------\n");
            for(size_t i = 0; i < _v.size(); i++) {
                s.append("| K: "+_v[i].first.string()+" V: "+_v[i].second.string()+" |\n");
            }
            s.append("-----------------------------------------\n");
            return s;
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
        const Comparator _cmp;
    };

    template <typename Key, typename Value, typename Comparator>
    class InternalNodeMap {
    private:
        typedef std::deque<std::pair<Key, Value>> ArrayMap;
        InternalNodeMap(Comparator cmp, typename ArrayMap::iterator begin, typename ArrayMap::iterator end)
        : _v(begin, end),
          _cmp(cmp) {
        }
    public:
        InternalNodeMap() = delete;
        InternalNodeMap(Comparator cmp, Value v1, 
                        const Key& k2, Value v2)
        : _v(),
          _cmp(cmp) {
            _v.push_back(std::make_pair(Key(), v1));
            _v.push_back(std::make_pair(k2, v2));
        }
        size_t size() {
            return _v.size();
        }

        std::vector<Value> get_values() {
            std::vector<Value> res;
            for(auto p : _v) {
                res.push_back(p.second);
            }
            return res;
        }

        void put(const Key& k, const Value& v) {
            auto offset = find_greater_or_equal(k);
            assert(size() == 1 || offset == size() ||_cmp(k, _v[offset].first));
            // this is an insertion
            _v.insert(_v.begin()+offset, std::make_pair(k, v));
        }
        // push this internalnodevalue to the front of is node, and the previous front key is set to right_k
        void push_front(const Value& v, const Key& right_k) {
           assert(size() > 0);
           _v[0].first = right_k;
           _v.push_front(std::make_pair(Key(), v)); 
        }
        void erase(const Key& k) {
            auto offset = find_greater_or_equal(k);
            // internal node will not delete a not existing key
            assert(offset < _v.size() && !_cmp(k, _v[offset].first) && !_cmp(_v[offset].first, k));
            _v.erase(_v.begin() + offset);
        }
        Value get(const Key& k) {
            if (size() == 0) {
                return nullptr;
            }
            auto offset = find_greater_or_equal(k);
            if (size() > 1 && offset < size() && !_cmp(k, _v[offset].first) && !_cmp(_v[offset].first, k)) {
                return _v[offset].second;
            }
            return _v[offset-1].second;
        }


        std::pair<Key, Value> get_middle(const Key& k) {
            auto offset = find_greater_or_equal(k);
            if (size() > 1 && offset < size() && !_cmp(k, _v[offset].first) && !_cmp(_v[offset].first, k)) {
                return _v[offset];
            } else {
                return _v[offset-1];
            }
        }
        // return the node and its corresponding key, that is at the right of the node which might contains k down below
        // return nullptr if don't have right node
        std::pair<Key, Value> get_right(const Key& k) {
            size_t i;
            auto offset = find_greater_or_equal(k);
            if (size() > 1 && offset < size() && !_cmp(k, _v[offset].first) && !_cmp(_v[offset].first, k)) {
                i = offset + 1;
            } else {
                i = offset-1 + 1;
            }
            assert(i <= size());
            if (i == size()) {
                return std::make_pair(Key(), nullptr);
            }
            return _v[i];
        }
        // return the node and its corresponding key, that is at the left of the node which might contains k down below
        // return nullptr if don't have left node (impossible)
        std::pair<Key, Value> get_left(const Key& k) {
            size_t i;
            auto offset = find_greater_or_equal(k);
            if (size() > 1 && offset < size() && !_cmp(k, _v[offset].first) && !_cmp(_v[offset].first, k)) {
                i = offset - 1;
            } else {
                i = offset-1 - 1;
            }
            assert(size() >= 0 && i >= 0);
            return _v[i];
        }



        InternalNodeMap<Key, Value, Comparator>* split(Key& k) {
            auto offset = _v.size() / 2;
            k = _v[offset].first;
            auto right_split_node = new InternalNodeMap(_cmp, _v.begin() + offset, _v.end());
            _v.erase(_v.begin() + offset, _v.end());
            return right_split_node;
        }
        InternalNodeMap<Key, Value, Comparator>* copy() {
            return new InternalNodeMap(_cmp, _v.begin(), _v.end());
        }
        std::pair<Key, Value> pop_first_internal_node_value_and_second_key() {
            assert(size() >= 2);
            auto p = _v.front();
            _v.pop_front();
            return std::make_pair(_v.front().first, p.second);
        }
        std::pair<Key, Value> pop_last_internal_node_value_and_last_key() {
            assert(size() >= 2);
            auto p = _v.back();
            _v.pop_back();
            return p;
        }
        virtual void append_right(InternalNodeMap<Key, Value, Comparator>* right, Key right_k) {
            auto offset = _v.size();
            _v.insert(_v.end(), right->_v.begin(), right->_v.end());
            _v[offset].first = right_k;
            right->_v.clear();
        }

        std::string dump() {
            std::string s;
            for(size_t i = 0; i < _v.size(); i++) {
                s.append(_v[i].second->dump());
            }
            s.append("-----------------------------------------\n");
            for(size_t i = 0; i < _v.size(); i++) {
                s.append("| K: "+_v[i].first.string()+" P: "+" |\n");
            }
            s.append("-----------------------------------------\n");
            return s;
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
        const Comparator _cmp;
    };
}

#endif