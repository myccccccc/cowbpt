#include <map>
#include <memory>
#include <mutex>
#include <cassert>


#ifndef NODE_H
#define NODE_H

namespace cowbpt {

// A COW node, it can be a leaf node or non-leaf node
// the maxium number of keys in the node should be a constant
template <typename Key, typename Value, typename Comparator = std::less<Key>>
class Node {
private:
    typedef std::shared_ptr<Value> ValuePtr;
    typedef std::map<Key, ValuePtr, Comparator> KVMap;
    typedef std::shared_ptr<KVMap> KVMapPtr;

public:
    Node();

    void put(const Key& k, ValuePtr v);
    ValuePtr get(const Key& k);
    void erase(const Key& k);

private:
    std::mutex _mutex; // when a shared ptr is accessed by mutiple threads, it needs external sync
    KVMapPtr _kvmap;
};

template <typename Key, typename Value, typename Comparator>
Node<Key, Value, Comparator>::Node()
: _kvmap(new KVMap) {

}

template <typename Key, typename Value, typename Comparator>
void Node<Key, Value, Comparator>::put(const Key& k, ValuePtr v) {
    std::lock_guard<std::mutex> lck(_mutex);
    if (!_kvmap.unique()) {
        _kvmap.reset(new KVMap(*_kvmap));
    }
    assert(_kvmap.unique());
    (*_kvmap)[k] = v;
}

template <typename Key, typename Value, typename Comparator>
typename Node<Key, Value, Comparator>::ValuePtr Node<Key, Value, Comparator>::get(const Key& k) {
    KVMapPtr kvmap;
    {
        std::lock_guard<std::mutex> lck(_mutex);
        kvmap = _kvmap;
        assert(!kvmap.unique());
    }
    if (kvmap->count(k) == 0) {
        return nullptr;
    }
    return (*kvmap)[k];
}

template <typename Key, typename Value, typename Comparator>
void Node<Key, Value, Comparator>::erase(const Key& k) {
    std::lock_guard<std::mutex> lck(_mutex);
    if (!_kvmap.unique()) {
        _kvmap.reset(new KVMap(*_kvmap));
    }
    assert(_kvmap.unique());
    _kvmap->erase(k);
}
}

#endif