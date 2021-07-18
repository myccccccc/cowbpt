#include <map>
#include <memory>
#include <mutex>
#include <cassert>
#include <atomic>
#include "gflags/gflags.h"


#ifndef NODE_H
#define NODE_H

namespace cowbpt {

DECLARE_int32(COWBPT_NODE_B_SZIE);

// A COW node impl, it can be a leaf node or non-leaf node
// it allows current reads and writes without external sync
// the maxium number of keys in the node should be a constant
template <typename Key, typename Value, typename Comparator = std::less<Key>>
class Node {
private:
    typedef std::shared_ptr<Node> NodePtr;
    typedef std::shared_ptr<Value> ValuePtr;
    typedef std::map<Key, ValuePtr, Comparator> KVMap;
    typedef std::shared_ptr<KVMap> KVMapPtr;

public:
    Node();

    void put(const Key& k, ValuePtr v);
    ValuePtr get(const Key& k, int& node_version);
    void erase(const Key& k);
    NodePtr clone();
    bool check_version(int node_version) {
        return node_version == _version.load(std::memory_order_acquire);
    }

private:
    Node(KVMapPtr kvmap);
    std::mutex _mutex; // when a shared ptr is accessed by mutiple threads, it needs external sync
    std::atomic<int> _version; // the node version will increment 1 for every write on this node
    KVMapPtr _kvmap;
};

template <typename Key, typename Value, typename Comparator>
Node<Key, Value, Comparator>::Node()
: _kvmap(new KVMap),
  _version(1) {

}

template <typename Key, typename Value, typename Comparator>
Node<Key, Value, Comparator>::Node(KVMapPtr kvmap)
: _kvmap(kvmap),
  _version(1) {

}

template <typename Key, typename Value, typename Comparator>
void Node<Key, Value, Comparator>::put(const Key& k, ValuePtr v) {
    std::lock_guard<std::mutex> lck(_mutex);
    if (!_kvmap.unique()) {
        _kvmap.reset(new KVMap(*_kvmap));
    }
    assert(_kvmap.unique());
    (*_kvmap)[k] = v;
    _version.fetch_add(1, std::memory_order_release);
}

template <typename Key, typename Value, typename Comparator>
typename Node<Key, Value, Comparator>::ValuePtr Node<Key, Value, Comparator>::get(const Key& k, int& node_version) {
    KVMapPtr kvmap;
    {
        std::lock_guard<std::mutex> lck(_mutex);
        kvmap = _kvmap;
        assert(!kvmap.unique());
        node_version = _version.load(std::memory_order_acquire);
    }
    auto iter = kvmap->find(k);
    if (iter == kvmap->end()) {
        return nullptr;
    }
    return iter->second;
}

template <typename Key, typename Value, typename Comparator>
void Node<Key, Value, Comparator>::erase(const Key& k) {
    std::lock_guard<std::mutex> lck(_mutex);
    if (!_kvmap.unique()) {
        _kvmap.reset(new KVMap(*_kvmap));
    }
    assert(_kvmap.unique());
    _kvmap->erase(k);
    _version.fetch_add(1, std::memory_order_release);
}

template <typename Key, typename Value, typename Comparator>
typename Node<Key, Value, Comparator>::NodePtr Node<Key, Value, Comparator>::clone() {
    KVMapPtr kvmap;
    {
        std::lock_guard<std::mutex> lck(_mutex);
        kvmap = _kvmap;
        assert(!kvmap.unique());
    }
    NodePtr cloned_node(new Node(kvmap));
    return cloned_node;
}
}

#endif