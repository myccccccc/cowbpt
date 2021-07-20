#include <memory>
#include <mutex>
#include <cassert>
#include <atomic>
#include "nodemap.h"
#include "gflags/gflags.h"


#ifndef NODE_H
#define NODE_H

namespace cowbpt {

DECLARE_int32(COWBPT_NODE_B_SZIE);

enum NodeType {
    internal,
    leaf
};

// A copy on write node impl, it can be a leaf node or non-leaf node
// it allows current reads without external sync
// and use lock coupling for write
// the maxium number of keys in a node should be a constant
template <typename Key, typename Value, typename Comparator = std::less<Key>>
class Node {
public:
    typedef Node Type;
protected:
    typedef std::shared_ptr<Node> NodePtr;
    typedef std::shared_ptr<Value> ValuePtr;
    typedef NodeMap<Key, ValuePtr, Comparator> KVMap;
    typedef std::shared_ptr<KVMap> KVMapPtr;

public:
    Node() = delete;
    virtual ~Node() = default;

    virtual NodeType type() = 0;
    void lock() {
        _mutex.lock();
    }
    void unlock() {
        _mutex.unlock();
    }
    bool check_version(int node_version) {
        return node_version == _version.load(std::memory_order_acquire);
    }
    // need to hold the lock (lock coupling) before call need_split
    bool need_split() {
        // TODO: assert _mutex is locked
        assert(_kvmap->size() <= 2 * FLAGS_COWBPT_NODE_B_SZIE + 1);
        return _kvmap->size() == 2 * FLAGS_COWBPT_NODE_B_SZIE + 1;
    }
    // need to hold the lock (lock coupling) before call need_fix
    bool need_fix() {
        // TODO: assert _mutex is locked
        assert(_kvmap->size() >= FLAGS_COWBPT_NODE_B_SZIE);
        return _kvmap->size() == FLAGS_COWBPT_NODE_B_SZIE;
    }
    // if this is a leaf node, get reutrns the pointer to the value if target key exist, otherwise nullptr
    // if this is an internal node, get returns the child node that may contains the target key
    // need to check this node's parent node's version after searching this node
    ValuePtr get(const Key& k, int& node_version);
    // need to hold the lock (lock coupling) before call put
    void put(const Key& k, ValuePtr v);
    // return a poniter to the right half split of the node
    // k is set to the first key in the right half split of the node
    // need to hold the lock (lock coupling) before call split
    virtual NodePtr split(Key& k) = 0;
    // TODO: delete and copy
protected:
    Node(KVMapPtr kvmap);
    std::mutex _mutex; // when a shared ptr is accessed by mutiple threads, it needs external sync
    std::atomic<int> _version; // the node version will increment 1 for every write on this node
    KVMapPtr _kvmap;
};

template <typename Key, typename Value, typename Comparator>
Node<Key, Value, Comparator>::Node(KVMapPtr kvmap)
: _kvmap(kvmap),
  _version(1) {

}

template <typename Key, typename Value, typename Comparator>
void Node<Key, Value, Comparator>::put(const Key& k, ValuePtr v) {
    // TODO: assert _mutex is locked
    if (!_kvmap.unique()) {
        _kvmap.reset(_kvmap->copy());
    }
    assert(_kvmap.unique());
    _kvmap->put(k, v);
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
    return kvmap->get(k);
}

template <typename Key, typename Value, typename Comparator = std::less<Key>>
class LeafNode : public Node<Key, Value, Comparator> {
private:
    using typename Node<Key, Value, Comparator>::NodePtr;
    using typename Node<Key, Value, Comparator>::ValuePtr;
    using typename Node<Key, Value, Comparator>::KVMap;
    using typename Node<Key, Value, Comparator>::KVMapPtr;
    typedef LeafNodeMap<Key, ValuePtr, Comparator> LeafKVMap;
public:
    LeafNode()
    : Node<Key, Value, Comparator>(KVMapPtr(new LeafKVMap)){
    }
    virtual NodeType type() override {
        return NodeType::leaf;
    }
    NodePtr split(Key& k) override {
        // TODO: assert _mutex is locked
        KVMapPtr rhs_kv_map(Node<Key, Value, Comparator>::_kvmap->split(k));
        NodePtr p(new LeafNode<Key, Value, Comparator>(rhs_kv_map));
        return p;
    }
private:
    LeafNode(KVMapPtr p)
    : Node<Key, Value, Comparator>(p) {

    }
};

template <typename Key, typename Value, typename Comparator = std::less<Key>>
class InternalNode : public Node<Key, Value, Comparator> {
private:
    using typename Node<Key, Value, Comparator>::NodePtr;
    using typename Node<Key, Value, Comparator>::ValuePtr;
    using typename Node<Key, Value, Comparator>::KVMap;
    using typename Node<Key, Value, Comparator>::KVMapPtr;
    typedef InternalNodeMap<Key, ValuePtr, Comparator> InternalKVMap;
public:
    InternalNode() = delete;
    InternalNode(ValuePtr v1, const Key& k2, ValuePtr v2) 
    : Node<Key, Value, Comparator>(KVMapPtr(new InternalKVMap(v1, k2, v2))){

    }
    virtual NodeType type() override {
        return NodeType::internal;
    }
    NodePtr split(Key& k) override {
        // TODO: assert _mutex is locked
        KVMapPtr rhs_kv_map(Node<Key, Value, Comparator>::_kvmap->split(k));
        NodePtr p(new InternalNode<Key, Value, Comparator>(rhs_kv_map));
        return p;
    }
private:
    InternalNode(KVMapPtr p)
    : Node<Key, Value, Comparator>(p) {

    }
};

}

#endif