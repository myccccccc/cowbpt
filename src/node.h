#include <memory>
#include <mutex>
#include <cassert>
#include <atomic>
#include "slice.h"
#include "nodemap.h"
#include "gflags/gflags.h"


#ifndef NODE_H
#define NODE_H

namespace cowbpt {

DECLARE_int32(COWBPT_NODE_B_SZIE);

// A copy on write node impl, it can be a leaf node or an internal node
// it allows current reads without external sync
// and use lock coupling for write
// the maxium number of keys in a node should be a constant
template <typename Comparator>
class Node {
protected:
    typedef Slice Key;
    typedef std::shared_ptr<Node> NodePtr;
    typedef NodePtr InternalNodeValue;
    typedef Slice LeafNodeValue;

public:
    Node(Comparator cmp)
    : _version(1),
      _mutex(),
      _cmp(cmp) {

    }
    virtual ~Node() = default;

    virtual bool is_leafnode() = 0;
    virtual bool is_internalnode() = 0;

    void lock() {
        _mutex.lock();
    }
    void unlock() {
        _mutex.unlock();
    }

    bool check_version(int node_version) {
        return node_version == _version.load(std::memory_order_acquire);
    }

    // return the number of key-value pairs in the node,
    // not thread safe
    virtual size_t size() = 0;

    // need to hold the lock (lock coupling) before call need_split
    bool need_split() {
        // TODO: assert _mutex is locked
        assert(size() <= 2 * FLAGS_COWBPT_NODE_B_SZIE + 1);
        return size() == 2 * FLAGS_COWBPT_NODE_B_SZIE + 1;
    }

    // need to hold the lock (lock coupling) before call need_fix
    bool need_fix() {
        // TODO: assert _mutex is locked
        assert(size() >= FLAGS_COWBPT_NODE_B_SZIE);
        return size() == FLAGS_COWBPT_NODE_B_SZIE;
    }

    // if this is a leaf node, panic
    // if this is an internal node, get returns the child node that may contains the target key
    // need to check this node's parent node's version after searching this node
    virtual InternalNodeValue get_internalnode_value(const Key& k, int& node_version) = 0;

    // if this is a leaf node, get reutrns the pointer to the value if target key exist, otherwise nullptr
    // if this is an internal node, panic
    // need to check this node's parent node's version after searching this node
    virtual LeafNodeValue get_leafnode_value(const Key& k, int& node_version) = 0;

    // need to hold the lock (lock coupling) before call put
    // if this is a leaf node, panic
    virtual void put(const Key& k, InternalNodeValue v) = 0;

    // need to hold the lock (lock coupling) before call put
    // if this is an internal node, panic
    virtual void put(const Key& k, LeafNodeValue v) = 0;


    // return a poniter to the right half split of the node
    // k is set to the first key in the right half split of the node
    // need to hold the lock (lock coupling) before call split
    virtual NodePtr split(Key& k) = 0;

    // TODO: delete and copy

protected:
    std::mutex _mutex; // when a shared ptr is accessed by mutiple threads, it needs external sync
    std::atomic<int> _version; // the node version will increment 1 for every write on this node
    const Comparator _cmp;
};

template <typename Comparator>
class LeafNode : public Node<Comparator> {
private:
    using typename Node<Comparator>::Key;
    using typename Node<Comparator>::NodePtr;
    using typename Node<Comparator>::InternalNodeValue;
    using typename Node<Comparator>::LeafNodeValue;
    typedef LeafNodeMap<Key, LeafNodeValue, Comparator> KVMap;
    typedef std::shared_ptr<KVMap> KVMapPtr;
public:
    LeafNode(Comparator cmp)
    : LeafNode(std::make_shared<KVMap>(cmp), cmp) {
    }

    virtual bool is_leafnode() override {
        return true;
    }
    virtual bool is_internalnode() override {
        return false;
    }

    virtual size_t size() override {
        return _kvmap->size();
    }

    // if this is a leaf node, panic
    // if this is an internal node, get returns the child node that may contains the target key
    // need to check this node's parent node's version after searching this node
    virtual InternalNodeValue get_internalnode_value(const Key& k, int& node_version) override {
        assert(false);
        return nullptr;
    }

    // if this is a leaf node, get reutrns the pointer to the value if target key exist, otherwise nullptr
    // if this is an internal node, panic
    // need to check this node's parent node's version after searching this node
    virtual LeafNodeValue get_leafnode_value(const Key& k, int& node_version) override {
        KVMapPtr kvmap;
        {
            std::lock_guard<std::mutex> lck(Node<Comparator>::_mutex);
            kvmap = _kvmap;
            assert(!kvmap.unique());
            node_version = Node<Comparator>::_version.load(std::memory_order_acquire);
        }
        return kvmap->get(k);
    }

    // need to hold the lock (lock coupling) before call put
    // if this is a leaf node, panic
    virtual void put(const Key& k, InternalNodeValue v) override {
        assert(false);
    }

    // need to hold the lock (lock coupling) before call put
    // if this is an internal node, panic
    virtual void put(const Key& k, LeafNodeValue v) override {
        // TODO: assert _mutex is locked
        if (!_kvmap.unique()) {
            _kvmap.reset(_kvmap->copy());
        }
        assert(_kvmap.unique());
        _kvmap->put(k, v);
        Node<Comparator>::_version.fetch_add(1, std::memory_order_release);
    }

    NodePtr split(Key& k) override {
        // TODO: assert _mutex is locked
        KVMapPtr rhs_kv_map(_kvmap->split(k));
        NodePtr p(new LeafNode<Comparator>(rhs_kv_map, Node<Comparator>::_cmp));
        return p;
    }
private:
    LeafNode(KVMapPtr p, Comparator cmp)
    : Node<Comparator>(cmp),
      _kvmap(p) {

    }

private:
    KVMapPtr _kvmap;
};

template <typename Comparator>
class InternalNode : public Node<Comparator> {
private:
    using typename Node<Comparator>::Key;
    using typename Node<Comparator>::NodePtr;
    using typename Node<Comparator>::InternalNodeValue;
    using typename Node<Comparator>::LeafNodeValue;
    typedef InternalNodeMap<Key, InternalNodeValue, Comparator> KVMap;
    typedef std::shared_ptr<KVMap> KVMapPtr;
public:
    InternalNode() = delete;
    InternalNode(Comparator cmp, NodePtr v1, const Key& k2, NodePtr v2) 
    : InternalNode(std::make_shared<KVMap>(cmp, v1, k2, v2), cmp){

    }

    virtual bool is_leafnode() override {
        return false;
    }
    virtual bool is_internalnode() override {
        return true;
    }

    virtual size_t size() override {
        return _kvmap->size();
    }

    // if this is a leaf node, panic
    // if this is an internal node, get returns the child node that may contains the target key
    // need to check this node's parent node's version after searching this node
    virtual InternalNodeValue get_internalnode_value(const Key& k, int& node_version) override {
        KVMapPtr kvmap;
        {
            std::lock_guard<std::mutex> lck(Node<Comparator>::_mutex);
            kvmap = _kvmap;
            assert(!kvmap.unique());
            node_version = Node<Comparator>::_version.load(std::memory_order_acquire);
        }
        return kvmap->get(k);
    }

    // if this is a leaf node, get reutrns the pointer to the value if target key exist, otherwise nullptr
    // if this is an internal node, panic
    // need to check this node's parent node's version after searching this node
    virtual LeafNodeValue get_leafnode_value(const Key& k, int& node_version) override {
        assert(false);
        return LeafNodeValue();
    }

    // need to hold the lock (lock coupling) before call put
    // if this is a leaf node, panic
    virtual void put(const Key& k, InternalNodeValue v) override {
        // TODO: assert _mutex is locked
        if (!_kvmap.unique()) {
            _kvmap.reset(_kvmap->copy());
        }
        assert(_kvmap.unique());
        _kvmap->put(k, v);
        Node<Comparator>::_version.fetch_add(1, std::memory_order_release);
    }

    // need to hold the lock (lock coupling) before call put
    // if this is an internal node, panic
    virtual void put(const Key& k, LeafNodeValue v) override {
        assert(false);
    }

    NodePtr split(Key& k) override {
        // TODO: assert _mutex is locked
        KVMapPtr rhs_kv_map(_kvmap->split(k));
        NodePtr p(new InternalNode<Comparator>(rhs_kv_map, Node<Comparator>::_cmp));
        return p;
    }
private:
    InternalNode(KVMapPtr p, Comparator cmp)
    : Node<Comparator>(cmp),
      _kvmap(p) {

    }

private:
    KVMapPtr _kvmap;
};

}

#endif