#include <memory>
#include <mutex>
#include <cassert>
#include <atomic>
#include "slice.h"
#include "nodemap.h"
#include "status.h"
#include "gflags/gflags.h"


#ifndef NODE_H
#define NODE_H

namespace cowbpt {

DECLARE_int32(COWBPT_NODE_B_SZIE);

template <typename Comparator>
class InternalNode;
template <typename Comparator>
class LeafNode;

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

    virtual std::string dump() = 0;

    void lock() {
        _mutex.lock();
    }
    void unlock() {
        _mutex.unlock();
    }

    bool check_version(int node_version) {
        return node_version == _version.load(std::memory_order_acquire);
    }

    uint64_t get_node_id() {
        return _node_id;
    }

    void set_node_id(uint64_t node_id) {
        _node_id = node_id;
    }

    bool is_dirty() {
        return _dirty;
    }

    bool is_in_memory() {
        return _in_memory;
    }

    // if this is a leaf node, panic
    virtual std::vector<NodePtr> get_child_nodes() = 0;

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
    bool need_fix(bool is_root_node) {
        if (is_root_node) {
            if(is_leafnode()) {
                return false;
            }
            assert(size() >= 1);
            return size() == 1;
        }
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

    virtual InternalNodeValue get_internalnode_value(const Key& k) = 0;

    virtual LeafNodeValue get_leafnode_value(const Key& k) = 0;

    // need to hold the lock (lock coupling) before call put
    // if this is a leaf node, panic
    virtual void put(const Key& k, InternalNodeValue v) = 0;

    // need to hold the lock (lock coupling) before call put
    // if this is an internal node, panic
    virtual void put(const Key& k, LeafNodeValue v) = 0;

    // need to hold the lock (lock coupling) before call erase
    virtual void erase(const Key& k) = 0;

    // return a poniter to the right half split of the node
    // k is set to the first key in the right half split of the node
    // need to hold the lock (lock coupling) before call split
    virtual NodePtr split(Key& k) = 0;

    // only internal node can call fix_child
    // find the child node by key, fix this node
    virtual void fix_child(const Key& k) = 0;

    virtual Status serialize(std::string& result) = 0;
    virtual Status deserialize(const std::string& byte_string) = 0;

    // TODO: copy

protected:
    std::atomic<int> _version; // the node version will increment 1 for every write on this node
    std::mutex _mutex; // when a shared ptr is accessed by mutiple threads, it needs external sync
    const Comparator _cmp;
    uint64_t _node_id = 0;
    bool _dirty = false;
    bool _in_memory = false;

protected:
    void increase_version() {
        _version.fetch_add(1, std::memory_order_release);
    }


friend class LeafNode<Comparator>;
friend class InternalNode<Comparator>;

private:

    virtual std::pair<Key, InternalNodeValue> pop_first_internal_node_value_and_second_key() = 0;
    virtual std::pair<Key, LeafNodeValue> pop_first_leaf_node_value_and_second_key(Key& first_key) = 0;

    virtual std::pair<Key, InternalNodeValue> pop_last_internal_node_value_and_last_key() = 0;
    virtual std::pair<Key, LeafNodeValue> pop_last_leaf_node_value_and_last_key() = 0;

    // push this internalnodevalue to the front of is node, and the previous front key is set to right_k
    virtual void push_front(InternalNodeValue v, Key right_k) = 0;

    // append all kv pairs of the right node, to this node, and clear right node
    virtual void append_right(NodePtr right, Key right_k) = 0;
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

    // if this is a leaf node, panic
    virtual std::vector<NodePtr> get_child_nodes() override {
        assert(false);
        return std::vector<NodePtr>();
    }
    virtual Status serialize(std::string& result) override {
        return Status::OK();
    }
    virtual Status deserialize(const std::string& byte_string) override {
        return Status::OK();
    }
    virtual std::string dump() override {
        return _kvmap->dump();
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

    virtual InternalNodeValue get_internalnode_value(const Key& k) override {
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

    virtual LeafNodeValue get_leafnode_value(const Key& k) override {
        // TODO: assert _mutex is locked
        return _kvmap->get(k);
    }

    // need to hold the lock (lock coupling) before call put
    // if this is a leaf node, panic
    virtual void put(const Key& k, InternalNodeValue v) override {
        assert(false);
    }

    // need to hold the lock (lock coupling) before call put
    // if this is an internal node, panic
    virtual void put(const Key& k, LeafNodeValue v) override {
        cow();
        _kvmap->put(k, v);
        this->increase_version();
    }

    // need to hold the lock (lock coupling) before call erase
    virtual void erase(const Key& k) override {
        cow();
        _kvmap->erase(k);
        this->increase_version();
    }

    NodePtr split(Key& k) override {
        cow();
        KVMapPtr rhs_kv_map(_kvmap->split(k));
        NodePtr p(new LeafNode<Comparator>(rhs_kv_map, Node<Comparator>::_cmp));
        this->increase_version();
        return p;
    }

    // only internal node can call fix_child
    // find the child node by key, fix this node
    virtual void fix_child(const Key& k) override {
        assert(false);
    }

private:
    LeafNode(KVMapPtr p, Comparator cmp)
    : Node<Comparator>(cmp),
      _kvmap(p) {

    }

    void cow() {
        // TODO: assert _mutex is locked
        if (!_kvmap.unique()) {
            _kvmap.reset(_kvmap->copy());
        }
        assert(_kvmap.unique());
    }

private:
    KVMapPtr _kvmap;

private:
    virtual std::pair<Key, InternalNodeValue> pop_first_internal_node_value_and_second_key() override {
        assert(false);
        return std::make_pair(Key(), nullptr);
    }
    virtual std::pair<Key, LeafNodeValue> pop_first_leaf_node_value_and_second_key(Key& first_key) override {
        cow();
        auto a = _kvmap->pop_first_leaf_node_value_and_second_key(first_key);
        this->increase_version();
        return a;
    }
    virtual std::pair<Key, InternalNodeValue> pop_last_internal_node_value_and_last_key() override {
        assert(false);
        return std::make_pair(Key(), nullptr);
    }
    virtual std::pair<Key, LeafNodeValue> pop_last_leaf_node_value_and_last_key() override {
        cow();
        auto a = _kvmap->pop_last_leaf_node_value_and_last_key();
        this->increase_version();
        return a;
    }
    // append all kv pairs of the right node, to this node, and clear right node
    virtual void append_right(NodePtr right, Key right_k) override {
        auto r = dynamic_cast<LeafNode<Comparator>*>(right.get());
        cow();
        r->cow();
        _kvmap->append_right(r->_kvmap.get());
        this->increase_version();
        right->increase_version();
    }
    // push this internalnodevalue to the front of is node, and the previous front key is set to right_k
    virtual void push_front(InternalNodeValue v, Key right_k) override {
        assert(false);
    }
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

    // if this is a leaf node, panic
    virtual std::vector<NodePtr> get_child_nodes() {
        return _kvmap->get_values();
    }
    virtual Status serialize(std::string& result) override {
        return Status::OK();
    }
    virtual Status deserialize(const std::string& byte_string) override {
        return Status::OK();
    }

    virtual bool is_leafnode() override {
        return false;
    }
    virtual bool is_internalnode() override {
        return true;
    }

    virtual std::string dump() override {
        return _kvmap->dump();
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

    virtual InternalNodeValue get_internalnode_value(const Key& k) override {
        // TODO: assert _mutex is locked
        return _kvmap->get(k);
    }

    // if this is a leaf node, get reutrns the pointer to the value if target key exist, otherwise nullptr
    // if this is an internal node, panic
    // need to check this node's parent node's version after searching this node
    virtual LeafNodeValue get_leafnode_value(const Key& k, int& node_version) override {
        assert(false);
        return LeafNodeValue();
    }

    virtual LeafNodeValue get_leafnode_value(const Key& k) override {
        assert(false);
        return LeafNodeValue();
    }

    // need to hold the lock (lock coupling) before call put
    // if this is a leaf node, panic
    virtual void put(const Key& k, InternalNodeValue v) override {
        cow();
        _kvmap->put(k, v);
        this->increase_version();
    }

    // need to hold the lock (lock coupling) before call put
    // if this is an internal node, panic
    virtual void put(const Key& k, LeafNodeValue v) override {
        assert(false);
    }

    // need to hold the lock (lock coupling) before call erase
    virtual void erase(const Key& k) override {
        cow();
        _kvmap->erase(k);
        this->increase_version();
    }

    NodePtr split(Key& k) override {
        cow();
        KVMapPtr rhs_kv_map(_kvmap->split(k));
        NodePtr p(new InternalNode<Comparator>(rhs_kv_map, Node<Comparator>::_cmp));
        this->increase_version();
        return p;
    }

    // only internal node can call fix_child
    // find the child node by key, fix this node
    virtual void fix_child(const Key& k) override {
        bool fixed = false;

        auto middle_node_kv = get_middle_node(k);
        Key middle_node_key = middle_node_kv.first;
        NodePtr need_fix_child = middle_node_kv.second;
        assert(need_fix_child->need_fix(false));

        auto right_node_kv = get_right_node(k);
        Key right_node_key = right_node_kv.first;
        NodePtr right_node = right_node_kv.second;
        if (right_node != nullptr) {
            right_node->lock();

            if (borrow_from_right_node(need_fix_child, right_node, right_node_key)) {
                fixed = true;
            } else if (merge_right_into_left(need_fix_child, right_node, right_node_key)){
                fixed = true;
            }

            right_node->unlock();
            if(fixed) return;
        }

        auto left_node_kv = get_left_node(k);
        Key left_node_key = left_node_kv.first;
        NodePtr left_node = left_node_kv.second;
        if (left_node != nullptr) {
            left_node->lock();

            if (borrow_from_left_node(left_node, need_fix_child, middle_node_key)) {
                fixed = true;
            } else if (merge_right_into_left(left_node, need_fix_child, middle_node_key)){
                fixed = true;
            }

            left_node->unlock();
            if(fixed) return;
        }
        
        assert(false);
    }

private:
    // return the node and its corresponding key, that is at the right of the node which might contains k down below
    // return nullptr if don't have right node
    std::pair<Key, NodePtr> get_right_node(const Key& k) {
        return _kvmap->get_right(k);
    }

    // return the node and its corresponding key, that might contains k down below
    std::pair<Key, NodePtr> get_middle_node(const Key& k) {
        return _kvmap->get_middle(k);
    }

    // return the node and its corresponding key, that is at the left of the node which might contains k down below
    // return nullptr if don't have left node (impossible)
    std::pair<Key, NodePtr> get_left_node(const Key& k) {
        return _kvmap->get_left(k);
    }

    // TODO: only borrow one from right node, maybe borrow more?
    bool borrow_from_right_node(const NodePtr& left, const NodePtr& right, const Key& right_k) {
        if(right->need_fix(false)) return false;
        if (right->is_leafnode()) {
            Key k;
            auto p = right->pop_first_leaf_node_value_and_second_key(k);
            Key new_right_k = p.first;
            LeafNodeValue borrowed_value = p.second;
            left->put(k, borrowed_value);
            this->erase(right_k);
            this->put(new_right_k, right);
        } else {
            auto p = right->pop_first_internal_node_value_and_second_key();
            Key new_right_k = p.first;
            InternalNodeValue borrowed_value = p.second;
            left->put(right_k, borrowed_value);
            this->erase(right_k);
            this->put(new_right_k, right);
        }
        assert(!left->need_fix(false));
        return true;
    }

    
    // TODO: only borrow one from left node, maybe borrow more?
    bool borrow_from_left_node(const NodePtr& left, const NodePtr& right, const Key& right_k) {
        if(left->need_fix(false)) return false;
        if (left->is_leafnode()) {
            auto p = left->pop_last_leaf_node_value_and_last_key();
            Key new_right_k = p.first;
            LeafNodeValue borrowed_value = p.second;
            right->put(new_right_k, borrowed_value);
            this->erase(right_k);
            this->put(new_right_k, right);
        } else {
            auto p = left->pop_last_internal_node_value_and_last_key();
            Key new_right_k = p.first;
            InternalNodeValue borrowed_value = p.second;
            right->push_front(borrowed_value, right_k);
            this->erase(right_k);
            this->put(new_right_k, right);
        }
        assert(!right->need_fix(false));
        return true;
    }

    bool merge_right_into_left(const NodePtr& left, const NodePtr& right, const Key& right_k) {
        assert(left->need_fix(false) && right->need_fix(false));

        this->erase(right_k);
        left->append_right(right, right_k);

        assert(right->size() == 0);
        assert(!left->need_fix(false));
        return true;
    }

private:
    InternalNode(KVMapPtr p, Comparator cmp)
    : Node<Comparator>(cmp),
      _kvmap(p) {

    }

    void cow() {
        // TODO: assert _mutex is locked
        if (!_kvmap.unique()) {
            _kvmap.reset(_kvmap->copy());
        }
        assert(_kvmap.unique());
    }

private:
    KVMapPtr _kvmap;

private:
    virtual std::pair<Key, InternalNodeValue> pop_first_internal_node_value_and_second_key() override {
        cow();
        auto a = _kvmap->pop_first_internal_node_value_and_second_key();
        this->increase_version();
        return a;
    }
    virtual std::pair<Key, LeafNodeValue> pop_first_leaf_node_value_and_second_key(Key& first_key) override {
        assert(false);
        return std::make_pair(Key(), LeafNodeValue());
    }
    virtual std::pair<Key, InternalNodeValue> pop_last_internal_node_value_and_last_key() override {
        cow();
        auto a = _kvmap->pop_last_internal_node_value_and_last_key();
        this->increase_version();
        return a;
    }
    virtual std::pair<Key, LeafNodeValue> pop_last_leaf_node_value_and_last_key() override {
        assert(false);
        return std::make_pair(Key(), LeafNodeValue());
    }
    // append all kv pairs of the right node, to this node, and clear right node
    virtual void append_right(NodePtr right, Key right_k) override {
        auto r = dynamic_cast<InternalNode<Comparator>*>(right.get());
        cow();
        r->cow();
        _kvmap->append_right(r->_kvmap.get(), right_k);
        this->increase_version();
        right->increase_version();
    }
    // push this internalnodevalue to the front of is node, and the previous front key is set to right_k
    virtual void push_front(InternalNodeValue v, Key right_k) {
        cow();
        _kvmap->push_front(v, right_k);
        this->increase_version();
    }
};

}

#endif