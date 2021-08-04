#include <memory>

#include "comparator.h"
#include "glog/logging.h"
#include "slice.h"
#include "node.h"
#include "leveldb/db.h"
#include "coding.h"

#ifndef BPT_H
#define BPT_H

namespace cowbpt
{

    class NodeManager;

    struct BptComparator
    {
    public:
        BptComparator(Comparator *user_comparator)
            : _user_comparator(user_comparator)
        {
        }

    private:
        Comparator *_user_comparator;

    public:
        typedef std::shared_ptr<Node<BptComparator>> NodePtr;

    public:
        bool operator()(const Slice &x, const Slice &y) const { return _user_comparator->operator()(x, y); }
    };

    class Bpt
    {
    public:
        typedef std::shared_ptr<Node<BptComparator> > NodePtr;

    public:
        Bpt(Comparator *user_comparator, NodeManager *nm = nullptr, NodePtr root = nullptr);

        Bpt(const Bpt &) = delete;
        Bpt &operator=(const Bpt &) = delete;
        ~Bpt() = default;

        void put(const Slice &key, const Slice &value);
        void erase(const Slice &key);
        Slice get(const Slice &key); // return an empty slice if key do not exist

        NodePtr snaphot();

        std::string dump();

        NodePtr get_root_node();

    private:
        std::mutex _mutex; // _root is a shared pointer, need to be protected when it is being read and write currently;
        BptComparator _cmp;
        NodePtr _root;
        NodeManager *_nm;
    };

    class NodeManager
    {
    private:
        typedef Bpt::NodePtr NodePtr;

    public:
        NodeManager(leveldb::DB *internalDB, Comparator *user_comparator, uint64_t snapshot_seq = 0, uint64_t next_node_id = 0)
            : _internalDB(internalDB),
              _snapshot_seq(snapshot_seq),
              _next_node_id(next_node_id),
              _cmp(user_comparator) {}

        // need to hold the lock if nptr not null
        NodePtr fetch(uint64_t page_id, NodePtr nptr = nullptr) {
            leveldb::ReadOptions options;
            std::string page_id_string;
            PutFixed64(&page_id_string, page_id);
            std::string value;
            leveldb::Status level_status = _internalDB->Get(options, page_id_string, &value, _snapshot_seq);
            if (!level_status.ok()) {
                LOG(FATAL) << "Fail to find page id: " << page_id << " " << level_status.ToString();
                return nullptr;
            }
            // var int 32 == 0 means is a leafnode
            uint32_t nodetype;
            if(GetVarint32Ptr(value.c_str(), value.c_str()+value.size(), &nodetype) == nullptr) {
                LOG(FATAL) << "Fail to decode the first varint32 which incating the node type";
            }
            if (nodetype == 0) {
                if (nptr == nullptr) nptr.reset(new LeafNode<BptComparator>(_cmp));
                nptr->deserialize(value);
                nptr->set_node_id(page_id);
                nptr->set_is_dirty(false);
                nptr->set_is_in_memory(true);
            } else if (nodetype == 1) {
                if (nptr == nullptr) nptr.reset(new InternalNode<BptComparator>(_cmp));
                nptr->deserialize(value);
                nptr->set_node_id(page_id);
                nptr->set_is_dirty(false);
                nptr->set_is_in_memory(true);
            } else {
                assert(false);
            }

            // TODO xuexinlei : maintain node statics

            return nptr;
        }

        void add_new_node(NodePtr new_node) {
            new_node->set_node_id(_next_node_id++);
            new_node->set_is_dirty(true);
            new_node->set_is_in_memory(true);

            // TODO xuexinlei : maintain node statics
        }

        // replce the old node whose node_id is equal to node_id with the new_node
        // since the old don't need to be maitained anymore
        // return status.ok if find the node_id
        Status replace_node(uint64_t node_id, NodePtr new_node) {
            //TODO: xinlei : maintain node statics
            
            return Status::OK();
        }

        void set_snapshot_seq(uint64_t snapshot_seq) {
            _snapshot_seq = snapshot_seq;
        }

    private:
        leveldb::DB *_internalDB;
        uint64_t _snapshot_seq;
        uint64_t _next_node_id;
        BptComparator _cmp;
    };
}

#endif