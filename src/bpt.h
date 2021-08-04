#include <memory>
#include <map>

#include "swap_manager.h"
#include "comparator.h"
#include "glog/logging.h"
#include "slice.h"
#include "node.h"
#include "leveldb/db.h"

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
              _cmp(user_comparator) {
                _swap_manager = std::make_shared<LRU_swap_manager>();
              }

        NodePtr fetch(uint64_t page_id)
        {
            leveldb::ReadOptions options;
            std::string val;
            auto ret = _internalDB->Get(options, std::to_string(page_id), &val);
            if (!ret.ok())
            {
                LOG(FATAL) << "fatal" << std::endl;
                return nullptr;
            }
            NodePtr nptr;
            if (val[0] == '1')
            {
                nptr.reset(new LeafNode<BptComparator>(_cmp));
                nptr->deserialize(val.substr(1));
            }
            else
            {
            //    nptr.reset(new InternalNode<BptComparator>(_cmp));
            //    nptr->deserialize(val.substr(1));
            }

            return nptr;
        }

        void set_snapshot_seq(uint64_t snapshot_seq)
        {
            _snapshot_seq = snapshot_seq;
        }
        void visit_node(uint64_t page_id){
            _swap_manager->visit_node(page_id);
        }
        void swap_node(){
            uint64_t swap_node_id = _swap_manager->get_swap_node_id();
            assert(PageID2NodePtr.count(swap_node_id) != 0);
            NodePtr p = PageID2NodePtr[swap_node_id];
            p->lock();
            flush(p);
            p->unlock();
        }

    private:
        void flush(NodePtr ptr){
            leveldb::WriteOptions options;
            std::string val;
            ptr->serialize(val);
            auto ret = _internalDB->Put(options, std::to_string(ptr->_node_id), val);
            if (!ret.ok()){
                LOG(FATAL) << "fatal" << std::endl;
                return;
            }
            ptr->free_kvmap();
        }
        leveldb::DB *_internalDB;
        uint64_t _snapshot_seq;
        uint64_t _next_node_id;
        BptComparator _cmp;
        std::map<uint64_t, NodePtr> PageID2NodePtr;
        std::shared_ptr<swap_manager> _swap_manager;
    };
}

#endif