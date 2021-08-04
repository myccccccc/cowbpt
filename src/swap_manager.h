#include <map>
#include <cassert>
#include <memory>
#include <mutex>

#ifndef SWAP_MANAGER
#define SWAP_MANAGER

namespace cowbpt{

    class Listnode{
    public:
        typedef std::shared_ptr<Listnode> ListnodePtr;
        Listnode(uint64_t id = -1);
        ~Listnode() = default;
        ListnodePtr next, pre;
        uint64_t page_id;
    };

    class swap_manager{
    public:
        swap_manager() = default;

        virtual ~swap_manager() = default;
        virtual void visit_node(uint64_t page_id) = 0;
        virtual uint64_t get_swap_node_id() = 0;
    };


    class LRU_swap_manager: public swap_manager{
    public:
        typedef Listnode::ListnodePtr ListnodePtr;
        LRU_swap_manager();
        virtual ~LRU_swap_manager() = default;

        virtual uint64_t get_swap_node_id();
        virtual void visit_node(uint64_t page_id);
    private:
        void insert(uint64_t page_id);
        void pop(uint64_t page_id);
        std::map<uint64_t, ListnodePtr> LRU_index;
        ListnodePtr head;
        std::mutex _mutex;
    };
}

#endif