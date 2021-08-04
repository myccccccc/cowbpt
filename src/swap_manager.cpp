#include "swap_manager.h"
using namespace cowbpt;

namespace cowbpt{

Listnode::Listnode(uint64_t id)
{
    page_id = id;
    next = pre = nullptr;
}

LRU_swap_manager::LRU_swap_manager(){
    head = std::make_shared<Listnode>();
}

uint64_t LRU_swap_manager::get_swap_node_id(){
    std::lock_guard<std::mutex> lck(_mutex);
    assert(head->next != nullptr);
    uint64_t res = (head->next)->page_id;
    pop(res);
    return res;
}

void LRU_swap_manager::visit_node(uint64_t page_id){
    std::lock_guard<std::mutex> lck(_mutex);
    if(LRU_index.count(page_id)){
        ListnodePtr t = LRU_index[page_id];
        (t->pre)->next = t->next;
        if(t->next!=nullptr)
            (t->next)->pre = t->pre;
    }
    insert(page_id);
}

void LRU_swap_manager::insert(uint64_t id){
    ListnodePtr t = std::make_shared<Listnode>(id);
    LRU_index[t->page_id] = t;
    t->pre = head;
    t->next = head->next;
    if(head->next != nullptr){
        (head->next)->pre = t;
        head->next = t;
    }
}

void LRU_swap_manager::pop(uint64_t id){
    ListnodePtr t = LRU_index[id];
    LRU_index.erase(id);
    (t->pre)->next = t -> next;
    if(t->next != nullptr)
        (t->next)->pre = t -> pre;
}

}