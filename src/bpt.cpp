#include "bpt.h"

namespace cowbpt {
    DECLARE_int32(DEAD_LOCK_WAIT_MS);

    Bpt::Bpt(Comparator* user_comparator)
    : _mutex(),
      _cmp(user_comparator),
      _root(new LeafNode<BptComparator>(_cmp)){

    }

    std::string Bpt::dump() {
      return _root->dump();
    }

    Slice Bpt::get(const Slice& key) {
      std::vector<NodePtr> parents;
      std::vector<int> parent_versions;
      while (true) {
        NodePtr child;
        {
          std::lock_guard<std::mutex> lck(_mutex);
          child = _root;
        }



        while (true) {
          Slice res;
          int parent_version;
          NodePtr new_child = nullptr;

          if (child->is_internalnode()) {
            new_child = child->get_internalnode_value(key, parent_version);
          } else {
            res = child->get_leafnode_value(key, parent_version);
          }


          // check parent version
          bool version_checked = false;
          if (!parents.empty()) {
            if (parents.back()->check_version(parent_versions.back())) {
              version_checked = true;
            } else {
              version_checked = false;
            }
          } else {
            {
              std::lock_guard<std::mutex> lck(_mutex);
              if (child.get() == _root.get()) {
                version_checked = true;
              } else {
                version_checked = false;
              }
              
            }
          }
          if (child->is_internalnode() && new_child == nullptr) {
            assert(version_checked == false); 
          }


          if (version_checked) {
            if (new_child == nullptr) {
              return res;
            }
            else {
              parents.push_back(child);
              parent_versions.push_back(parent_version);
              child = new_child;
            }
          }
          else {
            if (!parents.empty()) {
              child = parents.back();
              parents.pop_back();
              parent_versions.pop_back();
            }
            else {
              break;
            }
          }
        }



      }
    }

    void Bpt::put(const Slice& key, const Slice& value) {
      NodePtr parent = nullptr;
      NodePtr child = nullptr;
      bool hold_root_lock = false;
      
          retry:
          _mutex.lock();
          parent = nullptr;
          child = _root;
          child->lock();
          if (!hold_root_lock) {
            _mutex.unlock();
          }
        
      while (true) {
        if (child->need_split()) {
          if (parent == nullptr && !hold_root_lock) { // need split root, retry and get the root lock
            child->unlock();
            hold_root_lock = true;
            goto retry; 
          }
          Slice split_key;
          NodePtr new_child = child->split(split_key);
          if (parent != nullptr) { // split non root node
            parent->put(split_key, new_child);
          } else { // split root node
            assert(hold_root_lock);
            NodePtr new_root_node(new InternalNode<BptComparator>(_cmp, child, split_key, new_child));
            new_root_node->lock();
            _root = new_root_node; 
            parent = new_root_node;
          }
          if (hold_root_lock) {
            _mutex.unlock();
            hold_root_lock = false;
          }
          child->unlock();
          child = parent->get_internalnode_value(key);
          child->lock();
          continue;
        }
        if (hold_root_lock) {
            _mutex.unlock();
            hold_root_lock = false;
        }
        if (parent != nullptr) {
          parent->unlock();
        }
        if (child->is_leafnode()) {
          break;
        }
        parent = child;
        child = parent->get_internalnode_value(key);
        child->lock();
      }
      child->put(key, value);
      child->unlock();
    }

    void Bpt::erase(const Slice& key) {
      NodePtr parent = nullptr;
      NodePtr child = nullptr;
      bool hold_root_lock = false;

          retry:
          _mutex.lock();
          parent = nullptr;
          child = _root;
          child->lock();
          if (!hold_root_lock) {
            _mutex.unlock();
          }
    
      while (true) {
        if (child->need_fix(parent == nullptr)) {
          if (parent == nullptr && !hold_root_lock) { // need fix root, retry and get the root lock
            child->unlock();
            hold_root_lock = true;
            goto retry; 
          }

          if (parent != nullptr)  { // fix non root node
            parent->fix_child(key);
          } else { // fix root node
            assert(hold_root_lock);
            NodePtr new_root_node = _root->get_internalnode_value(Slice());
            new_root_node->lock();
            _root = new_root_node; 
            child = new_root_node;
          }

          if (hold_root_lock) {
            _mutex.unlock();
            hold_root_lock = false;
          }
          if (parent == nullptr) {
            continue;
          }
          child->unlock();
          child = parent->get_internalnode_value(key);
          child->lock();
          continue;
        }

        if (hold_root_lock) {
            _mutex.unlock();
            hold_root_lock = false;
        }
        if (parent != nullptr) {
          parent->unlock();
        }
        if (child->is_leafnode()) {
          break;
        }
        parent = child;
        child = parent->get_internalnode_value(key);
        child->lock();
      }

      child->erase(key);
      child->unlock();
    
    }
}