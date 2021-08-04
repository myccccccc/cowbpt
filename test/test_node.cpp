#include <gtest/gtest.h>
#include <string>
#include <memory>
#include <iostream>

#define private public
#define protected public

#include "node.h"
#include "slice.h"
#include "comparator.h"



using namespace cowbpt;

bool equal(Slice a, Slice b);
extern SliceComparator cmp;

TEST(NodeTest, LeafNodeCRUD) {
  std::shared_ptr<Node<SliceComparator>> n(new LeafNode<SliceComparator>(cmp));
  EXPECT_EQ(n->is_leafnode(), true);
  int node_version;
  EXPECT_TRUE(n->get_leafnode_value("1", node_version).empty());
  EXPECT_EQ(node_version, 1);

  EXPECT_FALSE(n->need_fix(true));

  n->put("1", "one");
  EXPECT_TRUE(equal(n->get_leafnode_value("1", node_version), "one"));
  EXPECT_EQ(node_version, 2);
  n->put("1", "ones");
  EXPECT_TRUE(equal(n->get_leafnode_value("1", node_version), "ones"));
  EXPECT_EQ(node_version, 3);

  n->put("2", "two");
  n->put("3", "three");
  n->put("4", "four");
  n->put("5", "five");
  EXPECT_TRUE(equal(n->get_leafnode_value("1", node_version), "ones"));
  EXPECT_TRUE(equal(n->get_leafnode_value("2", node_version), "two"));
  EXPECT_TRUE(equal(n->get_leafnode_value("3", node_version), "three"));
  EXPECT_TRUE(equal(n->get_leafnode_value("4", node_version), "four"));
  EXPECT_TRUE(equal(n->get_leafnode_value("5", node_version), "five"));

  EXPECT_EQ(n->need_split(), true);
  Slice split_key;
  auto n2 = n->split(split_key);
  EXPECT_TRUE(equal(split_key, "3"));

  EXPECT_TRUE(n->need_fix(false));

  EXPECT_TRUE(equal(n->get_leafnode_value("1", node_version), "ones"));
  EXPECT_TRUE(equal(n->get_leafnode_value("2", node_version), "two"));
  EXPECT_TRUE(n->get_leafnode_value("3", node_version).empty());
  EXPECT_TRUE(n->get_leafnode_value("4", node_version).empty());
  EXPECT_TRUE(n->get_leafnode_value("5", node_version).empty());

  EXPECT_TRUE(n2->get_leafnode_value("1", node_version).empty());
  EXPECT_TRUE(n2->get_leafnode_value("2", node_version).empty());
  EXPECT_TRUE(equal(n2->get_leafnode_value("3", node_version), "three"));
  EXPECT_TRUE(equal(n2->get_leafnode_value("4", node_version), "four"));
  EXPECT_TRUE(equal(n2->get_leafnode_value("5", node_version), "five"));

  n->erase("1");
  n->erase("2");
  n->erase("3");
  EXPECT_TRUE(n->get_leafnode_value("1", node_version).empty());
  EXPECT_TRUE(n->get_leafnode_value("2", node_version).empty());
  EXPECT_TRUE(n->get_leafnode_value("3", node_version).empty());
  EXPECT_TRUE(n->get_leafnode_value("4", node_version).empty());
  EXPECT_TRUE(n->get_leafnode_value("5", node_version).empty());

  Slice k;
  auto p = n2->pop_first_leaf_node_value_and_second_key(k);
  EXPECT_EQ(p.first.string(), "4");
  EXPECT_EQ(p.second.string(), "three");
  p = n2->pop_first_leaf_node_value_and_second_key(k);
  EXPECT_EQ(p.first.string(), "5");
  EXPECT_EQ(p.second.string(), "four");

  std::shared_ptr<Node<SliceComparator>> n3(new LeafNode<SliceComparator>(cmp));
  n3->put("2", "two");
  n3->put("3", "three");
  std::shared_ptr<Node<SliceComparator>> n4(new LeafNode<SliceComparator>(cmp));
  n4->put("4", "four");
  n4->put("5", "five");
  n3->append_right(n4, "4");
  EXPECT_EQ(n3->size(), 4);
  EXPECT_EQ(n4->size(), 0);
  EXPECT_TRUE(equal(n3->get_leafnode_value("2", node_version), "two"));
  EXPECT_TRUE(equal(n3->get_leafnode_value("3", node_version), "three"));
  EXPECT_TRUE(equal(n3->get_leafnode_value("4", node_version), "four"));
  EXPECT_TRUE(equal(n3->get_leafnode_value("5", node_version), "five"));

  p = n3->pop_last_leaf_node_value_and_last_key();
  EXPECT_EQ(n3->size(), 3);
  EXPECT_EQ(p.first.string(), "5");
  EXPECT_EQ(p.second.string(), "five");
  EXPECT_TRUE(equal(n3->get_leafnode_value("2", node_version), "two"));
  EXPECT_TRUE(equal(n3->get_leafnode_value("3", node_version), "three"));
  EXPECT_TRUE(equal(n3->get_leafnode_value("4", node_version), "four"));
}

TEST(NodeTest, InternalNodeCRUD) {
  std::shared_ptr<Node<SliceComparator>> n(new LeafNode<SliceComparator>(cmp));
  n->put("1", "one");
  n->put("2", "two");
  n->put("3", "three");
  n->put("4", "four");
  n->put("5", "five");
  EXPECT_EQ(n->need_split(), true);
  Slice split_key;
  auto n2 = n->split(split_key);
  std::shared_ptr<Node<SliceComparator>> n3(new InternalNode<SliceComparator>(cmp, n, split_key, n2));
  EXPECT_EQ(n3->is_internalnode(), true);
  int node_version;
  EXPECT_EQ(((n3->get_internalnode_value("0", node_version)).get()), n.get());
  EXPECT_EQ(((n3->get_internalnode_value("1", node_version)).get()), n.get());
  EXPECT_EQ(((n3->get_internalnode_value("2", node_version)).get()), n.get());
  EXPECT_EQ(((n3->get_internalnode_value("3", node_version)).get()), n2.get());
  EXPECT_EQ(((n3->get_internalnode_value("4", node_version)).get()), n2.get());
  EXPECT_EQ(((n3->get_internalnode_value("5", node_version)).get()), n2.get());
  EXPECT_EQ(((n3->get_internalnode_value("6", node_version)).get()), n2.get());

  EXPECT_FALSE(n3->need_fix(true));

  // auto w = static_cast<InternalNode<SliceComparator>*> (n3.get());
  // auto m = w->_kvmap;
  // auto c = m->_v;
  // for (int i = 0; i < c.size(); i++) {
  //   std::cout << c[i].first.string() << std::endl;
  // }
  // std::cout << "==================" << std::endl;

  n->put("0.9", Slice("zero"));
  n->put("0.8", Slice("fu yi"));
  n->put("0.7", Slice("fu er"));
  EXPECT_EQ(n->need_split(), true);
  auto n4 = n->split(split_key);
  n3->put(split_key, n4);

  n2->put("6", Slice("six"));
  n2->put("7", Slice("seven"));
  EXPECT_EQ(n2->need_split(), true);
  auto n5 = n2->split(split_key);
  n3->put(split_key, n5);

  EXPECT_EQ(((n3->get_internalnode_value("0.6", node_version)).get()), n.get());
  EXPECT_EQ(((n3->get_internalnode_value("0.7", node_version)).get()), n.get());
  EXPECT_EQ(((n3->get_internalnode_value("0.8", node_version)).get()), n.get());
  EXPECT_EQ(((n3->get_internalnode_value("0.9", node_version)).get()), n4.get());
  EXPECT_EQ(((n3->get_internalnode_value("1", node_version)).get()), n4.get());
  EXPECT_EQ(((n3->get_internalnode_value("2", node_version)).get()), n4.get());
  EXPECT_EQ(((n3->get_internalnode_value("3", node_version)).get()), n2.get());
  EXPECT_EQ(((n3->get_internalnode_value("4", node_version)).get()), n2.get());
  EXPECT_EQ(((n3->get_internalnode_value("5", node_version)).get()), n5.get());
  EXPECT_EQ(((n3->get_internalnode_value("6", node_version)).get()), n5.get());
  EXPECT_EQ(((n3->get_internalnode_value("7", node_version)).get()), n5.get());
  EXPECT_EQ(((n3->get_internalnode_value("8", node_version)).get()), n5.get());

  // w = static_cast<InternalNode<SliceComparator>*> (n3.get());
  // m = w->_kvmap;
  // c = m->_v;
  // for (int i = 0; i < c.size(); i++) {
  //   std::cout << c[i].first.string() << std::endl;
  // }
  // std::cout << "==================" << std::endl;

  EXPECT_EQ(n3->need_split(), false);

  n5->put("8", Slice("eight"));
  n5->put("9", Slice("nine"));
  EXPECT_EQ(n5->need_split(), true);
  auto n6 = n5->split(split_key);
  EXPECT_TRUE(equal(split_key, "7"));
  n3->put(split_key, n6);
  EXPECT_EQ(n3->need_split(), true);

  n3->erase(split_key);
  EXPECT_EQ(n3->need_split(), false);

  n5->put("7", Slice("seven"));
  n5->put("8", Slice("eight"));
  n5->put("9", Slice("nine"));
  EXPECT_EQ(n5->need_split(), true);
  n6 = n5->split(split_key);
  EXPECT_TRUE(equal(split_key, "7"));
  n3->put(split_key, n6);
  EXPECT_EQ(n3->need_split(), true);

  // w = static_cast<InternalNode<SliceComparator>*> (n3.get());
  // m = w->_kvmap;
  // c = m->_v;
  // for (int i = 0; i < c.size(); i++) {
  //   std::cout << c[i].first.string() << std::endl;
  // }
  // std::cout << "==================" << std::endl;

  //                          n3 internal _, 0.9, 3, 5, 7
  // n leaf 0.7, 0.8 | n4 leaf 0.9, 1, 2 | n2 leaf 3, 4 | n5 leaf 5, 6 | n6 leaf 7, 8, 9
  // std::cout << n3->dump() << std::endl;

  EXPECT_EQ(n3->need_split(), true);
  auto n7 = n3->split(split_key);

  
  std::shared_ptr<Node<SliceComparator>> n8(new InternalNode<SliceComparator>(cmp, n3, split_key, n7));

  //|n8 internal _,                                                         3
              // |n3 internal _,              0.9                           | n7 internal _,             5,             7
              //              |n leaf 0.7, 0.8 | n4 leaf 0.9, 1, 2                        | n2 leaf 3, 4 | n5 leaf 5, 6 | n6 leaf 7, 8, 9
  // std::cout << n8->dump() << std::endl;

  EXPECT_EQ(((n8->get_internalnode_value("0.1", node_version)).get()), n3.get());
  EXPECT_EQ(((n8->get_internalnode_value("0.9", node_version)).get()), n3.get());
  EXPECT_EQ(((n8->get_internalnode_value("1", node_version)).get()), n3.get());
  EXPECT_EQ(((n8->get_internalnode_value("2", node_version)).get()), n3.get());
  EXPECT_EQ(((n8->get_internalnode_value("3", node_version)).get()), n7.get());
  EXPECT_EQ(((n8->get_internalnode_value("4", node_version)).get()), n7.get());
  EXPECT_EQ(((n8->get_internalnode_value("5", node_version)).get()), n7.get());
  EXPECT_EQ(((n8->get_internalnode_value("99", node_version)).get()), n7.get());

  EXPECT_FALSE(n8->need_fix(true));
  EXPECT_TRUE(n3->need_fix(false));
  EXPECT_FALSE(n7->need_fix(false));

  EXPECT_TRUE((static_cast<InternalNode<SliceComparator>*>(n8.get()))->borrow_from_right_node(n3, n7, split_key));

  EXPECT_FALSE(n8->need_fix(true));
  EXPECT_FALSE(n3->need_fix(false));
  EXPECT_TRUE(n7->need_fix(false));

  EXPECT_EQ(((n8->get_internalnode_value("0.1", node_version)).get()), n3.get());
  EXPECT_EQ(((n8->get_internalnode_value("0.9", node_version)).get()), n3.get());
  EXPECT_EQ(((n8->get_internalnode_value("1", node_version)).get()), n3.get());
  EXPECT_EQ(((n8->get_internalnode_value("2", node_version)).get()), n3.get());
  EXPECT_EQ(((n8->get_internalnode_value("3", node_version)).get()), n3.get());
  EXPECT_EQ(((n8->get_internalnode_value("4", node_version)).get()), n3.get());
  EXPECT_EQ(((n8->get_internalnode_value("5", node_version)).get()), n7.get());
  EXPECT_EQ(((n8->get_internalnode_value("99", node_version)).get()), n7.get());
  //|n8 internal _,                                                                        5
              // |n3 internal _,              0.9                  3                       | n7 internal 5,             7
              //              |n leaf 0.7, 0.8 | n4 leaf 0.9, 1, 2 | n2 leaf 3, 4                        | n5 leaf 5, 6 | n6 leaf 7, 8, 9
  // std::cout << n8->dump() << std::endl;

  // w = static_cast<InternalNode<SliceComparator>*> (n8.get());
  // m = w->_kvmap;
  // c = m->_v;
  // for (int i = 0; i < c.size(); i++) {
  //   std::cout << c[i].first.string() << std::endl;
  // }
  // std::cout << "==================" << std::endl;

  EXPECT_TRUE(n3->get_internalnode_value("0.6")->need_fix(false));
  EXPECT_FALSE(n3->get_internalnode_value("0.9")->need_fix(false));
  n3->fix_child("0.6");
  EXPECT_FALSE(n3->get_internalnode_value("0.6")->need_fix(false));
  EXPECT_TRUE(n3->get_internalnode_value("1")->need_fix(false));
  
  //|n8 internal _,                                                                        5
  // |n3 internal _,                    1              3                       | n7 internal 5,             7
  //              |n leaf 0.7, 0.8, 0.9 | n4 leaf 1, 2 | n2 leaf 3, 4                        | n5 leaf 5, 6 | n6 leaf 7, 8, 9
  // std::cout << n8->dump() << std::endl;

  n3->fix_child("1.5");
  EXPECT_EQ(n3->size(), 2);
  EXPECT_EQ(n4->size(), 4);
  EXPECT_EQ(n2->size(), 0);
  EXPECT_TRUE(equal(n4->get_leafnode_value("1", node_version), "one"));
  EXPECT_TRUE(equal(n4->get_leafnode_value("2", node_version), "two"));
  EXPECT_TRUE(equal(n4->get_leafnode_value("3", node_version), "three"));
  EXPECT_TRUE(equal(n4->get_leafnode_value("4", node_version), "four"));

  //|n8 internal _,                                                           5
  // |n3 internal _,                    1                                     | n7 internal 5,             7
  //              |n leaf 0.7, 0.8, 0.9 | n4 leaf 1, 2, 3, 4                                | n5 leaf 5, 6 | n6 leaf 7, 8, 9
  // std::cout << n8->dump() << std::endl;

  n7->fix_child("5");
  EXPECT_EQ(n7->size(), 2);
  EXPECT_EQ(n5->size(), 3);
  EXPECT_EQ(n7->size(), 2);
  EXPECT_TRUE(equal(n5->get_leafnode_value("5", node_version), "five"));
  EXPECT_TRUE(equal(n5->get_leafnode_value("6", node_version), "six"));
  EXPECT_TRUE(equal(n5->get_leafnode_value("7", node_version), "seven"));
  EXPECT_TRUE(equal(n6->get_leafnode_value("8", node_version), "eight"));
  EXPECT_TRUE(equal(n6->get_leafnode_value("9", node_version), "nine"));
  EXPECT_EQ(n7->get_internalnode_value("7.5").get(), n5.get());
  EXPECT_EQ(n7->get_internalnode_value("8.1").get(), n6.get());

  //|n8 internal _,                                                           5
  // |n3 internal _,                    1                                     | n7 internal 5,                8
  //              |n leaf 0.7, 0.8, 0.9 | n4 leaf 1, 2, 3, 4                                | n5 leaf 5, 6, 7 | n6 leaf 8, 9
  // std::cout << n8->dump() << std::endl;

  n7->fix_child("8");
  EXPECT_EQ(n7->size(), 2);
  EXPECT_EQ(n5->size(), 2);
  EXPECT_EQ(n6->size(), 3);
  EXPECT_TRUE(equal(n5->get_leafnode_value("5", node_version), "five"));
  EXPECT_TRUE(equal(n5->get_leafnode_value("6", node_version), "six"));
  EXPECT_TRUE(equal(n6->get_leafnode_value("7", node_version), "seven"));
  EXPECT_TRUE(equal(n6->get_leafnode_value("8", node_version), "eight"));
  EXPECT_TRUE(equal(n6->get_leafnode_value("9", node_version), "nine"));
  EXPECT_EQ(n7->get_internalnode_value("5").get(), n5.get());
  EXPECT_EQ(n7->get_internalnode_value("7").get(), n6.get());

  //|n8 internal _,                                                           5
  // |n3 internal _,                    1                                     | n7 internal 5,             7
  //              |n leaf 0.7, 0.8, 0.9 | n4 leaf 1, 2, 3, 4                                | n5 leaf 5, 6 | n6 leaf 7, 8, 9
  // std::cout << n8->dump() << std::endl;

  n8->fix_child("6");
  EXPECT_EQ(n8->size(), 1);
  EXPECT_EQ(n3->size(), 4);
  EXPECT_EQ(n7->size(), 0);
  EXPECT_EQ(n3->get_internalnode_value("0.7").get(), n.get());
  EXPECT_EQ(n3->get_internalnode_value("1").get(), n4.get());
  EXPECT_EQ(n3->get_internalnode_value("5").get(), n5.get());
  EXPECT_EQ(n3->get_internalnode_value("7").get(), n6.get());

  //|n8 internal _,                                                           
  // |n3 internal _,                    1                                                   5,             7
  //              |n leaf 0.7, 0.8, 0.9 | n4 leaf 1, 2, 3, 4                                | n5 leaf 5, 6 | n6 leaf 7, 8, 9
  // std::cout << n8->dump() << std::endl;

}

TEST(NodeTest, LeafNodeSerialize) {
    std::shared_ptr<Node<SliceComparator>> n(new LeafNode<SliceComparator>(cmp));
    n->put("1", "one");
    n->put("2", "two");
    n->put("3", "three");
    n->put("4", "four");
    n->put("5", "five");
    std::string result;
    n->serialize(result);

    Slice input = result;
    uint32_t node_type;
    GetVarint32(&input, &node_type);
    EXPECT_EQ(node_type, 0);

    std::shared_ptr<Node<SliceComparator>> n2(new LeafNode<SliceComparator>(cmp));
    auto status = n2->deserialize(result);
    EXPECT_TRUE(status.ok());
    int node_version;
    EXPECT_TRUE(equal(n2->get_leafnode_value("1", node_version), "one"));
    EXPECT_TRUE(equal(n2->get_leafnode_value("2", node_version), "two"));
    EXPECT_TRUE(equal(n2->get_leafnode_value("3", node_version), "three"));
    EXPECT_TRUE(equal(n2->get_leafnode_value("4", node_version), "four"));
    EXPECT_TRUE(equal(n2->get_leafnode_value("5", node_version), "five"));
}

TEST(NodeTest, InternalNodeSerialize) {
    std::shared_ptr<Node<SliceComparator>> n1(new InternalNode<SliceComparator>(cmp));
    std::shared_ptr<Node<SliceComparator>> leaf1(new LeafNode<SliceComparator>(cmp));
    std::shared_ptr<Node<SliceComparator>> leaf2(new LeafNode<SliceComparator>(cmp));
    std::shared_ptr<Node<SliceComparator>> leaf3(new LeafNode<SliceComparator>(cmp));
    std::shared_ptr<Node<SliceComparator>> leaf4(new LeafNode<SliceComparator>(cmp));
    leaf1->set_node_id(1);
    leaf2->set_node_id(2);
    leaf3->set_node_id(3);
    leaf4->set_node_id(4);
    n1->put("1", leaf1);
    n1->put("2", leaf2);
    n1->put("3", leaf3);
    n1->put("4", leaf4);
    std::string result;
    n1->serialize(result);

    Slice input = result;
    uint32_t node_type;
    GetVarint32(&input, &node_type);
    EXPECT_EQ(node_type, 1);

    std::shared_ptr<Node<SliceComparator>> n2(new InternalNode<SliceComparator>(cmp));
    auto status = n2->deserialize(result);
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(n2->get_internalnode_value("1")->get_node_id(),
              n1->get_internalnode_value("1")->get_node_id());
    EXPECT_EQ(n2->get_internalnode_value("2")->get_node_id(),
              n1->get_internalnode_value("2")->get_node_id());
    EXPECT_EQ(n2->get_internalnode_value("3")->get_node_id(),
              n1->get_internalnode_value("3")->get_node_id());
    EXPECT_EQ(n2->get_internalnode_value("4")->get_node_id(),
              n1->get_internalnode_value("4")->get_node_id());

}