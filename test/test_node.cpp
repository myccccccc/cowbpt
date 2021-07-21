#include <gtest/gtest.h>
#include <string>
#include <memory>
#include <iostream>

#define private public
#define protected public

#include "node.h"
#include "slice.h"



using namespace cowbpt;

bool equal(Slice a, Slice b);

TEST(NodeTest, LeafNodeCRUD) {
  std::shared_ptr<Node<SliceComparator>> n(new LeafNode<SliceComparator>);
  EXPECT_EQ(n->is_leafnode(), true);
  int node_version;
  EXPECT_TRUE(equal(n->get_leafnode_value("1", node_version), EMPTYSLICE));
  EXPECT_EQ(node_version, 1);
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

  EXPECT_TRUE(equal(n->get_leafnode_value("1", node_version), "ones"));
  EXPECT_TRUE(equal(n->get_leafnode_value("2", node_version), "two"));
  EXPECT_TRUE(equal(n->get_leafnode_value("3", node_version), EMPTYSLICE));
  EXPECT_TRUE(equal(n->get_leafnode_value("4", node_version), EMPTYSLICE));
  EXPECT_TRUE(equal(n->get_leafnode_value("5", node_version), EMPTYSLICE));

  EXPECT_TRUE(equal(n2->get_leafnode_value("1", node_version), EMPTYSLICE));
  EXPECT_TRUE(equal(n2->get_leafnode_value("2", node_version), EMPTYSLICE));
  EXPECT_TRUE(equal(n2->get_leafnode_value("3", node_version), "three"));
  EXPECT_TRUE(equal(n2->get_leafnode_value("4", node_version), "four"));
  EXPECT_TRUE(equal(n2->get_leafnode_value("5", node_version), "five"));
}

TEST(NodeTest, InternalNodeCRUD) {
  std::shared_ptr<Node<SliceComparator>> n(new LeafNode<SliceComparator>);
  n->put("1", "one");
  n->put("2", "two");
  n->put("3", "three");
  n->put("4", "four");
  n->put("5", "five");
  EXPECT_EQ(n->need_split(), true);
  Slice split_key;
  auto n2 = n->split(split_key);
  std::shared_ptr<Node<SliceComparator>> n3(new InternalNode<SliceComparator>(n, split_key, n2));
  EXPECT_EQ(n3->is_internalnode(), true);
  int node_version;
  EXPECT_EQ(((n3->get_internalnode_value("0", node_version)).get()), n.get());
  EXPECT_EQ(((n3->get_internalnode_value("1", node_version)).get()), n.get());
  EXPECT_EQ(((n3->get_internalnode_value("2", node_version)).get()), n.get());
  EXPECT_EQ(((n3->get_internalnode_value("3", node_version)).get()), n2.get());
  EXPECT_EQ(((n3->get_internalnode_value("4", node_version)).get()), n2.get());
  EXPECT_EQ(((n3->get_internalnode_value("5", node_version)).get()), n2.get());
  EXPECT_EQ(((n3->get_internalnode_value("6", node_version)).get()), n2.get());

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

  // w = static_cast<InternalNode<SliceComparator>*> (n3.get());
  // m = w->_kvmap;
  // c = m->_v;
  // for (int i = 0; i < c.size(); i++) {
  //   std::cout << c[i].first.string() << std::endl;
  // }
  // std::cout << "==================" << std::endl;


  EXPECT_EQ(n3->need_split(), true);
  auto n7 = n3->split(split_key);

  
  std::shared_ptr<Node<SliceComparator>> n8(new InternalNode<SliceComparator>(n3, split_key, n7));

  EXPECT_EQ(((n8->get_internalnode_value("0.1", node_version)).get()), n3.get());
  EXPECT_EQ(((n8->get_internalnode_value("0.9", node_version)).get()), n3.get());
  EXPECT_EQ(((n8->get_internalnode_value("1", node_version)).get()), n3.get());
  EXPECT_EQ(((n8->get_internalnode_value("2", node_version)).get()), n3.get());
  EXPECT_EQ(((n8->get_internalnode_value("3", node_version)).get()), n7.get());
  EXPECT_EQ(((n8->get_internalnode_value("4", node_version)).get()), n7.get());
  EXPECT_EQ(((n8->get_internalnode_value("5", node_version)).get()), n7.get());
  EXPECT_EQ(((n8->get_internalnode_value("99", node_version)).get()), n7.get());

  // w = static_cast<InternalNode<SliceComparator>*> (n8.get());
  // m = w->_kvmap;
  // c = m->_v;
  // for (int i = 0; i < c.size(); i++) {
  //   std::cout << c[i].first.string() << std::endl;
  // }
  // std::cout << "==================" << std::endl;
}