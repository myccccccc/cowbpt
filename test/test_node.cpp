#include <gtest/gtest.h>
#include <string>
#include <memory>
#include "node.h"

using namespace cowbpt;

TEST(NodeTest, LeafNodeCRUD) {
  std::shared_ptr<Node<int, std::string>> n(new LeafNode<int, std::string>);
  int node_version;
  EXPECT_EQ(n->get(1, node_version), nullptr);
  EXPECT_EQ(node_version, 1);
  n->put(1, std::make_shared<std::string>("one"));
  EXPECT_EQ(*(n->get(1, node_version)), "one");
  EXPECT_EQ(node_version, 2);
  n->put(1, std::make_shared<std::string>("ones"));
  EXPECT_EQ(*(n->get(1, node_version)), "ones");
  EXPECT_EQ(node_version, 3);

  n->put(2, std::make_shared<std::string>("two"));
  n->put(3, std::make_shared<std::string>("three"));
  n->put(4, std::make_shared<std::string>("four"));
  n->put(5, std::make_shared<std::string>("five"));
  EXPECT_EQ(*(n->get(1, node_version)), "ones");
  EXPECT_EQ(*(n->get(2, node_version)), "two");
  EXPECT_EQ(*(n->get(3, node_version)), "three");
  EXPECT_EQ(*(n->get(4, node_version)), "four");
  EXPECT_EQ(*(n->get(5, node_version)), "five");

  EXPECT_EQ(n->need_split(), true);
  int split_key;
  auto n2 = n->split(split_key);
  EXPECT_EQ(split_key, 3);

  EXPECT_EQ(*(n->get(1, node_version)), "ones");
  EXPECT_EQ(*(n->get(2, node_version)), "two");
  EXPECT_EQ((n->get(3, node_version)), nullptr);
  EXPECT_EQ((n->get(4, node_version)), nullptr);
  EXPECT_EQ((n->get(5, node_version)), nullptr);

  EXPECT_EQ((n2->get(1, node_version)), nullptr);
  EXPECT_EQ((n2->get(2, node_version)), nullptr);
  EXPECT_EQ(*(n2->get(3, node_version)), "three");
  EXPECT_EQ(*(n2->get(4, node_version)), "four");
  EXPECT_EQ(*(n2->get(5, node_version)), "five");
}

TEST(NodeTest, InternalNodeCRUD) {
  std::shared_ptr<Node<int, std::string>> n(new LeafNode<int, std::string>);
  n->put(1, std::make_shared<std::string>("one"));
  n->put(2, std::make_shared<std::string>("two"));
  n->put(3, std::make_shared<std::string>("three"));
  n->put(4, std::make_shared<std::string>("four"));
  n->put(5, std::make_shared<std::string>("five"));
  EXPECT_EQ(n->need_split(), true);
  int split_key;
  auto n2 = n->split(split_key);
  std::shared_ptr<Node<int, Node<int, std::string>>> n3(new InternalNode<int, Node<int, std::string>>(n, split_key, n2));
  int node_version;
  EXPECT_EQ(((n3->get(0, node_version)).get()), n.get());
  EXPECT_EQ(((n3->get(1, node_version)).get()), n.get());
  EXPECT_EQ(((n3->get(2, node_version)).get()), n.get());
  EXPECT_EQ(((n3->get(3, node_version)).get()), n2.get());
  EXPECT_EQ(((n3->get(4, node_version)).get()), n2.get());
  EXPECT_EQ(((n3->get(5, node_version)).get()), n2.get());
  EXPECT_EQ(((n3->get(6, node_version)).get()), n2.get());

  n->put(0, std::make_shared<std::string>("zero"));
  n->put(-1, std::make_shared<std::string>("fu yi"));
  n->put(-2, std::make_shared<std::string>("fu er"));
  EXPECT_EQ(n->need_split(), true);
  auto n4 = n->split(split_key);
  n3->put(split_key, n4);

  n2->put(6, std::make_shared<std::string>("six"));
  n2->put(7, std::make_shared<std::string>("seven"));
  EXPECT_EQ(n2->need_split(), true);
  auto n5 = n2->split(split_key);
  n3->put(split_key, n5);

  EXPECT_EQ(((n3->get(-3, node_version)).get()), n.get());
  EXPECT_EQ(((n3->get(-2, node_version)).get()), n.get());
  EXPECT_EQ(((n3->get(-1, node_version)).get()), n.get());
  EXPECT_EQ(((n3->get(0, node_version)).get()), n4.get());
  EXPECT_EQ(((n3->get(1, node_version)).get()), n4.get());
  EXPECT_EQ(((n3->get(2, node_version)).get()), n4.get());
  EXPECT_EQ(((n3->get(3, node_version)).get()), n2.get());
  EXPECT_EQ(((n3->get(4, node_version)).get()), n2.get());
  EXPECT_EQ(((n3->get(5, node_version)).get()), n5.get());
  EXPECT_EQ(((n3->get(6, node_version)).get()), n5.get());
  EXPECT_EQ(((n3->get(7, node_version)).get()), n5.get());
  EXPECT_EQ(((n3->get(8, node_version)).get()), n5.get());

  EXPECT_EQ(n3->need_split(), false);

  n5->put(9, std::make_shared<std::string>("nine"));
  n5->put(10, std::make_shared<std::string>("ten"));

  EXPECT_EQ(n5->need_split(), true);
  auto n6 = n5->split(split_key);
  n3->put(split_key, n6);
  EXPECT_EQ(n3->need_split(), true);

  auto n7 = n3->split(split_key);
  std::shared_ptr<Node<int, Node<int, Node<int, std::string>>>> n8(new InternalNode<int, Node<int, Node<int, std::string>>>(n3, split_key, n7));

}