#include <gtest/gtest.h>
#include <string>
#include <memory>
#include "node.h"

TEST(NodeTest, LeafNodeCRUD) {
  cowbpt::Node<int, std::string> ln;
  int node_version;
  EXPECT_EQ(ln.get(1, node_version), nullptr);
  EXPECT_EQ(node_version, 1);
  ln.put(1, std::make_shared<std::string>("one"));
  EXPECT_EQ(*ln.get(1, node_version), "one");
  EXPECT_EQ(node_version, 2);
  ln.put(1, std::make_shared<std::string>("oneplus"));
  EXPECT_EQ(*ln.get(1, node_version), "oneplus");
  EXPECT_EQ(node_version, 3);
  ln.erase(1);
  EXPECT_EQ(ln.get(1, node_version), nullptr);
  EXPECT_EQ(node_version, 4);
  EXPECT_TRUE(ln.check_version(4));
}

TEST(NodeTest, LeafNodeClone) {
  cowbpt::Node<int, std::string> ln;
  ln.put(1, std::make_shared<std::string>("one"));
  auto cloned_ln = ln.clone();
  ln.put(1, std::make_shared<std::string>("oneplus"));
  int node_version;
  int cloned_node_version;
  EXPECT_EQ(*(ln.get(1, node_version)), "oneplus");
  EXPECT_EQ(*(cloned_ln->get(1, cloned_node_version)), "one");
  EXPECT_EQ(node_version, 3);
  EXPECT_EQ(cloned_node_version, 1);
}