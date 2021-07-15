#include <gtest/gtest.h>
#include <string>
#include <memory>
#include "node.h"

// Demonstrate some basic assertions.
TEST(NodeTest, LeafNodeCRUD) {
  cowbpt::Node<int, std::string> ln;
  EXPECT_EQ(ln.get(1), nullptr);
  ln.put(1, std::make_shared<std::string>("one"));
  EXPECT_EQ(*ln.get(1), "one");
  ln.put(1, std::make_shared<std::string>("oneplus"));
  EXPECT_EQ(*ln.get(1), "oneplus");
  ln.erase(1);
  EXPECT_EQ(ln.get(1), nullptr);
}