#include <gtest/gtest.h>
#include <string>
#include <memory>

#define private public
#define protected public

#include "nodemap.h"
#include "slice.h"
#include "node.h"
#include "comparator.h"

using namespace cowbpt;

bool equal(Slice a, Slice b);
extern SliceComparator cmp;

TEST(NodeMapTest, LeafNodeMapCRUD) {
  LeafNodeMap<Slice, Slice, SliceComparator>* lnm = new LeafNodeMap<Slice, Slice, SliceComparator>(cmp);
  lnm->put("2", Slice("two"));
  lnm->put("4", Slice("four"));
  lnm->put("6", Slice("six"));
  lnm->put("1", Slice("one"));
  lnm->put("3", Slice("three"));
  lnm->put("5", Slice("five"));
  EXPECT_TRUE(equal(lnm->get("1"), "one"));
  EXPECT_TRUE(equal(lnm->get("2"), "two"));
  EXPECT_TRUE(equal(lnm->get("3"), "three"));
  EXPECT_TRUE(equal(lnm->get("4"), "four"));
  EXPECT_TRUE(equal(lnm->get("5"), "five"));
  EXPECT_TRUE(equal(lnm->get("6"), "six"));
  EXPECT_TRUE(lnm->get("7").empty());
  EXPECT_EQ(lnm->size(), 6);
  EXPECT_TRUE(equal(lnm->_v[0].first, "1"));
  EXPECT_TRUE(equal(lnm->_v[1].first, "2"));
  EXPECT_TRUE(equal(lnm->_v[2].first, "3"));
  EXPECT_TRUE(equal(lnm->_v[3].first, "4"));
  EXPECT_TRUE(equal(lnm->_v[4].first, "5"));
  EXPECT_TRUE(equal(lnm->_v[5].first, "6"));

  Slice split_key;
  LeafNodeMap<Slice, Slice, SliceComparator>* lnm2 = lnm->split(split_key);
  EXPECT_TRUE(equal(split_key, "4"));
  EXPECT_EQ(lnm->size(), 3);
  EXPECT_EQ(lnm2->size(), 3);
  EXPECT_TRUE(equal(lnm->get("1"), "one"));
  EXPECT_TRUE(equal(lnm->get("2"), "two"));
  EXPECT_TRUE(equal(lnm->get("3"), "three"));
  EXPECT_TRUE(lnm->get("4").empty());
  EXPECT_TRUE(lnm->get("5").empty());
  EXPECT_TRUE(lnm->get("6").empty());
  EXPECT_TRUE(lnm2->get("1").empty());
  EXPECT_TRUE(lnm2->get("2").empty());
  EXPECT_TRUE(lnm2->get("3").empty());
  EXPECT_TRUE(equal(lnm2->get("4"), "four"));
  EXPECT_TRUE(equal(lnm2->get("5"), "five"));
  EXPECT_TRUE(equal(lnm2->get("6"), "six"));

  lnm->put("1", Slice("ones"));
  lnm->put("2", Slice("twos"));
  lnm->put("3", Slice("threes"));
  EXPECT_TRUE(equal(lnm->get("1"), "ones"));
  EXPECT_TRUE(equal(lnm->get("2"), "twos"));
  EXPECT_TRUE(equal(lnm->get("3"), "threes"));
}

TEST(NodeMapTest, InternalNodeMapCRUD) {
  std::shared_ptr<Node<SliceComparator>> n(new LeafNode<SliceComparator>(cmp));
  n->put("1", "one");
  n->put("2", "two");
  n->put("3", "three");
  n->put("4", "four");
  n->put("5", "five");
  EXPECT_EQ(n->need_split(), true);
  Slice split_key;
  auto n2 = n->split(split_key);

  InternalNodeMap<Slice, std::shared_ptr<Node<SliceComparator>>, SliceComparator>* nm3 = new InternalNodeMap<Slice, std::shared_ptr<Node<SliceComparator>>, SliceComparator>(cmp, n, split_key, n2);

  EXPECT_EQ(nm3->get("0").get(), n.get());
  EXPECT_EQ(nm3->get("1").get(), n.get());
  EXPECT_EQ(nm3->get("2").get(), n.get());
  EXPECT_EQ(nm3->get("3").get(), n2.get());
  EXPECT_EQ(nm3->get("4").get(), n2.get());
  EXPECT_EQ(nm3->get("5").get(), n2.get());
  EXPECT_EQ(nm3->get("6").get(), n2.get());

  EXPECT_TRUE(equal(nm3->_v[0].first, Slice()));
  EXPECT_TRUE(equal(nm3->_v[1].first, "3"));


  n->put("0.7", Slice("fu er"));
  n->put("0.8", Slice("fu yi"));
  n->put("0.9", Slice("zero"));
  EXPECT_EQ(n->need_split(), true);
  auto n3 = n->split(split_key);
  nm3->put(split_key, n3);

  n2->put("6", Slice("six"));
  n2->put("7", Slice("seven"));
  EXPECT_EQ(n2->need_split(), true);
  auto n4 = n2->split(split_key);
  nm3->put(split_key, n4);

  EXPECT_TRUE(equal(nm3->_v[0].first, Slice()));
  EXPECT_TRUE(equal(nm3->_v[1].first, "0.9"));
  EXPECT_TRUE(equal(nm3->_v[2].first, "3"));
  EXPECT_TRUE(equal(nm3->_v[3].first, "5"));

  InternalNodeMap<Slice, std::shared_ptr<Node<SliceComparator>>, SliceComparator>* nm4 = nm3->split(split_key);
  EXPECT_EQ(nm3->size(), 2);
  EXPECT_EQ(nm4->size(), 2);
}