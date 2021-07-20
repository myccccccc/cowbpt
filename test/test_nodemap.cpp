#include <gtest/gtest.h>
#include <string>
#include <memory>

#define private public
#define protected public

#include "nodemap.h"

using namespace cowbpt;

TEST(NodeMapTest, LeafNodeMapCRUD) {
  LeafNodeMap<int, std::shared_ptr<std::string>>* lnm = new LeafNodeMap<int, std::shared_ptr<std::string>>;
  NodeMap<int, std::shared_ptr<std::string>>* nm = lnm;
  nm->put(2, std::make_shared<std::string>("two"));
  nm->put(4, std::make_shared<std::string>("four"));
  nm->put(6, std::make_shared<std::string>("six"));
  nm->put(1, std::make_shared<std::string>("one"));
  nm->put(3, std::make_shared<std::string>("three"));
  nm->put(5, std::make_shared<std::string>("five"));
  EXPECT_EQ(nm->size(), 6);
  EXPECT_EQ(*(nm->get(1)), "one");
  EXPECT_EQ(*(nm->get(2)), "two");
  EXPECT_EQ(*(nm->get(3)), "three");
  EXPECT_EQ(*(nm->get(4)), "four");
  EXPECT_EQ(*(nm->get(5)), "five");
  EXPECT_EQ(*(nm->get(6)), "six");
  for (auto i = 0; i < lnm->_v.size(); i++) {
    EXPECT_EQ(lnm->_v[i].first, i+1);
  }

  int split_key;
  LeafNodeMap<int, std::shared_ptr<std::string>>* lnm2 = static_cast<LeafNodeMap<int, std::shared_ptr<std::string>>*> (nm->split(split_key));
  EXPECT_EQ(split_key, 4);
  NodeMap<int, std::shared_ptr<std::string>>* nm2 = lnm2;
  EXPECT_EQ(nm->size(), 3);
  EXPECT_EQ(nm2->size(), 3);
  EXPECT_EQ(*(nm->get(1)), "one");
  EXPECT_EQ(*(nm->get(2)), "two");
  EXPECT_EQ(*(nm->get(3)), "three");
  EXPECT_EQ((nm->get(4)), nullptr);
  EXPECT_EQ((nm->get(5)), nullptr);
  EXPECT_EQ((nm->get(6)), nullptr);
  EXPECT_EQ((nm2->get(1)), nullptr);
  EXPECT_EQ((nm2->get(2)), nullptr);
  EXPECT_EQ((nm2->get(3)), nullptr);
  EXPECT_EQ(*(nm2->get(4)), "four");
  EXPECT_EQ(*(nm2->get(5)), "five");
  EXPECT_EQ(*(nm2->get(6)), "six");

  nm->put(1, std::make_shared<std::string>("ones"));
  nm->put(2, std::make_shared<std::string>("twos"));
  nm->put(3, std::make_shared<std::string>("threes"));
  EXPECT_EQ(*(nm->get(1)), "ones");
  EXPECT_EQ(*(nm->get(2)), "twos");
  EXPECT_EQ(*(nm->get(3)), "threes");
}

TEST(NodeMapTest, InternalNodeMapCRUD) {
  std::shared_ptr<LeafNodeMap<int, std::shared_ptr<std::string>>> lnm = std::make_shared<LeafNodeMap<int, std::shared_ptr<std::string>>>();
  std::shared_ptr<NodeMap<int, std::shared_ptr<std::string>>> nm = lnm;
  nm->put(2, std::make_shared<std::string>("two"));
  nm->put(4, std::make_shared<std::string>("four"));
  nm->put(6, std::make_shared<std::string>("six"));
  nm->put(1, std::make_shared<std::string>("one"));
  nm->put(3, std::make_shared<std::string>("three"));
  nm->put(5, std::make_shared<std::string>("five"));
  int split_key;
  std::shared_ptr<LeafNodeMap<int, std::shared_ptr<std::string>>> lnm2(static_cast<LeafNodeMap<int, std::shared_ptr<std::string>>*> (nm->split(split_key)));
  std::shared_ptr<NodeMap<int, std::shared_ptr<std::string>>> nm2 = lnm2;
  
  InternalNodeMap<int, std::shared_ptr<NodeMap<int, std::shared_ptr<std::string>>>>* inm = new InternalNodeMap<int, std::shared_ptr<NodeMap<int, std::shared_ptr<std::string>>>>(nm, split_key, nm2);
  NodeMap<int, std::shared_ptr<NodeMap<int, std::shared_ptr<std::string>>>>* nm3 = inm;

  EXPECT_EQ(nm3->get(0).get(), nm.get());
  EXPECT_EQ(nm3->get(1).get(), nm.get());
  EXPECT_EQ(nm3->get(2).get(), nm.get());
  EXPECT_EQ(nm3->get(3).get(), nm.get());
  EXPECT_EQ(nm3->get(4).get(), nm2.get());
  EXPECT_EQ(nm3->get(5).get(), nm2.get());
  EXPECT_EQ(nm3->get(6).get(), nm2.get());
  EXPECT_EQ(nm3->get(7).get(), nm2.get());

  EXPECT_EQ(inm->_v[0].first, int());
  EXPECT_EQ(inm->_v[1].first, 4);

  nm->put(-2, std::make_shared<std::string>("fu er"));
  nm->put(-1, std::make_shared<std::string>("fu yi"));
  nm->put(0, std::make_shared<std::string>("zero"));
  std::shared_ptr<LeafNodeMap<int, std::shared_ptr<std::string>>> lnm3(static_cast<LeafNodeMap<int, std::shared_ptr<std::string>>*> (nm->split(split_key)));
  std::shared_ptr<NodeMap<int, std::shared_ptr<std::string>>> nm4 = lnm3;
  nm3->put(split_key, nm4);

  nm2->put(7, std::make_shared<std::string>("seven"));
  nm2->put(8, std::make_shared<std::string>("eight"));
  nm2->put(9, std::make_shared<std::string>("nine"));
  std::shared_ptr<LeafNodeMap<int, std::shared_ptr<std::string>>> lnm4(static_cast<LeafNodeMap<int, std::shared_ptr<std::string>>*> (nm2->split(split_key)));
  std::shared_ptr<NodeMap<int, std::shared_ptr<std::string>>> nm5 = lnm4;
  nm3->put(split_key, nm5);

  EXPECT_EQ(inm->_v[0].first, int());
  EXPECT_EQ(inm->_v[1].first, 1);
  EXPECT_EQ(inm->_v[2].first, 4);
  EXPECT_EQ(inm->_v[3].first, 7);
}