#include "gtest/gtest.h"
#include "gmock/gmock.h"

#define private public
#define protected public

#include "db.h"
#include "comparator.h"

using namespace cowbpt;

extern SliceComparator cmp;

namespace cowbpt {

// namespace {
//   inline unsigned int to_uint(char ch)
//   {
//       // EDIT: multi-cast fix as per David Hammen's comment
//       return static_cast<unsigned int>(static_cast<unsigned char>(ch));
//   }
//   void PrintHex(std::string s) {
//     using std::cout; using std::endl;
//     using std::string; using std::hex;
//     cout << hex;
//     for (char ch : s)
//     {
//         cout << "0x" << to_uint(ch) << ' '; 
//     }
//     cout << endl;
//   }
// }


MATCHER(IsOK, "") { return arg.ok(); }

#define EXPECT_COWBPT_OK(expression) \
  EXPECT_THAT(expression, cowbpt::IsOK())
#define ASSERT_COWBPT_OK(expression) \
  ASSERT_THAT(expression, cowbpt::IsOK())

TEST(DBImplTest, DBImplOpenAndDestory) {
  DB* db;
  Status s = DB::Open(Options(), "testDB", &db);
  ASSERT_COWBPT_OK(s);
  delete db;
  DestroyDB("testDB", Options());
}

TEST(DBImplTest, DBImplCRUD) {
  DestroyDB("testDB", Options());
  DB* db;
  Status s = DB::Open(Options(), "testDB", &db);
  ASSERT_COWBPT_OK(s);

  WriteOptions wo;
  wo.sync = true;
  ASSERT_COWBPT_OK(db->Put(wo, "1", "one"));
  ASSERT_COWBPT_OK(db->Put(wo, "1", "one"));
  ASSERT_COWBPT_OK(db->Put(wo, "2", "two"));
  ASSERT_COWBPT_OK(db->Put(wo, "3", "three"));
  ASSERT_COWBPT_OK(db->Put(wo, "4", "four"));
  ASSERT_COWBPT_OK(db->Put(wo, "5", "five"));
  ASSERT_COWBPT_OK(db->Put(wo, "6", "six"));
  ASSERT_COWBPT_OK(db->Put(wo, "7", "seven"));
  ASSERT_COWBPT_OK(db->Put(wo, "8", "eight"));
  ASSERT_COWBPT_OK(db->Put(wo, "9", "nine"));
  ASSERT_COWBPT_OK(db->Put(wo, "10", "ten"));
  ASSERT_COWBPT_OK(db->Put(wo, "11", "eleven"));

  std::string value;
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "1", &value));
  EXPECT_EQ(value, "one");
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "2", &value));
  EXPECT_EQ(value, "two");
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "3", &value));
  EXPECT_EQ(value, "three");
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "4", &value));
  EXPECT_EQ(value, "four");
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "5", &value));
  EXPECT_EQ(value, "five");
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "6", &value));
  EXPECT_EQ(value, "six");
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "7", &value));
  EXPECT_EQ(value, "seven");
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "8", &value));
  EXPECT_EQ(value, "eight");
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "9", &value));
  EXPECT_EQ(value, "nine");
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "10", &value));
  EXPECT_EQ(value, "ten");
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "11", &value));
  EXPECT_EQ(value, "eleven");

  delete db;

  s = DB::Open(Options(), "testDB", &db);
  ASSERT_COWBPT_OK(s);
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "1", &value));
  EXPECT_EQ(value, "one");
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "2", &value));
  EXPECT_EQ(value, "two");
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "3", &value));
  EXPECT_EQ(value, "three");
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "4", &value));
  EXPECT_EQ(value, "four");
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "5", &value));
  EXPECT_EQ(value, "five");
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "6", &value));
  EXPECT_EQ(value, "six");
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "7", &value));
  EXPECT_EQ(value, "seven");
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "8", &value));
  EXPECT_EQ(value, "eight");
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "9", &value));
  EXPECT_EQ(value, "nine");
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "10", &value));
  EXPECT_EQ(value, "ten");
  value.clear();
  ASSERT_COWBPT_OK(db->Get(ReadOptions(), "11", &value));
  EXPECT_EQ(value, "eleven");

  ASSERT_COWBPT_OK(db->Delete(wo, "1"));
  ASSERT_COWBPT_OK(db->Delete(wo, "1"));
  ASSERT_COWBPT_OK(db->Delete(wo, "2"));
  ASSERT_COWBPT_OK(db->Delete(wo, "3"));
  ASSERT_COWBPT_OK(db->Delete(wo, "4"));
  ASSERT_COWBPT_OK(db->Delete(wo, "5"));
  ASSERT_COWBPT_OK(db->Delete(wo, "6"));
  ASSERT_COWBPT_OK(db->Delete(wo, "7"));
  ASSERT_COWBPT_OK(db->Delete(wo, "8"));
  ASSERT_COWBPT_OK(db->Delete(wo, "9"));
  ASSERT_COWBPT_OK(db->Delete(wo, "10"));
  ASSERT_COWBPT_OK(db->Delete(wo, "11"));
  ASSERT_TRUE(db->Get(ReadOptions(), "1", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "2", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "3", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "4", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "5", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "6", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "7", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "8", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "9", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "10", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "11", &value).IsNotFound());

  delete db;

  s = DB::Open(Options(), "testDB", &db);
  ASSERT_COWBPT_OK(s);
  ASSERT_TRUE(db->Get(ReadOptions(), "1", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "2", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "3", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "4", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "5", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "6", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "7", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "8", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "9", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "10", &value).IsNotFound());
  ASSERT_TRUE(db->Get(ReadOptions(), "11", &value).IsNotFound());

}


} 