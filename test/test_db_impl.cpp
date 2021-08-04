#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include <unordered_set>
#include <mutex>
#include <thread>

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

namespace {
    std::string testdb_name;
}
TEST(DBImplTest, DBImplOpenAndDestory) {
  testdb_name = "DBImplOpenAndDestory";
  DB* db;
  Status s = DB::Open(Options(), testdb_name, &db);
  ASSERT_COWBPT_OK(s);
  delete db;
  DestroyDB(testdb_name, Options());
}

TEST(DBImplTest, DBImplCRUD) {
  testdb_name = "DBImplCRUD";
  DestroyDB(testdb_name, Options());
  DB* db;
  Status s = DB::Open(Options(), testdb_name, &db);
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

  s = DB::Open(Options(), testdb_name, &db);
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

  s = DB::Open(Options(), testdb_name, &db);
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

namespace {
  std::string gen_random(const int len) {
    
    std::string tmp_s;
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    /* using nano-seconds instead of seconds */
    srand((time_t)ts.tv_nsec);

    tmp_s.reserve(len);

    for (int i = 0; i < len; ++i) 
        tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
    
    
    return tmp_s;
    
  }

  int rand_int() {
      struct timespec ts;
      clock_gettime(CLOCK_MONOTONIC, &ts);

      /* using nano-seconds instead of seconds */
      srand((time_t)ts.tv_nsec);

      return rand();
  }
}

TEST(DBImplTest, DBImplRandomCRUD) {
    testdb_name = "DBImplRandomCRUD";
  std::unordered_set<std::string> st;
  while (st.size() < 100000) {
      std::string s = gen_random(15);
      if (st.count(s) == 0) {
          st.insert(s);
      }
  }


  DestroyDB(testdb_name, Options());
  DB* db;
  ASSERT_COWBPT_OK(DB::Open(Options(), testdb_name, &db));

  WriteOptions wo;
  wo.sync = false;
  for (std::string s : st) {
      ASSERT_COWBPT_OK(db->Put(wo, s, s));
  }

  delete db;
  ASSERT_COWBPT_OK(DB::Open(Options(), testdb_name, &db));
  std::string value;
  for(std::string s: st) {
      value.clear();
      ASSERT_COWBPT_OK(db->Get(ReadOptions(), s, &value));
      EXPECT_EQ(value, s);
  }
  EXPECT_TRUE(db->Get(ReadOptions(), "123", &value).IsNotFound());
  for (std::string s : st) {
      ASSERT_COWBPT_OK(db->Put(wo, s, "cnm"));
  }

  delete db;
  ASSERT_COWBPT_OK(DB::Open(Options(), testdb_name, &db));
  for(std::string s: st) {
      value.clear();
      ASSERT_COWBPT_OK(db->Get(ReadOptions(), s, &value));
      EXPECT_EQ(value, "cnm");
  }
  EXPECT_TRUE(db->Get(ReadOptions(), "123", &value).IsNotFound());
  for (std::string s : st) {
      ASSERT_COWBPT_OK(db->Delete(wo, s));
  }

  delete db;
  ASSERT_COWBPT_OK(DB::Open(Options(), testdb_name, &db));
  for(std::string s: st) {
      value.clear();
      ASSERT_TRUE(db->Get(ReadOptions(), s, &value).IsNotFound());
  }
}

namespace {
  void write_thread(DB* db, std::vector<std::string>* v1, std::vector<std::string>* v2, std::mutex* m) {
      WriteOptions wo;
      wo.sync = false;
      for(auto s : *v1) {
          ASSERT_COWBPT_OK(db->Put(wo, s, s));
          m->lock();
          v2->push_back(s);
          m->unlock();
      }
  }

  void del_thread(DB* db, std::vector<std::string>* v1, std::vector<std::string>* v2, std::mutex* m) {
      WriteOptions wo;
      wo.sync = false;
      for(auto s : *v1) {
          ASSERT_COWBPT_OK(db->Delete(wo, s));
          m->lock();
          v2->push_back(s);
          m->unlock();
      }
  }

  void read_thread(DB* db, std::vector<std::string>* v2, std::mutex* m) {
      while (true) {
          m->lock();
          if (v2->empty()) {
              m->unlock();
              continue;
          }
          int size = v2->size();
          std::string s = (*v2)[rand_int() % size];
          m->unlock();
          std::string value;
          ASSERT_COWBPT_OK(db->Get(ReadOptions(), s, &value));
          ASSERT_EQ(value, s);
          if (size == 100000) {
              break;
          }
      }
      
  }

//   void read_del_thread(DB* db, std::vector<std::string>* v2, std::mutex* m) {
//       while (true) {
//           m->lock();
//           if (v2->empty()) {
//               m->unlock();
//               continue;
//           }
//           int size = v2->size();
//           std::string s = (*v2)[rand_int() % size];
//           m->unlock();
//           std::string value;
//           ASSERT_TRUE(db->Get(ReadOptions(), s, &value).IsNotFound());
//           if (size == 100000) {
//               break;
//           }
//       }
//   }
}

TEST(DBImplTest, DBImplRandomCurrentCRUD) {
    testdb_name = "DBImplRandomCurrentCRUD";
  DestroyDB(testdb_name, Options());
  DB* db;
  ASSERT_COWBPT_OK(DB::Open(Options(), testdb_name, &db));

  std::unordered_set<std::string> st;
  while (st.size() < 100000) {
      std::string s = gen_random(15);
      if (st.count(s) == 0) {
          st.insert(s);
      }
  }

  std::unordered_set<std::string> del_st;
  while (del_st.size() < 100000) {
      std::string s = gen_random(15);
      if (st.count(s) == 0 && del_st.count(s) == 0) {
          del_st.insert(s);
      }
  }

  std::vector<std::vector<std::string>> sts; // 100 write thread
  int i = 0;
  for (auto s : st) {
      if (i % 1000 == 0) {
          sts.push_back(std::vector<std::string>());
      }
      sts.back().push_back(s);
      i++;
  }

  std::vector<std::vector<std::string>> del_sts; // 100 del thread
  i = 0;
  for (auto s : del_st) {
      if (i % 1000 == 0) {
          del_sts.push_back(std::vector<std::string>());
      }
      del_sts.back().push_back(s);
      i++;
  }

  std::vector<std::string> st_writen;
  std::mutex st_written_mutex;

  std::vector<std::string> st_delten;
  std::mutex st_delten_mutex;

  WriteOptions wo;
  wo.sync = false;
  for(auto s : del_st) {
      ASSERT_COWBPT_OK(db->Put(wo, s, s));
  }

  for(std::string s: del_st) {
      std::string value;
      value.clear();
      ASSERT_COWBPT_OK(db->Get(ReadOptions(), s, &value));
      ASSERT_EQ(value, s);
  }

  std::vector<std::thread> del_threads;
  assert(del_sts.size() == 100);
  for (size_t i = 0; i < del_sts.size(); i++) {
      del_threads.push_back(std::thread(del_thread, db, &del_sts[i], &st_delten, &st_delten_mutex));
  }

//   std::vector<std::thread> read_del_threads;
//   for(auto i = 0; i < 100; i++) {
//       read_del_threads.push_back(std::thread(read_del_thread, db, &st_delten, &st_delten_mutex));
//   }

  std::vector<std::thread> write_threads;
  assert(sts.size() == 100);
  for (size_t i = 0; i < sts.size(); i++) {
      write_threads.push_back(std::thread(write_thread, db, &sts[i], &st_writen, &st_written_mutex));
  }

  std::vector<std::thread> read_threads;
  for(auto i = 0; i < 100; i++) {
      read_threads.push_back(std::thread(read_thread, db, &st_writen, &st_written_mutex));
  }
  
  // TODO: current update thread

  for(size_t i  = 0; i < del_threads.size(); i++) {
      del_threads[i].join();
  }

//   for(size_t i  = 0; i < read_del_threads.size(); i++) {
//       read_del_threads[i].join();
//   }

  for(size_t i  = 0; i < write_threads.size(); i++) {
      write_threads[i].join();
  }

  for(size_t i  = 0; i < read_threads.size(); i++) {
      read_threads[i].join();
  }


  std::string value;
  for(std::string s: st) {
      value.clear();
      ASSERT_COWBPT_OK(db->Get(ReadOptions(), s, &value));
      ASSERT_EQ(value, s);
  }
  for(std::string s: del_st) {
      value.clear();
      ASSERT_TRUE(db->Get(ReadOptions(), s, &value).IsNotFound());
  }
  
  delete db;
  ASSERT_COWBPT_OK(DB::Open(Options(), testdb_name, &db));
  for(std::string s: st) {
      value.clear();
      ASSERT_COWBPT_OK(db->Get(ReadOptions(), s, &value));
      ASSERT_EQ(value, s);
  }
  for(std::string s: del_st) {
      value.clear();
      ASSERT_TRUE(db->Get(ReadOptions(), s, &value).IsNotFound());
  }

}

TEST(DBImplTest, DBImplCheckpoint) {
    testdb_name = "DBImplCheckpoint";
    DestroyDB(testdb_name, Options());
    DB* db;
    ASSERT_COWBPT_OK(DB::Open(Options(), testdb_name, &db));

    WriteOptions wo;
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

    ASSERT_COWBPT_OK(db->ManualCheckPoint());

    ASSERT_COWBPT_OK(db->Put(wo, "12", "twelve"));
    ASSERT_COWBPT_OK(db->Put(wo, "13", "thirteen"));

    delete db;

    ASSERT_COWBPT_OK(DB::Open(Options(), testdb_name, &db));
    ReadOptions ro;
    std::string result;
    ASSERT_COWBPT_OK(db->Get(ReadOptions(), "1", &result));
    ASSERT_EQ(result, "one");
    ASSERT_COWBPT_OK(db->Get(ReadOptions(), "2", &result));
    ASSERT_EQ(result, "two");
    ASSERT_COWBPT_OK(db->Get(ReadOptions(), "3", &result));
    ASSERT_EQ(result, "three");
    ASSERT_COWBPT_OK(db->Get(ReadOptions(), "4", &result));
    ASSERT_EQ(result, "four");
    ASSERT_COWBPT_OK(db->Get(ReadOptions(), "5", &result));
    ASSERT_EQ(result, "five");
    ASSERT_COWBPT_OK(db->Get(ReadOptions(), "6", &result));
    ASSERT_EQ(result, "six");
    ASSERT_COWBPT_OK(db->Get(ReadOptions(), "7", &result));
    ASSERT_EQ(result, "seven");
    ASSERT_COWBPT_OK(db->Get(ReadOptions(), "8", &result));
    ASSERT_EQ(result, "eight");
    ASSERT_COWBPT_OK(db->Get(ReadOptions(), "9", &result));
    ASSERT_EQ(result, "nine");
    ASSERT_COWBPT_OK(db->Get(ReadOptions(), "10", &result));
    ASSERT_EQ(result, "ten");
    ASSERT_COWBPT_OK(db->Get(ReadOptions(), "11", &result));
    ASSERT_EQ(result, "eleven");
    ASSERT_COWBPT_OK(db->Get(ReadOptions(), "12", &result));
    ASSERT_EQ(result, "twelve");
    ASSERT_COWBPT_OK(db->Get(ReadOptions(), "13", &result));
    ASSERT_EQ(result, "thirteen");

}

} 