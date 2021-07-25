#include <gtest/gtest.h>
#include <string>
#include <memory>
#include <iostream>
#include <ctime>
#include <unistd.h>
#include <unordered_set>
#include <thread>

#define private public
#define protected public

#include "bpt.h"


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

using namespace cowbpt;

bool equal(Slice a, Slice b);
extern SliceComparator cmp;

TEST(BptTest, BptCRUD) {
    Bpt b(&cmp);
    b.put("1", "one");
    b.put("2", "two");
    b.put("3", "three");
    b.put("4", "four");
    b.put("5", "five");
    
    b.put("6", "six");
    
    b.put("7", "seven");
    
    b.put("8", "eight");
    
    b.put("9", "nine");
    
    b.put("10", "ten");
    
    b.put("11", "eleven");
    EXPECT_EQ(b.get("1").string(), "one");
    EXPECT_EQ(b.get("2").string(), "two");
    EXPECT_EQ(b.get("3").string(), "three");
    EXPECT_EQ(b.get("4").string(), "four");
    EXPECT_EQ(b.get("5").string(), "five");
    EXPECT_EQ(b.get("6").string(), "six");
    EXPECT_EQ(b.get("7").string(), "seven");
    EXPECT_EQ(b.get("8").string(), "eight");
    EXPECT_EQ(b.get("9").string(), "nine");
    EXPECT_EQ(b.get("10").string(), "ten");
    EXPECT_EQ(b.get("11").string(), "eleven");

    // std::cout << b.dump() << std::endl;
    
    b.erase("0");
    b.erase("1");
    b.erase("2");
    b.erase("3");
    b.erase("4");
    b.erase("5");
    b.erase("6");
    b.erase("7");
    b.erase("8");
    b.erase("9");
    b.erase("10");
    b.erase("11");
    b.erase("2");
    EXPECT_EQ(b.get("1").string(), "");
    EXPECT_EQ(b.get("2").string(), "");
    EXPECT_EQ(b.get("3").string(), "");
    EXPECT_EQ(b.get("4").string(), "");
    EXPECT_EQ(b.get("5").string(), "");
    EXPECT_EQ(b.get("6").string(), "");
    EXPECT_EQ(b.get("7").string(), "");
    EXPECT_EQ(b.get("8").string(), "");
    EXPECT_EQ(b.get("9").string(), "");
    EXPECT_EQ(b.get("10").string(), "");
    EXPECT_EQ(b.get("11").string(), "");

    // std::cout << b.dump() << std::endl;
}

TEST(BptTest, BptRandomCRUD) {
    Bpt b(&cmp);
    std::unordered_set<std::string> st;
    while (st.size() < 100000) {
        std::string s = gen_random(15);
        if (st.count(s) == 0) {
            st.insert(s);
        }
    }


    for (std::string s : st) {
        b.put(s, s);
    }
    for(std::string s: st) {
        EXPECT_EQ(b.get(s).string(), s);
    }
    EXPECT_TRUE(b.get("123").empty());



    for (std::string s : st) {
        b.put(s, "cnm");
    }
    for(std::string s: st) {
        EXPECT_NE(b.get(s).string(), s);
        EXPECT_EQ(b.get(s).string(), "cnm");
    }

    for (std::string s : st) {
        b.erase(s);
    }
    for(std::string s: st) {
        EXPECT_EQ(b.get(s).string(), "");
    }

    // std::cout << b.dump() << std::endl;

}

void write_thread(Bpt* bt, std::vector<std::string>* v1, std::vector<std::string>* v2, std::mutex* m) {
    for(auto s : *v1) {
        bt->put(s, s);
        m->lock();
        v2->push_back(s);
        m->unlock();
    }
}

void del_thread(Bpt* bt, std::vector<std::string>* v1, std::vector<std::string>* v2, std::mutex* m) {
    for(auto s : *v1) {
        bt->erase(s);
        m->lock();
        v2->push_back(s);
        m->unlock();
    }
}

void read_thread(Bpt* bt, std::vector<std::string>* v2, std::mutex* m) {
    while (true) {
        m->lock();
        int size = v2->size();
        std::string s = (*v2)[rand_int() % size];
        m->unlock();
        EXPECT_EQ(bt->get(s).string(), s);
        if (size == 100000) {
            break;
        }
    }
    
}

void read_del_thread(Bpt* bt, std::vector<std::string>* v2, std::mutex* m) {
    while (true) {
        m->lock();
        int size = v2->size();
        std::string s = (*v2)[rand_int() % size];
        m->unlock();
        EXPECT_EQ(bt->get(s).string(), "");
        if (size == 100000) {
            break;
        }
    }
}

TEST(BptTest, BptRandomCurrentCRUD) {
    Bpt b(&cmp);
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

    for(auto s : del_st) {
        b.put(s, s);
    }
    std::vector<std::thread> del_threads;
    assert(del_sts.size() == 100);
    for (size_t i = 0; i < del_sts.size(); i++) {
        del_threads.push_back(std::thread(del_thread, &b, &del_sts[i], &st_delten, &st_delten_mutex));
    }

    std::vector<std::thread> read_del_threads;
    for(auto i = 0; i < 100; i++) {
        read_del_threads.push_back(std::thread(read_del_thread, &b, &st_delten, &st_delten_mutex));
    }

    std::vector<std::thread> write_threads;
    assert(sts.size() == 100);
    for (size_t i = 0; i < sts.size(); i++) {
        write_threads.push_back(std::thread(write_thread, &b, &sts[i], &st_writen, &st_written_mutex));
    }

    std::vector<std::thread> read_threads;
    for(auto i = 0; i < 100; i++) {
        read_threads.push_back(std::thread(read_thread, &b, &st_writen, &st_written_mutex));
    }
    
    // TODO: current update thread

    for(size_t i  = 0; i < del_threads.size(); i++) {
        del_threads[i].join();
    }

    for(size_t i  = 0; i < read_del_threads.size(); i++) {
        read_del_threads[i].join();
    }

    for(size_t i  = 0; i < write_threads.size(); i++) {
        write_threads[i].join();
    }

    for(size_t i  = 0; i < read_threads.size(); i++) {
        read_threads[i].join();
    }

    for(std::string s: st) {
        EXPECT_EQ(b.get(s).string(), s);
    }
    for(std::string s: del_st) {
        EXPECT_EQ(b.get(s).string(), "");
    }
    EXPECT_TRUE(b.get("123").empty());


    del_threads.clear();
    for (size_t i = 0; i < sts.size(); i++) {
        del_threads.push_back(std::thread(del_thread, &b, &sts[i], &st_writen, &st_written_mutex));
    }
    for(size_t i  = 0; i < del_threads.size(); i++) {
        del_threads[i].join();
    }
    for(std::string s: st) {
        EXPECT_EQ(b.get(s).string(), "");
    }
    for(std::string s: del_st) {
        EXPECT_EQ(b.get(s).string(), "");
    }
    
    // std::cout << b.dump() << std::endl;

}