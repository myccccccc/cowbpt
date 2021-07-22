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


}

void write_thread(Bpt* bt, std::vector<std::string>* v1, std::vector<std::string>* v2, std::mutex* m) {
    for(auto s : *v1) {
        bt->put(s, s);
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

TEST(BptTest, BptRandomCurrentCRUD) {
    Bpt b(&cmp);
    std::unordered_set<std::string> st;
    while (st.size() < 100000) {
        std::string s = gen_random(15);
        if (st.count(s) == 0) {
            st.insert(s);
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

    std::vector<std::string> st_writen;
    std::mutex st_written_mutex;

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

    for(size_t i  = 0; i < write_threads.size(); i++) {
        write_threads[i].join();
    }

    for(size_t i  = 0; i < read_threads.size(); i++) {
        read_threads[i].join();
    }

    for(std::string s: st) {
        EXPECT_EQ(b.get(s).string(), s);
    }
    EXPECT_TRUE(b.get("123").empty());
}