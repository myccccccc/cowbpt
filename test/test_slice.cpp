#include <gtest/gtest.h>
#include <string>
#include <memory>
#include "slice.h"
#include "comparator.h"

using namespace cowbpt;

SliceComparator cmp;

bool equal(Slice a, Slice b) {
    return (!cmp(a, b)) && (!cmp(b, a));
}

bool less(Slice a, Slice b) {
    return cmp(a, b);
}

TEST(SliceTest, SliceBasic) {
    EXPECT_TRUE(equal(Slice(), Slice()));
    EXPECT_TRUE(less(Slice("1"), Slice("2")));
    EXPECT_TRUE(less(Slice(), Slice("2")));
    EXPECT_TRUE(Slice().empty());
    EXPECT_FALSE(Slice("1").empty());
    EXPECT_TRUE(EMPTYSLICE.empty());
    EXPECT_TRUE(equal(Slice(), EMPTYSLICE));
    EXPECT_TRUE(less(EMPTYSLICE, Slice("2")));
    EXPECT_TRUE(less("0.9", "1"));
    std::string a = "abc";
    Slice s(std::move(a));
    EXPECT_TRUE(a.empty());
    EXPECT_EQ(s.string(), "abc");
    Slice s2("cba");
    s2 = s;
    EXPECT_EQ(s.string(), s2.string());

    Slice cnm;
    cnm = Slice("cnm");
    EXPECT_EQ(cnm.string(), "cnm");
}