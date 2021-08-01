#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "bpt.h"
#include "write_batch_internal.h"
#include "env.h"
#include "slice.h"

using namespace cowbpt;

extern SliceComparator cmp;

namespace cowbpt {

MATCHER(IsOK, "") { return arg.ok(); }

#define EXPECT_COWBPT_OK(expression) \
  EXPECT_THAT(expression, cowbpt::IsOK())
#define ASSERT_COWBPT_OK(expression) \
  ASSERT_THAT(expression, cowbpt::IsOK())

TEST(WriteBatchTest, Empty) {
  WriteBatch batch;
  ASSERT_EQ(0, WriteBatchInternal::Count(&batch));
}

TEST(WriteBatchTest, Multiple) {
  WriteBatch batch;
  batch.Put(Slice("foo"), Slice("bar"));
  batch.Delete(Slice("box"));
  batch.Put(Slice("baz"), Slice("boo"));
  WriteBatchInternal::SetSequence(&batch, 100);
  ASSERT_EQ(100, WriteBatchInternal::Sequence(&batch));
  ASSERT_EQ(3, WriteBatchInternal::Count(&batch));
  Bpt* b = new Bpt(&cmp);
  ASSERT_COWBPT_OK(WriteBatchInternal::InsertInto(&batch, b));
  ASSERT_EQ(b->get("foo").string(), "bar");
  ASSERT_EQ(b->get("box").string(), "");
  ASSERT_EQ(b->get("baz").string(), "boo");

  batch.Clear();
  batch.Delete(Slice("foo"));
  batch.Delete(Slice("baz"));
  WriteBatchInternal::SetSequence(&batch, 200);
  ASSERT_EQ(200, WriteBatchInternal::Sequence(&batch));
  ASSERT_EQ(2, WriteBatchInternal::Count(&batch));
  ASSERT_COWBPT_OK(WriteBatchInternal::InsertInto(&batch, b));
  ASSERT_EQ(b->get("foo").string(), "");
  ASSERT_EQ(b->get("box").string(), "");
  ASSERT_EQ(b->get("baz").string(), "");
}

TEST(WriteBatchTest, Append) {
  WriteBatch b1, b2;
  WriteBatchInternal::SetSequence(&b1, 200);
  WriteBatchInternal::SetSequence(&b2, 300);
  b1.Append(b2);
  b2.Put("a", "va");
  b1.Append(b2);
  b2.Clear();
  b2.Put("b", "vb");
  b1.Append(b2);
  b2.Delete("foo");
  b1.Append(b2);
  Bpt* b = new Bpt(&cmp);
  ASSERT_COWBPT_OK(WriteBatchInternal::InsertInto(&b1, b));
  ASSERT_EQ(b->get("a").string(), "va");
  ASSERT_EQ(b->get("b").string(), "vb");
  ASSERT_EQ(b->get("foo").string(), "");
}

TEST(WriteBatchTest, ApproximateSize) {
  WriteBatch batch;
  size_t empty_size = batch.ApproximateSize();

  batch.Put(Slice("foo"), Slice("bar"));
  size_t one_key_size = batch.ApproximateSize();
  ASSERT_LT(empty_size, one_key_size);

  batch.Put(Slice("baz"), Slice("boo"));
  size_t two_keys_size = batch.ApproximateSize();
  ASSERT_LT(one_key_size, two_keys_size);

  batch.Delete(Slice("box"));
  size_t post_delete_size = batch.ApproximateSize();
  ASSERT_LT(two_keys_size, post_delete_size);
}

} 