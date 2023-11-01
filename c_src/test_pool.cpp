#include <iostream>
#include <filesystem>

#include "pool_fifo.hpp"

using namespace arterial;

#define ASSERT_EQUAL(a, b) \
  if ((a)!=(b)) { \
    std::cerr << #a  << " != " << #b  << " ||| " \
              << (a) << " != " << (b) << " ["    \
              << std::filesystem::path(__FILE__).stem().c_str() \
              << ": " << __LINE__ << "]\n"; \
    exit(1); \
  } \
  else { \
    (void)0; \
  }

struct ObjT {
    int value;
};

constexpr const size_t SIZE = 5;

bool debug = false;

void test_lifo_pool(const char*);
void test_fifo_pool(const char*);

int main(int argc, char* argv[]) {

  debug = argc > 1 && std::string(argv[1]) == "-d";

  test_lifo_pool(argv[0]);
  test_fifo_pool(argv[0]);
}

//-----------------------------------------------------------------------------
// Test LIFO
//-----------------------------------------------------------------------------
void test_lifo_pool(const char* exec) {
  ObjectPoolLIFO<ObjT> pool(SIZE);

  ASSERT_EQUAL(0, pool.UnsafeSize());

  std::vector<ObjT*> objects;
  size_t count = 0;

  for (int i=0; i < SIZE; ++i) {
    auto* node = pool.Get(i);
    objects.push_back(node->Value());
    objects.back()->value = ++count;
  }

  ASSERT_EQUAL(SIZE, pool.UnsafeSize());

  auto print = [&](auto& node) {
    if (debug)
      std::cout << "[" << node.Index() << "] -> " << static_cast<BasePoolNode<ObjT>&>(node).Next().Index() << " " << (node.Valid() ? "valid" : "invalid") << "\n";
  };
  pool.ForEach(print);

  {
    auto obj = pool.CheckOut();

    ASSERT_EQUAL(SIZE-1, pool.UnsafeSize());

    pool.CheckIn(*obj);
  }

  ASSERT_EQUAL(SIZE, pool.UnsafeSize());

  std::vector<PooledObject<ObjT>*> holder; // Temporarily holds checked out resources
  holder.reserve(SIZE);

  for (int i=0; i < SIZE; ++i) {
    auto obj = pool.CheckOut();
    holder.push_back(obj);
    if (debug)
      std::cout << "Object[" << i << "] value="
                << (obj->Valid() ? std::to_string(obj->Value()->value) : " <invalid>")
                << "\n";
    ASSERT_EQUAL(true, holder.back()->Valid());
  }

  ASSERT_EQUAL(nullptr, pool.CheckOut());

  for (auto nd : holder)
    pool.CheckIn(*nd);

  ASSERT_EQUAL(SIZE, pool.UnsafeSize());

  // Check resolution of a pool node by an object pointer
  count = SIZE;
  for (auto nd : holder) {
    auto p = pool.Find(nd->Value());
    ASSERT_EQUAL(true, p != nullptr);
    ASSERT_EQUAL(count, p->Value()->value);
    count--;
  }

  std::vector<PooledObject<ObjT>*> same_indexes;
  same_indexes.reserve(SIZE);

  for (int i = 0; i < SIZE; ++i) {
    same_indexes.push_back(pool.CheckOut());
    ASSERT_EQUAL(i+1, pool.Get(*same_indexes.back())->value);
  }

  ASSERT_EQUAL(0, pool.UnsafeSize());

  for (auto nd : same_indexes)
    pool.CheckIn(*nd);

  ASSERT_EQUAL(SIZE, pool.UnsafeSize());

  std::cout << "==> " << std::filesystem::path(exec).stem().c_str() << ": success\n";
}

//-----------------------------------------------------------------------------
// Test FIFO
//-----------------------------------------------------------------------------
void test_fifo_pool(const char* exec) {
  ObjectPoolFIFO<ObjT> pool(SIZE);

  ASSERT_EQUAL(0, pool.UnsafeSize());

  std::vector<ObjT*> objects;
  size_t count = 0;

  for (int i=0; i < SIZE; ++i) {
    auto* node = pool.Get(i);
    objects.push_back(node->Value());
    objects.back()->value = ++count;
  }

  for (auto p : objects)
    pool.MakeAvailable(p);

  ASSERT_EQUAL(SIZE, pool.UnsafeSize());

  auto print = [&](auto& node) {
    if (debug)
      std::cout << "[" << node.Index() << "] -> " << static_cast<BasePoolNode<ObjT>&>(node).Next().Index() << " " << (node.Valid() ? "valid" : "invalid") << "\n";
  };
  pool.ForEach(print);

  {
    auto obj = pool.CheckOut();

    ASSERT_EQUAL(SIZE-1, pool.UnsafeSize());

    pool.CheckIn(*obj);
  }

  ASSERT_EQUAL(SIZE, pool.UnsafeSize());

  std::vector<PooledObject<ObjT>*> holder; // Temporarily holds checked out resources
  holder.reserve(SIZE);

  for (int i=0; i < SIZE; ++i) {
    auto obj = pool.CheckOut();
    holder.push_back(obj);
    if (debug)
      std::cout << "Object[" << i << "] value="
                << (obj->Valid() ? std::to_string(obj->Value()->value) : " <invalid>")
                << "\n";
    ASSERT_EQUAL(true, holder.back()->Valid());
  }

  ASSERT_EQUAL(nullptr, pool.CheckOut());

  for (auto nd : holder)
    pool.CheckIn(*nd);

  ASSERT_EQUAL(SIZE, pool.UnsafeSize());

  // Check resolution of a pool node by an object pointer
  count = 1;
  for (auto nd : holder) {
    auto p = pool.Find(nd->Value());
    ASSERT_EQUAL(true, p != nullptr);
    ASSERT_EQUAL((count++ % SIZE)+1, p->Value()->value);
  }

  std::vector<PooledObject<ObjT>*> same_indexes;
  same_indexes.reserve(SIZE);

  count = 1;
  for (int i = 0; i < SIZE; ++i) {
    same_indexes.push_back(pool.CheckOut());
    std::cout << "Object[" << pool.Get(*same_indexes.back())->value << "] (count=" << count << ")\n";
    ASSERT_EQUAL((count++ % SIZE)+1, pool.Get(*same_indexes.back())->value);
  }

  ASSERT_EQUAL(0, pool.UnsafeSize());

  for (auto nd : same_indexes)
    pool.CheckIn(*nd);

  ASSERT_EQUAL(SIZE, pool.UnsafeSize());

  std::cout << "==> " << std::filesystem::path(exec).stem().c_str() << ": success\n";
}