#include <iostream>
#include <filesystem>
#include "pool.hpp"

using namespace arterial;

#define ASSERT_EQUAL(a, b) \
  if ((a)!=(b)) { \
    std::cerr << #a  << " != " << #b  << " ||| " \
              << (a) << " != " << (b) << " [line: " << __LINE__ << "]\n"; \
    exit(1); \
  } \
  else { \
    (void)0; \
  }

struct ObjT {
    int value;
};

constexpr const size_t SIZE = 5;

int main(int argc, char* argv[]) {

  bool debug = argc > 1 && std::string(argv[1]) == "-d";

  ObjectPool<ObjT> pool(SIZE);

  ASSERT_EQUAL(0, pool.UnsafeFreeCapacity());

  std::vector<ObjT*> objects;
  size_t count = 0;

  auto add = [&](auto& node) {
    objects.push_back(node.Value());
    objects.back()->value = ++count;
  };
  pool.ForEach(add);

  for (auto p : objects)
    pool.MakeAvailable(p);

  ASSERT_EQUAL(SIZE, pool.UnsafeFreeCapacity());

  auto print = [&](auto& node) {
    if (debug)
      std::cout << "[" << node.Index() << "] -> " << static_cast<BasePoolNode<ObjT>&>(node).Next().Index() << " " << (node.Valid() ? "valid" : "invalid") << "\n";
  };
  pool.ForEach(print);

  {
    auto& obj = pool.CheckOut();

    ASSERT_EQUAL(SIZE-1, pool.UnsafeFreeCapacity());

    pool.CheckIn(obj);
  }

  ASSERT_EQUAL(SIZE, pool.UnsafeFreeCapacity());

  std::vector<PooledObject<ObjT>> holder; // Temporarily holds checked out resources
  holder.reserve(SIZE);

  for (int i=0; i < SIZE; ++i) {
    auto& obj = pool.CheckOut();
    holder.push_back(obj);
    if (debug)
      std::cout << "Object[" << i << "] value="
                << (obj.Valid() ? std::to_string(obj.Value()->value) : " <invalid>")
                << "\n";
    ASSERT_EQUAL(true, holder.back().Valid());
  }

  ASSERT_EQUAL(false, pool.CheckOut().Valid());
  ASSERT_EQUAL(true,  pool.CheckOut().Invalid());

  for (auto& nd : holder)
    pool.CheckIn(nd);

  ASSERT_EQUAL(SIZE, pool.UnsafeFreeCapacity());

  // Check resolution of a pool node by an object pointer
  count = SIZE;
  for (auto& nd : holder) {
    auto p = pool.Find(nd.Value());
    ASSERT_EQUAL(true, p != nullptr);
    ASSERT_EQUAL(count, p->Value()->value);
    count--;
  }

  std::vector<PooledObject<ObjT>> same_indexes;
  same_indexes.reserve(SIZE);

  for (int i = 0; i < SIZE; ++i) {
    same_indexes.push_back(pool.CheckOut());
    ASSERT_EQUAL(i+1, pool.Get(same_indexes.back())->value);
  }

  ASSERT_EQUAL(0, pool.UnsafeFreeCapacity());

  for (auto& nd : same_indexes)
    pool.CheckIn(nd);

  ASSERT_EQUAL(SIZE, pool.UnsafeFreeCapacity());

  std::cout << "==> " << std::filesystem::path(argv[0]).stem().c_str() << ": success\n";
}