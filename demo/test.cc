#include <cstdio>
#include <cstring>
#include <future>
#include <iostream>
using namespace std;
auto main() -> int {
  std::promise<int> a;
  std::future<int> b = a.get_future();
  struct C {
    std::promise<int> pro;
  } c;
  c.pro = std::move(a);

  auto d = new C;

  memcpy(d, &c, sizeof(C));

  d->pro.set_value(1);

  b.get();

}