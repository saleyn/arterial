CXX ?= clang++

all: test_pool # ../priv/pool.so

run test: test_pool
	./$< $(if $(debug),-d)

test_pool: test_pool.o
	$(CXX) -o $@ $< -latomic

test_pool.o : test_pool.cpp pool_lifo.hpp pool_fifo.hpp nifpp.h
	$(CXX) -c -g -O0 -std=c++20 -o $@ $<

nifpp.h:
	curl -s -O https://raw.githubusercontent.com/saleyn/nifpp/main/nifpp.h

clean:
	rm *.o test_pool

