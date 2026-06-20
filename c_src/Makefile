CXX     ?= clang++
ERL_INC := $(shell erl -eval 'io:format("~s/erts-~s/include", [code:root_dir(), erlang:system_info(version)])' -noshell -s init stop)
CXXFLAGS = -std=c++20 -fPIC -Wall -Wextra

all: test_pool ../priv/arterial.so

run test: test_pool
	@echo "==> Testing c_src/$<"
	@./$< $(if $(debug),-d)

test_pool: test_pool.o
	$(CXX) -o $@ $< -latomic

test_pool.o : test_pool.cpp pool_lifo.hpp pool_fifo.hpp nifpp.h
	$(CXX) -c -g -O0 $(CXXFLAGS) -o $@ $<

../priv:
	mkdir -p ../priv

../priv/arterial.so: arterial.cpp arterial.hpp connection.hpp backlog.hpp throttle.hpp \
                      pool_fifo.hpp pool_lifo.hpp nifpp.h | ../priv
	$(CXX) -shared $(CXXFLAGS) -I$(ERL_INC) -o $@ $< -latomic

nifpp.h:
	curl -s -O https://raw.githubusercontent.com/saleyn/nifpp/main/nifpp.h

clean:
	rm -f *.o test_pool ../priv/arterial.so

