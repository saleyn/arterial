CXX     ?= clang++
ERL_INC := $(shell erl -eval 'io:format("~s/erts-~s/include", [code:root_dir(), erlang:system_info(version)])' -noshell -s init stop)
CXXFLAGS = -std=c++20 -fPIC -Wall -Wextra

# On macOS `libatomic` is not provided/needed; only link -latomic on Linux
UNAME := $(shell uname -s)
ifeq ($(UNAME),Linux)
ATOMIC_LIB = -latomic
else
ATOMIC_LIB =
endif

# Shared library flags: use -dynamiclib and permissive undefined symbol handling on macOS
ifeq ($(UNAME),Darwin)
SHARED_FLAG = -dynamiclib
SHARED_EXTRA = -undefined dynamic_lookup
else
SHARED_FLAG = -shared
SHARED_EXTRA =
endif

all: test_pool ../priv/arterial.so

run test: test_pool
	@echo "==> Testing c_src/$<"
	@./$< $(if $(debug),-d)

test_pool: test_pool.o
	$(CXX) -o $@ $< $(ATOMIC_LIB)

test_pool.o : test_pool.cpp pool_lifo.hpp pool_fifo.hpp nifpp.h
	$(CXX) -c -g -O0 $(CXXFLAGS) -o $@ $<

../priv:
	mkdir -p ../priv

../priv/arterial.so: arterial.cpp arterial.hpp connection.hpp backlog.hpp throttle.hpp \
                      pool_fifo.hpp pool_lifo.hpp nifpp.h | ../priv
	$(CXX) $(SHARED_FLAG) $(CXXFLAGS) $(SHARED_EXTRA) -I$(ERL_INC) -o $@ $< $(ATOMIC_LIB)

nifpp.h:
	curl -s -O https://raw.githubusercontent.com/saleyn/nifpp/main/nifpp.h

clean:
	rm -f *.o test_pool ../priv/arterial.so

