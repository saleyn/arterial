CURDIR   := $(shell pwd)
BASEDIR  := $(abspath $(dir $(CURDIR)))
CXX      ?= g++
PRIV_DIR ?= $(if $(REBAR_BARE_COMPILER_OUTPUT_DIR),$(REBAR_BARE_COMPILER_OUTPUT_DIR),$(BASEDIR))/priv
OBJ_DIR  ?= $(BASEDIR)/obj
TEST_DIR ?= $(BASEDIR)/test

CXX     ?= clang++
ERL_INC := $(shell erl -noshell -noinput \
  -eval "io:format(\"~ts/erts-~ts/include\",[code:root_dir(),erlang:system_info(version)]),halt(0).")
CXXFLAGS = -std=c++23 -fPIC -Wall -Wextra -finline-functions -fvisibility=hidden \
           -I$(ERL_INC)

# On macOS `libatomic` is not provided/needed; only link -latomic on Linux.
# LDFLAGS (common to both the NIF .so and the test_pool executable) vs.
# SHARED_LDFLAGS (the .so-only bits) are kept separate -- test_pool is a
# plain executable invoked directly (see the `test` target below), so
# linking it with -shared/-dynamiclib would produce something that isn't
# runnable as a program.
UNAME := $(shell uname -s)
ifeq ($(UNAME),Linux)
  LDFLAGS += -latomic
endif

# Shared library flags: use -dynamiclib and permissive undefined symbol handling on macOS
ifeq ($(UNAME),Darwin)
  SHARED_LDFLAGS += -dynamiclib -undefined dynamic_lookup
else
  SHARED_LDFLAGS += -shared
endif

# erlang:load_nif/2 appends the platform-specific extension itself
# (.dll on Windows, .so elsewhere), so the build output must match.
ifneq (,$(findstring MINGW,$(shell uname -s))$(findstring MSYS,$(shell uname -s)))
  EXT := .dll
else
  EXT := .so
endif

SRC         := arterial.cpp
OBJ         := $(OBJ_DIR)/arterial.o
TARGET      := $(PRIV_DIR)/arterial$(EXT)

POOL_SRC    := arterial_nif_pool.cpp
POOL_OBJ    := $(OBJ_DIR)/arterial_nif_pool.o
POOL_TARGET := $(PRIV_DIR)/nif_pool$(EXT)

TEST_OBJ    := $(OBJ_DIR)/test_pool.o
TEST_TARGET  = $(TEST_DIR)/test_pool

# Detect compiler family for LTO flags.
# AppleClang uses ld64 which handles thin-LTO natively — no -fuse-ld=lld.
# Upstream Clang on Linux emits LLVM bitcode that requires lld.
# GCC uses -flto=auto with GNU ld/gold.
CXX_ID := $(shell $(CXX) --version 2>&1 | head -1)
ifneq ($(findstring clang,$(CXX_ID)),)
  LDFLAGS += -fuse-ld=lld
endif

ifeq ($(DEBUG),1)
  CXXFLAGS += -O0 -g
else
  CXXFLAGS += -O3 -DNDEBUG -march=native -mtune=native
endif

all: $(TARGET) $(POOL_TARGET) $(TEST_TARGET)

$(TARGET): $(OBJ) | $(PRIV_DIR)
	$(CXX) $(LDFLAGS) $(SHARED_LDFLAGS) -I$(ERL_INC) -o $@ $<

$(POOL_TARGET): $(POOL_OBJ) | $(PRIV_DIR)
	$(CXX) $(LDFLAGS) $(SHARED_LDFLAGS) -I$(ERL_INC) -o $@ $<

$(TEST_TARGET): $(TEST_OBJ) | $(TEST_DIR)
	$(CXX) $(LDFLAGS) -I$(ERL_INC) -o $@ $<

$(OBJ): $(SRC) $(wildcard *.hpp) enif.hpp | $(OBJ_DIR)
	$(CXX) -c $(CXXFLAGS) -o $@ $<

$(POOL_OBJ): $(POOL_SRC) enif.hpp | $(OBJ_DIR)
	$(CXX) -c $(CXXFLAGS) -o $@ $<

$(TEST_OBJ) : test_pool.cpp pool_fifo.hpp enif.hpp | $(OBJ_DIR)
	$(CXX) -c -g -O0 $(CXXFLAGS) -o $@ $<

$(PRIV_DIR) $(OBJ_DIR) $(TEST_DIR):
	@mkdir -p $@

run test: $(TEST_TARGET)
	@echo "==> Testing $<"
	@$< $(if $(debug),-d)

../priv:
	mkdir -p ../priv

enif.hpp:
	curl -s -O https://raw.githubusercontent.com/saleyn/nifpp/main/enif.hpp

clean:
	rm -f *.o $(TEST_DIR)/test_pool $(PRIV_DIR)/arterial.so $(PRIV_DIR)/nif_pool.so $(OBJ_DIR)/*.o
