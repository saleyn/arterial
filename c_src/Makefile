CURDIR   := $(shell pwd)
BASEDIR  := $(abspath $(dir $(CURDIR)))
CXX      ?= g++
PRIV_DIR ?= $(if $(REBAR_BARE_COMPILER_OUTPUT_DIR),$(REBAR_BARE_COMPILER_OUTPUT_DIR),$(BASEDIR))/priv
OBJ_DIR  ?= $(BASEDIR)/obj
TEST_DIR ?= $(BASEDIR)/test

CXX      ?= clang++
ERL_INC  := $(shell erl -noshell -noinput \
  -eval "io:format(\"~ts/erts-~ts/include\",[code:root_dir(),erlang:system_info(version)]),halt(0).")
CXXFLAGS  = -std=c++23 -fPIC -Wall -Wextra -finline-functions -fvisibility=hidden \
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

# Check for OpenSSL and add to pool NIF linking
HAS_OPENSSL := $(shell pkg-config --exists openssl && echo yes)
ifeq ($(HAS_OPENSSL),yes)
  OPENSSL_LIBS   := $(shell pkg-config --libs openssl)
  OPENSSL_CFLAGS := $(shell pkg-config --cflags openssl)
  CXXFLAGS       += -DHAVE_OPENSSL $(OPENSSL_CFLAGS)
  POOL_LDFLAGS   += $(OPENSSL_LIBS)
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

SRC    := arterial_nif.cpp
OBJ    := $(OBJ_DIR)/arterial_nif.o
TARGET := $(PRIV_DIR)/arterial_nif$(EXT)


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

all: $(TARGET)

$(TARGET): $(OBJ) | $(PRIV_DIR)
	$(CXX) $(LDFLAGS) $(POOL_LDFLAGS) $(SHARED_LDFLAGS) -I$(ERL_INC) -o $@ $<

$(OBJ): $(SRC) enif.hpp | $(OBJ_DIR)
	$(CXX) -c $(CXXFLAGS) -o $@ $<

$(PRIV_DIR) $(OBJ_DIR) $(TEST_DIR):
	@mkdir -p $@

test:
	@echo "==> No C++ tests for arterial (NIF-based implementation)"
	@echo "    Use 'rebar3 eunit' or 'rebar3 ct' for Erlang tests"

../priv:
	mkdir -p ../priv

enif.hpp:
	curl -s -O https://raw.githubusercontent.com/saleyn/nifpp/main/enif.hpp

clean:
	rm -f *.o $(PRIV_DIR)/arterial_nif.so $(OBJ_DIR)/*.o
