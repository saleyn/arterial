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

# io_uring (Linux reactor backend)
# Normalise HAS_URING: accept "1", "Y", "yes" from env/cmdline; fall back to pkg-config.
# Automatically disabled on WSL: io_uring POLL_ADD is unreliable on WSL kernels.
# We cannot use := to override a command-line variable, so derive a private variable.
_IS_WSL    := $(shell grep -qsiE 'Microsoft|WSL' /proc/version 2>/dev/null && echo yes)
# HAS_URING can be set explicitly to override WSL auto-detection.
# Without an explicit override, WSL disables io_uring (unreliable POLL_ADD).
_HAS_URING := $(if $(filter 1 Y yes,$(HAS_URING)),yes,$(if $(filter yes,$(_IS_WSL)),no,$(shell pkg-config --exists liburing && echo yes)))
ifeq ($(_HAS_URING),yes)
  URING_CFLAGS  := $(shell pkg-config --cflags liburing)
  POOL_LDFLAGS  += $(shell pkg-config --libs liburing)
  CXXFLAGS      += $(URING_CFLAGS)
else
  CXXFLAGS      += -DREACTOR_NO_URING
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
ifneq ($(findstring Apple,$(CXX_ID)),)
  LTO_FLAGS  := -flto=thin
  LINK_FLAGS :=
else ifneq ($(findstring clang,$(CXX_ID)),)
  LTO_FLAGS  := -flto=thin
  LINK_FLAGS := -fuse-ld=lld
else
  LTO_FLAGS  := -flto=auto -fno-fat-lto-objects
  LINK_FLAGS :=
endif

# Detect compiler family for LTO flags.
# AppleClang uses ld64 which handles thin-LTO natively — no -fuse-ld=lld.
# Upstream Clang on Linux emits LLVM bitcode that requires lld.
# GCC uses -flto=auto with GNU ld/gold.
CXX_ID := $(shell $(CXX) --version 2>&1 | head -1)
ifneq ($(findstring clang,$(CXX_ID)),)
  LDFLAGS += -fuse-ld=lld
endif

ifeq ($(TSAN),1)
  CXXFLAGS += -O1 -g -fsanitize=thread -fno-omit-frame-pointer
  LDFLAGS  += -fsanitize=thread
else ifeq ($(ASAN),1)
  CXXFLAGS += -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer
  LDFLAGS  += -fsanitize=address,undefined
else ifeq ($(DEBUG),1)
  CXXFLAGS += -O0 -g
else
  CXXFLAGS += -O3 -DNDEBUG -march=native -mtune=native $(LTO_FLAGS)
	LDFLAGS  += $(LINK_FLAGS)
endif

all: $(TARGET)

$(TARGET): $(OBJ) | $(PRIV_DIR)
	$(CXX) $(LDFLAGS) $(POOL_LDFLAGS) $(SHARED_LDFLAGS) -I$(ERL_INC) -o $@ $<

# Build-time info injected into the NIF
ARTERIAL_VERSION     := $(shell git -C .. describe --tags --always 2>/dev/null || echo unknown)
ARTERIAL_APP_VERSION := $(shell awk -F'"' '/vsn/{print $$2; exit}' ../src/arterial.app.src 2>/dev/null || echo unknown)
ifeq ($(DEBUG),1)
  ARTERIAL_OPT_LEVEL := none
else
  ARTERIAL_OPT_LEVEL := O3
endif
ARTERIAL_PGO         := 0

BUILD_DEFINES := \
  -DARTERIAL_VERSION='"$(ARTERIAL_VERSION)"' \
  -DARTERIAL_APP_VERSION='"$(ARTERIAL_APP_VERSION)"' \
  -DARTERIAL_OPT_LEVEL='"$(ARTERIAL_OPT_LEVEL)"' \
  -DARTERIAL_PGO=$(ARTERIAL_PGO)

HDRS := $(wildcard *.hpp *.hxx *.h)
$(OBJ): $(SRC) $(HDRS) | $(OBJ_DIR)
	$(CXX) -c $(CXXFLAGS) $(BUILD_DEFINES) -o $@ $<

$(PRIV_DIR) $(OBJ_DIR) $(TEST_DIR):
	@mkdir -p $@

REACTOR_TEST := $(PRIV_DIR)/reactor_test
REACTOR_TEST_OBJ := $(OBJ_DIR)/reactor_test.o
# Use the same WSL-aware uring selection as the NIF build above.
ifeq ($(_HAS_URING),yes)
  URING_CFLAGS  := $(shell pkg-config --cflags liburing)
  URING_LDFLAGS := $(shell pkg-config --libs liburing)
else
  URING_CFLAGS  := -DREACTOR_NO_URING
  URING_LDFLAGS :=
endif

$(REACTOR_TEST_OBJ): reactor_test.cpp reactor.hpp reactor_platform.hpp erl_nif_stub.h | $(OBJ_DIR)
	$(CXX) -c $(CXXFLAGS) $(URING_CFLAGS) -DREACTOR_TEST_STUB_NIF -o $@ $<

$(REACTOR_TEST): $(REACTOR_TEST_OBJ) | $(PRIV_DIR)
	$(CXX) $(LDFLAGS) $(URING_LDFLAGS) -o $@ $< -lpthread

test_reactor: $(REACTOR_TEST)
	@echo "==> Running reactor C++ tests"
	$(REACTOR_TEST)

REACTOR_BENCH     := $(PRIV_DIR)/reactor_bench
REACTOR_BENCH_OBJ := $(OBJ_DIR)/reactor_bench.o

$(REACTOR_BENCH_OBJ): reactor_bench.cpp reactor.hpp reactor_platform.hpp erl_nif_stub.h | $(OBJ_DIR)
	$(CXX) -c $(CXXFLAGS) $(URING_CFLAGS) -DREACTOR_TEST_STUB_NIF -o $@ $<

$(REACTOR_BENCH): $(REACTOR_BENCH_OBJ) | $(PRIV_DIR)
	$(CXX) $(LDFLAGS) $(URING_LDFLAGS) -o $@ $< -lpthread

bench-reactor: $(REACTOR_BENCH)
	@echo "==> Running reactor C++ benchmark (Scenario 1 standalone)"
	$(REACTOR_BENCH)

# Full seven-scenario Erlang benchmark.
# Runs via rebar3 so the NIF is loaded; bypasses EUnit timeouts.
#   make bench                            — defaults
#   make bench CONNS=32 REQS=1000 MSG=1024
CONNS      ?= 8
REQS       ?= 50000
MSG        ?= 8
BENCH_OPTS ?=
BENCH_PA    = $(addprefix -pa ../,$(wildcard _build/test/lib/*/ebin))
bench: $(REACTOR_BENCH)
	@echo "==> Reactor benchmark (all scenarios)"
	cd .. && rebar3 as test compile 2>/dev/null; \
	erl -noshell \
	    $(BENCH_PA) \
	    -config test/test.config \
	    -eval "application:ensure_all_started(arterial), \
	           reactor_bench:run_with($(CONNS), $(REQS), $(MSG)), halt()."

test:
	@echo "==> No C++ tests for arterial (NIF-based implementation)"
	@echo "    Use 'rebar3 eunit' or 'rebar3 ct' for Erlang tests"

../priv:
	mkdir -p ../priv

enif.hpp:
	curl -s -O https://raw.githubusercontent.com/saleyn/nifpp/main/enif.hpp

clean:
	rm -f *.o $(PRIV_DIR)/arterial_nif.so $(OBJ_DIR)/*.o
