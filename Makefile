ifndef VERBOSE
MAKEFLAGS += --no-print-directory
endif

PRIV_DIR := $(abspath priv)
OBJ_DIR  := $(abspath obj)
DEBUG    ?= 0
REBAR    ?= rebar3
APP      := $(shell sed -nE 's/^\{application, ([a-zA-Z0-9_]+),.*/\1/p' src/*.app.src | head -n1)

all: deps.get compile

deps.get:
	rebar3 get-deps

compile:
	rebar3 $@

nif:	# Invoked by rebar3 through compile pre-hook
	$(MAKE) -C c_src

cover:
	$(REBAR) cover --verbose

check:
	$(REBAR) xref
	$(REBAR) dialyzer

memcheck:
	@echo "==> Building NIF with AddressSanitizer"
	@$(MAKE) -C c_src PRIV_DIR=$(PRIV_DIR) OBJ_DIR=$(OBJ_DIR) ASAN=1 \
	  $(if $(VERBOSE),VERBOSE=1,) clean all
	@$(REBAR) compile
	@echo "==> Running eunit under ASan$(if $(filter 1,$(DETECT_LEAKS)), + LeakSanitizer,) ($(ASAN_PRELOAD))"
	ERL_FLAGS="+A 1" ASAN_OPTIONS="detect_leaks=$(DETECT_LEAKS)" \
	  LSAN_OPTIONS="suppressions=$(LSAN_SUPPRESSIONS)" \
	  $(ASAN_PRELOAD) \
	  $(REBAR) eunit
	@echo "==> Rebuilding normal NIF (removing ASan instrumentation)"
	@$(MAKE) -C c_src PRIV_DIR=$(PRIV_DIR) OBJ_DIR=$(OBJ_DIR) \
	  $(if $(VERBOSE),VERBOSE=1,) clean all
	@$(REBAR) compile

doc docs:
	$(REBAR) ex_doc

test:
	$(MAKE) --no-print-directory -C c_src test
	rebar3 eunit

# Runs test/arterial_bench.erl against a real test_tcp_server over
# loopback TCP -- not part of `test`/eunit (no _test exports), so it
# needs its own target. Pass options either as a plain comma-separated
# `key=value` list:
#   make bench BENCH_OPTS='pool_size=16, duration_s=10'
#   make bench BENCH_OPTS='mode=sync'
# or, if you need a non-trivial value (e.g. a binary), as a full Erlang
# map literal -- detected by a leading '#{' and passed through as-is:
#   make bench BENCH_OPTS='#{pool_size => 16, payload => <<"hi">>}'
HASH := \#
BENCH_OPTS ?=
override BENCH_OPTS := $(strip $(BENCH_OPTS))
ifeq ($(BENCH_OPTS),)
BENCH_OPTS_MAP := $(HASH){}
else ifneq ($(findstring $(HASH){,$(BENCH_OPTS)),)
BENCH_OPTS_MAP := $(BENCH_OPTS)
else
BENCH_OPTS_MAP := $(HASH){$(shell echo '$(BENCH_OPTS)' | sed -E 's/([a-zA-Z_][a-zA-Z0-9_]*)[[:space:]]*=[[:space:]]*([^,]+)/\1 => \2/g')}
endif
bench bench-help:
	@$(REBAR) as test compile
	@erl -noshell -noinput -pa _build/test/lib/arterial/ebin \
	  -pa _build/test/lib/arterial/test \
	  -eval "arterial_bench:$(subst -,_,$@)($(BENCH_OPTS_MAP)), halt()."

# Runs test/shackle_bench.erl -- the same workload shape/wire framing as
# `bench` above, but driven through https://github.com/lpgauth/shackle
# instead of arterial, for a direct throughput/latency comparison. The
# `shackle` dependency is only pulled in under the `test` profile (see
# rebar.config), so this needs every dep app's ebin on the code path,
# not just arterial's.
bench-shackle bench-shackle-help:
	@$(REBAR) as test compile
	@erl -noshell -noinput -pa _build/test/lib/*/ebin \
	  -pa _build/test/lib/arterial/test \
	  -eval "shackle_bench:$(subst -,_,$(subst bench-shackle,bench,$@))($(BENCH_OPTS_MAP)), halt()."

clean:
	$(MAKE) -C c_src $@
	rebar3 clean

distclean: clean
	rm -fr _build obj doc

bump-version:
	@APP_FILE=$$(ls -1 src/*.app.src | head -n1); \
	APP=$$(grep -m1 '{application,' $$APP_FILE | sed -nE '/\{application,/s/[^,]+,\s*([a-z-]+).*/\1/p'); \
	CURRENT=$$(grep -m1 '{vsn,' $$APP_FILE | sed -E 's/.*"([0-9]+\.[0-9]+\.[0-9]+)".*/\1/'); \
	MAJOR=$$(echo $$CURRENT | cut -d. -f1); \
	MINOR=$$(echo $$CURRENT | cut -d. -f2); \
	PATCH=$$(echo $$CURRENT | cut -d. -f3); \
	NEW=$$(echo "$${MAJOR}.$${MINOR}.$$((PATCH + 1))" | tr -d '\n'); \
	echo "Bumping version from $${CURRENT} to $${NEW}"; \
	sed -i -E 's/(\{vsn,\s*"[^"0-9]*)[^"]+"\s*\}/\1'$${NEW}'"}/' $$APP_FILE; \
	echo "Changed: {vsn, \"$${CURRENT}\"} -> {vsn, \"$${NEW}\"}"; \
	sed -i -E 's/(\{:?'$$APP',\s*"[^"0-9]*)[0-9]+\.[0-9]+/\1'$$MAJOR.$$MINOR'/' README.md; \
	echo ""; \
	read -p "Commit this change? [Y/n] " -n 1 -r || true; \
	echo ""; \
	if [[ $$REPLY =~ ^[Yy]$$ ]] || [[ -z $$REPLY ]]; then \
	  git commit -am "Bump $${APP} version to $${NEW}"; \
	fi

.PHONY: test doc
