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

compile: nif
	rebar3 $@

nif:
	$(MAKE) --no-print-directory -C c_src

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

bump-version:
	@FILE=$$(ls -1 src/*.app.src | head -n1); \
	CURRENT=$$(grep -m1 '{vsn,' $$FILE | sed -E 's/.*"([0-9]+\.[0-9]+\.[0-9]+)".*/\1/'); \
	MAJOR=$$(echo $$CURRENT | cut -d. -f1); \
	MINOR=$$(echo $$CURRENT | cut -d. -f2); \
	PATCH=$$(echo $$CURRENT | cut -d. -f3); \
	NEW=$$(echo "$${MAJOR}.$${MINOR}.$$((PATCH + 1))" | tr -d '\n'); \
	echo "Bumping version from $${CURRENT} to $${NEW}"; \
	sed -i "s/{vsn, \"$${CURRENT}\"}/{vsn, \"$${NEW}\"}/" $$FILE; \
	echo "Changed: {vsn, \"$${CURRENT}\"} -> {vsn, \"$${NEW}\"}"; \
	sed -i 's/\({:\?glazer,[[:space:]]*"~>\)[^"]*/\1 '"$${MAJOR}.$${MINOR}"'/' README.md; \
	echo ""; \
	read -p "Commit this change? [Y/n] " -n 1 -r || true; \
	echo ""; \
	if [[ $$REPLY =~ ^[Yy]$$ ]] || [[ -z $$REPLY ]]; then \
		git commit -am "Bump version to $${NEW}"; \
	fi

.PHONY: test
