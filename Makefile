all: deps.get compile

deps.get:
	rebar3 get-deps

compile:
	rebar3 $@

test:
	make -C c_src test

.PHONY: test
