all: deps.get compile

deps.get:
	rebar3 get-deps

compile:
	rebar3 $@

.PHONY: test
