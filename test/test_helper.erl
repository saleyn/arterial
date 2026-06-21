-module(test_helper).

-moduledoc """
Shared eunit test setup. Several suites deliberately exercise
warning/notice-logged paths (bounced connections, refused reconnects,
etc.) -- `set_log_level/0` silences anything below `error` so eunit's own
pass/fail output isn't buried in expected log noise. Call it once at the
top of a suite's `setup/0` (or equivalent); idempotent, so it's safe to
call from every suite that runs in the same VM.
""".

-export([set_log_level/0]).

-doc "Sets the kernel logger's primary level to `error` (silencing `notice`/`warning`/`info`/`debug`).".
-spec set_log_level() -> ok.
set_log_level() ->
  logger:set_primary_config(level, error).
