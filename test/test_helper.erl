-module(test_helper).

-behaviour(gen_event).

-moduledoc """
Shared eunit test setup. Several suites deliberately exercise
warning/notice-logged paths (bounced connections, refused reconnects,
etc.) -- `set_log_level/0` silences anything below `error` so eunit's own
pass/fail output isn't buried in expected log noise. Call it once at the
top of a suite's `setup/0` (or equivalent); idempotent, so it's safe to
call from every suite that runs in the same VM.
""".

%% gen_event callbacks for null error handler
-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).

-export([set_log_level/0, silence_supervisor_reports/0, set_test_logging/0]).

-doc "Sets the kernel logger's primary level to `error` (silencing `notice`/`warning`/`info`/`debug`).".
-spec set_log_level() -> ok.
set_log_level() ->
  logger:set_primary_config(level, error).

-doc "Silences supervisor reports by installing a null error logger handler.".
-spec silence_supervisor_reports() -> ok.
silence_supervisor_reports() ->
  %% Install a logger filter to drop supervisor reports
  FilterFun = fun(LogEvent, _Config) ->
    case LogEvent of
      #{msg := {report, Report}} when is_map(Report) ->
        case maps:get(label, Report, undefined) of
          {supervisor, _} -> ignore;
          _ -> LogEvent
        end;
      _ -> LogEvent
    end
  end,
  logger:add_primary_filter(silence_supervisor_reports, {FilterFun, []}),

  %% Also install a null error_logger handler for legacy reports
  error_logger:add_report_handler(test_helper, []),
  ok.

-doc "Comprehensive test logging setup: sets error level and silences supervisor reports.".
-spec set_test_logging() -> ok.
set_test_logging() ->
  %% Set primary log level to error
  set_log_level(),

  %% Simple approach: disable SASL completely for tests
  application:stop(sasl),

  ok.

%% Null error handler callbacks - silently ignore all reports
init([]) -> {ok, []}.
handle_event(_Event, State) -> {ok, State}.
handle_call(_Request, State) -> {ok, ignored, State}.
handle_info(_Info, State) -> {ok, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
