-module(arterial_nif).
-export([create/5, create/6, create/7, destroy/1]).
-export([checkout_connection/2, checkin_connection/2, checkin_connection/4]).
-export([checkout_async/3]).
-export([set_socket/3, make_available/2, make_unavailable/2]).
-export([track_inflight/5, sweep_timeouts/1]).

-on_load(init/0).

-define(LIBNAME, arterial).
-define(NOT_LOADED_ERROR,
  erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]})).

-type pool() :: atom().
-type mode() :: sync | async.

-export_type([pool/0, mode/0]).

%%%-----------------------------------------------------------------------------
%%% Public API
%%%-----------------------------------------------------------------------------

%% @doc Equivalent to `create/6' with `FixedTimeoutUs = 0' (per-request
%% timeouts, set individually via `track_inflight/5').
-spec create(pool(), pos_integer(), pos_integer(), boolean(), module()) -> ok.
create(Pool, Size, Backlog, Fifo, Protocol) ->
  create(Pool, Size, Backlog, Fifo, Protocol, 0).

%% @doc Equivalent to `create/7' with `MaxWaiters = 0' (queue-when-busy
%% disabled; `checkout_async/4,5' behaves like `checkout_connection/2').
-spec create(pool(), pos_integer(), pos_integer(), boolean(), module(),
              non_neg_integer()) -> ok.
create(Pool, Size, Backlog, Fifo, Protocol, FixedTimeoutUs) ->
  create(Pool, Size, Backlog, Fifo, Protocol, FixedTimeoutUs, 0).

%% @doc Create a pool of `Size' connections with a per-connection backlog
%% of `Backlog' in-flight requests (FIFO order if `Fifo' is true, otherwise
%% random-access by wire-level request ID), using `Protocol' to encode/decode
%% wire-level messages on every connection in the pool. Connections have no
%% socket and are unavailable for checkout until `set_socket/3' +
%% `make_available/2' are called on each one (see arterial_connection's
%% reconnect logic).
%%
%% `FixedTimeoutUs' selects how asynchronous in-flight requests tracked via
%% `track_inflight/5' expire: `0' means each request uses its own timeout
%% (the TtlUs passed to track_inflight/5); a positive value makes every
%% in-flight request in this pool expire after that same fixed duration
%% (the TtlUs argument to track_inflight/5 is then ignored).
%%
%% `MaxWaiters' bounds the queue-when-busy feature used by
%% `checkout_async/4,5': `0' disables it (a checkout attempt with no
%% connection available fails immediately, like `checkout_connection/2'
%% does); a positive value allows up to that many callers to be queued
%% while every connection is busy, each serviced (or timed out) later.
-spec create(pool(), pos_integer(), pos_integer(), boolean(), module(),
              non_neg_integer(), non_neg_integer()) -> ok.
create(Pool, Size, Backlog, Fifo, Protocol, FixedTimeoutUs, MaxWaiters)
    when is_atom(Pool), is_integer(Size), Size > 0,
         is_integer(Backlog), Backlog > 0, is_boolean(Fifo), is_atom(Protocol),
         is_integer(FixedTimeoutUs), FixedTimeoutUs >= 0,
         is_integer(MaxWaiters), MaxWaiters >= 0 ->
  {ok, Resource} = create_pool(Size, Backlog, Fifo, FixedTimeoutUs, MaxWaiters),
  persistent_term:put(pool_key(Pool), Resource),
  persistent_term:put(proto_key(Pool), Protocol),
  ets:new(buf_table(Pool), [set, public, named_table]),
  ok.

-spec destroy(pool()) -> ok.
destroy(Pool) when is_atom(Pool) ->
  Resource = resource(Pool),
  ets:delete(buf_table(Pool)),
  persistent_term:erase(pool_key(Pool)),
  persistent_term:erase(proto_key(Pool)),
  destroy_pool(Resource).

%% @doc Set the socket currently used by connection `ConnID' of `Pool'.
%% Called by the connection worker right after it (re)connects.
-spec set_socket(pool(), non_neg_integer(), arterial:socket()) -> boolean().
set_socket(Pool, ConnID, Socket) ->
  ets:insert_new(buf_table(Pool), {ConnID, <<>>}),
  set_socket_nif(resource(Pool), ConnID, Socket).

-spec make_available(pool(), non_neg_integer()) -> boolean().
make_available(Pool, ConnID) ->
  make_available_nif(resource(Pool), ConnID).

-spec make_unavailable(pool(), non_neg_integer()) -> boolean().
make_unavailable(Pool, ConnID) ->
  make_unavailable_nif(resource(Pool), ConnID).

%% @doc Check out a connection able to accept one new request. `Mode' does
%% not change the underlying reservation (always 1 backlog slot); it only
%% selects how the caller intends to use the connection (kept for callers
%% that want to log/instrument sync vs async use).
-spec checkout_connection(pool(), mode()) ->
  {ok, #{
    conn_ref => reference(),
    conn_id  => non_neg_integer(),
    protocol => module(),
    socket   => arterial:socket(),
    buffer   => binary(),
    req_ids  => [non_neg_integer()]
  }} | {error, no_connection}.
checkout_connection(Pool, Mode) when Mode =:= sync; Mode =:= async ->
  case checkout_nif(resource(Pool), 1) of
    {ok, {ConnID, Socket, ReqIDs}} ->
      Buffer = case ets:lookup(buf_table(Pool), ConnID) of
        [{_, Buf}] -> Buf;
        []         -> <<>>
      end,
      {ok, #{
        conn_ref => make_ref(),
        conn_id  => ConnID,
        protocol => protocol(Pool),
        socket   => Socket,
        buffer   => Buffer,
        req_ids  => ReqIDs
      }};
    {error, no_connection} ->
      {error, no_connection}
  end.

%% @doc Release a connection back to the pool without completing any of its
%% reserved requests (e.g. the connection died before a reply was received).
%% The reserved backlog slots are intentionally not released here: the
%% corresponding requests may still arrive on the wire once the connection
%% is reused, and FIFO backlogs in particular require slots to be released
%% in checkout order.
%%
%% Also tries to service the head of `Pool''s queue-when-busy wait-list
%% (see `checkout_async/4,5') now that this connection is available again.
-spec checkin_connection(pool(), non_neg_integer()) -> ok.
checkin_connection(Pool, ConnID) ->
  checkin_nif(resource(Pool), ConnID, [], Pool).

%% @doc Release a connection back to the pool after successfully completing
%% requests `ReqIDs' (freeing their backlog slots), storing `Buffer' as the
%% connection's leftover (undecoded) bytes for the next checkout.
%%
%% Also tries to service the head of `Pool''s queue-when-busy wait-list
%% (see `checkout_async/4,5') now that this connection is available again.
-spec checkin_connection(pool(), non_neg_integer(), [non_neg_integer()], binary()) -> ok.
checkin_connection(Pool, ConnID, ReqIDs, Buffer)
    when is_list(ReqIDs), is_binary(Buffer) ->
  ets:insert(buf_table(Pool), {ConnID, Buffer}),
  checkin_nif(resource(Pool), ConnID, ReqIDs, Pool).

%% @doc Check out a connection for asynchronous use, queuing the request
%% (up to `Pool''s `MaxWaiters', see `create/7') if every connection is
%% currently busy instead of failing immediately. `TtlUs' bounds the total
%% time from this call until a reply is checked in (covering both time
%% spent queued and time spent in-flight once a connection is assigned);
%% it's ignored if `Pool' was created with a fixed timeout (see `create/7').
%%
%% Three outcomes:
%% <ul>
%%  <li>`{ok, Map}' -- a connection was available immediately, exactly
%%      like `checkout_connection/2'. No message will follow; `ReqIDs' in
%%      the returned map is the correlation id to use with
%%      `checkin_connection/4'.</li>
%%  <li>`{queued, WaiterID}' -- every connection was busy but the request
%%      was queued; the caller's process will later receive either
%%      `{arterial_ready, Pool, ReqID, ConnID, Socket, ReqIDs}' (call
%%      `checkin_connection/4' when done, using the real wire-level
%%      `ReqID'/`ReqIDs' from that message, exactly as with a synchronous
%%      checkout) or `{arterial_timeout, Pool, WaiterID}' if `TtlUs'
%%      microseconds pass first while still queued. `WaiterID' (an opaque
%%      internal id, NOT a wire-level request id) is only ever used to
%%      match that eventual message back to this call.</li>
%%  <li>`{error, no_connection}' -- every connection was busy AND the
%%      wait-list was disabled (`MaxWaiters = 0') or already full.</li>
%% </ul>
-spec checkout_async(pool(), pid(), non_neg_integer()) ->
  {ok, #{
    conn_id  => non_neg_integer(),
    protocol => module(),
    socket   => arterial:socket(),
    buffer   => binary(),
    req_ids  => [non_neg_integer()]
  }} | {queued, non_neg_integer()} | {error, no_connection}.
checkout_async(Pool, Pid, TtlUs)
    when is_pid(Pid), is_integer(TtlUs), TtlUs >= 0 ->
  case checkout_async_nif(resource(Pool), Pid, TtlUs) of
    {ok, {ConnID, Socket, ReqIDs}} ->
      Buffer = case ets:lookup(buf_table(Pool), ConnID) of
        [{_, Buf}] -> Buf;
        []         -> <<>>
      end,
      {ok, #{
        conn_id  => ConnID,
        protocol => protocol(Pool),
        socket   => Socket,
        buffer   => Buffer,
        req_ids  => ReqIDs
      }};
    {queued, WaiterID} ->
      {queued, WaiterID};
    {error, no_connection} ->
      {error, no_connection}
  end.

%% @doc Register `ReqID' (reserved on connection `ConnID', as returned by
%% `checkout_connection/2') as an in-flight asynchronous request owned by
%% `Pid', so that if no matching `checkin_connection/3,4' call (which
%% untracks it) happens within `TtlUs' microseconds, `Pid' receives
%% `{arterial_timeout, Pool, ReqID}' the next time `sweep_timeouts/1' runs,
%% and `ReqID''s backlog slot on `ConnID' is released automatically.
%% Only meant for the asynchronous request path: synchronous callers block
%% on their own socket-level timeout and never need this.
%% `TtlUs' is ignored if `Pool' was created with a fixed timeout (see
%% `create/6').
-spec track_inflight(pool(), non_neg_integer(), non_neg_integer(), pid(),
                      non_neg_integer()) -> ok.
track_inflight(Pool, ConnID, ReqID, Pid, TtlUs)
    when is_integer(ConnID), is_integer(ReqID), is_pid(Pid),
         is_integer(TtlUs), TtlUs >= 0 ->
  track_inflight_nif(resource(Pool), ConnID, ReqID, Pid, TtlUs).

%% @doc Evict every in-flight request of `Pool' that has expired, sending
%% each owning process `{arterial_timeout, Pool, ReqID}'.
%% Meant to be called periodically (e.g. via `erlang:send_after/3') by a
%% supervised process; see `arterial_proxy'.
-spec sweep_timeouts(pool()) -> {ok, non_neg_integer()}.
sweep_timeouts(Pool) ->
  sweep_timeouts_nif(resource(Pool), Pool).

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

pool_key(Pool)  -> {?MODULE, Pool, resource}.
proto_key(Pool) -> {?MODULE, Pool, protocol}.
buf_table(Pool) -> list_to_atom("arterial_buf_" ++ atom_to_list(Pool)).

resource(Pool) ->
  case persistent_term:get(pool_key(Pool), undefined) of
    undefined -> error({unknown_pool, Pool});
    Resource  -> Resource
  end.

protocol(Pool) ->
  case persistent_term:get(proto_key(Pool), undefined) of
    undefined -> error({unknown_pool, Pool});
    Protocol  -> Protocol
  end.

%%%-----------------------------------------------------------------------------
%%% NIF functions
%%%-----------------------------------------------------------------------------

create_pool(_Size, _Backlog, _Fifo, _FixedTtlUs, _MaxWaiters) -> ?NOT_LOADED_ERROR.
destroy_pool(_Rsrc)                                      -> ?NOT_LOADED_ERROR.
set_socket_nif(_Rsrc, _ConnID, _Socket)                  -> ?NOT_LOADED_ERROR.
make_available_nif(_Rsrc, _ConnID)                       -> ?NOT_LOADED_ERROR.
make_unavailable_nif(_Rsrc, _ConnID)                     -> ?NOT_LOADED_ERROR.
checkout_nif(_Rsrc, _Samples)                            -> ?NOT_LOADED_ERROR.
checkin_nif(_Rsrc, _ConnID, _ReqIDs, _PoolName)          -> ?NOT_LOADED_ERROR.
checkout_async_nif(_Rsrc, _Pid, _TtlUs)                  -> ?NOT_LOADED_ERROR.
track_inflight_nif(_Rsrc, _ConnID, _ReqID, _Pid, _TtlUs) -> ?NOT_LOADED_ERROR.
sweep_timeouts_nif(_Rsrc, _PoolName)                     -> ?NOT_LOADED_ERROR.

init() ->
  SoName  =
    case code:priv_dir(?LIBNAME) of
      {error, bad_name} ->
        case code:which(?MODULE) of
          Filename when is_list(Filename) ->
            Dir = filename:dirname(filename:dirname(Filename)),
            filename:join([Dir, "priv", "arterial"]);
          _ ->
            filename:join("../priv", "arterial")
        end;
      Dir ->
        filename:join(Dir, "arterial")
  end,
  erlang:load_nif(SoName, 0).
