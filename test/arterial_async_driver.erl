-module(arterial_async_driver).

-moduledoc """
Minimal asynchronous request dispatcher for `arterial`, built on top of
`arterial_nif:checkout_async/3` + `c:arterial_client:handle_request/2`/
`handle_data/2` -- the building blocks the library ships, with no
end-to-end async driver of its own (unlike `shackle`, where
`shackle_server` already *is* this loop).

One `request/3` call is one full checkout -> send -> receive-reply ->
checkin cycle, same as `arterial_client:call/3` -- the difference is
*where* the blocking socket I/O happens: `call/3` blocks the caller's
own process on the socket directly, while `request/3` casts to one of a
small, fixed pool of dispatcher processes (`pool_size` of them, started
by `start_link/2`) that hand the request off to its connection's socket
and wait for the reply asynchronously, then forward it to the caller via
its own mailbox once done.

"Asynchronously" here means via OTP's `socket` module's own
completion-based recv, not a blocking call: `socket:recv(Sock, 0,
nowait)` either returns the reply immediately (already buffered) or
`{select, SelectInfo}`, in which case the dispatcher's own `handle_info`
later receives a `{'$socket', Sock, select, Ref}` message once data is
ready, and only then calls `socket:recv/3` again to actually fetch it.
This is the `socket`-module equivalent of `gen_tcp`'s `{active, true}`
mode -- the same shape `shackle_server` itself uses (a long-lived process
that owns its socket and has replies delivered straight into its mailbox)
-- so, unlike an earlier version of this module that `spawn`ed a
throwaway blocking-`recv` reader process per request, there is no extra
process and no extra message hop per request here at all.

This does NOT pipeline multiple in-flight requests on one connection
(see the module doc for why: `arterial`'s public API has no way to hold
a connection checked out across more than one in-flight request --
`checkin_connection/4` unconditionally returns the connection to pool
availability). Each dispatcher serves one request to completion before
picking up the next, exactly like the sync path -- the benefit measured
here is avoiding N busy/blocked benchmark-worker processes in favor of a
small, fixed set of dispatchers, not pipelining.
""".

-behaviour(gen_server).

-export([start_link/3, stop/1, request/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
  pool       :: arterial_pool:name(),
  client     :: arterial:client(),
  client_st  :: term(),
  select_ref :: undefined | reference(),
  conn_id    :: undefined | non_neg_integer(),
  socket     :: undefined | arterial:socket(),
  req_ids    :: [non_neg_integer()],
  buf        :: binary(),
  from       :: undefined | gen_server:from(),
  %% Requests that arrived while `from` was already busy -- since
  %% `request/3` picks one of a *fixed* set of dispatchers (not 1:1 with
  %% callers), more than one caller can land on the same dispatcher
  %% concurrently. Queue them instead of rejecting, same as if the
  %% caller itself had simply waited its turn.
  queue      :: queue:queue({term(), gen_server:from()}),
  ttl_us     :: non_neg_integer()
}).

-doc """
Start `PoolSize` dispatcher processes for `Pool`, registered under
`{?MODULE, Pool, N}` (`N` in `1..PoolSize`), round-robin selected by
`request/3` (the same selection strategy `shackle_pool`'s default
`round_robin` `pool_strategy` uses, for a fair head-to-head -- uniform
random selection would let two concurrent callers collide on the same
busy dispatcher while others sit idle, purely by chance, which has
nothing to do with either library's actual throughput). `Client` must be
the same `c:arterial_client` callback module the pool itself was started
with.

## Examples

```
1> arterial_async_driver:start_link(my_pool, 8, test_echo_client).
[{ok,<0.200.0>}, ...]
```
""".
-spec start_link(arterial_pool:name(), pos_integer(), module()) -> [{ok, pid()}].
start_link(Pool, PoolSize, Client) ->
  persistent_term:put({?MODULE, Pool, count}, PoolSize),
  Counter = atomics:new(1, [{signed, false}]),
  persistent_term:put({?MODULE, Pool, counter}, Counter),
  [
    {ok, _Pid} =
      gen_server:start_link({local, reg_name(Pool, N)}, ?MODULE, [Pool, Client], [])
    || N <- lists:seq(1, PoolSize)
  ].

-doc "Stop all `PoolSize` dispatcher processes started for `Pool`.".
-spec stop(arterial_pool:name()) -> ok.
stop(Pool) ->
  PoolSize = dispatcher_count(Pool),
  lists:foreach(
    fun(N) -> gen_server:stop(reg_name(Pool, N)) end,
    lists:seq(1, PoolSize)),
  persistent_term:erase({?MODULE, Pool, count}),
  persistent_term:erase({?MODULE, Pool, counter}).

-doc """
Send `Request` via one of `Pool`'s dispatcher processes (picked
round-robin) and block for its decoded reply or `TimeoutMs`, whichever
comes first.

## Examples

```
1> arterial_async_driver:request(my_pool, {echo, hello}, 5000).
{ok, hello}
```
""".
-spec request(arterial_pool:name(), term(), timeout()) ->
  {ok, term()} | {error, term()}.
request(Pool, Request, TimeoutMs) ->
  PoolSize = dispatcher_count(Pool),
  Counter = persistent_term:get({?MODULE, Pool, counter}),
  %% atomics counters wrap at 2^64, not at PoolSize -- reduce mod
  %% PoolSize ourselves; the 1-based dispatcher numbering (1..PoolSize)
  %% matches reg_name/2's own N range.
  N = (atomics:add_get(Counter, 1, 1) rem PoolSize) + 1,
  gen_server:call(reg_name(Pool, N), {request, Request}, TimeoutMs + 1000).

%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------
-doc false.
init([Pool, Client]) ->
  {ok, #state{
    pool = Pool, client = Client, req_ids = [], buf = <<>>,
    queue = queue:new(), ttl_us = 5_000_000
  }}.

-doc false.
handle_call({request, Request}, From, #state{from = undefined} = State) ->
  dispatch(Request, From, State);
handle_call({request, Request}, From, #state{queue = Q} = State) ->
  {noreply, State#state{queue = queue:in({Request, From}, Q)}}.

-doc false.
handle_cast(_Msg, State) ->
  {noreply, State}.

-doc false.
%% Delivered by the `socket` module once a previously-armed
%% `socket:recv(Socket, 0, nowait)` has data ready -- the `socket`-module
%% equivalent of a `{tcp, Socket, Data}` active-mode message. `Ref` is
%% only acted on if it matches the select we're currently waiting on
%% (stale messages can arrive after a connection's socket already
%% changed via reconnect, since `socket:cancel/2` on the old select isn't
%% guaranteed to suppress an in-flight message that raced it).
handle_info({'$socket', Socket, select, Ref}, #state{socket = Socket, select_ref = Ref} = State) ->
  on_readable(State#state{select_ref = undefined});
handle_info({'$socket', _Socket, select, _Ref}, State) ->
  {noreply, State};
handle_info({'$socket', Socket, abort, {Ref, Reason}}, #state{socket = Socket, select_ref = Ref} = State) ->
  reply_and_reset({error, Reason}, State#state{select_ref = undefined});
handle_info({'$socket', _Socket, abort, _}, State) ->
  {noreply, State};
handle_info(_Msg, State) ->
  {noreply, State}.

-doc false.
terminate(_Reason, #state{socket = undefined}) -> ok;
terminate(_Reason, #state{socket = Socket, select_ref = undefined}) ->
  _ = Socket,
  ok;
terminate(_Reason, #state{socket = Socket, select_ref = Ref}) ->
  _ = socket:cancel(Socket, Ref),
  ok.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

reg_name(Pool, N) ->
  list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(Pool) ++ "_" ++ integer_to_list(N)).

dispatcher_count(Pool) ->
  persistent_term:get({?MODULE, Pool, count}).

%% Claim a connection via checkout_async/3 (queuing if every connection
%% is busy and the pool's wait-list is enabled; rejecting immediately
%% otherwise -- exactly arterial_client:call/3's own contract, just
%% encode/send happening here instead of in the caller's process), then
%% encode+send the request and arm a non-blocking recv for the reply.
dispatch(Request, From, #state{pool = Pool, client = Client, client_st = ClientSt0} = State) ->
  ClientSt = ensure_client_state(Client, ClientSt0),
  case arterial_nif:checkout_async(Pool, self(), State#state.ttl_us) of
    {ok, #{conn_id := ConnID, socket := Socket, buffer := Buffer, req_ids := ReqIDs}} ->
      send_request(Request, From, State#state{
        client_st = ClientSt, conn_id = ConnID, socket = Socket,
        buf = Buffer, req_ids = ReqIDs
      });
    {queued, _WaiterID} ->
      %% Not exercised by the benchmark (max_waiters is 0 by default,
      %% same as arterial_client:call/3 never queuing) -- fail closed
      %% rather than silently hang if a caller ever does enable it.
      {reply, {error, queuing_not_supported}, State#state{client_st = ClientSt}};
    {error, no_connection} ->
      {reply, {error, no_connection}, State#state{client_st = ClientSt}}
  end.

ensure_client_state(_Client, ClientSt) when ClientSt =/= undefined ->
  ClientSt;
ensure_client_state(Client, undefined) ->
  {ok, ClientSt} = Client:init(#{}),
  ClientSt.

send_request(Request, From, #state{
  client = Client, client_st = ClientSt, socket = Socket, pool = Pool, conn_id = ConnID
} = State) ->
  {ok, _ReqIDs, Data, ClientSt1} = Client:handle_request(Request, ClientSt),
  case test_echo_protocol:send(Socket, Data) of
    ok ->
      arm_recv(State#state{client_st = ClientSt1, from = From});
    {error, Reason} ->
      arterial_nif:checkin_connection(Pool, ConnID, State#state.req_ids, <<>>),
      {reply, {error, Reason}, State#state{client_st = ClientSt1, from = undefined}}
  end.

%% checkout_async/3 returns the connection's leftover undecoded buffer
%% (per checkin_connection/4's Buffer contract) -- if a full reply was
%% already sitting in it (shouldn't normally happen with backlog=1/one
%% in-flight request at a time, but cheap to handle), decode it without
%% touching the socket at all.
arm_recv(#state{buf = Buf} = State) when Buf =/= <<>> ->
  on_data(<<>>, State);
arm_recv(#state{socket = Socket} = State) ->
  %% Calls socket:recv/3 directly (NOT test_echo_protocol:recv/2): that
  %% function's contract only covers the blocking {ok,_}/{error,_}
  %% shapes the synchronous call/3 path uses, with no `nowait` case --
  %% `{select, _}` would simply fail to match there. `0` is "read
  %% whatever's available" (same as the blocking recv/2 callers
  %% elsewhere use), not "read 0 bytes" -- socket:recv/3's `Length`
  %% argument is only a hard cap for datagram-style sockets; for `tcp` it
  %% just bounds one single recv, same semantics either way.
  case socket:recv(Socket, 0, nowait) of
    {ok, Data} ->
      on_data(Data, State);
    {select, {select_info, recv, Ref}} ->
      {noreply, State#state{select_ref = Ref}};
    {error, Reason} ->
      reply_and_reset({error, Reason}, State)
  end.

%% A previously-armed select fired -- fetch the now-ready bytes (this
%% call is expected to return {ok, _} immediately, never another
%% {select, _}, since the select_info we just consumed already confirmed
%% readability) and feed them through the same decode path as arm_recv/1.
on_readable(#state{socket = Socket} = State) ->
  case socket:recv(Socket, 0, nowait) of
    {ok, Data} ->
      on_data(Data, State);
    {error, Reason} ->
      reply_and_reset({error, Reason}, State)
  end.

on_data(Data, #state{client = Client, client_st = ClientSt, buf = Buf} = State) ->
  case Client:handle_data(<<Buf/binary, Data/binary>>, ClientSt) of
    {ok, [Reply | _], ClientSt1} ->
      finish_request({ok, Reply}, State#state{client_st = ClientSt1, buf = <<>>});
    {ok, [], ClientSt1} ->
      %% Not a full frame yet -- arm another recv for the rest of it.
      arm_recv(State#state{client_st = ClientSt1, buf = <<>>});
    {error, Reason, ClientSt1} ->
      finish_request({error, Reason}, State#state{client_st = ClientSt1, buf = <<>>})
  end.

finish_request(Result, #state{pool = Pool, conn_id = ConnID, req_ids = ReqIDs} = State) ->
  ok = arterial_nif:checkin_connection(Pool, ConnID, ReqIDs, <<>>),
  reply_and_reset(Result, State).

reply_and_reset(Result, #state{from = From, queue = Q} = State) ->
  From =/= undefined andalso gen_server:reply(From, Result),
  State1 = State#state{from = undefined, req_ids = [], buf = <<>>},
  drain_queue(State1#state{queue = Q}).

%% Pop and dispatch the next queued request, if any. dispatch/3 returns
%% either {reply, Result, State} (synchronous failure, e.g. no_connection
%% -- the dispatcher stays idle, but `from` is never set for this
%% request, so it must NOT be left as the lone reason this dispatcher
%% ever wakes up again: keep draining the rest of the queue immediately)
%% or {noreply, State} (send succeeded, reply arrives later via
%% handle_info, so stop draining and wait for that).
drain_queue(#state{queue = Q} = State) ->
  case queue:out(Q) of
    {empty, Q1} ->
      {noreply, State#state{queue = Q1}};
    {{value, {NextRequest, NextFrom}}, Q1} ->
      case dispatch(NextRequest, NextFrom, State#state{queue = Q1}) of
        {reply, NextResult, State1} ->
          gen_server:reply(NextFrom, NextResult),
          drain_queue(State1);
        {noreply, State1} ->
          {noreply, State1}
      end
  end.
