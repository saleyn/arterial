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
by `start_link/2`) that do the blocking I/O themselves and reply to the
caller via its own mailbox once done.

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
  reader     :: undefined | pid(),
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
`{?MODULE, Pool, N}` (`N` in `1..PoolSize`), randomly selected by
`request/3`. `Client` must be the same `c:arterial_client` callback
module the pool itself was started with.

## Examples

```
1> arterial_async_driver:start_link(my_pool, 8, test_echo_client).
[{ok,<0.200.0>}, ...]
```
""".
-spec start_link(arterial_pool:name(), pos_integer(), module()) -> [{ok, pid()}].
start_link(Pool, PoolSize, Client) ->
  persistent_term:put({?MODULE, Pool, count}, PoolSize),
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
  persistent_term:erase({?MODULE, Pool, count}).

-doc """
Send `Request` via one of `Pool`'s dispatcher processes (picked at
random) and block for its decoded reply or `TimeoutMs`, whichever comes
first.

## Examples

```
1> arterial_async_driver:request(my_pool, {echo, hello}, 5000).
{ok, hello}
```
""".
-spec request(arterial_pool:name(), term(), timeout()) ->
  {ok, term()} | {error, term()}.
request(Pool, Request, TimeoutMs) ->
  N = rand:uniform(dispatcher_count(Pool)),
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
handle_info({tcp_data, Data}, State) ->
  on_data(Data, State);
handle_info({tcp_closed, _Reason}, State) ->
  %% The connection died mid-request -- fail this one request back to
  %% its caller; arterial_connection's own reconnect logic handles
  %% getting the connection itself back up on its own schedule.
  reply_and_reset({error, connection_closed}, State);
handle_info(_Msg, State) ->
  {noreply, State}.

-doc false.
terminate(_Reason, #state{reader = undefined}) -> ok;
terminate(_Reason, #state{reader = Reader}) ->
  exit(Reader, kill),
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
%% encode+send the request and wait for handle_info({tcp_data, _}, _) to
%% decode and reply to `From`.
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
      ensure_reader(State#state{client_st = ClientSt1, from = From});
    {error, Reason} ->
      arterial_nif:checkin_connection(Pool, ConnID, State#state.req_ids, <<>>),
      {reply, {error, Reason}, State#state{client_st = ClientSt1, from = undefined}}
  end.

%% One-shot blocking-recv reader, spawned fresh for every request (NOT
%% reused across requests/persistent like a per-dispatcher loop): once a
%% connection's reply has been fully decoded, finish_request/1 checks the
%% connection back into the pool immediately, where a *different*
%% dispatcher can check it out and start its own reader on the same
%% socket right away. A persistent reader_loop that immediately re-issues
%% recv/2 after delivering one frame would still be mid-recv on that same
%% socket at that point -- two readers blocked on one socket race for
%% whichever bytes arrive next, and whichever loses is stuck forever
%% (this was a real, reproducible hang under concurrent dispatcher
%% checkout/checkin churn). Spawning fresh per request and never looping
%% means the only reader ever blocked on a given socket is the one
%% belonging to whichever dispatcher currently holds the connection.
%% Plain spawn, not spawn_link: this gen_server doesn't trap_exit, so a
%% linked reader's death (even from our own exit(Reader, kill) below)
%% would otherwise kill the dispatcher itself via the link.
ensure_reader(#state{reader = undefined, socket = Socket} = State) ->
  Self = self(),
  Reader = spawn(fun() -> reader_once(Self, Socket) end),
  drain_buffer(State#state{reader = Reader});
ensure_reader(#state{socket = Socket} = State) ->
  %% Socket may have changed (reconnect) since the reader was started.
  exit(State#state.reader, kill),
  Self = self(),
  Reader = spawn(fun() -> reader_once(Self, Socket) end),
  drain_buffer(State#state{reader = Reader}).

%% checkout_async/3 returns the connection's leftover undecoded buffer
%% (per checkin_connection/4's Buffer contract) -- if a full reply was
%% already sitting in it (shouldn't normally happen with backlog=1/one
%% in-flight request at a time, but cheap to handle), decode it without
%% waiting on the reader.
drain_buffer(#state{buf = <<>>} = State) ->
  {noreply, State};
drain_buffer(State) ->
  on_data(<<>>, State).

reader_once(Owner, Socket) ->
  case test_echo_protocol:recv(Socket, infinity) of
    {ok, Data}      -> Owner ! {tcp_data, Data};
    {error, Reason} -> Owner ! {tcp_closed, Reason}
  end.

on_data(Data, #state{client = Client, client_st = ClientSt, buf = Buf} = State) ->
  case Client:handle_data(<<Buf/binary, Data/binary>>, ClientSt) of
    {ok, [Reply | _], ClientSt1} ->
      finish_request({ok, Reply}, State#state{client_st = ClientSt1});
    {ok, [], ClientSt1} ->
      %% Not a full frame yet -- the one-shot reader has already
      %% delivered its single message and exited, so a new one must be
      %% spawned to wait for the rest of the frame (reader is still this
      %% same connection's, no reconnect -- reuse ensure_reader's "reader
      %% already set" clause, which kills-and-respawns; harmless since
      %% the old one already exited after sending its one message).
      ensure_reader(State#state{client_st = ClientSt1, buf = <<>>});
    {error, Reason, ClientSt1} ->
      finish_request({error, Reason}, State#state{client_st = ClientSt1})
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
