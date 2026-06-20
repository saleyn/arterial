-module(arterial_proxy).
-behaviour(gen_server).

-export([start_link/1, send/4]).
-export([init/1, handle_cast/2]).

-record(state, {
  pool,
  pool_size,
  conn_index,
  conns        = queue:new(),
  conns_count  = 0,
  max_conn     = 10,
  backlog      = queue:new(),
  throt_window = {1000000, 1000}
}).

start_link(Pool) ->
  Name = name(Pool),
  persistent_term:get(Name) == undefined andalso persistent_term:put(Name),
  gen_server:start_link({local, Name}, ?MODULE, [Pool]).

send(Pool, ReqID, Data, Expiration) when is_integer(Expiration) ->
  gen_server:cast(name(Pool), {req, self(), Pool, ReqID, Data, Expiration}).

init(Pool) ->
  {ok, #state{pool = Pool, backlog = queue:new()}}.

handle_cast({req, Pid, Pool, ReqID, Data, Expiration} = Req,
            #state{conns_count = 0, throt_window = {Rate, Win}} = State) ->
  case arterial_nif:checkout_connection(Pool, async) of
    {ok, Conn = #{
      conn_ref := ConnRef,
      protocol := Proto,
      socket   := Socket,
      buffer   := Buffer
    }} ->
      TS = os:system_time(microsecond),
      Timeout = arterial_utils:calc_expiration(TS, Expiration),
      maybe
        ok ?= Proto:send(Socket, Bin, Timeout),
        ok ?= Proto:setopts(Socket, [{active, 10}]),
        Throttle = throttle:new(Rate, Win, TS)
                 / throttle:add(),
        Q1       = queue:in({Conn, Throttle}, State#state.queue),
        {noreply, State#state{conns = Q1, conns_count = 1}}
      else
        {error, Reason} ->
          arterial_nif:checkin_connection(Pool, ConnRef),
          Pid ! {ReqID, {error, Reason}},
          {noreply, State}
      end;
    {error, _} = Error ->
      {noreply, State#state{backlog = queue:in(Req, State#state.backlog)}}
  end;

handle_cast({req, Pid, Pool, ReqID, Data, Expiration} = Req, State) ->
  Now = os:system_time(microsecond),
  case find_connection(State, Now) of

  case arterial_nif:checkout_connection(Pool, async) of
    {ok, Conn = #{
      conn_ref := ConnRef,
      protocol := Proto,
      socket   := Socket,
      buffer   := Buffer
    }} ->
      TS = os:system_time(microsecond),
      Timeout = arterial_utils:calc_expiration(TS, Expiration),
      maybe
        ok ?= Proto:send(Socket, Bin, Timeout),
        ok ?= Proto:setopts(Socket, [{active, 10}]),
        Throttle = throttle:new(Rate, Win, TS)
                 / throttle:add(),
        Q1       = queue:in({Conn, Throttle}, State#state.queue),
        {noreply, State#state{conns = Q1, conns_count = 1}}
      else
        {error, Reason} ->
          arterial_nif:checkin_connection(Pool, ConnRef),
          Pid ! {ReqID, {error, Reason}},
          {noreply, State}
      end;
    {error, _} = Error ->
      {noreply, State#state{backlog = queue:in(Req, State#state.backlog)}}
  end.

find_connection(#state{conns = CQ, throt_window = {Rate, Win}, conn_count = CN} = State, Now) ->
  case queue:out(CQ) of
    {{value, {Conn, Throttle}}, Q} ->
      case throttle:available(Throttle) of
        0 when CN < State#state.max_conn ->
          case arterial_nif:checkout_connection(Pool, async) of
            {ok, _} = Conn ->
              TT = throttle:new(Rate, Win, Now),
              {ok, Conn, TT, State#state{conns = CQ}};
            {error, _} ->
              {error, no_avail_connections, State}
          end;
        0 ->
          {error, backlog_full, State};
        _ ->
          TT = throttle:add(Throttle),
          {ok, Conn, TT, State#state{conn_count = CN-1}}
      end;
    {empty, Q} when CN < State#state.max_conn ->
      case arterial_nif:checkout_connection(Pool, async) of
        {ok, _} = Conn ->
          TT = throttle:new(Rate, Win, Now),
          {ok, Conn, TT, State#state{conns = CQ}};
        {error, _} ->
          {error, no_avail_connections, State}
      end
  end.


name(Pool) -> {?MODULE, Pool}.