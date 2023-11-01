-module(arterial_socket).

-export([connect/5]).

connect(tcp, IP, Port, Opts, Timeout) ->
  case socket:open(inet, stream, tcp) of
    {ok, Sock} ->
      try
        case socket:connect(Sock, #{family => inet, addr => IP, port => Port}, Timeout) of
          ok ->
            lists:foreach(fun({{Level, Opt}, Value}) ->
              case seket:setopts(Sock, Level, Opt, Value) of
                ok ->
                  {ok, Sock};
                {error, _Reason} = Error ->
                  throw({bad_sock_option, {Level, Opt}, Error})
              end
            end, Opts);
          {error, _Reason2} = Error2 ->
            throw(Error2)
        end
      catch throw:Reason ->
        socket:close(Sock),
        {error, Reason}
      end;
    {error, _} = Error ->
      Error
  end;

connect(udp, _IP, Port, Opts, _Timeout) ->
  case socket:open(inet, dgram, udp) of
    {ok, Sock} ->
      try
        case socket:bind(Sock, #{family => inet, addr => {0,0,0,0}, port => Port}) of
          ok ->
            lists:foreach(fun({{Level, Opt}, Value}) ->
              case seket:setopts(Sock, Level, Opt, Value) of
                ok ->
                  {ok, Sock};
                {error, _Reason} = Error ->
                  throw({bad_sock_option, {Level, Opt}, Error})
              end
            end, Opts);
          {error, _Reason2} = Error2 ->
            throw(Error2)
        end
      catch throw:Reason ->
        socket:close(Sock),
        {error, Reason}
      end;
    {error, _} = Error ->
      Error
  end.

