defmodule Arterial.Pool do
  use GenServer, shutdown: 10_000

  @on_load :load_nifs

  @doc "Start the server"
  def start_link(args), do:
    GenServer.start_link(__MODULE__, args)

  def start_link(name, args), do:
    GenServer.start_link(__MODULE__, args, name: name)

  def call(name, args), do:
    GenServer.call(name, args)

  def cast(name, args), do:
    GenServer.cast(name, args)

  # Callbacks

  @impl true
  def init(args) do
    state = %{}
    {:ok, state}
  end

  @impl true
  def handle_call(:pop, _from, [head | tail]) do
    {:reply, head, tail}
  end

  @impl true
  def handle_cast({:push, element}, state) do
    {:noreply, [element | state]}
  end


  def load_nifs do
    lib =
      case :code.priv_dir(:application.get_application()) do
        {:error, _} ->
          :code.which(__MODULE__)
          |> Path.dirname
          |> Path.dirname
        dir ->
          List.to_string(dir)
      end
      |> Path.join("priv")

    :erlang.load_nif(String.to_charlist(lib), 0)
  end

  defp create(name, size) do
    pool = init_nif(name, size)
    :persistent_term.put({__MODULE__, name}, pool)
    pool
  end

  defp destroy(name), do:
    :persistent_term.erase({__MODULE__, name})

  defp init_nif(pool_name, pool_size), do: raise "init_nif not implemented"
  defp CheckOut_nif(),                 do: raise "CheckOut_nif not implemented"
  defp CheckIn_nif(connection),        do: raise "CheckIn_nif not implemented"
end
