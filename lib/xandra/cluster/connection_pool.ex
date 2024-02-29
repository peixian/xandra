defmodule Xandra.Cluster.ConnectionPool do
  @moduledoc false

  use Supervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    connection_opts = Keyword.fetch!(opts, :connection_options)
    pool_size = Keyword.fetch!(opts, :pool_size)
    Supervisor.start_link(__MODULE__, {connection_opts, pool_size})
  end

  @spec checkout(pid()) :: pid()
  def checkout(sup_pid) when is_pid(sup_pid) do
    [{:pool_size, pool_size}] = :ets.lookup(:pool_config, :pool_size)

    children = Supervisor.which_children(sup_pid)

    # Generate a random index and attempt to fetch a valid PID
    random_index = Enum.random(1..pool_size) - 1
    selected_child = Enum.at(children, random_index)

    IO.inspect("got pid #{inspect(selected_child)}")

    case selected_child do
      {_, pid, :worker, _} when is_pid(pid) ->
        pid
      _ ->
        # If the selected child is not a valid PID, find the first valid one
        find_first_valid_pid(children)
    end
  end

  defp find_first_valid_pid(children) do
    Enum.find_value(children, fn
      {_, pid, :worker, _} when is_pid(pid) -> pid
      _ -> nil
    end)
  end


  ## Callbacks

  @impl true
  def init({connection_opts, pool_size}) do
    children =
      for index <- 1..pool_size do
        Supervisor.child_spec({Xandra, connection_opts}, id: {Xandra, index})
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
