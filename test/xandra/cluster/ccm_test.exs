defmodule Xandra.Cluster.CCMTest do
  use ExUnit.Case

  import Xandra.TestHelper, only: [cmd!: 2]

  @moduletag :integration
  @moduletag :ccm

  @cluster_name "xandra_test_cluster"
  @cassandra_version "4.1.3"
  @node_count 3

  test "integration" do
    validate_ifaddresses()

    if ccm("list") =~ "#{@cluster_name}" do
      ccm("switch #{@cluster_name}")
    else
      ccm("create #{@cluster_name} -v #{@cassandra_version}")
      ccm("populate -n #{@node_count}")
    end

    ccm("start")

    cluster =
      start_supervised!(
        {Xandra.Cluster, nodes: ["127.0.0.1"], target_pools: 2, sync_connect: 1000}
      )

    cluster_state = :sys.get_state(cluster)
    assert %{{{127, 0, 0, 1}, 9042} => _, {{127, 0, 0, 2}, 9042} => _} = cluster_state.pools

    assert {{:connected, _connected_node}, ctrl_conn_state} =
             :sys.get_state(cluster_state.control_connection)

    assert %{
             {{127, 0, 0, 1}, 9042} => %{host: _host1, status: :up},
             {{127, 0, 0, 2}, 9042} => %{host: _host2, status: :up},
             {{127, 0, 0, 3}, 9042} => %{host: _host3, status: :up}
           } = ctrl_conn_state.peers

    assert [
             {{{{127, 0, 0, 1}, 9042}, 1}, _pid1, :up},
             {{{{127, 0, 0, 2}, 9042}, 1}, _pid2, :up}
           ] =
             cluster_state.registry
             |> Registry.select([{{:"$1", :"$2", :"$3"}, [], [{{:"$1", :"$2", :"$3"}}]}])
             |> Enum.sort()
  end

  defp ccm(args) do
    cmd!("ccm", String.split(args))
  end

  defp validate_ifaddresses do
    {:ok, addresses} = :inet.getifaddrs()
    assert {~c"lo0", info} = List.keyfind!(addresses, ~c"lo0", 0)

    localhosts = for {:addr, {127, 0, 0, _} = addr} <- info, do: addr

    assert Enum.sort(localhosts) == [
             {127, 0, 0, 1},
             {127, 0, 0, 2},
             {127, 0, 0, 3}
           ]
  end
end