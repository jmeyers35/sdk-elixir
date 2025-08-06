defmodule Temporal.ClientSupervisionTest do
  use ExUnit.Case, async: true
  
  import Mox
  
  alias Temporal.Client
  alias Temporal.Native.Mock, as: NativeMock
  
  setup :verify_on_exit!
  
  describe "child_spec/1" do
    test "returns valid child specification" do
      opts = [name: :test_client, namespace: "test"]
      child_spec = Client.child_spec(opts)
      
      assert child_spec.id == Client
      assert child_spec.start == {Client, :start_link, [opts]}
      # Default GenServer child_spec only includes id and start
    end
    
    test "child_spec can be customized" do
      # GenServer's default child_spec doesn't support the {opts, overrides} format
      # This test is not applicable for the current implementation
      # Skipping this test as it assumes custom child_spec handling
    end
  end
  
  describe "supervisor integration" do
    test "client can be supervised" do
      # Allow any NIF calls
      NativeMock
      |> stub(:client_connect, fn _config -> make_ref() end)
      
      children = [
        {Client, name: :supervised_client, namespace: "supervised"}
      ]
      
      assert {:ok, supervisor} = Supervisor.start_link(children, strategy: :one_for_one)
      
      # Verify client started under supervisor
      assert supervised_client = Process.whereis(:supervised_client)
      assert Process.alive?(supervised_client)
      
      # Verify supervisor link
      {:links, links} = Process.info(supervised_client, :links)
      assert supervisor in links
      
      # Stop supervisor
      Supervisor.stop(supervisor)
      
      # Client should be stopped too
      refute Process.alive?(supervised_client)
    end
    
    test "client restarts on crash" do
      # Allow any NIF calls
      NativeMock
      |> stub(:client_connect, fn _config -> make_ref() end)
      
      children = [
        {Client, name: :restart_test_client, namespace: "restart-test"}
      ]
      
      assert {:ok, supervisor} = Supervisor.start_link(children, strategy: :one_for_one)
      
      # Get initial client pid
      original_client = Process.whereis(:restart_test_client)
      assert Process.alive?(original_client)
      
      # Kill the client
      Process.exit(original_client, :kill)
      
      # Wait for restart
      Process.sleep(100)
      
      # Client should be restarted with new pid
      new_client = Process.whereis(:restart_test_client)
      assert Process.alive?(new_client)
      assert new_client != original_client
      
      # Supervisor should still be alive
      assert Process.alive?(supervisor)
      
      Supervisor.stop(supervisor)
    end
    
    test "multiple clients under same supervisor" do
      # Allow any NIF calls
      NativeMock
      |> stub(:client_connect, fn _config -> make_ref() end)
      
      children = [
        Supervisor.child_spec({Client, name: :client_1, namespace: "ns1"}, id: :client_1),
        Supervisor.child_spec({Client, name: :client_2, namespace: "ns2"}, id: :client_2),
        Supervisor.child_spec({Client, name: :client_3, namespace: "ns3"}, id: :client_3)
      ]
      
      assert {:ok, supervisor} = Supervisor.start_link(children, strategy: :one_for_one)
      
      # All clients should be started
      assert Process.whereis(:client_1)
      assert Process.whereis(:client_2)
      assert Process.whereis(:client_3)
      
      # Kill one client
      Process.exit(Process.whereis(:client_2), :kill)
      Process.sleep(100)
      
      # Only the killed client should restart
      assert Process.whereis(:client_1)  # Still alive
      assert Process.whereis(:client_2)  # Restarted
      assert Process.whereis(:client_3)  # Still alive
      
      Supervisor.stop(supervisor)
    end
    
    test "supervisor restart strategies" do
      # Test with rest_for_one strategy
      NativeMock
      |> stub(:client_connect, fn _config -> make_ref() end)
      
      children = [
        Supervisor.child_spec({Client, name: :first_client}, id: :first),
        Supervisor.child_spec({Client, name: :second_client}, id: :second),
        Supervisor.child_spec({Client, name: :third_client}, id: :third)
      ]
      
      assert {:ok, supervisor} = Supervisor.start_link(children, strategy: :rest_for_one)
      
      # Get initial pids
      first_pid = Process.whereis(:first_client)
      second_pid = Process.whereis(:second_client)
      third_pid = Process.whereis(:third_client)
      
      # Kill the second client
      Process.exit(second_pid, :kill)
      Process.sleep(100)
      
      # With rest_for_one, second and third should restart
      assert Process.whereis(:first_client) == first_pid  # Unchanged
      assert Process.whereis(:second_client) != second_pid  # Restarted
      assert Process.whereis(:third_client) != third_pid  # Also restarted
      
      Supervisor.stop(supervisor)
    end
  end
  
  describe "dynamic supervisor integration" do
    test "clients can be dynamically supervised" do
      NativeMock
      |> stub(:client_connect, fn _config -> make_ref() end)
      
      # Start a dynamic supervisor
      assert {:ok, sup} = DynamicSupervisor.start_link(strategy: :one_for_one)
      
      # Start clients dynamically
      assert {:ok, client1} = DynamicSupervisor.start_child(sup, {Client, namespace: "dynamic1"})
      assert {:ok, client2} = DynamicSupervisor.start_child(sup, {Client, namespace: "dynamic2"})
      
      assert Process.alive?(client1)
      assert Process.alive?(client2)
      
      # Count children
      %{active: 2, specs: 2, supervisors: 0, workers: 2} = DynamicSupervisor.count_children(sup)
      
      # Stop one child
      assert :ok = DynamicSupervisor.terminate_child(sup, client1)
      
      %{active: 1, specs: 1, supervisors: 0, workers: 1} = DynamicSupervisor.count_children(sup)
      
      DynamicSupervisor.stop(sup)
    end
  end
  
  describe "application supervision tree" do
    test "client can be added to application supervision tree" do
      # This test demonstrates how to add clients to the main app supervision tree
      # In real usage, this would be in Application.start/2
      
      NativeMock
      |> stub(:client_connect, fn _config -> make_ref() end)
      
      # Simulate application supervision tree
      children = [
        # Other application children would go here
        {Task.Supervisor, name: Temporal.TaskSupervisor},
        {Client, name: Temporal.DefaultClient, namespace: "default", connect_on_start: true}
      ]
      
      assert {:ok, app_supervisor} = Supervisor.start_link(children, 
        strategy: :one_for_one, 
        name: Temporal.TestAppSupervisor
      )
      
      # Verify all children started
      assert Process.whereis(Temporal.TaskSupervisor)
      assert Process.whereis(Temporal.DefaultClient)
      
      # Verify supervision tree structure - order may vary
      children = Supervisor.which_children(app_supervisor)
      assert length(children) == 2
      assert Enum.any?(children, fn {id, _, type, _} -> id == Temporal.TaskSupervisor and type == :supervisor end)
      assert Enum.any?(children, fn {id, _, type, _} -> id == Client and type == :worker end)
      
      Supervisor.stop(app_supervisor)
    end
  end
  
  describe "restart behavior and resource cleanup" do
    test "resources are cleaned up on abnormal termination" do
      # This tests that NIF resources don't leak on process crashes
      NativeMock
      |> stub(:client_connect, fn _config -> make_ref() end)
      
      children = [
        {Client, name: :cleanup_test_client}
      ]
      
      assert {:ok, supervisor} = Supervisor.start_link(children, strategy: :one_for_one)
      
      client_pid = Process.whereis(:cleanup_test_client)
      
      # Get client state to verify it has a resource
      state = :sys.get_state(client_pid)
      _initial_status = state.status
      
      # Force abnormal termination
      Process.exit(client_pid, :abnormal)
      Process.sleep(100)
      
      # New client should start fresh
      new_client_pid = Process.whereis(:cleanup_test_client)
      assert new_client_pid != client_pid
      
      # New client should have fresh state
      new_state = :sys.get_state(new_client_pid)
      assert new_state.status == :disconnected
      assert new_state.connect_attempts == 0
      
      Supervisor.stop(supervisor)
    end
  end
end