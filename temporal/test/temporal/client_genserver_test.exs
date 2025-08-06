defmodule Temporal.ClientGenServerTest do
  use ExUnit.Case, async: false
  
  alias Temporal.Client
  alias Temporal.TestContainer
  
  @moduletag :integration
  @moduletag timeout: 60_000
  
  setup_all do
    case TestContainer.start_container() do
      {:ok, container, ports} ->
        {:ok, %{container: container, ports: ports}}
      
      {:error, reason} ->
        IO.puts("⚠️  Skipping tests: Could not start Temporal container: #{inspect(reason)}")
        IO.puts("   Make sure Docker is installed and running")
        {:skip, "Docker container failed to start: #{inspect(reason)}"}
    end
  end
  
  describe "start_link/1" do
    test "starts client process with default configuration" do
      {:ok, client} = Client.start_link()
      assert Process.alive?(client)
      assert Client.status(client) == :disconnected
      Client.stop(client)
    end
    
    test "starts client process with custom configuration" do
      {:ok, client} = Client.start_link(
        target_url: "custom:7233",
        namespace: "test-namespace"
      )
      
      assert Process.alive?(client)
      assert Client.status(client) == :disconnected
      Client.stop(client)
    end
    
    test "starts client process with registered name" do
      {:ok, client} = Client.start_link(name: :test_client)
      assert Process.whereis(:test_client) == client
      Client.stop(client)
    end
    
    test "connects on start when requested", context do
      case context do
        %{ports: ports} ->
          {:ok, client} = Client.start_link(
            target_url: TestContainer.server_url(ports),
            namespace: "default",
            connect_on_start: true
          )
          
          # Connection should be established synchronously via handle_continue
          assert Client.status(client) == :connected
          Client.stop(client)
          
        _ ->
          {:skip, "No test container available"}
      end
    end
  end
  
  describe "connect/1" do
    test "establishes connection to Temporal server", context do
      case context do
        %{ports: ports} ->
          {:ok, client} = Client.start_link(
            target_url: TestContainer.server_url(ports),
            namespace: "default"
          )
          
          assert Client.status(client) == :disconnected
          assert :ok = Client.connect(client)
          assert Client.status(client) == :connected
          
          Client.stop(client)
          
        _ ->
          {:skip, "No test container available"}
      end
    end
    
    test "returns ok if already connected", context do
      case context do
        %{ports: ports} ->
          {:ok, client} = Client.start_link(
            target_url: TestContainer.server_url(ports),
            namespace: "default"
          )
          
          # First connection
          assert :ok = Client.connect(client)
          assert Client.status(client) == :connected
          
          # Second connection should also return ok
          assert :ok = Client.connect(client)
          assert Client.status(client) == :connected
          
          Client.stop(client)
          
        _ ->
          {:skip, "No test container available"}
      end
    end
    
    test "returns error for invalid server" do
      {:ok, client} = Client.start_link(
        target_url: "invalid-host:7233",
        namespace: "default"
      )
      
      assert {:error, reason} = Client.connect(client)
      assert is_binary(reason)
      # Validate meaningful error content - can be DNS error or connection error
      assert reason =~ ~r/(connection (failed|refused|timeout)|dns error|failed to lookup)/i
      assert Client.status(client) == :error
      
      Client.stop(client)
    end
  end
  
  describe "start_workflow/2" do
    setup context do
      case context do
        %{ports: ports} ->
          {:ok, client} = Client.start_link(
            target_url: TestContainer.server_url(ports),
            namespace: "default"
          )
          
          {:ok, %{client: client, ports: ports}}
          
        _ ->
          {:skip, "No test container available"}
      end
    end
    
    test "starts workflow with valid parameters", %{client: client} do
      # Ensure connected
      assert :ok = Client.connect(client)
      
      result = Client.start_workflow(client, %{
        workflow_type: "TestWorkflow",
        task_queue: "test-queue",
        workflow_id: "test-wf-#{System.unique_integer([:positive])}"
      })
      
      assert {:ok, handle} = result
      assert is_map(handle)
      assert Map.has_key?(handle, "workflow_id")
      assert Map.has_key?(handle, "run_id")
      
      Client.stop(client)
    end
    
    test "auto-connects if not connected", %{client: client} do
      # Should not be connected initially
      assert Client.status(client) == :disconnected
      
      result = Client.start_workflow(client, %{
        workflow_type: "TestWorkflow",
        task_queue: "test-queue",
        workflow_id: "test-wf-#{System.unique_integer([:positive])}"
      })
      
      # Should succeed and auto-connect
      assert {:ok, handle} = result
      assert is_map(handle)
      assert Client.status(client) == :connected
      
      Client.stop(client)
    end
    
    test "handles atom keys in parameters", %{client: client} do
      assert :ok = Client.connect(client)
      
      # Use atom keys instead of strings
      result = Client.start_workflow(client, %{
        workflow_type: :TestWorkflow,
        task_queue: "test-queue",
        workflow_id: "test-wf-#{System.unique_integer([:positive])}"
      })
      
      assert {:ok, handle} = result
      assert is_map(handle)
      
      Client.stop(client)
    end
    
    test "includes optional parameters", %{client: client} do
      assert :ok = Client.connect(client)
      
      result = Client.start_workflow(client, %{
        workflow_type: "TestWorkflow",
        task_queue: "test-queue",
        workflow_id: "test-wf-#{System.unique_integer([:positive])}",
        execution_timeout: 3600,
        run_timeout: 1800,
        task_timeout: 60
      })
      
      assert {:ok, handle} = result
      assert is_map(handle)
      
      Client.stop(client)
    end
    
    test "returns error when not connected and can't connect" do
      # Create client with invalid config
      {:ok, client} = Client.start_link(
        target_url: "invalid-host:99999",
        namespace: "default"
      )
      
      result = Client.start_workflow(client, %{
        workflow_type: "TestWorkflow",
        task_queue: "test-queue",
        workflow_id: "test-id"
      })
      
      assert {:error, :not_connected} = result
      
      Client.stop(client)
    end
  end
  
  describe "telemetry events" do
    test "emits init event on startup" do
      self = self()
      
      :telemetry.attach(
        "test-init",
        [:temporal, :client, :init],
        fn event, measurements, metadata, _ ->
          send(self, {:telemetry, event, measurements, metadata})
        end,
        nil
      )
      
      {:ok, client} = Client.start_link(namespace: "test-ns")
      
      assert_receive {:telemetry, [:temporal, :client, :init], %{}, %{options: opts}}
      assert Keyword.get(opts, :namespace) == "test-ns"
      
      Client.stop(client)
      :telemetry.detach("test-init")
    end
    
    test "emits connect event on connection", context do
      case context do
        %{ports: ports} ->
          self = self()
          
          :telemetry.attach(
            "test-connect",
            [:temporal, :client, :connect],
            fn event, measurements, metadata, _ ->
              send(self, {:telemetry, event, measurements, metadata})
            end,
            nil
          )
          
          {:ok, client} = Client.start_link(
            target_url: TestContainer.server_url(ports),
            namespace: "default"
          )
          
          Client.connect(client)
          
          assert_receive {:telemetry, [:temporal, :client, :connect], measurements, metadata}
          assert is_integer(measurements.duration)
          assert metadata.success == true
          assert metadata.config["namespace"] == "default"
          
          Client.stop(client)
          :telemetry.detach("test-connect")
          
        _ ->
          {:skip, "No test container available"}
      end
    end
    
    test "emits workflow started event", context do
      case context do
        %{ports: ports} ->
          self = self()
          
          :telemetry.attach(
            "test-workflow-started",
            [:temporal, :client, :workflow, :started],
            fn event, measurements, metadata, _ ->
              send(self, {:telemetry, event, measurements, metadata})
            end,
            nil
          )
          
          {:ok, client} = Client.start_link(
            target_url: TestContainer.server_url(ports),
            namespace: "default"
          )
          
          workflow_id = "test-wf-#{System.unique_integer([:positive])}"
          
          {:ok, _handle} = Client.start_workflow(client, %{
            workflow_type: "TestWorkflow",
            task_queue: "test-queue",
            workflow_id: workflow_id
          })
          
          assert_receive {:telemetry, [:temporal, :client, :workflow, :started], %{}, metadata}
          assert metadata.workflow_id == workflow_id
          assert is_binary(metadata.run_id)
          
          Client.stop(client)
          :telemetry.detach("test-workflow-started")
          
        _ ->
          {:skip, "No test container available"}
      end
    end
  end
  
  describe "configuration management" do
    test "uses application environment configuration" do
      # Set application config
      original_config = Application.get_env(:temporal, Client)
      Application.put_env(:temporal, Client, 
        target_url: "app-config:7233",
        namespace: "app-namespace"
      )
      
      {:ok, client} = Client.start_link()
      
      # Access GenServer state to validate configuration
      state = :sys.get_state(client)
      assert state.config["target_url"] == "app-config:7233"
      assert state.config["namespace"] == "app-namespace"
      
      Client.stop(client)
      
      # Restore original config
      if original_config do
        Application.put_env(:temporal, Client, original_config)
      else
        Application.delete_env(:temporal, Client)
      end
    end
    
    test "command line options override application config" do
      # Set application config
      original_config = Application.get_env(:temporal, Client)
      Application.put_env(:temporal, Client, 
        target_url: "app-config:7233",
        namespace: "app-namespace"
      )
      
      {:ok, client} = Client.start_link(
        namespace: "override-namespace"
      )
      
      # Validate that command-line options take precedence
      state = :sys.get_state(client)
      assert state.config["target_url"] == "app-config:7233"  # From app config
      assert state.config["namespace"] == "override-namespace"  # From options
      
      Client.stop(client)
      
      # Restore original config
      if original_config do
        Application.put_env(:temporal, Client, original_config)
      else
        Application.delete_env(:temporal, Client)
      end
    end
    
    test "defaults are used when no config provided" do
      # Ensure no application config
      original_config = Application.get_env(:temporal, Client)
      Application.delete_env(:temporal, Client)
      
      {:ok, client} = Client.start_link()
      
      # Validate defaults are applied
      state = :sys.get_state(client)
      assert state.config["target_url"] == "localhost:7233"
      assert state.config["namespace"] == "default"
      
      Client.stop(client)
      
      # Restore original config
      if original_config do
        Application.put_env(:temporal, Client, original_config)
      end
    end
  end
  
  describe "process lifecycle" do
    test "cleans up resources on termination", context do
      case context do
        %{ports: ports} ->
          {:ok, client} = Client.start_link(
            target_url: TestContainer.server_url(ports),
            namespace: "default"
          )
          
          # Connect to create a resource
          assert :ok = Client.connect(client)
          
          # Stop the process
          Client.stop(client)
          
          # Process should be dead
          refute Process.alive?(client)
          
        _ ->
          {:skip, "No test container available"}
      end
    end
    
    test "handles concurrent workflow starts", context do
      case context do
        %{ports: ports} ->
          {:ok, client} = Client.start_link(
            target_url: TestContainer.server_url(ports),
            namespace: "default"
          )
          
          # Connect first
          assert :ok = Client.connect(client)
          
          # Start multiple workflows concurrently
          tasks = for i <- 1..5 do
            Task.async(fn ->
              Client.start_workflow(client, %{
                workflow_type: "TestWorkflow",
                task_queue: "test-queue",
                workflow_id: "concurrent-wf-#{i}-#{System.unique_integer([:positive])}"
              })
            end)
          end
          
          results = Task.await_many(tasks, 5000)
          
          # All should succeed
          for result <- results do
            assert {:ok, handle} = result
            assert is_map(handle)
          end
          
          Client.stop(client)
          
        _ ->
          {:skip, "No test container available"}
      end
    end
  end
end