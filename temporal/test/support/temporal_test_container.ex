defmodule Temporal.TestContainer do
  @moduledoc """
  Lightweight Temporal test container using Testcontainers.
  
  This module provides a simple way to run Temporal server in tests
  using the temporalio/auto-setup Docker image.
  """
  
  # Use Agent to maintain container state across test runs
  use Agent

  @temporal_image "temporalio/auto-setup:1.22.0"
  @temporal_grpc_port 7233
  @temporal_ui_port 8080
  @agent_name __MODULE__
  
  def start_link(_opts \\ []) do
    Agent.start_link(fn -> nil end, name: @agent_name)
  end
  
  def start_container do
    # Check if container is already running
    case get_container() do
      %{container: container, ports: ports} when not is_nil(container) ->
        IO.puts("🔄 Reusing existing Temporal test container")
        IO.puts("   gRPC: localhost:#{ports.grpc_port}")
        IO.puts("   UI: http://localhost:#{ports.ui_port}")
        {:ok, container, ports}
        
      _ ->
        start_new_container()
    end
  end
  
  defp start_new_container do
    IO.puts("🐳 Starting Temporal test containers...")
    
    # Ensure Testcontainers supervision tree is started with retry
    case ensure_testcontainers_started() do
      :ok ->
        # Start PostgreSQL container first
        postgres_config = Testcontainers.Container.new("postgres:13")
          |> Testcontainers.Container.with_exposed_ports([5432])
          |> Testcontainers.Container.with_environment("POSTGRES_DB", "temporal")
          |> Testcontainers.Container.with_environment("POSTGRES_USER", "temporal")
          |> Testcontainers.Container.with_environment("POSTGRES_PASSWORD", "temporal")
        
        case Testcontainers.start_container(postgres_config) do
          {:ok, postgres_container} ->
            postgres_port = postgres_container.exposed_ports 
              |> Enum.find(fn {internal, _} -> internal == 5432 end) 
              |> elem(1)
            
            # Wait for PostgreSQL to be ready
            case wait_for_postgres_ready(postgres_port) do
              :ok ->
                # Now start Temporal container
                temporal_config = Testcontainers.Container.new(@temporal_image)
                  |> Testcontainers.Container.with_exposed_ports([@temporal_grpc_port, @temporal_ui_port])
                  |> Testcontainers.Container.with_environment("DB", "postgresql")
                  |> Testcontainers.Container.with_environment("DB_PORT", "#{postgres_port}")
                  |> Testcontainers.Container.with_environment("POSTGRES_USER", "temporal")
                  |> Testcontainers.Container.with_environment("POSTGRES_PWD", "temporal")
                  |> Testcontainers.Container.with_environment("POSTGRES_SEEDS", "host.docker.internal")
                
                case Testcontainers.start_container(temporal_config) do
                  {:ok, container} ->
                    case extract_ports(container) do
                      {:ok, ports} ->
                        case wait_for_temporal_ready(ports.grpc_port) do
                          :ok ->
                            IO.puts("✅ Temporal containers started!")
                            IO.puts("   gRPC: localhost:#{ports.grpc_port}")
                            IO.puts("   UI: http://localhost:#{ports.ui_port}")
                            # Store both containers info in Agent
                            set_container(%{container: container, postgres_container: postgres_container, ports: ports})
                            {:ok, container, ports}
                          
                          {:error, reason} ->
                            IO.puts("❌ Temporal container failed health check: #{inspect(reason)}")
                            # Clean up both containers if health check fails
                            Testcontainers.stop_container(container.container_id, Testcontainers)
                            Testcontainers.stop_container(postgres_container.container_id, Testcontainers)
                            {:error, reason}
                        end
                      
                      {:error, reason} ->
                        IO.puts("❌ Failed to extract container ports: #{inspect(reason)}")
                        Testcontainers.stop_container(container.container_id, Testcontainers)
                        Testcontainers.stop_container(postgres_container.container_id, Testcontainers)
                        {:error, reason}
                    end
                    
                  {:error, reason} ->
                    IO.puts("❌ Failed to start Temporal container: #{inspect(reason)}")
                    Testcontainers.stop_container(postgres_container.container_id, Testcontainers)
                    {:error, reason}
                end
              
              {:error, reason} ->
                IO.puts("❌ PostgreSQL not ready: #{inspect(reason)}")
                Testcontainers.stop_container(postgres_container.container_id, Testcontainers)
                {:error, reason}
            end
          
          {:error, reason} ->
            IO.puts("❌ Failed to start PostgreSQL container: #{inspect(reason)}")
            {:error, reason}
        end
        
      {:error, reason} ->
        IO.puts("❌ Failed to start Testcontainers: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp ensure_testcontainers_started do
    case Testcontainers.start_link() do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
      {:error, _reason} ->
        # Retry once after a brief delay
        Process.sleep(100)
        case Testcontainers.start_link() do
          {:ok, _} -> :ok
          {:error, {:already_started, _}} -> :ok
          {:error, _retry_reason} ->
            IO.puts("❌ Testcontainers startup failed after retry")
            {:error, :testcontainers_startup_failed}
        end
    end
  end

  defp extract_ports(container) do
    try do
      grpc_port = container.exposed_ports 
        |> Enum.find(fn {internal, _} -> internal == @temporal_grpc_port end) 
        |> elem(1)
      
      ui_port = container.exposed_ports 
        |> Enum.find(fn {internal, _} -> internal == @temporal_ui_port end) 
        |> elem(1)
      
      {:ok, %{grpc_port: grpc_port, ui_port: ui_port}}
    rescue
      error ->
        {:error, "Failed to extract ports: #{inspect(error)}"}
    end
  end

  defp wait_for_temporal_ready(grpc_port, retries \\ 120) do
    # Use exponential backoff for better resource utilization
    case :gen_tcp.connect(~c"localhost", grpc_port, [:binary, {:active, false}], 5000) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        IO.puts("   Port #{grpc_port} is open, waiting for service initialization...")
        
        # Use Task.async for non-blocking health checks (BEAM best practice)
        task = Task.async(fn -> 
          wait_for_service_ready(grpc_port, 60)  # Increased retries for stability
        end)
        
        case Task.await(task, 65_000) do  # Match increased retry timeout
          :ok -> 
            IO.puts("   ✅ Temporal server is ready!")
            :ok
          {:error, reason} -> 
            IO.puts("   Server health check failed: #{inspect(reason)}")
            {:error, reason}
        end
      
      {:error, reason} ->
        if retries > 0 do
          # Exponential backoff with jitter (BEAM pattern)
          base_delay = 120 - retries
          jitter = :rand.uniform(500)
          delay = min(base_delay * 100 + jitter, 2000)
          Process.sleep(delay)
          wait_for_temporal_ready(grpc_port, retries - 1)
        else
          {:error, "Temporal gRPC port not ready after #{120} retries: #{inspect(reason)}"}
        end
    end
  end
  
  # New function using proper BEAM patterns
  defp wait_for_service_ready(grpc_port, retries) when retries > 0 do
    # Simple health check: ensure port is accepting connections
    # and give Temporal server extra time to fully initialize
    case :gen_tcp.connect(~c"localhost", grpc_port, [:binary, {:active, false}], 1000) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        # Port is open, but Temporal needs more time to initialize
        # Wait a bit longer to ensure server is fully ready
        IO.puts("   Waiting for Temporal to fully initialize...")
        Process.sleep(3000)
        :ok
        
      {:error, :econnrefused} ->
        # Port not open yet
        Process.sleep(1000)
        wait_for_service_ready(grpc_port, retries - 1)
        
      {:error, reason} ->
        # Other connection error
        IO.puts("   Health check error: #{inspect(reason)}")
        Process.sleep(1000)
        wait_for_service_ready(grpc_port, retries - 1)
    end
  end
  
  defp wait_for_service_ready(_grpc_port, 0) do
    {:error, "Server not ready after health check retries"}
  end
  
  defp wait_for_postgres_ready(postgres_port, retries \\ 30) do
    case :gen_tcp.connect(~c"localhost", postgres_port, [:binary, {:active, false}], 2000) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        IO.puts("   ✅ PostgreSQL is ready on port #{postgres_port}")
        :ok
      
      {:error, reason} ->
        if retries > 0 do
          IO.puts("   Waiting for PostgreSQL... (#{retries} retries left)")
          Process.sleep(1000)
          wait_for_postgres_ready(postgres_port, retries - 1)
        else
          {:error, "PostgreSQL not ready after 30 retries: #{inspect(reason)}"}
        end
    end
  end
  
  
  defp get_container do
    if Process.whereis(@agent_name) do
      Agent.get(@agent_name, & &1)
    else
      nil
    end
  end
  
  def get_running_container do
    case get_container() do
      %{container: container} when not is_nil(container) ->
        {:ok, container}
      _ ->
        {:error, :no_container}
    end
  end
  
  defp set_container(container_info) do
    if Process.whereis(@agent_name) do
      Agent.update(@agent_name, fn _ -> container_info end)
    end
  end
  
  def stop_container(container) do
    # Only stop if this is the actual container we're tracking
    case get_container() do
      %{container: stored_container, postgres_container: postgres_container} when stored_container.container_id == container.container_id ->
        IO.puts("🛑 Stopping Temporal test containers...")
        
        # Clear the stored container first
        set_container(nil)
        
        # Stop both containers
        temporal_result = case Testcontainers.stop_container(container.container_id, Testcontainers) do
          :ok ->
            IO.puts("✅ Temporal container stopped")
            :ok
          {:error, reason} ->
            IO.puts("⚠️  Warning: Failed to stop Temporal container: #{inspect(reason)}")
            {:error, reason}
        end
        
        postgres_result = case Testcontainers.stop_container(postgres_container.container_id, Testcontainers) do
          :ok ->
            IO.puts("✅ PostgreSQL container stopped")
            :ok
          {:error, reason} ->
            IO.puts("⚠️  Warning: Failed to stop PostgreSQL container: #{inspect(reason)}")
            {:error, reason}
        end
        
        case {temporal_result, postgres_result} do
          {:ok, :ok} -> :ok
          _ -> {:error, "Failed to stop one or more containers"}
        end
        
      %{container: stored_container} when stored_container.container_id == container.container_id ->
        # Handle legacy single container case
        IO.puts("🛑 Stopping Temporal test container...")
        set_container(nil)
        case Testcontainers.stop_container(container.container_id, Testcontainers) do
          :ok ->
            IO.puts("✅ Temporal container stopped")
            :ok
          {:error, reason} ->
            IO.puts("⚠️  Warning: Failed to stop container: #{inspect(reason)}")
            {:error, reason}
        end
        
      _ ->
        # Not our container or already stopped
        :ok
    end
  end
  
  def server_config(ports) do
    %{
      "target_url" => "http://localhost:#{ports.grpc_port}",
      "namespace" => "default"
    }
  end
  
  def server_url(ports), do: "http://localhost:#{ports.grpc_port}"
  def ui_url(ports), do: "http://localhost:#{ports.ui_port}"
end