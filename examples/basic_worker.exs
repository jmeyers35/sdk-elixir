#!/usr/bin/env elixir

# Basic Worker Creation Example
# 
# This example demonstrates how to create a Temporal worker using the Elixir SDK.
# Note: This is for Task 3.1.1 (Worker Creation) only.
# Actual polling and task processing will be implemented in Tasks 3.1.2 and 3.1.3.

alias Temporal.Native

defmodule BasicWorkerExample do
  @doc """
  Demonstrates basic worker creation with the Temporal Elixir SDK.
  """
  def run() do
    IO.puts("🔧 Basic Worker Creation Example")
    IO.puts("================================")

    # Step 1: Create a client connection
    IO.puts("\n📡 Step 1: Creating client connection...")
    
    client_config = %{
      "target_host" => "localhost:7233",
      "namespace" => "default"
    }

    case Native.client_connect(client_config) do
      client_resource when is_reference(client_resource) ->
        IO.puts("✅ Client connected successfully!")
        
        # Step 2: Create a worker
        create_worker(client_resource)

      {:error, reason} ->
        IO.puts("❌ Failed to connect to Temporal server: #{reason}")
        IO.puts("   Make sure Temporal server is running on localhost:7233")
        IO.puts("   You can start Temporal using: temporal server start-dev")
        :error
    end
  end

  defp create_worker(client_resource) do
    IO.puts("\n🏗️  Step 2: Creating worker...")
    
    # Worker configuration
    worker_config = %{
      "namespace" => "default",
      "task_queue" => "hello-world-queue",
      "max_outstanding_workflow_tasks" => 10,
      "max_outstanding_activities" => 20,
      "max_outstanding_local_activities" => 10,
      "no_remote_activities" => false,
      "sticky_queue_schedule_to_start_timeout_ms" => 10_000,
      "max_heartbeat_throttle_interval_ms" => 60_000,
      "default_heartbeat_throttle_interval_ms" => 5_000
    }

    case Native.worker_new(client_resource, worker_config) do
      worker_resource when is_reference(worker_resource) ->
        IO.puts("✅ Worker created successfully!")
        IO.puts("   Namespace: #{worker_config["namespace"]}")
        IO.puts("   Task Queue: #{worker_config["task_queue"]}")
        IO.puts("   Max Workflow Tasks: #{worker_config["max_outstanding_workflow_tasks"]}")
        IO.puts("   Max Activities: #{worker_config["max_outstanding_activities"]}")
        
        IO.puts("\n📋 Worker Resource: #{inspect(worker_resource)}")
        IO.puts("\n✨ Worker creation completed!")
        IO.puts("   Note: This is a placeholder implementation for Task 3.1.1")
        IO.puts("   Polling and task processing will be available in Tasks 3.1.2 and 3.1.3")
        
        :ok

      {:error, reason} ->
        IO.puts("❌ Failed to create worker: #{reason}")
        :error
    end
  end

  def run_with_minimal_config() do
    IO.puts("\n🔧 Minimal Configuration Example")
    IO.puts("================================")

    # Create client
    client_config = %{
      "target_host" => "localhost:7233",
      "namespace" => "default"
    }

    case Native.client_connect(client_config) do
      client_resource when is_reference(client_resource) ->
        IO.puts("✅ Client connected!")

        # Minimal worker config (only required fields)
        minimal_config = %{
          "task_queue" => "minimal-queue"
          # namespace defaults to "default"
          # all other fields use Rust implementation defaults
        }

        case Native.worker_new(client_resource, minimal_config) do
          worker_resource when is_reference(worker_resource) ->
            IO.puts("✅ Worker created with minimal config!")
            IO.puts("   Task Queue: minimal-queue")
            IO.puts("   Namespace: default (defaulted)")
            IO.puts("   Other settings: using defaults")
            :ok

          {:error, reason} ->
            IO.puts("❌ Failed to create worker: #{reason}")
            :error
        end

      {:error, reason} ->
        IO.puts("❌ Failed to connect: #{reason}")
        :error
    end
  end

  def demonstrate_validation() do
    IO.puts("\n🔧 Configuration Validation Example")
    IO.puts("===================================")

    client_config = %{
      "target_host" => "localhost:7233",
      "namespace" => "default"
    }

    case Native.client_connect(client_config) do
      client_resource when is_reference(client_resource) ->
        IO.puts("✅ Client connected!")

        # Test various invalid configurations
        test_cases = [
          {%{}, "Missing task_queue should fail"},
          {%{"task_queue" => ""}, "Empty task_queue should fail"},
          {"not a map", "Non-map config should fail"}
        ]

        Enum.each(test_cases, fn {config, description} ->
          IO.puts("\n🧪 #{description}...")
          
          case Native.worker_new(client_resource, config) do
            {:error, reason} ->
              IO.puts("  ✅ Correctly failed: #{reason}")
              
            unexpected ->
              IO.puts("  ❌ Unexpected result: #{inspect(unexpected)}")
          end
        end)

      {:error, reason} ->
        IO.puts("❌ Failed to connect: #{reason}")
    end
  end
end

# Run the examples
IO.puts("Starting Basic Worker Creation Examples...\n")

case BasicWorkerExample.run() do
  :ok ->
    BasicWorkerExample.run_with_minimal_config()
    BasicWorkerExample.demonstrate_validation()
    
    IO.puts("\n🎉 All examples completed!")
    IO.puts("   Next steps: Implement Tasks 3.1.2 and 3.1.3 for polling and task completion")

  :error ->
    IO.puts("\n⚠️  Examples skipped due to connection issues")
    IO.puts("   Start Temporal server with: temporal server start-dev")
end