defmodule Temporal.WorkerTest do
  @moduledoc """
  Tests for basic worker creation functionality (Task 3.1.1).
  
  These are placeholder tests for the MVP implementation.
  Full worker functionality will be tested when polling and task completion are implemented.
  """

  use ExUnit.Case

  alias Temporal.Native

  describe "worker_new/2" do
    test "creates worker with valid configuration" do
      # Create a client first (using mock for now)
      client_config = %{
        "target_host" => "localhost:7233",
        "namespace" => "default"
      }

      # This should succeed in creating a client resource
      case Native.client_connect(client_config) do
        client_resource when is_reference(client_resource) ->
          # Now test worker creation
          worker_config = %{
            "namespace" => "test-namespace",
            "task_queue" => "test-task-queue",
            "max_outstanding_workflow_tasks" => 50,
            "max_outstanding_activities" => 100,
            "no_remote_activities" => false
          }

          # Worker creation should succeed
          result = Native.worker_new(client_resource, worker_config)
          assert is_reference(result)

        {:error, _reason} ->
          # Skip the test if we can't connect to a Temporal server
          # This is expected in CI/testing environments without Temporal running
          :ok
      end
    end

    test "fails with invalid configuration" do
      # Create a client first
      client_config = %{
        "target_host" => "localhost:7233",
        "namespace" => "default"
      }

      case Native.client_connect(client_config) do
        client_resource when is_reference(client_resource) ->
          # Test with empty task queue
          invalid_config = %{
            "namespace" => "test-namespace",
            "task_queue" => "",
            "max_outstanding_workflow_tasks" => 50
          }

          result = Native.worker_new(client_resource, invalid_config)
          assert {:error, "Task queue cannot be empty"} = result

        {:error, _reason} ->
          # Skip test if no Temporal server
          :ok
      end
    end

    test "fails with missing task_queue" do
      # Create a client first
      client_config = %{
        "target_host" => "localhost:7233", 
        "namespace" => "default"
      }

      case Native.client_connect(client_config) do
        client_resource when is_reference(client_resource) ->
          # Test with missing task_queue
          invalid_config = %{
            "namespace" => "test-namespace"
            # task_queue is missing
          }

          result = Native.worker_new(client_resource, invalid_config)
          assert {:error, "task_queue is required"} = result

        {:error, _reason} ->
          # Skip test if no Temporal server
          :ok
      end
    end

    test "fails with invalid configuration format" do
      # Create a client first
      client_config = %{
        "target_host" => "localhost:7233",
        "namespace" => "default"
      }

      case Native.client_connect(client_config) do
        client_resource when is_reference(client_resource) ->
          # Test with non-map configuration
          invalid_config = "not a map"

          result = Native.worker_new(client_resource, invalid_config)
          assert {:error, "Configuration must be a map"} = result

        {:error, _reason} ->
          # Skip test if no Temporal server
          :ok
      end
    end
  end

  describe "worker configuration defaults" do
    test "uses proper defaults for optional fields" do
      # This test verifies that our Rust implementation properly handles 
      # missing optional configuration fields by using sensible defaults
      
      client_config = %{
        "target_host" => "localhost:7233",
        "namespace" => "default"
      }

      case Native.client_connect(client_config) do
        client_resource when is_reference(client_resource) ->
          # Minimal config with just required fields
          minimal_config = %{
            "task_queue" => "test-queue"
            # namespace will default to "default"
            # all other fields will use Rust defaults
          }

          result = Native.worker_new(client_resource, minimal_config)
          # Should succeed with defaults applied
          assert is_reference(result)

        {:error, _reason} ->
          :ok
      end
    end
  end
end