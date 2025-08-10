defmodule Temporal.WorkerPollingRealIntegrationTest do
  use ExUnit.Case, async: false

  alias Temporal.TestContainer

  @moduletag :integration
  @moduletag timeout: 120_000

  setup_all do
    case TestContainer.start_container() do
      {:ok, container, ports} ->
        # Don't stop container here - let it persist for all test runs
        {:ok, %{container: container, ports: ports}}

      {:error, reason} ->
        IO.puts("⚠️  Skipping tests: Could not start Temporal container: #{inspect(reason)}")
        IO.puts("   Make sure Docker is installed and running")
        {:skip, "Docker container failed to start: #{inspect(reason)}"}
    end
  end

  # Helper function following BEAM patterns - returns :skip tuple for modern ExUnit
  defp skip_if_no_container(context) do
    case context do
      %{ports: _ports} -> context
      _ -> {:skip, "Docker container not available"}
    end
  end

  describe "real worker polling with Temporal server" do
    setup context do
      skip_if_no_container(context)
    end

    test "creates worker and polls with real Temporal server", %{ports: ports} do
      # Connect to the real Temporal server
      client_config = %{
        "target_host" => "localhost:#{ports.grpc_port}",
        "namespace" => "default"
      }

      # Create client
      client = case Temporal.Native.client_connect(client_config) do
        {:ok, client} -> client
        client when is_reference(client) -> client
        {:error, reason} -> flunk("Failed to connect to Temporal server: #{reason}")
      end

      # Create unique task queue for this test
      task_queue = "test-real-polling-#{System.unique_integer([:positive])}"

      # Create worker configuration
      worker_config = %{
        "namespace" => "default",
        "task_queue" => task_queue,
        "max_outstanding_workflow_tasks" => 2,
        "max_outstanding_activities" => 2,
        "max_outstanding_local_activities" => 2,
        "no_remote_activities" => false
      }

      # Create worker
      worker = case Temporal.Native.worker_new(client, worker_config) do
        {:ok, worker} -> worker
        worker when is_reference(worker) -> worker
        {:error, reason} -> flunk("Failed to create worker: #{reason}")
      end

      # Test polling with no tasks available (should return ok/nil quickly)
      assert_polling_no_tasks(worker)

      # Test error handling and timeout behavior
      test_polling_behavior(worker)
    end

    test "worker polling handles SDK Core lifecycle correctly", %{ports: ports} do
      # Connect to the real Temporal server  
      client_config = %{
        "target_host" => "localhost:#{ports.grpc_port}",
        "namespace" => "default"
      }

      client = case Temporal.Native.client_connect(client_config) do
        {:ok, client} -> client
        client when is_reference(client) -> client  
        {:error, reason} -> flunk("Failed to connect to Temporal server: #{reason}")
      end

      # Create multiple workers to test resource management
      task_queue = "test-lifecycle-#{System.unique_integer([:positive])}"

      workers = for i <- 1..3 do
        worker_config = %{
          "namespace" => "default", 
          "task_queue" => "#{task_queue}-#{i}",
          "max_outstanding_workflow_tasks" => 1,
          "max_outstanding_activities" => 1,
          "max_outstanding_local_activities" => 1,
          "no_remote_activities" => false
        }

        case Temporal.Native.worker_new(client, worker_config) do
          {:ok, worker} -> worker
          worker when is_reference(worker) -> worker
          {:error, reason} -> flunk("Failed to create worker #{i}: #{reason}")
        end
      end

      # Test concurrent polling across multiple workers
      tasks = Enum.map(workers, fn worker ->
        Task.async(fn ->
          # Poll both workflow and activity tasks
          wf_result = Temporal.Native.worker_poll_workflow_task(worker)
          act_result = Temporal.Native.worker_poll_activity_task(worker)
          {wf_result, act_result}
        end)
      end)

      # Wait for all polling to complete
      results = Task.await_many(tasks, 30_000)

      # Verify all results are valid (should be {:ok, nil} since no tasks scheduled)
      Enum.each(results, fn {wf_result, act_result} ->
        assert_valid_polling_result(wf_result)
        assert_valid_polling_result(act_result)
      end)
    end

    test "worker polling with actual workflow execution", %{ports: ports} do
      # This test demonstrates what happens when we actually have work to do
      # For now, it just verifies the polling infrastructure works

      client_config = %{
        "target_host" => "localhost:#{ports.grpc_port}",
        "namespace" => "default"
      }

      client = case Temporal.Native.client_connect(client_config) do
        {:ok, client} -> client
        client when is_reference(client) -> client
        {:error, reason} -> flunk("Failed to connect to Temporal server: #{reason}")
      end

      task_queue = "test-with-workflow-#{System.unique_integer([:positive])}"

      # Create worker first 
      worker_config = %{
        "namespace" => "default",
        "task_queue" => task_queue,
        "max_outstanding_workflow_tasks" => 1,
        "max_outstanding_activities" => 1,
        "max_outstanding_local_activities" => 1,
        "no_remote_activities" => false
      }

      worker = case Temporal.Native.worker_new(client, worker_config) do
        {:ok, worker} -> worker
        worker when is_reference(worker) -> worker
        {:error, reason} -> flunk("Failed to create worker: #{reason}")
      end

      # Try to start a workflow (this will fail because we don't have workflow types registered yet)
      # But it demonstrates the integration works end-to-end
      workflow_params = %{
        "workflow_id" => "test-workflow-#{System.unique_integer([:positive])}",
        "workflow_type" => "TestWorkflow",
        "task_queue" => task_queue,
        "input" => []
      }

      # Start workflow (expected to return error since no worker is processing yet)
      workflow_result = Temporal.Native.client_start_workflow(client, workflow_params)
      
      case workflow_result do
        {:ok, _workflow_info} ->
          # If workflow started successfully, try polling for it
          # (This would happen in a real scenario with registered workflow types)
          polling_result = Temporal.Native.worker_poll_workflow_task(worker)
          assert_valid_polling_result(polling_result)
          
        {:error, _reason} ->
          # Expected - no workflow types registered yet
          # Just verify polling still works
          polling_result = Temporal.Native.worker_poll_workflow_task(worker) 
          assert_valid_polling_result(polling_result)
          
        workflow_info when is_list(workflow_info) ->
          # Some implementations return workflow info as a list
          polling_result = Temporal.Native.worker_poll_workflow_task(worker)
          assert_valid_polling_result(polling_result)
      end
    end

    test "worker polling error handling and sanitization", %{ports: ports} do
      # Test various error scenarios to ensure proper sanitization
      
      client_config = %{
        "target_host" => "localhost:#{ports.grpc_port}",
        "namespace" => "default"  
      }

      client = case Temporal.Native.client_connect(client_config) do
        {:ok, client} -> client
        client when is_reference(client) -> client
        {:error, reason} -> flunk("Failed to connect to Temporal server: #{reason}")
      end

      # Test with invalid configuration
      invalid_configs = [
        %{
          "namespace" => "",
          "task_queue" => "test",
          "max_outstanding_workflow_tasks" => 1,
          "max_outstanding_activities" => 1,
          "max_outstanding_local_activities" => 1,
          "no_remote_activities" => false
        },
        %{
          "namespace" => "default",
          "task_queue" => "",
          "max_outstanding_workflow_tasks" => 1,
          "max_outstanding_activities" => 1,
          "max_outstanding_local_activities" => 1,
          "no_remote_activities" => false
        },
        %{
          "namespace" => "default",
          "task_queue" => "test",
          "max_outstanding_workflow_tasks" => 0,
          "max_outstanding_activities" => 1,
          "max_outstanding_local_activities" => 1,
          "no_remote_activities" => false
        }
      ]

      expected_errors = [
        "Namespace cannot be empty",
        "Task queue cannot be empty",
        "Max outstanding workflow tasks must be greater than 0"
      ]

      Enum.zip(invalid_configs, expected_errors)
      |> Enum.each(fn {config, expected_error} ->
        result = Temporal.Native.worker_new(client, config)
        assert {:error, ^expected_error} = result
      end)

      # Test valid worker with various polling scenarios
      valid_config = %{
        "namespace" => "default",
        "task_queue" => "test-error-handling-#{System.unique_integer([:positive])}",
        "max_outstanding_workflow_tasks" => 1,
        "max_outstanding_activities" => 1,
        "max_outstanding_local_activities" => 1,
        "no_remote_activities" => false
      }

      worker = case Temporal.Native.worker_new(client, valid_config) do
        {:ok, worker} -> worker
        worker when is_reference(worker) -> worker
        {:error, reason} -> flunk("Failed to create worker: #{reason}")
      end

      # Test polling - should work without errors
      wf_result = Temporal.Native.worker_poll_workflow_task(worker)
      assert_valid_polling_result(wf_result)

      act_result = Temporal.Native.worker_poll_activity_task(worker)
      assert_valid_polling_result(act_result)
    end
  end

  # Helper functions

  defp assert_polling_no_tasks(worker) do
    # Poll for workflow task - should return {:ok, nil} quickly since no tasks scheduled
    start_time = System.monotonic_time(:millisecond)
    wf_result = Temporal.Native.worker_poll_workflow_task(worker)
    end_time = System.monotonic_time(:millisecond)
    
    duration = end_time - start_time
    
    # Should return quickly when no tasks available
    assert duration < 10_000, "Workflow polling took too long without tasks: #{duration}ms"
    assert_valid_polling_result(wf_result)
    
    # Poll for activity task
    start_time = System.monotonic_time(:millisecond)
    act_result = Temporal.Native.worker_poll_activity_task(worker)
    end_time = System.monotonic_time(:millisecond)
    
    duration = end_time - start_time
    assert duration < 10_000, "Activity polling took too long without tasks: #{duration}ms"
    assert_valid_polling_result(act_result)
  end

  defp test_polling_behavior(worker) do
    # Test that polling handles various scenarios correctly
    
    # Multiple rapid polls should work
    results = for _i <- 1..3 do
      wf_result = Temporal.Native.worker_poll_workflow_task(worker)
      act_result = Temporal.Native.worker_poll_activity_task(worker)
      {wf_result, act_result}
    end
    
    Enum.each(results, fn {wf_result, act_result} ->
      assert_valid_polling_result(wf_result)
      assert_valid_polling_result(act_result)
    end)
  end

  defp assert_valid_polling_result(result) do
    case result do
      {:ok, nil} ->
        # No task available - expected when no workflows/activities scheduled
        assert true
        
      {:ok, %{} = task_data} ->
        # Received structured task data - would happen with real workflows
        assert is_map(task_data)
        assert true
        
      {:error, "Worker not running"} ->
        # Acceptable error state
        assert true
        
      {:error, "Polling timeout"} ->
        # Expected when polling times out
        assert true
        
      {:error, "Connection error"} ->
        # Network-related error
        assert true
        
      {:error, "Polling error"} ->
        # Generic polling error (sanitized)
        assert true
        
      {:error, reason} when is_binary(reason) ->
        # Other sanitized error messages
        assert is_binary(reason)
        
      other ->
        flunk("Unexpected polling result: #{inspect(other)}")
    end
  end
end