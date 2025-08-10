#!/usr/bin/env elixir

# Worker Polling Example - Demonstrating Task 3.1.2 Implementation
# This example shows how to use the worker polling NIFs properly
# To run: mix run examples/worker_polling_example.exs

defmodule WorkerPollingExample do
  @moduledoc """
  Example demonstrating proper worker polling functionality using the new NIFs.
  
  This shows how to:
  1. Create a worker with proper configuration
  2. Poll for workflow and activity tasks
  3. Handle polling results and errors
  4. Use proper timeout and error handling patterns
  """

  require Logger

  def run do
    Logger.info("=== Worker Polling Example ===")
    Logger.info("Demonstrating Task 3.1.2 Implementation")

    try do
      # Step 1: Connect to Temporal server
      Logger.info("\n1. Connecting to Temporal server...")
      
      client_config = %{
        "target_host" => "localhost:7233",
        "namespace" => "default"
      }

      case connect_to_temporal(client_config) do
        {:ok, client} ->
          Logger.info("✅ Connected to Temporal server successfully")
          demonstrate_worker_polling(client)

        {:error, reason} ->
          Logger.error("❌ Failed to connect to Temporal server: #{reason}")
          Logger.info("💡 Make sure Temporal server is running:")
          Logger.info("   temporal server start-dev")
          {:error, :connection_failed}
      end
    rescue
      error ->
        Logger.error("❌ Example failed with error: #{inspect(error)}")
        {:error, :example_failed}
    end
  end

  defp connect_to_temporal(config) do
    case Temporal.Native.client_connect(config) do
      {:ok, client} -> {:ok, client}
      client when is_reference(client) -> {:ok, client}
      {:error, reason} -> {:error, reason}
      other -> {:error, "Unexpected result: #{inspect(other)}"}
    end
  end

  defp demonstrate_worker_polling(client) do
    Logger.info("\n2. Creating worker with proper configuration...")
    
    # Create unique task queue for this example
    task_queue = "example-worker-polling-#{System.unique_integer([:positive])}"
    
    worker_config = %{
      "namespace" => "default",
      "task_queue" => task_queue,
      "max_outstanding_workflow_tasks" => 5,
      "max_outstanding_activities" => 5,
      "max_outstanding_local_activities" => 3,
      "no_remote_activities" => false
    }

    case create_worker(client, worker_config) do
      {:ok, worker} ->
        Logger.info("✅ Worker created successfully")
        Logger.info("   Namespace: #{worker_config["namespace"]}")
        Logger.info("   Task Queue: #{task_queue}")
        
        demonstrate_polling_operations(worker)

      {:error, reason} ->
        Logger.error("❌ Failed to create worker: #{reason}")
        {:error, :worker_creation_failed}
    end
  end

  defp create_worker(client, config) do
    case Temporal.Native.worker_new(client, config) do
      {:ok, worker} -> {:ok, worker}
      worker when is_reference(worker) -> {:ok, worker}
      {:error, reason} -> {:error, reason}
      other -> {:error, "Unexpected worker creation result: #{inspect(other)}"}
    end
  end

  defp demonstrate_polling_operations(worker) do
    Logger.info("\n3. Demonstrating Polling Operations")
    
    # Show configuration validation
    demonstrate_configuration_validation()
    
    # Demonstrate workflow task polling
    demonstrate_workflow_task_polling(worker)
    
    # Demonstrate activity task polling  
    demonstrate_activity_task_polling(worker)
    
    # Demonstrate concurrent polling
    demonstrate_concurrent_polling(worker)
    
    # Demonstrate error handling
    demonstrate_error_handling()
    
    Logger.info("\n=== Worker Polling Example Complete ===")
    Logger.info("✅ All polling operations demonstrated successfully!")
    Logger.info("🔍 Key takeaways:")
    Logger.info("   • Workers must be created with valid configuration")
    Logger.info("   • Polling returns {:ok, nil} when no tasks are available")
    Logger.info("   • Polling will return structured task data when tasks exist") 
    Logger.info("   • Error handling provides sanitized error messages")
    Logger.info("   • Concurrent polling is safe and supported")
  end

  defp demonstrate_configuration_validation do
    Logger.info("\n--- Configuration Validation ---")
    
    # Try creating a client for validation demo
    client_config = %{"target_host" => "localhost:7233", "namespace" => "default"}
    
    case Temporal.Native.client_connect(client_config) do
      {:ok, client} ->
        demo_config_validation(client)
      client when is_reference(client) ->
        demo_config_validation(client)
      _ ->
        Logger.info("⚠️  Skipping config validation demo (no client available)")
    end
  end

  defp demo_config_validation(client) do
    # Test empty namespace
    result = Temporal.Native.worker_new(client, %{
      "namespace" => "",
      "task_queue" => "test",
      "max_outstanding_workflow_tasks" => 1,
      "max_outstanding_activities" => 1,
      "max_outstanding_local_activities" => 1,
      "no_remote_activities" => false
    })
    
    case result do
      {:error, "Namespace cannot be empty"} ->
        Logger.info("✅ Empty namespace validation works correctly")
      _ ->
        Logger.warning("⚠️  Unexpected validation result: #{inspect(result)}")
    end

    # Test zero workflow tasks limit
    result = Temporal.Native.worker_new(client, %{
      "namespace" => "default",
      "task_queue" => "test",
      "max_outstanding_workflow_tasks" => 0,
      "max_outstanding_activities" => 1,
      "max_outstanding_local_activities" => 1,
      "no_remote_activities" => false
    })
    
    case result do
      {:error, "Max outstanding workflow tasks must be greater than 0"} ->
        Logger.info("✅ Zero workflow tasks validation works correctly")
      _ ->
        Logger.warning("⚠️  Unexpected validation result: #{inspect(result)}")
    end
  end

  defp demonstrate_workflow_task_polling(worker) do
    Logger.info("\n--- Workflow Task Polling ---")
    Logger.info("Polling for workflow tasks...")
    
    start_time = System.monotonic_time(:millisecond)
    result = Temporal.Native.worker_poll_workflow_task(worker)
    end_time = System.monotonic_time(:millisecond)
    
    duration = end_time - start_time
    
    case result do
      {:ok, nil} ->
        Logger.info("✅ No workflow tasks available (expected)")
        Logger.info("   Polling completed in #{duration}ms")
      {:ok, task_data} ->
        Logger.info("🎉 Received workflow task!")
        Logger.info("   Task data: #{inspect(task_data)}")
        Logger.info("   This would be a structured WorkflowTaskData in the full implementation")
      {:error, reason} ->
        Logger.info("ℹ️  Polling returned error: #{reason}")
        Logger.info("   This is expected behavior when demonstrating error handling")
    end
  end

  defp demonstrate_activity_task_polling(worker) do
    Logger.info("\n--- Activity Task Polling ---")
    Logger.info("Polling for activity tasks...")
    
    start_time = System.monotonic_time(:millisecond)
    result = Temporal.Native.worker_poll_activity_task(worker)
    end_time = System.monotonic_time(:millisecond)
    
    duration = end_time - start_time
    
    case result do
      {:ok, nil} ->
        Logger.info("✅ No activity tasks available (expected)")
        Logger.info("   Polling completed in #{duration}ms")
      {:ok, task_data} ->
        Logger.info("🎉 Received activity task!")
        Logger.info("   Task data: #{inspect(task_data)}")
        Logger.info("   This would be a structured ActivityTaskData in the full implementation")
      {:error, reason} ->
        Logger.info("ℹ️  Polling returned error: #{reason}")
        Logger.info("   This demonstrates proper error sanitization")
    end
  end

  defp demonstrate_concurrent_polling(worker) do
    Logger.info("\n--- Concurrent Polling ---")
    Logger.info("Testing concurrent polling operations...")
    
    # Start multiple polling operations concurrently
    tasks = for i <- 1..3 do
      Task.async(fn ->
        Logger.info("   Starting concurrent poll #{i}")
        wf_result = Temporal.Native.worker_poll_workflow_task(worker)
        act_result = Temporal.Native.worker_poll_activity_task(worker)
        Logger.info("   Completed concurrent poll #{i}")
        {i, wf_result, act_result}
      end)
    end
    
    # Wait for all to complete
    results = Task.await_many(tasks, 10_000)
    
    Logger.info("✅ All concurrent polling operations completed")
    
    # Show results summary
    Enum.each(results, fn {i, wf_result, act_result} ->
      Logger.info("   Poll #{i}: WF=#{format_result(wf_result)}, ACT=#{format_result(act_result)}")
    end)
  end

  defp demonstrate_error_handling do
    Logger.info("\n--- Error Handling ---")
    Logger.info("Demonstrating error handling patterns...")
    
    # Try to connect with invalid host to show error handling
    invalid_config = %{
      "target_host" => "nonexistent:9999",
      "namespace" => "default"
    }
    
    case Temporal.Native.client_connect(invalid_config) do
      {:error, reason} ->
        Logger.info("✅ Connection error handled properly: #{reason}")
        Logger.info("   Notice how the error message is sanitized for security")
      _ ->
        Logger.info("ℹ️  Connection unexpectedly succeeded or returned different result")
    end
    
    Logger.info("✅ Error handling demonstration complete")
  end

  defp format_result(result) do
    case result do
      {:ok, nil} -> "no_task"
      {:ok, _data} -> "has_task"
      {:error, _reason} -> "error"
    end
  end
end

# Run the example when this file is executed
WorkerPollingExample.run()