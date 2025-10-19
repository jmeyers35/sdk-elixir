# Signal and Query Example
#
# This example demonstrates how to:
# 1. Start a workflow execution
# 2. Send signals to the workflow
# 3. Query the workflow for its current state
#
# Usage:
#   mix run examples/signal_and_query_example.exs
#
# Prerequisites:
#   - Temporal server running on localhost:7233
#   - A worker implementation that handles the signals and queries
#   - The workflow should support "update_status" signal and "get_status" query

alias Temporal.Client

# Configuration for connecting to Temporal
config = [
  host: "localhost:7233",
  namespace: "default"
]

IO.puts("🚀 Starting Signal and Query Example")
IO.puts("====================================")

# Start the client GenServer
IO.puts("1. Starting Temporal client...")
{:ok, client} = Client.start_link(config)

# Connect to the server
IO.puts("2. Connecting to Temporal server...")
case Client.connect(client) do
  :ok -> 
    IO.puts("   ✅ Connected successfully!")
  {:error, reason} -> 
    IO.puts("   ❌ Connection failed: #{reason}")
    System.halt(1)
end

# Generate unique workflow ID
workflow_id = "signal-query-demo-#{System.unique_integer([:positive])}"
IO.puts("3. Generated workflow ID: #{workflow_id}")

# Start a workflow
IO.puts("4. Starting workflow execution...")
workflow_params = %{
  workflow_type: "OrderProcessingWorkflow",
  task_queue: "order-processing", 
  workflow_id: workflow_id,
  input: %{
    order_id: 12345,
    customer_id: "cust-567",
    items: [
      %{product: "laptop", quantity: 1, price: 999.99},
      %{product: "mouse", quantity: 2, price: 29.99}
    ]
  }
}

case Client.start_workflow(client, workflow_params) do
  {:ok, handle} ->
    IO.puts("   ✅ Workflow started successfully!")
    IO.puts("   📋 Handle: #{inspect(handle)}")
    run_id = handle["run_id"]
    
    # Wait a moment for workflow to initialize
    IO.puts("5. Waiting for workflow to initialize...")
    Process.sleep(1000)
    
    # Send a signal to update order status
    IO.puts("6. Sending signal to update order status...")
    signal_params = %{
      workflow_id: workflow_id,
      run_id: run_id,  # Optional - can specify specific run
      signal_name: "update_status",
      input: %{
        status: "processing",
        timestamp: DateTime.utc_now() |> DateTime.to_iso8601(),
        message: "Order is being processed"
      }
    }
    
    case Client.signal_workflow(client, signal_params) do
      :ok ->
        IO.puts("   ✅ Signal sent successfully!")
      {:error, reason} ->
        IO.puts("   ⚠️  Signal failed: #{reason}")
        IO.puts("      This is expected if no worker is running")
    end
    
    # Query the workflow for current status
    IO.puts("7. Querying workflow for current status...")
    query_params = %{
      workflow_id: workflow_id,
      run_id: run_id,  # Optional - can specify specific run
      query_type: "get_status",
      input: %{
        include_details: true
      }
    }
    
    case Client.query_workflow(client, query_params) do
      {:ok, result} ->
        IO.puts("   ✅ Query successful!")
        IO.puts("   📊 Result: #{inspect(result)}")
      {:error, reason} ->
        IO.puts("   ⚠️  Query failed: #{reason}")
        IO.puts("      This is expected if no worker is running")
    end
    
    # Send another signal to complete the order
    IO.puts("8. Sending completion signal...")
    completion_signal_params = %{
      workflow_id: workflow_id,
      signal_name: "complete_order", 
      input: %{
        status: "completed",
        completion_time: DateTime.utc_now() |> DateTime.to_iso8601(),
        total_amount: 1059.97
      }
    }
    
    case Client.signal_workflow(client, completion_signal_params) do
      :ok ->
        IO.puts("   ✅ Completion signal sent!")
      {:error, reason} ->
        IO.puts("   ⚠️  Completion signal failed: #{reason}")
    end
    
    # Query again for final status
    IO.puts("9. Final status query...")
    final_query_params = %{
      workflow_id: workflow_id,
      query_type: "get_order_summary"
    }
    
    case Client.query_workflow(client, final_query_params) do
      {:ok, summary} ->
        IO.puts("   ✅ Final query successful!")
        IO.puts("   📋 Order Summary: #{inspect(summary)}")
      {:error, reason} ->
        IO.puts("   ⚠️  Final query failed: #{reason}")
    end

  {:error, reason} ->
    IO.puts("   ❌ Workflow start failed: #{reason}")
    IO.puts("      Make sure Temporal server is running on localhost:7233")
end

# Demonstrate signal/query with different input types
IO.puts("\n🔄 Advanced Examples")
IO.puts("===================")

# Example with complex nested data
complex_workflow_id = "complex-demo-#{System.unique_integer([:positive])}"

IO.puts("10. Starting workflow with complex data...")
complex_params = %{
  workflow_type: "DataProcessingWorkflow",
  task_queue: "data-processing",
  workflow_id: complex_workflow_id,
  input: %{
    dataset: %{
      name: "user_analytics",
      format: "json",
      size_mb: 150.5,
      columns: ["user_id", "event_type", "timestamp", "metadata"]
    },
    processing_config: %{
      batch_size: 1000,
      parallel_workers: 4,
      timeout_seconds: 3600,
      retry_attempts: 3
    }
  }
}

case Client.start_workflow(client, complex_params) do
  {:ok, _handle} ->
    IO.puts("   ✅ Complex workflow started!")
    
    # Send configuration update signal
    IO.puts("11. Updating processing configuration...")
    config_signal = %{
      workflow_id: complex_workflow_id,
      signal_name: "update_config",
      input: %{
        config_changes: %{
          batch_size: 2000,
          parallel_workers: 6
        },
        reason: "Performance optimization"
      }
    }
    
    case Client.signal_workflow(client, config_signal) do
      :ok -> IO.puts("   ✅ Configuration update signal sent!")
      {:error, reason} -> IO.puts("   ⚠️  Config signal failed: #{reason}")
    end
    
    # Query processing metrics
    IO.puts("12. Querying processing metrics...")
    metrics_query = %{
      workflow_id: complex_workflow_id,
      query_type: "get_metrics",
      input: %{
        metric_types: ["throughput", "error_rate", "memory_usage"],
        time_range: "last_hour"
      }
    }
    
    case Client.query_workflow(client, metrics_query) do
      {:ok, metrics} ->
        IO.puts("   ✅ Metrics query successful!")
        IO.puts("   📈 Metrics: #{inspect(metrics)}")
      {:error, reason} ->
        IO.puts("   ⚠️  Metrics query failed: #{reason}")
    end

  {:error, reason} ->
    IO.puts("   ❌ Complex workflow failed: #{reason}")
end

# Clean up
IO.puts("\n🧹 Cleaning up...")
Client.stop(client)
IO.puts("   ✅ Client stopped")

IO.puts("\n🎉 Example completed!")
IO.puts("================")
IO.puts("")
IO.puts("📝 Notes:")
IO.puts("   - Signals and queries will fail if no worker is running")
IO.puts("   - This is expected behavior when testing without a full Temporal setup")
IO.puts("   - The client successfully communicates with the Temporal server")
IO.puts("   - Check the Temporal Web UI at http://localhost:8233 to see workflow executions")
IO.puts("")
IO.puts("🔗 Next steps:")
IO.puts("   1. Implement a worker that handles these workflow types")
IO.puts("   2. Add signal handlers for 'update_status', 'complete_order', 'update_config'")
IO.puts("   3. Add query handlers for 'get_status', 'get_order_summary', 'get_metrics'")
IO.puts("   4. Run this example with workers active to see full functionality")