#!/usr/bin/env elixir

# Protobuf Workflow Example
# 
# This example demonstrates launching a Temporal workflow with protobuf messages
# as input parameters using the Temporal Elixir SDK.
#
# Run with: mix run examples/protobuf_workflow_example.exs

defmodule ProtobufWorkflowExample do
  @moduledoc """
  Example demonstrating how to use protobuf messages as workflow input/output.
  """

  # Define a simple protobuf message for workflow input
  defmodule WorkflowRequest do
    use Protobuf, syntax: :proto3
    
    field :request_id, 1, type: :string
    field :user_id, 2, type: :int64
    field :operation, 3, type: :string
    field :data, 4, type: :string
    field :timestamp, 5, type: :int64

    def new(attrs \\ %{}) do
      struct(__MODULE__, attrs)
    end
  end

  alias Temporal.Client

  def run do
    IO.puts("🚀 Protobuf Workflow Example")
    IO.puts("=" |> String.duplicate(50))
    
    # Start Temporal client
    case start_client() do
      {:ok, client_pid} ->
        # Create protobuf message as workflow input
        request = WorkflowRequest.new(
          request_id: "req-#{System.unique_integer([:positive])}",
          user_id: 12345,
          operation: "process_order",
          data: Jason.encode!(%{items: ["item1", "item2"], total: 99.99}),
          timestamp: System.system_time(:second)
        )
        
        IO.puts("\n📦 Created protobuf request:")
        IO.puts("  Request ID: #{request.request_id}")
        IO.puts("  User ID: #{request.user_id}")
        IO.puts("  Operation: #{request.operation}")
        IO.puts("  Timestamp: #{request.timestamp}")
        
        # Launch workflow with protobuf input
        workflow_id = "protobuf-workflow-#{System.unique_integer([:positive])}"
        
        IO.puts("\n🔄 Starting workflow with ID: #{workflow_id}")
        
        case Client.start_workflow(client_pid, %{
          workflow_type: "ProtobufDemoWorkflow",
          task_queue: "protobuf-demo-queue",
          workflow_id: workflow_id,
          input: [request]  # Pass protobuf message as input
        }) do
          {:ok, handle} ->
            IO.puts("✅ Workflow started successfully!")
            IO.puts("   Run ID: #{handle["run_id"]}")
            IO.puts("   Workflow ID: #{handle["workflow_id"]}")
            
            # The workflow would process the protobuf message
            IO.puts("\n📊 Workflow will receive the protobuf message and can:")
            IO.puts("   - Decode the protobuf data")
            IO.puts("   - Process the operation")
            IO.puts("   - Return results (also as protobuf if desired)")
            
          {:error, reason} ->
            IO.puts("❌ Failed to start workflow: #{inspect(reason)}")
        end
        
        # Stop the client
        GenServer.stop(client_pid)
        
      {:error, reason} ->
        IO.puts("❌ Failed to start Temporal client: #{inspect(reason)}")
        IO.puts("\n💡 Make sure Temporal server is running:")
        IO.puts("   docker run --rm -p 7233:7233 temporalio/auto-setup:latest")
    end
  end

  defp start_client do
    # Start with default config (localhost:7233)
    config = %{
      target_url: System.get_env("TEMPORAL_URL", "http://localhost:7233"),
      namespace: System.get_env("TEMPORAL_NAMESPACE", "default")
    }
    
    IO.puts("\n🔌 Connecting to Temporal at #{config.target_url}")
    
    case Client.start_link(config: config) do
      {:ok, pid} ->
        # Give the client a moment to establish connection
        Process.sleep(100)
        {:ok, pid}
        
      error ->
        error
    end
  end
end

# Main execution
try do
  ProtobufWorkflowExample.run()
rescue
  error ->
    IO.puts("\n❌ Example failed: #{inspect(error)}")
    IO.puts("\n🔍 Stack trace:")
    IO.puts(Exception.format(:error, error, __STACKTRACE__))
    
    case error do
      %RuntimeError{message: msg} ->
        if String.contains?(msg, "Connection refused") do
          IO.puts("\n💡 Make sure Temporal server is running:")
          IO.puts("   docker run --rm -p 7233:7233 temporalio/auto-setup:latest")
        end
        
      _ ->
        :ok
    end
end