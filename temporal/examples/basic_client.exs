#!/usr/bin/env elixir

# Basic example of using Temporal.Client
# 
# This example demonstrates:
# - Starting a Temporal client
# - Connecting to a Temporal server
# - Starting a workflow
# - Handling errors

# Ensure we can find the temporal modules
Code.prepend_path("_build/dev/lib/temporal/ebin")

defmodule BasicClientExample do
  require Logger
  
  def run do
    Logger.info("Starting Temporal client example...")
    Logger.info("Make sure Temporal is running: docker-compose up -d")
    
    # Start the client
    {:ok, client} = Temporal.Client.start_link(
      target_url: "http://localhost:7233",
      namespace: "default"
    )
    
    Logger.info("Client started, current status: #{Temporal.Client.status(client)}")
    
    # Connect to the server
    case Temporal.Client.connect(client) do
      :ok ->
        Logger.info("Successfully connected to Temporal server!")
        
      {:error, reason} ->
        Logger.error("Failed to connect: #{inspect(reason)}")
        Logger.info("Make sure Temporal server is running on localhost:7233")
        System.halt(1)
    end
    
    # Start a workflow
    workflow_id = "example-workflow-#{System.unique_integer([:positive])}"
    
    Logger.info("Starting workflow with ID: #{workflow_id}")
    
    case Temporal.Client.start_workflow(client, %{
      workflow_type: "ExampleWorkflow",
      task_queue: "example-queue",
      workflow_id: workflow_id,
      input: %{
        message: "Hello from Elixir!",
        timestamp: DateTime.utc_now()
      }
    }) do
      {:ok, handle} ->
        Logger.info("Workflow started successfully!")
        Logger.info("  Workflow ID: #{handle["workflow_id"]}")
        Logger.info("  Run ID: #{handle["run_id"]}")
        
      {:error, reason} ->
        Logger.error("Failed to start workflow: #{inspect(reason)}")
        Logger.info("This might be because no worker is running for the task queue")
    end
    
    # Check final status
    Logger.info("Final client status: #{Temporal.Client.status(client)}")
    
    # Stop the client
    Temporal.Client.stop(client)
    Logger.info("Client stopped")
  end
end

# Configure logger
Application.put_env(:logger, :console, format: "$time $metadata[$level] $message\n")

# Run the example
BasicClientExample.run()