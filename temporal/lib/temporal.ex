defmodule Temporal do
  @moduledoc """
  Elixir SDK for Temporal workflow orchestration.
  
  Temporal is a workflow orchestration platform that makes it easy to build reliable,
  scalable applications. This SDK provides Elixir bindings for interacting with
  Temporal services.
  
  ## Getting Started
  
  The main entry point for most applications is the `Temporal.Client` module:
  
      # Start a client (usually under a supervisor)
      {:ok, client} = Temporal.Client.start_link(
        target_url: "localhost:7233",
        namespace: "default"
      )
      
      # Start a workflow
      {:ok, handle} = Temporal.Client.start_workflow(client, %{
        workflow_type: "MyWorkflow",
        task_queue: "my-task-queue",
        workflow_id: "unique-workflow-id"
      })
  
  ## Key Modules
  
  - `Temporal.Client` - High-level client for interacting with Temporal
  - `Temporal.Native` - Low-level NIF bindings (not intended for direct use)
  
  ## Configuration
  
  You can configure the client globally in your application config:
  
      config :temporal, Temporal.Client,
        target_url: "localhost:7233",
        namespace: "default"
  
  Or pass configuration directly when starting a client:
  
      {:ok, client} = Temporal.Client.start_link(
        target_url: "temporal.example.com:7233",
        namespace: "production",
        tls: %{
          client_cert_path: "/path/to/cert.pem",
          client_key_path: "/path/to/key.pem"
        }
      )
  """
end
