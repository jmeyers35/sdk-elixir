# Temporal Elixir SDK - Rustler NIF Implementation Specification

## Overview

This specification describes the architecture and implementation details for a Temporal SDK for Elixir using Rustler NIFs to integrate with the Temporal SDK Core (written in Rust). This approach follows the pattern established by the Python (PyO3) and TypeScript (Neon/NAPI) SDKs.

## Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Elixir Application                     │
├─────────────────────────────────────────────────────────┤
│                   Temporal Elixir SDK                    │
│  ┌─────────────────────────────────────────────────┐    │
│  │  Elixir API Layer                               │    │
│  │  - Client, Worker, Workflow, Activity modules   │    │
│  └─────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────┐    │
│  │  GenServer/Supervisor Layer                     │    │
│  │  - Worker process management                    │    │
│  │  - Connection lifecycle                         │    │
│  └─────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────┐    │
│  │  Rustler NIF Bridge                             │    │
│  │  - Safe Rust wrappers around SDK Core          │    │
│  │  - BEAM resource management                     │    │
│  └─────────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────────┤
│              Temporal SDK Core (Rust)                    │
│  - State machines, gRPC client, event processing        │
└─────────────────────────────────────────────────────────┘
                            │ gRPC
                            ▼
                  ┌─────────────────────┐
                  │  Temporal Service   │
                  └─────────────────────┘
```

### Component Responsibilities

#### Elixir API Layer
- Provides idiomatic Elixir interfaces
- Handles Elixir-specific patterns (GenServer, Supervisors)
- Manages process lifecycle and error handling
- Implements OTP behaviors

#### Rustler NIF Bridge
- Wraps SDK Core functionality in safe Rust code
- Manages memory and resources using BEAM allocators
- Handles async Rust operations with dirty schedulers
- Converts between Elixir and Rust data types

#### SDK Core Integration
- Leverages existing Temporal SDK Core
- Handles all Temporal protocol complexity
- Manages workflow state machines
- Communicates with Temporal service

## Detailed Design

### Project Structure

```
temporal_elixir/
├── lib/
│   ├── temporal/
│   │   ├── client.ex
│   │   ├── worker.ex
│   │   ├── workflow.ex
│   │   ├── activity.ex
│   │   ├── converter.ex
│   │   └── native.ex        # NIF module
│   └── temporal.ex
├── native/temporal_nif/     # Rust crate
│   ├── Cargo.toml
│   ├── src/
│   │   ├── lib.rs
│   │   ├── client.rs
│   │   ├── worker.rs
│   │   ├── workflow.rs
│   │   ├── activity.rs
│   │   └── resources.rs
├── priv/                    # Compiled NIFs
├── test/
└── mix.exs
```

### Rust NIF Implementation

#### Cargo.toml
```toml
[package]
name = "temporal_nif"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
rustler = "0.34"
temporal-sdk-core = "0.1"
temporal-sdk-core-protos = "0.1"
tokio = { version = "1", features = ["full"] }
prost = "0.13"
anyhow = "1.0"
parking_lot = "0.12"

[features]
default = ["rustler/derive"]
```

#### Core NIF Module (lib.rs)
```rust
use rustler::{Encoder, Env, Error, ResourceArc, Term};
use temporal_sdk_core::{
    Client as CoreClient, Worker as CoreWorker, 
    WorkerConfig, ClientConfig
};
use std::sync::Arc;
use tokio::runtime::Runtime;

// Resource types for BEAM garbage collection
struct ClientResource {
    client: Arc<CoreClient>,
    runtime: Arc<Runtime>,
}

struct WorkerResource {
    worker: Arc<CoreWorker>,
    runtime: Arc<Runtime>,
}

// Resource implementations
impl rustler::Resource for ClientResource {}
impl rustler::Resource for WorkerResource {}

// Module initialization
rustler::init!(
    "Elixir.Temporal.Native",
    [
        // Client functions
        client_connect,
        client_start_workflow,
        client_signal_workflow,
        client_query_workflow,
        client_describe_workflow,
        
        // Worker functions
        worker_new,
        worker_poll_workflow_task,
        worker_poll_activity_task,
        worker_complete_workflow_task,
        worker_complete_activity_task,
        worker_shutdown,
        
        // Utility functions
        get_runtime_info,
    ],
    load = on_load
);

fn on_load(env: Env) -> bool {
    // Register resource types
    rustler::resource!(ClientResource, env);
    rustler::resource!(WorkerResource, env);
    true
}
```

#### Client Implementation (client.rs)
```rust
use rustler::{Env, Error, Term, ResourceArc};
use temporal_sdk_core::{Client, ClientConfig};
use tokio::runtime::Runtime;

#[rustler::nif(schedule = "DirtyCpu")]
fn client_connect<'a>(
    env: Env<'a>,
    config: ClientConfigTerm<'a>
) -> Result<Term<'a>, Error> {
    // Create Tokio runtime for async operations
    let runtime = Runtime::new()
        .map_err(|e| Error::Term(Box::new(e.to_string())))?;
    
    let config: ClientConfig = decode_client_config(config)?;
    
    // Connect to Temporal service
    let client = runtime.block_on(async {
        Client::connect(config).await
    }).map_err(|e| Error::Term(Box::new(e.to_string())))?;
    
    let resource = ResourceArc::new(ClientResource {
        client: Arc::new(client),
        runtime: Arc::new(runtime),
    });
    
    Ok((atoms::ok(), resource).encode(env))
}

#[rustler::nif(schedule = "DirtyIo")]
fn client_start_workflow<'a>(
    env: Env<'a>,
    client: ResourceArc<ClientResource>,
    workflow_id: String,
    workflow_type: String,
    task_queue: String,
    input: Term<'a>,
) -> Result<Term<'a>, Error> {
    let input_payload = encode_payload(env, input)?;
    
    let handle = client.runtime.block_on(async {
        client.client
            .start_workflow(
                workflow_id,
                workflow_type,
                task_queue,
                input_payload,
            )
            .await
    }).map_err(|e| Error::Term(Box::new(e.to_string())))?;
    
    Ok((atoms::ok(), handle.run_id()).encode(env))
}
```

#### Worker Implementation (worker.rs)
```rust
use rustler::{Env, Error, Term, ResourceArc, ThreadSpawner};
use temporal_sdk_core::{Worker, WorkerConfig, WorkflowActivation};
use std::sync::mpsc;

#[rustler::nif]
fn worker_new<'a>(
    env: Env<'a>,
    client: ResourceArc<ClientResource>,
    config: WorkerConfigTerm<'a>,
) -> Result<Term<'a>, Error> {
    let config: WorkerConfig = decode_worker_config(config)?;
    
    let worker = Worker::new(
        client.client.clone(),
        config,
    );
    
    let resource = ResourceArc::new(WorkerResource {
        worker: Arc::new(worker),
        runtime: client.runtime.clone(),
    });
    
    Ok((atoms::ok(), resource).encode(env))
}

#[rustler::nif(schedule = "DirtyIo")]
fn worker_poll_workflow_task<'a>(
    env: Env<'a>,
    worker: ResourceArc<WorkerResource>,
) -> Result<Term<'a>, Error> {
    // Poll for workflow tasks
    let activation = worker.runtime.block_on(async {
        worker.worker.poll_workflow_activation().await
    }).map_err(|e| Error::Term(Box::new(e.to_string())))?;
    
    // Convert activation to Elixir term
    let activation_term = encode_workflow_activation(env, activation)?;
    
    Ok((atoms::ok(), activation_term).encode(env))
}

#[rustler::nif(schedule = "DirtyIo")]
fn worker_complete_workflow_task<'a>(
    env: Env<'a>,
    worker: ResourceArc<WorkerResource>,
    completion: Term<'a>,
) -> Result<Term<'a>, Error> {
    let completion = decode_workflow_completion(completion)?;
    
    worker.runtime.block_on(async {
        worker.worker.complete_workflow_activation(completion).await
    }).map_err(|e| Error::Term(Box::new(e.to_string())))?;
    
    Ok(atoms::ok().encode(env))
}
```

### Elixir Implementation

#### Native Module (lib/temporal/native.ex)
```elixir
defmodule Temporal.Native do
  @moduledoc false
  use Rustler, otp_app: :temporal, crate: "temporal_nif"

  # Client NIFs
  def client_connect(_config), do: :erlang.nif_error(:nif_not_loaded)
  def client_start_workflow(_client, _workflow_id, _workflow_type, _task_queue, _input), 
    do: :erlang.nif_error(:nif_not_loaded)
  def client_signal_workflow(_client, _workflow_id, _signal_name, _input),
    do: :erlang.nif_error(:nif_not_loaded)
  def client_query_workflow(_client, _workflow_id, _query_type, _input),
    do: :erlang.nif_error(:nif_not_loaded)
    
  # Worker NIFs
  def worker_new(_client, _config), do: :erlang.nif_error(:nif_not_loaded)
  def worker_poll_workflow_task(_worker), do: :erlang.nif_error(:nif_not_loaded)
  def worker_poll_activity_task(_worker), do: :erlang.nif_error(:nif_not_loaded)
  def worker_complete_workflow_task(_worker, _completion), do: :erlang.nif_error(:nif_not_loaded)
  def worker_complete_activity_task(_worker, _completion), do: :erlang.nif_error(:nif_not_loaded)
  def worker_shutdown(_worker), do: :erlang.nif_error(:nif_not_loaded)
  
  # Utility NIFs
  def get_runtime_info(), do: :erlang.nif_error(:nif_not_loaded)
end
```

#### Client Module (lib/temporal/client.ex)
```elixir
defmodule Temporal.Client do
  @moduledoc """
  Temporal Client for interacting with the Temporal service.
  """
  
  use GenServer
  require Logger
  
  defstruct [:native_client, :config]
  
  @type t :: %__MODULE__{
    native_client: reference(),
    config: map()
  }
  
  @doc """
  Connect to Temporal service.
  """
  @spec connect(keyword()) :: {:ok, t()} | {:error, term()}
  def connect(opts \\ []) do
    config = build_config(opts)
    
    case Temporal.Native.client_connect(config) do
      {:ok, native_client} ->
        {:ok, %__MODULE__{native_client: native_client, config: config}}
        
      {:error, reason} ->
        {:error, reason}
    end
  end
  
  @doc """
  Start a workflow execution.
  """
  @spec start_workflow(t(), String.t(), module(), term(), keyword()) :: 
    {:ok, String.t()} | {:error, term()}
  def start_workflow(client, workflow_id, workflow_module, args, opts \\ []) do
    workflow_type = workflow_module.__workflow_type__()
    task_queue = Keyword.get(opts, :task_queue, client.config.task_queue)
    
    Temporal.Native.client_start_workflow(
      client.native_client,
      workflow_id,
      workflow_type,
      task_queue,
      args
    )
  end
  
  @doc """
  Signal a workflow execution.
  """
  @spec signal_workflow(t(), String.t(), String.t(), term()) :: 
    :ok | {:error, term()}
  def signal_workflow(client, workflow_id, signal_name, input) do
    case Temporal.Native.client_signal_workflow(
      client.native_client,
      workflow_id,
      signal_name,
      input
    ) do
      {:ok, _} -> :ok
      error -> error
    end
  end
  
  defp build_config(opts) do
    %{
      target_host: Keyword.get(opts, :host, "localhost:7233"),
      namespace: Keyword.get(opts, :namespace, "default"),
      task_queue: Keyword.get(opts, :task_queue, "default"),
      tls_config: build_tls_config(opts[:tls])
    }
  end
  
  defp build_tls_config(nil), do: nil
  defp build_tls_config(tls_opts) do
    %{
      server_root_ca_cert: tls_opts[:ca_cert],
      domain: tls_opts[:domain],
      client_cert: tls_opts[:client_cert],
      client_private_key: tls_opts[:client_key]
    }
  end
end
```

#### Worker Module (lib/temporal/worker.ex)
```elixir
defmodule Temporal.Worker do
  @moduledoc """
  Temporal Worker that executes workflows and activities.
  """
  
  use GenServer
  require Logger
  
  defstruct [
    :native_worker,
    :client,
    :task_queue,
    :workflows,
    :activities,
    :workflow_runner,
    :activity_runner
  ]
  
  @doc """
  Start a worker linked to the current process.
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end
  
  @impl true
  def init(opts) do
    client = Keyword.fetch!(opts, :client)
    task_queue = Keyword.fetch!(opts, :task_queue)
    workflows = Keyword.get(opts, :workflows, [])
    activities = Keyword.get(opts, :activities, [])
    
    config = %{
      task_queue: task_queue,
      workflows: Enum.map(workflows, & &1.__workflow_type__()),
      activities: Enum.map(activities, & &1.__activity_type__()),
      max_concurrent_activities: Keyword.get(opts, :max_concurrent_activities, 100),
      max_concurrent_workflows: Keyword.get(opts, :max_concurrent_workflows, 100)
    }
    
    case Temporal.Native.worker_new(client.native_client, config) do
      {:ok, native_worker} ->
        state = %__MODULE__{
          native_worker: native_worker,
          client: client,
          task_queue: task_queue,
          workflows: build_workflow_map(workflows),
          activities: build_activity_map(activities),
          workflow_runner: Temporal.Worker.WorkflowRunner,
          activity_runner: Temporal.Worker.ActivityRunner
        }
        
        # Start polling
        send(self(), :poll_workflow)
        send(self(), :poll_activity)
        
        {:ok, state}
        
      {:error, reason} ->
        {:stop, {:worker_init_failed, reason}}
    end
  end
  
  @impl true
  def handle_info(:poll_workflow, state) do
    # Poll for workflow tasks
    case Temporal.Native.worker_poll_workflow_task(state.native_worker) do
      {:ok, activation} ->
        # Process workflow activation
        Task.Supervisor.start_child(
          Temporal.TaskSupervisor,
          fn -> handle_workflow_activation(state, activation) end
        )
        
      {:error, :shutdown} ->
        {:stop, :normal, state}
        
      {:error, reason} ->
        Logger.error("Workflow poll error: #{inspect(reason)}")
        Process.sleep(1000)
    end
    
    # Continue polling
    send(self(), :poll_workflow)
    {:noreply, state}
  end
  
  @impl true
  def handle_info(:poll_activity, state) do
    # Poll for activity tasks
    case Temporal.Native.worker_poll_activity_task(state.native_worker) do
      {:ok, task} ->
        # Process activity task
        Task.Supervisor.start_child(
          Temporal.TaskSupervisor,
          fn -> handle_activity_task(state, task) end
        )
        
      {:error, :shutdown} ->
        {:stop, :normal, state}
        
      {:error, reason} ->
        Logger.error("Activity poll error: #{inspect(reason)}")
        Process.sleep(1000)
    end
    
    # Continue polling
    send(self(), :poll_activity)
    {:noreply, state}
  end
  
  defp handle_workflow_activation(state, activation) do
    workflow_type = activation.workflow_type
    workflow_module = Map.get(state.workflows, workflow_type)
    
    if workflow_module do
      completion = state.workflow_runner.run(workflow_module, activation)
      
      case Temporal.Native.worker_complete_workflow_task(
        state.native_worker, 
        completion
      ) do
        :ok -> :ok
        {:error, reason} -> 
          Logger.error("Failed to complete workflow task: #{inspect(reason)}")
      end
    else
      Logger.error("Unknown workflow type: #{workflow_type}")
    end
  end
  
  defp handle_activity_task(state, task) do
    activity_type = task.activity_type
    activity_module = Map.get(state.activities, activity_type)
    
    if activity_module do
      completion = state.activity_runner.run(activity_module, task)
      
      case Temporal.Native.worker_complete_activity_task(
        state.native_worker,
        completion
      ) do
        :ok -> :ok
        {:error, reason} ->
          Logger.error("Failed to complete activity task: #{inspect(reason)}")
      end
    else
      Logger.error("Unknown activity type: #{activity_type}")
    end
  end
  
  defp build_workflow_map(workflows) do
    Map.new(workflows, fn module ->
      {module.__workflow_type__(), module}
    end)
  end
  
  defp build_activity_map(activities) do
    Map.new(activities, fn module ->
      {module.__activity_type__(), module}
    end)
  end
end
```

#### Workflow Definition Module (lib/temporal/workflow.ex)
```elixir
defmodule Temporal.Workflow do
  @moduledoc """
  Behaviour and macros for defining Temporal workflows.
  """
  
  @callback execute(args :: term()) :: {:ok, term()} | {:error, term()}
  
  defmacro __using__(opts) do
    quote do
      @behaviour Temporal.Workflow
      
      @workflow_type unquote(opts[:type]) || __MODULE__ |> to_string() |> String.split(".") |> List.last()
      
      def __workflow_type__, do: @workflow_type
      
      # Import workflow-safe functions
      import Temporal.Workflow.Functions
    end
  end
end
```

### Safety and Error Handling

#### NIF Panic Handling
```rust
use std::panic;

fn setup_panic_handler() {
    panic::set_hook(Box::new(|panic_info| {
        // Log panic information
        eprintln!("NIF panic: {:?}", panic_info);
        
        // Attempt graceful cleanup
        // Note: This is last resort - prefer Result types
    }));
}
```

#### Resource Cleanup
```rust
impl Drop for WorkerResource {
    fn drop(&mut self) {
        // Ensure worker is properly shut down
        let worker = self.worker.clone();
        let runtime = self.runtime.clone();
        
        std::thread::spawn(move || {
            runtime.block_on(async {
                let _ = worker.shutdown().await;
            });
        });
    }
}
```

#### Dirty Scheduler Usage
- All blocking operations use `DirtyIo` or `DirtyCpu` schedulers
- Non-blocking operations can run on normal schedulers
- Long-running operations should yield periodically

### Testing Strategy

#### Unit Tests
```elixir
defmodule Temporal.ClientTest do
  use ExUnit.Case
  
  test "connects to local server" do
    assert {:ok, client} = Temporal.Client.connect(
      host: "localhost:7233",
      namespace: "default"
    )
  end
  
  test "starts workflow" do
    {:ok, client} = Temporal.Client.connect()
    
    assert {:ok, run_id} = Temporal.Client.start_workflow(
      client,
      "test-workflow-#{System.unique_integer()}",
      TestWorkflow,
      %{name: "test"}
    )
    
    assert is_binary(run_id)
  end
end
```

#### Integration Tests
```elixir
defmodule Temporal.IntegrationTest do
  use Temporal.Testing.WorkflowEnvironment
  
  test "executes workflow end-to-end" do
    {:ok, worker} = start_worker([TestWorkflow], [TestActivity])
    
    {:ok, handle} = execute_workflow(TestWorkflow, %{input: "test"})
    
    assert {:ok, "processed: test"} = await_workflow_result(handle)
  end
end
```

### Performance Considerations

#### Connection Pooling
```elixir
defmodule Temporal.ConnectionPool do
  use GenServer
  
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end
  
  def get_connection do
    GenServer.call(__MODULE__, :get_connection)
  end
  
  # Implementation details...
end
```

#### Batch Operations
```rust
#[rustler::nif(schedule = "DirtyIo")]
fn worker_poll_workflow_tasks_batch<'a>(
    env: Env<'a>,
    worker: ResourceArc<WorkerResource>,
    count: usize,
) -> Result<Term<'a>, Error> {
    // Poll multiple tasks in one call
    let mut activations = Vec::with_capacity(count);
    
    for _ in 0..count {
        match worker.runtime.block_on(worker.worker.poll_workflow_activation()) {
            Ok(activation) => activations.push(activation),
            Err(_) => break,
        }
    }
    
    Ok(encode_activations(env, activations))
}
```

### Deployment

#### Mix Configuration
```elixir
# mix.exs
defmodule Temporal.MixProject do
  use Mix.Project
  
  def project do
    [
      app: :temporal,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      compilers: [:rustler] ++ Mix.compilers(),
      rustler_crates: [
        temporal_nif: [
          mode: rustc_mode(Mix.env()),
          features: features()
        ]
      ],
      deps: deps()
    ]
  end
  
  defp rustc_mode(:prod), do: :release
  defp rustc_mode(_), do: :debug
  
  defp features do
    if System.get_env("TEMPORAL_SDK_TELEMETRY") == "true" do
      ["telemetry"]
    else
      []
    end
  end
  
  defp deps do
    [
      {:rustler, "~> 0.34"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.2"},
      {:ex_doc, "~> 0.31", only: :dev}
    ]
  end
end
```

#### Release Configuration
```elixir
# config/runtime.exs
import Config

config :temporal,
  host: System.get_env("TEMPORAL_HOST", "localhost:7233"),
  namespace: System.get_env("TEMPORAL_NAMESPACE", "default"),
  task_queue: System.get_env("TEMPORAL_TASK_QUEUE", "default"),
  tls: [
    ca_cert: System.get_env("TEMPORAL_TLS_CA"),
    client_cert: System.get_env("TEMPORAL_TLS_CERT"),
    client_key: System.get_env("TEMPORAL_TLS_KEY")
  ]
```

### Monitoring and Observability

#### Telemetry Integration
```elixir
defmodule Temporal.Telemetry do
  def setup do
    events = [
      [:temporal, :client, :request, :start],
      [:temporal, :client, :request, :stop],
      [:temporal, :worker, :workflow, :start],
      [:temporal, :worker, :workflow, :stop],
      [:temporal, :worker, :activity, :start],
      [:temporal, :worker, :activity, :stop]
    ]
    
    :telemetry.attach_many(
      "temporal-metrics",
      events,
      &handle_event/4,
      nil
    )
  end
  
  defp handle_event(event, measurements, metadata, _config) do
    # Handle telemetry events
  end
end
```

## Migration Path

### Phase 1: Core Implementation
1. Implement basic Client and Worker NIFs
2. Support simple workflow execution
3. Add activity support
4. Implement error handling

### Phase 2: Feature Parity
1. Add query and signal support
2. Implement workflow timers and child workflows
3. Add retry policies and timeouts
4. Support custom data converters

### Phase 3: Elixir-Specific Features
1. OTP integration (supervisors, gen_servers)
2. LiveView integration for workflow monitoring
3. Ecto integration for activity implementations
4. Phoenix integration for webhooks

### Phase 4: Production Readiness
1. Performance optimization
2. Comprehensive testing
3. Documentation
4. Example applications

## Conclusion

This Rustler NIF approach provides:
- **Performance**: Direct native calls without IPC overhead
- **Safety**: Rust's memory safety with BEAM's fault tolerance
- **Compatibility**: Follows patterns from Python/TypeScript SDKs
- **Idiomatic**: Leverages Elixir/OTP patterns

The implementation leverages the battle-tested SDK Core while providing an Elixir-friendly API that integrates well with the BEAM ecosystem.
