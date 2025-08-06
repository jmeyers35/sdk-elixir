defmodule Temporal.Client do
  @moduledoc """
  GenServer-based client for interacting with Temporal services.
  
  This module provides a high-level, idiomatic Elixir interface for:
  - Managing connections to Temporal servers
  - Starting workflow executions
  - Handling configuration and connection lifecycle
  - Integrating with OTP supervision trees
  
  ## Usage
  
      # Start a client (typically under a supervisor)
      {:ok, client} = Temporal.Client.start_link(
        target_url: "localhost:7233",
        namespace: "default"
      )
      
      # Start a workflow
      {:ok, handle} = Temporal.Client.start_workflow(client, %{
        workflow_type: "MyWorkflow",
        task_queue: "my-task-queue",
        workflow_id: "unique-id"
      })
  """
  
  use GenServer
  require Logger
  
  alias Temporal.Native
  
  # Client API
  
  @doc """
  Starts a new Temporal client process.
  
  ## Options
  
    * `:target_url` - The Temporal server URL (default: "localhost:7233")
    * `:namespace` - The Temporal namespace to use (default: "default")
    * `:name` - Optional name for the GenServer process
    * `:tls` - Optional TLS configuration map
    * `:connect_on_start` - Whether to connect immediately (default: false)
  
  ## Examples
  
      # Basic usage
      {:ok, client} = Temporal.Client.start_link()
      
      # With configuration
      {:ok, client} = Temporal.Client.start_link(
        target_url: "temporal.example.com:7233",
        namespace: "production",
        tls: %{
          client_cert_path: "/path/to/cert.pem",
          client_key_path: "/path/to/key.pem"
        }
      )
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name)
    
    case name do
      nil -> GenServer.start_link(__MODULE__, opts)
      _ -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end
  
  @doc """
  Establishes a connection to the Temporal server.
  
  This is called automatically on first operation if not already connected.
  """
  @spec connect(GenServer.server()) :: :ok | {:error, term()}
  def connect(client) do
    GenServer.call(client, :connect)
  end
  
  @doc """
  Starts a new workflow execution.
  
  ## Parameters
  
    * `client` - The client process
    * `params` - Workflow parameters map with required fields:
      * `:workflow_type` - The workflow type name
      * `:task_queue` - The task queue name
      * `:workflow_id` - Unique workflow identifier
      * `:input` - Optional workflow input data
      * `:execution_timeout` - Optional execution timeout in seconds
      * `:run_timeout` - Optional run timeout in seconds
      * `:task_timeout` - Optional task timeout in seconds
  
  ## Examples
  
      {:ok, handle} = Temporal.Client.start_workflow(client, %{
        workflow_type: "OrderProcessing",
        task_queue: "orders",
        workflow_id: "order-123",
        input: %{order_id: 123, amount: 99.99}
      })
  """
  @spec start_workflow(GenServer.server(), map()) :: {:ok, map()} | {:error, term()}
  def start_workflow(client, params) when is_map(params) do
    GenServer.call(client, {:start_workflow, params})
  end
  
  @doc """
  Stops the client process gracefully.
  """
  @spec stop(GenServer.server()) :: :ok
  def stop(client) do
    GenServer.stop(client)
  end
  
  @doc """
  Returns the current connection status of the client.
  """
  @spec status(GenServer.server()) :: :connected | :connecting | :disconnected | :error
  def status(client) do
    GenServer.call(client, :status)
  end
  
  # GenServer callbacks
  
  @impl true
  def init(opts) do
    # Emit telemetry event for client startup
    :telemetry.execute(
      [:temporal, :client, :init],
      %{},
      %{options: opts}
    )
    
    config = build_config(opts)
    
    state = %{
      config: config,
      client_resource: nil,
      status: :disconnected,
      connect_attempts: 0
    }
    
    # Connect on start if requested
    if Keyword.get(opts, :connect_on_start, false) do
      {:ok, state, {:continue, :connect}}
    else
      {:ok, state}
    end
  end
  
  @impl true
  def handle_continue(:connect, state) do
    case do_connect(state.config) do
      {:ok, client_resource} ->
        {:noreply, %{state | client_resource: client_resource, status: :connected}}
      
      {:error, reason} ->
        Logger.error("Failed to connect to Temporal: #{inspect(reason)}")
        {:noreply, %{state | status: :error}}
    end
  end
  
  @impl true
  def handle_call(:connect, _from, %{status: :connected} = state) do
    {:reply, :ok, state}
  end
  
  def handle_call(:connect, _from, state) do
    case do_connect(state.config) do
      {:ok, client_resource} ->
        new_state = %{state | 
          client_resource: client_resource, 
          status: :connected,
          connect_attempts: 0
        }
        {:reply, :ok, new_state}
      
      {:error, _reason} = error ->
        new_state = %{state | 
          status: :error,
          connect_attempts: state.connect_attempts + 1
        }
        {:reply, error, new_state}
    end
  end
  
  def handle_call({:start_workflow, params}, _from, state) do
    # Ensure we're connected
    state = ensure_connected(state)
    
    case state.status do
      :connected ->
        # Add required string conversions
        params = normalize_workflow_params(params)
        
        case Native.client_start_workflow(state.client_resource, params) do
          {:ok, handle} ->
            # Convert handle list to map for easier access
            handle_map = Map.new(handle)
            
            # Emit telemetry event
            :telemetry.execute(
              [:temporal, :client, :workflow, :started],
              %{},
              %{
                workflow_id: handle_map["workflow_id"],
                run_id: handle_map["run_id"]
              }
            )
            
            {:reply, {:ok, handle_map}, state}
          
          {:error, reason} = error ->
            Logger.error("Failed to start workflow: #{inspect(reason)}")
            
            # Emit telemetry event for failure
            :telemetry.execute(
              [:temporal, :client, :workflow, :start_failed],
              %{},
              %{reason: reason}
            )
            
            {:reply, error, state}
        end
      
      _ ->
        {:reply, {:error, :not_connected}, state}
    end
  end
  
  def handle_call(:status, _from, state) do
    {:reply, state.status, state}
  end
  
  @impl true
  def terminate(reason, state) do
    # Emit telemetry event
    :telemetry.execute(
      [:temporal, :client, :terminated],
      %{},
      %{reason: reason}
    )
    
    # The NIF resource will be cleaned up by BEAM GC
    # but we log for debugging
    if state.client_resource do
      Logger.debug("Temporal client terminating, resource will be cleaned up by GC")
    end
    
    :ok
  end
  
  # Private functions
  
  defp build_config(opts) do
    defaults = %{
      "target_url" => get_config_value(:target_url, opts, "localhost:7233"),
      "namespace" => get_config_value(:namespace, opts, "default")
    }
    
    config = 
      opts
      |> Keyword.take([:tls, :api_key, :identity])
      |> Enum.reduce(defaults, fn
        {:tls, tls_config}, acc when is_map(tls_config) ->
          Map.put(acc, "tls", tls_config)
        {:api_key, api_key}, acc when is_binary(api_key) ->
          Map.put(acc, "api_key", api_key)
        {:identity, identity}, acc when is_binary(identity) ->
          Map.put(acc, "identity", identity)
        _, acc ->
          acc
      end)
    
    config
  end
  
  defp get_config_value(key, opts, default) do
    # Priority: opts > application env > default
    Keyword.get_lazy(opts, key, fn ->
      Application.get_env(:temporal, __MODULE__, [])
      |> Keyword.get(key, default)
    end)
    |> to_string()
  end
  
  defp do_connect(config) do
    start_time = System.monotonic_time(:millisecond)
    
    result = Native.client_connect(config)
    
    duration = System.monotonic_time(:millisecond) - start_time
    
    # Emit telemetry event
    :telemetry.execute(
      [:temporal, :client, :connect],
      %{duration: duration},
      %{
        success: match?({:ok, _}, result) or is_reference(result),
        config: Map.take(config, ["target_url", "namespace"])
      }
    )
    
    case result do
      ref when is_reference(ref) -> {:ok, ref}
      {:error, _} = error -> error
      other -> {:error, {:unexpected_result, other}}
    end
  end
  
  defp ensure_connected(%{status: :connected} = state), do: state
  
  defp ensure_connected(state) do
    case do_connect(state.config) do
      {:ok, client_resource} ->
        %{state | 
          client_resource: client_resource, 
          status: :connected,
          connect_attempts: 0
        }
      
      {:error, _reason} ->
        %{state | 
          status: :error,
          connect_attempts: state.connect_attempts + 1
        }
    end
  end
  
  defp normalize_workflow_params(params) do
    params
    |> Enum.map(fn
      {:workflow_type, v} -> {"workflow_type", to_string(v)}
      {:task_queue, v} -> {"task_queue", to_string(v)}
      {:workflow_id, v} -> {"workflow_id", to_string(v)}
      {:request_id, v} -> {"request_id", to_string(v)}
      {:execution_timeout, v} -> {"execution_timeout", v}
      {:run_timeout, v} -> {"run_timeout", v}
      {:task_timeout, v} -> {"task_timeout", v}
      {:input, v} -> {"input", List.wrap(v)}
      {k, v} when is_atom(k) -> {Atom.to_string(k), v}
      {k, v} when is_binary(k) -> {k, v}
    end)
    |> Map.new()
  end
end