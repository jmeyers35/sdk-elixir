defmodule Temporal.Client do
  @moduledoc """
  GenServer-based client for interacting with Temporal services.

  This module provides a high-level, idiomatic Elixir interface for:
  - Managing connections to Temporal servers
  - Starting workflow executions
  - Signaling workflow executions
  - Querying workflow executions
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
  alias Temporal.Client.Config

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
  Sends a signal to a workflow execution.

  ## Parameters

    * `client` - The client process
    * `params` - Signal parameters map with required fields:
      * `:workflow_id` - The workflow identifier to signal
      * `:signal_name` - The signal name to send
      * `:run_id` - Optional specific run ID to signal
      * `:input` - Optional signal input data
      * `:namespace` - Optional namespace (defaults to client namespace)

  ## Examples

      :ok = Temporal.Client.signal_workflow(client, %{
        workflow_id: "order-123",
        signal_name: "cancel_order",
        input: %{reason: "customer_request"}
      })
  """
  @spec signal_workflow(GenServer.server(), map()) :: :ok | {:error, term()}
  def signal_workflow(client, params) when is_map(params) do
    GenServer.call(client, {:signal_workflow, params})
  end

  @doc """
  Queries a workflow execution.

  ## Parameters

    * `client` - The client process
    * `params` - Query parameters map with required fields:
      * `:workflow_id` - The workflow identifier to query
      * `:query_type` - The query type name
      * `:run_id` - Optional specific run ID to query
      * `:input` - Optional query input data
      * `:namespace` - Optional namespace (defaults to client namespace)

  ## Examples

      {:ok, result} = Temporal.Client.query_workflow(client, %{
        workflow_id: "order-123",
        query_type: "get_status"
      })
  """
  @spec query_workflow(GenServer.server(), map()) :: {:ok, term()} | {:error, term()}
  def query_workflow(client, params) when is_map(params) do
    GenServer.call(client, {:query_workflow, params})
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
        new_state = %{
          state
          | client_resource: client_resource,
            status: :connected,
            connect_attempts: 0
        }

        {:reply, :ok, new_state}

      {:error, _reason} = error ->
        new_state = %{state | status: :error, connect_attempts: state.connect_attempts + 1}
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

  def handle_call({:signal_workflow, params}, _from, state) do
    # Ensure we're connected
    state = ensure_connected(state)

    case state.status do
      :connected ->
        # Add required string conversions
        params = normalize_signal_params(params)

        case Native.client_signal_workflow(state.client_resource, params) do
          :ok ->
            # Emit telemetry event
            :telemetry.execute(
              [:temporal, :client, :workflow, :signaled],
              %{},
              %{
                workflow_id: params["workflow_id"],
                signal_name: params["signal_name"],
                run_id: params["run_id"]
              }
            )

            {:reply, :ok, state}

          {:error, reason} = error ->
            Logger.error("Failed to signal workflow: #{inspect(reason)}")

            # Emit telemetry event for failure
            :telemetry.execute(
              [:temporal, :client, :workflow, :signal_failed],
              %{},
              %{reason: reason}
            )

            {:reply, error, state}
        end

      _ ->
        {:reply, {:error, :not_connected}, state}
    end
  end

  def handle_call({:query_workflow, params}, _from, state) do
    # Ensure we're connected
    state = ensure_connected(state)

    case state.status do
      :connected ->
        # Add required string conversions
        params = normalize_query_params(params)

        case Native.client_query_workflow(state.client_resource, params) do
          {:ok, result} ->
            # Emit telemetry event
            :telemetry.execute(
              [:temporal, :client, :workflow, :queried],
              %{},
              %{
                workflow_id: params["workflow_id"],
                query_type: params["query_type"],
                run_id: params["run_id"]
              }
            )

            {:reply, {:ok, result}, state}

          {:error, reason} = error ->
            Logger.error("Failed to query workflow: #{inspect(reason)}")

            # Emit telemetry event for failure
            :telemetry.execute(
              [:temporal, :client, :workflow, :query_failed],
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

  defp build_config(opts) when is_struct(opts, Config) do
    {:ok, cfg} = Config.validate(opts)
    to_nif_config(cfg)
  end

  defp build_config(opts) when is_list(opts) do
    env = Config.from_env()
    cfg = struct(env, %{})
    cfg = Map.merge(cfg, opts_to_cfg(opts))
    {:ok, cfg} = Config.validate(cfg)
    to_nif_config(cfg)
  end

  defp opts_to_cfg(opts) do
    %Config{
      host: to_string(Keyword.get(opts, :host, "localhost:7233")),
      namespace: to_string(Keyword.get(opts, :namespace, "default")),
      task_queue: to_string(Keyword.get(opts, :task_queue, "default")),
      tls: Keyword.get(opts, :tls),
      retries: Keyword.get(opts, :retries, %{}),
      identity: Keyword.get(opts, :identity),
      headers: Keyword.get(opts, :headers, %{})
    }
  end

  defp to_nif_config(%Config{} = cfg) do
    %{
      "target_host" => cfg.host,
      "namespace" => cfg.namespace,
      "task_queue" => cfg.task_queue,
      "identity" => cfg.identity,
      "headers" => cfg.headers,
      "tls_config" => cfg.tls,
      "retries" => cfg.retries,
      "payload_converter" => cfg.payload_converter,
      "payload_converter_options" => cfg.payload_converter_options
    }
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
        config: Map.take(config, ["target_host", "target_url", "namespace"])
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
        %{state | client_resource: client_resource, status: :connected, connect_attempts: 0}

      {:error, _reason} ->
        %{state | status: :error, connect_attempts: state.connect_attempts + 1}
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
      {:input, v} -> 
        # Convert input data to payloads at the Elixir layer
        input_list = List.wrap(v)
        case Temporal.PayloadConverter.to_payloads(input_list) do
          {:ok, payloads} -> {"input", payloads}
          {:error, _reason} -> {"input", []}
        end
      {k, v} when is_atom(k) -> {Atom.to_string(k), v}
      {k, v} when is_binary(k) -> {k, v}
    end)
    |> Map.new()
  end

  defp normalize_signal_params(params) do
    params
    |> Enum.map(fn
      {:workflow_id, v} -> {"workflow_id", to_string(v)}
      {:run_id, v} -> {"run_id", to_string(v)}
      {:signal_name, v} -> {"signal_name", to_string(v)}
      {:namespace, v} -> {"namespace", to_string(v)}
      {:input, v} -> 
        # Convert input data to payloads at the Elixir layer
        input_list = List.wrap(v)
        case Temporal.PayloadConverter.to_payloads(input_list) do
          {:ok, payloads} -> {"input", payloads}
          {:error, _reason} -> {"input", []}
        end
      {k, v} when is_atom(k) -> {Atom.to_string(k), v}
      {k, v} when is_binary(k) -> {k, v}
    end)
    |> Map.new()
  end

  defp normalize_query_params(params) do
    params
    |> Enum.map(fn
      {:workflow_id, v} -> {"workflow_id", to_string(v)}
      {:run_id, v} -> {"run_id", to_string(v)}
      {:query_type, v} -> {"query_type", to_string(v)}
      {:namespace, v} -> {"namespace", to_string(v)}
      {:input, v} -> 
        # Convert input data to payloads at the Elixir layer
        input_list = List.wrap(v)
        case Temporal.PayloadConverter.to_payloads(input_list) do
          {:ok, payloads} -> {"input", payloads}
          {:error, _reason} -> {"input", []}
        end
      {k, v} when is_atom(k) -> {Atom.to_string(k), v}
      {k, v} when is_binary(k) -> {k, v}
    end)
    |> Map.new()
  end
end
