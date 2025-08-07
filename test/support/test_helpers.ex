defmodule Temporal.TestHelpers do
  @moduledoc """
  Common test utilities and helpers for Temporal SDK tests.
  """

  import ExUnit.Assertions

  @doc """
  Waits for a condition to become true within a timeout.

  Useful for replacing sleep-based synchronization in tests.
  """
  def wait_until(condition_fn, timeout \\ 5000, check_interval \\ 50) do
    deadline = System.monotonic_time(:millisecond) + timeout

    do_wait_until(condition_fn, deadline, check_interval)
  end

  defp do_wait_until(condition_fn, deadline, check_interval) do
    if condition_fn.() do
      :ok
    else
      now = System.monotonic_time(:millisecond)

      if now >= deadline do
        flunk("Condition did not become true within timeout")
      else
        Process.sleep(check_interval)
        do_wait_until(condition_fn, deadline, check_interval)
      end
    end
  end

  @doc """
  Generates a unique workflow ID for testing.
  """
  def unique_workflow_id(prefix \\ "test-wf") do
    "#{prefix}-#{System.unique_integer([:positive])}"
  end

  @doc """
  Generates a unique task queue name for testing.
  """
  def unique_task_queue(prefix \\ "test-queue") do
    "#{prefix}-#{System.unique_integer([:positive])}"
  end

  @doc """
  Creates test workflow parameters with all required fields.
  """
  def workflow_params(overrides \\ %{}) do
    defaults = %{
      workflow_type: "TestWorkflow",
      task_queue: unique_task_queue(),
      workflow_id: unique_workflow_id()
    }

    Map.merge(defaults, overrides)
  end

  @doc """
  Asserts that an error message contains expected content.

  More specific than just checking if it's a binary.
  """
  def assert_error_contains({:error, reason}, expected) when is_binary(reason) do
    assert reason =~ expected,
           "Expected error to contain '#{expected}', but got: #{reason}"
  end

  def assert_error_contains(other, _expected) do
    flunk("Expected {:error, reason}, but got: #{inspect(other)}")
  end

  @doc """
  Starts a supervised client for testing and ensures cleanup.
  """
  defmacro with_supervised_client(opts, do: block) do
    quote do
      opts = unquote(opts)

      # Ensure unique name if not provided
      opts =
        if Keyword.has_key?(opts, :name) do
          opts
        else
          Keyword.put(opts, :name, :"test_client_#{System.unique_integer([:positive])}")
        end

      {:ok, client} = start_supervised({Temporal.Client, opts})

      try do
        var!(client) = client
        unquote(block)
      after
        stop_supervised(client)
      end
    end
  end

  @doc """
  Captures telemetry events during test execution.
  """
  def capture_telemetry(event_names) when is_list(event_names) do
    test_pid = self()
    ref = make_ref()

    handler_id = "test-handler-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler_id,
      event_names,
      fn event_name, measurements, metadata, _config ->
        send(test_pid, {ref, event_name, measurements, metadata})
      end,
      nil
    )

    {ref, handler_id}
  end

  @doc """
  Waits for a telemetry event and returns it.
  """
  def assert_telemetry_event(ref, event_name, timeout \\ 1000) do
    receive do
      {^ref, ^event_name, measurements, metadata} ->
        {measurements, metadata}
    after
      timeout ->
        flunk("Did not receive telemetry event #{inspect(event_name)} within #{timeout}ms")
    end
  end

  @doc """
  Cleans up telemetry handler after test.
  """
  def cleanup_telemetry(handler_id) do
    :telemetry.detach(handler_id)
  end

  @doc """
  Validates that a GenServer state has expected structure.
  """
  def assert_valid_client_state(state) do
    assert is_map(state)
    assert Map.has_key?(state, :config)
    assert Map.has_key?(state, :client_resource)
    assert Map.has_key?(state, :status)
    assert Map.has_key?(state, :connect_attempts)

    assert is_map(state.config)
    assert state.status in [:disconnected, :connecting, :connected, :error]
    assert is_integer(state.connect_attempts) and state.connect_attempts >= 0
  end

  @doc """
  Helper to safely get GenServer state in tests.
  """
  def get_client_state(client) do
    :sys.get_state(client)
  end

  @doc """
  Asserts that a process is linked to another process.
  """
  def assert_linked(process1, process2) do
    {:links, links} = Process.info(process1, :links)

    assert process2 in links,
           "Expected #{inspect(process1)} to be linked to #{inspect(process2)}"
  end

  @doc """
  Creates a test configuration for client.
  """
  def test_client_config(overrides \\ %{}) do
    defaults = %{
      "target_host" => "localhost:7233",
      "namespace" => "test-namespace"
    }

    Map.merge(defaults, overrides)
  end
end
