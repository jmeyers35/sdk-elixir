defmodule Temporal.Mocks do
  @moduledoc """
  Mock definitions for Temporal NIF functions using Mox.

  This module defines mocks for testing Temporal.Client GenServer logic
  without requiring actual NIF calls or Docker containers.
  """

  import Mox

  # Define mock for Temporal.Native NIF module
  defmock(Temporal.Native.Mock, for: Temporal.Native.Behaviour)

  @doc """
  Set up common mock expectations for successful operations.
  """
  def expect_successful_connection(mock_ref \\ make_ref()) do
    Temporal.Native.Mock
    |> expect(:client_connect, fn config ->
      if is_map(config) and
           (Map.has_key?(config, "target_host") or Map.has_key?(config, "target_url")) and
           Map.has_key?(config, "namespace") do
        mock_ref
      else
        {:error, "Invalid configuration"}
      end
    end)
  end

  @doc """
  Set up mock expectations for connection failures.
  """
  def expect_connection_failure(reason \\ "Connection failed") do
    Temporal.Native.Mock
    |> expect(:client_connect, fn _config ->
      {:error, reason}
    end)
  end

  @doc """
  Set up mock expectations for successful workflow start.
  """
  def expect_successful_workflow_start(workflow_id, run_id \\ "test-run-id") do
    Temporal.Native.Mock
    |> expect(:client_start_workflow, fn _client_resource, params ->
      if is_map(params) and Map.has_key?(params, "workflow_type") do
        {:ok,
         [
           {"workflow_id", Map.get(params, "workflow_id", workflow_id)},
           {"run_id", run_id},
           {"first_execution_run_id", run_id}
         ]}
      else
        {:error, "Invalid workflow parameters"}
      end
    end)
  end

  @doc """
  Set up mock expectations for workflow start failures.
  """
  def expect_workflow_start_failure(reason \\ "Workflow start failed") do
    Temporal.Native.Mock
    |> expect(:client_start_workflow, fn _client_resource, _params ->
      {:error, reason}
    end)
  end

  @doc """
  Set up mock to allow any number of calls.
  """
  def allow_any_calls do
    Temporal.Native.Mock
    |> stub(:client_connect, fn _config -> make_ref() end)
    |> stub(:client_start_workflow, fn _client_resource, params ->
      workflow_id = Map.get(params, "workflow_id", "test-workflow-id")

      {:ok,
       [
         {"workflow_id", workflow_id},
         {"run_id", "test-run-id"},
         {"first_execution_run_id", "test-run-id"}
       ]}
    end)
  end
end
