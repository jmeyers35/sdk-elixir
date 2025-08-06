defmodule Temporal.Native.Behaviour do
  @moduledoc """
  Behaviour definition for Temporal Native NIF interface.
  
  This behaviour allows for mocking of NIF functions during testing.
  """

  @doc """
  Connect to Temporal server with given configuration.
  """
  @callback client_connect(config :: map()) :: reference() | {:error, String.t()}

  @doc """
  Start a workflow execution.
  """
  @callback client_start_workflow(client :: reference(), params :: map()) :: 
    {:ok, list({String.t(), String.t()})} | {:error, String.t()}

  @doc """
  Send a signal to a workflow.
  """
  @callback client_signal_workflow(client :: reference(), params :: map()) :: 
    :ok | {:error, String.t()}

  @doc """
  Query a workflow.
  """
  @callback client_query_workflow(client :: reference(), params :: map()) :: 
    {:ok, term()} | {:error, String.t()}

  @doc """
  Create a new worker.
  """
  @callback worker_new(client :: reference(), config :: map()) :: 
    reference() | {:error, String.t()}

  @doc """
  Poll for workflow tasks.
  """
  @callback worker_poll_workflow_task(worker :: reference()) :: 
    {:ok, map()} | {:error, String.t()}

  @doc """
  Poll for activity tasks.
  """
  @callback worker_poll_activity_task(worker :: reference()) :: 
    {:ok, map()} | {:error, String.t()}

  @doc """
  Complete a workflow task.
  """
  @callback worker_complete_workflow_task(worker :: reference(), completion :: map()) :: 
    :ok | {:error, String.t()}

  @doc """
  Complete an activity task.
  """
  @callback worker_complete_activity_task(worker :: reference(), completion :: map()) :: 
    :ok | {:error, String.t()}
end