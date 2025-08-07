defmodule Temporal.Native do
  @moduledoc """
  Native interface module for Temporal NIFs.
  """

  @behaviour Temporal.Native.Behaviour
  use Rustler, otp_app: :temporal, crate: "temporal_nif"

  # Client functions
  def client_connect(_config), do: :erlang.nif_error(:nif_not_loaded)
  def client_start_workflow(_client, _params), do: :erlang.nif_error(:nif_not_loaded)
  def client_signal_workflow(_client, _params), do: :erlang.nif_error(:nif_not_loaded)
  def client_query_workflow(_client, _params), do: :erlang.nif_error(:nif_not_loaded)

  # Worker functions
  def worker_new(_client, _config), do: :erlang.nif_error(:nif_not_loaded)
  def worker_poll_workflow_task(_worker), do: :erlang.nif_error(:nif_not_loaded)
  def worker_poll_activity_task(_worker), do: :erlang.nif_error(:nif_not_loaded)
  def worker_complete_workflow_task(_worker, _completion), do: :erlang.nif_error(:nif_not_loaded)
  def worker_complete_activity_task(_worker, _completion), do: :erlang.nif_error(:nif_not_loaded)
end
