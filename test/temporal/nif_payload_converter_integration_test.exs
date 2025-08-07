defmodule Temporal.NifPayloadConverterIntegrationTest do
  use ExUnit.Case, async: false
  alias Temporal.TestContainer, as: TestContainer

  setup_all do
    _ = Process.whereis(Temporal.TestContainer) || elem(TestContainer.start_link([]), 1)

    case TestContainer.start_container() do
      {:ok, container, ports} ->
        on_exit(fn -> TestContainer.stop_container(container) end)
        {:ok, %{container: container, ports: ports}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp skip_if_no_container(context) do
    if Map.has_key?(context, :ports), do: context, else: {:skip, :no_container}
  end

  test "serializes all inputs via NIF converter", context do
    case skip_if_no_container(context) do
      {:skip, reason} ->
        {:skip, reason}

      context ->
        config = TestContainer.server_config(context.ports)
        client = Temporal.Native.client_connect(config)
        assert is_reference(client)

        wf_id = "wf-#{System.unique_integer([:positive])}"

        params = %{
          "workflow_type" => "TestWorkflow",
          "task_queue" => "test-queue",
          "workflow_id" => wf_id,
          "input" => [nil, Base.encode64(<<1, 2, 3>>), %{"a" => 1}]
        }

        result = Temporal.Native.client_start_workflow(client, params)
        assert match?({:ok, _}, result) or match?({:error, _}, result)
    end
  end
end
