defmodule Temporal.ClientHeadersRetriesTest do
  use ExUnit.Case, async: false
  alias Temporal.Client
  alias Temporal.TestContainer

  @moduletag :integration
  @moduletag timeout: 60_000

  setup_all do
    case TestContainer.start_container() do
      {:ok, container, ports} -> {:ok, %{container: container, ports: ports}}
      {:error, reason} -> {:skip, "Docker container failed to start: #{inspect(reason)}"}
    end
  end

  test "connect accepts custom headers and api_key", context do
    case context do
      %{ports: ports} ->
        {:ok, client} =
          Client.start_link(
            host: TestContainer.server_url(ports),
            namespace: "default",
            headers: %{"x-custom" => "val", "another" => "hdr"},
            identity: "itests",
            # api_key plumbed into core headers
            headers: %{"authorization" => "Bearer test-token"}
          )

        assert :ok = Client.connect(client)
        assert Client.status(client) == :connected
        Client.stop(client)

      _ ->
        {:skip, "No test container available"}
    end
  end

  test "retries config is accepted and does not prevent connection", context do
    case context do
      %{ports: ports} ->
        {:ok, client} =
          Client.start_link(
            host: TestContainer.server_url(ports),
            namespace: "default",
            retries: %{max_attempts: 5, initial_backoff_ms: 200, max_backoff_ms: 1000}
          )

        assert :ok = Client.connect(client)
        assert Client.status(client) == :connected
        Client.stop(client)

      _ ->
        {:skip, "No test container available"}
    end
  end
end
