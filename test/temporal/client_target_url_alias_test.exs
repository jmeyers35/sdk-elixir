defmodule Temporal.ClientTargetUrlAliasTest do
  use ExUnit.Case, async: true

  alias Temporal.Client

  test "accepts :target_url as alias for :host" do
    {:ok, client} = Client.start_link(target_url: "alias-host:7233", namespace: "default")

    state = :sys.get_state(client)
    assert state.config["target_host"] == "alias-host:7233"

    Client.stop(client)
  end

  test ":host overrides :target_url when both provided" do
    {:ok, client} =
      Client.start_link(host: "canonical-host:9000", target_url: "ignored:7233", namespace: "default")

    state = :sys.get_state(client)
    assert state.config["target_host"] == "canonical-host:9000"

    Client.stop(client)
  end
end
