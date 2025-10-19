#!/usr/bin/env elixir

# Payload Converter Example
#
# Demonstrates configuring the payload converter chain and
# sending mixed-type inputs (nil, binary, json) through the client.
#
# Usage:
#   mix run examples/payload_converter_example.exs

Code.prepend_path("_build/dev/lib/temporal/ebin")

alias Temporal.Client

config = [
  host: "localhost:7233",
  namespace: "default",
  payload_converter: [nil, :binary, :json],
  payload_converter_options: %{json_max_depth: 32, binary_max_size: 1024 * 1024}
]

IO.puts("🚀 Payload Converter Example")

{:ok, client} = Client.start_link(config)

case Client.connect(client) do
  :ok ->
    IO.puts("   ✅ Connected!")

  {:error, reason} ->
    IO.puts("   ❌ Connection failed: #{inspect(reason)}")
    System.halt(1)
end

workflow_id = "converter-demo-#{System.unique_integer([:positive])}"
IO.puts("Starting workflow: #{workflow_id}")

params = %{
  workflow_type: "ConverterWorkflow",
  task_queue: "converter-queue",
  workflow_id: workflow_id,
  input: [
    nil,
    Base.encode64(<<1, 2, 3, 4>>),
    %{message: "json payload", at: DateTime.utc_now() |> DateTime.to_iso8601()}
  ]
}

case Client.start_workflow(client, params) do
  {:ok, handle} -> IO.puts("   ✅ Started: #{inspect(handle)}")
  {:error, reason} -> IO.puts("   ⚠️  Start failed (expected without worker): #{inspect(reason)}")
end

Client.stop(client)
IO.puts("Done")
