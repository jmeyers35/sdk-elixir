defmodule Temporal.PayloadConverter.NilConverter do
  @moduledoc """
  Converter for nil values, matching Temporal SDK patterns across languages.

  This converter handles Elixir's `nil` and produces payloads compatible
  with other SDKs' undefined/null value handling.
  """

  @behaviour Temporal.PayloadConverter

  defstruct []

  @impl true
  def to_payload(_converter, nil) do
    {:ok,
     %{
       data: <<>>,
       metadata: %{"encoding" => "binary/null"}
     }}
  end

  def to_payload(_converter, _data), do: :skip

  @impl true
  def from_payload(_converter, %{metadata: %{"encoding" => "binary/null"}}) do
    {:ok, nil}
  end

  def from_payload(_converter, _payload), do: :skip

  @impl true
  def encoding(_converter), do: "binary/null"

  @impl true
  def priority(_converter), do: 10

  @impl true
  def can_convert?(_converter, nil), do: true
  def can_convert?(_converter, _data), do: false
end
