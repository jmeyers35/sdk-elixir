defmodule Temporal.PayloadConverter.BinaryConverter do
  @moduledoc """
  Converter for binary data, preserving raw bytes without encoding.

  This converter handles Elixir binaries and maintains compatibility
  with other Temporal SDKs' binary payload handling.
  """

  @behaviour Temporal.PayloadConverter

  defstruct max_size: 1024 * 1024

  @impl true
  def to_payload(%{max_size: max_size}, data) when is_binary(data) do
    if byte_size(data) <= max_size do
      {:ok,
       %{
         data: data,
         metadata: %{"encoding" => "binary/plain"}
       }}
    else
      {:error, {:binary_too_large, byte_size(data), max_size}}
    end
  end

  def to_payload(_converter, _data), do: :skip

  @impl true
  def from_payload(_converter, %{data: data, metadata: %{"encoding" => "binary/plain"}})
      when is_binary(data) do
    {:ok, data}
  end

  def from_payload(_converter, _payload), do: :skip

  @impl true
  def encoding(_converter), do: "binary/plain"

  @impl true
  def priority(_converter), do: 20

  @impl true
  def can_convert?(%{max_size: max_size}, data) when is_binary(data) do
    byte_size(data) <= max_size
  end

  def can_convert?(_converter, _data), do: false
end
