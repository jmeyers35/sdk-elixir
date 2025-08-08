defmodule Temporal.PayloadConverter.JsonConverter do
  @moduledoc """
  Deterministic JSON converter for cross-SDK compatibility.

  This converter implements deterministic JSON serialization with:
  - Sorted keys for consistent output
  - Configurable depth limiting
  - Atom handling strategies
  - UTF-8 encoding compliance
  """

  @behaviour Temporal.PayloadConverter

  defstruct max_depth: 32,
            atom_handling: :string,
            sort_keys: true

  @type atom_handling :: :string | :atom | :error

  @impl true
  def to_payload(%{max_depth: max_depth} = converter, data) do
    case validate_depth(data, max_depth, 0) do
      :ok ->
        case encode_json(data, converter) do
          {:ok, json_binary} ->
            {:ok,
             %{
               data: json_binary,
               metadata: %{
                 "encoding" => "json/plain",
                 "contentType" => "application/json"
               }
             }}

          {:error, reason} ->
            {:error, {:json_encoding_failed, reason}}
        end

      {:error, _} = error ->
        error
    end
  end

  def to_payload(_converter, _data), do: :skip

  @impl true
  def from_payload(_converter, %{data: data, metadata: %{"encoding" => "json/plain"}})
      when is_binary(data) do
    case Jason.decode(data) do
      {:ok, decoded} -> {:ok, decoded}
      {:error, reason} -> {:error, {:json_decoding_failed, reason}}
    end
  end

  def from_payload(_converter, _payload), do: :skip

  @impl true
  def encoding(_converter), do: "json/plain"

  @impl true
  def priority(_converter), do: 100

  @impl true
  def can_convert?(%{max_depth: max_depth}, data) do
    case validate_depth(data, max_depth, 0) do
      :ok -> true
      {:error, _} -> false
    end
  end

  # Private functions for deterministic JSON encoding

  defp encode_json(data, %{sort_keys: true}) do
    # Use Jason with sorted keys for deterministic output
    case prepare_for_encoding(data) do
      {:ok, prepared} ->
        Jason.encode(prepared, pretty: false)

      {:error, _} = error ->
        error
    end
  end

  defp prepare_for_encoding(data) when is_map(data) do
    # Convert to sorted map for deterministic key ordering
    sorted_pairs =
      data
      |> Map.to_list()
      |> Enum.map(fn {k, v} ->
        case prepare_for_encoding(v) do
          {:ok, prepared_v} -> {:ok, {to_string(k), prepared_v}}
          error -> error
        end
      end)
      |> collect_results()

    case sorted_pairs do
      {:ok, pairs} ->
        {:ok, pairs |> Enum.sort_by(fn {k, _} -> k end) |> Map.new()}

      error ->
        error
    end
  end

  defp prepare_for_encoding(data) when is_list(data) do
    data
    |> Enum.map(&prepare_for_encoding/1)
    |> collect_results()
  end

  defp prepare_for_encoding(data) when is_atom(data) and data in [nil, true, false] do
    {:ok, data}
  end

  defp prepare_for_encoding(data) when is_atom(data) do
    {:ok, Atom.to_string(data)}
  end

  defp prepare_for_encoding(data) when is_binary(data) or is_number(data) do
    {:ok, data}
  end

  defp prepare_for_encoding(data) do
    {:error, {:unsupported_type, typeof(data)}}
  end

  defp collect_results(results) do
    results
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, item}, {:ok, acc} -> {:cont, {:ok, [item | acc]}}
      {:error, _} = error, _acc -> {:halt, error}
    end)
    |> case do
      {:ok, items} -> {:ok, Enum.reverse(items)}
      error -> error
    end
  end

  defp validate_depth(_data, max_depth, current_depth) when current_depth >= max_depth do
    {:error, {:depth_limit_exceeded, max_depth}}
  end

  defp validate_depth(data, max_depth, current_depth) when is_map(data) do
    data
    |> Map.values()
    |> Enum.reduce_while(:ok, fn value, :ok ->
      case validate_depth(value, max_depth, current_depth + 1) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp validate_depth(data, max_depth, current_depth) when is_list(data) do
    data
    |> Enum.reduce_while(:ok, fn value, :ok ->
      case validate_depth(value, max_depth, current_depth + 1) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp validate_depth(_data, _max_depth, _current_depth), do: :ok

  defp typeof(data) when is_binary(data), do: :binary
  defp typeof(data) when is_atom(data), do: :atom
  defp typeof(data) when is_number(data), do: :number
  defp typeof(data) when is_list(data), do: :list
  defp typeof(data) when is_map(data), do: :map
  defp typeof(data) when is_tuple(data), do: :tuple
  defp typeof(_data), do: :unknown
end
