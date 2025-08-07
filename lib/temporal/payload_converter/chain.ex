defmodule Temporal.PayloadConverter.Chain do
  @moduledoc """
  Manages converter chain execution with fallback strategies.

  This module implements the chain-of-responsibility pattern used
  across all Temporal SDKs for composable payload conversion.

  ## Converter Selection

  For serialization (to_payload):
  - Try converters in priority order until one succeeds
  - Return error if no converter can handle the data

  For deserialization (from_payload):
  - Select converter based on payload encoding metadata
  - Fall back to chain iteration if encoding not recognized

  ## Error Handling

  Individual converter failures are logged but don't stop the chain.
  Only when all converters fail (or are skipped) does the operation fail.
  """

  require Logger

  @type converter_chain :: [Temporal.PayloadConverter.t()]
  @type payload_list :: [Temporal.PayloadConverter.payload()]

  @doc """
  Convert a list of Elixir terms to Temporal payloads using the converter chain.

  ## Examples

      chain = [
        %Temporal.PayloadConverter.NilConverter{},
        %Temporal.PayloadConverter.BinaryConverter{},
        %Temporal.PayloadConverter.JsonConverter{}
      ]
      
      {:ok, payloads} = Chain.to_payloads([nil, "hello", %{user: 123}], chain)
  """
  @spec to_payloads([term()], converter_chain()) :: {:ok, payload_list()} | {:error, term()}
  def to_payloads(data_list, converters) when is_list(data_list) and is_list(converters) do
    sorted_converters = sort_converters_by_priority(converters)

    results =
      Enum.map(data_list, fn data ->
        convert_single_data(data, sorted_converters)
      end)

    case extract_results(results) do
      {:ok, payloads} ->
        {:ok, payloads}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Convert a list of Temporal payloads back to Elixir terms.

  ## Examples

      {:ok, data} = Chain.from_payloads(payloads, chain)
  """
  @spec from_payloads(payload_list(), converter_chain()) :: {:ok, [term()]} | {:error, term()}
  def from_payloads(payload_list, converters)
      when is_list(payload_list) and is_list(converters) do
    results =
      Enum.map(payload_list, fn payload ->
        convert_single_payload(payload, converters)
      end)

    extract_results(results)
  end

  @doc """
  Convert a single term to a payload using the converter chain.
  """
  @spec to_payload(term(), converter_chain()) ::
          {:ok, Temporal.PayloadConverter.payload()} | {:error, term()}
  def to_payload(data, converters) do
    sorted_converters = sort_converters_by_priority(converters)
    convert_single_data(data, sorted_converters)
  end

  @doc """
  Convert a single payload back to an Elixir term.
  """
  @spec from_payload(Temporal.PayloadConverter.payload(), converter_chain()) ::
          {:ok, term()} | {:error, term()}
  def from_payload(payload, converters) do
    convert_single_payload(payload, converters)
  end

  # Private functions

  defp sort_converters_by_priority(converters) do
    Enum.sort_by(converters, &Temporal.PayloadConverter.priority/1)
  end

  defp convert_single_data(data, converters) do
    case try_converters_for_data(data, converters) do
      {:ok, payload} -> {:ok, payload}
      :no_converter -> {:error, {:no_converter_found, data}}
      {:error, reason} -> {:error, {:conversion_failed, reason, data}}
    end
  end

  defp convert_single_payload(payload, converters) do
    # First try to select converter by encoding metadata
    encoding = get_in(payload, [:metadata, "encoding"])

    case select_converter_by_encoding(converters, encoding) do
      {:ok, converter} ->
        case Temporal.PayloadConverter.from_payload(converter, payload) do
          {:ok, data} -> {:ok, data}
          {:error, reason} -> {:error, {:conversion_failed, reason, payload}}
          :skip -> try_converters_for_payload(payload, converters)
        end

      :not_found ->
        try_converters_for_payload(payload, converters)
    end
  end

  defp try_converters_for_data(data, [converter | rest]) do
    try do
      case Temporal.PayloadConverter.to_payload(converter, data) do
        {:ok, payload} ->
          {:ok, payload}

        :skip ->
          try_converters_for_data(data, rest)

        {:error, _reason} ->
          try_converters_for_data(data, rest)
      end
    rescue
      e ->
        Logger.warning("Converter #{inspect(converter.__struct__)} crashed: #{inspect(e)}")
        try_converters_for_data(data, rest)
    end
  end

  defp try_converters_for_data(_data, []), do: :no_converter

  defp try_converters_for_payload(payload, [converter | rest]) do
    try do
      case Temporal.PayloadConverter.from_payload(converter, payload) do
        {:ok, data} ->
          {:ok, data}

        :skip ->
          try_converters_for_payload(payload, rest)

        {:error, _reason} ->
          try_converters_for_payload(payload, rest)
      end
    rescue
      e ->
        Logger.warning("Converter #{inspect(converter.__struct__)} crashed: #{inspect(e)}")
        try_converters_for_payload(payload, rest)
    end
  end

  defp try_converters_for_payload(_payload, []), do: {:error, :no_converter_found}

  defp select_converter_by_encoding(_converters, nil), do: :not_found

  defp select_converter_by_encoding(converters, encoding) do
    case Enum.find(converters, fn converter ->
           Temporal.PayloadConverter.encoding(converter) == encoding
         end) do
      nil -> :not_found
      converter -> {:ok, converter}
    end
  end

  defp extract_results(results) do
    case Enum.split_with(results, &match?({:ok, _}, &1)) do
      {successes, []} ->
        {:ok, Enum.map(successes, fn {:ok, result} -> result end)}

      {_, [error | _]} ->
        error
    end
  end
end
