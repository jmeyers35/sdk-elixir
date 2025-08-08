defmodule Temporal.PayloadConverter do
  @moduledoc """
  Protocol for converting Elixir terms to/from Temporal Payloads.

  This protocol enables pluggable serialization strategies that maintain
  cross-SDK compatibility with other Temporal language implementations.

  ## Built-in Converters

  - `JsonConverter` - JSON serialization with deterministic key ordering
  - `BinaryConverter` - Raw binary data handling  
  - `NilConverter` - Handles nil/undefined values
  - `ProtobufConverter` - Protocol Buffer message serialization

  ## Example Usage

      # Single converter
      converter = %Temporal.PayloadConverter.Json{}
      {:ok, payload} = Temporal.PayloadConverter.to_payload(converter, %{user_id: 123})
      
      # Converter chain
      chain = [
        %Temporal.PayloadConverter.Nil{},
        %Temporal.PayloadConverter.Binary{},
        %Temporal.PayloadConverter.Json{}
      ]
      {:ok, payloads} = Temporal.PayloadConverter.Chain.to_payloads(data_list, chain)
  """

  @type payload_metadata :: %{String.t() => binary()}

  @type payload :: %{
          data: binary(),
          metadata: payload_metadata()
        }

  @type t :: struct()
  @type conversion_result :: {:ok, payload()} | {:error, term()}

  @doc """
  Convert Elixir term to Temporal Payload.

  Returns `:skip` if this converter cannot handle the data type.
  """
  @callback to_payload(converter :: t(), data :: term()) :: conversion_result() | :skip

  @doc """
  Convert Temporal Payload back to Elixir term.

  Returns `:skip` if this converter cannot handle the payload encoding.
  """
  @callback from_payload(converter :: t(), payload()) :: {:ok, term()} | {:error, term()} | :skip

  @doc "Returns the encoding identifier for this converter"
  @callback encoding(converter :: t()) :: String.t()

  @doc "Returns priority order (lower = higher priority)"
  @callback priority(converter :: t()) :: non_neg_integer()

  @doc "Test if this converter can handle the given data type"
  @callback can_convert?(converter :: t(), data :: term()) :: boolean()

  @spec to_payload(t(), term()) :: conversion_result() | :skip
  def to_payload(converter, data) do
    converter.__struct__.to_payload(converter, data)
  end

  @spec from_payload(t(), payload()) :: {:ok, term()} | {:error, term()} | :skip
  def from_payload(converter, payload) do
    converter.__struct__.from_payload(converter, payload)
  end

  @spec encoding(t()) :: String.t()
  def encoding(converter) do
    converter.__struct__.encoding(converter)
  end

  @spec priority(t()) :: non_neg_integer()
  def priority(converter) do
    converter.__struct__.priority(converter)
  end

  @spec can_convert?(t(), term()) :: boolean()
  def can_convert?(converter, data) do
    converter.__struct__.can_convert?(converter, data)
  end

  @doc """
  Returns the default converter chain used by the SDK.
  
  The default chain includes (in priority order):
  1. NilConverter - handles nil values
  2. ProtobufConverter - handles Protocol Buffer messages
  3. BinaryConverter - handles binary data
  4. JsonConverter - handles everything else via JSON encoding
  """
  def default_converter_chain do
    [
      %Temporal.PayloadConverter.NilConverter{},
      %Temporal.PayloadConverter.ProtobufConverter{},
      %Temporal.PayloadConverter.BinaryConverter{},
      %Temporal.PayloadConverter.JsonConverter{}
    ]
  end

  @doc """
  Converts a list of Elixir terms to payloads using the default converter chain.
  """
  def to_payloads(data_list) when is_list(data_list) do
    Temporal.PayloadConverter.Chain.to_payloads(data_list, default_converter_chain())
  end

  @doc """
  Converts payloads back to Elixir terms using the default converter chain.
  """
  def from_payloads(payload_list) when is_list(payload_list) do
    Temporal.PayloadConverter.Chain.from_payloads(payload_list, default_converter_chain())
  end
end
