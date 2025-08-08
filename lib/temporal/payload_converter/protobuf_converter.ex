defmodule Temporal.PayloadConverter.ProtobufConverter do
  @moduledoc """
  Payload converter for Protocol Buffer messages.

  This converter handles encoding/decoding of protobuf messages to maintain
  cross-SDK compatibility with other Temporal language implementations.

  Supports both binary and JSON encoding modes:
  - Binary protobuf (proto/binary) - compact binary format  
  - JSON protobuf (proto/json) - human-readable JSON format

  ## Example Usage

      # Define a protobuf message
      defmodule MyApp.UserMessage do
        use Protobuf, syntax: :proto3
        
        field :user_id, 1, type: :int64
        field :name, 2, type: :string
        field :email, 3, type: :string
      end

      # Convert to payload
      user = MyApp.UserMessage.new(user_id: 123, name: "Alice", email: "alice@example.com")
      converter = %Temporal.PayloadConverter.ProtobufConverter{}
      {:ok, payload} = Temporal.PayloadConverter.to_payload(converter, user)

      # Convert back from payload
      {:ok, decoded_user} = Temporal.PayloadConverter.from_payload(converter, payload)
  """

  @behaviour Temporal.PayloadConverter

  defstruct mode: :binary,
            include_type_url: true

  @type mode :: :binary | :json
  @type t :: %__MODULE__{
          mode: mode(),
          include_type_url: boolean()
        }

  @impl Temporal.PayloadConverter
  def to_payload(%__MODULE__{mode: mode, include_type_url: include_type_url}, data) do
    cond do
      is_protobuf_struct?(data) ->
        encode_protobuf(data, mode, include_type_url)

      true ->
        :skip
    end
  end

  @impl Temporal.PayloadConverter
  def from_payload(%__MODULE__{}, %{metadata: metadata, data: data}) do
    encoding = Map.get(metadata, "encoding", "")

    case encoding do
      "proto/binary" ->
        decode_protobuf_binary(data, metadata)

      "proto/json" ->
        decode_protobuf_json(data, metadata)

      _ ->
        :skip
    end
  end

  @impl Temporal.PayloadConverter
  def encoding(%__MODULE__{mode: :binary}), do: "proto/binary"
  def encoding(%__MODULE__{mode: :json}), do: "proto/json"

  @impl Temporal.PayloadConverter
  def priority(%__MODULE__{}), do: 15

  @impl Temporal.PayloadConverter
  def can_convert?(%__MODULE__{}, data) do
    is_protobuf_struct?(data)
  end

  # Private helper functions

  defp is_protobuf_struct?(data) do
    case data do
      %module{} ->
        # Check if the module uses Protobuf behavior
        module_implements_protobuf?(module)

      _ ->
        false
    end
  end

  defp module_implements_protobuf?(module) do
    try do
      # Check if the module has protobuf functions
      function_exported?(module, :encode, 1) and 
      function_exported?(module, :decode, 1) and
      function_exported?(module, :__message_props__, 0)
    rescue
      _ -> false
    end
  end

  defp encode_protobuf(data, mode, include_type_url) do
    module = data.__struct__

    case mode do
      :binary ->
        encode_protobuf_binary(data, module, include_type_url)

      :json ->
        encode_protobuf_json(data, module, include_type_url)
    end
  end

  defp encode_protobuf_binary(data, module, include_type_url) do
    try do
      encoded_data = module.encode(data)
      
      metadata = %{
        "encoding" => "proto/binary",
        "messageType" => get_message_type(module),
        "sdk" => "elixir"
      }

      metadata = 
        if include_type_url do
          Map.put(metadata, "messageTypeUrl", get_type_url(module))
        else
          metadata
        end

      {:ok, %{data: encoded_data, metadata: metadata}}
    rescue
      error ->
        {:error, {:protobuf_encoding_failed, error}}
    end
  end

  defp encode_protobuf_json(data, module, include_type_url) do
    try do
      # First encode to binary, then convert to JSON representation
      binary_data = module.encode(data)
      
      # For JSON mode, we store the binary data as base64
      json_data = Base.encode64(binary_data)
      
      metadata = %{
        "encoding" => "proto/json", 
        "messageType" => get_message_type(module),
        "contentType" => "application/x-protobuf",
        "sdk" => "elixir"
      }

      metadata = 
        if include_type_url do
          Map.put(metadata, "messageTypeUrl", get_type_url(module))
        else
          metadata
        end

      {:ok, %{data: json_data, metadata: metadata}}
    rescue
      error ->
        {:error, {:protobuf_json_encoding_failed, error}}
    end
  end

  defp decode_protobuf_binary(data, metadata) do
    case get_module_from_metadata(metadata) do
      {:ok, module} ->
        try do
          decoded = module.decode(data)
          {:ok, decoded}
        rescue
          error ->
            {:error, {:protobuf_decoding_failed, error}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp decode_protobuf_json(data, metadata) do
    case get_module_from_metadata(metadata) do
      {:ok, module} ->
        try do
          # For JSON mode, data is base64 encoded binary
          binary_data = Base.decode64!(data)
          decoded = module.decode(binary_data)
          {:ok, decoded}
        rescue
          error ->
            {:error, {:protobuf_json_decoding_failed, error}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp get_module_from_metadata(metadata) do
    message_type = Map.get(metadata, "messageType")

    case message_type do
      nil ->
        {:error, :missing_message_type}

      type_string when is_binary(type_string) ->
        try do
          # Convert message type string to Elixir module
          # e.g., "MyApp.UserMessage" -> MyApp.UserMessage
          module = String.to_existing_atom("Elixir.#{type_string}")
          
          if module_implements_protobuf?(module) do
            {:ok, module}
          else
            {:error, {:not_protobuf_module, module}}
          end
        rescue
          ArgumentError ->
            {:error, {:unknown_message_type, type_string}}
        end

      _ ->
        {:error, {:invalid_message_type_format, message_type}}
    end
  end

  defp get_message_type(module) do
    # Convert module name to message type string
    # e.g., MyApp.UserMessage -> "MyApp.UserMessage"
    module 
    |> Atom.to_string()
    |> String.replace_prefix("Elixir.", "")
  end

  defp get_type_url(module) do
    # Generate type URL following protobuf convention
    message_type = get_message_type(module)
    "type.googleapis.com/#{message_type}"
  end
end