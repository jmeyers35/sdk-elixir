defmodule Temporal.PayloadConverter.ProtobufConverterTest do
  use ExUnit.Case
  
  alias Temporal.PayloadConverter
  alias Temporal.PayloadConverter.ProtobufConverter
  alias Temporal.Test.ProtobufTestMessages.{TestMessage, NestedMessage}

  describe "binary mode converter" do
    setup do
      converter = %ProtobufConverter{mode: :binary, include_type_url: true}
      {:ok, converter: converter}
    end

    test "successfully converts protobuf message to payload", %{converter: converter} do
      message = TestMessage.new(
        user_id: 123, 
        name: "Alice", 
        email: "alice@example.com", 
        active: true
      )

      assert {:ok, payload} = PayloadConverter.to_payload(converter, message)
      
      # Verify payload structure
      assert is_binary(payload.data)
      assert payload.metadata["encoding"] == "proto/binary"
      assert payload.metadata["messageType"] == "Temporal.Test.ProtobufTestMessages.TestMessage"
      assert payload.metadata["messageTypeUrl"] == "type.googleapis.com/Temporal.Test.ProtobufTestMessages.TestMessage"
      assert payload.metadata["sdk"] == "elixir"
    end

    test "roundtrip conversion preserves data", %{converter: converter} do
      original = TestMessage.new(
        user_id: 456,
        name: "Bob", 
        email: "bob@example.com",
        active: false
      )

      # Convert to payload
      {:ok, payload} = PayloadConverter.to_payload(converter, original)
      
      # Convert back from payload
      {:ok, decoded} = PayloadConverter.from_payload(converter, payload)
      
      # Verify data integrity
      assert decoded.user_id == original.user_id
      assert decoded.name == original.name
      assert decoded.email == original.email
      assert decoded.active == original.active
    end

    test "handles nested protobuf messages", %{converter: converter} do
      user = TestMessage.new(user_id: 789, name: "Charlie", email: "charlie@example.com", active: true)
      nested = NestedMessage.new(id: 1, user: user, tags: ["admin", "user"])

      {:ok, payload} = PayloadConverter.to_payload(converter, nested)
      {:ok, decoded} = PayloadConverter.from_payload(converter, payload)
      
      assert decoded.id == 1
      assert decoded.user.user_id == 789
      assert decoded.user.name == "Charlie"
      assert decoded.tags == ["admin", "user"]
    end

    test "converter without type URL works", %{converter: _converter} do
      converter = %ProtobufConverter{mode: :binary, include_type_url: false}
      message = TestMessage.new(user_id: 999, name: "David")

      {:ok, payload} = PayloadConverter.to_payload(converter, message)
      
      assert payload.metadata["encoding"] == "proto/binary"
      assert payload.metadata["messageType"] == "Temporal.Test.ProtobufTestMessages.TestMessage"
      refute Map.has_key?(payload.metadata, "messageTypeUrl")
    end
  end

  describe "json mode converter" do
    setup do
      converter = %ProtobufConverter{mode: :json, include_type_url: true}
      {:ok, converter: converter}
    end

    test "successfully converts protobuf message to JSON payload", %{converter: converter} do
      message = TestMessage.new(user_id: 123, name: "Alice", email: "alice@example.com")

      assert {:ok, payload} = PayloadConverter.to_payload(converter, message)
      
      # Verify payload structure for JSON mode
      assert is_binary(payload.data)
      assert payload.metadata["encoding"] == "proto/json"
      assert payload.metadata["messageType"] == "Temporal.Test.ProtobufTestMessages.TestMessage"
      assert payload.metadata["contentType"] == "application/x-protobuf"
      assert payload.metadata["sdk"] == "elixir"
    end

    test "JSON mode roundtrip preserves data", %{converter: converter} do
      original = TestMessage.new(user_id: 555, name: "Eve", email: "eve@example.com", active: true)

      {:ok, payload} = PayloadConverter.to_payload(converter, original)
      {:ok, decoded} = PayloadConverter.from_payload(converter, payload)
      
      assert decoded.user_id == original.user_id
      assert decoded.name == original.name
      assert decoded.email == original.email
      assert decoded.active == original.active
    end
  end

  describe "converter behavior interface" do
    test "encoding returns correct string for binary mode" do
      converter = %ProtobufConverter{mode: :binary}
      assert PayloadConverter.encoding(converter) == "proto/binary"
    end

    test "encoding returns correct string for JSON mode" do
      converter = %ProtobufConverter{mode: :json}
      assert PayloadConverter.encoding(converter) == "proto/json"
    end

    test "priority returns 15" do
      converter = %ProtobufConverter{}
      assert PayloadConverter.priority(converter) == 15
    end

    test "can_convert? returns true for protobuf messages" do
      converter = %ProtobufConverter{}
      message = TestMessage.new(user_id: 123, name: "Test")
      
      assert PayloadConverter.can_convert?(converter, message) == true
    end

    test "can_convert? returns false for non-protobuf data" do
      converter = %ProtobufConverter{}
      
      refute PayloadConverter.can_convert?(converter, %{user_id: 123})
      refute PayloadConverter.can_convert?(converter, "string")
      refute PayloadConverter.can_convert?(converter, 123)
      refute PayloadConverter.can_convert?(converter, nil)
    end

    test "to_payload returns :skip for non-protobuf data" do
      converter = %ProtobufConverter{}
      
      assert PayloadConverter.to_payload(converter, %{user_id: 123}) == :skip
      assert PayloadConverter.to_payload(converter, "string") == :skip
      assert PayloadConverter.to_payload(converter, 123) == :skip
    end

    test "from_payload returns :skip for non-protobuf payloads" do
      converter = %ProtobufConverter{}
      
      json_payload = %{
        data: ~s({"user_id": 123}),
        metadata: %{"encoding" => "json/plain"}
      }
      
      assert PayloadConverter.from_payload(converter, json_payload) == :skip
    end
  end

  describe "error handling" do
    setup do
      converter = %ProtobufConverter{mode: :binary}
      {:ok, converter: converter}
    end

    test "handles missing messageType in metadata", %{converter: converter} do
      payload = %{
        data: <<1, 2, 3>>,
        metadata: %{"encoding" => "proto/binary"}
      }
      
      assert {:error, :missing_message_type} = PayloadConverter.from_payload(converter, payload)
    end

    test "handles unknown messageType", %{converter: converter} do
      payload = %{
        data: <<1, 2, 3>>,
        metadata: %{
          "encoding" => "proto/binary",
          "messageType" => "UnknownModule.NonExistentMessage"
        }
      }
      
      assert {:error, {:unknown_message_type, _}} = PayloadConverter.from_payload(converter, payload)
    end

    test "handles invalid protobuf data", %{converter: converter} do
      payload = %{
        data: <<255, 255, 255, 255>>, # Invalid protobuf data
        metadata: %{
          "encoding" => "proto/binary",
          "messageType" => "Temporal.Test.ProtobufTestMessages.TestMessage"
        }
      }
      
      assert {:error, {:protobuf_decoding_failed, _}} = PayloadConverter.from_payload(converter, payload)
    end

    test "handles invalid base64 in JSON mode" do
      converter = %ProtobufConverter{mode: :json}
      
      payload = %{
        data: "invalid-base64!@#", 
        metadata: %{
          "encoding" => "proto/json",
          "messageType" => "Temporal.Test.ProtobufTestMessages.TestMessage"
        }
      }
      
      assert {:error, {:protobuf_json_decoding_failed, _}} = PayloadConverter.from_payload(converter, payload)
    end
  end

  describe "cross-SDK compatibility" do
    test "metadata format matches Python SDK patterns" do
      converter = %ProtobufConverter{mode: :binary, include_type_url: true}
      message = TestMessage.new(user_id: 123, name: "Test")

      {:ok, payload} = PayloadConverter.to_payload(converter, message)
      
      # Verify metadata follows Python SDK patterns
      assert payload.metadata["encoding"] == "proto/binary"
      assert payload.metadata["messageType"] 
      assert payload.metadata["messageTypeUrl"]
      assert payload.metadata["sdk"] == "elixir"
    end

    test "JSON mode uses application/x-protobuf content type" do
      converter = %ProtobufConverter{mode: :json}
      message = TestMessage.new(user_id: 123)

      {:ok, payload} = PayloadConverter.to_payload(converter, message)
      
      assert payload.metadata["contentType"] == "application/x-protobuf"
    end
  end
end