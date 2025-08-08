defmodule Temporal.PayloadConverterProtobufIntegrationTest do
  use ExUnit.Case
  
  alias Temporal.PayloadConverter
  alias Temporal.PayloadConverter.{Chain, ProtobufConverter}
  alias Temporal.Test.ProtobufTestMessages.{UserMessage, OrderMessage}

  describe "converter chain with protobuf support" do
    test "default converter chain includes protobuf converter" do
      chain = PayloadConverter.default_converter_chain()
      
      protobuf_converter = Enum.find(chain, fn converter -> 
        converter.__struct__ == ProtobufConverter
      end)
      
      assert protobuf_converter != nil
      assert PayloadConverter.priority(protobuf_converter) == 15
    end

    test "converter chain selects protobuf converter for protobuf messages" do
      user = UserMessage.new(
        user_id: 123,
        name: "Alice",
        email: "alice@example.com",
        active: true,
        created_at: System.system_time(:second)
      )
      
      {:ok, payloads} = PayloadConverter.to_payloads([user])
      payload = List.first(payloads)
      
      assert payload.metadata["encoding"] == "proto/binary"
      assert payload.metadata["messageType"] == "Temporal.Test.ProtobufTestMessages.UserMessage"
      assert payload.metadata["sdk"] == "elixir"
    end

    test "mixed data types use appropriate converters" do
      user = UserMessage.new(user_id: 456, name: "Bob", email: "bob@example.com")
      mixed_data = [nil, user, %{json: "data"}, Base.encode64(<<1, 2, 3>>)]
      
      {:ok, payloads} = PayloadConverter.to_payloads(mixed_data)
      
      assert length(payloads) == 4
      
      # Nil uses NilConverter
      assert Enum.at(payloads, 0).metadata["encoding"] == "binary/null"
      
      # Protobuf message uses ProtobufConverter  
      assert Enum.at(payloads, 1).metadata["encoding"] == "proto/binary"
      
      # Map uses JsonConverter
      assert Enum.at(payloads, 2).metadata["encoding"] == "json/plain"
      
      # Binary uses BinaryConverter
      assert Enum.at(payloads, 3).metadata["encoding"] == "binary/plain"
    end

    test "roundtrip conversion through chain preserves all data types" do
      user = UserMessage.new(user_id: 789, name: "Charlie", active: true)
      original_data = [nil, user, %{"key" => "value"}, Base.encode64(<<4, 5, 6>>)]
      
      # Convert to payloads
      {:ok, payloads} = PayloadConverter.to_payloads(original_data)
      
      # Convert back to data
      {:ok, decoded_data} = PayloadConverter.from_payloads(payloads)
      
      # Verify data integrity
      assert Enum.at(decoded_data, 0) == nil
      assert Enum.at(decoded_data, 1).user_id == 789
      assert Enum.at(decoded_data, 1).name == "Charlie"
      assert Enum.at(decoded_data, 2) == %{"key" => "value"} # JSON preserves string keys
      assert Enum.at(decoded_data, 3) == Base.encode64(<<4, 5, 6>>)
    end
  end

  describe "complex protobuf message handling" do
    test "nested protobuf messages convert correctly" do
      user = UserMessage.new(
        user_id: 1001,
        name: "David",
        email: "david@example.com",
        active: true
      )
      
      order = OrderMessage.new(
        order_id: "ORD-12345",
        user: user,
        items: ["item1", "item2", "item3"],
        total_amount: 99.99
      )
      
      {:ok, payload} = Chain.to_payload(order, PayloadConverter.default_converter_chain())
      {:ok, decoded_order} = Chain.from_payload(payload, PayloadConverter.default_converter_chain())
      
      assert decoded_order.order_id == "ORD-12345"
      assert decoded_order.user.user_id == 1001
      assert decoded_order.user.name == "David"
      assert decoded_order.items == ["item1", "item2", "item3"]
      assert decoded_order.total_amount == 99.99
    end

    test "large protobuf messages handle correctly" do
      # Create a large protobuf message
      large_items = Enum.map(1..1000, fn i -> "item_#{i}" end)
      
      order = OrderMessage.new(
        order_id: "LARGE-ORDER",
        user: UserMessage.new(user_id: 9999, name: "LargeOrderUser"),
        items: large_items,
        total_amount: 999999.99
      )
      
      {:ok, payload} = Chain.to_payload(order, PayloadConverter.default_converter_chain())
      {:ok, decoded_order} = Chain.from_payload(payload, PayloadConverter.default_converter_chain())
      
      assert length(decoded_order.items) == 1000
      assert decoded_order.total_amount == 999999.99
      assert decoded_order.user.user_id == 9999
    end
  end

  describe "cross-SDK compatibility simulation" do
    test "simulates Python SDK protobuf payload format" do
      # Simulate what a Python SDK might produce
      user = UserMessage.new(user_id: 2001, name: "PythonUser", email: "python@sdk.com")
      {:ok, elixir_payload} = Chain.to_payload(user, PayloadConverter.default_converter_chain())
      
      # Simulate Python SDK metadata format (should be compatible)
      python_style_payload = %{
        data: elixir_payload.data,
        metadata: %{
          "encoding" => "proto/binary",
          "messageType" => "Temporal.Test.ProtobufTestMessages.UserMessage",
          "sdk" => "python" # Different SDK but same format
        }
      }
      
      # Elixir should be able to decode Python-style payload
      {:ok, decoded} = Chain.from_payload(python_style_payload, PayloadConverter.default_converter_chain())
      
      assert decoded.user_id == 2001
      assert decoded.name == "PythonUser"
      assert decoded.email == "python@sdk.com"
    end

    test "metadata includes required fields for cross-SDK compatibility" do
      user = UserMessage.new(user_id: 3001, name: "CrossSDKUser")
      {:ok, payload} = Chain.to_payload(user, PayloadConverter.default_converter_chain())
      
      # Verify all required metadata fields are present
      assert payload.metadata["encoding"] == "proto/binary"
      assert payload.metadata["messageType"] != nil
      assert payload.metadata["messageTypeUrl"] != nil
      assert payload.metadata["sdk"] == "elixir"
      
      # Verify messageTypeUrl follows convention
      expected_type_url = "type.googleapis.com/Temporal.Test.ProtobufTestMessages.UserMessage"
      assert payload.metadata["messageTypeUrl"] == expected_type_url
    end

    test "JSON mode produces base64 encoded protobuf compatible with other SDKs" do
      converter_chain = [
        %Temporal.PayloadConverter.NilConverter{},
        %ProtobufConverter{mode: :json, include_type_url: true},
        %Temporal.PayloadConverter.BinaryConverter{},
        %Temporal.PayloadConverter.JsonConverter{}
      ]
      
      user = UserMessage.new(user_id: 4001, name: "JSONModeUser")
      {:ok, payload} = Chain.to_payload(user, converter_chain)
      
      assert payload.metadata["encoding"] == "proto/json"
      assert payload.metadata["contentType"] == "application/x-protobuf"
      
      # Verify data is base64 encoded
      assert String.match?(payload.data, ~r/^[A-Za-z0-9+\/]*={0,2}$/)
      
      # Should roundtrip correctly
      {:ok, decoded} = Chain.from_payload(payload, converter_chain)
      assert decoded.user_id == 4001
      assert decoded.name == "JSONModeUser"
    end
  end

  describe "error handling in integration scenarios" do
    test "gracefully handles unknown message types in chain" do
      # Create payload with unknown message type
      unknown_payload = %{
        data: <<1, 2, 3>>,
        metadata: %{
          "encoding" => "proto/binary",
          "messageType" => "NonExistent.UnknownMessage"
        }
      }
      
      # Chain should return error for unknown type
      result = Chain.from_payload(unknown_payload, PayloadConverter.default_converter_chain())
      assert {:error, _} = result
    end

    test "chain falls back to other converters when protobuf fails" do
      # This shouldn't happen in normal use, but tests fallback behavior
      chain_without_protobuf = [
        %Temporal.PayloadConverter.NilConverter{},
        %Temporal.PayloadConverter.BinaryConverter{},
        %Temporal.PayloadConverter.JsonConverter{}
      ]
      
      user = UserMessage.new(user_id: 5001, name: "FallbackTest")
      
      # Should fall back to JSON converter
      {:ok, payload} = Chain.to_payload(user, chain_without_protobuf)
      assert payload.metadata["encoding"] == "json/plain"
      
      # Should still roundtrip, but as JSON
      {:ok, decoded} = Chain.from_payload(payload, chain_without_protobuf)
      # Note: This will be a map, not a protobuf struct
      assert is_map(decoded)
    end
  end

  describe "performance and resource usage" do
    test "handles multiple protobuf conversions efficiently" do
      users = Enum.map(1..100, fn i ->
        UserMessage.new(
          user_id: i,
          name: "User#{i}",
          email: "user#{i}@example.com",
          active: rem(i, 2) == 0
        )
      end)
      
      # Convert all users to payloads
      start_time = System.monotonic_time(:millisecond)
      {:ok, payloads} = PayloadConverter.to_payloads(users)
      conversion_time = System.monotonic_time(:millisecond) - start_time
      
      # Should complete reasonably quickly (adjust threshold as needed)
      assert conversion_time < 1000 # 1 second for 100 conversions
      assert length(payloads) == 100
      
      # All should use protobuf encoding
      Enum.each(payloads, fn payload ->
        assert payload.metadata["encoding"] == "proto/binary"
      end)
    end
  end
end