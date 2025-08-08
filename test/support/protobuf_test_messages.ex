defmodule Temporal.Test.ProtobufTestMessages do
  @moduledoc """
  Test protobuf message definitions for testing protobuf converter.
  """

  # Simple test message
  defmodule TestMessage do
    use Protobuf, syntax: :proto3
    
    field :user_id, 1, type: :int64
    field :name, 2, type: :string
    field :email, 3, type: :string
    field :active, 4, type: :bool

    # Add helper function for creating messages
    def new(attrs \\ %{}) do
      struct(__MODULE__, attrs)
    end
  end

  # Nested test message
  defmodule NestedMessage do
    use Protobuf, syntax: :proto3
    
    field :id, 1, type: :int32
    field :user, 2, type: TestMessage
    field :tags, 3, repeated: true, type: :string

    def new(attrs \\ %{}) do
      struct(__MODULE__, attrs)
    end
  end

  # User message for integration tests
  defmodule UserMessage do
    use Protobuf, syntax: :proto3
    
    field :user_id, 1, type: :int64
    field :name, 2, type: :string
    field :email, 3, type: :string
    field :active, 4, type: :bool
    field :created_at, 5, type: :int64

    def new(attrs \\ %{}) do
      struct(__MODULE__, attrs)
    end
  end

  # Order message with nested user for integration tests
  defmodule OrderMessage do
    use Protobuf, syntax: :proto3
    
    field :order_id, 1, type: :string
    field :user, 2, type: UserMessage
    field :items, 3, repeated: true, type: :string
    field :total_amount, 4, type: :double

    def new(attrs \\ %{}) do
      struct(__MODULE__, attrs)
    end
  end
end