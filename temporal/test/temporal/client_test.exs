defmodule Temporal.ClientTest do
  use ExUnit.Case, async: false
  import ExUnit.Case

  alias Temporal.TestContainer

  @moduletag :integration
  @moduletag timeout: 60_000

  setup_all do
    case TestContainer.start_container() do
      {:ok, container, ports} ->
        # Don't stop container here - let it persist for all test runs
        {:ok, %{container: container, ports: ports}}

      {:error, reason} ->
        IO.puts("⚠️  Skipping tests: Could not start Temporal container: #{inspect(reason)}")
        IO.puts("   Make sure Docker is installed and running")
        {:skip, "Docker container failed to start: #{inspect(reason)}"}
    end
  end

  # Helper function following BEAM patterns - returns :skip tuple for modern ExUnit
  defp skip_if_no_container(context) do
    case context do
      %{ports: _ports} -> context
      _ -> {:skip, "Docker container not available"}
    end
  end

  describe "client_connect/1" do
    test "successfully connects to running Temporal server", context do
      case skip_if_no_container(context) do
        {:skip, reason} -> {:skip, reason}
        context -> 
          config = TestContainer.server_config(context.ports)

      result = Temporal.Native.client_connect(config)

          # Should return a valid resource reference when server is running
          assert is_reference(result)
          refute match?({:error, _}, result)
      end
    end

    test "connects with default config pointing to test server", context do
      case skip_if_no_container(context) do
        {:skip, reason} -> {:skip, reason}
        context ->
          # Override default to point to our test server
          config = %{
            "target_url" => TestContainer.server_url(context.ports),
            "namespace" => "default"
          }

          result = Temporal.Native.client_connect(config)

          assert is_reference(result)
      end
    end

    test "connects with custom namespace", context do
      case skip_if_no_container(context) do
        {:skip, reason} -> {:skip, reason}
        context ->
          config = %{
            "target_url" => TestContainer.server_url(context.ports),
            "namespace" => "test-namespace"
          }

          result = Temporal.Native.client_connect(config)

          assert is_reference(result)
      end
    end

    test "returns error for missing required fields" do
      # Test missing target_url
      result1 = Temporal.Native.client_connect(%{"namespace" => "default"})
      assert {:error, reason1} = result1
      assert String.contains?(reason1, "target_url is required")

      # Test missing namespace
      result2 = Temporal.Native.client_connect(%{"target_url" => "localhost:7233"})
      assert {:error, reason2} = result2
      assert String.contains?(reason2, "namespace is required")
    end

    test "returns error for invalid URL format" do
      # This test doesn't need Docker - testing NIF error handling
      invalid_config = %{
        "target_url" => "invalid-url-format",
        "namespace" => "default"
      }

      result = Temporal.Native.client_connect(invalid_config)

      assert {:error, reason} = result
      assert is_binary(reason)
      assert String.contains?(reason, "Invalid URL")
    end

    test "handles TLS configuration parsing" do
      # Test with invalid TLS config
      invalid_tls_config = %{
        "target_url" => "localhost:7233",
        "namespace" => "default",
        "tls" => "not-a-map"
      }

      result = Temporal.Native.client_connect(invalid_tls_config)
      assert {:error, reason} = result
      assert String.contains?(reason, "TLS configuration must be a map")
    end

    test "accepts valid TLS configuration structure" do
      # Test with valid TLS config structure (will fail to connect since certs don't exist)
      valid_tls_config = %{
        "target_url" => "localhost:7233",
        "namespace" => "default",
        "tls" => %{
          "client_cert_path" => "/nonexistent/cert.pem",
          "client_key_path" => "/nonexistent/key.pem",
          "ca_cert_path" => "/nonexistent/ca.pem"
        }
      }

      result = Temporal.Native.client_connect(valid_tls_config)
      # Should fail with file read error, not config parsing error
      assert {:error, reason} = result
      assert String.contains?(reason, "Failed to read")
    end
  end

  describe "client resource management" do
    test "can create multiple client connections", context do
      case skip_if_no_container(context) do
        {:skip, reason} -> {:skip, reason}
        context ->
          config = TestContainer.server_config(context.ports)

          client1 = Temporal.Native.client_connect(config)
          client2 = Temporal.Native.client_connect(config)

          assert is_reference(client1)
          assert is_reference(client2)
          # Should be different references
          refute client1 == client2
      end
    end

    test "handles concurrent connection attempts", context do
      case skip_if_no_container(context) do
        {:skip, reason} -> {:skip, reason}
        context ->
          config = TestContainer.server_config(context.ports)

          # Spawn 10 concurrent connection attempts
          tasks = for i <- 1..10 do
            Task.async(fn ->
              result = Temporal.Native.client_connect(config)
              {i, result}
            end)
          end

          results = Task.await_many(tasks, 10_000)

          # All should succeed
          for {i, result} <- results do
            assert is_reference(result), "Connection #{i} failed: #{inspect(result)}"
          end
      end
    end

    @tag :slow
    test "client references are properly garbage collected", context do
      case skip_if_no_container(context) do
        {:skip, reason} -> {:skip, reason}
        context ->
          config = TestContainer.server_config(context.ports)

          # Get initial memory baseline before creating resources
          # Multiple samples for stability in CI environments
          _warm_up = :erlang.garbage_collect()
          Process.sleep(50)
          initial_memory = :erlang.memory(:total)

          # Create clients in isolated process following BEAM resource testing patterns
          # This ensures references go completely out of scope
          parent_pid = self()
          test_pid = spawn_link(fn ->
            clients = for _i <- 1..50 do  # Reasonable count for resource testing
              client = Temporal.Native.client_connect(config)
              assert is_reference(client)
              client
            end
            
            # Verify clients were created and send confirmation
            send(parent_pid, {:clients_created, length(clients)})
            
            # Hold references until explicitly released
            receive do
              :release -> :ok
            end
            # clients go out of scope here
          end)

          # Wait for client creation confirmation
          assert_receive {:clients_created, 50}, 10_000

          # Monitor the process before signaling release to avoid race conditions
          ref = Process.monitor(test_pid)
          
          # Signal process to release all client references
          send(test_pid, :release)
          
          # Wait for process termination to ensure resource cleanup
          # Accept both :normal and :noproc (process already dead)
          assert_receive {:DOWN, ^ref, :process, ^test_pid, reason}, 5_000
          assert reason in [:normal, :noproc]

          # Force garbage collection following BEAM/NIF resource cleanup patterns
          # Multiple GC passes to handle potential reference cycles
          :erlang.garbage_collect()  # Current process
          :erlang.garbage_collect()  # Second pass for completeness
          Process.sleep(100)  # Allow async NIF resource destructors to complete

          # Measure final memory state
          final_memory = :erlang.memory(:total)
          memory_growth = final_memory - initial_memory
          growth_percentage = memory_growth / initial_memory * 100

          # Assert memory growth is within acceptable bounds (20% for CI stability)
          # NIF resources should be properly cleaned up by Rustler
          assert memory_growth < initial_memory * 0.20,
                 "Potential resource leak detected: memory grew by #{memory_growth} bytes (#{Float.round(growth_percentage, 1)}%). " <>
                 "Initial: #{initial_memory}, Final: #{final_memory}. " <>
                 "Check NIF resource cleanup in ClientResource."
      end
    end

    test "client handles network errors gracefully" do
      # Test error handling without requiring Docker
      config = %{
        "target_url" => "localhost:99999",  # Non-existent port
        "namespace" => "default"
      }

      result = Temporal.Native.client_connect(config)

      assert {:error, reason} = result
      assert is_binary(reason)
      assert String.contains?(reason, "Connection failed")
    end
  end
end

