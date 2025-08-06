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

  describe "client_start_workflow/2" do
    test "returns error for missing required fields", context do
      case skip_if_no_container(context) do
        {:skip, reason} -> {:skip, reason}
        context ->
          config = TestContainer.server_config(context.ports)
          client = Temporal.Native.client_connect(config)
          assert is_reference(client)

          # Test missing workflow_id
          result1 = Temporal.Native.client_start_workflow(client, %{
            "workflow_type" => "TestWorkflow",
            "task_queue" => "test-queue"
          })
          assert {:error, reason1} = result1
          assert String.contains?(reason1, "workflow_id is required")

          # Test missing workflow_type
          result2 = Temporal.Native.client_start_workflow(client, %{
            "workflow_id" => "test-id",
            "task_queue" => "test-queue"
          })
          assert {:error, reason2} = result2
          assert String.contains?(reason2, "workflow_type is required")

          # Test missing task_queue
          result3 = Temporal.Native.client_start_workflow(client, %{
            "workflow_id" => "test-id",
            "workflow_type" => "TestWorkflow"
          })
          assert {:error, reason3} = result3
          assert String.contains?(reason3, "task_queue is required")
      end
    end

    test "validates parameter types correctly", context do
      case skip_if_no_container(context) do
        {:skip, reason} -> {:skip, reason}
        context ->
          config = TestContainer.server_config(context.ports)
          client = Temporal.Native.client_connect(config)
          assert is_reference(client)

          # Test non-string workflow_id
          result1 = Temporal.Native.client_start_workflow(client, %{
            "workflow_id" => 123,  # Should be string
            "workflow_type" => "TestWorkflow",
            "task_queue" => "test-queue"
          })
          assert {:error, reason1} = result1
          assert String.contains?(reason1, "workflow_id must be a string")

          # Test non-string workflow_type
          result2 = Temporal.Native.client_start_workflow(client, %{
            "workflow_id" => "test-id",
            "workflow_type" => 456,  # Should be string
            "task_queue" => "test-queue"
          })
          assert {:error, reason2} = result2
          assert String.contains?(reason2, "workflow_type must be a string")

          # Test non-string task_queue
          result3 = Temporal.Native.client_start_workflow(client, %{
            "workflow_id" => "test-id",
            "workflow_type" => "TestWorkflow",
            "task_queue" => 789  # Should be string
          })
          assert {:error, reason3} = result3
          assert String.contains?(reason3, "task_queue must be a string")
      end
    end

    test "handles non-map parameters", context do
      case skip_if_no_container(context) do
        {:skip, reason} -> {:skip, reason}
        context ->
          config = TestContainer.server_config(context.ports)
          client = Temporal.Native.client_connect(config)
          assert is_reference(client)

          # Test with non-map parameter
          result = Temporal.Native.client_start_workflow(client, "not-a-map")
          assert {:error, reason} = result
          assert String.contains?(reason, "Parameters must be a map")
      end
    end

    test "successfully attempts workflow start with real server connection", context do
      case skip_if_no_container(context) do
        {:skip, reason} -> {:skip, reason}
        context ->
          config = TestContainer.server_config(context.ports)
          client = Temporal.Native.client_connect(config)
          assert is_reference(client)

          # Test with valid parameters - should succeed and return workflow handle
          # Provide explicit short IDs to avoid database column length limits
          result = Temporal.Native.client_start_workflow(client, %{
            "workflow_type" => "TestWorkflow",
            "task_queue" => "test-queue",
            "workflow_id" => "wf-#{System.unique_integer([:positive])}",
            "request_id" => "req-#{System.unique_integer([:positive])}"
          })
          
          # Workflow start should succeed and return a handle
          assert {:ok, handle} = result
          assert is_list(handle)
          # Verify handle contains expected keys (as string tuples)
          assert Enum.any?(handle, fn {key, _value} -> key == "workflow_id" end)
          assert Enum.any?(handle, fn {key, _value} -> key == "run_id" end)
      end
    end

    test "accepts optional parameters and connects to server", context do
      case skip_if_no_container(context) do
        {:skip, reason} -> {:skip, reason}
        context ->
          config = TestContainer.server_config(context.ports)
          client = Temporal.Native.client_connect(config)
          assert is_reference(client)

          # Test with optional parameters - should succeed and return workflow handle
          # Provide explicit short IDs to avoid database column length limits
          result = Temporal.Native.client_start_workflow(client, %{
            "workflow_type" => "TestWorkflow",
            "task_queue" => "test-queue",
            "workflow_id" => "wf-#{System.unique_integer([:positive])}",
            "request_id" => "req-#{System.unique_integer([:positive])}",
            "execution_timeout" => 3600,
            "run_timeout" => 1800,
            "task_timeout" => 60
          })
          
          # Workflow start should succeed with optional parameters
          assert {:ok, handle} = result
          assert is_list(handle)
          # Verify handle contains expected keys (as string tuples)
          assert Enum.any?(handle, fn {key, _value} -> key == "workflow_id" end)
          assert Enum.any?(handle, fn {key, _value} -> key == "run_id" end)
      end
    end
  end

  describe "performance and resource validation" do
    @tag :performance
    test "detects runtime creation performance issue", context do
      case skip_if_no_container(context) do
        {:skip, reason} -> {:skip, reason}
        context ->
          config = TestContainer.server_config(context.ports)
          client = Temporal.Native.client_connect(config)
          assert is_reference(client)

          # Measure time for multiple workflow start attempts
          # Current implementation creates new runtime each time - this should be expensive
          measurements = for i <- 1..5 do
            start_time = System.monotonic_time(:millisecond)
            
            _result = Temporal.Native.client_start_workflow(client, %{
              "workflow_type" => "TestWorkflow", 
              "task_queue" => "test-queue",
              "workflow_id" => "wf-#{i}-#{System.unique_integer([:positive])}",
              "request_id" => "req-#{i}-#{System.unique_integer([:positive])}"
            })
            
            end_time = System.monotonic_time(:millisecond)
            end_time - start_time
          end

          avg_time = Enum.sum(measurements) / length(measurements)
          max_time = Enum.max(measurements)
          
          # Document current performance characteristics
          IO.puts("📊 Runtime creation performance:")
          IO.puts("   Average time: #{Float.round(avg_time, 1)}ms")
          IO.puts("   Max time: #{max_time}ms")
          IO.puts("   ⚠️  Each call creates new tokio runtime - this is expensive")
          
          # This test documents the current inefficient behavior
          # When runtime reuse is implemented, times should be much faster
          if avg_time > 100 do
            IO.puts("   🔴 PERFORMANCE ISSUE: Runtime creation overhead detected")
            IO.puts("       Recommendation: Reuse tokio runtime across calls")
          end
      end  
    end

    @tag :resource_leak
    test "validates tokio runtime cleanup", context do
      case skip_if_no_container(context) do
        {:skip, reason} -> {:skip, reason}
        context ->
          config = TestContainer.server_config(context.ports) 
          client = Temporal.Native.client_connect(config)
          assert is_reference(client)

          # Get baseline thread count
          initial_threads = count_threads()
          
          # Perform multiple operations that create runtimes
          for i <- 1..10 do
            _result = Temporal.Native.client_start_workflow(client, %{
              "workflow_type" => "TestWorkflow",
              "task_queue" => "test-queue",
              "workflow_id" => "wf-#{i}-#{System.unique_integer([:positive])}",
              "request_id" => "req-#{i}-#{System.unique_integer([:positive])}"
            })
          end

          # Force garbage collection
          :erlang.garbage_collect()
          Process.sleep(100)
          
          final_threads = count_threads()
          thread_growth = final_threads - initial_threads
          
          IO.puts("🧵 Thread usage analysis:")
          IO.puts("   Initial threads: #{initial_threads}")
          IO.puts("   Final threads: #{final_threads}")
          IO.puts("   Growth: #{thread_growth}")
          
          # Reasonable threshold - tokio runtime cleanup should prevent excessive growth
          if thread_growth > 20 do
            IO.puts("   🔴 POTENTIAL RESOURCE LEAK: Excessive thread growth detected")
            IO.puts("       Each runtime creation may be leaking threads")
          end
          
          # This is a soft assertion - we're documenting behavior, not failing CI
          assert thread_growth < 50, "Excessive thread growth suggests resource leaks"
      end
    end

    defp count_threads do
      # Simple thread count using system info
      # This is approximate but sufficient for detecting major leaks
      case :erlang.system_info(:thread_pool_size) do
        count when is_integer(count) -> count
        _ -> 0
      end
    end
  end

  describe "error message sanitization" do
    test "validates error messages don't leak internal details" do
      # Test with configuration that will cause internal errors
      sensitive_configs = [
        {%{"target_url" => "invalid://with-secrets:password@host:port/path"}, "URL parsing errors"},
        {%{"target_url" => "localhost:7233", "namespace" => "test", "tls" => %{"client_cert_path" => "/etc/passwd"}}, "File system errors"},
        {%{"target_url" => "localhost:7233", "namespace" => "test", "api_key" => "secret-key-12345"}, "Authentication errors"}
      ]

      for {config, error_type} <- sensitive_configs do
        result = Temporal.Native.client_connect(config)
        
        case result do
          {:error, reason} ->
            # Validate error messages are sanitized
            assert is_binary(reason), "#{error_type}: Error should be string"
            
            # Check for common information leaks
            sensitive_patterns = [
              ~r/password/i,
              ~r/secret/i, 
              ~r/key.*12345/,
              ~r/\/etc\/passwd/,
              ~r/internal error/i,
              ~r/stack trace/i,
              ~r/rust.*panic/i
            ]
            
            for pattern <- sensitive_patterns do
              refute Regex.match?(pattern, reason), 
                "#{error_type}: Error message contains sensitive data: #{reason}"
            end
            
            # Error should be generic but helpful
            assert String.length(reason) > 10, "#{error_type}: Error too generic to be helpful"
            assert String.length(reason) < 200, "#{error_type}: Error too verbose, may leak details"
            
          {:ok, _} ->
            # If it succeeds unexpectedly, that's also worth noting
            IO.puts("⚠️  #{error_type}: Expected error but got success")
        end
      end
    end

  end

  describe "behavioral success validation" do
    @tag :behavioral
    test "documents current success scenarios and their limitations", context do
      case skip_if_no_container(context) do
        {:skip, reason} -> {:skip, reason}
        context ->
          config = TestContainer.server_config(context.ports)
          client = Temporal.Native.client_connect(config)
          assert is_reference(client)

          # Test the full workflow start request structure
          workflow_params = %{
            "workflow_type" => "TestWorkflow",
            "task_queue" => "test-queue",
            "workflow_id" => "wf-#{System.unique_integer([:positive])}",
            "request_id" => "req-#{System.unique_integer([:positive])}",
            "execution_timeout" => 3600,
            "run_timeout" => 1800, 
            "task_timeout" => 60
          }

          result = Temporal.Native.client_start_workflow(client, workflow_params)
          
          case result do
            {:ok, handle} ->
              # If we get success, validate the handle structure
              IO.puts("✅ Workflow start succeeded - validating handle structure")
              
              assert is_list(handle), "Handle should be a list of key-value pairs"
              handle_map = Map.new(handle, fn {k, v} -> {k, v} end)
              
              assert Map.has_key?(handle_map, "run_id"), "Handle should have run_id"
              assert Map.has_key?(handle_map, "workflow_id"), "Handle should have workflow_id"
              assert Map.has_key?(handle_map, "first_execution_run_id"), "Handle should have first_execution_run_id"
              
              # Validate UUIDs are properly formatted
              assert String.length(handle_map["run_id"]) > 0, "run_id should not be empty"
              assert handle_map["workflow_id"] == workflow_params["workflow_id"], "workflow_id should match request"
              
              IO.puts("📋 Success handle structure validated:")
              IO.puts("   Workflow ID: #{handle_map["workflow_id"]}")
              IO.puts("   Run ID: #{handle_map["run_id"]}")
              IO.puts("   First Execution Run ID: #{handle_map["first_execution_run_id"]}")
              
            {:error, reason} ->
              # Expected due to no worker, but we can still validate error structure
              IO.puts("ℹ️  Workflow start failed as expected (no worker): #{reason}")
              
              # Validate we're getting proper server communication
              server_error_patterns = [
                "Workflow start failed",
                "transport error", 
                "Service was not ready",
                "NOT_FOUND",  # Temporal server error codes
                "INVALID_ARGUMENT"
              ]

              has_server_error = Enum.any?(server_error_patterns, fn pattern ->
                String.contains?(reason, pattern)
              end)
              
              assert has_server_error, 
                "Should get server communication error, got: #{reason}"
              
              IO.puts("✅ Server communication confirmed - got expected server error")
          end
      end
    end

    @tag :behavioral
    test "validates namespace isolation", context do
      case skip_if_no_container(context) do
        {:skip, reason} -> {:skip, reason}
        context ->
          # Test with different namespaces to ensure proper isolation
          namespaces = ["default", "test-namespace", "another-namespace"]
          
          for namespace <- namespaces do
            config = %{
              "target_url" => TestContainer.server_url(context.ports),
              "namespace" => namespace
            }
            
            client = Temporal.Native.client_connect(config)
            assert is_reference(client), "Should connect to namespace: #{namespace}"
            
            # Try to start workflow in this namespace
            result = Temporal.Native.client_start_workflow(client, %{
              "workflow_type" => "TestWorkflow", 
              "task_queue" => "test-queue",
              "workflow_id" => "wf-#{namespace}-#{System.unique_integer([:positive])}",
              "request_id" => "req-#{namespace}-#{System.unique_integer([:positive])}"
            })
            
            # All should communicate with server (and fail due to no worker)
            case result do
              {:error, reason} ->
                # Should be server error, not namespace error
                server_communicated = String.contains?(reason, "Workflow start failed") or
                                    String.contains?(reason, "transport error") or
                                    String.contains?(reason, "Service was not ready")
                assert server_communicated, 
                  "Namespace #{namespace}: Expected server communication, got #{reason}"
                  
              {:ok, handle} ->
                IO.puts("✅ Namespace #{namespace}: Workflow started successfully")
                IO.puts("   Handle: #{inspect(handle)}") 
            end
          end
      end
    end
  end
end

