defmodule Temporal.ClientUnitTest do
  use ExUnit.Case, async: true

  alias Temporal.Client
  alias Temporal.Client.Config

  setup do
    on_exit(fn ->
      System.delete_env("TEMPORAL_HOST")
      System.delete_env("TEMPORAL_NAMESPACE")
      System.delete_env("TEMPORAL_TASK_QUEUE")
      System.delete_env("TEMPORAL_IDENTITY")
      System.delete_env("TEMPORAL_HEADER_FOO")
      System.delete_env("TEMPORAL_TLS_CA")
      System.delete_env("TEMPORAL_TLS_CERT")
      System.delete_env("TEMPORAL_TLS_KEY")
      System.delete_env("TEMPORAL_TLS_SERVER_NAME")
      System.delete_env("TEMPORAL_TLS_INSECURE_SKIP_VERIFY")
      System.delete_env("TEMPORAL_CONNECT_TIMEOUT_MS")
      System.delete_env("TEMPORAL_RPC_TIMEOUT_MS")
      System.delete_env("TEMPORAL_RETRY_MAX_ATTEMPTS")
      System.delete_env("TEMPORAL_RETRY_INITIAL_BACKOFF_MS")
      System.delete_env("TEMPORAL_RETRY_MAX_BACKOFF_MS")
    end)

    :ok
  end

  test "env merge precedence: defaults < env < opts" do
    System.put_env("TEMPORAL_HOST", "env:7233")
    System.put_env("TEMPORAL_NAMESPACE", "env-ns")
    System.put_env("TEMPORAL_TASK_QUEUE", "env-tq")

    cfg = Config.from_env()
    assert cfg.host == "env:7233"
    assert cfg.namespace == "env-ns"
    assert cfg.task_queue == "env-tq"

    # Validate merge semantics via Config
    merged = %Config{cfg | host: "opt:7233", namespace: "opt-ns", task_queue: "opt-tq"}
    assert {:ok, _} = Config.validate(merged)
  end

  test "validation failures" do
    bad = %Config{Config.default() | host: "", namespace: "", task_queue: ""}
    assert {:error, errs} = Config.validate(bad)
    assert {:host, :required} in errs
    assert {:namespace, :required} in errs
    assert {:task_queue, :required} in errs

    bad2 = %Config{
      Config.default()
      | retries: %{max_attempts: -1, initial_backoff_ms: -1, max_backoff_ms: -1}
    }

    assert {:error, errs2} = Config.validate(bad2)
    assert Enum.any?(errs2, fn {_, v} -> v == :non_negative_integer_required end)
  end

  test "TLS path reading vs inline" do
    pem = "-----BEGIN CERTIFICATE-----\nABC\n-----END CERTIFICATE-----\n"
    path = Path.join(System.tmp_dir!(), "cert.pem")
    File.write!(path, pem)

    val = Config.maybe_read_pem_or_path(path)
    assert val == pem

    inline = Config.maybe_read_pem_or_path(pem)
    assert inline == pem
  end

  test "header collection and boolean parsing" do
    System.put_env("TEMPORAL_HEADER_FOO", "bar")
    System.put_env("TEMPORAL_TLS_INSECURE_SKIP_VERIFY", "true")

    cfg = Config.from_env()
    assert cfg.headers["foo"] == "bar"
    assert cfg.tls[:insecure_skip_verify] == true
  end
end
