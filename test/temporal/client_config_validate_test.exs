defmodule Temporal.ClientConfigValidateTest do
  use ExUnit.Case, async: true
  alias Temporal.Client.Config

  test "valid default config" do
    {:ok, cfg} = Config.validate(Config.default())
    assert cfg.host == "localhost:7233"
    assert cfg.namespace == "default"
    assert cfg.task_queue == "default"
  end

  test "required fields must be present" do
    {:error, errs} = Config.validate(%Config{host: "", namespace: "", task_queue: ""})
    assert {:host, :required} in errs
    assert {:namespace, :required} in errs
    assert {:task_queue, :required} in errs
  end

  test "normalizes strings and downcases namespace" do
    {:ok, cfg} = Config.validate(%Config{host: " host ", namespace: " DEFAULT ", task_queue: " tq "})
    assert cfg.host == "host"
    assert cfg.namespace == "default"
    assert cfg.task_queue == "tq"
  end

  test "retries must be non-negative" do
    bad = %Config{host: "h", namespace: "n", task_queue: "q", retries: %{max_attempts: -1, initial_backoff_ms: -10, max_backoff_ms: -5}}
    {:error, errs} = Config.validate(bad)

    assert {{:retries, :max_attempts}, :non_negative_integer_required} in errs
    assert {{:retries, :initial_backoff_ms}, :non_negative_integer_required} in errs
    assert {{:retries, :max_backoff_ms}, :non_negative_integer_required} in errs
  end

  test "tls invalid type" do
    {:error, errs} = Config.validate(%Config{host: "h", namespace: "n", task_queue: "q", tls: :bad})
    assert {:tls, :invalid} in errs
  end

  test "tls insecure_skip_verify must be boolean if present" do
    {:error, errs} = Config.validate(%Config{host: "h", namespace: "n", task_queue: "q", tls: %{insecure_skip_verify: "nope"}})
    assert {:tls, :invalid_boolean} in errs

    {:ok, _cfg} = Config.validate(%Config{host: "h", namespace: "n", task_queue: "q", tls: %{insecure_skip_verify: true}})
  end

  test "from_env builds headers and tls" do
    System.put_env("TEMPORAL_HEADER_FOO", "bar")
    System.put_env("TEMPORAL_TLS_CA", "PEMCA")
    System.put_env("TEMPORAL_TLS_CERT", "PEMCERT")
    System.put_env("TEMPORAL_TLS_KEY", "PEMKEY")
    System.put_env("TEMPORAL_TLS_SERVER_NAME", "example")

    cfg = Config.from_env()
    assert cfg.headers["foo"] == "bar"
    assert is_map(cfg.tls)
    assert cfg.tls[:server_name] == "example"

    System.delete_env("TEMPORAL_HEADER_FOO")
    System.delete_env("TEMPORAL_TLS_CA")
    System.delete_env("TEMPORAL_TLS_CERT")
    System.delete_env("TEMPORAL_TLS_KEY")
    System.delete_env("TEMPORAL_TLS_SERVER_NAME")
  end
end
