defmodule Temporal.Client.Config do
  @enforce_keys [:host, :namespace, :task_queue]
  defstruct host: "localhost:7233",
            namespace: "default",
            task_queue: "default",
            tls: nil,
            retries: %{max_attempts: 3, initial_backoff_ms: 100, max_backoff_ms: 5000},
            identity: nil,
            headers: %{}

  @type tls_t :: %{
          optional(:ca_cert) => String.t(),
          optional(:client_cert) => String.t(),
          optional(:client_key) => String.t(),
          optional(:server_name) => String.t(),
          optional(:insecure_skip_verify) => boolean()
        }
  @type retries_t :: %{
          optional(:max_attempts) => non_neg_integer(),
          optional(:initial_backoff_ms) => non_neg_integer(),
          optional(:max_backoff_ms) => non_neg_integer()
        }

  @type t :: %__MODULE__{
          host: String.t(),
          namespace: String.t(),
          task_queue: String.t(),
          tls: nil | tls_t,
          retries: retries_t,
          identity: nil | String.t(),
          headers: map()
        }

  @spec default() :: t()
  def default,
    do: %__MODULE__{host: "localhost:7233", namespace: "default", task_queue: "default"}

  @spec from_env() :: t()
  def from_env do
    headers =
      System.get_env()
      |> Enum.filter(fn {k, _} -> String.starts_with?(k, "TEMPORAL_HEADER_") end)
      |> Enum.reduce(%{}, fn {k, v}, acc ->
        key = k |> String.replace_prefix("TEMPORAL_HEADER_", "") |> String.downcase()
        Map.put(acc, key, v)
      end)

    tls =
      with ca <- System.get_env("TEMPORAL_TLS_CA"),
           cert <- System.get_env("TEMPORAL_TLS_CERT"),
           key <- System.get_env("TEMPORAL_TLS_KEY"),
           server <- System.get_env("TEMPORAL_TLS_SERVER_NAME"),
           insecure <- parse_bool(System.get_env("TEMPORAL_TLS_INSECURE_SKIP_VERIFY")) do
        maybe = %{
          ca_cert: ca && maybe_read_pem_or_path(ca),
          client_cert: cert && maybe_read_pem_or_path(cert),
          client_key: key && maybe_read_pem_or_path(key),
          server_name: server,
          insecure_skip_verify: insecure
        }

        if Enum.any?(maybe, fn {_k, v} -> v not in [nil, ""] end), do: maybe, else: nil
      end

    %__MODULE__{
      host: System.get_env("TEMPORAL_HOST") || "localhost:7233",
      namespace: System.get_env("TEMPORAL_NAMESPACE") || "default",
      task_queue: System.get_env("TEMPORAL_TASK_QUEUE") || "default",
      tls: tls,
      retries: %{
        max_attempts: parse_int(System.get_env("TEMPORAL_RETRY_MAX_ATTEMPTS"), 3),
        initial_backoff_ms: parse_int(System.get_env("TEMPORAL_RETRY_INITIAL_BACKOFF_MS"), 100),
        max_backoff_ms: parse_int(System.get_env("TEMPORAL_RETRY_MAX_BACKOFF_MS"), 5000)
      },
      identity: System.get_env("TEMPORAL_IDENTITY"),
      headers: headers
    }
  end

  @spec validate(t()) :: {:ok, t()} | {:error, [term()]}
  def validate(%__MODULE__{} = cfg) do
    cfg = normalize(cfg)

    errs = []
    errs = if blank?(cfg.host), do: [{:host, :required} | errs], else: errs
    errs = if blank?(cfg.namespace), do: [{:namespace, :required} | errs], else: errs
    errs = if blank?(cfg.task_queue), do: [{:task_queue, :required} | errs], else: errs

    retries =
      Map.merge(
        %{max_attempts: 3, initial_backoff_ms: 100, max_backoff_ms: 5000},
        cfg.retries || %{}
      )

    errs =
      errs
      |> check_nonneg({:retries, :max_attempts}, retries.max_attempts)
      |> check_nonneg({:retries, :initial_backoff_ms}, retries.initial_backoff_ms)
      |> check_nonneg({:retries, :max_backoff_ms}, retries.max_backoff_ms)

    cfg = %__MODULE__{cfg | retries: retries}

    errs =
      case cfg.tls do
        nil ->
          errs

        m when is_map(m) ->
          if m[:insecure_skip_verify] in [true, false] or is_nil(m[:insecure_skip_verify]),
            do: errs,
            else: [{:tls, :invalid_boolean} | errs]

        _ ->
          [{:tls, :invalid} | errs]
      end

    if errs == [], do: {:ok, cfg}, else: {:error, Enum.reverse(errs)}
  end

  @spec maybe_read_pem_or_path(String.t()) :: String.t()
  def maybe_read_pem_or_path(val) when is_binary(val) do
    path? = String.contains?(val, "/") or String.contains?(val, "\\")

    if path? and File.exists?(val) and File.regular?(val) do
      case File.read(val) do
        {:ok, content} -> content
        _ -> val
      end
    else
      val
    end
  end

  defp normalize(%__MODULE__{} = cfg) do
    %__MODULE__{
      cfg
      | host: cfg.host |> to_string() |> String.trim(),
        namespace: cfg.namespace |> to_string() |> String.trim() |> String.downcase(),
        task_queue: cfg.task_queue |> to_string() |> String.trim(),
        identity: (cfg.identity && String.trim(cfg.identity)) || cfg.identity,
        headers: cfg.headers || %{}
    }
  end

  defp blank?(v), do: v in [nil, ""]

  defp check_nonneg(errs, _key, v) when is_integer(v) and v >= 0, do: errs
  defp check_nonneg(errs, key, _v), do: [{key, :non_negative_integer_required} | errs]

  defp parse_int(nil, default), do: default

  defp parse_int(str, default) do
    case Integer.parse(str) do
      {i, _} when i >= 0 -> i
      _ -> default
    end
  end

  defp parse_bool(nil), do: nil

  defp parse_bool(v) when is_binary(v) do
    case String.downcase(String.trim(v)) do
      "true" -> true
      "1" -> true
      "false" -> false
      "0" -> false
      _ -> nil
    end
  end
end
