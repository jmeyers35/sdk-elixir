#![allow(clippy::uninlined_format_args)] // Will be addressed in future cleanup

use rustler::{Encoder, Env, NifResult, ResourceArc, Term};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

mod client;
mod converter;
mod worker;

use client::{
    ClientOptions, ClientResource, ClientTlsConfig, WorkflowQueryParams, WorkflowSignalParams,
    WorkflowStartParams,
};
use worker::WorkerResource;

/// Global shared runtime for all Temporal operations
/// This prevents resource exhaustion from creating multiple runtimes
static SHARED_RUNTIME: OnceLock<Arc<Runtime>> = OnceLock::new();

/// Get or create the shared Tokio runtime
/// This ensures we have exactly one runtime per BEAM instance
fn get_runtime() -> &'static Arc<Runtime> {
    SHARED_RUNTIME.get_or_init(|| {
        Arc::new(
            Runtime::new()
                .expect("Failed to create shared Tokio runtime - this is a critical error"),
        )
    })
}

/// Sanitize polling errors to avoid information disclosure
fn sanitize_polling_error(error: &str) -> String {
    // Map specific error patterns to safe messages
    if error.contains("not running") {
        "Worker not running".to_string()
    } else if error.contains("timeout") {
        "Operation timeout".to_string()
    } else if error.contains("connection") {
        "Connection error".to_string()
    } else {
        "Polling error".to_string()
    }
}

/// Convert Elixir payload map to Rust Payload struct
fn term_to_payload(
    term: &rustler::Term,
) -> Result<temporal_sdk_core_protos::temporal::api::common::v1::Payload, String> {
    use temporal_sdk_core_protos::temporal::api::common::v1::Payload;

    // Convert Elixir payload map with atom keys to Rust Payload

    // Try to decode as a map with atom keys (Elixir default)
    if let Ok(payload_map) =
        term.decode::<std::collections::HashMap<rustler::types::atom::Atom, rustler::Term>>()
    {
        // Successfully decoded Elixir map with atom keys

        // Find keys by comparing atom string representation
        let mut metadata_term = None;
        let mut data_term = None;

        for (atom_key, term_val) in payload_map.iter() {
            // Use the debug string representation and strip the : prefix
            let key_debug = format!("{:?}", atom_key);
            let key_str = key_debug.strip_prefix(':').unwrap_or(&key_debug);
            if key_str == "metadata" {
                metadata_term = Some(term_val);
            } else if key_str == "data" {
                data_term = Some(term_val);
            }
        }

        // Extract metadata (required)
        let metadata = if let Some(metadata_term) = metadata_term {
            // Try different metadata formats - Elixir uses binary keys/values
            if let Ok(metadata_map) = metadata_term.decode::<HashMap<Vec<u8>, Vec<u8>>>() {
                metadata_map
                    .into_iter()
                    .map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), v))
                    .collect()
            } else if let Ok(metadata_map) = metadata_term.decode::<HashMap<String, Vec<u8>>>() {
                metadata_map
            } else if let Ok(metadata_map) = metadata_term.decode::<HashMap<String, String>>() {
                metadata_map
                    .into_iter()
                    .map(|(k, v)| (k, v.into_bytes()))
                    .collect()
            } else {
                HashMap::new()
            }
        } else {
            HashMap::new()
        };

        // Extract data (optional, defaults to empty)
        let data = if let Some(data_term) = data_term {
            // Try to decode as rustler::Binary first (Elixir binary type)
            if let Ok(binary) = data_term.decode::<rustler::Binary>() {
                binary.as_slice().to_vec()
            } else if let Ok(binary) = data_term.decode::<Vec<u8>>() {
                binary
            } else if let Ok(string) = data_term.decode::<String>() {
                // If it's a string, convert to bytes
                string.into_bytes()
            } else {
                // Default to empty if can't decode
                Vec::new()
            }
        } else {
            Vec::new()
        };

        return Ok(Payload { metadata, data });
    }

    // Fallback: try to decode as map with string keys
    let payload_map: HashMap<String, rustler::Term> = term.decode().map_err(|_| {
        "Failed to decode payload as map (tried both atom and string keys)".to_string()
    })?;

    // Extract metadata (required)
    let metadata = if let Some(metadata_term) = payload_map.get("metadata") {
        let metadata_map: HashMap<String, Vec<u8>> = metadata_term
            .decode::<HashMap<String, String>>()
            .map_err(|_| "Failed to decode metadata as map".to_string())?
            .into_iter()
            .map(|(k, v)| (k, v.into_bytes()))
            .collect();
        metadata_map
    } else {
        HashMap::new()
    };

    // Extract data (optional, defaults to empty)
    let data = if let Some(data_term) = payload_map.get("data") {
        // Try to decode as binary
        if let Ok(binary) = data_term.decode::<Vec<u8>>() {
            binary
        } else if let Ok(string) = data_term.decode::<String>() {
            // If it's a string, convert to bytes
            string.into_bytes()
        } else {
            // Default to empty if can't decode
            Vec::new()
        }
    } else {
        Vec::new()
    };

    Ok(Payload { metadata, data })
}

/// Convert serde_json::Value back to Rustler Term
fn json_value_to_elixir_term<'a>(
    env: Env<'a>,
    value: &serde_json::Value,
) -> Result<rustler::Term<'a>, String> {
    match value {
        serde_json::Value::Null => Ok(rustler::types::atom::nil().encode(env)),
        serde_json::Value::Bool(b) => Ok(b.encode(env)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.encode(env))
            } else if let Some(f) = n.as_f64() {
                Ok(f.encode(env))
            } else {
                Err("Invalid number format".to_string())
            }
        }
        serde_json::Value::String(s) => Ok(s.encode(env)),
        serde_json::Value::Array(arr) => {
            let elixir_list: Result<Vec<rustler::Term>, String> = arr
                .iter()
                .map(|v| json_value_to_elixir_term(env, v))
                .collect();
            Ok(elixir_list?.encode(env))
        }
        serde_json::Value::Object(obj) => {
            let elixir_map: Result<std::collections::HashMap<String, rustler::Term>, String> = obj
                .iter()
                .map(|(k, v)| json_value_to_elixir_term(env, v).map(|term| (k.clone(), term)))
                .collect();
            Ok(elixir_map?.encode(env))
        }
    }
}

// Runtime is managed globally via OnceLock - no need for a separate resource

rustler::init!("Elixir.Temporal.Native", load = load);

fn load(_env: rustler::Env, _info: rustler::Term) -> bool {
    true
}

// Helper function to parse TLS configuration from Elixir map
fn parse_tls_config(
    config_map: &HashMap<String, rustler::Term>,
) -> Result<Option<ClientTlsConfig>, String> {
    if let Some(tls_term) = config_map
        .get("tls_config")
        .or_else(|| config_map.get("tls"))
    {
        let tls_map: HashMap<String, rustler::Term> = match tls_term.decode() {
            Ok(m) => m,
            Err(_) => return Ok(None),
        };
        let client_cert_path = tls_map
            .get("client_cert_path")
            .and_then(|term| term.decode::<String>().ok());
        let client_key_path = tls_map
            .get("client_key_path")
            .and_then(|term| term.decode::<String>().ok());
        let ca_cert_path = tls_map
            .get("ca_cert_path")
            .and_then(|term| term.decode::<String>().ok());
        let client_cert_inline = tls_map
            .get("client_cert")
            .and_then(|term| term.decode::<String>().ok());
        let client_key_inline = tls_map
            .get("client_key")
            .and_then(|term| term.decode::<String>().ok());
        let ca_cert_inline = tls_map
            .get("ca_cert")
            .and_then(|term| term.decode::<String>().ok());
        let (client_cert_path, client_key_path) = match (
            client_cert_path,
            client_key_path,
            client_cert_inline,
            client_key_inline,
        ) {
            (Some(cp), Some(kp), _, _) => (Some(cp), Some(kp)),
            (_, _, Some(cert), Some(key)) => {
                let cert_path = write_temp_pem("temporal_cert", &cert)?;
                let key_path = write_temp_pem("temporal_key", &key)?;
                (Some(cert_path), Some(key_path))
            }
            _ => (None, None),
        };
        let ca_cert_path = match (ca_cert_path, ca_cert_inline) {
            (Some(p), _) => Some(p),
            (None, Some(pem)) => Some(write_temp_pem("temporal_ca", &pem)?),
            _ => None,
        };
        Ok(Some(ClientTlsConfig {
            client_cert_path,
            client_key_path,
            ca_cert_path,
        }))
    } else {
        Ok(None)
    }
}

fn write_temp_pem(prefix: &str, contents: &str) -> Result<String, String> {
    use std::io::Write;
    let mut file = tempfile::Builder::new()
        .prefix(prefix)
        .suffix(".pem")
        .tempfile()
        .map_err(|e| format!("Failed to create temp file: {}", e))?;
    file.write_all(contents.as_bytes())
        .map_err(|e| format!("Failed to write temp file: {}", e))?;
    let path = file.into_temp_path();
    let path_str = path
        .to_path_buf()
        .into_os_string()
        .into_string()
        .map_err(|_| "Invalid temp path".to_string())?;
    // Keep file until process exit
    path.persist_noclobber(&path_str)
        .map_err(|e| format!("Failed to persist temp file: {}", e))?;
    Ok(path_str)
}

// Client functions
#[rustler::nif(schedule = "DirtyIo")]
fn client_connect<'a>(env: Env<'a>, config_term: Term<'a>) -> NifResult<Term<'a>> {
    // Parse configuration from Elixir term
    let config_map: HashMap<String, rustler::Term> = match config_term.decode() {
        Ok(map) => map,
        Err(_) => {
            let error_tuple = (rustler::types::atom::error(), "Configuration must be a map");
            return Ok(error_tuple.encode(env));
        }
    };

    // Extract required fields - no defaults following OSS SDK patterns
    let target_url = match config_map.get("target_host") {
        Some(term) => match term.decode::<String>() {
            Ok(url) => url,
            Err(_) => {
                let error_tuple = (
                    rustler::types::atom::error(),
                    "target_host must be a string",
                );
                return Ok(error_tuple.encode(env));
            }
        },
        None => {
            let error_tuple = (rustler::types::atom::error(), "target_host is required");
            return Ok(error_tuple.encode(env));
        }
    };

    let namespace = match config_map.get("namespace") {
        Some(term) => match term.decode::<String>() {
            Ok(ns) => ns,
            Err(_) => {
                let error_tuple = (rustler::types::atom::error(), "namespace must be a string");
                return Ok(error_tuple.encode(env));
            }
        },
        None => {
            let error_tuple = (rustler::types::atom::error(), "namespace is required");
            return Ok(error_tuple.encode(env));
        }
    };

    // Extract payload converter config
    let (converter, _conv_opts) = {
        let spec_term = config_map.get("payload_converter");
        let opts_term = config_map.get("payload_converter_options");
        let binary_max = opts_term
            .and_then(|t| {
                t.decode::<std::collections::HashMap<String, rustler::Term>>()
                    .ok()
            })
            .and_then(|m| {
                m.get("binary_max_size")
                    .and_then(|t| t.decode::<u64>().ok())
            })
            .unwrap_or(1024 * 1024) as usize;
        let json_depth = opts_term
            .and_then(|t| {
                t.decode::<std::collections::HashMap<String, rustler::Term>>()
                    .ok()
            })
            .and_then(|m| m.get("json_max_depth").and_then(|t| t.decode::<u64>().ok()))
            .unwrap_or(32) as usize;
        let mut conv = crate::converter::CompositeConverter::default();
        if let Some(spec) = spec_term {
            if let Ok(list) = spec.decode::<Vec<rustler::Term>>() {
                let mut v: Vec<Box<dyn crate::converter::PayloadConverter>> = Vec::new();
                for item in list {
                    if let Ok(s) = item.decode::<String>() {
                        match s.as_str() {
                            "nil" => v.push(Box::new(crate::converter::NilConverter)),
                            "binary" => {
                                v.push(Box::new(crate::converter::BinaryConverter::new(binary_max)))
                            }
                            "json" => v.push(Box::new(crate::converter::JsonConverter::new(
                                json_depth, true,
                            ))),
                            _ => {}
                        }
                    }
                }
                if !v.is_empty() {
                    conv = crate::converter::CompositeConverter::new(v);
                }
            }
        }
        (conv, ())
    };

    // Extract optional configuration fields
    let client_name = config_map
        .get("client_name")
        .and_then(|term| term.decode::<String>().ok());

    let client_version = config_map
        .get("client_version")
        .and_then(|term| term.decode::<String>().ok());

    let identity = config_map
        .get("identity")
        .and_then(|term| term.decode::<String>().ok());

    let api_key = config_map
        .get("api_key")
        .and_then(|term| term.decode::<String>().ok());

    let skip_system_info = config_map
        .get("skip_system_info")
        .and_then(|term| term.decode::<bool>().ok())
        .unwrap_or(true); // Default to true to avoid issues in tests

    // Parse TLS configuration
    let tls = match parse_tls_config(&config_map) {
        Ok(tls) => tls,
        Err(err) => {
            let error_tuple = (rustler::types::atom::error(), err);
            return Ok(error_tuple.encode(env));
        }
    };

    // Optional headers
    let headers = config_map
        .get("headers")
        .and_then(|term| term.decode::<HashMap<String, String>>().ok());

    // Optional retries
    let retries = config_map.get("retries").and_then(|term| {
        let m: HashMap<String, rustler::Term> = term.decode().ok()?;
        let max_attempts = m
            .get("max_attempts")
            .and_then(|t| t.decode::<u32>().ok())
            .unwrap_or(3);
        let initial_backoff_ms = m
            .get("initial_backoff_ms")
            .and_then(|t| t.decode::<u64>().ok())
            .unwrap_or(100);
        let max_backoff_ms = m
            .get("max_backoff_ms")
            .and_then(|t| t.decode::<u64>().ok())
            .unwrap_or(5_000);
        Some(client::RetryOptions {
            max_attempts,
            initial_backoff_ms,
            max_backoff_ms,
        })
    });

    // Build options struct
    let options = ClientOptions {
        tls,
        client_name,
        client_version,
        identity,
        api_key,
        skip_system_info,
        headers,
        retries,
    };

    // Use shared runtime for connection - prevents resource exhaustion
    let rt = get_runtime();

    match rt.block_on(ClientResource::connect(target_url, namespace, options)) {
        Ok(mut client) => {
            client.converter = converter;
            let resource = ResourceArc::new(client);
            Ok(resource.encode(env))
        }
        Err(err) => {
            let error_tuple = (rustler::types::atom::error(), err);
            Ok(error_tuple.encode(env))
        }
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn client_start_workflow<'a>(
    env: Env<'a>,
    client: ResourceArc<ClientResource>,
    params: Term<'a>,
) -> NifResult<Term<'a>> {
    // Parse parameters from Elixir term
    let params_map: HashMap<String, rustler::Term> = match params.decode() {
        Ok(map) => map,
        Err(_) => {
            let error_tuple = (rustler::types::atom::error(), "Parameters must be a map");
            return Ok(error_tuple.encode(env));
        }
    };

    // Extract required fields
    let workflow_id = match params_map.get("workflow_id") {
        Some(term) => match term.decode::<String>() {
            Ok(id) => id,
            Err(_) => {
                let error_tuple = (
                    rustler::types::atom::error(),
                    "workflow_id must be a string",
                );
                return Ok(error_tuple.encode(env));
            }
        },
        None => {
            let error_tuple = (rustler::types::atom::error(), "workflow_id is required");
            return Ok(error_tuple.encode(env));
        }
    };

    let workflow_type = match params_map.get("workflow_type") {
        Some(term) => match term.decode::<String>() {
            Ok(wf_type) => wf_type,
            Err(_) => {
                let error_tuple = (
                    rustler::types::atom::error(),
                    "workflow_type must be a string",
                );
                return Ok(error_tuple.encode(env));
            }
        },
        None => {
            let error_tuple = (rustler::types::atom::error(), "workflow_type is required");
            return Ok(error_tuple.encode(env));
        }
    };

    let task_queue = match params_map.get("task_queue") {
        Some(term) => match term.decode::<String>() {
            Ok(queue) => queue,
            Err(_) => {
                let error_tuple = (rustler::types::atom::error(), "task_queue must be a string");
                return Ok(error_tuple.encode(env));
            }
        },
        None => {
            let error_tuple = (rustler::types::atom::error(), "task_queue is required");
            return Ok(error_tuple.encode(env));
        }
    };

    // Extract optional fields - parse pre-converted payloads from Elixir
    let input: Option<Vec<temporal_sdk_core_protos::temporal::api::common::v1::Payload>> =
        params_map.get("input").and_then(|term| {
            // Try to decode as Vec<rustler::Term> first
            let elixir_list: Vec<rustler::Term> = term.decode().ok()?;

            // Convert each Elixir payload map to Rust Payload
            let mut payloads = Vec::new();
            for elixir_term in elixir_list.iter() {
                match term_to_payload(elixir_term) {
                    Ok(payload) => {
                        payloads.push(payload);
                    }
                    Err(_) => {
                        return None;
                    }
                }
            }
            Some(payloads)
        });

    let request_id = params_map
        .get("request_id")
        .and_then(|term| term.decode::<String>().ok());

    let execution_timeout = params_map
        .get("execution_timeout")
        .and_then(|term| term.decode::<u64>().ok());

    let run_timeout = params_map
        .get("run_timeout")
        .and_then(|term| term.decode::<u64>().ok());

    let task_timeout = params_map
        .get("task_timeout")
        .and_then(|term| term.decode::<u64>().ok());

    // Build parameters struct
    let workflow_params = WorkflowStartParams {
        workflow_id,
        workflow_type,
        task_queue,
        input,
        request_id,
        execution_timeout,
        run_timeout,
        task_timeout,
    };

    // Use shared runtime for workflow operations - prevents resource exhaustion
    let rt = get_runtime();

    // Now we can use the client with our fixed mutable access via async Mutex
    match rt.block_on(async { client.start_workflow(workflow_params).await }) {
        Ok(handle) => {
            // Convert WorkflowHandle to Elixir term
            let result_map = vec![
                ("run_id".to_string(), handle.run_id.encode(env)),
                ("workflow_id".to_string(), handle.workflow_id.encode(env)),
                (
                    "first_execution_run_id".to_string(),
                    handle.first_execution_run_id.encode(env),
                ),
            ];
            let ok_tuple = (rustler::types::atom::ok(), result_map);
            Ok(ok_tuple.encode(env))
        }
        Err(err) => {
            let error_tuple = (rustler::types::atom::error(), err);
            Ok(error_tuple.encode(env))
        }
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn client_signal_workflow<'a>(
    env: Env<'a>,
    client: ResourceArc<ClientResource>,
    params: Term<'a>,
) -> NifResult<Term<'a>> {
    // Parse parameters from Elixir term
    let params_map: HashMap<String, rustler::Term> = match params.decode() {
        Ok(map) => map,
        Err(_) => {
            let error_tuple = (rustler::types::atom::error(), "Parameters must be a map");
            return Ok(error_tuple.encode(env));
        }
    };

    // Extract required fields
    let workflow_id = match params_map.get("workflow_id") {
        Some(term) => match term.decode::<String>() {
            Ok(id) => id,
            Err(_) => {
                let error_tuple = (
                    rustler::types::atom::error(),
                    "workflow_id must be a string",
                );
                return Ok(error_tuple.encode(env));
            }
        },
        None => {
            let error_tuple = (rustler::types::atom::error(), "workflow_id is required");
            return Ok(error_tuple.encode(env));
        }
    };

    let signal_name = match params_map.get("signal_name") {
        Some(term) => match term.decode::<String>() {
            Ok(name) => name,
            Err(_) => {
                let error_tuple = (
                    rustler::types::atom::error(),
                    "signal_name must be a string",
                );
                return Ok(error_tuple.encode(env));
            }
        },
        None => {
            let error_tuple = (rustler::types::atom::error(), "signal_name is required");
            return Ok(error_tuple.encode(env));
        }
    };

    // Extract optional fields
    let run_id = params_map
        .get("run_id")
        .and_then(|term| term.decode::<String>().ok());

    let namespace = params_map
        .get("namespace")
        .and_then(|term| term.decode::<String>().ok());

    // Parse pre-converted payloads from Elixir
    let input: Option<Vec<temporal_sdk_core_protos::temporal::api::common::v1::Payload>> =
        params_map.get("input").and_then(|term| {
            let elixir_list: Vec<rustler::Term> = term.decode().ok()?;
            let payloads: Result<Vec<_>, _> = elixir_list
                .iter()
                .map(|elixir_term| term_to_payload(elixir_term))
                .collect();
            payloads.ok()
        });

    // Build parameters struct
    let signal_params = WorkflowSignalParams {
        workflow_id,
        run_id,
        signal_name,
        input,
        namespace,
    };

    // Use shared runtime for signal operation
    let rt = get_runtime();

    match rt.block_on(async { client.signal_workflow(signal_params).await }) {
        Ok(()) => Ok(rustler::types::atom::ok().encode(env)),
        Err(err) => {
            let error_tuple = (rustler::types::atom::error(), err);
            Ok(error_tuple.encode(env))
        }
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn client_query_workflow<'a>(
    env: Env<'a>,
    client: ResourceArc<ClientResource>,
    params: Term<'a>,
) -> NifResult<Term<'a>> {
    // Parse parameters from Elixir term
    let params_map: HashMap<String, rustler::Term> = match params.decode() {
        Ok(map) => map,
        Err(_) => {
            let error_tuple = (rustler::types::atom::error(), "Parameters must be a map");
            return Ok(error_tuple.encode(env));
        }
    };

    // Extract required fields
    let workflow_id = match params_map.get("workflow_id") {
        Some(term) => match term.decode::<String>() {
            Ok(id) => id,
            Err(_) => {
                let error_tuple = (
                    rustler::types::atom::error(),
                    "workflow_id must be a string",
                );
                return Ok(error_tuple.encode(env));
            }
        },
        None => {
            let error_tuple = (rustler::types::atom::error(), "workflow_id is required");
            return Ok(error_tuple.encode(env));
        }
    };

    let query_type = match params_map.get("query_type") {
        Some(term) => match term.decode::<String>() {
            Ok(qt) => qt,
            Err(_) => {
                let error_tuple = (rustler::types::atom::error(), "query_type must be a string");
                return Ok(error_tuple.encode(env));
            }
        },
        None => {
            let error_tuple = (rustler::types::atom::error(), "query_type is required");
            return Ok(error_tuple.encode(env));
        }
    };

    // Extract optional fields
    let run_id = params_map
        .get("run_id")
        .and_then(|term| term.decode::<String>().ok());

    let namespace = params_map
        .get("namespace")
        .and_then(|term| term.decode::<String>().ok());

    // Parse pre-converted payloads from Elixir
    let input: Option<Vec<temporal_sdk_core_protos::temporal::api::common::v1::Payload>> =
        params_map.get("input").and_then(|term| {
            let elixir_list: Vec<rustler::Term> = term.decode().ok()?;
            let payloads: Result<Vec<_>, _> = elixir_list
                .iter()
                .map(|elixir_term| term_to_payload(elixir_term))
                .collect();
            payloads.ok()
        });

    // Build parameters struct
    let query_params = WorkflowQueryParams {
        workflow_id,
        run_id,
        query_type,
        input,
        namespace,
    };

    // Use shared runtime for query operation
    let rt = get_runtime();

    match rt.block_on(async { client.query_workflow(query_params).await }) {
        Ok(response) => {
            if let Some(rejection) = response.query_rejected {
                let error_tuple = (rustler::types::atom::error(), rejection);
                Ok(error_tuple.encode(env))
            } else {
                let result = match response.result {
                    Some(json_value) => {
                        // Convert JSON back to Elixir term
                        json_value_to_elixir_term(env, &json_value)
                            .unwrap_or_else(|_| rustler::types::atom::nil().encode(env))
                    }
                    None => rustler::types::atom::nil().encode(env),
                };
                let ok_tuple = (rustler::types::atom::ok(), result);
                Ok(ok_tuple.encode(env))
            }
        }
        Err(err) => {
            let error_tuple = (rustler::types::atom::error(), err);
            Ok(error_tuple.encode(env))
        }
    }
}

// Worker functions
#[rustler::nif]
fn worker_new<'a>(
    env: Env<'a>,
    client: ResourceArc<ClientResource>,
    config_term: Term<'a>,
) -> NifResult<Term<'a>> {
    // Parse configuration from Elixir term
    let config_map: HashMap<String, rustler::Term> = match config_term.decode() {
        Ok(map) => map,
        Err(_) => {
            let error_tuple = (rustler::types::atom::error(), "Configuration must be a map");
            return Ok(error_tuple.encode(env));
        }
    };

    // Extract required fields
    let namespace = match config_map.get("namespace") {
        Some(term) => match term.decode::<String>() {
            Ok(ns) => ns,
            Err(_) => {
                let error_tuple = (rustler::types::atom::error(), "namespace must be a string");
                return Ok(error_tuple.encode(env));
            }
        },
        None => "default".to_string(), // Use default if not provided
    };

    let task_queue = match config_map.get("task_queue") {
        Some(term) => match term.decode::<String>() {
            Ok(tq) => tq,
            Err(_) => {
                let error_tuple = (rustler::types::atom::error(), "task_queue must be a string");
                return Ok(error_tuple.encode(env));
            }
        },
        None => {
            let error_tuple = (rustler::types::atom::error(), "task_queue is required");
            return Ok(error_tuple.encode(env));
        }
    };

    // Extract optional configuration fields with defaults
    let max_cached_workflows = config_map
        .get("max_cached_workflows")
        .and_then(|term| term.decode::<usize>().ok())
        .unwrap_or(0);

    let max_outstanding_workflow_tasks = config_map
        .get("max_outstanding_workflow_tasks")
        .and_then(|term| term.decode::<usize>().ok())
        .unwrap_or(100);

    let max_outstanding_activities = config_map
        .get("max_outstanding_activities")
        .and_then(|term| term.decode::<usize>().ok())
        .unwrap_or(100);

    let max_outstanding_local_activities = config_map
        .get("max_outstanding_local_activities")
        .and_then(|term| term.decode::<usize>().ok())
        .unwrap_or(100);

    let no_remote_activities = config_map
        .get("no_remote_activities")
        .and_then(|term| term.decode::<bool>().ok())
        .unwrap_or(false);

    let sticky_queue_schedule_to_start_timeout_ms = config_map
        .get("sticky_queue_schedule_to_start_timeout_ms")
        .and_then(|term| term.decode::<u32>().ok())
        .unwrap_or(10_000);

    let max_heartbeat_throttle_interval_ms = config_map
        .get("max_heartbeat_throttle_interval_ms")
        .and_then(|term| term.decode::<u32>().ok())
        .unwrap_or(60_000);

    let default_heartbeat_throttle_interval_ms = config_map
        .get("default_heartbeat_throttle_interval_ms")
        .and_then(|term| term.decode::<u32>().ok())
        .unwrap_or(5_000);

    // Build worker configuration
    let config = worker::WorkerConfig {
        namespace,
        task_queue,
        max_cached_workflows,
        max_outstanding_workflow_tasks,
        max_outstanding_activities,
        max_outstanding_local_activities,
        no_remote_activities,
        sticky_queue_schedule_to_start_timeout_ms,
        max_heartbeat_throttle_interval_ms,
        default_heartbeat_throttle_interval_ms,
    };

    // Create the worker resource
    match WorkerResource::new(client, config) {
        Ok(worker) => {
            let resource = ResourceArc::new(worker);
            Ok(resource.encode(env))
        }
        Err(err) => {
            let error_tuple = (rustler::types::atom::error(), err);
            Ok(error_tuple.encode(env))
        }
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn worker_poll_workflow_task<'a>(
    env: Env<'a>,
    worker: ResourceArc<WorkerResource>,
) -> NifResult<Term<'a>> {
    let rt = get_runtime();

    match rt.block_on(async {
        // Start the worker if not already running
        worker.start().await?;

        // Poll for workflow task with timeout
        match tokio::time::timeout(
            std::time::Duration::from_secs(60), // 60 second timeout for long polling
            worker.poll_workflow_task(),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err("Polling timeout".to_string()),
        }
    }) {
        Ok(Some(task_data)) => {
            // Task available - return the task data as binary
            Ok((rustler::types::atom::ok(), task_data).encode(env))
        }
        Ok(None) => {
            // No task available - return ok with nil
            Ok((rustler::types::atom::ok(), rustler::types::atom::nil()).encode(env))
        }
        Err(err) => {
            // Worker error - return sanitized error
            tracing::error!("Workflow task polling error: {}", err);
            let sanitized_error = sanitize_polling_error(&err);
            Ok((rustler::types::atom::error(), sanitized_error).encode(env))
        }
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn worker_poll_activity_task<'a>(
    env: Env<'a>,
    worker: ResourceArc<WorkerResource>,
) -> NifResult<Term<'a>> {
    let rt = get_runtime();

    match rt.block_on(async {
        // Start the worker if not already running
        worker.start().await?;

        // Poll for activity task with timeout
        match tokio::time::timeout(
            std::time::Duration::from_secs(60), // 60 second timeout for long polling
            worker.poll_activity_task(),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err("Polling timeout".to_string()),
        }
    }) {
        Ok(Some(task_data)) => {
            // Task available - return the task data as binary
            Ok((rustler::types::atom::ok(), task_data).encode(env))
        }
        Ok(None) => {
            // No task available - return ok with nil
            Ok((rustler::types::atom::ok(), rustler::types::atom::nil()).encode(env))
        }
        Err(err) => {
            // Worker error - return sanitized error
            tracing::error!("Activity task polling error: {}", err);
            let sanitized_error = sanitize_polling_error(&err);
            Ok((rustler::types::atom::error(), sanitized_error).encode(env))
        }
    }
}

#[rustler::nif]
fn worker_complete_workflow_task<'a>(
    env: Env<'a>,
    _worker: ResourceArc<WorkerResource>,
    _completion: Term<'a>,
) -> NifResult<Term<'a>> {
    Ok(rustler::types::atom::error().to_term(env))
}

#[rustler::nif]
fn worker_complete_activity_task<'a>(
    env: Env<'a>,
    _worker: ResourceArc<WorkerResource>,
    _completion: Term<'a>,
) -> NifResult<Term<'a>> {
    Ok(rustler::types::atom::error().to_term(env))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustler::{Encoder, Env, Term};
    use serde_json::json;
    use std::collections::HashMap;

    /// Helper to create a test environment and encode values
    fn with_test_env<F, R>(f: F) -> R
    where
        F: for<'a> FnOnce(Env<'a>) -> R,
    {
        rustler::env::OwnedEnv::new().run(f)
    }

    /// Create a Rustler Term from a value that can be encoded
    fn encode_term<'a, T: Encoder>(env: Env<'a>, value: T) -> Term<'a> {
        value.encode(env)
    }

    #[test]
    fn test_term_to_json_basic_types() {
        with_test_env(|env| {
            // Test string
            let string_term = encode_term(env, "hello world");
            let result = term_to_json_value(&string_term).unwrap();
            assert_eq!(result, json!("hello world"));

            // Test integer i32
            let int_term = encode_term(env, 42i32);
            let result = term_to_json_value(&int_term).unwrap();
            assert_eq!(result, json!(42));

            // Test integer u32
            let uint_term = encode_term(env, 123u32);
            let result = term_to_json_value(&uint_term).unwrap();
            assert_eq!(result, json!(123));

            // Test integer i64
            let int64_term = encode_term(env, 9223372036854775807i64);
            let result = term_to_json_value(&int64_term).unwrap();
            assert_eq!(result, json!(9223372036854775807i64));

            // Test float
            let float_term = encode_term(env, 3.14159f64);
            let result = term_to_json_value(&float_term).unwrap();
            assert_eq!(result, json!(3.14159));

            // Test boolean true
            let bool_term = encode_term(env, true);
            let result = term_to_json_value(&bool_term).unwrap();
            assert_eq!(result, json!(true));

            // Test boolean false
            let bool_term = encode_term(env, false);
            let result = term_to_json_value(&bool_term).unwrap();
            assert_eq!(result, json!(false));

            // Test nil/null (encoded as unit type)
            let nil_term = encode_term(env, ());
            let result = term_to_json_value(&nil_term).unwrap();
            assert_eq!(result, json!(null));
        });
    }

    #[test]
    fn test_term_to_json_large_integers() {
        with_test_env(|env| {
            // Test u64 within i64 range
            let valid_u64_term = encode_term(env, 9223372036854775806u64);
            let result = term_to_json_value(&valid_u64_term).unwrap();
            assert_eq!(result, json!(9223372036854775806i64));

            // Test u64 at boundary (i64::MAX as u64)
            let boundary_u64_term = encode_term(env, i64::MAX as u64);
            let result = term_to_json_value(&boundary_u64_term).unwrap();
            assert_eq!(result, json!(i64::MAX));

            // Test usize within bounds
            let usize_term = encode_term(env, 1000usize);
            let result = term_to_json_value(&usize_term).unwrap();
            assert_eq!(result, json!(1000));

            // Test isize
            let isize_term = encode_term(env, -500isize);
            let result = term_to_json_value(&isize_term).unwrap();
            assert_eq!(result, json!(-500));
        });
    }

    #[test]
    fn test_term_to_json_collections() {
        with_test_env(|env| {
            // Test empty list
            let empty_list: Vec<i32> = vec![];
            let empty_list_term = encode_term(env, empty_list);
            let result = term_to_json_value(&empty_list_term).unwrap();
            assert_eq!(result, json!([]));

            // Test list with mixed types
            // Note: In practice, Elixir lists are homogeneous, but we'll test conversion capabilities
            let list_term = encode_term(env, vec![1i32, 2i32, 3i32]);
            let result = term_to_json_value(&list_term).unwrap();
            assert_eq!(result, json!([1, 2, 3]));

            // Test string list
            let string_list = vec!["hello".to_string(), "world".to_string()];
            let string_list_term = encode_term(env, string_list);
            let result = term_to_json_value(&string_list_term).unwrap();
            assert_eq!(result, json!(["hello", "world"]));
        });
    }

    #[test]
    fn test_term_to_json_maps() {
        with_test_env(|env| {
            // Test empty map with string keys
            let empty_map: HashMap<String, i32> = HashMap::new();
            let empty_map_term = encode_term(env, empty_map);
            let result = term_to_json_value(&empty_map_term).unwrap();
            assert_eq!(result, json!({}));

            // Test map with string keys
            let mut string_map = HashMap::new();
            string_map.insert("name".to_string(), encode_term(env, "Alice"));
            string_map.insert("age".to_string(), encode_term(env, 30i32));
            string_map.insert("active".to_string(), encode_term(env, true));
            let map_term = encode_term(env, string_map);
            let result = term_to_json_value(&map_term).unwrap();

            // Since HashMap order is not guaranteed, check keys individually
            let obj = result.as_object().unwrap();
            assert_eq!(obj.len(), 3);
            assert!(obj.contains_key("name"));
            assert!(obj.contains_key("age"));
            assert!(obj.contains_key("active"));
        });
    }

    #[test]
    fn test_term_to_json_atom_keys() {
        with_test_env(|env| {
            // Test map with atom keys (common Elixir pattern)
            let mut atom_map = HashMap::new();
            let name_atom = rustler::Atom::from_str(env, "name").unwrap();
            let age_atom = rustler::Atom::from_str(env, "age").unwrap();

            atom_map.insert(name_atom, encode_term(env, "Bob"));
            atom_map.insert(age_atom, encode_term(env, 25i32));
            let map_term = encode_term(env, atom_map);
            let result = term_to_json_value(&map_term).unwrap();

            let obj = result.as_object().unwrap();
            assert_eq!(obj.len(), 2);
            // Atom keys get converted using Debug format
            assert!(obj.contains_key("name"));
            assert!(obj.contains_key("age"));
        });
    }

    #[test]
    fn test_term_to_json_tuples() {
        with_test_env(|env| {
            // Test single element tuple
            let single_tuple = (encode_term(env, "hello"),);
            let tuple_term = encode_term(env, single_tuple);
            let result = term_to_json_value(&tuple_term).unwrap();
            assert_eq!(result, json!(["hello"]));

            // Test two element tuple
            let two_tuple = (encode_term(env, "key"), encode_term(env, 42i32));
            let tuple_term = encode_term(env, two_tuple);
            let result = term_to_json_value(&tuple_term).unwrap();
            assert_eq!(result, json!(["key", 42]));

            // Test three element tuple
            let three_tuple = (
                encode_term(env, "first"),
                encode_term(env, 123i32),
                encode_term(env, true),
            );
            let tuple_term = encode_term(env, three_tuple);
            let result = term_to_json_value(&tuple_term).unwrap();
            assert_eq!(result, json!(["first", 123, true]));
        });
    }

    #[test]
    fn test_term_to_json_nested_structures() {
        with_test_env(|env| {
            // Test nested map
            let mut inner_map = HashMap::new();
            inner_map.insert("inner_key".to_string(), encode_term(env, "inner_value"));

            let mut outer_map = HashMap::new();
            outer_map.insert("outer_key".to_string(), encode_term(env, "outer_value"));
            outer_map.insert("nested".to_string(), encode_term(env, inner_map));

            let nested_term = encode_term(env, outer_map);
            let result = term_to_json_value(&nested_term).unwrap();

            let _expected = json!({
                "outer_key": "outer_value",
                "nested": {
                    "inner_key": "inner_value"
                }
            });

            let obj = result.as_object().unwrap();
            assert_eq!(obj.len(), 2);
            assert!(obj.contains_key("outer_key"));
            assert!(obj.contains_key("nested"));

            let nested_obj = obj["nested"].as_object().unwrap();
            assert_eq!(nested_obj["inner_key"], "inner_value");
        });
    }

    #[test]
    fn test_term_to_json_depth_limit() {
        with_test_env(|env| {
            // Create deeply nested structure to test depth limit
            let mut current_map = HashMap::new();
            current_map.insert("value".to_string(), encode_term(env, 42i32));

            // Create nested maps up to the depth limit
            for i in 0..MAX_JSON_DEPTH {
                let mut next_map = HashMap::new();
                next_map.insert(format!("level_{}", i), encode_term(env, current_map));
                current_map = next_map;
            }

            let deep_term = encode_term(env, current_map);
            let result = term_to_json_value(&deep_term);

            // Should fail due to depth limit
            assert!(result.is_err());
            assert!(result.unwrap_err().contains("JSON nesting too deep"));
        });
    }

    #[test]
    fn test_term_to_json_special_floats() {
        with_test_env(|env| {
            // Test normal float
            let normal_float = encode_term(env, 1.23f64);
            let result = term_to_json_value(&normal_float).unwrap();
            assert_eq!(result, json!(1.23));

            // Test zero
            let zero_float = encode_term(env, 0.0f64);
            let result = term_to_json_value(&zero_float).unwrap();
            assert_eq!(result, json!(0.0));

            // Test negative float
            let neg_float = encode_term(env, -3.14f64);
            let result = term_to_json_value(&neg_float).unwrap();
            assert_eq!(result, json!(-3.14));

            // Note: NaN and Infinity are not directly encodable via rustler in this context
            // but serde_json handles them properly when they occur
        });
    }

    #[test]
    fn test_term_to_json_workflow_input_patterns() {
        with_test_env(|env| {
            // Test typical workflow input structure - map with various data types
            let mut workflow_input = HashMap::new();
            workflow_input.insert("user_id".to_string(), encode_term(env, 12345i32));
            workflow_input.insert("username".to_string(), encode_term(env, "alice"));
            workflow_input.insert("is_premium".to_string(), encode_term(env, true));
            workflow_input.insert("balance".to_string(), encode_term(env, 99.99f64));
            workflow_input.insert(
                "tags".to_string(),
                encode_term(env, vec!["vip".to_string(), "early_adopter".to_string()]),
            );

            // Add nested settings
            let mut settings = HashMap::new();
            settings.insert("theme".to_string(), encode_term(env, "dark"));
            settings.insert("notifications".to_string(), encode_term(env, true));
            workflow_input.insert("settings".to_string(), encode_term(env, settings));

            let input_term = encode_term(env, workflow_input);
            let result = term_to_json_value(&input_term).unwrap();

            let obj = result.as_object().unwrap();
            assert_eq!(obj["user_id"], 12345);
            assert_eq!(obj["username"], "alice");
            assert_eq!(obj["is_premium"], true);
            assert_eq!(obj["balance"], 99.99);
            assert_eq!(obj["tags"], json!(["vip", "early_adopter"]));

            let settings_obj = obj["settings"].as_object().unwrap();
            assert_eq!(settings_obj["theme"], "dark");
            assert_eq!(settings_obj["notifications"], true);
        });
    }
}
