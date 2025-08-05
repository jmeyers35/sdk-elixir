use rustler::{Encoder, Env, NifResult, ResourceArc, Term};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

mod client;
mod worker;

use client::{ClientOptions, ClientResource, ClientTlsConfig, WorkflowStartParams};
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

/// Maximum nesting depth for JSON conversion to prevent stack overflow
const MAX_JSON_DEPTH: usize = 32;

/// Convert a Rustler Term to serde_json::Value with depth protection
fn term_to_json_value(term: &rustler::Term) -> Result<serde_json::Value, String> {
    term_to_json_value_depth(term, 0)
}

fn term_to_json_value_depth(
    term: &rustler::Term,
    depth: usize,
) -> Result<serde_json::Value, String> {
    if depth > MAX_JSON_DEPTH {
        return Err("JSON nesting too deep".to_string());
    }
    // Try different Rust types that can be converted to JSON
    if let Ok(s) = term.decode::<String>() {
        return Ok(serde_json::Value::String(s));
    }
    if let Ok(n) = term.decode::<i64>() {
        return Ok(serde_json::Value::Number(serde_json::Number::from(n)));
    }
    if let Ok(n) = term.decode::<f64>() {
        if let Some(num) = serde_json::Number::from_f64(n) {
            return Ok(serde_json::Value::Number(num));
        }
    }
    if let Ok(b) = term.decode::<bool>() {
        return Ok(serde_json::Value::Bool(b));
    }
    // Try as map (Elixir map -> JSON object)
    if let Ok(map) = term.decode::<HashMap<String, rustler::Term>>() {
        let mut json_map = serde_json::Map::new();
        for (key, value) in map {
            json_map.insert(key, term_to_json_value_depth(&value, depth + 1)?);
        }
        return Ok(serde_json::Value::Object(json_map));
    }
    // Try as list (Elixir list -> JSON array)
    if let Ok(list) = term.decode::<Vec<rustler::Term>>() {
        let json_array: Result<Vec<serde_json::Value>, String> = list
            .iter()
            .map(|t| term_to_json_value_depth(t, depth + 1))
            .collect();
        return Ok(serde_json::Value::Array(json_array?));
    }
    // Handle nil/null
    if let Ok(()) = term.decode::<()>() {
        return Ok(serde_json::Value::Null);
    }
    Err("Unsupported term type for JSON conversion".to_string())
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
    if let Some(tls_term) = config_map.get("tls") {
        let tls_map: HashMap<String, rustler::Term> = tls_term
            .decode()
            .map_err(|_| "TLS configuration must be a map".to_string())?;

        let client_cert_path = tls_map
            .get("client_cert_path")
            .and_then(|term| term.decode::<String>().ok());
        let client_key_path = tls_map
            .get("client_key_path")
            .and_then(|term| term.decode::<String>().ok());
        let ca_cert_path = tls_map
            .get("ca_cert_path")
            .and_then(|term| term.decode::<String>().ok());

        Ok(Some(ClientTlsConfig {
            client_cert_path,
            client_key_path,
            ca_cert_path,
        }))
    } else {
        Ok(None)
    }
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
    let target_url = match config_map.get("target_url") {
        Some(term) => match term.decode::<String>() {
            Ok(url) => url,
            Err(_) => {
                let error_tuple = (rustler::types::atom::error(), "target_url must be a string");
                return Ok(error_tuple.encode(env));
            }
        },
        None => {
            let error_tuple = (rustler::types::atom::error(), "target_url is required");
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

    // Build options struct
    let options = ClientOptions {
        tls,
        client_name,
        client_version,
        identity,
        api_key,
        skip_system_info,
    };

    // Use shared runtime for connection - prevents resource exhaustion
    let rt = get_runtime();

    match rt.block_on(ClientResource::connect(target_url, namespace, options)) {
        Ok(client) => {
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

    // Extract optional fields - parse input from Elixir term
    let input: Option<Vec<serde_json::Value>> = params_map.get("input").and_then(|term| {
        // Try to decode as Vec<rustler::Term> first
        let elixir_list: Vec<rustler::Term> = term.decode().ok()?;
        // Convert each term to serde_json::Value
        let json_values: Result<Vec<serde_json::Value>, _> = elixir_list
            .iter()
            .map(|elixir_term| {
                // Decode as Rust value that can be converted to JSON
                // This handles maps, lists, strings, numbers, booleans
                term_to_json_value(elixir_term)
            })
            .collect();
        json_values.ok()
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

#[rustler::nif]
fn client_signal_workflow<'a>(
    env: Env<'a>,
    _client: ResourceArc<ClientResource>,
    _params: Term<'a>,
) -> NifResult<Term<'a>> {
    Ok(rustler::types::atom::error().to_term(env))
}

#[rustler::nif]
fn client_query_workflow<'a>(
    env: Env<'a>,
    _client: ResourceArc<ClientResource>,
    _params: Term<'a>,
) -> NifResult<Term<'a>> {
    Ok(rustler::types::atom::error().to_term(env))
}

// Worker functions
#[rustler::nif]
fn worker_new<'a>(
    env: Env<'a>,
    _client: ResourceArc<ClientResource>,
    _config: Term<'a>,
) -> NifResult<Term<'a>> {
    let worker = WorkerResource::new();
    let resource = ResourceArc::new(worker);
    Ok(resource.encode(env))
}

#[rustler::nif]
fn worker_poll_workflow_task<'a>(
    env: Env<'a>,
    _worker: ResourceArc<WorkerResource>,
) -> NifResult<Term<'a>> {
    Ok(rustler::types::atom::error().to_term(env))
}

#[rustler::nif]
fn worker_poll_activity_task<'a>(
    env: Env<'a>,
    _worker: ResourceArc<WorkerResource>,
) -> NifResult<Term<'a>> {
    Ok(rustler::types::atom::error().to_term(env))
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
