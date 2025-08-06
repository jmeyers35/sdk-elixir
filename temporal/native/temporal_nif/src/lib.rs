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

    // Try different integer types - start with smaller types for efficiency
    if let Ok(n) = term.decode::<i32>() {
        return Ok(serde_json::Value::Number(serde_json::Number::from(n)));
    }
    if let Ok(n) = term.decode::<u32>() {
        return Ok(serde_json::Value::Number(serde_json::Number::from(n)));
    }
    if let Ok(n) = term.decode::<i64>() {
        return Ok(serde_json::Value::Number(serde_json::Number::from(n)));
    }
    if let Ok(n) = term.decode::<u64>() {
        // Check if it fits in JSON number range
        if n <= i64::MAX as u64 {
            return Ok(serde_json::Value::Number(serde_json::Number::from(
                n as i64,
            )));
        }
    }
    if let Ok(n) = term.decode::<isize>() {
        return Ok(serde_json::Value::Number(serde_json::Number::from(
            n as i64,
        )));
    }
    if let Ok(n) = term.decode::<usize>() {
        if n <= i64::MAX as usize {
            return Ok(serde_json::Value::Number(serde_json::Number::from(
                n as i64,
            )));
        }
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
    // First try string keys
    if let Ok(map) = term.decode::<HashMap<String, rustler::Term>>() {
        let mut json_map = serde_json::Map::new();
        for (key, value) in map {
            json_map.insert(key, term_to_json_value_depth(&value, depth + 1)?);
        }
        return Ok(serde_json::Value::Object(json_map));
    }

    // Then try atom keys (common in Elixir)
    if let Ok(map) = term.decode::<HashMap<rustler::Atom, rustler::Term>>() {
        let mut json_map = serde_json::Map::new();
        for (key, value) in map {
            // Convert atom to string - use debug format since atoms don't implement Display
            let key_str = format!("{:?}", key);
            json_map.insert(key_str, term_to_json_value_depth(&value, depth + 1)?);
        }
        return Ok(serde_json::Value::Object(json_map));
    }

    // Try as tuples (convert to JSON arrays)
    if let Ok(tuple) = term.decode::<(rustler::Term,)>() {
        return Ok(serde_json::Value::Array(vec![term_to_json_value_depth(
            &tuple.0,
            depth + 1,
        )?]));
    }
    if let Ok(tuple) = term.decode::<(rustler::Term, rustler::Term)>() {
        return Ok(serde_json::Value::Array(vec![
            term_to_json_value_depth(&tuple.0, depth + 1)?,
            term_to_json_value_depth(&tuple.1, depth + 1)?,
        ]));
    }
    if let Ok(tuple) = term.decode::<(rustler::Term, rustler::Term, rustler::Term)>() {
        return Ok(serde_json::Value::Array(vec![
            term_to_json_value_depth(&tuple.0, depth + 1)?,
            term_to_json_value_depth(&tuple.1, depth + 1)?,
            term_to_json_value_depth(&tuple.2, depth + 1)?,
        ]));
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
