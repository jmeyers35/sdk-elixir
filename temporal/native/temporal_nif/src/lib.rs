use rustler::{Encoder, Env, NifResult, ResourceArc, Term};
use std::collections::HashMap;

mod client;
mod worker;

use client::{ClientOptions, ClientResource, ClientTlsConfig};
use worker::WorkerResource;

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
#[rustler::nif(schedule = "DirtyCpu")]
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

    // Simple runtime creation - following OSS SDK patterns
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            let error_msg = format!("Failed to create runtime: {}", e);
            let error_tuple = (rustler::types::atom::error(), error_msg);
            return Ok(error_tuple.encode(env));
        }
    };

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

#[rustler::nif]
fn client_start_workflow<'a>(
    env: Env<'a>,
    _client: ResourceArc<ClientResource>,
    _params: Term<'a>,
) -> NifResult<Term<'a>> {
    Ok(rustler::types::atom::error().to_term(env))
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
