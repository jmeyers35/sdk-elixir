use rustler::{Encoder, Env, NifResult, ResourceArc, Term};

mod client;
mod worker;

use client::ClientResource;
use worker::WorkerResource;

rustler::init!("Elixir.Temporal.Native");

// Client functions
#[rustler::nif]
fn client_connect<'a>(env: Env<'a>, _config: Term<'a>) -> NifResult<Term<'a>> {
    let client = ClientResource::new();
    let resource = ResourceArc::new(client);
    Ok(resource.encode(env))
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
