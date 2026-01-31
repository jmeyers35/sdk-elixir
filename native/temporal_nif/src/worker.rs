use crate::client::ClientResource;
use rustler::{NifStruct, ResourceArc};
use std::sync::{Arc, OnceLock};
use temporal_sdk_core::{init_worker, CoreRuntime, TokioRuntimeBuilder, WorkerConfigBuilder};
use temporal_sdk_core_api::{
    errors::PollError, telemetry::TelemetryOptionsBuilder, Worker as WorkerTrait,
};
use temporal_sdk_core_protos::coresdk::{
    activity_task::ActivityTask as CoreActivityTask,
    workflow_activation::WorkflowActivation as CoreWorkflowActivation,
};
use tokio::sync::Mutex;

/// Global shared CoreRuntime for all worker operations
/// This prevents resource exhaustion from creating multiple runtimes
static SHARED_CORE_RUNTIME: OnceLock<Arc<CoreRuntime>> = OnceLock::new();

/// Get or create the shared CoreRuntime
/// This ensures we have exactly one CoreRuntime per BEAM instance
fn get_core_runtime() -> Result<&'static Arc<CoreRuntime>, String> {
    SHARED_CORE_RUNTIME
        .get_or_init(|| {
            // Create telemetry options with default configuration
            let telemetry_options = TelemetryOptionsBuilder::default()
                .build()
                .expect("Failed to build telemetry options");

            // Create tokio runtime builder
            let tokio_builder = TokioRuntimeBuilder::default();

            // Create CoreRuntime
            match CoreRuntime::new(telemetry_options, tokio_builder) {
                Ok(runtime) => Arc::new(runtime),
                Err(e) => {
                    panic!("Failed to create CoreRuntime: {}", e);
                }
            }
        })
        .as_ref();
    Ok(SHARED_CORE_RUNTIME.get().unwrap())
}

/// Workflow task data returned to Elixir
/// Note: Workflow tasks use run_id for completion, NOT task tokens
#[derive(Debug, Clone, NifStruct)]
#[module = "Temporal.WorkflowTaskData"]
pub struct WorkflowTaskData {
    pub run_id: String, // Used for workflow completion - this is the correct identifier
    pub workflow_execution: WorkflowExecution,
    pub workflow_type: WorkflowType,
    pub started_event_id: i64,
    pub previous_started_event_id: i64,
    pub attempt: i32,
    pub history_events: Vec<u8>, // Serialized history events
}

/// Activity task data returned to Elixir
#[derive(Debug, Clone, NifStruct)]
#[module = "Temporal.ActivityTaskData"]
pub struct ActivityTaskData {
    pub task_token: Vec<u8>,
    pub workflow_execution: WorkflowExecution,
    pub activity_id: String,
    pub activity_type: ActivityType,
    pub input: Vec<u8>, // Serialized input payloads
    pub scheduled_time_ms: i64,
    pub schedule_to_close_timeout_ms: i64,
    pub start_to_close_timeout_ms: i64,
    pub heartbeat_timeout_ms: i64,
    pub attempt: i32,
}

/// Workflow execution information
#[derive(Debug, Clone, NifStruct)]
#[module = "Temporal.WorkflowExecution"]
pub struct WorkflowExecution {
    pub workflow_id: String,
    pub run_id: String,
}

/// Workflow type information
#[derive(Debug, Clone, NifStruct)]
#[module = "Temporal.WorkflowType"]
pub struct WorkflowType {
    pub name: String,
}

/// Activity type information
#[derive(Debug, Clone, NifStruct)]
#[module = "Temporal.ActivityType"]
pub struct ActivityType {
    pub name: String,
}

/// Completion data for workflow tasks
#[derive(Debug, Clone)]
pub enum WorkflowTaskCompletion {
    /// Successful completion with commands
    Success {
        run_id: String,
        commands: Vec<u8>, // Serialized workflow commands
    },
    /// Failed with error message
    Failure {
        run_id: String,
        failure: String,
    },
}

/// Completion data for activity tasks
#[derive(Debug, Clone)]
pub enum ActivityTaskCompletion {
    /// Successful completion with result
    Success {
        task_token: Vec<u8>,
        result: Vec<u8>, // Serialized result payload
    },
    /// Failed with error details
    Failure {
        task_token: Vec<u8>,
        failure: String,
    },
    /// Activity cancelled
    Cancel {
        task_token: Vec<u8>,
        details: Vec<u8>, // Optional cancellation details
    },
}

/// Resource wrapping a Temporal worker using SDK Core
pub struct WorkerResource {
    client: ResourceArc<ClientResource>,
    namespace: String,
    task_queue: String,
    config: WorkerConfig,
    // SDK Core worker instance
    core_worker: Arc<Mutex<Option<Box<dyn WorkerTrait + Send + Sync>>>>,
    // Worker state
    state: Arc<WorkerState>,
}

/// Worker state management using atomic operations
struct WorkerState {
    is_running: std::sync::atomic::AtomicBool,
}

/// Configuration for worker creation
#[derive(Debug, Clone)]
#[allow(dead_code)] // These fields will be used when we implement full worker functionality
pub struct WorkerConfig {
    pub namespace: String,
    pub task_queue: String,
    pub max_cached_workflows: usize,
    pub max_outstanding_workflow_tasks: usize,
    pub max_outstanding_activities: usize,
    pub max_outstanding_local_activities: usize,
    pub no_remote_activities: bool,
    pub sticky_queue_schedule_to_start_timeout_ms: u32,
    pub max_heartbeat_throttle_interval_ms: u32,
    pub default_heartbeat_throttle_interval_ms: u32,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            namespace: "default".to_string(),
            task_queue: "default".to_string(),
            max_cached_workflows: 0,
            max_outstanding_workflow_tasks: 100,
            max_outstanding_activities: 100,
            max_outstanding_local_activities: 100,
            no_remote_activities: false,
            sticky_queue_schedule_to_start_timeout_ms: 10_000,
            max_heartbeat_throttle_interval_ms: 60_000,
            default_heartbeat_throttle_interval_ms: 5_000,
        }
    }
}

#[allow(dead_code)] // These methods will be used in future tasks
impl WorkerResource {
    /// Create a new worker with the given client and configuration
    pub fn new(client: ResourceArc<ClientResource>, config: WorkerConfig) -> Result<Self, String> {
        // Validate configuration
        if config.namespace.is_empty() {
            return Err("Namespace cannot be empty".to_string());
        }
        if config.task_queue.is_empty() {
            return Err("Task queue cannot be empty".to_string());
        }
        if config.max_outstanding_workflow_tasks == 0 {
            return Err("Max outstanding workflow tasks must be greater than 0".to_string());
        }
        if config.max_outstanding_activities == 0 {
            return Err("Max outstanding activities must be greater than 0".to_string());
        }

        let state = Arc::new(WorkerState {
            is_running: std::sync::atomic::AtomicBool::new(false),
        });

        Ok(Self {
            namespace: config.namespace.clone(),
            task_queue: config.task_queue.clone(),
            client,
            config,
            core_worker: Arc::new(Mutex::new(None)),
            state,
        })
    }

    /// Initialize the SDK Core worker
    async fn initialize_core_worker(&self) -> Result<(), String> {
        use std::sync::atomic::Ordering;

        if self.state.is_running.load(Ordering::Relaxed) {
            return Ok(()); // Already initialized
        }

        // Get the shared CoreRuntime
        let core_runtime = get_core_runtime()?;

        // Build SDK Core worker configuration
        let worker_config = WorkerConfigBuilder::default()
            .namespace(&self.namespace)
            .task_queue(&self.task_queue)
            .max_outstanding_workflow_tasks(self.config.max_outstanding_workflow_tasks)
            .max_outstanding_activities(self.config.max_outstanding_activities)
            .max_outstanding_local_activities(self.config.max_outstanding_local_activities)
            .build()
            .map_err(|e| format!("Failed to build worker config: {}", e))?;

        // Get the client's underlying temporal client for SDK Core worker creation
        let temporal_client = self.client.get_inner_client();

        // Create the SDK Core worker using init_worker
        match init_worker(core_runtime, worker_config, temporal_client) {
            Ok(sdk_worker) => {
                let mut core_worker_guard = self.core_worker.lock().await;
                *core_worker_guard = Some(Box::new(sdk_worker));
                self.state.is_running.store(true, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => Err(format!("Failed to initialize SDK Core worker: {}", e)),
        }
    }

    /// Start the worker
    pub async fn start(&self) -> Result<(), String> {
        self.initialize_core_worker().await
    }

    /// Stop the worker
    pub async fn stop(&self) -> Result<(), String> {
        use std::sync::atomic::Ordering;

        self.state.is_running.store(false, Ordering::Relaxed);

        let mut core_worker_guard = self.core_worker.lock().await;
        *core_worker_guard = None;

        Ok(())
    }

    /// Check if worker is running
    pub fn is_running(&self) -> bool {
        use std::sync::atomic::Ordering;
        self.state.is_running.load(Ordering::Relaxed)
    }

    /// Poll for workflow tasks using SDK Core patterns
    pub async fn poll_workflow_task(&self) -> Result<Option<WorkflowTaskData>, String> {
        if !self.is_running() {
            return Err("Worker is not running".to_string());
        }

        tracing::debug!("Polling for workflow task on queue: {}", self.task_queue);

        // Get the SDK Core worker
        let core_worker_guard = self.core_worker.lock().await;
        let worker = match core_worker_guard.as_ref() {
            Some(worker) => worker,
            None => return Err("SDK Core worker not initialized".to_string()),
        };

        // Poll for workflow activation using SDK Core
        match worker.poll_workflow_activation().await {
            Ok(activation) => {
                // Convert CoreWorkflowActivation to our WorkflowTaskData
                match self.convert_workflow_activation(activation) {
                    Ok(task_data) => Ok(Some(task_data)),
                    Err(e) => Err(format!("Failed to convert workflow activation: {}", e)),
                }
            }
            Err(PollError::ShutDown) => {
                // Worker is shutting down - this is expected
                tracing::info!("Worker is shutting down, polling stopped");
                Ok(None)
            }
            Err(e) => Err(format!("SDK Core polling error: {}", e)),
        }
    }

    /// Convert SDK Core WorkflowActivation to our WorkflowTaskData
    fn convert_workflow_activation(
        &self,
        activation: CoreWorkflowActivation,
    ) -> Result<WorkflowTaskData, String> {
        // Find workflow_id and workflow_type from InitializeWorkflow job
        let (workflow_id, workflow_type_name) = {
            let mut wf_id = "unknown".to_string();
            let mut wf_type = "unknown".to_string();

            for job in &activation.jobs {
                if let Some(temporal_sdk_core_protos::coresdk::workflow_activation::workflow_activation_job::Variant::InitializeWorkflow(init_workflow)) = &job.variant {
                    wf_id = init_workflow.workflow_id.clone();
                    wf_type = init_workflow.workflow_type.clone();
                    break;
                }
            }
            (wf_id, wf_type)
        };

        // Clone run_id for multiple uses
        let run_id = activation.run_id.clone();

        // Build workflow execution info
        let workflow_execution = WorkflowExecution {
            workflow_id,
            run_id: run_id.clone(),
        };

        let workflow_type = WorkflowType {
            name: workflow_type_name,
        };

        // For now, serialize the activation info as JSON bytes
        // In a production implementation, you'd want to use the actual protobuf encoding
        let history_events = {
            let activation_info = serde_json::json!({
                "run_id": run_id,
                "timestamp": activation.timestamp.map(|ts| ts.seconds),
                "is_replaying": activation.is_replaying,
                "history_length": activation.history_length,
                "job_count": activation.jobs.len()
            });
            serde_json::to_vec(&activation_info)
                .map_err(|e| format!("Failed to serialize activation info: {}", e))?
        };

        // Use the run_id directly - workflow tasks are completed using run_id, not task tokens
        Ok(WorkflowTaskData {
            run_id: run_id.clone(),
            workflow_execution,
            workflow_type,
            started_event_id: activation.history_length as i64,
            previous_started_event_id: 0, // Not directly available, would need to be tracked
            attempt: 1,                   // Not directly available from activation
            history_events,
        })
    }

    /// Poll for activity tasks using SDK Core patterns
    pub async fn poll_activity_task(&self) -> Result<Option<ActivityTaskData>, String> {
        if !self.is_running() {
            return Err("Worker is not running".to_string());
        }

        tracing::debug!("Polling for activity task on queue: {}", self.task_queue);

        // Get the SDK Core worker
        let core_worker_guard = self.core_worker.lock().await;
        let worker = match core_worker_guard.as_ref() {
            Some(worker) => worker,
            None => return Err("SDK Core worker not initialized".to_string()),
        };

        // Poll for activity task using SDK Core
        match worker.poll_activity_task().await {
            Ok(activity_task) => {
                // Convert CoreActivityTask to our ActivityTaskData
                match self.convert_activity_task(activity_task) {
                    Ok(task_data) => Ok(Some(task_data)),
                    Err(e) => Err(format!("Failed to convert activity task: {}", e)),
                }
            }
            Err(PollError::ShutDown) => {
                // Worker is shutting down - this is expected
                tracing::info!("Worker is shutting down, activity polling stopped");
                Ok(None)
            }
            Err(e) => Err(format!("SDK Core activity polling error: {}", e)),
        }
    }

    /// Convert SDK Core ActivityTask to our ActivityTaskData
    fn convert_activity_task(
        &self,
        activity_task: CoreActivityTask,
    ) -> Result<ActivityTaskData, String> {
        // Extract activity start information from the variant
        let start = match &activity_task.variant {
            Some(
                temporal_sdk_core_protos::coresdk::activity_task::activity_task::Variant::Start(
                    start,
                ),
            ) => start,
            Some(
                temporal_sdk_core_protos::coresdk::activity_task::activity_task::Variant::Cancel(_),
            ) => {
                return Err("Received activity cancel task, not start task".to_string());
            }
            None => {
                return Err("Activity task has no variant".to_string());
            }
        };

        // Extract workflow execution info
        let workflow_execution = WorkflowExecution {
            workflow_id: start
                .workflow_execution
                .as_ref()
                .map(|we| we.workflow_id.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            run_id: start
                .workflow_execution
                .as_ref()
                .map(|we| we.run_id.clone())
                .unwrap_or_else(|| "unknown".to_string()),
        };

        // Extract activity type
        let activity_type = ActivityType {
            name: start.activity_type.clone(),
        };

        // Serialize input payloads
        let input = {
            let input_info = serde_json::json!({
                "activity_id": start.activity_id,
                "activity_type": start.activity_type,
                "input_size": start.input.len(),
                "scheduled_time": start.scheduled_time.as_ref().map(|ts| ts.seconds),
            });
            serde_json::to_vec(&input_info)
                .map_err(|e| format!("Failed to serialize activity input: {}", e))?
        };

        Ok(ActivityTaskData {
            task_token: activity_task.task_token,
            workflow_execution,
            activity_id: start.activity_id.clone(),
            activity_type,
            input,
            scheduled_time_ms: start
                .scheduled_time
                .as_ref()
                .map(|ts| ts.seconds * 1000 + (ts.nanos as i64) / 1_000_000)
                .unwrap_or(0),
            schedule_to_close_timeout_ms: start
                .schedule_to_close_timeout
                .as_ref()
                .map(|d| d.seconds * 1000 + (d.nanos as i64) / 1_000_000)
                .unwrap_or(0),
            start_to_close_timeout_ms: start
                .start_to_close_timeout
                .as_ref()
                .map(|d| d.seconds * 1000 + (d.nanos as i64) / 1_000_000)
                .unwrap_or(0),
            heartbeat_timeout_ms: start
                .heartbeat_timeout
                .as_ref()
                .map(|d| d.seconds * 1000 + (d.nanos as i64) / 1_000_000)
                .unwrap_or(0),
            attempt: start.attempt as i32,
        })
    }

    /// Complete a workflow task
    pub async fn complete_workflow_task(
        &self,
        completion: WorkflowTaskCompletion,
    ) -> Result<(), String> {
        if !self.is_running() {
            return Err("Worker is not running".to_string());
        }

        tracing::debug!("Completing workflow task on queue: {}", self.task_queue);

        // TODO: Wire this up to SDK Core's workflow completion API
        // For now, acknowledge receipt of completion data
        match completion {
            WorkflowTaskCompletion::Success { run_id, .. } => {
                tracing::debug!("Workflow task success for run_id: {}", run_id);
            }
            WorkflowTaskCompletion::Failure { run_id, failure } => {
                tracing::warn!("Workflow task failure for run_id: {} - {}", run_id, failure);
            }
        }

        Ok(())
    }

    /// Complete an activity task
    pub async fn complete_activity_task(
        &self,
        completion: ActivityTaskCompletion,
    ) -> Result<(), String> {
        if !self.is_running() {
            return Err("Worker is not running".to_string());
        }

        tracing::debug!("Completing activity task on queue: {}", self.task_queue);

        // TODO: Wire this up to SDK Core's activity completion API
        // For now, acknowledge receipt of completion data
        match completion {
            ActivityTaskCompletion::Success { task_token, .. } => {
                tracing::debug!("Activity task success, token length: {}", task_token.len());
            }
            ActivityTaskCompletion::Failure { task_token, failure } => {
                tracing::warn!("Activity task failure, token length: {} - {}", task_token.len(), failure);
            }
            ActivityTaskCompletion::Cancel { task_token, .. } => {
                tracing::info!("Activity task cancel, token length: {}", task_token.len());
            }
        }

        Ok(())
    }

    /// Get the worker's namespace
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Get the worker's task queue
    pub fn task_queue(&self) -> &str {
        &self.task_queue
    }

    /// Get the worker's configuration
    pub fn config(&self) -> &WorkerConfig {
        &self.config
    }

    /// Get a reference to the client
    pub fn client(&self) -> &ResourceArc<ClientResource> {
        &self.client
    }

    /// Validate worker configuration (placeholder for actual validation)
    /// This will be implemented properly when we integrate with SDK Core
    pub fn validate(&self) -> Result<(), String> {
        if self.namespace.is_empty() {
            return Err("Invalid namespace".to_string());
        }
        if self.task_queue.is_empty() {
            return Err("Invalid task queue".to_string());
        }
        Ok(())
    }
}

impl Drop for WorkerResource {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering;

        // Mark worker as not running
        self.state.is_running.store(false, Ordering::Relaxed);

        tracing::debug!("WorkerResource dropped, worker stopped and cleaned up");
    }
}

// Completion implementations that will be wired up to SDK Core in future iteration
// For now, these accept the completion data and acknowledge receipt
impl WorkerResource {
    /// Complete a workflow task
    pub async fn complete_workflow_task(
        &self,
        completion: WorkflowTaskCompletion,
    ) -> Result<(), String> {
        tracing::debug!("Completing workflow task on queue: {}", self.task_queue);

        // TODO: Wire this up to SDK Core's workflow completion API
        // For now, acknowledge receipt of completion data
        match completion {
            WorkflowTaskCompletion::Success { run_id, .. } => {
                tracing::debug!("Workflow task success for run_id: {}", run_id);
            }
            WorkflowTaskCompletion::Failure { run_id, failure } => {
                tracing::warn!("Workflow task failure for run_id: {} - {}", run_id, failure);
            }
        }

        Ok(())
    }

    /// Complete an activity task
    pub async fn complete_activity_task(
        &self,
        completion: ActivityTaskCompletion,
    ) -> Result<(), String> {
        tracing::debug!("Completing activity task on queue: {}", self.task_queue);

        // TODO: Wire this up to SDK Core's activity completion API
        // For now, acknowledge receipt of completion data
        match completion {
            ActivityTaskCompletion::Success { task_token, .. } => {
                tracing::debug!("Activity task success, token length: {}", task_token.len());
            }
            ActivityTaskCompletion::Failure { task_token, failure } => {
                tracing::warn!("Activity task failure, token length: {} - {}", task_token.len(), failure);
            }
            ActivityTaskCompletion::Cancel { task_token, .. } => {
                tracing::info!("Activity task cancel, token length: {}", task_token.len());
            }
        }

        Ok(())
    }
}

#[rustler::resource_impl]
impl rustler::Resource for WorkerResource {}
