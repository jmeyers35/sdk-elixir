use std::sync::Arc;
use temporal_client::{
    Client, ClientOptionsBuilder, NamespacedClient, RetryClient, TlsConfig, WorkflowService,
};
use temporal_sdk_core_protos::temporal::api::common::v1::{Payload, Payloads, WorkflowType};
use temporal_sdk_core_protos::temporal::api::taskqueue::v1::TaskQueue;
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::StartWorkflowExecutionRequest;
use url::Url;
use uuid::Uuid;

/// Elixir client resource wrapping sdk-core's RetryClient<Client>
/// Uses Arc for efficient sharing - RetryClient<Client> is already thread-safe
/// and designed for concurrent usage in Temporal SDK Core
pub struct ClientResource {
    inner: Arc<RetryClient<Client>>,
}

/// TLS configuration for client connections
#[derive(Debug, Clone)]
pub struct ClientTlsConfig {
    pub client_cert_path: Option<String>,
    pub client_key_path: Option<String>,
    pub ca_cert_path: Option<String>,
}

/// Client configuration options
#[derive(Debug, Clone, Default)]
pub struct ClientOptions {
    pub tls: Option<ClientTlsConfig>,
    pub client_name: Option<String>,
    pub client_version: Option<String>,
    pub identity: Option<String>,
    pub api_key: Option<String>,
    pub skip_system_info: bool,
}

/// Parameters for starting a workflow
#[derive(Debug, Clone)]
pub struct WorkflowStartParams {
    pub workflow_id: String,
    pub workflow_type: String,
    pub task_queue: String,
    pub input: Option<Vec<serde_json::Value>>,
    pub request_id: Option<String>,
    #[allow(dead_code)] // Will be implemented in future iteration
    pub execution_timeout: Option<u64>,
    #[allow(dead_code)] // Will be implemented in future iteration
    pub run_timeout: Option<u64>,
    #[allow(dead_code)] // Will be implemented in future iteration
    pub task_timeout: Option<u64>,
}

/// Return value for started workflow
#[derive(Debug, Clone)]
pub struct WorkflowHandle {
    pub run_id: String,
    pub workflow_id: String,
    pub first_execution_run_id: String,
}

impl ClientResource {
    /// Create a new Temporal client using sdk-core directly following OSS SDK patterns
    pub async fn connect(
        target_url: String,
        namespace: String,
        options: ClientOptions,
    ) -> Result<Self, String> {
        // Simple URL parsing - let Url::parse handle validation like other SDKs
        let parsed_url = Url::parse(&target_url).map_err(|e| format!("Invalid URL: {}", e))?;

        // Build client options using sdk-core's ClientOptionsBuilder
        let mut builder = ClientOptionsBuilder::default();
        builder
            .target_url(parsed_url)
            .client_name(
                options
                    .client_name
                    .unwrap_or_else(|| "temporal-elixir-sdk".to_string()),
            )
            .client_version(
                options
                    .client_version
                    .unwrap_or_else(|| "0.1.0".to_string()),
            )
            .skip_get_system_info(options.skip_system_info);

        if let Some(identity) = options.identity {
            builder.identity(identity);
        }

        if let Some(api_key) = options.api_key {
            builder.api_key(Some(api_key));
        }

        // Add TLS configuration if provided
        if let Some(tls_config) = options.tls {
            let mut tls_cfg = TlsConfig {
                server_root_ca_cert: None,
                domain: None,
                client_tls_config: None,
            };

            // Read CA cert if provided
            if let Some(ca_path) = tls_config.ca_cert_path {
                // Basic path validation - prevent directory traversal
                if ca_path.contains("../") || ca_path.contains("..\\") {
                    return Err("Invalid CA cert path".to_string());
                }
                let ca_data = std::fs::read(&ca_path)
                    .map_err(|e| format!("Failed to read CA cert file {}: {}", ca_path, e))?;
                tls_cfg.server_root_ca_cert = Some(ca_data);
            }

            // Read client cert and key if both provided
            if let (Some(cert_path), Some(key_path)) =
                (tls_config.client_cert_path, tls_config.client_key_path)
            {
                // Basic path validation - prevent directory traversal
                if cert_path.contains("../")
                    || cert_path.contains("..\\")
                    || key_path.contains("../")
                    || key_path.contains("..\\")
                {
                    return Err("Invalid cert or key path".to_string());
                }
                let cert_data = std::fs::read(&cert_path)
                    .map_err(|e| format!("Failed to read client cert file {}: {}", cert_path, e))?;
                let key_data = std::fs::read(&key_path)
                    .map_err(|e| format!("Failed to read client key file {}: {}", key_path, e))?;

                tls_cfg.client_tls_config = Some(temporal_client::ClientTlsConfig {
                    client_cert: cert_data,
                    client_private_key: key_data,
                });
            }

            builder.tls_cfg(tls_cfg);
        }

        let client_options = builder
            .build()
            .map_err(|e| format!("Failed to build client options: {}", e))?;

        // Connect to Temporal server using sdk-core
        let client = client_options
            .connect(namespace, None)
            .await
            .map_err(|e| format!("Connection failed: {}", e))?;

        Ok(Self {
            inner: Arc::new(client),
        })
    }

    /// Start a workflow execution using sdk-core directly
    pub async fn start_workflow(
        &self,
        params: WorkflowStartParams,
    ) -> Result<WorkflowHandle, String> {
        // Serialize workflow inputs to JSON payloads
        let input_payloads = if let Some(inputs) = params.input {
            let mut payloads = Vec::new();
            for input in inputs {
                let json_bytes = serde_json::to_vec(&input)
                    .map_err(|e| format!("Failed to serialize input: {}", e))?;

                let payload = Payload {
                    metadata: [("encoding".to_string(), "json/plain".as_bytes().to_vec())].into(),
                    data: json_bytes,
                };
                payloads.push(payload);
            }
            Some(Payloads { payloads })
        } else {
            None
        };

        // Generate request ID if not provided
        let request_id = params
            .request_id
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        // Build the StartWorkflowExecutionRequest
        let request = StartWorkflowExecutionRequest {
            namespace: self.inner.namespace().to_owned(),
            workflow_id: params.workflow_id.clone(),
            workflow_type: Some(WorkflowType {
                name: params.workflow_type,
            }),
            task_queue: Some(TaskQueue {
                name: params.task_queue,
                kind: 0, // Normal task queue
                normal_name: "".to_string(),
            }),
            input: input_payloads,
            request_id,
            // Skip timeout fields for now - will be added in a future iteration
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: None,
            ..Default::default()
        };

        // Execute the workflow start request
        // Clone the Arc to get a owned RetryClient for mutable access
        let mut client = (*self.inner).clone();
        let response = client
            .start_workflow_execution(request)
            .await
            .map_err(|e| format!("Workflow start failed: {}", e))?;

        let inner_response = response.into_inner();
        Ok(WorkflowHandle {
            run_id: inner_response.run_id.clone(),
            workflow_id: params.workflow_id,
            first_execution_run_id: inner_response.run_id,
        })
    }
}

// No custom Drop implementation needed - sdk-core handles cleanup

#[rustler::resource_impl]
impl rustler::Resource for ClientResource {}
