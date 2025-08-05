use temporal_client::{Client, ClientOptionsBuilder, RetryClient, TlsConfig};
use url::Url;

/// Elixir client resource wrapping sdk-core's RetryClient<Client>
pub struct ClientResource {
    #[allow(dead_code)]
    inner: RetryClient<Client>,
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
                let ca_data = std::fs::read(&ca_path)
                    .map_err(|e| format!("Failed to read CA cert file {}: {}", ca_path, e))?;
                tls_cfg.server_root_ca_cert = Some(ca_data);
            }

            // Read client cert and key if both provided
            if let (Some(cert_path), Some(key_path)) =
                (tls_config.client_cert_path, tls_config.client_key_path)
            {
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

        Ok(Self { inner: client })
    }
}

// No custom Drop implementation needed - sdk-core handles cleanup

#[rustler::resource_impl]
impl rustler::Resource for ClientResource {}
