use rustler::{Encoder, Env, Term};
use std::future::Future;
use tokio::runtime::Runtime;

/// Helper for executing async operations with proper error handling
/// This provides a consistent pattern for Temporal async operations
pub struct AsyncExecutor {
    runtime: &'static std::sync::Arc<Runtime>,
}

impl AsyncExecutor {
    pub fn new(runtime: &'static std::sync::Arc<Runtime>) -> Self {
        Self { runtime }
    }

    /// Execute an async operation and convert the result to an Elixir term
    /// This handles the common pattern of running async code and returning {:ok, result} | {:error, reason}
    pub fn execute_async<'a, F, T, E>(
        &self,
        env: Env<'a>,
        operation: F,
    ) -> rustler::NifResult<Term<'a>>
    where
        F: Future<Output = Result<T, E>>,
        T: Encoder,
        E: std::fmt::Display,
    {
        match self.runtime.block_on(operation) {
            Ok(result) => {
                let ok_tuple = (rustler::types::atom::ok(), result);
                Ok(ok_tuple.encode(env))
            }
            Err(err) => {
                let error_tuple = (rustler::types::atom::error(), format!("{}", err));
                Ok(error_tuple.encode(env))
            }
        }
    }

    /// Execute an async operation that returns a sanitized error
    /// This version ensures that internal implementation details are not exposed
    pub fn execute_async_sanitized<'a, F, T, E>(
        &self,
        env: Env<'a>,
        operation: F,
        error_context: &str,
    ) -> rustler::NifResult<Term<'a>>
    where
        F: Future<Output = Result<T, E>>,
        T: Encoder,
        E: std::fmt::Display,
    {
        match self.runtime.block_on(operation) {
            Ok(result) => {
                let ok_tuple = (rustler::types::atom::ok(), result);
                Ok(ok_tuple.encode(env))
            }
            Err(err) => {
                // Log the full error for debugging but return sanitized version
                tracing::error!("{}: {}", error_context, err);

                // Return a sanitized error message
                let sanitized_error = sanitize_error_message(&err.to_string(), error_context);
                let error_tuple = (rustler::types::atom::error(), sanitized_error);
                Ok(error_tuple.encode(env))
            }
        }
    }
}

/// Sanitize error messages to avoid exposing internal implementation details
fn sanitize_error_message(error: &str, context: &str) -> String {
    // Common patterns to sanitize
    if error.contains("connection") || error.contains("Connection") {
        format!(
            "{}: Connection failed - check server configuration",
            context
        )
    } else if error.contains("timeout") || error.contains("Timeout") {
        format!("{}: Operation timed out", context)
    } else if error.contains("authentication") || error.contains("Authentication") {
        format!("{}: Authentication failed", context)
    } else if error.contains("permission") || error.contains("Permission") {
        format!("{}: Permission denied", context)
    } else if error.contains("not found") || error.contains("Not found") {
        format!("{}: Resource not found", context)
    } else {
        // For other errors, provide a generic message but log the details
        format!("{}: Operation failed", context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_error_message() {
        assert_eq!(
            sanitize_error_message(
                "tonic::transport::Error: Connection refused",
                "Client connection"
            ),
            "Client connection: Connection failed - check server configuration"
        );

        assert_eq!(
            sanitize_error_message("Request timeout after 30s", "Workflow start"),
            "Workflow start: Operation timed out"
        );

        assert_eq!(
            sanitize_error_message("Some internal error details", "Generic operation"),
            "Generic operation: Operation failed"
        );
    }
}
