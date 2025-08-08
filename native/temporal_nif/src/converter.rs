#![allow(clippy::wrong_self_convention)] // from_payload follows Temporal SDK patterns

use base64::{engine::general_purpose::STANDARD, Engine as _};
use std::collections::HashMap;
use temporal_sdk_core::protos::temporal::api::common::v1::Payload;

/// Error types for payload conversion
#[derive(Debug, thiserror::Error)]
pub enum ConversionError {
    #[error("Unsupported type: {type_name}")]
    #[allow(dead_code)] // Will be used for future type validation
    UnsupportedType { type_name: String },
    #[error("Serialization failed: {source}")]
    SerializationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("Depth limit exceeded (max: {max})")]
    DepthLimitExceeded { max: usize },
    #[error("Binary data too large: {size} bytes (max: {max})")]
    BinaryTooLarge { size: usize, max: usize },
    #[error("Deserialization failed: {reason}")]
    DeserializationFailed { reason: String },
    #[error("No suitable converter found")]
    NoConverterFound,
}

/// Trait for payload converters that can transform Elixir terms to/from Temporal Payloads
pub trait PayloadConverter: Send + Sync {
    /// Convert Elixir term to Temporal Payload
    /// Returns None if this converter cannot handle the data type
    #[allow(dead_code)] // Used by individual converters, not called directly
    fn to_payload(&self, data: &serde_json::Value) -> Result<Option<Payload>, ConversionError>;

    /// Convert Temporal Payload back to JSON value
    /// Returns None if this converter cannot handle the payload encoding
    fn from_payload(&self, payload: &Payload)
        -> Result<Option<serde_json::Value>, ConversionError>;

    /// Returns the encoding identifier for this converter
    fn encoding(&self) -> &'static str;

    /// Returns priority order (lower = higher priority)
    fn priority(&self) -> u32;

    /// Test if this converter can handle the given data type
    #[allow(dead_code)] // Used by individual converters for validation
    fn can_convert(&self, data: &serde_json::Value) -> bool;
}

/// Nil/null value converter
pub struct NilConverter;

impl PayloadConverter for NilConverter {
    fn to_payload(&self, data: &serde_json::Value) -> Result<Option<Payload>, ConversionError> {
        if data.is_null() {
            let mut metadata = HashMap::new();
            metadata.insert("encoding".to_string(), "binary/null".as_bytes().to_vec());
            metadata.insert("sdk".to_string(), "elixir".as_bytes().to_vec());

            Ok(Some(Payload {
                metadata,
                data: Vec::new(),
            }))
        } else {
            Ok(None)
        }
    }

    fn from_payload(
        &self,
        payload: &Payload,
    ) -> Result<Option<serde_json::Value>, ConversionError> {
        if let Some(encoding) = payload.metadata.get("encoding") {
            if encoding == "binary/null".as_bytes() {
                return Ok(Some(serde_json::Value::Null));
            }
        }
        Ok(None)
    }

    fn encoding(&self) -> &'static str {
        "binary/null"
    }

    fn priority(&self) -> u32 {
        10
    }

    fn can_convert(&self, data: &serde_json::Value) -> bool {
        data.is_null()
    }
}

/// Binary data converter
pub struct BinaryConverter {
    pub max_size: usize,
}

impl BinaryConverter {
    pub fn new(max_size: usize) -> Self {
        Self { max_size }
    }
}

impl PayloadConverter for BinaryConverter {
    fn to_payload(&self, data: &serde_json::Value) -> Result<Option<Payload>, ConversionError> {
        if let Some(s) = data.as_str() {
            // Check if this looks like base64 encoded binary data
            if let Ok(binary_data) = STANDARD.decode(s) {
                if binary_data.len() > self.max_size {
                    return Err(ConversionError::BinaryTooLarge {
                        size: binary_data.len(),
                        max: self.max_size,
                    });
                }

                let mut metadata = HashMap::new();
                metadata.insert("encoding".to_string(), "binary/plain".as_bytes().to_vec());
                metadata.insert("sdk".to_string(), "elixir".as_bytes().to_vec());

                return Ok(Some(Payload {
                    metadata,
                    data: binary_data,
                }));
            }
        }
        Ok(None)
    }

    fn from_payload(
        &self,
        payload: &Payload,
    ) -> Result<Option<serde_json::Value>, ConversionError> {
        if let Some(encoding) = payload.metadata.get("encoding") {
            if encoding == "binary/plain".as_bytes() {
                let base64_encoded = STANDARD.encode(&payload.data);
                return Ok(Some(serde_json::Value::String(base64_encoded)));
            }
        }
        Ok(None)
    }

    fn encoding(&self) -> &'static str {
        "binary/plain"
    }

    fn priority(&self) -> u32 {
        20
    }

    fn can_convert(&self, data: &serde_json::Value) -> bool {
        if let Some(s) = data.as_str() {
            STANDARD.decode(s).is_ok()
        } else {
            false
        }
    }
}

/// Deterministic JSON converter
pub struct JsonConverter {
    pub max_depth: usize,
    pub sort_keys: bool,
}

impl JsonConverter {
    pub fn new(max_depth: usize, sort_keys: bool) -> Self {
        Self {
            max_depth,
            sort_keys,
        }
    }

    /// Convert JSON value to deterministic bytes
    fn to_deterministic_json(&self, value: &serde_json::Value) -> Result<Vec<u8>, ConversionError> {
        if self.sort_keys {
            // Use canonical JSON serialization with sorted keys
            self.serialize_canonical(value)
        } else {
            serde_json::to_vec(value).map_err(|e| ConversionError::SerializationFailed {
                source: Box::new(e),
            })
        }
    }

    fn serialize_canonical(&self, value: &serde_json::Value) -> Result<Vec<u8>, ConversionError> {
        // Recursively canonicalize the entire value structure
        let canonicalized = Self::canonicalize_value(value)?;
        serde_json::to_vec(&canonicalized).map_err(|e| ConversionError::SerializationFailed {
            source: Box::new(e),
        })
    }

    fn canonicalize_value(value: &serde_json::Value) -> Result<serde_json::Value, ConversionError> {
        match value {
            serde_json::Value::Object(map) => {
                // Sort keys and recursively canonicalize values
                let mut sorted_pairs: Vec<_> = map.iter().collect();
                sorted_pairs.sort_by(|a, b| a.0.cmp(b.0));

                let sorted_map: Result<
                    serde_json::Map<String, serde_json::Value>,
                    ConversionError,
                > = sorted_pairs
                    .into_iter()
                    .map(|(k, v)| {
                        let canonical_v = Self::canonicalize_value(v)?;
                        Ok((k.clone(), canonical_v))
                    })
                    .collect();

                Ok(serde_json::Value::Object(sorted_map?))
            }
            serde_json::Value::Array(arr) => {
                // Recursively canonicalize array elements
                let canonical_elements: Result<Vec<serde_json::Value>, ConversionError> =
                    arr.iter().map(Self::canonicalize_value).collect();

                Ok(serde_json::Value::Array(canonical_elements?))
            }
            _ => {
                // Primitive values (null, bool, number, string) are already canonical
                Ok(value.clone())
            }
        }
    }

    fn validate_depth(
        &self,
        value: &serde_json::Value,
        current_depth: usize,
    ) -> Result<(), ConversionError> {
        if current_depth >= self.max_depth {
            return Err(ConversionError::DepthLimitExceeded {
                max: self.max_depth,
            });
        }

        match value {
            serde_json::Value::Array(arr) => {
                for item in arr {
                    self.validate_depth(item, current_depth + 1)?;
                }
            }
            serde_json::Value::Object(obj) => {
                for (_, v) in obj {
                    self.validate_depth(v, current_depth + 1)?;
                }
            }
            _ => {}
        }

        Ok(())
    }
}

impl PayloadConverter for JsonConverter {
    fn to_payload(&self, data: &serde_json::Value) -> Result<Option<Payload>, ConversionError> {
        // Skip if it's null (handled by NilConverter)
        if data.is_null() {
            return Ok(None);
        }

        // Validate depth to prevent stack overflow
        self.validate_depth(data, 0)?;

        // Convert to deterministic JSON bytes
        let json_bytes = self.to_deterministic_json(data)?;

        let mut metadata = HashMap::new();
        metadata.insert("encoding".to_string(), "json/plain".as_bytes().to_vec());
        metadata.insert(
            "contentType".to_string(),
            "application/json".as_bytes().to_vec(),
        );
        metadata.insert("sdk".to_string(), "elixir".as_bytes().to_vec());

        Ok(Some(Payload {
            metadata,
            data: json_bytes,
        }))
    }

    fn from_payload(
        &self,
        payload: &Payload,
    ) -> Result<Option<serde_json::Value>, ConversionError> {
        if let Some(encoding) = payload.metadata.get("encoding") {
            if encoding == "json/plain".as_bytes() {
                let json_value = serde_json::from_slice(&payload.data).map_err(|e| {
                    ConversionError::DeserializationFailed {
                        reason: e.to_string(),
                    }
                })?;
                return Ok(Some(json_value));
            }
        }
        Ok(None)
    }

    fn encoding(&self) -> &'static str {
        "json/plain"
    }

    fn priority(&self) -> u32 {
        100
    }

    fn can_convert(&self, _data: &serde_json::Value) -> bool {
        // JSON converter can handle any JSON-serializable data as fallback
        true
    }
}

/// Composite converter that tries converters in priority order
pub struct CompositeConverter {
    converters: Vec<Box<dyn PayloadConverter>>,
}

impl CompositeConverter {
    pub fn new(mut converters: Vec<Box<dyn PayloadConverter>>) -> Self {
        // Sort by priority (lower values = higher priority)
        converters.sort_by_key(|c| c.priority());
        Self { converters }
    }

    /// Create default converter chain matching other Temporal SDKs
    pub fn default() -> Self {
        let converters: Vec<Box<dyn PayloadConverter>> = vec![
            Box::new(NilConverter),
            Box::new(BinaryConverter::new(1024 * 1024)), // 1MB max
            Box::new(JsonConverter::new(32, true)),      // 32 levels, sorted keys
        ];
        Self::new(converters)
    }

    /// Convert a list of JSON values to payloads
    #[allow(dead_code)] // Will be integrated with NIFs in future phase
    pub fn to_payloads(
        &self,
        values: &[serde_json::Value],
    ) -> Result<Vec<Payload>, ConversionError> {
        let mut payloads = Vec::with_capacity(values.len());

        for value in values {
            let payload = self.to_payload(value)?;
            payloads.push(payload);
        }

        Ok(payloads)
    }

    /// Convert payloads back to JSON values
    #[allow(dead_code)] // Will be integrated with NIFs in future phase
    pub fn from_payloads(
        &self,
        payloads: &[Payload],
    ) -> Result<Vec<serde_json::Value>, ConversionError> {
        let mut values = Vec::with_capacity(payloads.len());

        for payload in payloads {
            let value = self.from_payload(payload)?;
            values.push(value);
        }

        Ok(values)
    }

    /// Convert single JSON value to payload using converter chain
    #[allow(dead_code)] // Will be integrated with NIFs in future phase
    pub fn to_payload(&self, data: &serde_json::Value) -> Result<Payload, ConversionError> {
        for converter in &self.converters {
            match converter.to_payload(data) {
                Ok(Some(payload)) => return Ok(payload),
                Ok(None) => continue,
                Err(_) => continue, // Skip converters that error, try next one
            }
        }

        Err(ConversionError::NoConverterFound)
    }

    /// Convert single payload back to JSON value
    pub fn from_payload(&self, payload: &Payload) -> Result<serde_json::Value, ConversionError> {
        // First try to select converter by encoding metadata
        if let Some(encoding) = payload.metadata.get("encoding") {
            let encoding_str = String::from_utf8_lossy(encoding);

            for converter in &self.converters {
                if converter.encoding() == encoding_str {
                    if let Some(value) = converter.from_payload(payload)? {
                        return Ok(value);
                    }
                }
            }
        }

        // Fallback: try all converters
        for converter in &self.converters {
            match converter.from_payload(payload)? {
                Some(value) => return Ok(value),
                None => continue,
            }
        }

        Err(ConversionError::NoConverterFound)
    }
}
