use std::fmt::{self, Display};

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Undr9Error>;

#[derive(Debug, Error)]
pub enum Undr9Error {
    #[error("validation error: {0}")]
    Validation(String),
    #[error("conflict detected: {0}")]
    Conflict(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("corruption detected: {0}")]
    Corruption(String),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EdgeId(String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TransactionId(String);

impl NodeId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_identifier("node", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl EdgeId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_identifier("edge", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TransactionId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_identifier("transaction", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Display for EdgeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

pub fn crc32(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

fn validate_identifier(kind: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Undr9Error::Validation(format!(
            "{kind} identifier cannot be empty"
        )));
    }

    let valid = value
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | ':'));

    if !valid {
        return Err(Undr9Error::Validation(format!(
            "{kind} identifier may only contain ASCII letters, digits, '-', '_' or ':'"
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{crc32, EdgeId, NodeId, TransactionId};

    #[test]
    fn rejects_invalid_identifier_characters() {
        let error = NodeId::new("bad id").expect_err("identifier should be rejected");
        assert!(error.to_string().contains("may only contain"));
    }

    #[test]
    fn creates_strongly_typed_ids() {
        let node_id = NodeId::new("node_1").expect("node id should be valid");
        let edge_id = EdgeId::new("edge:1").expect("edge id should be valid");
        let transaction_id = TransactionId::new("tx_1").expect("transaction id should be valid");

        assert_eq!(node_id.as_str(), "node_1");
        assert_eq!(edge_id.as_str(), "edge:1");
        assert_eq!(transaction_id.as_str(), "tx_1");
    }

    #[test]
    fn computes_stable_crc32_checksums() {
        assert_eq!(crc32(b"undr9"), 557574162);
    }
}
