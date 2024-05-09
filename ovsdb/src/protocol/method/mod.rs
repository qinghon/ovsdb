//! Available OVSDB methods.

use erased_serde::Serialize as ErasedSerialize;
use serde::{Deserialize, Serialize, Serializer};

pub use echo::{EchoParams, EchoResult};
pub use get_schema::{GetSchemaParams, GetSchemaResult};
pub use list_dbs::ListDbsResult;
pub use monitor::{MonitorParams, MonitorRequest, MonitorUpdate, MonitorCancel};
pub use transact::{Operation, TransactParams};

mod echo;
mod get_schema;
mod list_dbs;
mod transact;
mod monitor;

/// OVSDB method.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize)]
pub enum Method {
    /// OVSDB `echo` method.
    #[serde(rename = "echo")]
    Echo,
    /// OVSDB `list_dbs` method.
    #[serde(rename = "list_dbs")]
    ListDatabases,
    /// OVSDB `get_schema` method.
    #[serde(rename = "get_schema")]
    GetSchema,
    /// OVSDB `transact` method.
    #[serde(rename = "transact")]
    Transact,
    // Cancel,
    #[serde(rename = "monitor")]
    Monitor,
    #[serde(rename = "update")]
    Update,
    #[serde(rename = "monitor_cancel")]
    MonitorCancel,
    // Lock,
    // Steal,
    // Unlock,
    // Locked,
    // Stolen,
}

impl Serialize for Method {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let method = match self {
            Self::Echo => "echo",
            Self::ListDatabases => "list_dbs",
            Self::GetSchema => "get_schema",
            Self::Transact => "transact",
            Self::Monitor => "monitor",
            Self::Update => "update",
            Self::MonitorCancel => "monitor_cancel",
        };
        method.serialize(serializer)
    }
}

impl TryFrom<String> for Method {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "echo" => Ok(Self::Echo),
            "list_dbs" => Ok(Self::ListDatabases),
            "get_schema" => Ok(Self::GetSchema),
            "transact" => Ok(Self::Transact),
            "monitor" => Ok(Self::Monitor),
            "update" => Ok(Self::Update),
            "monitor_cancel" => Ok(Self::MonitorCancel),
            _ => Err(format!("Invalid method: {}", value)),
        }
    }
}

/// Trait specifying requirements for a valid OVSDB wire request.
///
/// Primary exists to ensure type-safety.
pub trait Params: ErasedSerialize + Send + std::fmt::Debug {}
erased_serde::serialize_trait_object!(Params);
