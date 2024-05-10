use std::collections::HashMap;

use serde::{Deserialize, ser::SerializeSeq, Serialize, Serializer};
use serde_json::Value;

use super::Params;

/// OVSDB operation to be performed.  Somewhat analgous to a SQL statement.
#[derive(Debug, Deserialize, Serialize)]
pub struct   MonitorRequest {
    /// monitor table columns
    pub columns: Vec<String>,
}

/// Parameters for the `monitor` OVSDB method.
#[derive(Debug, Deserialize)]
pub struct MonitorParams {
    database: String,
    monid: Option<Value>,
    request: HashMap<String, MonitorRequest>,
}
/// monitor update response
#[derive(Debug, Deserialize)]
pub struct MonitorUpdate {
    cond: Option<String>,
    update: HashMap<String, Value>
}

impl MonitorParams {
    /// Create a new set of `transact` parameters.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::collections::HashMap;
    /// use ovsdb::protocol::Map;
    /// use ovsdb::protocol::method::{Operation, MonitorParams, MonitorRequest};
    ///
    /// let op = Operation::Select { table: "Bridges".into(), clauses: vec![] };
    /// let req = MonitorRequest { columns: vec!["type"].iter_mut().map(|x|x.to_string()).collect() };
    /// let params = MonitorParams::new("Open_vSwitch", None, HashMap::from([("Interface".to_string(), req)]));
    /// ```
    pub fn new<T>(database: T, monid: Option<Value>, request: HashMap<String, MonitorRequest>) -> Self
    where
        T: Into<String>,
    {
        Self {
            database: database.into(),
            monid,
            request
        }
    }
}

impl Params for MonitorParams {}

impl Serialize for MonitorParams {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(3))?;
        seq.serialize_element(&self.database)?;
        seq.serialize_element(&self.monid)?;
        seq.serialize_element(&self.request)?;
        seq.end()
    }
}

impl Params for MonitorUpdate {}

impl Serialize for MonitorUpdate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(3))?;
        seq.serialize_element(&self.cond)?;
        seq.serialize_element(&self.update)?;
        seq.end()
    }
}

/// ovsdb monitor cancel request params
#[derive(Debug, Deserialize)]
pub struct MonitorCancel {
    monid: Value,
}

impl Params for MonitorCancel {}

impl MonitorCancel {
    /// ovsdb monitor cancel create helper
    pub fn new(monid: Value) -> Self {
        Self {
            monid,
        }
    }
}

impl Serialize for MonitorCancel {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(1))?;

        seq.serialize_element(&self.monid)?;
        seq.end()
    }
}