use serde::{Deserialize, Serialize, };
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{Error::ParseError, Result};

use super::{method::Method, ResponseResult};

/// Wire-format representation of an OVSDB method call.
#[derive(Debug, Deserialize, Serialize)]
pub struct Update {
    id: Option<super::Uuid>,
    method: Option<Method>,
    params: [Value;2],
}

impl Update {

    /// Data returned by the server in response to a method call.
    pub fn result<T>(&self) -> Result<Option<T>>
        where
            T: DeserializeOwned,
    {
        let v:T = serde_json::from_value(self.params[1].clone()).map_err(ParseError)?;
        Ok(Some(v))
    }
    /// return monitor uuid
    pub fn id(&self) -> Option<super::Uuid> {
        let monid: super::Uuid = match serde_json::from_value(self.params[0].clone()) {
            Ok(id) => id,
            Err(_) => {return None}
        };
        Some(monid)
    }
}

impl ResponseResult for Update {
    fn result_value(&self) -> Result<Option<Value>> {
        Ok(Some(self.params[1].clone()))
    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_request_de() {
        let s = r#"{"id":null,"method":"update","params":[null,{"Interface":{"2123f56a-e492-4022-ab77-d74d0665c101":{"new":{"mtu_request":9216,"type":"patch"},"old":{"mtu_request":1500}}}}]}"#;

        let req = serde_json::from_str::<Update>(s);
        println!("{:?}", req);
        assert!(req.is_ok());


        // let v:Value = req.unwrap().result();



    }

}
