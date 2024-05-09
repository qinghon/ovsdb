use std::convert::From;

use serde::{
    de::{self, Deserializer, MapAccess, Visitor},
    Deserialize,
    ser::Serializer, Serialize,
};

use super::{Request, Response, Update};

/// A single wire-protocol message exchanged between OVSDB client and server.
#[derive(Debug)]
pub enum Message {
    /// A single request message.
    Request(Request),
    /// A single response message.
    Response(Response),
    /// multi respone message.
    Update(Update)
}

impl From<Request> for Message {
    fn from(value: Request) -> Self {
        Self::Request(value)
    }
}

impl From<Response> for Message {
    fn from(value: Response) -> Self {
        Self::Response(value)
    }
}

impl Serialize for Message {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Response(r) => r.serialize(serializer),
            Self::Request(r) => r.serialize(serializer),
            Self::Update(r) => r.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Message {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MessageVisitor;

        impl<'de> Visitor<'de> for MessageVisitor {
            type Value = Message;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("`object`")
            }

            fn visit_map<S>(self, mut map: S) -> Result<Self::Value, S::Error>
            where
                S: MapAccess<'de>,
            {
                let mut target = serde_json::Map::new();

                while let Some((k, v)) = map.next_entry()? {
                    let key: String = k;
                    target.insert(key, v);
                }

                match target.get("method") {

                    Some(v) => {
                        match v.as_str() {
                            None => {
                                let req: super::Request =
                                    serde_json::from_value(serde_json::Value::Object(target))
                                        .map_err(de::Error::custom)?;
                                Ok(Message::Request(req))
                            }
                            Some("update") => {
                                let req: super::Update =
                                    serde_json::from_value(serde_json::Value::Object(target))
                                        .map_err(de::Error::custom)?;
                                Ok(Message::Update(req))
                            }
                            _ => {
                                let req: super::Request =
                                    serde_json::from_value(serde_json::Value::Object(target))
                                        .map_err(de::Error::custom)?;
                                Ok(Message::Request(req))
                            }
                        }

                        // Ok(res)
                    }
                    None => {
                        let res: super::Response =
                            serde_json::from_value(serde_json::Value::Object(target))
                                .map_err(de::Error::custom)?;
                        Ok(Message::Response(res))
                    }
                }
            }
        }

        deserializer.deserialize_map(MessageVisitor)
    }
}
