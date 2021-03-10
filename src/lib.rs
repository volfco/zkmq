use std::fmt;
use serde::{Deserialize, Serialize};
use chrono::prelude::{DateTime, Utc};

pub mod consumer;
pub mod producer;

#[derive(Debug, Serialize, Deserialize)]
pub struct ZkMQMessageMetadata {
    insert_time: DateTime<Utc>,
    src_node: String
}

#[derive(Debug)]
pub struct ZkMQMessage {
    id: String,
    pub tags: Vec<(String, Vec<u8>)>,
    pub body: Vec<u8>,
    pub meta: Option<ZkMQMessageMetadata>,
}
impl ZkMQMessage {
    pub fn new<B: Into<Vec<u8>>>(body: B) -> Self {
        ZkMQMessage {
            id: uuid::Uuid::new_v4().to_string(),
            tags: vec![],
            body: body.into(),
            meta: None
        }
    }

    pub fn set_tag<S: ToString, B: Into<Vec<u8>>>(&mut self, key: S, value: B) {
        self.tags.push((key.to_string(), value.into()))
    }

}

#[derive(Clone, Debug)]
pub struct ZkPath {
    parts: Vec<String>
}
impl ZkPath {
    pub fn new<R: ToString>(base: R) -> Self {
        Self { parts: base.to_string().split('/').map(|s| s.to_string()).collect() }
    }
    pub fn join<D: ToString>(&self, dir: D) -> Self {
        let mut parts = self.parts.clone();
        parts.push(dir.to_string());
        ZkPath {
            parts
        }
    }
}
impl fmt::Display for ZkPath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.parts.join("/"))
    }
}
impl Into<String> for ZkPath {
    fn into(self) -> String {
        self.parts.join("/")
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
