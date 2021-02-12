use std::fmt;
use serde::{Deserialize, Serialize};
use chrono::prelude::{DateTime, Utc};
use zookeeper::ZooKeeper;
use std::sync::Arc;
use anyhow::{Result, Context};

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
    filters: Vec<(String, Vec<u8>)>,
    body: Vec<u8>,
    meta: Option<ZkMQMessageMetadata>,
}
impl ZkMQMessage {
    fn new<B: Into<Vec<u8>>>(body: B) -> Self {
        ZkMQMessage {
            id: uuid::Uuid::new_v4().to_string(),
            filters: vec![],
            body: body.into(),
            meta: None
        }
    }

    pub fn set_filter<S: ToString, B: Into<Vec<u8>>>(&mut self, key: S, value: B) {
        self.filters.push((key.to_string(), value.into()))
    }

}

pub struct ZkPath {
    parts: Vec<String>
}
impl ZkPath {
    fn new<R: ToString>(base: R) -> Self {
        Self { parts: base.to_string().split('/').map(|s| s.to_string()).collect() }
    }
    fn join<D: ToString>(&self, dir: D) -> Self {
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


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
