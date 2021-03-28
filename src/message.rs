use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ZkMQMessageMetadata {
    pub insert_time: DateTime<Utc>,
    pub src_node: String
}

#[derive(Debug)]
pub struct ZkMQMessage {
    pub id: String,
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