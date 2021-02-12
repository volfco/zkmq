use std::sync::Arc;
use zookeeper::{ZooKeeper, ZkResult, CreateMode, ZooKeeperExt};
use anyhow::{Result, Context};
use uuid::Uuid;
use log::{debug, trace};
use crate::{ZkMQMessage, ZkPath, ZkMQMessageMetadata};

use std::convert;
use chrono::Utc;
use std::time::Instant;

/// Prefix to use in /<dir>/queue/<prefix>000000001
pub(crate) const ZK_DISTRIBUTEDQUEUE_PREFIX: &str = "zkmq-";


pub struct ZkMQProducer {
    zk: Arc<ZooKeeper>,
    dir: String,
    id: String,

    acl: Vec<zookeeper::Acl>,
}

impl ZkMQProducer{
    pub fn new(zk: Arc<ZooKeeper>, dir: &str, id: Option<&str>) -> Result<Self> {
        let acl = zookeeper::Acl::open_unsafe().clone();
        let id = match id.is_some() {
            true  => id.unwrap().to_string(),
            false => Uuid::new_v4().to_string()
        };
        debug!("setting instance id to {}", &id);

        if zk.exists(&dir, false)?.is_none() {
            zk.create(&dir, vec![], acl.clone(), zookeeper::CreateMode::Container)?;
        }

        for znode in &["queue", "task", "conf", "lock"] {
            if zk.exists(format!("{}/{}", &dir, &znode).as_str(), false)?.is_none(){
                trace!("{}/{} does not exist. creating", &dir, &znode);
                zk.create(format!("{}/{}", &dir, &znode).as_str(), vec![], acl.clone(), zookeeper::CreateMode::Persistent)?;
            } else {
                trace!("{}/{} already exists", &dir, &znode);
            }
        }

        Ok(Self { zk, dir: dir.to_string(), id, acl })
    }

    fn create_znode<S: ToString>(&self, path: S, data: Vec<u8>) -> Result<String> {
        trace!("creating znode {} ", &path.to_string());
        self.zk.create(&*path.to_string(), data, self.acl.clone(), CreateMode::Persistent).context(format!("creating znode {}", &path.to_string()))
    }

    pub fn produce<P: Into<Vec<u8>>>(&self, payload: P) -> Result<()> {
        let mut msg = ZkMQMessage::new(payload);
        msg.meta = Some(ZkMQMessageMetadata {
            insert_time: Utc::now(),
            src_node: self.id.clone()
        });

        self.product_adv(msg)
    }

    pub fn product_adv(&self, message: ZkMQMessage) -> Result<()> {

        let start = Instant::now();

        debug!("enqueuing message {}", &message.id);

        let path = ZkPath::new(&self.dir).join("task").join(&message.id);

        let tx: Result<()> = {
            let _ = self.create_znode(&path, vec![])?;
            let _ = self.create_znode(path.join("filters"), vec![])?;
            let _ = self.create_znode(path.join("state"), vec![])?;
            let _ = self.create_znode(path.join("data"), message.body)?;
            let _ = self.create_znode(path.join("metadata"), message.meta.map_or_else(|| {vec![]}, |i| serde_json::to_vec(&i).unwrap() ))?;
            // populate filters
            let filter_dir = path.join("filters");
            for filter in message.tags {
                let _ = self.create_znode(filter_dir.join(filter.0), filter.1)?;
            }

            Ok(())
        };

        if tx.is_err() {
            // delete the tree we made and discard the result because we're already broken
            let _ = self.zk.delete_recursive(&*path.to_string());
            return Err(tx.err().unwrap().context("creating task znode tree"))
        }

        // our task creation operation was successful, now we can insert the task into the queue
        // the message should contain just the ID of this task
        let msg = self.zk.create(
            &*format!("{}/queue/{}", self.dir, ZK_DISTRIBUTEDQUEUE_PREFIX),
            message.id.clone().into_bytes(),
            zookeeper::Acl::open_unsafe().clone(),
            CreateMode::PersistentSequential)?;

        debug!("message {} enqueued. resulting znode: {}. took {}ms", &message.id, msg, start.elapsed().as_millis());
        Ok(())
    }
}