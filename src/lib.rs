
use chrono::prelude::Utc;
use anyhow::{Result, Context};
use std::time::Instant;
use zookeeper::{ZkResult, ZooKeeper, CreateMode, ZooKeeperExt};
use std::sync::{Arc, Mutex};
use log::{debug, warn, trace, error};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};

pub mod filter;
pub use filter::*;
pub mod message;
pub use message::*;
pub mod path;
pub use path::*;

const ZKMQ_CONSUMER_CONSUMPTION_LIMIT: usize = 12;




pub struct ZkMQBuilder {
    zk: Arc<ZooKeeper>,
    id: Option<String>,
    consumer_started: bool,
    producer_started: bool,
    dir: String,
    acl: Vec<zookeeper::Acl>,
    message_prefix: String,
}
impl ZkMQBuilder {

    pub fn new(zk: Arc<ZooKeeper>) -> Self {
        Self {
            zk,
            id: None,
            consumer_started: false,
            producer_started: false,
            dir: "/zkmq".to_string(),
            acl: zookeeper::Acl::open_unsafe().clone(),
            message_prefix: "zkmq-".to_string()
        }
    }

    pub fn set_base(mut self, base: String) -> Self {
        self.dir = base;
        self
    }

    pub fn set_id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }

    /// Should this instance of ZkMQ enable the consumer
    pub fn consumer(mut self, enable: bool) -> Self {
        self.consumer_started = enable;
        self
    }
    /// Should this instance of ZkMQ enable the producer
    pub fn producer(mut self, enable: bool) -> Self {
        self.producer_started = enable;
        self
    }

    fn build_tree(&self, nodes: Vec<&str>) -> ZkResult<String> {

        fn create_node(zk: Arc<ZooKeeper>, path: &str) -> ZkResult<String> {
            if zk.exists(path, false)?.is_none() {
                debug!("creating {}", path);
                zk.create(path, vec![], zookeeper::Acl::open_unsafe().clone(), zookeeper::CreateMode::Container)
            } else {
                trace!("{} already exists", path);
                Ok("".to_string())
            }
        }

        // create base first
        let paths = self.dir.split('/').collect::<Vec<&str>>();
        let mut working_path = "".to_string();

        for path in paths.iter() {
            if path == &"" { continue; }
            working_path.push('/');
            working_path.push_str(path);

           create_node(self.zk.clone(), working_path.as_str())?;
        }

        // now create nodes in the base
        for node in nodes {
            create_node(self.zk.clone(), format!("{}/{}", &working_path, node).as_str())?;
        }

        Ok("".to_string())
    }

    pub fn build(self) -> Result<ZkMQ> {

        if !self.producer_started && !self.consumer_started {
            warn!("building ZkMQ object that cannot consume nor produce messages");
        }

        let id = self.id.clone().unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        self.build_tree(["queue", "task", "conf", "lock"].to_vec())?;
        let dir = ZkPath::new(self.dir.clone());

        let z = ZkMQ {
            id,
            dir,
            config: self,
            cache: Arc::new(Mutex::new(lru::LruCache::new(1024)))
        };

        Ok(z)

    }
}

pub struct ZkMQ {
    config: ZkMQBuilder,
    id: String,
    dir: ZkPath,
    cache: Arc<Mutex<lru::LruCache<String, Vec<u8>>>>
}
impl ZkMQ {

    // helper functions
    /// Create a ZNode
    fn create_znode(&self, path: ZkPath, data: Vec<u8>) -> ZkResult<String> {
        self.config.zk.create(path.to_string().as_str(), data, self.config.acl.clone(), zookeeper::CreateMode::Persistent)
    }

    // consumer
    fn claim(&self, key: String) -> zookeeper::ZkResult<Vec<u8>> {
        let data = self.config.zk.get_data(&key, false)?;
        self.config.zk.delete(&key, None)?;
        Ok(data.0)
    }

    /// Returns a Vec of the children, in order, of the task znode
    fn ordered_children<W: zookeeper::Watcher + 'static>(&self, watcher: Option<W>) -> Result<Vec<String>> {
        let mut children: Vec<(u64, String)> = Vec::new();
        match watcher {
            Some(w) => self.config.zk.get_children_w(self.dir.join("queue").to_string().as_str(), w),
            None => self.config.zk.get_children(self.dir.join("queue").to_string().as_str(), false) // false I think?
        }?.iter().for_each(|child| {
            // the child names will be like qn-0000001. chop off the prefix, and try and convert the
            // rest to a u64. if it fails, let's ignore it and move on
            if let Ok(index) = child.replace(&self.config.message_prefix, "").parse::<u64>() {
                children.push((index, child.clone()))
            } else {
                warn!("found child with improper name: {}. ignoring", child);
            }
        });
        children.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(children.iter().map(|i| i.1.clone()).collect())
    }

    /// Filter Children.
    ///
    /// Process is to iterate over each given child, then iterate over each filter to check:
    /// 1. if the filter field exists (/<dir>/tasks/<id>/filters/<filter>)
    /// 2. if the filter matches the conditional
    /// Once all filters have been checked, evaluate the conditional. If the filer group is using
    /// FilterConditional::ALL, all filters need to match. If FilterConditional::ANY, only a single
    /// filter needs to match
    fn filter_children(&mut self, children: Vec<String>, filters: &filter::Filters, limit: usize) -> Result<Vec<String>> {
        trace!("filter_children got {} children and {} filters", children.len(), filters.filters.len());
        if children.is_empty() || filters.filters.is_empty() {
            debug!("filter_children was given 0 children or 0 filters, nothing to do");
            return Ok(children);
        }
        let mut valid_children = vec![];

        // loop over each child
        for child in children {
            let child_dir = self.dir.join("task").join(&child).join("filters");

            // TODO can we make this parallel? that might require support in the zookeeper library
            // TODO there should be a more logical way to do this
            let mut filter_results: Vec<bool> = vec![];
            for f in &filters.filters {
                let child_value = self.get_child(child_dir.join(&f.field)).context(format!("looking up the value of filter {} for task {}", &f.field, &child));
                filter_results.push(match child_value {
                    Ok(v) => f.check_match(v)?,
                    Err(_) => false
                });
            }
            let mut valid = 0;
            for fr in &filter_results {
                if *fr { valid += 1; }
            }

            match filters.conditional {
                // if ALL filters must pass, check if the number of filters matches the number of true filters
                filter::FilterConditional::All => if filter_results.len() == valid { valid_children.push(child) },
                // if ANY N filters must pass, check if the number of filter matches is >= then the number of matches required
                filter::FilterConditional::Any(size) => if valid >= size { valid_children.push(child) }
            }

            // if we're over the given "limit" of results, break the loop
            if valid_children.len() >= limit {
                break;
            }
        }
        Ok(valid_children)
    }

    /// Get the value of a ZNode, from either the LRU cache or Zookeeper (and cache it)
    fn get_child(&mut self, path: ZkPath) -> Result<Vec<u8>> {
        let mut handle = self.cache.lock().unwrap();
        if let Some(data) = handle.get(&path.to_string()) {
            trace!("key {} was a cache hit", &path);
            Ok(data.clone())
        } else {
            // we don't have the key in the cache, so get it, put it in the cache, and return it
            let rq = self.config.zk.get_data(&path.to_string(), false).context(format!("looking up {}", &path))?.0;
            let _ = handle.put(path.to_string(), rq.clone());
            trace!("key {} was a cache miss", &path);
            Ok(rq)
        }
    }

    fn build_message(&mut self, raw_claim: Vec<u8>) -> Result<ZkMQMessage> {
        let claim = String::from_utf8(raw_claim).context("parsing raw claim data")?;
        let dir = ZkPath::new(&self.dir).join("task").join(claim);
        trace!("building ZkMQMessage from {}", &dir);

        let metadata: Option<ZkMQMessageMetadata> = match self.config.zk.get_data(&dir.join("metadata").to_string(), false) {
            Ok(d) => serde_json::from_slice(&*d.0).map(|v| { Some(v) }).unwrap_or_else(|_| { None }),
            _ => None
        };

        let id = dir.parts.last().unwrap();
        let mut filters: Vec<(String, Vec<u8>)> = vec![];

        for child in self.config.zk.get_children(&dir.join("filters").to_string(), false).context(format!("getting children of {}", &dir.join("filters")))? {
            filters.push((child.clone(), self.get_child(dir.join("filters").join(child))?))
        }

        let body = self.get_child(dir.join("data"));
        if body.is_err() {
            let error = body.err().unwrap();
            error!("unable to build message body. got error {:?}", &error);
            return Err(error);
        }

        let message = ZkMQMessage {
            id: id.clone(),
            tags: filters,
            body: body.unwrap(),
            meta: metadata
        };

        trace!("successfully built message {}", &dir);
        trace!("{:?}", &message);

        Ok(message)

    }

    /// Consume messages from the Queue
    ///
    /// Optional Constraints can be provided as a Filters struct. Filters are basically a blacklist
    /// on message tags. If a message is tagged with `scope`, and you start a consumer without any
    /// filters, you will get all messages- tagged and untagged.
    ///
    /// If you add a filter on `scope`, messages not matching the filter will be ignored. Messages
    /// missing the `scope` tag will be treated as if they fail to match the filter. So, if your
    /// `FilterConditional` is ALL and the `scope` tag is missing, it will fail. If is ANY, then it
    /// could still be valid.
    pub fn consume(&mut self, constraints: Option<filter::Filters>) -> Result<ZkMQMessage> {
        if !self.config.consumer_started {
            anyhow::bail!("consumer is not started");
        }
        let latch: (SyncSender<bool>, Receiver<bool>) = sync_channel(1);
        loop {
            let start = Instant::now();
            let tx = latch.0.clone();
            let op = self.ordered_children(Some(move |ev| {
                handle_znode_change(&tx, ev)
            }))?;

            debug!("got {} messages during consumption loop", &op.len());

            let children = match constraints {
                Some(ref inner) => self.filter_children(op, inner, ZKMQ_CONSUMER_CONSUMPTION_LIMIT)?,
                None => op
            };

            if !children.is_empty()  {
                // pick the smaller value- the default ZKMQ_CONSUMER_CONSUMPTION_LIMIT or the number
                // of children returned
                let limit = match ZKMQ_CONSUMER_CONSUMPTION_LIMIT > children.len() {
                    true => ZKMQ_CONSUMER_CONSUMPTION_LIMIT,
                    false => children.len()
                };

                for child in children.iter().take(limit) {
                    return match self.claim(format!("{}/queue/{}", self.dir, child)) {
                        // someone else has claimed this task already, so try again
                        Err(e) if e == zookeeper::ZkError::NoNode => continue,
                        Err(e) => Err(e.into()),
                        Ok(claim) => {
                            // TODO We need to handle a transient failure here and either re-try, DLX the task, or something else
                            let inner = self.build_message(claim)?;
                            debug!("consumed message {} ({}). took {}ms", &inner.id, &child, start.elapsed().as_millis());
                            Ok(inner)
                        }
                    };
                }

                // at this point, we can assume we exhausted the `limit`. the assumption is that there
                // are more tasks to to check, but we're working with old data at this point. So,
                // skip back to the top of the routine and try again. If we're out, the latch will
                // block
                continue;
            }

            // otherwise, wait until the handler is called and try this again
            let _ = latch.1.recv().unwrap();
        }
    }


    // Producer Functions
    pub fn produce<P: Into<Vec<u8>>>(&self, payload: P) -> Result<()> {
        if !self.config.producer_started {
            anyhow::bail!("producer is not started");
        }

        let mut msg = ZkMQMessage::new(payload);
        msg.meta = Some(ZkMQMessageMetadata {
            insert_time: Utc::now(),
            src_node: self.id.clone()
        });

        self.product_adv(msg)
    }

    pub fn product_adv(&self, message: ZkMQMessage) -> Result<()> {
        if !self.config.producer_started {
            anyhow::bail!("producer is not started");
        }

        let start = Instant::now();

        debug!("enqueuing message {}", &message.id);

        let path = self.dir.join("task").join(&message.id);

        let tx: Result<()> = {
            let _ = self.create_znode(path.clone(), vec![])?;
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
            let _ = self.config.zk.delete_recursive(&*path.to_string());
            return Err(tx.err().unwrap().context("creating task znode tree"))
        }

        // our task creation operation was successful, now we can insert the task into the queue
        // the message should contain just the ID of this task
        let msg = self.config.zk.create(
            &*format!("{}/queue/{}", self.config.dir, self.config.message_prefix),
            message.id.clone().into_bytes(),
            zookeeper::Acl::open_unsafe().clone(),
            CreateMode::PersistentSequential)?;

        debug!("message {} enqueued. resulting znode: {}. took {}ms", &message.id, msg, start.elapsed().as_millis());
        Ok(())
    }
}

fn handle_znode_change(chan: &SyncSender<bool>, ev: zookeeper::WatchedEvent) {
    if let zookeeper::WatchedEventType::NodeChildrenChanged = ev.event_type {
        let _ = chan.send(true);
    }
}

