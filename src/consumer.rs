use std::sync::Arc;
use zookeeper::{ZooKeeper};
use anyhow::{Result, Context};
use uuid::Uuid;
use log::{debug, trace, warn};
use std::sync::mpsc::{SyncSender, Receiver, sync_channel};
use crate::producer::ZK_DISTRIBUTEDQUEUE_PREFIX;
use crate::{ZkMQMessage, ZkPath, ZkMQMessageMetadata};
use std::time::Instant;

/// Number of children to check when we're filtering the results.
const ZKMQ_CONSUMER_CONSUMPTION_LIMIT: usize = 12;

// const ZKMQ_TASK_GC_SWEEP_INTERVAL: u32 = 600000; // 10 minutes in ms
// const ZKMQ_DEFAULT_TASK_GC_INTERVAL: u32 = 28800000;  // 8 hours in ms

pub enum FilterValue {
    String(String),
    // Boolean(bool),
    Integer(i32),
    OrderedEnum(String, Vec<String>)
}

pub enum FilterOperator {
    Eq,  // ==
    Gt,  // >
    Lt,  // <
    Ge,  // >=
    Le,  // <=
}

#[derive(PartialEq)]
pub enum FilterConditional {
    /// All filters must pass before the message is consumed
    ALL,
    /// Any of N filters must pass before the message is consumed
    ANY(usize)
}

pub struct Filter {
    pub field: String,
    pub value: FilterValue,
    pub operator: FilterOperator
}
impl Filter {
    pub fn check_match(&self, value: Vec<u8>) -> Result<bool> {
        Ok(match &self.value {
            FilterValue::String(v) => {
                let d = String::from_utf8(value).context("parsing filter field to string")?;
                match &self.operator {
                    FilterOperator::Eq => &d == v,
                    FilterOperator::Gt => &d > v,
                    FilterOperator::Ge => &d >= v,
                    FilterOperator::Lt => &d < v,
                    FilterOperator::Le => &d <= v,
                }
            },
            // FilterValue::Boolean(b) => {
            //     let d = String::from_utf8(claim.0).context("parsing filter field to string")?;
            //     match &self.operator {
            //         FilterOperator::Equal => d == s,
            //         FilterOperator::GreaterThan => d > s,
            //         FilterOperator::LessThan => d < s,
            //     }
            // },
            FilterValue::Integer(v) => {
                let d: i32 = String::from_utf8(value).context("parsing filter field to i32")?.parse()?;
                match &self.operator {
                    FilterOperator::Eq => &d == v,
                    FilterOperator::Gt => &d > v,
                    FilterOperator::Ge => &d >= v,
                    FilterOperator::Lt => &d < v,
                    FilterOperator::Le => &d <= v,
                }
            },
            FilterValue::OrderedEnum(key, values) => {
                let tgt = String::from_utf8(value).context("parsing filter field to string")?;
                let key_index = values.iter().position(|k| k == key);
                let target_index = values.iter().position(|k| k == &tgt);

                if key_index.is_none() || target_index.is_none() { false }
                else {
                    let d = key_index.unwrap();
                    let v = target_index.unwrap();
                    match &self.operator {
                        FilterOperator::Eq => d == v,
                        FilterOperator::Gt => d > v,
                        FilterOperator::Ge => d >= v,
                        FilterOperator::Lt => d < v,
                        FilterOperator::Le => d <= v,
                    }
                }
            }
        })
    }
}

pub struct Filters {
    pub conditional: FilterConditional,
    pub filters: Vec<Filter>
}

pub struct ZkMQConsumer {
    zk: Arc<ZooKeeper>,
    dir: String,
    _id: String,

    _acl: Vec<zookeeper::Acl>,
    cache: lru::LruCache<String, Vec<u8>>
}

impl ZkMQConsumer {
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

        let cache = lru::LruCache::new(1024);

        Ok(Self { zk, dir: dir.to_string(), _id: id, _acl: acl , cache})
    }

    fn claim(&self, key: String) -> zookeeper::ZkResult<Vec<u8>> {
        let data = self.zk.get_data(&key, false)?;
        self.zk.delete(&key, None)?;
        Ok(data.0)
    }

    /// Returns a Vec of the children, in order, of the task znode
    fn ordered_children<W: zookeeper::Watcher + 'static>(&self, watcher: Option<W>) -> Result<Vec<String>> {
        let mut children: Vec<(u64, String)> = Vec::new();
        match watcher {
            Some(w) => self.zk.get_children_w(&*format!("{}/queue", &self.dir), w),
            None => self.zk.get_children(&self.dir, false) // false I think?
        }?.iter().for_each(|child| {
            // the child names will be like qn-0000001. chop off the prefix, and try and convert the
            // rest to a u64. if it fails, let's ignore it and move on
            if let Ok(index) = child.replace(ZK_DISTRIBUTEDQUEUE_PREFIX, "").parse::<u64>() {
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
    fn filter_children(&mut self, children: Vec<String>, filters: &Filters, limit: usize) -> Result<Vec<String>> {
        if children.is_empty() || filters.filters.is_empty() {
            trace!("filter_children was given 0 children or 0 filters, nothing to do");
            return Ok(children);
        }
        let mut valid_children = vec![];
        let dir = ZkPath::new(&self.dir);

        // loop over each child
        for child in children {
            let child_dir = dir.join("task").join(&child).join("filters");

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
                FilterConditional::ALL => if filter_results.len() == valid { valid_children.push(child) },
                // if ANY N filters must pass, check if the number of filter matches is >= then the number of matches required
                FilterConditional::ANY(size) => if valid >= size { valid_children.push(child) }
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
        if let Some(data) = self.cache.get(&path.to_string()) {
            Ok(data.clone())
        } else {
            // we don't have the key in the cache, so get it, put it in the cache, and return it
            let rq = self.zk.get_data(&path.to_string(), false).context(format!("looking up {}", &path))?.0;
            let _ = self.cache.put(path.to_string(), rq.clone());
            Ok(rq)
        }
    }

    fn build_message(&mut self, raw_claim: Vec<u8>) -> Result<ZkMQMessage> {
        let claim = String::from_utf8(raw_claim).context("parsing raw claim data")?;
        let dir = ZkPath::new(&self.dir).join("task").join(claim);
        trace!("building ZkMQMessage from {}", &dir);

        let metadata: Option<ZkMQMessageMetadata> = match self.zk.get_data(&dir.join("metadata").to_string(), false) {
            Ok(d) => serde_json::from_slice(&*d.0).map(|v| { Some(v) }).unwrap_or_else(|_| { None }),
            _ => None
        };

        let id = dir.parts.last().unwrap();
        let mut filters: Vec<(String, Vec<u8>)> = vec![];

        for child in self.zk.get_children(&dir.join("filters").to_string(), false).context(format!("getting children of {}", &dir.join("filters")))? {
            filters.push((child.clone(), self.get_child(dir.join("filters").join(child))?))
        }

        Ok(ZkMQMessage {
            id: id.clone(),
            tags: filters,
            body: self.get_child(dir.join("data"))?,
            meta: metadata
        })

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
    pub fn consume(&mut self, constraints: Option<Filters>) -> Result<ZkMQMessage> {
        let latch: (SyncSender<bool>, Receiver<bool>) = sync_channel(1);
        loop {
            let start = Instant::now();
            let tx = latch.0.clone();
            let op = self.ordered_children(Some(move |ev| {
                handle_znode_change(&tx, ev)
            }))?;

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
}

fn handle_znode_change(chan: &SyncSender<bool>, ev: zookeeper::WatchedEvent) {
    if let zookeeper::WatchedEventType::NodeChildrenChanged = ev.event_type {
        let _ = chan.send(true);
    }
}