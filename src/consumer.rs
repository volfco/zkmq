use std::sync::Arc;
use zookeeper::{ZooKeeper, Watcher};
use anyhow::{Result, Context};
use uuid::Uuid;
use log::{debug, trace};
use std::sync::mpsc::{SyncSender, Receiver, sync_channel};
use lru;


/// Prefix to use in /<dir>/queue/<prefix>000000001
const ZK_DISTRIBUTEDQUEUE_PREFIX: &str = "zkmq-";

/// Number of children to check when we're filtering the results.
const ZKMQ_CONSUMER_CONSUMPTION_LIMIT: usize = 12;

const ZKMQ_TASK_GC_SWEEP_INTERVAL: u16 = 600000; // 10 minutes in ms
const ZKMQ_DEFAULT_TASK_GC_INTERVAL: u16 = 28800000;  // 8 hours in ms

pub enum FilterValue {
    String(String),
    // Boolean(bool),
    Integer(i32),
}

pub enum FilterOperator {
    Equal,
    GreaterThan,
    LessThan
}

pub enum FilterConditional {
    ALL,
    ANY
}

pub struct Filter {
    pub field: String,
    pub value: FilterValue,
    pub operator: FilterOperator
}
impl Filter {
    pub fn check_match(&self, o: &mut ZkMQConsumer, task: &String) -> Result<bool> {
        let path = format!("{}/tasks/{}", o.dir, task);
        // loop up the key in the lru cache- because we might have seen it before
        let value = match o.cache.get(&path) {
            None => match o.zk.get_data(path.as_str(), false) {
                Err(e) if e == zookeeper::ZkError::NoNode => return Ok(false),
                Err(e) => return Err(e.into()),
                Ok(claim) => claim.0
            },
            Some(val) => val.to_owned()
        };

        Ok(match &self.value {
            FilterValue::String(s) => {
                let d = String::from_utf8(value).context("parsing filter field to string")?;
                match &self.operator {
                    FilterOperator::Equal => d == s,
                    FilterOperator::GreaterThan => d > s,
                    FilterOperator::LessThan => d < s,
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
            FilterValue::Integer(i) => {
                let d: i32 = i32::from(value).context("parsing filter field to i32")?;
                match &self.operator {
                    FilterOperator::Equal => &d == i,
                    FilterOperator::GreaterThan => &d > i,
                    FilterOperator::LessThan => &d < i,
                }
            },
        })l
    }
}

pub struct Filters {
    pub conditional: FilterConditional,
    pub filters: Vec<Filter>
}

pub struct ZkMQConsumer {
    zk: Arc<ZooKeeper>,
    dir: String,
    id: String,

    acl: Vec<zookeeper::Acl>,
    cache: lru::LruCache<String, Vec<u8>>
}

impl ZkMQConsumer {
    pub fn new(zk: Arc<ZooKeeper>, dir: String, id: Option<String>) -> Result<Self> {
        let acl = zookeeper::Acl::open_unsafe().clone();
        let id = match id.is_some() {
            true  => id.unwrap(),
            false => Uuid::new_v4().into()
        };
        debug!("setting instance id to {}", &id);

        if !zk.exists(&dir, false)? {
            zk.create(&dir, vec![], acl.clone(), zookeeper::CreateMode::Container)?;
        }

        for znode in vec!["queue", "tasks", "conf", "lock"] {
            if !zk.exists(format!("{}/{}", &dir, &znode).into(), false)? {
                trace!("{}/{} does not exist. creating", &dir, &znode);
                zk.create(format!("{}/{}", &dir, &znode).into(), vec![], acl.clone(), zookeeper::CreateMode::Container)?;
            } else {
                trace!("{}/{} already exists", &dir, &znode);
            }
        }

        let cache = lru::LruCache::new(1024);

        Ok(Self { zk, dir, id, acl , cache})
    }

    fn claim(&self, key: String) -> Result<Vec<u8>> {
        let data = self.zk.get_data(&key, false)?;
        self.zk.delete(&key, None)?;
        Ok(data.0)
    }

    /// Returns a Vec of the children, in order, of the task znode
    fn ordered_children<W: zookeeper::Watcher + 'static>(&self, watcher: Option<W>) -> Result<Vec<String>> {
        let mut children: Vec<(u64, String)> = Vec::new();
        match watcher {
            Some(w) => self.zk.get_children_w(&self.dir, w),
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
    /// 1. if the filter field exists (/<dir>/tasks/<id>/filters/<filter>
    /// 2. if the filter matches the conditional
    /// Once all filters have been checked, evaluate the conditional. If the filer group is using
    /// FilterConditional::ALL, all filters need to match. If FilterConditional::ANY, only a single
    /// filter needs to match
    fn filter_children(&self, children: Vec<String>, filters: Filters, limit: usize) -> Result<Vec<String>> {
        if children.is_empty() || filters.is_empty() {
            trace!("filter_children was given 0 children or 0 filters, nothing to do");
            return Ok(children);
        }
        let mut valid_children = vec![];

        // loop over each child
        for child in children {

            // TODO can we make this parallel? that might require support in the zookeeper library
            // TODO there should be a more logical way to do this
            let filter_results: Vec<bool> = filters.filters.iter().map(|f| f.check_match(&self, &child)).collect();
            let mut valid = 0;
            for fr in filter_results {
                if fr { valid += 1; }
            }

            // if we're doing AND, and we have as many valid filters as filters- we can say this child matches
            if filters.conditional == FilterConditional::ALL && filter_results.len() == valid {
                valid_children.push(child);
            } else if filters.conditional == FilterConditional::ANY && !filter_results.is_empty() && valid > 0 {
                valid_children.push(child);
            }

            // if we're over the given "limit" of results, break the loop
            if valid_children.len() >= limit {
                break;
            }
        }
        Ok(valid_children)
    }

    pub fn consume(&self, constraints: Option<Filters>) -> Result<Vec<u8>> {
        let latch: (SyncSender<bool>, Receiver<bool>) = sync_channel(1);
        loop {
            let tx = latch.0.clone();
            let op = self.ordered_children(Some(move |ev| {
                handle_znode_change(&tx, ev)
            }))?;

            let children = match constraints.is_some() {
                true => self.filter_children(op, constraints.unwrap(), ZKMQ_CONSUMER_CONSUMPTION_LIMIT),
                false => op
            };

            if !children.is_empty()  {
                // pick the smaller value- the default ZKMQ_CONSUMER_CONSUMPTION_LIMIT or the number
                // of children returned
                let limit = match ZKMQ_CONSUMER_CONSUMPTION_LIMIT > children.len() {
                    true => ZKMQ_CONSUMER_CONSUMPTION_LIMIT,
                    false => children.len()
                };

                for i in 0..limit {
                    return match self.claim(format!("{}/queue/{}", self.dir, children[i])) {
                        // someone else has claimed this task already, so try again
                        Err(e) if e == zookeeper::ZkError::NoNode => continue,
                        Err(e) => Err(e),
                        Ok(claim) => Ok(claim)
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