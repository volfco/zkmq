// use std::sync::Arc;
// use zookeeper::ZooKeeper;
// use anyhow::Result;
// use uuid::Uuid;
// use log::{debug, trace};
//
// const ZKMQ_TASK_GC_SWEEP_INTERVAL: u16 = 600000; // 10 minutes in ms
// const ZKMQ_DEFAULT_TASK_GC_INTERVAL: u16 = 28800000;  // 8 hours in ms
//
// pub struct ZkMQ {
//     zk: Arc<ZooKeeper>,
//     dir: String,
//     id: String,
//
//     acl: Vec<zookeeper::Acl>,
// }
//
// impl ZkMQ {
//     pub fn new(zk: Arc<ZooKeeper>, dir: String, id: Option<String>) -> Result<Self> {
//         let acl = zookeeper::Acl::open_unsafe().clone();
//         let id = match id.is_some() {
//             true  => id.unwrap(),
//             false => Uuid::new_v4().into()
//         };
//         debug!("setting instance id to {}", &id);
//
//         if !zk.exists(&dir, false)? {
//             zk.create(&dir, vec![], acl.clone(), zookeeper::CreateMode::Container)?;
//         }
//
//         for znode in vec!["queue", "tasks", "conf", "lock"] {
//             if !zk.exists(format!("{}/{}", &dir, &znode).into(), false)? {
//                 trace!("{}/{} does not exist. creating", &dir, &znode);
//                 zk.create(format!("{}/{}", &dir, &znode).into(), vec![], acl.clone(), zookeeper::CreateMode::Container)?;
//             } else {
//                 trace!("{}/{} already exists", &dir, &znode);
//             }
//         }
//
//         Ok(Self { zk, dir, id, acl })
//     }
//
//     pub fn register
// }