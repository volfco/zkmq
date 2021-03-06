
use zkmq;
use zookeeper;
use anyhow;
use std::time::Duration;
use std::env;
use std::sync::Arc;

struct NoopWatcher;

impl zookeeper::Watcher for NoopWatcher {
    fn handle(&self, _ev: zookeeper::WatchedEvent) {}
}

fn zk_server_urls() -> String {
    let key = "ZOOKEEPER_SERVERS";
    match env::var(key) {
        Ok(val) => val,
        Err(_) => "localhost:2181".to_string(),
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let zk_urls = zk_server_urls();

    let zk = zookeeper::ZooKeeper::connect(&*zk_urls, Duration::from_millis(2500), NoopWatcher).unwrap();

    let zkmq = zkmq::ZkMQBuilder::new(Arc::new(zk))
        .consumer(false)
        .producer(true)
        .build()?;

    let r = zkmq.produce("hello world".to_string())?;
    println!("{:?}", r);

    Ok(())
}