use std::fmt;

#[derive(Clone, Debug)]
pub struct ZkPath {
    pub parts: Vec<String>
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