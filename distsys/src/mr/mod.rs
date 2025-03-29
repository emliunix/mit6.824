use std::pin::Pin;

pub mod rpc;
pub mod worker;
pub mod master;

pub struct KeyValue {
    pub key: String,
    pub value: String,
}

pub trait MRApp {
    fn map(&self, key: String, value: String) -> Pin<Box<dyn Future<Output=Result<Vec<KeyValue>, anyhow::Error>> + 'static>>;
    fn reduce(&self, key: String, value: Vec<String>) -> Pin<Box<dyn Future<Output=Result<String, anyhow::Error>> + 'static>>;
}
