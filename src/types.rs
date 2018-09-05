use std::error;
use std::fmt;

pub type ServiceName = String;

pub trait Storage: Send + Sync + Clone + 'static {
    type E: fmt::Display + error::Error;
    fn query_items(&self, name: &ServiceName) -> Result<Vec<Host>, Self::E>;
    fn store_item(&self, name: ServiceName, host: Host) -> Result<(), Self::E>;
    fn delete_item(&self, name: ServiceName, ip: String, port: u64) -> Result<(), Self::E>;
    fn ttl(&self) -> u64;
}

#[derive(Debug, Clone)]
pub struct Config {
    pub listen_port: u16,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Registration {
    pub service: String,
    pub env: String,
    pub hosts: Vec<Host>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Host {
    pub ip_address: String,
    pub port: u16,
    pub last_check_in: String,
    pub expire_time: u64,
    pub revision: String,
    pub service: String,
    pub tags: Tag,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Tag {
    pub az: String,
    pub region: String,
    pub instance_id: String,
    pub canary: bool,
    pub load_balancing_weight: Option<u8>,
}
