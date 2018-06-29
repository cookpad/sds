use std::env;
use std::process::exit;
use std::str;

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate sds;

use sds::storage::StorageImpl;
use sds::types::Config;

// rusoto requires AWS_DEFAULT_REGION env.
fn main() {
    env_logger::init();

    let listen_port = {
        let v = fetch_env_var("PORT");
        parse_uint(v)
    };

    let ttl = {
        let v = fetch_env_var("HOST_TTL");
        parse_uint(v)
    };
    let table_name = fetch_env_var("DDB_TABLE");

    let storage: StorageImpl = StorageImpl {
        table_name: table_name,
        ttl: ttl,
    };
    let c = Config {
        listen_port: listen_port,
    };
    sds::server::run(c, storage);
}

fn fetch_env_var(k: &'static str) -> String {
    match env::var(k) {
        Ok(v) => v,
        Err(_e) => {
            error!("{} env is missing", k);
            exit(1);
        }
    }
}

fn parse_uint<T>(s: String) -> T
where
    T: str::FromStr,
{
    match s.parse() {
        Ok(i) => i,
        Err(_) => {
            error!("env var is invalid: value={}", s);
            exit(1)
        }
    }
}
