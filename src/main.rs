use log::error;
use std::env;
use std::process::exit;
use std::str;

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
    let dynamodb_client = rusoto_dynamodb::DynamoDbClient::new(Default::default());

    let storage = StorageImpl {
        table_name: table_name,
        ttl: ttl,
        dynamodb_client: dynamodb_client,
        timeout: get_timeout(),
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

fn get_timeout() -> std::time::Duration {
    const DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

    std::env::var("DDB_TIMEOUT_SEC")
        .ok()
        .and_then(|timeout_str| match timeout_str.parse() {
            Ok(timeout_sec) => {
                log::info!("set DynamoDB API timeout to {}", timeout_sec);
                Some(std::time::Duration::from_secs(timeout_sec))
            }
            Err(e) => {
                log::warn!("unable to parse DDB_TIMEOUT_SEC into integer: {}", e);
                None
            }
        })
        .unwrap_or(DEFAULT_TIMEOUT)
}
