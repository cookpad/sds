use std::collections::HashMap;
use std::error;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use log::info;
use rusoto_dynamodb::{AttributeValue, DeleteItemInput, PutItemInput, QueryInput};

use super::types::{Host, Storage, Tag};

#[derive(Debug, Clone)]
enum ErrorKind {
    ApiError,
    DataError,
    SystemError,
}

#[derive(Debug, Clone)]
pub struct StorageError {
    kind: ErrorKind,
    msg: String,
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl error::Error for StorageError {
    fn cause(&self) -> Option<&error::Error> {
        // TODO
        None
    }
}

#[derive(Clone)]
pub struct StorageImpl<DynamoDb> {
    pub table_name: String,
    pub ttl: u64,
    pub dynamodb_client: DynamoDb,
    pub timeout: std::time::Duration,
}

impl<DynamoDb> Storage for StorageImpl<DynamoDb>
where
    DynamoDb: rusoto_dynamodb::DynamoDb + Send + Sync + Clone + 'static,
{
    type E = StorageError;

    fn query_items(&self, name: &str) -> Result<Vec<Host>, Self::E> {
        let mut hosts = Vec::new();
        let mut last_evaluated_key: Option<HashMap<String, AttributeValue>> = None;
        let table_name = self.table_name.to_owned();
        let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(v) => v,
            Err(_) => {
                return Err(StorageError {
                    kind: ErrorKind::SystemError,
                    msg: "Cloud not fetch system time".to_owned(),
                })
            }
        };
        let epoch_now = now.as_secs();

        loop {
            let tn = table_name.to_owned();
            let mut query_input = build_query_input(tn, &name);
            query_input.exclusive_start_key = last_evaluated_key;
            let res = match self
                .dynamodb_client
                .query(query_input)
                .with_timeout(self.timeout)
                .sync()
            {
                Ok(res) => res,
                Err(e) => {
                    return Err(StorageError {
                        kind: ErrorKind::ApiError,
                        msg: format!("API Error in query: {}", e.to_string()),
                    })
                }
            };
            last_evaluated_key = res.last_evaluated_key;
            let items = res.items.expect("items of query result is missing");
            for h in items {
                let host = convert_ddb_host_to_domain_host(&name, h)?;
                if host.expire_time >= epoch_now {
                    hosts.push(host);
                } else {
                    info!(
                        "Expired host found: service={}, ip={}, port={}, expire_time={}, now={}",
                        name, host.ip_address, host.port, host.expire_time, epoch_now
                    );
                }
            }
            if last_evaluated_key.is_none() {
                break;
            }
        }
        info!(
            "query_items(): succeed to return hosts: service={}, hosts-size={}",
            name,
            hosts.len()
        );
        Ok(hosts)
    }

    fn store_item(&self, name: &str, host: Host) -> Result<(), Self::E> {
        let table_name = self.table_name.to_owned();
        let ip = host.ip_address.to_owned();
        let port = host.port;

        if let Err(e) = self
            .dynamodb_client
            .put_item(build_put_item_input(table_name, &name, host))
            .with_timeout(self.timeout)
            .sync()
        {
            Err(StorageError {
                kind: ErrorKind::ApiError,
                msg: format!("API Error in put_item: {}", e.to_string()),
            })
        } else {
            info!(
                "store_item(): succeed to store item: service={}, ip={}, port={}",
                name, ip, port
            );
            Ok(())
        }
    }

    fn delete_item(&self, name: &str, ip: String, port: u64) -> Result<Option<Host>, Self::E> {
        let table_name = self.table_name.to_owned();

        match self
            .dynamodb_client
            .delete_item(build_delete_item_input(table_name, name, &ip, port))
            .with_timeout(self.timeout)
            .sync()
        {
            Ok(out) => {
                info!(
                    "delete_item(): succeed to delete_item item: service={}, ip={}, port={}",
                    name, ip, port
                );
                match out.attributes {
                    Some(m) => {
                        let h = convert_ddb_host_to_domain_host(name, m)?;
                        let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
                            Ok(v) => v,
                            Err(_) => {
                                return Err(StorageError {
                                    kind: ErrorKind::SystemError,
                                    msg: "Cloud not fetch system time".to_owned(),
                                })
                            }
                        };
                        if h.expire_time >= now.as_secs() {
                            return Ok(Some(h));
                        } else {
                            return Ok(None);
                        }
                    }
                    None => return Ok(None),
                }
            }
            Err(e) => {
                return Err(StorageError {
                    kind: ErrorKind::ApiError,
                    msg: format!("API Error in delete_item: {}", e.to_string()),
                })
            }
        }
    }

    fn ttl(&self) -> u64 {
        self.ttl
    }
}

fn build_query_input(table_name: String, name: &str) -> QueryInput {
    let mut attr_service: AttributeValue = Default::default();
    attr_service.s = Some(name.to_owned());
    let mut expression_attribute_values: HashMap<String, AttributeValue> = HashMap::new();
    expression_attribute_values.insert(":service_val".to_owned(), attr_service);

    let mut query_input: QueryInput = Default::default();
    query_input.table_name = table_name;
    query_input.expression_attribute_values = Some(expression_attribute_values);
    query_input.key_condition_expression = Some("service = :service_val".to_owned());
    query_input
}

fn build_put_item_input(table_name: String, name: &str, host: Host) -> PutItemInput {
    let mut put_item_input: PutItemInput = Default::default();
    put_item_input.table_name = table_name;
    put_item_input.item = convert_domain_host_to_ddb_host(&name, host);
    put_item_input
}

fn build_delete_item_input(table_name: String, name: &str, ip: &str, port: u64) -> DeleteItemInput {
    let mut delete_item_input: DeleteItemInput = Default::default();
    delete_item_input.table_name = table_name;
    let mut pk = HashMap::new();
    pk.insert("service".to_owned(), build_string_attr(name.to_owned()));
    let ip_and_port = format!("{}:{}", ip, port);
    pk.insert("ip_port".to_owned(), build_string_attr(ip_and_port));
    delete_item_input.key = pk;
    delete_item_input.return_values = Some("ALL_OLD".to_owned());
    delete_item_input
}

fn convert_domain_host_to_ddb_host(name: &str, host: Host) -> HashMap<String, AttributeValue> {
    let mut map = HashMap::new();
    map.insert("service".to_owned(), build_string_attr(name.to_owned()));
    let ip_port = format!("{}:{}", host.ip_address, host.port);
    map.insert("ip_port".to_owned(), build_string_attr(ip_port));
    map.insert(
        "last_check_in".to_owned(),
        build_string_attr(host.last_check_in),
    );
    let mut v: AttributeValue = Default::default();
    v.n = Some(host.expire_time.to_string());
    map.insert("expire_time".to_owned(), v);
    map.insert("revision".to_owned(), build_string_attr(host.revision));
    let mut v: AttributeValue = Default::default();
    v.m = Some(convert_domain_tag_to_ddb_tag(host.tags));
    map.insert("tags".to_owned(), v);
    map
}

fn convert_domain_tag_to_ddb_tag(tag: Tag) -> HashMap<String, AttributeValue> {
    let mut map = HashMap::new();
    map.insert("az".to_owned(), build_string_attr(tag.az));
    map.insert("region".to_owned(), build_string_attr(tag.region));
    map.insert("instance_id".to_owned(), build_string_attr(tag.instance_id));
    let mut v: AttributeValue = Default::default();
    v.bool = Some(tag.canary);
    map.insert("canary".to_owned(), v);

    if let Some(weight) = tag.load_balancing_weight {
        let v = AttributeValue {
            n: Some(weight.to_string()),
            ..Default::default()
        };
        map.insert("load_balancing_weight".to_owned(), v);
    }

    map
}

fn build_string_attr(s: String) -> AttributeValue {
    let mut v: AttributeValue = Default::default();
    v.s = Some(s);
    v
}

fn convert_ddb_host_to_domain_host(
    name: &str,
    mut h: HashMap<String, AttributeValue>,
) -> Result<Host, StorageError> {
    let tag = convert_ddb_tags_to_domain_tag(extract_map(&mut h, "tags")?)?;

    let addr_and_port_string = extract_string(&mut h, "ip_port")?;
    let addr_and_port: Vec<&str> = addr_and_port_string.split(':').collect();
    if addr_and_port.len() != 2 {
        return build_data_error(format!(
            "\"{}\" must be formated with colon like \"ip:port\"",
            addr_and_port_string
        ));
    }
    let port_string = addr_and_port[1].to_string();
    let port = match port_string.parse() {
        Ok(v) => v,
        Err(_e) => {
            return build_data_error(format!(
                "port value must be a valid integer: {}",
                port_string
            ))
        }
    };
    Ok(Host {
        ip_address: addr_and_port[0].to_string(),
        port,
        last_check_in: extract_string(&mut h, "last_check_in")?,
        expire_time: extract_number(&mut h, "expire_time")?,
        revision: extract_string(&mut h, "revision")?,
        service: name.to_owned(),
        tags: tag,
    })
}

fn build_data_error<T>(msg: String) -> Result<T, StorageError> {
    Err(StorageError {
        kind: ErrorKind::DataError,
        msg,
    })
}

fn convert_ddb_tags_to_domain_tag(
    mut tag_map: HashMap<String, AttributeValue>,
) -> Result<Tag, StorageError> {
    Ok(Tag {
        az: extract_string(&mut tag_map, "az")?,
        region: extract_string(&mut tag_map, "region")?,
        instance_id: extract_string(&mut tag_map, "instance_id")?,
        canary: extract_bool(&mut tag_map, "canary")?,
        load_balancing_weight: extract_u8(&mut tag_map, "load_balancing_weight")?,
    })
}

fn extract_string(
    m: &mut HashMap<String, AttributeValue>,
    k: &str,
) -> Result<String, StorageError> {
    let v = extract(m, k)?;
    match v.s {
        Some(s) => Ok(s),
        None => build_data_error(format!(
            "Key \"{}\" is expected to be a String but is not",
            k
        )),
    }
}

fn extract_bool(m: &mut HashMap<String, AttributeValue>, k: &str) -> Result<bool, StorageError> {
    let v = extract(m, k)?;
    match v.bool {
        Some(b) => Ok(b),
        None => build_data_error(format!(
            "Key \"{}\" is expected to be a Boolean but is not",
            k
        )),
    }
}

fn extract_number(m: &mut HashMap<String, AttributeValue>, k: &str) -> Result<u64, StorageError> {
    let v = extract(m, k)?;
    match v.n {
        Some(s) => match s.parse() {
            Ok(u) => Ok(u),
            Err(_e) => build_data_error(format!(
                "Key \"{}\" is expected to be a Number (u64) value but is not: {}",
                k, s,
            )),
        },
        None => build_data_error(format!(
            "Key \"{}\" is expected to be a Number but is not",
            k
        )),
    }
}

fn extract_map(
    m: &mut HashMap<String, AttributeValue>,
    k: &str,
) -> Result<HashMap<String, AttributeValue>, StorageError> {
    let v = extract(m, k)?;
    match v.m {
        Some(map) => Ok(map),
        None => build_data_error(format!("Key \"{}\" is expected to be a Map but is not", k)),
    }
}

fn extract_u8(
    m: &mut HashMap<String, AttributeValue>,
    k: &str,
) -> Result<Option<u8>, StorageError> {
    match m.remove(k).and_then(|v| v.n) {
        Some(s) => match s.parse() {
            Ok(u) => Ok(Some(u)),
            Err(_e) => build_data_error(format!(
                "Key \"{}\" is expected to be a Number (u8) value but is not: {}",
                k, s,
            )),
        },
        None => Ok(None),
    }
}

fn extract(
    m: &mut HashMap<String, AttributeValue>,
    k: &str,
) -> Result<AttributeValue, StorageError> {
    match m.remove(k) {
        Some(v) => Ok(v),
        None => build_data_error(format!("Missing required value for key: {}", k)),
    }
}
