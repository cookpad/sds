use std::collections::HashMap;

use serde_derive::{Deserialize, Serialize};
use serde_json;

use super::types::Host;

pub const EDS_TYPE_URL: &str = "type.googleapis.com/envoy.api.v2.ClusterLoadAssignment";

#[derive(Serialize, Deserialize, Debug)]
pub struct DiscoveryRequest {
    pub version_info: Option<String>,
    pub node: Node,
    pub resource_names: Vec<String>,
    pub type_url: Option<String>,
    pub response_nonce: Option<String>,
    pub error_detail: Option<Status>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EdsDiscoveryResponse {
    pub version_info: String,
    pub resources: Vec<ClusterLoadAssignment>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Node {
    pub id: String,
    pub cluster: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Status {
    pub code: i32,
    pub message: String,
    pub details: Vec<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClusterLoadAssignment {
    pub cluster_name: String,
    pub endpoints: Vec<LocalityLbEndpoints>,
    #[serde(rename = "@type")]
    pub type_url: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LocalityLbEndpoints {
    pub locality: Locality,
    pub lb_endpoints: Vec<LbEndpoint>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
pub struct Locality {
    pub region: String,
    pub zone: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LbEndpoint {
    pub endpoint: Endpoint,
    pub metadata: Metadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_balancing_weight: Option<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Endpoint {
    pub address: Address,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Address {
    pub socket_address: SocketAddress,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SocketAddress {
    pub address: String,
    pub port_value: u16,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Metadata {
    pub filter_metadata: HashMap<String, LbFilterMetadata>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LbFilterMetadata {
    pub canary: bool,
    pub revision: String,
}

pub fn hosts_to_locality_lb_endpoints(mut hosts: Vec<Host>) -> Vec<LocalityLbEndpoints> {
    let mut lle_map: HashMap<Locality, Vec<LbEndpoint>> = HashMap::new();
    for h in hosts.drain(..) {
        let locality = Locality {
            region: h.tags.region.to_owned(),
            zone: h.tags.az.to_owned(),
        };
        let le = convert_host_to_le(h);

        match lle_map.entry(locality) {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(vec![le]);
            }
            std::collections::hash_map::Entry::Occupied(mut e) => {
                e.get_mut().push(le);
            }
        }
    }

    let mut lle_vec = Vec::new();
    for (k, v) in lle_map {
        lle_vec.push(LocalityLbEndpoints {
            locality: k,
            lb_endpoints: v,
        });
    }
    lle_vec
}

fn convert_host_to_le(h: Host) -> LbEndpoint {
    let mut filter_metadata = HashMap::new();
    filter_metadata.insert(
        "envoy.lb".to_owned(),
        LbFilterMetadata {
            canary: h.tags.canary,
            revision: h.revision,
        },
    );

    LbEndpoint {
        load_balancing_weight: h.tags.load_balancing_weight,
        metadata: Metadata { filter_metadata },
        endpoint: Endpoint {
            address: Address {
                socket_address: SocketAddress {
                    address: h.ip_address,
                    port_value: h.port,
                },
            },
        },
    }
}
