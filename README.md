sds
===

Envoy's v1 Service Discovery Service API. In contrast of https://github.com/lyft/discovery, the sds allow users to serve multiple application instances of single service in single host instance (with single ip address).

## Endpoints
### v1 SDS
`GET /v1/registration/:name/`

Responses v1 SDS data: https://www.envoyproxy.io/docs/envoy/v1.8.0/api-v1/cluster_manager/sds

### v2 EDS
`POST /v2/discovery:endpoints`

Accepts [v2 DiscoveryRequest](https://www.envoyproxy.io/docs/envoy/v1.8.0/api-v2/api/v2/discovery.proto#discoveryrequest),
then responses [v2 DiscoveryResponse](https://www.envoyproxy.io/docs/envoy/v1.8.0/api-v2/api/v2/discovery.proto#discoveryresponse).

### Registration
`POST /v1/registration/:name/`

Request body

```
{
  ip: String,
  port: u16,
  revision: String,
  tags: {
    az: String,
    region: String,
    instance_id: String,
    canary: bool,
    load_balancing_weight: Option<u8>,
  },
}
```

Responses 202 on success, 400 on bad requests, 500 for internal server errors.

### Deregistration
`DELETE /v1/registration/:name/:ip_addr_and_port/`

e.g. `DELETE /v1/registration/user_service/10.0.0.10:34005/`

Responses 202 on success, 400 on bad requests, 500 for internal server errors, and response 400 with JSON message when
the entry not found:

```json
{
  "id": "HostNotFound",
  "reason": "Not found the entry"
}
```

## Environment variables
- AWS_DEFAULT_REGION: AWS region like `us-east-1`
- DDB_TABLE: DynamoDB's table name
- HOST_TTL: the TTL of the DynamoDB's entries
- PORT: the listen port

## Createing DynamoDB table
- Create with PK: `service` as String and `ip_port` as String
- Set TTL setting using `expire_time` key

## IAM permissions
- DynamoDB's `query`, `put_item`, `delete_item`
