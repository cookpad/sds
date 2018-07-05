sds
===

Envoy's v1 Service Discovery Service API. In contrast of https://github.com/lyft/discovery, the sds allow users to serve multiple application instances of single service in single host instance (with single ip address).

## Environment variables
- AWS_DEFAULT_REGION: AWS region like `us-east-1`
- DDB_TABLE: DynamoDB's table name
- HOST_TTL: the TTL of the DynamoDB's entries
- PORT: the listen port

## Createing DynamoDB table
- create with PK: `service` as String and `ip_port` as String
- set TTL setting using `expire_time` key

## IAM permissions
- DynamoDB's `query`, `put_item`, `delete_item`
