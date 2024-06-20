---
description: >-
  
---

# S3StorageIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration)

Manages the integration of Snowflake with S3 storage.


## Examples

### Python

```python
s3_storage_integration = S3StorageIntegration(
    name="some_s3_storage_integration",
    enabled=True,
    storage_aws_role_arn="arn:aws:iam::123456789012:role/MyS3AccessRole",
    storage_allowed_locations=["s3://mybucket/myfolder/"],
    storage_blocked_locations=["s3://mybucket/myblockedfolder/"],
    storage_aws_object_acl="bucket-owner-full-control",
    comment="This is a sample S3 storage integration."
)
```


### YAML

```yaml
s3_storage_integrations:
  - name: some_s3_storage_integration
    enabled: true
    storage_aws_role_arn: "arn:aws:iam::123456789012:role/MyS3AccessRole"
    storage_allowed_locations:
      - "s3://mybucket/myfolder/"
    storage_blocked_locations:
      - "s3://mybucket/myblockedfolder/"
    storage_aws_object_acl: "bucket-owner-full-control"
    comment: "This is a sample S3 storage integration."
```


## Fields

* `name` (string, required) - The name of the storage integration.
* `enabled` (bool, required) - Whether the storage integration is enabled. Defaults to True.
* `storage_aws_role_arn` (string, required) - The AWS IAM role ARN to access the S3 bucket.
* `storage_allowed_locations` (list, required) - A list of allowed locations for storage in the format 's3://<bucket>/<path>/'.
* `storage_blocked_locations` (list) - A list of blocked locations for storage in the format 's3://<bucket>/<path>/'. Defaults to an empty list.
* `storage_aws_object_acl` (string) - The ACL policy for objects stored in S3. Defaults to 'bucket-owner-full-control'.
* `type` (string) - The type of storage integration. Defaults to 'EXTERNAL_STAGE'.
* `owner` (string or [Role](role.md)) - The owner role of the storage integration. Defaults to 'ACCOUNTADMIN'.
* `comment` (string) - An optional comment about the storage integration.


