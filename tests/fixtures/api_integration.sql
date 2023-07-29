create or replace api integration demonstration_external_api_integration_01
    api_provider=aws_api_gateway
    api_aws_role_arn='arn:aws:iam::123456789012:role/my_cloud_account_role'
    api_allowed_prefixes=('https://xyz.execute-api.us-west-2.amazonaws.com/production')
    enabled=true;

