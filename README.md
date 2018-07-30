# dynamo-parquet-etl
Lambda function that is notified on DynamoDB table changes, creates a parquet file and uploads to S3. This is designed to be triggered by a DynamoDB stream, a scheduled event, or invoked directly.
Note this is intended for writing reference tables from DynamoDB to parquet files. As such, **it will overwrite the existing file on every run**.

# Environment Variables:
- **S3_BUCKET** The S3 bucket to write the parquet file(s) to
- **DYNAMO_PARQUET_ETL** JSON with the following format. Recommend that you minify it. The values in the map _"fields"_ are in JSON Pointer format (RFC6901 - https://tools.ietf.org/html/rfc6901)
```
{
  "<dynamo-table-name>": {
    "s3_key": "<key>",
    "fields": {
      "<field1>": {
        "value": "/val1",
        "type": "string"
      },
      "<field2>": {
        "value": "/val2/subval3",
        "type": "int32"
      },
      "<field3>": {
        "value": "/val3" - defaults to the "string" type
      }
    }
  }
}
```



# Handler Method
function.handler


