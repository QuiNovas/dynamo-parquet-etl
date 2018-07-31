# dynamo-parquet-etl
Lambda function that is notified on DynamoDB table changes(streams), creates a parquet file and uploads to S3. This is designed 
to be triggered by a DynamoDB stream, a scheduled event, or invoked directly. 
- If the DynamoDB stream method is used, only the table referenced in the stream will be ETL'd. 
- If the scheduled event is used, all tables referenced will be ETL'd.
- If it is invoked directly, the JSON passed to it _may_ contain a list of tables to ETL - if no list is sent then all tables referenced will be ETL'd.

Note this is intended for writing reference tables from DynamoDB to parquet files. As such, **it will overwrite the existing file on every run**.

# Environment Variables:
- **S3_OUTPUT_BUCKET** The S3 bucket to write the parquet file(s) to
- **DYNAMO_PARQUET_ETL** Can either be an S3 location in the format _s3://bucket/key_ or direct JSON in the variable. You must minify it if it is directly in the variable. The values in the map _"fields"_ are in JSON Pointer format (RFC6901 - https://tools.ietf.org/html/rfc6901)
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
      },
      "<field4>": {
        "value" "/val4"
        "type": "timestamp"
    }
  }
}
```



# Handler Method
function.handler


