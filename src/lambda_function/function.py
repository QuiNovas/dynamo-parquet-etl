import boto3
import json
import logging.config
import os
import pandas
import pyarrow
import pyarrow.parquet as parquet
import re

from boto3.dynamodb.types import TypeDeserializer
from botocore.client import Config
from datetime import datetime
from jsonpointer import resolve_pointer


def load_etl():
    bucket_key = re.search('^s3://(.+?)/(.*)', os.environ['DYNAMO_PARQUET_ETL'])
    if bucket_key:
        s3 = boto3.client('s3', config=Config(signature_version='s3v4'))
        etl = s3.get_object(
            Bucket=bucket_key.group(1),
            Key=bucket_key.group(2)
        )
        etl_config = etl['Body'].read()
    else:
        etl_config = os.environ['DYNAMO_PARQUET_ETL']
    return json.loads(etl_config)


boto3.resource('dynamodb')
deserializer = TypeDeserializer()
dynamo_parquet_etl = load_etl()
dynamo_table_re = re.compile('^arn:aws:dynamodb:[a-z]{2}-[a-z]*-[0-9]:[0-9]*:table/(.+?)/')


def handler(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    logger.debug('Processing event {}'.format(json.dumps(event)))

    return event
