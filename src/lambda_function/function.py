import boto3
import contextlib
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
from multiprocessing.pool import ThreadPool

dynamodb = boto3.resource('dynamodb')


def load_etl():
    bucket_key = re.search('^s3://(.+?)/(.*)', os.environ['DYNAMO_PARQUET_ETL'])
    if bucket_key:
        s3 = boto3.client('s3', config=Config(signature_version='s3v4'))
        response = s3.get_object(
            Bucket=bucket_key.group(1),
            Key=bucket_key.group(2)
        )
        etl_config = response['Body'].read()
    else:
        etl_config = os.environ['DYNAMO_PARQUET_ETL']
    etl = json.loads(etl_config)
    for dynamodb_table_name in etl:
        etl[dynamodb_table_name]['table'] = dynamodb.Table(dynamodb_table_name)
    return etl


deserializer = TypeDeserializer()
dynamo_parquet_etl = load_etl()
dynamo_table_re = re.compile('^arn:aws:dynamodb:[a-z]{2}-[a-z]*-[0-9]:[0-9]*:table/(.+?)/')


def handler(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    logger.debug('Processing event {}'.format(json.dumps(event)))
    if 'detail-type' in event and event['detail-type'] == 'Scheduled Event':
        process_tables(dynamo_parquet_etl.keys())
    elif 'Records' in event:
        dynamodb_table_names = set()
        for record in event['Records']:
            dynamodb_table_name = dynamo_table_re.search(record['eventSourceARN']).group(1)
            if dynamodb_table_name in dynamo_parquet_etl:
                dynamodb_table_names.add(dynamodb_table_name)
        process_tables(list(dynamodb_table_names))
    else:
        if isinstance(event, list):
            dynamodb_table_names = set()
            for dynamodb_table_name in event:
                if dynamodb_table_name in dynamo_parquet_etl:
                    dynamodb_table_names.add(dynamodb_table_name)
            process_tables(list(dynamodb_table_names))
    return event


def scan_table(table, start_key):
    return []


def process_table(dynamodb_table_name):
    records = scan_table(dynamo_parquet_etl[dynamodb_table_name])



def process_tables(dynamodb_table_names):
    if dynamodb_table_names and len(dynamodb_table_names):
        return
    if len(dynamodb_table_names) == 1:
        process_table(dynamodb_table_names[0])
    else:
        with contextlib.closing(ThreadPool()) as pool:
            pool.map(process_table, dynamodb_table_names)
