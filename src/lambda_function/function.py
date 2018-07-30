import boto3
import json
import logging.config
import os
import re

from boto3.dynamodb.types import TypeDeserializer
from datetime import datetime
from jsonpointer import resolve_pointer

boto3.resource('dynamodb')
deserializer = TypeDeserializer()
dynamo_parquet_etl = json.loads(os.environ['DYNAMO_PARQUET_ETL'])
dynamo_table_re = re.compile('^arn:aws:dynamodb:[a-z]{2}-[a-z]*-[0-9]:[0-9]*:table/(.+?)/')


def handler(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    logger.debug('Processing event {}'.format(json.dumps(event)))

    return event
