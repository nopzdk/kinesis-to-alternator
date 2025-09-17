import random

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os
import multiprocessing
from datetime import datetime
import json
from boto3.dynamodb.types import TypeDeserializer

load_dotenv()

def get_env_var(var_name):
    value = os.getenv(var_name)
    if value is None:
        raise EnvironmentError(f"Missing required environment variable: {var_name}")
    return value


kinesis_stream = get_env_var('KINESIS_STREAM_NAME')
kinesis_region = get_env_var('KINESIS_REGION')
s3_export_arn = get_env_var('S3_EXPORT_ARN')
s3_bucket = get_env_var('S3_BUCKET')
s3_manifest_key = get_env_var('S3_MANIFEST_KEY')
alternator_ips = get_env_var('ALTERNATOR_IPS').split(',')
alternator_port = 8000
alternator_region = get_env_var('ALTERNATOR_REGION')
ALTERNATOR_ACCESS_KEY = get_env_var('ALTERNATOR_ACCESS_KEY')
ALTERNATOR_SECRET_KEY = get_env_var('ALTERNATOR_SECRET_KEY')
dst_table = get_env_var('DST_TABLE_NAME')
fetch_batch_size = int(get_env_var('FETCH_BATCH_SIZE'))
put_batch_size = int(get_env_var('PUT_BATCH_SIZE'))


start_time_str = get_env_var('START_TIME')
time_parts = [int(part) for part in start_time_str.split(',')]
start_time = datetime(*time_parts)


def chunk_list(lst, size):
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def process_shard(shard_id, at_timestamp, fetch_size, put_size):
    deserializer = TypeDeserializer()
    kinesis = boto3.client('kinesis', region_name=kinesis_region)
    iterator = kinesis.get_shard_iterator(
        StreamName=kinesis_stream,
        ShardId=shard_id,
        ShardIteratorType='AT_TIMESTAMP',
        Timestamp=at_timestamp
    )['ShardIterator']
    alternator = boto3.resource('dynamodb', endpoint_url=alternator_url, region_name=alternator_region,
                                aws_secret_access_key=ALTERNATOR_SECRET_KEY,
                                aws_access_key_id=ALTERNATOR_ACCESS_KEY)
    table = alternator.Table(dst_table)
    # Initialize static pkeys for the function
    table_desc = table.meta.client.describe_table(TableName=dst_table)
    key_schema = table_desc['Table']['KeySchema']
    pkeys = [k['AttributeName'] for k in key_schema]

    while iterator:
        records = kinesis.get_records(ShardIterator=iterator, Limit=int(fetch_size))
        print(shard_id, 'batch size: ', len(records['Records']), 'MillisBehindLatest: ', records['MillisBehindLatest'])

        records_lists = chunk_list(records['Records'], int(put_size))
        for record_list in records_lists:

            with table.batch_writer(overwrite_by_pkeys=pkeys) as batch:
                for record in record_list:
                    item = json.loads(record['Data'])
                    print(item)
                    match item['eventName']:
                        case 'INSERT' | 'MODIFY':
                            insert_item = {k: deserializer.deserialize(v) for k, v in item['dynamodb']['NewImage'].items()}
                            batch.put_item(insert_item)
                        case 'REMOVE':
                            delete_item = {k: deserializer.deserialize(v) for k, v in item['dynamodb']['Keys'].items()}
                            batch.delete_item(delete_item)
        iterator = records.get('NextShardIterator')


def get_timestamp_s3export(s3_arn, region):
    dynamodb = boto3.client('dynamodb', region_name=region)
    describe = dynamodb.describe_export(ExportArn=s3_arn)
    export_start_time = describe['ExportDescription']['StartTime']  # when export started
    print("Export started at:", export_start_time)
    return export_start_time

def get_timestamp_from_manifest(bucket,manifestkey):
    s3 = boto3.client('s3')
    #resp = s3.head_object(Bucket='bucket', Key='manifestkey')
    resp = s3.get_object(Bucket='bucket', Key='manifestkey')
    print(resp['LastModified'])
    return resp['LastModified']

if __name__ == "__main__":
    print(f'Starting streaming {datetime.now()}')
    client = boto3.client('kinesis', region_name=kinesis_region)
    shards = client.describe_stream(StreamName=kinesis_stream)['StreamDescription']['Shards']
    processes = []
    for shard in shards:
        alternator_url = f"http://{alternator_ips[random.randint(0, len(alternator_ips) - 1)]}:{alternator_port}"
        print(alternator_url)
        p = multiprocessing.Process(target=process_shard, args=(shard['ShardId'], start_time, fetch_batch_size, put_batch_size))
        #p = multiprocessing.Process(target=get_records, args=(shard['ShardId'], start_time))
        #p = multiprocessing.Process(target=process_shard, args=(shard['ShardId'], start_time))
        p.start()
        processes.append(p)
        print('started:', shard['ShardId'])
    for p in processes:
        p.join()
    print(f'Ending stream {datetime.now()}')