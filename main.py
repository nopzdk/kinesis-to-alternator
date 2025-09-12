import boto3
from dotenv import load_dotenv
import os
import multiprocessing
from datetime import datetime
import json
from boto3.dynamodb.types import TypeDeserializer

load_dotenv()

kinesis_stream = os.getenv('KINESIS_STREAM')
scylla_url = os.getenv('SCYLLA_URL')
dst_table = os.getenv('DST_TABLE')
region = os.getenv('REGION')

start_time_str = (os.getenv('START_TIME'))
time_parts = [int(part) for part in start_time_str.split(',')]
start_time = datetime(*time_parts)


def process_and_insert(items, table_name):
    records = [items]
    deserializer = TypeDeserializer()
    dynamodb = boto3.resource('dynamodb', endpoint_url=scylla_url, region_name=region)
    table = dynamodb.Table(table_name)
    with table.batch_writer() as batch:
        for record in records:
            # Assuming data payload is JSON in record['Data']
            # Adjust parsing as needed for your data format
            #print('record',record['Data'])
            #print('record type', type(record))
            item = json.loads(record['Data'])
            #print('item type ',type(item))
            #print(item)
            #print(item['eventName'])
            if item['eventName'] in ('INSERT', 'MODIFY'):
                print('inserting: ', item['dynamodb']['NewImage'])
                insert_item = {k: deserializer.deserialize(v) for k, v in item['dynamodb']['NewImage'].items()}
                batch.put_item(insert_item)
            elif item['eventName'] == 'REMOVE':
                print('removing: ', item['dynamodb']['Keys'])
                delete_item = {k: deserializer.deserialize(v) for k, v in item['dynamodb']['Keys'].items()}
                batch.delete_item(delete_item)


def decode_and_transform(record):
    # Custom extraction logic, e.g. base64 decode, then JSON parse, then map to DynamoDB keys
    item_data = json.loads(record['Data'])
    print(item_data)

    payload = record['Data']
    item_data = json.loads(payload)
    return item_data  # Adjust this mapping for your schema


def process_shard(shard_id, at_timestamp):
    count = 0
    kinesis = boto3.client('kinesis', region_name=region)
    # Get an iterator for this shard starting at AT_TIMESTAMP
    iterator = kinesis.get_shard_iterator(
        StreamName=kinesis_stream,
        ShardId=shard_id,
        ShardIteratorType='AT_TIMESTAMP',
        Timestamp=at_timestamp
    )['ShardIterator']
    while iterator:
        batch = kinesis.get_records(ShardIterator=iterator, Limit=1000)
        for r in batch['Records']:
            # process each record
            #print('r type',type(r))
            #print(r)
            process_and_insert(r, dst_table)
            count += 1
            #print(count)
            if count % 3 == 0:
                savepoint = r['SequenceNumber']
                print('SavePoint:', savepoint)
            pass
        iterator = batch.get('NextShardIterator')

if __name__ == "__main__":
    print('Starting streaming')
    client = boto3.client('kinesis', region_name=region)
    shards = client.describe_stream(StreamName=kinesis_stream)['StreamDescription']['Shards']
    processes = []
    for shard in shards:
        p = multiprocessing.Process(target=process_shard, args=(shard['ShardId'], start_time))
        p.start()
        processes.append(p)
        print('started shard:', shard['ShardId'])
    for p in processes:
        p.join()
