import boto3
import multiprocessing
from datetime import datetime
import json
from boto3.dynamodb.types import TypeDeserializer


KINESIS_STREAM = 'test'
START_TIME = datetime(2025, 9, 10, 13, 30, 0)
SCYLLA_URL = 'http://node-0.aws-us-east-1.165a4a243f0c0b41fd2f.clusters.scylla.cloud:8000'
DST_TABLE = 'partner_user_id_mappings_small'
REGION = 'us-east-1'


def process_and_insert(items, table_name):
    records = [items]
    deserializer = TypeDeserializer()
    dynamodb = boto3.resource('dynamodb', endpoint_url=SCYLLA_URL, region_name=REGION)
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
    kinesis = boto3.client('kinesis',region_name=REGION)
    # Get an iterator for this shard starting at AT_TIMESTAMP
    iterator = kinesis.get_shard_iterator(
        StreamName=KINESIS_STREAM,
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
            process_and_insert(r,DST_TABLE)
            count += 1
            #print(count)
            if count % 3 == 0:
                savepoint = r['SequenceNumber']
                print('SavePoint:', savepoint)
            pass
        iterator = batch.get('NextShardIterator')

if __name__ == "__main__":
    print('Starting streaming')
    client = boto3.client('kinesis',region_name=REGION)
    shards = client.describe_stream(StreamName=KINESIS_STREAM)['StreamDescription']['Shards']
    processes = []
    for shard in shards:
        p = multiprocessing.Process(target=process_shard, args=(shard['ShardId'], START_TIME))
        p.start()
        processes.append(p)
        print('started shard:', shard['ShardId'])
    for p in processes:
        p.join()
