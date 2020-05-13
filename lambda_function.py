import json
import urllib.parse
import boto3
import re
from datetime import datetime
import os
import sys
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from aws_requests_auth.aws_auth import AWSRequestsAuth
from requests_aws4auth import AWS4Auth
from collections import deque


print('Loading function')

s3 = boto3.client('s3')
BUCKET = os.environ['S3_BUCKET']
ES_HOST = os.environ['ES_HOST']
ES_REGION = os.environ['ES_REGION']

credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, ES_REGION, 'es', session_token=credentials.token)

# use the requests connection_class and pass in our custom auth class
es = Elasticsearch(
    timeout=60,
    hosts=[{'host': ES_HOST, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

def store_items(es_alias, server_name, faction, items):
    es_temp_index = "ah_item_{0}_{1}_{2}".format(server_name, faction, int(datetime.now().timestamp()))

    # Check if alias is set up for this realm and faction.
    last_indices = []
    if es.indices.exists(es_alias):
        last_indices = list(es.indices.get_alias(name=es_alias).keys())
        print("Last indices: " + " ".join(last_indices))

    es.indices.create(es_temp_index,
                      body={
                          "mappings": {
                              "_doc": {
                                  "properties": {
                                      "suggest": {
                                          "type": "completion"
                                      }
                                  }
                              },
                          },
                          "settings": {
                              "number_of_replicas": 0,
                              "refresh_interval": -1
                          }
                      }
                      )

    print("Created Index: " + es_temp_index)
    actions = []
    for item in items:
        actions.append(
            {
                "_index": es_temp_index,
                "_type": "_doc",
                "_source": item
            }
        )

    print("Number of documents from " + server_name + ": ", len(actions), ", File Size (bytes): ", sys.getsizeof(actions))

    # Bulk insert to temporary index
    # helpers.bulk(es, actions)
    deque(helpers.parallel_bulk(es, actions), maxlen=0)

    # Point temporary index to alias
    es.indices.put_alias(name=es_alias, index=es_temp_index)

    # Clean up old indices pointing to alias
    if len(last_indices) > 0:
        es.indices.delete_alias(name=es_alias, index=last_indices)
        es.indices.delete(index=last_indices, ignore=[400, 404])

    es.indices.put_settings(index=es_temp_index, body={
        "settings": {
            "refresh_interval": "1s"
        }
    })



def lambda_handler(event, context):
    for record in event['Records']:
        key = record['s3']['object']['key']
        key_split = key.split("/")
        server_name, faction = key_split[1], key_split[2]
        response = s3.get_object(Bucket=BUCKET, Key=key)
        json_body = response['Body'].read()
        items = json.loads(json.loads(json_body))
        es_alias = "ah_item_{0}_{1}".format(server_name, faction)

        # This is to make the Lambda function more idempotent.
        # Workaround for using a cache or Dynamo just to lock transactions.
        es_lock = es_alias + "_tmp"
        if es.indices.exists(es_lock):
            # Fail fast - event triggered twice or more and is likely to be running already
            return

        # Create lock
        es.indices.create(es_lock)

        try:
            store_items(es_alias, server_name, faction, items)
        finally:
            # Release lock
            es.indices.delete(index=es_lock)

    return response['ContentType']

