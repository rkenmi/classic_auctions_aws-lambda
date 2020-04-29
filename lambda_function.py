import json
import urllib.parse
import boto3
import re
from datetime import datetime
import os
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from aws_requests_auth.aws_auth import AWSRequestsAuth
from requests_aws4auth import AWS4Auth

print('Loading function')

s3 = boto3.client('s3')
es = boto3.client('es')


BUCKET = os.environ['S3_BUCKET']
ES_HOST = os.environ['ES_HOST']
ES_REGION = os.environ['ES_REGION']

def lambda_handler(event, context):
    session = boto3.session.Session()
    credentials = session.get_credentials().get_frozen_credentials()

    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, ES_REGION, service, session_token=credentials.token)

    # use the requests connection_class and pass in our custom auth class
    es = Elasticsearch(
        hosts=[{'host': ES_HOST, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    key = 'realm/bigglesworth/horde/Auc-ScanData.lua'
    try:
        response = s3.get_object(Bucket=BUCKET, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
        body = response['Body'].read().decode('utf-8')
        pattern = re.compile(r'(\{\\"\|[a-zA-Z0-9]+\|Hitem:[0-9]+[:0-9\:\'\|a-zA-Z\[\]\s\\\",]+})')

        m = pattern.findall(body)
        actions = []
        for match in m:
            unescaped_s = match.replace('\\', '')
            pattern2 = re.compile(r'{"\|cff([a-zA-Z0-9]+)\|Hitem:([0-9]+)[:0-9]+\|h\[([a-zA-Z\:\'\"\s0-9]+)\]\|h\|r",([0-9]+|nil),[0-9]+,[0-9]+,(?:[0-9]+|nil),([0-9]+),([1234]),[0-9]+,"[a-zA-Z\:\'\"\s0-9]+",(?:[0-9]+|nil),([0-9]+),[0-9]+,(?:false|true),([0-9]+),[0-9]+,[0-9]+,([0-9]+),[0-9]+,(?:true|false),"([a-zA-Z]*)",[0-9]+,"([a-zA-Z]*)",[0-9]+,[0-9]+,[0-9]+,[0-9]+,[0-9]+,}')
            g = pattern2.match(unescaped_s)
            actions.append(
                {
                    "_index": "ah_item",
                    "_type": "_doc",
                    "_source": {
                        "rarity": g.group(1),
                        "id": g.group(2),
                        "itemName": g.group(3),
                        "itemLvl": g.group(4),
                        "bid": g.group(5),
                        "timeRemaining": g.group(6),
                        "quantity": g.group(7),
                        "minLvlRequired": g.group(8),
                        "buyout": g.group(9),
                        "seller": g.group(10),
                        "timestamp": datetime.now()
                    },
                }
            )

        es.indices.delete(index='ah_item', ignore=[400, 404])
        helpers.bulk(es, actions)
        return response['ContentType']
    except Exception as e:
        print(e)
        raise e

