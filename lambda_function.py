import json
import urllib.parse
import boto3
import re
from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth.aws_auth import AWSRequestsAuth

print('Loading function')

s3 = boto3.client('s3')
es = boto3.client('es')


BUCKET = os.environ['S3_BUCKET']
ES_HOST = os.environ['ES_HOST']

def lambda_handler(event, context):
    session = boto3.session.Session()
    credentials = session.get_credentials().get_frozen_credentials()

    awsauth = AWSRequestsAuth(
        aws_access_key=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        aws_token=credentials.token,
        aws_host=ES_HOST,
        aws_region=session.region_name,
        aws_service='es'
    )

    # use the requests connection_class and pass in our custom auth class
    es = Elasticsearch(
        hosts=[{'host': ES_HOST, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    print(es.info())

    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    # bucket = event['Records'][0]['s3']['bucket']['name']
    # key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    key = 'realm/bigglesworth/horde/Auc-ScanData.lua'
    try:
        response = s3.get_object(Bucket=BUCKET, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
        body = response['Body'].read().decode('utf-8')
        pattern = re.compile(r'(\{\\"\|[a-zA-Z0-9]+\|Hitem:[0-9]+[:0-9\:\'\|a-zA-Z\[\]\s\\\",]+})')

        m = pattern.findall(body)
        for match in m:
            unescaped_s = match.replace('\\', '')
            pattern2 = re.compile(r'{"\|cff([a-zA-Z0-9]+)\|Hitem:([0-9]+)[:0-9]+\|h\[([a-zA-Z\:\'\"\s]+)\]\|h\|r",([0-9]+|nil),[0-9]+,[0-9]+,(?:[0-9]+|nil),([0-9]+),([1234]),[0-9]+,"[a-zA-Z\:\'\"\s]+",(?:[0-9]+|nil),[0-9]+,[0-9]+,(?:false|true),([0-9]+),[0-9]+,[0-9]+,([0-9]+),[0-9]+,(?:true|false),"([a-zA-Z]*)",[0-9]+,"([a-zA-Z]*)",[0-9]+,[0-9]+,[0-9]+,[0-9]+,[0-9]+,}')
            g = pattern2.match(unescaped_s)
            for item in g.groups():

                print(g.groups())
        # print(m)
        return response['ContentType']
    except Exception as e:
        print(e)
        raise e

