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

BUCKET = os.environ['S3_BUCKET']
ES_HOST = os.environ['ES_HOST']
ES_REGION = os.environ['ES_REGION']

def tokenizeItemName(itemName):
    tokens = itemName.split(" ")
    results = []

    for windowSize in range(2, len(tokens)):
        for i in range(0, len(tokens)):
            e_i = i + windowSize
            if e_i > len(tokens):
                break

            sublist = tokens[i:e_i]
            results.append(" ".join(sublist))
    tokens.append(itemName)
    tokens.extend(results)
    return tokens

def lambda_handler(event, context):
    session = boto3.session.Session()
    credentials = session.get_credentials().get_frozen_credentials()

    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, ES_REGION, service, session_token=credentials.token)

    # use the requests connection_class and pass in our custom auth class
    es = Elasticsearch(
        timeout=30,
        hosts=[{'host': ES_HOST, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    actions = []
    for faction in ['horde']:
        key = 'faction/{0}/Auc-ScanData.lua'.format(faction)
        response = s3.get_object(Bucket=BUCKET, Key=key)
        body = response['Body'].read().decode('utf-8')
        realm_sections = body.split("scanstats")[1:]
        for realm_sect in realm_sections:
            # 17 is the number of characters in '["serverKey"] = '. We care about what's after ;)
            server_key_char_index = realm_sect.find('["serverKey"] = ') + 17

            # Search ahead for the delimiter right after the server name, which is \r
            # Shouldn't be THAT far... so we'll just look 100 chars ahead
            end_delimiter_index = realm_sect[server_key_char_index:server_key_char_index+100].find("\r") - 2 # -2 for ", right before the \r
            server_key_end_char_index = server_key_char_index + end_delimiter_index

            # Getting the server name
            server_name = realm_sect[server_key_char_index:server_key_end_char_index].replace(" ", "_").lower()
            pattern = re.compile(r'(\{\\"\|[a-zA-Z0-9]+\|Hitem:[0-9]+(?:[:0-9\:\'\|a-zA-Z\[\]\s\\\",]|[^\x00-\x7F])+})')

            m = pattern.findall(realm_sect[server_key_end_char_index:])
            es_alias = "ah_item_{0}_{1}".format(server_name, faction)

            # Check if alias is set up for this realm and faction.

            last_indices = []
            if es.indices.exists(es_alias):
                last_indices = list(es.indices.get_alias(name=es_alias).keys())
                print("Last indices: " + " ".join(last_indices))

            es_temp_index = "ah_item_{0}_{1}_{2}".format(server_name, faction, int(datetime.now().timestamp()))

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
                                  }
                              }
                              )

            for match in m:
                unescaped_s = match.replace('\\', '')
                pattern2 = re.compile(
                    r'{"\|cff([a-zA-Z0-9]+)\|Hitem:([0-9]+)[:0-9]+\|h\[([a-zA-Z\:\'\"\s0-9]+)\]\|h\|r",([0-9]+|nil),[0-9]+,[0-9]+,(?:[0-9]+|nil),([0-9]+),([1234]),[0-9]+,"[a-zA-Z\:\'\"\s0-9]+",(?:[0-9]+|nil),([0-9]+),[0-9]+,(?:false|true),([0-9]+),[0-9]+,[0-9]+,([0-9]+),[0-9]+,(?:true|false),"((?:[^\x00-\x7F]|[a-zA-Z])*)",[0-9]+,"((?:[^\x00-\x7F]|[a-zA-Z])*)",[0-9]+,[0-9]+,[0-9]+,[0-9]+,[0-9]+,}')
                g = pattern2.match(unescaped_s)
                actions.append(
                    {
                        "_index": es_temp_index,
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
                            "timestamp": datetime.now(),
                            "suggest": tokenizeItemName(g.group(3))
                        },
                    }
                )

            print("Number of documents from " + server_name + ": ", len(actions))

            # Bulk insert to temporary index
            helpers.bulk(es, actions)

            # Point temporary index to alias
            es.indices.put_alias(name=es_alias, index=es_temp_index)

            # Clean up old indices pointing to alias
            if len(last_indices) > 0:
                es.indices.delete_alias(name=es_alias, index=last_indices)
                es.indices.delete(index=last_indices, ignore=[400, 404])

            actions.clear()

    return response['ContentType']
