from kafka import KafkaConsumer
from json import loads
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': '185.235.40.116', 'port': 9200}])

consumer = KafkaConsumer(
    'cleaneddata',
     bootstrap_servers=['185.235.40.116:9092'],
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')),
    api_version=(0, 10, 1))

for message in consumer:
    value = message.value
    es.index(index='tweets', doc_type='tweet', body=value)
    for item in value['tokens']:
        token = value
        token['token'] = item
        es.index(index='tokens', doc_type='tweet', body=token)
        print(item)
    print (value)


