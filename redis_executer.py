import redis
from kafka import KafkaConsumer
from json import loads
import time
from datetime import datetime

from elasticsearch import Elasticsearch

redis_host = "185.235.40.116"
redis_port = 6379
redis_password = ""

r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
def get_last_6h_tweet_ids(user):
    tweets = str(r.get("msg:"+user+"-6h"))
    if tweets == 'None':
        return []

    return  tweets.split('&')

def get_last_today_ids():
    tweets = str(r.get("msg:today"))
    if tweets == 'None':
        return []
    return  tweets.split('&')

def get_last_1000_hashtag():
    tweets = str(r.get("msg:1000_hashtag"))
    if tweets == 'None':
        return []
    items =  tweets.split('&')
    if len(items) > 1000:
        return items[:-1000]
    return  items

def get_last_100_tweets():
    tweets = str(r.get("msg:100_tweets"))
    if tweets == 'None':
        return []
    items =  tweets.split('&')
    if len(items) > 100:
        return items[:-100]
    return  items

def get_tweets(ids):
    ids = list(map(lambda id:"msg:"+ id, ids))

    for id in ids:
        print(r.get(id))

while True:
    print( len(get_last_6h_tweet_ids("@FarsNewsInt")))
    print(len(get_last_100_tweets()))
    print(get_last_1000_hashtag())
    get_tweets(get_last_today_ids())

    time.sleep(1)