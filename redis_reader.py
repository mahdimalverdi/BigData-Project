import redis
from kafka import KafkaConsumer
from json import loads
import time
from datetime import datetime

from elasticsearch import Elasticsearch

consumer = KafkaConsumer(
    'cleaneddata',
     bootstrap_servers=['185.235.40.116:9092'],
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    api_version=(0, 10, 1))


redis_host = "185.235.40.116"
redis_port = 6379
redis_password = ""

r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)

def get_last_6h_tweet_ids(user):
    tweets = str(r.get("msg:"+user+"-6h"))
    if tweets == 'None':
        return []

    items =  tweets.split('&')
    if len(items) > 6:
        return items[:-6]
    return  items

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

for message in consumer:
    time = message.value["time"]
    user = message.value["user"]
    tweet =  message.value["tweet"]
    key_tweet = time+"-"+user
    r.set("msg:"+key_tweet, tweet,ex=7*24*3600)

    latest_tweets = get_last_6h_tweet_ids(user)
    latest_tweets.append(key_tweet)
    r.set("msg:" + user + "-6h", "&".join(latest_tweets))

    today_tweets = get_last_today_ids()
    today_tweets.append(key_tweet)
    r.set("msg:today", "&".join(latest_tweets))

    latest_hashtags = get_last_1000_hashtag()
    for token in message.value["tokens"]:
        latest_hashtags.append(token)
    r.set("msg:1000_hashtag", "&".join(set(latest_hashtags)))

    last_100_tweets = get_last_100_tweets()
    last_100_tweets.append(key_tweet)
    r.set("msg:100_tweets", "&".join(last_100_tweets))

    print(get_last_100_tweets())
