from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from json import dumps
from kafka import KafkaConsumer, KafkaProducer
from json import loads

producer = KafkaProducer(bootstrap_servers='185.235.40.116:9092')

driver = webdriver.Chrome()
driver.get("https://www.sahamyab.com/stocktwits")
last_tweet = ''
while driver.execute_script("return (true)"):
    tweet = driver.find_element(By.XPATH, "//div[2]/p").text
    if(last_tweet != tweet):
        x = dumps(tweet).encode('utf-8')
        result =  producer.send(topic='quickstart-events', value=b'x')
        print("tweet: " + tweet )
        username = driver.find_element(By.XPATH, "//app-user-status-bar/a").text
        print("username: " + username)
        last_tweet = tweet
        print("---------------------------------")
        r = producer.flush()
        x = 0
    time.sleep(.01)

#
# from kafka import KafkaProducer
# producer = KafkaProducer(bootstrap_servers=['185.235.40.116:9092'])
# producer.send('sample', b'Hello, World!').get(10)
# producer.send('sample', key=b'message-two', value=b'This is Kafka-Python').get(10)