from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
from json import dumps
from kafka import KafkaConsumer, KafkaProducer
from json import loads

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome("/usr/bin/chromedriver",chrome_options=chrome_options)
driver.get("https://www.sahamyab.com/stocktwits")
last_tweet = ''
while driver.execute_script("return (true)"):
    tweet = driver.find_element(By.XPATH, "//div[2]/p").text
    if(last_tweet != tweet):
        print("tweet: " + tweet )
        username = driver.find_element(By.XPATH, "//app-user-status-bar/a").text
        print("username: " + username)
        producer.send('sample', {'tweet':tweet, 'user': username}).get(10)
        last_tweet = tweet
        print("---------------------------------")
    time.sleep(.01)

#
# from kafka import KafkaProducer
# producer = KafkaProducer(bootstrap_servers=['185.235.40.116:9092'])
# producer.send('sample', b'Hello, World!').get(10)
# producer.send('sample', key=b'message-two', value=b'This is Kafka-Python').get(10)