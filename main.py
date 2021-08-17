from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
from json import dumps
from kafka import KafkaConsumer, KafkaProducer
from json import loads
from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver.support.wait import WebDriverWait

producer = KafkaProducer(bootstrap_servers=['185.235.40.116:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
#
# chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument('--headless')
# chrome_options.add_argument('--no-sandbox')
# chrome_options.add_argument('--disable-dev-shm-usage')
# driver = webdriver.Chrome("/usr/bin/chromedriver",chrome_options=chrome_options)
driver = webdriver.Chrome()
driver.get("https://twitter.com/login")

with open('Include/config', encoding='utf-8') as f:
    configs = [word.strip() for word in f.readlines()]

driver.find_element(By.NAME, "session[username_or_email]").send_keys(configs[0])
driver.find_element(By.NAME, "session[password]").send_keys(configs[1])
driver.find_element(By.XPATH, "//div[3]/div/div").click()
wait = WebDriverWait(driver, 1000)
element = wait.until(EC.element_to_be_clickable((By.XPATH, '//article/div/div/div/div[2]/div[2]/div[2]')))

tweets = set()

while True:
    for j in range(1, 100000):
        for i in range(1, 100000):
            try:
                tweet = driver.find_element(By.XPATH, "(//article/div/div/div/div[2]/div[2]/div[2]/div[1])["+str(i)+"]").text
                if tweet not in tweets:
                    display_name = driver.find_element(By.XPATH, "(//article/div/div/div/div[2]/div[2]/div[1]/div/div/div[1]/div[1]/a/div/div[1])["+str(i)+"]").text
                    user_name = driver.find_element(By.XPATH, "(//article/div/div/div/div[2]/div[2]/div[1]/div/div/div[1]/div[1]/a/div/div[2])["+str(i)+"]").text
                    datetime = driver.find_element(By.XPATH, "(//article/div/div/div/div[2]/div[2]/div[1]/div/div/div[1]/a/time)["+str(i)+"]").get_attribute("datetime")
                    print("username: " + user_name)
                    print("display name: " + display_name)
                    print("time: " + datetime)
                    producer.send('sample', {
                        'tweet': tweet,
                        'user': user_name,
                        'display_name': display_name,
                        'time': datetime
                    })
                    print("---------------------------------")
                    tweets.add(tweet)
            except:
                break;
        driver.execute_script("window.scrollBy(0,100000)")
        time.sleep(1)
    driver.execute_script("window.scrollTo(0,0)")
