# from kafka import KafkaConsumer, KafkaProducer
# from json import loads
#
# consumer = KafkaConsumer('quickstart-events',
#                          bootstrap_servers=['185.235.40.116:9092'],
#                          auto_offset_reset='earliest',
#                          enable_auto_commit=True,
#                          value_deserializer=lambda x: loads(x.decode('utf-8')))
# for msg in consumer:
#     print (msg)

# from kafka import KafkaProducer
# producer = KafkaProducer(bootstrap_servers='185.235.40.116:9092')
# producer.send('quickstart-events', b'Hello, World!')
# producer.send('quickstart-events', key=b'message-two', value=b'This is Kafka-Python')

from kafka import KafkaConsumer
consumer = KafkaConsumer('sample',bootstrap_servers=['185.235.40.116:9092'])
for message in consumer:
    print (message)