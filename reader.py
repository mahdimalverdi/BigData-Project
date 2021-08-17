from kafka import KafkaConsumer, KafkaProducer
from json import loads
from parsivar import Normalizer
from parsivar import Tokenizer
from parsivar import FindStems
from json import dumps

consumer = KafkaConsumer(
    'sample',
     bootstrap_servers=['185.235.40.116:9092'],
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')),
    api_version=(0, 10, 1))

producer = KafkaProducer(bootstrap_servers=['185.235.40.116:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

my_normalizer = Normalizer(statistical_space_correction=True)
my_tokenizer = Tokenizer()
my_stemmer = FindStems()

stopwords = set()
with open('Include/stopwords.txt', encoding='utf-8') as f:
    stopwords = set([word.strip() for word in f.readlines()])



for message in consumer:
    value = message.value
    cleaned_sentence = my_normalizer.normalize(value['tweet'])
    tokens = my_tokenizer.tokenize_words(cleaned_sentence)
    stems = []

    for word in [my_stemmer.convert_to_stem(item) for item in tokens]:
        if word not in stopwords:
            stems.append(word)

    value['cleaned_tweet'] = cleaned_sentence
    value['tokens'] = list(set(stems))
    # producer.send('cleaneddata', value)
    print (value)