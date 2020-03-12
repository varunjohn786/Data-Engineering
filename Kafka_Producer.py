from kafka import KafkaProducer
import json
import urllib.request
import time

coins = [['BTC', 'ZSVALLHOGMQTE9SQ'], ['ETH', 'NX04MCTUUG0NHG3V'], ['LTC', 'WBH0P3ZRKL0943TV'],
         ['XRP', 'OSVVNT4I4JQQCGTF']]

curr = ['USD', 'INR', 'EUR', 'JPY', 'CAD']

for x in coins:
    for y in curr:
        url = "https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_DAILY&symbol=" + x[0] +\
              "&market=" + y + "&apikey=" + x[1]
        print(url)
        contents = urllib.request.urlopen(url).read()
        json_data = json.loads(contents)
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        producer.send('Data_Engineering8', json.dumps(json_data).encode('utf-8'))
        producer.flush()
    if x[0]!= "XRP":
        time.sleep(60)

producer.send('Data_Engineering8', ("CLOSE").encode('utf-8'))
producer.flush()
print("CLOSE 1")

while True:
    for v in coins:
        url3 = "https://min-api.cryptocompare.com/data/price?fsym=" + v[0] + "&tsyms=USD,JPY,EUR,INR,CAD"
        print(url3)
        contents = urllib.request.urlopen(url3).read()
        json_data = json.loads(contents)
        json_data['Coin'] = v[0]
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        producer.send('Data_Engineering9', json.dumps(json_data).encode('utf-8'))
        producer.flush()
    time.sleep(10)