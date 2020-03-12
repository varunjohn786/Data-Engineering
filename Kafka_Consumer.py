from kafka.admin import KafkaAdminClient, NewTopic
import json
from kafka import KafkaConsumer
import pymysql
import time

admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='test')

topic_list = []
topic_list.append(NewTopic(name="Data_Engineering9", num_partitions=1, replication_factor=1))
admin_client.create_topics(new_topics=topic_list, validate_only=False)

db = pymysql.connect("localhost", "root", "Zenfone@271993", "test")
cursor = db.cursor()

KafkaConsumer(consumer_timeout_ms=1000)
consumer = KafkaConsumer("Data_Engineering8", bootstrap_servers=['localhost:9092'], group_id=None
                         # ,
                         # auto_offset_reset='earliest'
)

cursor.execute("truncate table data_eng.bitcoin_daily")

for msg in consumer:
    if msg.value.decode('utf8') == "CLOSE":
        consumer.close()
        break
    else:
        tmp = json.loads(msg.value.decode('utf8')).get('Time Series (Digital Currency Daily)')
        metadata = json.loads(msg.value.decode('utf8')).get('Meta Data')
        DCN = metadata.get('3. Digital Currency Name')
        MC = metadata.get('4. Market Code')
        MN = metadata.get('5. Market Name')
        MCC = "4a. close (" + MC + ")"
        print(metadata)
        print(DCN,MC,MCC,MN)
        keyss = tmp.keys()
        for f in keyss:
            sql = """Insert into data_eng.bitcoin_daily values (%s,%s,%s,%s)"""
            cursor.execute(sql, (DCN, MN, f, tmp[f].get(MCC)))
            db.commit()
            print("Inserted")




consumer2 = KafkaConsumer("Data_Engineering9", bootstrap_servers=['localhost:9092'], group_id=None
                         # ,
                         # auto_offset_reset='earliest'
)

db = pymysql.connect("localhost", "root", "Zenfone@271993", "test")
cursor = db.cursor()
cursor.execute("truncate table data_eng.bitcoin_exchange_min")

for msg in consumer2:
    tmp = json.loads(msg.value.decode('utf8'))
    sql = """Insert into data_eng.bitcoin_exchange_min values (%s,%s,%s,%s,%s,%s,%s)"""
    cursor.execute(sql, (tmp.get('USD'), tmp.get('JPY'), tmp.get('EUR'), tmp.get('INR'),
                         tmp.get('CAD'), tmp.get('Coin'), time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())))
    db.commit()
    print("Inserted")