import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps

# Creating the Kafka Producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'),
                         acks = 'all')

# Reading the csv file and coverting the data in JSON format and also doing some data cleaning processes
df = pd.read_csv('C:/Users/chira/PycharmProjects/Kafka-SparkStream-HDFS-PostgreSql-DataEngineering-Project/data', dtype={'CustomerID': float})
df['CustomerID'].fillna(0, inplace=True)
df['Description'].fillna("No Discription Given", inplace=True)
df['CustomerID'] = df['CustomerID'].astype(int)
df['Description'] = df['Description'].astype(object)

count = 0
while True:
    for _, row in df.iterrows():
        if count != 100:
            json_data = row.to_dict()
            producer.send('sales', value=json_data)
            print(json_data)
            sleep(1)
            producer.flush()
            count +=1
        else:
            break




