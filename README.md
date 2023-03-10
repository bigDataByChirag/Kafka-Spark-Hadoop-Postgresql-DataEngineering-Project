# Kafka-SparkStream-HDFS-PostgreSql-DataEngineering-Project

![Beige Colorful Minimal Flowchart Infographic Graph (1)](https://user-images.githubusercontent.com/127500166/224323893-1d8d1ede-ac2f-4402-972c-b2383b6ae017.png)


1 . CSV to JSON Conversion: The project started with a CSV file that contained data in tabular form. The first step was to convert this CSV data into JSON format using the Pandas library. The CSV data was read into a Pandas DataFrame.

2. Kafka Producer: Once the CSV data was converted into JSON format, a Kafka Producer was used to send the data to a Kafka topic in a streaming fashion. The Producer sent each row of JSON data to the Kafka topic as a separate message. The messages were sent in real-time as they were generated, ensuring that the data was being streamed continuously.

# This Image shows what data is being produced:
![sc4](https://user-images.githubusercontent.com/127500166/224329182-0cfb89aa-e0bd-40a0-806b-e2464a7d01d9.png)

3. Spark Structured Streaming and Kafka Consumer: On the other end, Spark Structured Streaming and Kafka Consumer were used together to consume the data in real-time from the Kafka topic. The Consumer subscribed to the Kafka topic and consumed each message as it arrived. Spark Structured Streaming was used to process the data in real-time and transform it as needed.

# This Image shows how the Spark is sending streamed data in dataFrame Batches:
![sc5](https://user-images.githubusercontent.com/127500166/224329421-479c8cd3-aa0b-4a3e-be39-6792aa180325.png)

4. Hadoop Distributed File System (HDFS): The data was then stored in HDFS, a distributed file system that is designed to store large amounts of data across multiple servers. The data was continuously streamed into HDFS in real-time, allowing for the storage of large amounts of data in a distributed manner.

# This Image shows the files that are being stored in real time on the HDFS which contains json data: 
![SC3](https://user-images.githubusercontent.com/127500166/224329919-bf085fc6-8f14-4619-87d3-3bbd97f0b67c.png)


5. PostgreSQL Database: Finally, the real-time data from HDFS was taken and sent to a PostgreSQL database in real-time until all the data was sent. This allowed for the data to be stored in a relational database, making it easier to query and analyze.

# This is the first count:
![sc6](https://user-images.githubusercontent.com/127500166/224330358-fa6d76e4-2f23-48e9-995e-703f02b86475.png)


# This is the second count of the batch which is coming in real time:
![sc7](https://user-images.githubusercontent.com/127500166/224330309-fca63dce-5f80-4cad-b4f7-afd05db7b6d7.png)

# This is the Data which is stored in the Database:
![SC1](https://user-images.githubusercontent.com/127500166/224330650-e061e0ec-cb78-4a69-aacb-f96251c63190.png)


In summary, the project involved converting CSV data to JSON, streaming the data in real-time using Kafka, processing the data using Spark Structured Streaming, storing the data in HDFS, and then sending it to a PostgreSQL database in real-time. This allowed for the data to be collected and analyzed in real-time, making it possible to make timely decisions based on the data.

# Code to start Zookeeper -->

D:\kafka_2.12-3.4.0\bin\windows\zookeeper-server-start.bat D:\kafka_2.12-3.4.0\config\zookeeper.properties


# Code to start Kafka Server -->

D:/kafka_2.12-3.4.0/bin/windows/kafka-server-start.bat D:/kafka_2.12-3.4.0/config/server.properties

# Create a Topic -->

 D:/kafka_2.12-3.4.0/bin/windows/kafka-topics.bat --create --topic sales --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
