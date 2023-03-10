from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from kafka import KafkaConsumer

# creating a Kafka consumer
consumer = KafkaConsumer(
    'sales',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: m.decode('utf-8'),
    auto_offset_reset='latest'
)

# defining the schema for the incoming JSON data
schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("UnitPrice", FloatType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Country", StringType(), True)
])

# creating a SparkSession
spark = SparkSession.builder.appName("KafkaToHdfs")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")\
    .master('local[*]')\
    .getOrCreate()


# creating a streaming DataFrame from the Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales") \
    .option("startingOffsets", "latest")\
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data"))\
    .select("data.*")

# writing the JSON data to a file in HDFS
query = df \
    .writeStream \
    .format("json") \
    .option("checkpointLocation", "hdfs://localhost:9000/checkpoints/") \
    .option("path", "hdfs://localhost:9000/Spark_output/") \
    .trigger(processingTime='10 seconds') \
    .outputMode('append')\
    .option("maxFilesPerTrigger", 1) \
    .start()\
    .awaitTermination()


