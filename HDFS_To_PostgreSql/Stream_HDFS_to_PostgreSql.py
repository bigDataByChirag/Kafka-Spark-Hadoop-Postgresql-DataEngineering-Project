from time import sleep
from pyspark.sql import SparkSession
import psycopg2
import json

# Creating a Spark session
spark = SparkSession.builder.appName("ReadJSONFromHDFS").getOrCreate()

# Setting the HDFS directory path
hdfs_path = "hdfs://localhost:9000/Spark_output/*.json"

# Connecting to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="Company_Sales",
    user="postgres",
    password="chiku"
)

while True:
    # Reading all JSON files in the directory into a DataFrame
    df = spark.read.json(hdfs_path)

    # Converting the DataFrame to a list of dictionaries
    data = df.toJSON().map(json.loads).collect()

    # Inserting each record into the PostgreSQL database
    with conn.cursor() as cur:
        for record in data:
            # Checking if a record with the same InvoiceNo and CustomerID already exists in the table
            cur.execute("""
                SELECT COUNT(*) FROM sales
                WHERE InvoiceNo = %s AND CustomerID = %s AND Description = %s
            """, (record['InvoiceNo'], record['CustomerID'], record['Description']))
            count = cur.fetchone()[0]
            if count == 0:
                # If no matching record is found, then inserting the record into the table
                cur.execute("""
                    INSERT INTO sales (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (record['InvoiceNo'], record['StockCode'], record['Description'], record['Quantity'], record['InvoiceDate'], record['UnitPrice'], record['CustomerID'], record['Country']))
                conn.commit()

    # Allowing to Sleep for some time before reading the directory again
    sleep(10)

# Closing the database connection after completion
conn.close()
