import sqlite3
import os
from pyspark.sql import SparkSession

DB_name = "transfer"
DB = DB_name + ".db"


def create_spark(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark


def load(dataset="data/transfer.csv"):
    print(os.getcwd())
    spark = create_spark("transfer")

    # Load data using PySpark
    payload = spark.read.csv(dataset, header=True, inferSchema=True)

    # Rename columns to match your SQLite table schema
    payload = (
        payload.withColumnRenamed("ID", "player_id")
        .withColumnRenamed("Federation", "federation")
        .withColumnRenamed("Form.Fed", "former_fed")
        .withColumnRenamed("Transfer Date", "transfer_date")
    )

    # Handle missing values in former_fed column
    payload = payload.fillna({"former_fed": "UNKNOWN"})  # Replace NULL with "UNKNOWN"

    # Convert Spark DataFrame to a list of tuples for SQLite
    data_tuples = [tuple(row) for row in payload.collect()]

    # Connect to SQLite and create the table
    conn = sqlite3.connect(DB)
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {DB_name}")
    cursor.execute(
        f"""CREATE TABLE {DB_name} 
        (id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT NOT NULL, 
        player_id INTEGER NOT NULL, 
        federation CHARACTER NOT NULL,
        former_fed CHARACTER NOT NULL, 
        transfer_date TEXT NOT NULL)"""
    )

    # Insert data into the SQLite table
    cursor.executemany(
        f"""INSERT INTO {DB_name} (url,
         player_id,
          federation,
           former_fed,
            transfer_date)
        VALUES (?, ?, ?, ?, ?)""",
        data_tuples,
    )

    conn.commit()
    conn.close()
    return "transfer.db"
