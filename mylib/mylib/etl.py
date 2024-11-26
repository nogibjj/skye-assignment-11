"""ETL Functions Using PySpark"""
import logging
from pyspark.sql import SparkSession


def create_spark(app_name):
    """Initialize a Spark session"""
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark


def load_data(spark, dataset="data/transfer.csv"):
    """Load data into PySpark DataFrame"""
    df = spark.read.csv(dataset, header=True, inferSchema=True)
    logging.info("Data loaded successfully into PySpark DataFrame.")
    return df


def transform_data(df):
    """Perform data transformations"""
    logging.info("Transforming data...")

    # Rename columns
    df = (
        df.withColumnRenamed("ID", "player_id")
        .withColumnRenamed("Federation", "federation")
        .withColumnRenamed("Form.Fed", "former_fed")
        .withColumnRenamed("Transfer Date", "transfer_date")
    )

    # Fill missing values
    df = df.fillna({"former_fed": "UNKNOWN"})

    logging.info("Data transformation complete.")
    return df


def save_data(df, path="data/transformed_transfer.csv"):
    """Save transformed data to a CSV file"""
    df.write.mode("overwrite").csv(path, header=True)
    logging.info(f"Transformed data saved to {path}.")


def query_data(df, spark):
    """Perform SQL queries on the DataFrame"""
    logging.info("Registering DataFrame as a SQL temporary view...")
    df.createOrReplaceTempView("transfer_view")

    # Query: Count transfers by federation
    logging.info("Running query to count transfers by federation...")
    result = spark.sql(
        """
        SELECT federation, COUNT(*) AS transfer_count
        FROM transfer_view
        GROUP BY federation
        ORDER BY transfer_count DESC
        """
    )
    result.show()

    # Save query results to a CSV
    result.write.mode("overwrite").csv("data/transfer_summary.csv", header=True)
    logging.info("Query results saved to 'data/transfer_summary.csv'.")

    return result
