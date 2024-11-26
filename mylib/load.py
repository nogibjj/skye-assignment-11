import logging
from pyspark.sql import SparkSession

# Constants for paths
DATASET_PATH = "data/transfer.csv"
OUTPUT_PATH = "data/transformed_transfer"

def create_spark(app_name="ChessTransfersPipeline"):
    """Initialize a Spark session."""
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_data(spark, dataset=DATASET_PATH):
    """Load the extracted data into a PySpark DataFrame."""
    logging.info("Loading data into Spark DataFrame...")
    df = spark.read.csv(dataset, header=True, inferSchema=True)
    print("Data loaded successfully:")
    df.show()
    return df

def transform_data(df):
    """Perform transformations on the data."""
    logging.info("Transforming data...")

    # Rename columns
    transformed_df = (
        df.withColumnRenamed("ID", "player_id")
          .withColumnRenamed("Federation", "federation")
          .withColumnRenamed("Form.Fed", "former_fed")
          .withColumnRenamed("Transfer Date", "transfer_date")
    )

    # Fill missing values
    transformed_df = transformed_df.fillna({"former_fed": "UNKNOWN"})
    
    print("Data transformation complete:")
    transformed_df.show()
    return transformed_df

def save_data(df, path=OUTPUT_PATH):
    """Save the transformed data to the specified output path."""
    logging.info(f"Saving data to {path}...")
    df.write.mode("overwrite").csv(path, header=True)
    print(f"Transformed data saved to {path}")

if __name__ == "__main__":
    spark = create_spark()
    raw_df = load_data(spark)
    transformed_df = transform_data(raw_df)
    save_data(transformed_df)
