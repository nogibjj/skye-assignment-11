import logging
from pyspark.sql import SparkSession

# Constants for paths
INPUT_PATH = "dbfs:/FileStore/skye-assignment-11/transformed_transfer.csv"
OUTPUT_PATH = "dbfs:/FileStore/skye-assignment-11/transfer_summary.csv"

def create_spark(app_name="ChessTransfersQueries"):
    """Initialize a Spark session."""
    return SparkSession.builder.appName(app_name).getOrCreate()

def query_data(input_path=INPUT_PATH, output_path=OUTPUT_PATH):
    """Run queries on the transformed data."""
    spark = create_spark()

    # Load the transformed data
    logging.info(f"Loading transformed data from: {input_path}")
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    print("Transformed data loaded:")
    df.show()

    # Create a temporary view for SQL queries
    df.createOrReplaceTempView("transfer_view")

    # Query: Count transfers by federation
    logging.info("Running query to count transfers by federation...")
    transfer_count_df = spark.sql("""
        SELECT federation, COUNT(*) AS transfer_count
        FROM transfer_view
        GROUP BY federation
        ORDER BY transfer_count DESC
    """)
    print("Query results:")
    transfer_count_df.show()

    # Save the query results as a single CSV file
    transfer_count_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
    print(f"Query results saved to: {output_path}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    query_data()
