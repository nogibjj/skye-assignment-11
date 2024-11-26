import logging
from pyspark.sql import SparkSession

# Constants for paths
INPUT_PATH = "data/transformed_transfer"

def create_spark(app_name="ChessTransfersQueries"):
    """Initialize a Spark session."""
    return SparkSession.builder.appName(app_name).getOrCreate()

def query_data(input_path=INPUT_PATH):
    """Run queries on the transformed data."""
    spark = create_spark()
    logging.info("Loading transformed data...")
    
    # Read the transformed data
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
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
    transfer_count_df.show()

    # Save query results
    output_path = "data/transfer_summary"
    transfer_count_df.write.mode("overwrite").csv(output_path, header=True)
    print(f"Query results saved to {output_path}")

if __name__ == "__main__":
    query_data()
