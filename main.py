"""
Main ETL-Query Script Using PySpark
"""
from mylib.log import init_log
from mylib.load import create_spark, load_data, transform_data, save_data, query_data

if __name__ == "__main__":
    # Initialize logging
    logger = init_log()
    logger.info("Starting ETL-Query Process...")

    # Create Spark session
    logger.info("Initializing Spark session...")
    spark = create_spark("TransferETL")

    # Load data
    logger.info("Loading data...")
    data = load_data(spark)

    # Transform data
    logger.info("Transforming data...")
    transformed_data = transform_data(data)

    # Save transformed data
    logger.info("Saving transformed data...")
    save_data(transformed_data)

    # Query data
    logger.info("Querying data...")
    query_results = query_data(transformed_data, spark)

    # Stop Spark session
    spark.stop()
    logger.info("ETL-Query process completed.")
