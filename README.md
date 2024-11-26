[![CI](https://github.com/nogibjj/skye-assignment-10/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/skye-assignment-10/actions/workflows/cicd.yml)

# Skye Assignment 10: Chess Player Transfers with PySpark

This project processes chess player transfer data using PySpark. It integrates an ETL pipeline to clean, transform, and query the data efficiently. A CI/CD pipeline is also configured using GitHub Actions to validate the code and ensure all workflows are tested.

## Pyspark script
The pyspark script can be found [here](https://github.com/nogibjj/skye-assignment-10/blob/main/mylib/etl.py)

## Output
The output can be found in [log](https://github.com/nogibjj/skye-assignment-10/blob/main/log/database.log) and [data](https://github.com/nogibjj/skye-assignment-10/tree/main/data)

## Features

1. **ETL Pipeline with PySpark**:
   - **Extract**: Reads raw data from a CSV file.
   - **Transform**: Renames columns, handles missing data, and prepares the dataset for querying.
   - **Load**: Saves the transformed data to a CSV file for further analysis.

2. **SQL Queries Using PySpark**:
   - Performs queries directly on the PySpark DataFrame using Spark SQL.
   - Example queries include transfer counts by federation and listing all transfers with specific conditions.

3. **CI/CD Pipeline**:
   - Automated with GitHub Actions.
   - Ensures code formatting, linting, and testing of the ETL and query workflows.

## Workflow

1. **Data Processing with PySpark**:
   - Reads the raw dataset from [FiveThirtyEight's dataset](https://github.com/fivethirtyeight/data/blob/master/chess-transfers/transfers.csv).
   - Applies transformations to rename columns and handle missing data.
   - Saves the cleaned data to a CSV file (`data/transformed_transfer.csv`).

2. **SQL Queries**:
   - Registers the DataFrame as a SQL temporary view.
   - Runs Spark SQL queries to analyze the data, such as:
     - Counting transfers by federation.
     - Filtering and retrieving specific records.

3. **CI/CD Pipeline**:
   - Validates the codebase using `ruff` for linting and `black` for formatting.
   - Tests the ETL pipeline and query workflows for correctness.

## Setup Instructions

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/nogibjj/skye-assignment-10.git
   cd skye-assignment-10
   ```

2. Install Dependencies:
   ```bash
   make install
   ```

3. Run the Project:
    ```bash
    make run
    ```

4. Format and Lint the Code:
    ```bash
    make format
    make lint
    ```

5. Run the CI/CD Pipeline Locally:
    ```bash
    make all
    ```

## CI/CD Pipeline

The CI/CD pipeline is configured using GitHub Actions. It includes the following stages:

1. **Code Formatting**:
   
   Enforced with `black` for clean and consistent code formatting.

2. **Linting**:
   
   Checked using `ruff` to ensure compliance with Python best practices.

3. **Testing**:
   
   Runs the ETL workflow and SQL queries on the PySpark DataFrame to verify correctness.

## Deliverables

1. **PySpark Script**:
   
   Processes the raw dataset using PySpark and saves the results as CSV files.

2. **Transformed Dataset**:
   
   Output data saved to `data/transformed_transfer.csv`.

3. **Query Results**:
   
   Query results saved to `data/transfer_summary.csv`.

4. **CI/CD Pipeline**:
   
   Automated testing, formatting, and linting.
