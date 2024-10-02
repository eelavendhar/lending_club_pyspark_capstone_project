## Project Title: Lending Club Capstone Project

### Overview
This project involves a comprehensive data processing and analysis workflow using PySpark, focusing on loan scoring. The primary objective is to score loan applications based on various factors, including customer history, loan details, and payment history, while filtering out bad data.

### Key Components
1. **Data Ingestion**: 
   - Loads data from various sources, including customer records, loan default records, and payment history.
   - Uses Parquet format for efficient data storage and retrieval.

2. **Data Cleaning**: 
   - Filters out bad customers by comparing datasets against a list of known bad records.
   - Employs left anti joins to exclude bad data efficiently.

3. **External Table Creation**:
   - Defines schemas for different datasets and creates external tables in the Spark SQL context for subsequent analysis.

4. **Scoring Mechanism**:
   - Implements a dynamic points configuration system to assign scores based on various customer attributes and loan details.
   - Calculates scores based on payment history, loan default history, and loan statuses.

5. **Final Scoring Calculation**:
   - Combines individual scoring components to generate a comprehensive total score for each loan application.
   - Creates a temporary view for further analysis and reporting.

### Data Flow
- The data flow in this project can be summarized as follows:
  1. Load raw data from CSV files.
  2. Clean the data by removing bad records.
  3. Store cleaned data as Parquet files for optimized storage.
  4. Create external tables for easy access in Spark SQL.
  5. Calculate scores based on pre-defined criteria and rules.
  6. Output the final loan scores for further analysis or reporting.

### Project Structure
- **databricks_original_notebook/**: Contains the original Databricks notebook from which you can create external tables.
- **data/input_dataset**: Holds the input data files used in the project.

## Disclaimer
Since I don't have Hive support on my local machine, the script won't create external tables. However, you can find the code in the "databricks_original_notebook" folder, which includes the Databricks notebook that allows you to create external tables.

### Technologies Used
- **PySpark**: For big data processing and analytics.
- **Apache Spark**: To create DataFrames, run SQL queries, and manage large datasets efficiently.
- **Parquet**: A columnar storage file format optimized for Spark.

### Conclusion
This loan scoring project demonstrates the use of PySpark for processing large datasets, implementing a scoring system based on multiple criteria, and effectively managing data quality through filtering techniques.