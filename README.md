# Flight Delay Prediction and Analysis Using PySpark and AWS

This repository provides an end-to-end workflow for **flight delay analysis and prediction**, leveraging **PySpark**, **AWS EMR**, **Amazon S3**, and **AWS Athena**. The project uses a publicly available **US DOT Flight Delays** dataset (from 2015) to demonstrate data cleaning, enrichment, visualization, and machine learning (ML) modeling. Below is an overview of each step, from setup to deployment.

## Table of Contents
1. [Project Overview](#project-overview)
2. [Data Description](#data-description)
3. [Architecture and Tools](#architecture-and-tools)
4. [Setup and Installation](#setup-and-installation)
5. [Workflow Outline](#workflow-outline)
6. [Key Steps and Scripts](#key-steps-and-scripts)
    - [1. Environment Setup](#1-environment-setup)
    - [2. Data Ingestion](#2-data-ingestion)
    - [3. Data Cleaning and Imputation](#3-data-cleaning-and-imputation)
    - [4. Exploratory Data Analysis (EDA)](#4-exploratory-data-analysis-eda)
    - [5. Data Enrichment](#5-data-enrichment)
    - [6. Writing Data to AWS S3](#6-writing-data-to-aws-s3)
    - [7. Athena Integration](#7-athena-integration)
    - [8. Machine Learning Pipeline](#8-machine-learning-pipeline)
    - [9. Model Training and Evaluation](#9-model-training-and-evaluation)
7. [Results](#results)
8. [Future Improvements](#future-improvements)
9. [License](#license)
10. [Contact](#contact)

---

## Project Overview

This project aims to **predict flight delays** and analyze factors influencing them. Key steps include:

- **Data Loading**: Import flight, airline, and airport data from **Amazon S3** using PySpark.
- **Data Cleaning & Imputation**: Handle missing values, remove duplicates, and transform columns.
- **Data Enrichment**: Join with additional airport and airline information to provide more context.
- **Exploratory Data Analysis**: Generate summary statistics, null counts, histograms, and correlation analyses.
- **Machine Learning Pipeline**: Build a logistic regression model using **Spark MLlib**, with **StringIndexer**, **OneHotEncoder**, and **VectorAssembler**.
- **Model Evaluation**: Compute accuracy, AUC, and confusion matrix to assess performance.
- **AWS Athena**: Create an external table to query the cleaned and enriched data stored in Parquet format on S3.

---

## Data Description

### Dataset Source
- **US DOT Flight Delays (2015)** from Kaggle: [Link](https://www.kaggle.com/datasets/usdot/flight-delays/data)

### Contents
- **Flights**: Metadata about flights, including departure times, delays, cancellations, distance, etc.
- **Airports**: Airport names, codes, cities, and states.
- **Airlines**: Airline codes and names.

### Size & Format
- The main data is in CSV format, converted to **Parquet** for efficient querying with AWS Athena.

---

## Architecture and Tools

- **PySpark (Spark DataFrames / Spark MLlib)**  
  For scalable data processing and machine learning pipelines.
  
- **AWS EMR**  
  For a distributed environment to handle large-scale data with Spark.
  
- **Amazon S3**  
  As the central storage for raw, cleaned, and processed datasets.
  
- **AWS Athena**  
  SQL querying on top of data in S3, enabling quick data retrieval without additional ETL overhead.

- **Boto3**  
  Python SDK for AWS, enabling programmatic operations (uploading files to S3, executing Athena queries, etc.).

- **Matplotlib / Pandas**  
  For additional data exploration, plotting histograms, and summarizing results locally.

---

## Setup and Installation

1. **AWS EMR Cluster**  
   - Create an EMR cluster with Spark installed.
   - Attach an IAM role with appropriate permissions to read/write to S3 and interact with Athena.

2. **Install Dependencies**  
   - This project uses PySpark, python-dateutil, numpy, pandas, matplotlib, boto3, fsspec, and s3fs.  
   - In a PySpark environment (e.g., a Jupyter notebook on EMR), install via:
     ```python
     sc.install_pypi_package("python-dateutil==2.8.2")
     sc.install_pypi_package("numpy")
     sc.install_pypi_package("pandas")
     sc.install_pypi_package("matplotlib")
     sc.install_pypi_package("boto3")
     sc.install_pypi_package("fsspec")
     sc.install_pypi_package("s3fs")
     ```
3. **Environment Variables**  
   - Configure AWS credentials (access key, secret key, session token if needed) either on the EMR cluster or via environment variables in your local environment.

---

## Workflow Outline

1. **Environment Setup**  
   Initialize SparkSession, install dependencies, and import necessary libraries.

2. **Data Ingestion**  
   Load CSV data from S3 into Spark DataFrames (`flights_df`, `airports_df`, and `airlines_df`).

3. **Data Cleaning and Imputation**  
   Identify columns with missing values, impute or drop as necessary:
   - Major delay columns: Replace nulls with `0`.
   - Cancellation reasons: Replace nulls with `"Not Cancelled"`.

4. **EDA & Visualizations**  
   - Summaries: `describe()`
   - Histograms of delay columns
   - Correlation analysis of numeric features
   - Upload plots to S3 for reporting

5. **Data Enrichment**  
   Join flights data with airlines and airports data to add more descriptive context.

6. **Save to S3 (Parquet)**  
   Store the enriched data in Parquet format in an S3 bucket for efficient querying.

7. **Athena Integration**  
   - Create a new database.
   - Create an external table pointing to the Parquet files on S3.
   - Validate data with `SELECT` queries, retrieving results into Pandas.

8. **Machine Learning**  
   - Feature Engineering (e.g., extracting hour from `SCHEDULED_DEPARTURE`, binary `early_morning`).
   - ML Pipeline in Spark (StringIndexer, OneHotEncoder, VectorAssembler).
   - Train/test split.
   - Logistic Regression for binary classification (`IS_DELAYED`).

9. **Evaluation**  
   - Accuracy and AUC metrics
   - Confusion Matrix

10. **Results**  
    - High accuracy and AUC on demonstration data
    - Potential need for real-world threshold tuning or advanced feature engineering

---

## Key Steps and Scripts

### 1. Environment Setup
- **`install_pypi_package`** calls for necessary libraries.
- **Imports** from `pyspark.sql`, `pyspark.ml`, `boto3`, etc.

### 2. Data Ingestion
- `spark.read.csv(...)` loads flights, airports, and airlines data from `s3://final-csc555/...`.

### 3. Data Cleaning and Imputation
- **Null checks** (`count_nulls`) to summarize missing data.
- **`fillna(...)`** for replacing missing values in delay columns with `0` and `"Not Cancelled"` for cancellation reasons.

### 4. Exploratory Data Analysis (EDA)
- **Describe** calls to inspect summary stats.
- **Sampling** to `.sample(...)` for plotting histograms with `matplotlib`.
- **Correlation** with `stat.corr(...)`.

### 5. Data Enrichment
- **Joins** with `airlines_df` and `airports_df` to add airline and airport details.
- **Dropping** non-essential columns.

### 6. Writing Data to AWS S3
- `.write.parquet("s3://...")` saves the cleaned and enriched DataFrame for further use.

### 7. Athena Integration
- **Create Database**: `CREATE DATABASE IF NOT EXISTS flight2_db;`
- **Create External Table**: Points to Parquet data.  
- **Query**: Sample data with `SELECT * FROM flights_transformed LIMIT 900;`.

### 8. Machine Learning Pipeline
- **Feature Engineering**: Convert date/time fields into numeric (`departure_hour`, `early_morning`).
- **StringIndexer**, **OneHotEncoder** for categorical fields.
- **VectorAssembler** merges numeric + categorical features into a single vector.

### 9. Model Training and Evaluation
- **Logistic Regression**: Family = “binomial”, label column = `IS_DELAYED`.
- **MulticlassClassificationEvaluator**: Accuracy.
- **BinaryClassificationEvaluator**: AUC (area under ROC).
- **Confusion Matrix**: Group by `IS_DELAYED`, `prediction`.

---

## Results

- **Model Performance**: Demonstrated near-perfect accuracy and AUC in the example subset.  
  *Real-world performance would vary based on comprehensive delay definitions and more realistic labels.*
- **Visualization**: Histograms and correlation analyses highlight patterns and relationships in flight delays.
- **Data Lake Architecture**: Using S3 + Athena allows scalable, serverless queries on large data volumes.

---

## Future Improvements

1. **Realistic Delay Labeling**  
   - Use actual departure/arrival delay thresholds (e.g., 15 minutes) to define `IS_DELAYED`.
2. **Feature Engineering**  
   - Incorporate weather data, seasonal trends, and advanced time-series features.
3. **Advanced Models**  
   - Experiment with Random Forests, Gradient Boosted Trees, or neural networks for improved accuracy.
4. **Hyperparameter Tuning**  
   - Use Spark’s ML tuning (CrossValidator, ParamGridBuilder) for model optimization.
5. **Handling Imbalanced Classes**  
   - If delayed vs. on-time flights are heavily imbalanced, consider resampling or class weighting.

---

## License

This project is provided under the [MIT License](LICENSE) (or your preferred license). You are free to use, modify, and distribute the code as stated in the license.

---

## Contact

For questions, suggestions, or collaboration opportunities, feel free to reach out:

- **Author**: Nishant Singh  
- **Email**: [nishantsinghns.mail@gmail.com](mailto:nishantsinghns.mail@gmail.com)

Feel free to open issues or pull requests in this repository if you have any improvements or feature requests. Thank you for exploring this project!
