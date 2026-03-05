## Snowflake ELT Pipeline – Customer Transactions
### Project Overview

This project demonstrates an ELT pipeline in Snowflake that ingests customer transaction data from a CSV file, loads it into a raw layer, performs data transformations and quality checks, and creates reporting views for analytics.

### Architecture

CSV File → Snowflake Stage → RAW Table → CLEAN Table → Reporting Views

## Files in This Repository
### File	Description

Customer.sql -> Main SQL script containing warehouse creation, database/schema setup, stage creation, raw table loading, transformation logic, reporting views, and task automation.

Customer_Validation_rawrows_10...csv  -> 	Sample validation output showing raw table data after ingestion.

Customer_clean_Qualitycheck.csv	-> Data quality validation results for the cleaned dataset.

Customer_validate_view (churn ...).csv	-> Output validation for reporting views such as churn analysis.

DQ_report_validation.png ->	Screenshot showing the data quality validation results executed in Snowflake.


## Key Pipeline Steps

Created a Snowflake warehouse, database, and schemas (RAW, CLEAN).

Loaded CSV data into Snowflake using internal stage and COPY INTO command.

Built a RAW table storing original data as VARCHAR for safe ingestion.

Created a CLEAN table with proper data types and derived columns.

Implemented data quality checks and validation queries.

Built analytics reporting views for churn and spending insights.

## Key SQL Functions Used

TRY_TO_NUMBER, TRY_TO_DOUBLE, TRY_TO_DATE – safe data type conversions

TRIM, INITCAP – text standardization

IFF – conditional logic

DATE_TRUNC – time-based aggregation

## Validation

Data validation was performed to ensure:

Successful raw data ingestion

Correct transformation of data types

Accurate reporting view outputs

Data quality checks for null values and inconsistencies

## Tools Used

Snowflake

SQL

Snowflake Snowsight UI
