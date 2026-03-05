-- Create Warehouse
CREATE OR REPLACE WAREHOUSE KAVYA_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

USE WAREHOUSE KAVYA_WH;

-- Create DB + Schema
CREATE OR REPLACE DATABASE CUSTOMER_DB;
CREATE OR REPLACE SCHEMA CUSTOMER_DB.RAW;
CREATE OR REPLACE SCHEMA CUSTOMER_DB.CLEAN;

USE DATABASE CUSTOMER_DB;
USE SCHEMA RAW;

-- Create a File formart
CREATE OR REPLACE FILE FORMAT CUSTOMER_CSV_FMT
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  EMPTY_FIELD_AS_NULL = TRUE;

-- Create a Stage
  CREATE OR REPLACE STAGE CUSTOMER_STAGE
  FILE_FORMAT = CUSTOMER_CSV_FMT;

-- Create the RAW table
CREATE OR REPLACE TABLE RAW.CUSTOMER_TRANSACTIONS_RAW (
  customer_id VARCHAR,
  age VARCHAR,
  gender VARCHAR,
  country VARCHAR,
  annual_income VARCHAR,
  spending_score VARCHAR,
  num_purchases VARCHAR,
  avg_purchase_value VARCHAR,
  membership_years VARCHAR,
  website_visits_per_month VARCHAR,
  cart_abandon_rate VARCHAR,
  churned VARCHAR,
  feedback_text VARCHAR,
  last_purchase_date VARCHAR
);

-- COPY INTO (load from stage -> Raw table)
 COPY INTO RAW.CUSTOMER_TRANSACTIONS_RAW 
 FROM @CUSTOMER_STAGE/Customer_Transactions.csv
 FILE_FORMAT = (FORMAT_NAME = CUSTOMER_CSV_FMT)
 ON_ERROR = 'CONTINUE';

-- Quick Validation checks
 SELECT COUNT(*) AS raw_rows FROM RAW.CUSTOMER_TRANSACTIONS_RAW;

SELECT * FROM RAW.CUSTOMER_TRANSACTIONS_RAW LIMIT 10;

-- Transform step (Raw -> Clean)
CREATE OR REPLACE TABLE CLEAN.CUSTOMER_TRANSACTIONS AS
SELECT
  TRY_TO_NUMBER(customer_id)                 AS customer_id,
  TRY_TO_NUMBER(age)                         AS age,
  TRIM(INITCAP(gender))                      AS gender,
  TRIM(country)                              AS country,
  TRY_TO_NUMBER(annual_income)               AS annual_income,
  TRY_TO_NUMBER(spending_score)              AS spending_score,
  TRY_TO_NUMBER(num_purchases)               AS num_purchases,
  TRY_TO_DOUBLE(avg_purchase_value)          AS avg_purchase_value,
  TRY_TO_NUMBER(membership_years)            AS membership_years,
  TRY_TO_NUMBER(website_visits_per_month)    AS website_visits_per_month,
  TRY_TO_DOUBLE(cart_abandon_rate)           AS cart_abandon_rate,
  IFF(TRY_TO_NUMBER(churned)=1, TRUE, FALSE) AS churned,
  feedback_text                              AS feedback_text,
  TRY_TO_DATE(last_purchase_date)            AS last_purchase_date,

  /* Derived columns (useful for reporting) */
  (TRY_TO_NUMBER(num_purchases) * TRY_TO_DOUBLE(avg_purchase_value)) AS estimated_total_spend,
  DATE_TRUNC('MONTH', TRY_TO_DATE(last_purchase_date))               AS last_purchase_month

FROM RAW.CUSTOMER_TRANSACTIONS_RAW;

-- Data quality checks
SELECT
  COUNT(*) AS total_rows,
  SUM(IFF(customer_id IS NULL, 1, 0)) AS bad_customer_id,
  SUM(IFF(last_purchase_date IS NULL, 1, 0)) AS bad_dates
FROM CLEAN.CUSTOMER_TRANSACTIONS;

-- Check duplicates on Customer_id
SELECT customer_id, COUNT(*) cnt
FROM CLEAN.CUSTOMER_TRANSACTIONS
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

-- Create 2 reporting views
-- Country level churn rate
CREATE OR REPLACE VIEW CLEAN.V_COUNTRY_CHURN AS
SELECT
  country,
  COUNT(*) AS customers,
  AVG(IFF(churned, 1, 0)) AS churn_rate
FROM CLEAN.CUSTOMER_TRANSACTIONS
GROUP BY country;

-- Monthly revenue trend
CREATE OR REPLACE VIEW CLEAN.V_MONTHLY_SPEND AS
SELECT
  last_purchase_month,
  SUM(estimated_total_spend) AS total_estimated_spend
FROM CLEAN.CUSTOMER_TRANSACTIONS
GROUP BY last_purchase_month
ORDER BY last_purchase_month;

-- validate the views
SELECT * FROM CLEAN.V_COUNTRY_CHURN ORDER BY churn_rate DESC LIMIT 20;
SELECT * FROM CLEAN.V_MONTHLY_SPEND ORDER BY last_purchase_month;

--create a BI ready semantic layer
-- create a dedicated schema
CREATE OR REPLACE SCHEMA CUSTOMER_DB.MART;

-- create final BI views/tables in MART
CREATE OR REPLACE VIEW MART.COUNTRY_CHURN AS
SELECT * FROM CLEAN.V_COUNTRY_CHURN;

--Add data quality checks
-- Create a simple 'DQ report' View
CREATE OR REPLACE VIEW MART.DQ_REPORT AS
SELECT
  COUNT(*) AS total_rows,
  SUM(IFF(customer_id IS NULL,1,0)) AS null_customer_id,
  SUM(IFF(last_purchase_date IS NULL,1,0)) AS null_last_purchase_date,
  SUM(IFF(age IS NULL,1,0)) AS null_age
FROM CLEAN.CUSTOMER_TRANSACTIONS;

-- Connect Power BI
