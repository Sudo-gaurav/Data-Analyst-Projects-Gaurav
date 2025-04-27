-- *******************************************************************
-- Retail Business Performance & Profitability Analysis
-- SQL Script: Data Setup, Cleaning, Views, and Analysis Queries
-- Dataset: Rossmann Store Sales
-- Prepared by Gaurav Tawri | Date: 2025-04-27
-- *******************************************************************

-- 0. Create schema (optional)
-- CREATE SCHEMA IF NOT EXISTS rossmann;

-- 1. Create and load raw tables

-- 1.1. Store metadata table
------------------------------------------------
CREATE TABLE IF NOT EXISTS store_table (
  Store                      INTEGER,
  StoreType                  TEXT,
  Assortment                 TEXT,
  CompetitionDistance        REAL,
  CompetitionOpenSinceMonth  REAL,
  CompetitionOpenSinceYear   REAL,
  Promo2                     INTEGER,
  Promo2SinceWeek            REAL,
  Promo2SinceYear            REAL,
  PromoInterval              TEXT
);

-- Load store data from CSV (psql \copy)
\COPY store_table
FROM 'C:/Users/gaura/Downloads/Data Analyst (Elevate Labs)/Projects/project 1/store.csv'
WITH (FORMAT csv, HEADER true);

-- 1.2. Sales transactions table
------------------------------------------------
CREATE TABLE IF NOT EXISTS train_table (
  Store         INTEGER,
  DayOfWeek     INTEGER,
  Date          TEXT,
  Sales         INTEGER,
  Customers     INTEGER,
  Open          INTEGER,
  Promo         INTEGER,
  StateHoliday  TEXT,
  SchoolHoliday INTEGER
);

-- Load sales data from cleaned CSV
COPY train_table
FROM 'C:/Users/gaura/Downloads/Data Analyst (Elevate Labs)/Projects/project 1/train_cleaned.csv'
WITH (FORMAT csv, HEADER true);

-- 1.3. Verify raw tables
------------------------------------------------
-- SELECT COUNT(*) FROM store_table;
-- SELECT COUNT(*) FROM train_table;

-- 2. Create cleaned staging views

-- 2.1. Clean sales: parse dates, normalize flags, extract date parts
------------------------------------------------
CREATE OR REPLACE VIEW v_cleaned_sales AS
SELECT
  store,
  -- Cast string Date to proper DATE type
  TO_DATE(date, 'YYYY-MM-DD')            AS sales_date,
  sales,
  customers,
  -- Normalize boolean flags
  CASE WHEN open = 1 THEN TRUE ELSE FALSE END AS is_open,
  CASE WHEN promo = 1 THEN TRUE ELSE FALSE END AS in_promo,
  NULLIF(stateholiday, '0')              AS state_holiday,
  schoolholiday::BOOLEAN                 AS is_school_holiday,
  -- Extract year, month, ISO week, and day of week (1=Mon..7=Sun)
  EXTRACT(YEAR  FROM TO_DATE(date, 'YYYY-MM-DD')) AS year,
  EXTRACT(MONTH FROM TO_DATE(date, 'YYYY-MM-DD')) AS month,
  EXTRACT(WEEK  FROM TO_DATE(date, 'YYYY-MM-DD')) AS week,
  EXTRACT(DOW   FROM TO_DATE(date, 'YYYY-MM-DD')) + 1 AS day_of_week
FROM train_table
WHERE open = 1;

-- 2.2. Clean store: handle nulls and normalize flags
------------------------------------------------
CREATE OR REPLACE VIEW v_cleaned_store AS
SELECT
  store,
  storetype,
  assortment,
  COALESCE(competitiondistance, 999999)   AS competition_distance,
  competitionopensincemonth                AS comp_open_month,
  competitionopensinceyear                 AS comp_open_year,
  CASE WHEN promo2 = 1 THEN TRUE ELSE FALSE END AS has_promo2,
  promo2sinceweek                          AS promo2_start_week,
  promo2sinceyear                          AS promo2_start_year,
  promointerval
FROM store_table;

-- 3. Merge cleaned sales and store data into a single view
------------------------------------------------
CREATE OR REPLACE VIEW v_merged_sales_store AS
SELECT
  s.store,
  s.sales_date,
  s.sales,
  s.customers,
  s.is_open,
  s.in_promo,
  s.state_holiday,
  s.is_school_holiday,
  s.year,
  s.month,
  s.week,
  s.day_of_week,
  st.storetype,
  st.assortment,
  st.competition_distance,
  st.comp_open_month,
  st.comp_open_year,
  st.has_promo2,
  st.promo2_start_week,
  st.promo2_start_year,
  st.promointerval
FROM v_cleaned_sales s
LEFT JOIN v_cleaned_store st
  ON s.store = st.store;

-- 4. Quick verification of record counts
------------------------------------------------
-- SELECT COUNT(*) AS sales_records  FROM v_cleaned_sales;
-- SELECT COUNT(*) AS store_records  FROM v_cleaned_store;
-- SELECT COUNT(*) AS merged_records FROM v_merged_sales_store;

-- 5. Intermediate aggregation: monthly store-level metrics
------------------------------------------------
CREATE OR REPLACE VIEW v_monthly_store_metrics AS
SELECT
  store,
  year,
  month,
  SUM(sales)                                     AS total_sales,
  AVG(customers)                                 AS avg_customers,
  SUM(CASE WHEN in_promo THEN sales ELSE 0 END)  AS promo_sales,
  SUM(CASE WHEN in_promo THEN sales ELSE 0 END)  / NULLIF(SUM(sales), 0) AS promo_sales_percentage
FROM v_merged_sales_store
GROUP BY store, year, month
ORDER BY store, year, month;

-- 6. Final profitability analysis view
------------------------------------------------
CREATE OR REPLACE VIEW v_profit_analysis AS
SELECT
  m.store,
  m.year,
  m.month,
  -- Use assumption for category and subcategory
  ms.storetype    AS category,
  ms.assortment   AS subcategory,
  -- Sales metrics
  m.total_sales,
  m.avg_customers    AS total_customers,
  -- Promotion metrics
  m.promo_sales,
  m.promo_sales_percentage,
  -- Estimated profit (assumed 10% margin)
  ROUND(m.total_sales * 0.10, 2)                             AS estimated_profit,
  CASE WHEN m.total_sales > 0
       THEN ROUND((m.total_sales * 0.10) / m.total_sales, 4)
       ELSE NULL
  END                                                      AS estimated_profit_margin
FROM v_monthly_store_metrics m
JOIN v_merged_sales_store ms
  ON m.store = ms.store
  AND m.year  = ms.year
  AND m.month = ms.month
GROUP BY m.store, m.year, m.month, ms.storetype, ms.assortment, m.total_sales, m.avg_customers, m.promo_sales, m.promo_sales_percentage
ORDER BY m.store, m.year, m.month;

-- 7. Sample query to preview results
------------------------------------------------
-- SELECT * FROM v_profit_analysis LIMIT 5;

-- End of Script
-- *******************************************************************
