-- Databricks notebook source
SELECT * FROM 
 
  bronze.ticker_value tv

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW ticker_value_update AS
-- melt in SQL the database
SELECT 
  tv.Ticker, CAST(tv.variable as DATE) as stock_date, Open, Close, High, Low, Volume
--, CAST(tv1.min_variable AS DATE) AS open_date_stock --> Open stock is just    used to filter  null rows
FROM 
  bronze.ticker_value tv
PIVOT (
  MAX(CAST(tv.value AS DECIMAL(20,2)))
  FOR Value_Type IN ('1. open' AS Open, '4. close' AS Close, '2. high' as High, '3. low' as Low, '5. volume' as Volume)
)
LEFT JOIN (SELECT MIN(CAST(variable AS DATE)) AS min_variable, Ticker 
          FROM 
            bronze.ticker_value 
          WHERE 
            value IS NOT NULL GROUP BY Ticker) tv1
  ON tv.Ticker = tv1.Ticker
WHERE 
  CAST(variable as DATE) > CAST(tv1.min_variable AS DATE) 
-- doc : https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-pivot.html

-- COMMAND ----------

MERGE INTO silver.s_ticker_value
USING ticker_value_update
ON silver.s_ticker_value.Ticker=ticker_value_update.Ticker
and silver.s_ticker_value.stock_date = ticker_value_update.stock_date
WHEN MATCHED THEN
UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- DBTITLE 1,Only for the first run we need to create the delta table
-- MAGIC %python
-- MAGIC #sp_silver_ticker_value = spark.sql('''
-- MAGIC # SELECT DISTINCT
-- MAGIC #-- melt in SQL the database
-- MAGIC #    tv.Ticker, CAST(variable as DATE) as stock_date, Open, Close, High, Low, Volume 
-- MAGIC #--, CAST(tv1.min_variable AS DATE) AS open_date_stock --> Open stock is just used to filter  null rows
-- MAGIC #FROM
-- MAGIC #  bronze.ticker_value tv
-- MAGIC #PIVOT (
-- MAGIC #  MAX(CAST(value AS DECIMAL(20,2)))
-- MAGIC #  FOR Value_Type IN ('1. open' AS Open, '4. close' AS Close, '2. high' as High, '3. low' as Low, '5. volume' as Volume)
-- MAGIC #)
-- MAGIC #LEFT JOIN (SELECT MIN(CAST(variable AS DATE)) AS min_variable, Ticker FROM bronze.ticker_value WHERE value IS NOT NULL GROUP BY Ticker) tv1
-- MAGIC #  ON tv.Ticker = tv1.Ticker --AND tv.variable = tv1.min_variable
-- MAGIC #WHERE 
-- MAGIC #    CAST(variable as DATE) > CAST(tv1.min_variable AS DATE) 
-- MAGIC #-- doc : https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-pivot.html
-- MAGIC #'''
-- MAGIC #)
-- MAGIC #sp_silver_ticker_value.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/silver/s_ticker_value") 
-- MAGIC #spark.sql("CREATE TABLE IF NOT EXISTS silver.s_ticker_value USING DELTA LOCATION '/FileStore/silver/s_ticker_value'")

-- COMMAND ----------


