-- Databricks notebook source
-- MAGIC %md
-- MAGIC How to pivot bronze.ticker_value table?
-- MAGIC
-- MAGIC How to cast string to date / string to decimal?
-- MAGIC
-- MAGIC How to handle null values in the dataset (ex. Google)?
-- MAGIC
-- MAGIC Why don’t we drop all null Values? 
-- MAGIC
-- MAGIC Answer: You want to be sure that you are excluding null value for a specific scenario as you don’t know how your data may behave later 
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW ticker_meta_update AS
SELECT DISTINCT
    tm.*, tl.name, tl.exchange, tl.assetType FROM bronze.ticker_metadata tm
LEFT JOIN default.ticker_listing tl
    ON tm.Ticker = tl.Symbol

-- COMMAND ----------

MERGE INTO silver.s_ticker_metadata
USING ticker_meta_update
ON silver.s_ticker_metadata.Ticker=ticker_meta_update.Ticker
WHEN MATCHED THEN
UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #sp_silver_ticker_meta = spark.sql('''  
-- MAGIC #SELECT DISTINCT
-- MAGIC #    tm.*, tl.name, tl.exchange, tl.assetType FROM bronze.ticker_metadata tm
-- MAGIC #LEFT JOIN default.ticker_listing tl
-- MAGIC #    ON tm.Ticker = tl.Symbol
-- MAGIC #'''
-- MAGIC #)
-- MAGIC #sp_silver_ticker_meta.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/silver/s_ticker_metadata") 
-- MAGIC #spark.sql("CREATE TABLE IF NOT EXISTS silver.s_ticker_metadata USING DELTA LOCATION '/FileStore/silver/s_ticker_metadata'")
