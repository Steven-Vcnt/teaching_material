-- Databricks notebook source
-- MAGIC
-- MAGIC %md
-- MAGIC Why do we have some prices drops for Google & Apple?
-- MAGIC
-- MAGIC Google price drop == 20-to-1 in July 2022 stock split 
-- MAGIC
-- MAGIC What just happened to the Alphabet share price? | The Motley Fool UK  https://www.fool.co.uk/2022/07/18/what-just-happened-to-the-alphabet-share-price/
-- MAGIC
-- MAGIC What Happens After a Stock Split https://www.investopedia.com/articles/01/072501.asp#:~:text=A%20stock%20split%20is%20a,shares%2C%20diminishing%20its%20stock%20price
-- MAGIC
-- MAGIC To be able to have the same graph as google finance https://www.google.com/finance/quote/GOOGL:NASDAQ?authuser=0&window=5Y
-- MAGIC
-- MAGIC A split is linked to the outstanding shares that the company has declared
-- MAGIC  Outstanding Shares Definition and How to Locate the Number  https://www.investopedia.com/terms/o/outstandingshares.asp
-- MAGIC
-- MAGIC To avoid this problem, we could use a split list 
-- MAGIC https://finnhub.io/docs/api/stock-splits 
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW gold_ticker_value_update AS
SELECT DISTINCT
  tm.name, tv.stock_date, tv.Open, tv.Close, tv.High, tv.Low, tv.Volume, (tv.Close - tv.Open)/tv.Open * 100 AS intra_day_evolution
  ,AVG(tv.Close) 
  OVER (
       ORDER BY tm.name, tv.stock_date
       ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
       ) AS 1D_Moving_Average
  , (tv.Close - 1D_Moving_Average)/1D_Moving_Average * 100 AS day_evolution
  ,AVG(tv.Close) 
    OVER (
        ORDER BY tm.name, tv.stock_date
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS 5D_Moving_Average
  , (tv.Close - 5D_Moving_Average)/5D_Moving_Average * 100 AS week_evolution
  ,AVG(tv.Close) 
    OVER (
        ORDER BY tm.name, tv.stock_date
        ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
        ) AS 1M_Moving_Average
        , (tv.Close - 1M_Moving_Average)/1M_Moving_Average * 100 AS month_evolution
  ,AVG(tv.Close) 
    OVER (
        ORDER BY tm.name, tv.stock_date
        ROWS BETWEEN 180 PRECEDING AND CURRENT ROW
        ) AS 6M_Moving_Average
        , (tv.Close - 6M_Moving_Average)/6M_Moving_Average * 100 AS semester_evolution
    ,AVG(tv.Close)
    OVER (
        ORDER BY tm.name, tv.stock_date
        ROWS BETWEEN 360 PRECEDING AND CURRENT ROW
        ) AS 1Y_Moving_Average
    , (tv.Close - 1Y_Moving_Average)/1Y_Moving_Average * 100 AS year_evolution

FROM 
  silver.s_ticker_value tv
LEFT JOIN silver.s_ticker_metadata tm
  ON tm.Ticker = tv.Ticker

-- COMMAND ----------

MERGE INTO gold.g_ticker_value
USING gold_ticker_value_update
ON gold.g_ticker_value.name=gold_ticker_value_update.name
and gold.g_ticker_value.stock_date = gold_ticker_value_update.stock_date
WHEN MATCHED THEN
UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #sp_gold_ticker_value = spark.sql('''
-- MAGIC #SELECT 
-- MAGIC #  tm.name, tv.stock_date, tv.Open, tv.Close, tv.High, tv.Low, tv.Volume, (tv.Close - tv.Open)/tv.Open * 100 AS intra_day_evolution
-- MAGIC #  ,AVG(tv.Close) 
-- MAGIC #  OVER (
-- MAGIC #       ORDER BY tm.name, tv.stock_date
-- MAGIC #       ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
-- MAGIC #       ) AS 1D_Moving_Average
-- MAGIC #  , (tv.Close - 1D_Moving_Average)/1D_Moving_Average * 100 AS day_evolution
-- MAGIC #  ,AVG(tv.Close) 
-- MAGIC #    OVER (
-- MAGIC #        ORDER BY tm.name, tv.stock_date
-- MAGIC #        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
-- MAGIC #        ) AS 5D_Moving_Average
-- MAGIC #  , (tv.Close - 5D_Moving_Average)/5D_Moving_Average * 100 AS week_evolution
-- MAGIC #  ,AVG(tv.Close) 
-- MAGIC #    OVER (
-- MAGIC #        ORDER BY tm.name, tv.stock_date
-- MAGIC #        ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
-- MAGIC #        ) AS 1M_Moving_Average
-- MAGIC #        , (tv.Close - 1M_Moving_Average)/1M_Moving_Average * 100 AS month_evolution
-- MAGIC #  ,AVG(tv.Close) 
-- MAGIC #    OVER (
-- MAGIC #        ORDER BY tm.name, tv.stock_date
-- MAGIC #        ROWS BETWEEN 180 PRECEDING AND CURRENT ROW
-- MAGIC #        ) AS 6M_Moving_Average
-- MAGIC #        , (tv.Close - 6M_Moving_Average)/6M_Moving_Average * 100 AS semester_evolution
-- MAGIC #    ,AVG(tv.Close)
-- MAGIC #    OVER (
-- MAGIC #        ORDER BY tm.name, tv.stock_date
-- MAGIC #        ROWS BETWEEN 360 PRECEDING AND CURRENT ROW
-- MAGIC #        ) AS 1Y_Moving_Average
-- MAGIC #    , (tv.Close - 1Y_Moving_Average)/1Y_Moving_Average * 100 AS year_evolution
-- MAGIC #FROM 
-- MAGIC #  silver.s_ticker_value tv
-- MAGIC #LEFT JOIN silver.s_ticker_metadata tm
-- MAGIC #  ON tm.Ticker = tv.Ticker
-- MAGIC #'''
-- MAGIC #)
-- MAGIC #sp_gold_ticker_value.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/gold/g_ticker_value") 
-- MAGIC #spark.sql("CREATE TABLE IF NOT EXISTS gold.g_ticker_value USING DELTA LOCATION '/FileStore/gold/g_ticker_value'")
