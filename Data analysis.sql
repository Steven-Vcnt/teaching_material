-- Databricks notebook source
-- DBTITLE 1,Descriptive Statistics:
SELECT
    name,
    AVG(Close) AS AvgClosingPrice,
    MAX(Close) AS MaxClosingPrice,
    MIN(Close) AS MinClosingPrice,
    SUM(Volume) AS Total_Trading_Volume
FROM
    gold.g_ticker_value
GROUP BY
    name


-- COMMAND ----------

-- DBTITLE 1,Trend Analysis:
SELECT 
    name,
    COUNT(stock_date) AS Trading_Days,
    AVG(Volume) AS Avg_Daily_Trading_Volume
FROM 
    gold.g_ticker_value
GROUP BY 
    name;


-- COMMAND ----------

-- DBTITLE 1,Correlation and Relationships
SELECT
    CORR(t1.Close, t2.Close) AS Correlation
FROM
    gold.g_ticker_value t1
LEFT JOIN
    gold.g_ticker_value t2 ON t1.stock_date = t2.stock_date
WHERE
    t1.name = 'Alphabet Inc - Class A' AND t2.name = 'Apple Inc'


-- COMMAND ----------

-- DBTITLE 1,Volatility Analysis
SELECT 
    name,
    STDDEV(Close) AS Volatility
FROM 
    gold.g_ticker_value
GROUP BY 
    name;


-- COMMAND ----------

-- DBTITLE 1,Volatility Analysis in the last N days (depending of the LAG)
SELECT
    name,
    STDDEV(DailyReturn) AS Volatility
FROM
    (
        SELECT
            name,
            (Close - LAG(Close,5) OVER (PARTITION BY name ORDER BY stock_date)) / LAG(Close) OVER (PARTITION BY name ORDER BY stock_date) AS DailyReturn
        FROM
                gold.g_ticker_value

    ) AS Returns
GROUP BY
    name
ORDER BY
    Volatility DESC
LIMIT 5;


-- COMMAND ----------

-- DBTITLE 1,Performance Comparison
SELECT
    name,
    sum(CumulativeReturn) AS CumulativeReturn
FROM
(SELECT 
    Close - LAG(Close) OVER (PARTITION BY name ORDER BY stock_date) AS CumulativeReturn, name

FROM
    gold.g_ticker_value
WHERE
    stock_date >= '2023-01-01' AND stock_date <= '2023-12-31'
)
GROUP BY
    name
ORDER BY
    CumulativeReturn DESC
LIMIT 10

