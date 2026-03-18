-- ============================================================
-- Instamart_Merchant_Weight
-- Merchant's share of daily/weekly sales within each
-- (cdate, categoryid, subcategoryid) bucket.
-- Source: Instamart_Sales_Weekly
-- ============================================================

CREATE TABLE IF NOT EXISTS Instamart_Merchant_Weight
(
    cdate                  Date,
    merchantid             String,
    categoryid             String,
    subcategoryid          String,

    cityname               AggregateFunction(min, String),
    daily_sales            AggregateFunction(sum, Float64),
    weekly_sales           AggregateFunction(sum, Float64),

    merchant_daily_sales   AggregateFunction(sum, Float64),
    merchant_weekly_sales  AggregateFunction(sum, Float64),
    category_daily_sales   AggregateFunction(sum, Float64),
    category_weekly_sales  AggregateFunction(sum, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(cdate)
ORDER BY (cdate, merchantid, categoryid, subcategoryid);




CREATE MATERIALIZED VIEW IF NOT EXISTS Instamart_Merchant_Weight_MV
TO Instamart_Merchant_Weight
AS
WITH category_sales AS (
    SELECT
        cdate,
        categoryid,
        subcategoryid,
        sum(daily_sales)  AS daily_sales,
        sum(weekly_sales) AS weekly_sales
    FROM Instamart_Sales_Weekly
    GROUP BY
        cdate,
        categoryid,
        subcategoryid
)
SELECT
    sw.cdate,
    sw.merchantid,
    sw.categoryid,
    sw.subcategoryid,
    minState(sw.cityname)                                         AS cityname,
    sumState(sw.daily_sales)                                      AS daily_sales,
    sumState(sw.weekly_sales)                                     AS weekly_sales,
    sumState(sw.daily_sales)                                      AS merchant_daily_sales,
    sumState(sw.weekly_sales)                                     AS merchant_weekly_sales,
    sumState(toFloat64(cs.daily_sales))                           AS category_daily_sales,
    sumState(toFloat64(cs.weekly_sales))                          AS category_weekly_sales
FROM Instamart_Sales_Weekly AS sw
INNER JOIN category_sales AS cs
    ON  sw.cdate         = cs.cdate
    AND sw.categoryid    = cs.categoryid
    AND sw.subcategoryid = cs.subcategoryid
GROUP BY
    sw.cdate,
    sw.merchantid,
    sw.categoryid,
    sw.subcategoryid;


-- ============================================================
-- INITIAL BACKFILL INSERT
-- Run ONCE after creating the table and MV to populate
-- Instamart_Merchant_Weight with all historical data from
-- Instamart_Sales_Weekly. The MV will handle new inserts
-- going forward automatically.
-- ============================================================

INSERT INTO Instamart_Merchant_Weight
WITH category_sales AS (
    SELECT
        cdate,
        categoryid,
        subcategoryid,
        sum(daily_sales)  AS daily_sales,
        sum(weekly_sales) AS weekly_sales
    FROM Instamart_Sales_Weekly
    GROUP BY
        cdate,
        categoryid,
        subcategoryid
)
SELECT
    sw.cdate,
    sw.merchantid,
    sw.categoryid,
    sw.subcategoryid,
    minState(sw.cityname)               AS cityname,
    sumState(sw.daily_sales)            AS daily_sales,
    sumState(sw.weekly_sales)           AS weekly_sales,
    sumState(sw.daily_sales)            AS merchant_daily_sales,
    sumState(sw.weekly_sales)           AS merchant_weekly_sales,
    sumState(toFloat64(cs.daily_sales)) AS category_daily_sales,
    sumState(toFloat64(cs.weekly_sales))AS category_weekly_sales
FROM Instamart_Sales_Weekly AS sw
INNER JOIN category_sales AS cs
    ON  sw.cdate         = cs.cdate
    AND sw.categoryid    = cs.categoryid
    AND sw.subcategoryid = cs.subcategoryid
GROUP BY
    sw.cdate,
    sw.merchantid,
    sw.categoryid,
    sw.subcategoryid;
