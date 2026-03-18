-- ============================================================
-- Zepto_Sales_Weekly
-- Stores daily sales + 7-day rolling average (weekly_sales)
-- Source: Zepto_Sales + ZeptoMerchant (for cityname)
-- ============================================================

CREATE TABLE IF NOT EXISTS Zepto_Sales_Weekly
(
    cdate         Date,
    productid     String,
    merchantid    String,
    brandid       String,
    cityname      String,
    categoryid    String,
    subcategoryid String,
    daily_sales   Float64,
    weekly_sales  Float64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(cdate)
ORDER BY (cdate, productid, merchantid);




INSERT INTO Zepto_Sales_Weekly
WITH daily_aggregation AS (
    SELECT
        s.cdate,
        s.productid,
        s.merchantid,
        min(s.brandid)               AS brandid,
        any(m.cityName)              AS cityname,
        min(s.categoryid)            AS categoryid,
        min(s.subcategoryid)         AS subcategoryid,
        greatest(sum(s.sales), 0.01) AS daily_sales
    FROM Zepto_Sales AS s
    LEFT JOIN ZeptoMerchant AS m
        ON toString(m.id) = s.merchantid
    WHERE s.cdate >= (
        SELECT if(max(cdate) = toDate(0), toDate('2025-01-01'), max(cdate)) - toIntervalDay(6)
        FROM Zepto_Sales_Weekly
    )
    GROUP BY s.cdate, s.productid, s.merchantid
),
windowed AS (
    SELECT
        cdate,
        productid,
        merchantid,
        brandid,
        cityname,
        categoryid,
        subcategoryid,
        daily_sales,
        greatest(
            coalesce(
                avg(daily_sales) OVER (
                    PARTITION BY merchantid, productid
                    ORDER BY cdate
                    ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
                ),
                daily_sales
            ),
            0.01
        ) AS weekly_sales
    FROM daily_aggregation
)
SELECT
    cdate,
    productid,
    merchantid,
    brandid,
    cityname,
    categoryid,
    subcategoryid,
    daily_sales,
    weekly_sales
FROM windowed
WHERE cdate >= (
    SELECT if(max(cdate) = toDate(0), toDate('2025-01-01'), max(cdate))
    FROM Zepto_Sales_Weekly
);