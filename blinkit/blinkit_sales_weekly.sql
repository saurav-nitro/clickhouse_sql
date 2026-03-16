CREATE TABLE blinkit_sales_weekly
(
    cdate Date,
    productid UInt32,
    merchantid UInt32,
    brandid String,
    cityname LowCardinality(String),
    categoryid Int32,
    subcategoryid Int32,
    daily_sales Float64,
    weekly_sales Float64,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (productid, merchantid, cdate);



CREATE MATERIALIZED VIEW blinkit_sales_weekly_mv
TO blinkit_sales_weekly
AS
WITH last_processed_date AS (
    SELECT
        coalesce(max(cdate), toDate('2026-01-01')) AS max_date
    FROM blinkit_sales_weekly
),

daily_aggregation AS (
    SELECT
        bs.cdate,
        bs.productid,
        bs.merchantid,
        any(bs.brandid) AS brandid,
        any(m.cityName) AS cityname,
        any(bs.categoryid) AS categoryid,
        any(bs.subcategoryid) AS subcategoryid,
        greatest(sum(bs.sales), 1e-2) AS daily_sales
    FROM blinkit_sales bs
    INNER JOIN blinkitmerchant m ON m.id = bs.merchantid
    WHERE bs.cdate >= (
        (SELECT max_date FROM last_processed_date) - INTERVAL 6 DAY
    )
    GROUP BY
        bs.merchantid,
        bs.productid,
        bs.cdate
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
    greatest(
        sum(daily_sales) OVER (
            PARTITION BY merchantid, productid
            ORDER BY cdate
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ),
        1e-2
    ) AS weekly_sales,
    now() AS updated_at
FROM daily_aggregation
WHERE cdate >= (
    SELECT max_date FROM last_processed_date
);