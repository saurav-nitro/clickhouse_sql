CREATE TABLE blinkit_merchant_weight
(
    merchantid UInt32,
    cdate Date,
    categoryid Int32,
    subcategoryid Int32,
    cityname LowCardinality(String),
    daily_sales Float64,
    weekly_sales Float64,
    merchant_weight_daily Float64,
    merchant_weight Float64,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (merchantid, cdate, categoryid, subcategoryid);


CREATE MATERIALIZED VIEW blinkit_merchant_weight_mv
TO blinkit_merchant_weight
AS
WITH last_processed AS (
    SELECT
        coalesce(max(cdate), toDate('2026-01-01')) AS max_date
    FROM blinkit_merchant_weight
),

category_sales AS (
    SELECT
        sw.cdate,
        sw.categoryid,
        sw.subcategoryid,
        sum(sw.daily_sales) AS daily_sales,
        sum(sw.weekly_sales) AS weekly_sales
    FROM blinkit_sales_weekly sw
    WHERE sw.cdate >= (
        (SELECT max_date FROM last_processed)
    )
    GROUP BY
        sw.cdate,
        sw.categoryid,
        sw.subcategoryid
)

SELECT
    sw.merchantid,
    sw.cdate,
    sw.categoryid,
    sw.subcategoryid,
    any(sw.cityname) AS cityname,
    sum(sw.daily_sales) AS daily_sales,
    sum(sw.weekly_sales) AS weekly_sales,

    coalesce(
        sum(sw.daily_sales) / nullIf(any(cs.daily_sales), 0),
        0.0
    ) AS merchant_weight_daily,

    coalesce(
        sum(sw.weekly_sales) / nullIf(any(cs.weekly_sales), 0),
        0.0
    ) AS merchant_weight,

    now() AS updated_at

FROM blinkit_sales_weekly sw
JOIN category_sales cs
    ON sw.cdate = cs.cdate
   AND sw.categoryid = cs.categoryid
   AND sw.subcategoryid = cs.subcategoryid

WHERE sw.cdate >= (
    SELECT max_date FROM last_processed
)

GROUP BY
    sw.merchantid,
    sw.cdate,
    sw.categoryid,
    sw.subcategoryid;