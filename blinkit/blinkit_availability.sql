CREATE TABLE blinkit_availability
(
    cdate Date,
    cityname LowCardinality(String),
    productid UInt32,
    brandid String,
    categoryid Int32,
    subcategoryid Int32,
    city_category_weight Float64,
    lst_count Float64,
    avl_count Float64,
    lst Float64,
    avl Float64,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (productid, cdate, cityname);



CREATE MATERIALIZED VIEW blinkit_availability_mv
TO blinkit_availability
AS
WITH last_processed AS (
    SELECT
        coalesce(max(cdate), toDate('2026-01-01')) AS max_date
    FROM blinkit_availability
),

city_weights AS (
    SELECT
        cityname,
        categoryid,
        subcategoryid,
        cdate,
        sum(merchant_weight) AS city_category_weight
    FROM blinkit_merchant_weight
    WHERE cdate > (SELECT max_date FROM last_processed)
    GROUP BY
        cityname,
        categoryid,
        subcategoryid,
        cdate
)

SELECT
    pm.cdate,
    pm.cityname,
    pm.productid,
    any(pm.brandid) AS brandid,
    any(pm.categoryid) AS categoryid,
    any(pm.subcategoryid) AS subcategoryid,
    any(cw.city_category_weight) AS city_category_weight,

    count() AS lst_count,
    sum(if(pm.inventory > 0,1,0)) AS avl_count,

    coalesce(100.0 * sum(mw.merchant_weight) / nullIf(any(cw.city_category_weight),0), 0.0) AS lst,

    coalesce(100.0 * sum(if(pm.inventory>0,1,0) * mw.merchant_weight) / nullIf(any(cw.city_category_weight),0), 0.0 ) AS avl,

    now() AS updated_at

FROM blinkitproductmerchant_daily pm

LEFT JOIN blinkit_merchant_weight mw
    ON mw.merchantid = pm.merchantid
   AND mw.categoryid = pm.categoryid
   AND mw.subcategoryid = pm.subcategoryid
   AND mw.cdate = pm.cdate

LEFT JOIN city_weights cw
    ON cw.cityname = pm.cityname
   AND cw.categoryid = pm.categoryid
   AND cw.subcategoryid = pm.subcategoryid
   AND cw.cdate = pm.cdate

WHERE pm.cdate > (SELECT max_date FROM last_processed)

GROUP BY
    pm.cdate,
    pm.cityname,
    pm.productid;