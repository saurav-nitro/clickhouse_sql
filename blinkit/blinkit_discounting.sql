CREATE TABLE blinkit_discounting
(
    cdate Date,
    cityname LowCardinality(String),
    productid UInt32,
    brandid String,
    categoryid Int32,
    subcategoryid Int32,
    unit LowCardinality(String),
    unit_base LowCardinality(String),
    mrp Float64,
    city_category_weight Float64,
    weighted_discount Float64,
    weighted_ppu Float64,
    weighted_price Float64,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (cdate, cityname, productid);



CREATE MATERIALIZED VIEW blinkit_discounting_mv
TO blinkit_discounting
AS
WITH last_processed AS (
    SELECT
        coalesce(max(cdate), toDate('2026-01-01')) AS max_date
    FROM blinkit_discounting
)

SELECT
    pm.cdate,
    pm.cityname,
    pm.productid,

    any(pm.brandid) AS brandid,
    any(pm.categoryid) AS categoryid,
    any(pm.subcategoryid) AS subcategoryid,

    any(pm.unit) AS unit,
    any(uw.baseunit) AS unit_base,
    any(pm.mrp) AS mrp,

    sum(mw.merchant_weight) AS city_category_weight,

    if(sum(mw.merchant_weight) = 0, 0.0, sum(pm.discount * mw.merchant_weight) / sum(mw.merchant_weight)
    ) AS weighted_discount,

    if(sum(mw.merchant_weight) = 0, 0.0, sum((pm.price / if(uw.quantity = 0 OR uw.quantity IS NULL, 1, uw.quantity)) * mw.merchant_weight) / sum(mw.merchant_weight)
    ) AS weighted_ppu,

    if(sum(mw.merchant_weight) = 0, 0.0, sum(pm.price * mw.merchant_weight) / sum(mw.merchant_weight)
    ) AS weighted_price,

    now() AS updated_at

FROM blinkitproductmerchant_daily pm

LEFT JOIN unitweight uw
    ON uw.unit = pm.unit

INNER JOIN blinkit_merchant_weight mw
    ON mw.merchantid = pm.merchantid
   AND mw.categoryid = pm.categoryid
   AND mw.subcategoryid = pm.subcategoryid
   AND mw.cdate = pm.cdate

WHERE
    pm.inventory > 0
    AND pm.cdate > (SELECT max_date FROM last_processed)

GROUP BY
    pm.cdate,
    pm.cityname,
    pm.productid;