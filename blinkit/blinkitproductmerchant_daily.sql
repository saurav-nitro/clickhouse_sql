CREATE TABLE blinkitproductmerchant_daily
(
    cdate Date,
    productid UInt32,
    merchantid UInt32,
    brandid String,
    categoryid Int32,
    subcategoryid Int32,
    cityname LowCardinality(String),
    iscombo UInt8,
    unit LowCardinality(String),
    mrp Float64,
    inventory Float64,
    discount Float64,
    price Float64,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(cdate)
ORDER BY (cdate, productid, merchantid);


CREATE MATERIALIZED VIEW blinkitproductmerchant_daily_mv
TO blinkitproductmerchant_daily
AS
WITH last_processed AS (
    SELECT
        coalesce(max(cdate), toDate('2026-01-01')) AS max_date
    FROM blinkitproductmerchant_daily
)

SELECT
    toDate(createdAt) AS cdate,
    productId AS productid,
    merchantId AS merchantid,
    inventory AS inventory,
    discount AS discount,
    price AS price,
    now() AS updated_at
FROM BlinkitProductMerchant where toDate(cdate) > '2026-02-15'

INNER JOIN BlinkitProduct p
    ON pm.productId = p.id

INNER JOIN BlinkitMerchant m
    ON pm.merchantId = m.id

WHERE
    p.isCombo = 0
    AND toDate(pm.createdAt) > (SELECT max_date FROM last_processed)

GROUP BY
    cdate,
    pm.productId,
    pm.merchantId;