CREATE TABLE blinkitkeywordranking_daily
(
    cdate Date,
    productid UInt32,
    merchantid UInt32,
    keywordid String,
    brandid String,
    categoryid Int32,
    subcategoryid Int32,
    cityname LowCardinality(String),
    ov_rank Float64,
    ad_rank Float64,
    org_rank Float64,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(cdate)
ORDER BY (cdate, keywordid, merchantid, productid);




CREATE MATERIALIZED VIEW blinkitkeywordranking_daily_mv
REFRESH EVERY 4 HOUR APPEND
TO blinkitkeywordranking_daily
AS
WITH last_processed AS (
    SELECT
        coalesce(max(cdate), toDate('2026-01-01')) AS max_date
    FROM blinkitkeywordranking_daily
)

SELECT
    toDate(kr.createdAt) AS cdate,

    kr.productid AS productid,
    kr.merchantid AS merchantid,
    kr.keywordid AS keywordid,

    any(p.brandid) AS brandid,
    any(p.categoryid) AS categoryid,
    any(p.subcategoryid) AS subcategoryid,
    any(m.cityname) AS cityname,

    avg(toFloat64(kr.rank)) AS ov_rank,
    avg(toFloat64(kr.adrank)) AS ad_rank,
    avg(toFloat64(kr.organicrank)) AS org_rank,

    now() AS updated_at

FROM blinkitkeywordranking kr

INNER JOIN blinkitproduct p
    ON kr.productid = p.id

INNER JOIN blinkitmerchant m
    ON kr.merchantid = m.id

WHERE
    p.isCombo = 0
    AND toDate(kr.createdAt) > (SELECT max_date FROM last_processed)

GROUP BY
    cdate,
    productid,
    merchantid,
    kr.keywordid;