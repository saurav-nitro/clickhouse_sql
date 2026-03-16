CREATE TABLE blinkit_impressions
(
    cdate Date,
    productid UInt32,
    merchantid UInt32,
    keywordid String,

    brandid LowCardinality(String),
    categoryid Int32,
    subcategoryid Int32,
    cityname LowCardinality(String),

    overall_impressions Float64,
    organic_impressions Float64,
    ad_impressions Float64,

    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(cdate)
ORDER BY (cdate, keywordid, merchantid, productid);



CREATE MATERIALIZED VIEW blinkit_impressions_mv
TO blinkit_impressions
AS
WITH last_processed AS (
    SELECT
        coalesce(max(cdate), toDate('2026-01-01')) AS max_date
    FROM blinkit_impressions
)

SELECT
    kr.cdate,
    kr.productid,
    kr.merchantid,
    kr.keywordid,

    any(kr.brandid) AS brandid,
    any(kr.categoryid) AS categoryid,
    any(kr.subcategoryid) AS subcategoryid,
    any(kr.cityname) AS cityname,

    avg(rf_ov.rankfactor * mki.impressions) AS overall_impressions,
    avg(rf_org.rankfactor * mki.impressions) AS organic_impressions,
    avg(rf_ad.rankfactor * mki.impressions) AS ad_impressions,

    now() AS updated_at

FROM blinkitkeywordranking_daily kr

INNER JOIN blinkit_merchantkeyword_impressions mki
    ON kr.keywordid = mki.keywordid
    AND kr.merchantid = mki.merchantid
    AND kr.cdate = mki.cdate

LEFT JOIN rankfactor rf_ov
    ON round(kr.ov_rank) = rf_ov.rank

LEFT JOIN rankfactor rf_org
    ON round(kr.org_rank) = rf_org.rank

LEFT JOIN rankfactor rf_ad
    ON round(kr.ad_rank) = rf_ad.rank

WHERE kr.cdate > (SELECT max_date FROM last_processed)

GROUP BY
    kr.cdate,
    kr.merchantid,
    kr.keywordid,
    kr.productid;