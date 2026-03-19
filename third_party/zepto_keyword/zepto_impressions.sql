-- ============================================================
-- Zepto_Impressions
-- Per-(cdate, productid, merchantid, keywordid) estimated
-- overall, organic, and ad impressions, weighted by rank factor.
--
-- overall_impressions  = rankfactor(ov_rank)  * impressions
-- organic_impressions  = rankfactor(org_rank) * impressions
-- ad_impressions       = rankfactor(ad_rank)  * impressions
--
-- impressions is the merchant-keyword daily estimated impression
-- share (from ZeptoMerchantKeyword_Impressions).
-- rankfactor is a lookup table mapping integer rank → weight.
--
-- Source: ZeptoKeywordRanking_daily
--           × ZeptoMerchantKeyword_Impressions
--           × RankFactor (LEFT JOIN × 3)
-- Destination: ZeptoImpressions
--
-- Dependencies (must be populated first):
--   ZeptoKeywordRanking_daily          (avgMerge → ov_rank, ad_rank, org_rank)
--   ZeptoMerchantKeyword_Impressions   (sumMerge → impressions)
--   RankFactor                         (rank Int, rankFactor Float64)
--
-- To query final values use:
--   SELECT sumMerge(overall_impressions),
--          sumMerge(organic_impressions),
--          sumMerge(ad_impressions)
--   FROM ZeptoImpressions GROUP BY ...
-- ============================================================


-- ============================================================
-- TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS ZeptoImpressions
(
    cdate                Date,
    productid            String,
    merchantid           String,
    keywordid            String,

    brandid              String,
    categoryid           String,
    subcategoryid        String,
    cityname             String,

    overall_impressions  AggregateFunction(sum, Float64),
    organic_impressions  AggregateFunction(sum, Float64),
    ad_impressions       AggregateFunction(sum, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(cdate)
ORDER BY (cdate, keywordid, productid, merchantid);


-- ============================================================
-- MATERIALIZED VIEW
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS ZeptoImpressions_MV
TO ZeptoImpressions
AS
WITH
    kr AS (
        SELECT
            cdate,
            productid,
            merchantid,
            keywordid,
            anyLast(brandid)       AS brandid,
            anyLast(categoryid)    AS categoryid,
            anyLast(subcategoryid) AS subcategoryid,
            anyLast(cityname)      AS cityname,
            avgMerge(ov_rank)      AS ov_rank,
            avgMerge(ad_rank)      AS ad_rank,
            avgMerge(org_rank)     AS org_rank
        FROM ZeptoKeywordRanking_daily
        GROUP BY cdate, productid, merchantid, keywordid
    ),
    mki AS (
        SELECT
            cdate,
            merchantid,
            keywordid,
            sumMerge(impressions) AS impressions
        FROM ZeptoMerchantKeyword_Impressions
        GROUP BY cdate, merchantid, keywordid
    )
SELECT
    kr.cdate                                                                    AS cdate,
    kr.productid                                                                AS productid,
    kr.merchantid                                                               AS merchantid,
    kr.keywordid                                                                AS keywordid,

    anyLast(kr.brandid)                                                         AS brandid,
    anyLast(kr.categoryid)                                                      AS categoryid,
    anyLast(kr.subcategoryid)                                                   AS subcategoryid,
    anyLast(kr.cityname)                                                        AS cityname,

    sumState(toFloat64(coalesce(rf_ov.rankFactor  * mki.impressions, 0.0)))    AS overall_impressions,
    sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0)))    AS organic_impressions,
    sumState(toFloat64(coalesce(rf_ad.rankFactor  * mki.impressions, 0.0)))    AS ad_impressions

FROM kr
JOIN mki
    ON  kr.keywordid  = mki.keywordid
    AND kr.merchantid = mki.merchantid
    AND kr.cdate      = mki.cdate
LEFT JOIN RankFactor AS rf_ov  ON toInt64(round(kr.ov_rank))  = rf_ov.rank
LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank
LEFT JOIN RankFactor AS rf_ad  ON toInt64(round(kr.ad_rank))  = rf_ad.rank

GROUP BY
    kr.cdate,
    kr.productid,
    kr.merchantid,
    kr.keywordid;


-- ============================================================
-- BACKFILL INSERT — 15-day chunks
-- Run each block separately.
-- Both source tables use AggregatingMergeTree states (avgMerge /
-- sumMerge). Each chunk covers a 15-day window.
-- ============================================================

-- Chunk 1 (2026-01-01 to 2026-01-15)
INSERT INTO ZeptoImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM ZeptoKeywordRanking_daily WHERE cdate >= '2026-01-01' AND cdate <= '2026-01-15' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM ZeptoMerchantKeyword_Impressions WHERE cdate >= '2026-01-01' AND cdate <= '2026-01-15' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 2 (2026-01-16 to 2026-01-30)
INSERT INTO ZeptoImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM ZeptoKeywordRanking_daily WHERE cdate >= '2026-01-16' AND cdate <= '2026-01-30' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM ZeptoMerchantKeyword_Impressions WHERE cdate >= '2026-01-16' AND cdate <= '2026-01-30' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 3 (2026-01-31 to 2026-02-14)
INSERT INTO ZeptoImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM ZeptoKeywordRanking_daily WHERE cdate >= '2026-01-31' AND cdate <= '2026-02-14' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM ZeptoMerchantKeyword_Impressions WHERE cdate >= '2026-01-31' AND cdate <= '2026-02-14' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 4 (2026-02-15 to 2026-03-01)
INSERT INTO ZeptoImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM ZeptoKeywordRanking_daily WHERE cdate >= '2026-02-15' AND cdate <= '2026-03-01' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM ZeptoMerchantKeyword_Impressions WHERE cdate >= '2026-02-15' AND cdate <= '2026-03-01' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 5 (2026-03-02 to 2026-03-16)
INSERT INTO ZeptoImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM ZeptoKeywordRanking_daily WHERE cdate >= '2026-03-02' AND cdate <= '2026-03-16' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM ZeptoMerchantKeyword_Impressions WHERE cdate >= '2026-03-02' AND cdate <= '2026-03-16' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 6 (2026-03-17 to 2026-03-19)
INSERT INTO ZeptoImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM ZeptoKeywordRanking_daily WHERE cdate >= '2026-03-17' AND cdate <= '2026-03-19' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM ZeptoMerchantKeyword_Impressions WHERE cdate >= '2026-03-17' AND cdate <= '2026-03-19' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- ============================================================
-- QUERY HELPER — read final merged impressions
-- ============================================================

SELECT
    cdate,
    productid,
    merchantid,
    keywordid,
    brandid,
    categoryid,
    subcategoryid,
    cityname,
    sumMerge(overall_impressions)  AS overall_impressions,
    sumMerge(organic_impressions)  AS organic_impressions,
    sumMerge(ad_impressions)       AS ad_impressions
FROM ZeptoImpressions
GROUP BY
    cdate,
    productid,
    merchantid,
    keywordid,
    brandid,
    categoryid,
    subcategoryid,
    cityname
ORDER BY cdate DESC;
