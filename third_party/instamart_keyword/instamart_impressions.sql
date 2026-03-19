-- ============================================================
-- Instamart_Impressions
-- Per-(cdate, productid, merchantid, keywordid) estimated
-- overall, organic, and ad impressions, weighted by rank factor.
--
-- overall_impressions  = rankfactor(ov_rank)  * impressions
-- organic_impressions  = rankfactor(org_rank) * impressions
-- ad_impressions       = rankfactor(ad_rank)  * impressions
--
-- impressions is the merchant-keyword daily estimated impression
-- share (from InstamartMerchantKeyword_Impressions).
-- rankfactor is a lookup table mapping integer rank → weight.
--
-- Source: InstamartKeywordRanking_daily
--           × InstamartMerchantKeyword_Impressions
--           × RankFactor (LEFT JOIN × 3)
-- Destination: InstamartImpressions
--
-- Dependencies (must be populated first):
--   InstamartKeywordRanking_daily          (avgMerge → ov_rank, ad_rank, org_rank)
--   InstamartMerchantKeyword_Impressions   (sumMerge → impressions)
--   RankFactor                             (rank Int, rankFactor Float64)
--
-- To query final values use:
--   SELECT sumMerge(overall_impressions),
--          sumMerge(organic_impressions),
--          sumMerge(ad_impressions)
--   FROM InstamartImpressions GROUP BY ...
-- ============================================================


-- ============================================================
-- TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS InstamartImpressions
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

CREATE MATERIALIZED VIEW IF NOT EXISTS InstamartImpressions_MV
TO InstamartImpressions
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
        FROM InstamartKeywordRanking_daily
        GROUP BY cdate, productid, merchantid, keywordid
    ),
    mki AS (
        SELECT
            cdate,
            merchantid,
            keywordid,
            sumMerge(impressions) AS impressions
        FROM InstamartMerchantKeyword_Impressions
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
-- BACKFILL INSERT — 5-day chunks (2026-01-01 → 2026-03-19)
-- Run each block separately.
-- Both source tables use AggregatingMergeTree states (avgMerge /
-- sumMerge). Each chunk covers a 5-day window.
-- ============================================================

-- Chunk 1 (2026-01-01 to 2026-01-05)
INSERT INTO InstamartImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM InstamartKeywordRanking_daily WHERE cdate >= '2026-01-01' AND cdate <= '2026-01-05' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM InstamartMerchantKeyword_Impressions WHERE cdate >= '2026-01-01' AND cdate <= '2026-01-05' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 2 (2026-01-06 to 2026-01-10)
INSERT INTO InstamartImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM InstamartKeywordRanking_daily WHERE cdate >= '2026-01-06' AND cdate <= '2026-01-10' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM InstamartMerchantKeyword_Impressions WHERE cdate >= '2026-01-06' AND cdate <= '2026-01-10' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 3 (2026-01-11 to 2026-01-15)
INSERT INTO InstamartImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM InstamartKeywordRanking_daily WHERE cdate >= '2026-01-11' AND cdate <= '2026-01-15' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM InstamartMerchantKeyword_Impressions WHERE cdate >= '2026-01-11' AND cdate <= '2026-01-15' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 4 (2026-01-16 to 2026-01-20)
INSERT INTO InstamartImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM InstamartKeywordRanking_daily WHERE cdate >= '2026-01-16' AND cdate <= '2026-01-20' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM InstamartMerchantKeyword_Impressions WHERE cdate >= '2026-01-16' AND cdate <= '2026-01-20' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 5 (2026-01-21 to 2026-01-25)
INSERT INTO InstamartImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM InstamartKeywordRanking_daily WHERE cdate >= '2026-01-21' AND cdate <= '2026-01-25' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM InstamartMerchantKeyword_Impressions WHERE cdate >= '2026-01-21' AND cdate <= '2026-01-25' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 6 (2026-01-26 to 2026-01-31)
INSERT INTO InstamartImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM InstamartKeywordRanking_daily WHERE cdate >= '2026-01-26' AND cdate <= '2026-01-31' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM InstamartMerchantKeyword_Impressions WHERE cdate >= '2026-01-26' AND cdate <= '2026-01-31' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 7 (2026-02-01 to 2026-02-05)
INSERT INTO InstamartImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM InstamartKeywordRanking_daily WHERE cdate >= '2026-02-01' AND cdate <= '2026-02-05' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM InstamartMerchantKeyword_Impressions WHERE cdate >= '2026-02-01' AND cdate <= '2026-02-05' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 8 (2026-02-06 to 2026-02-10)
INSERT INTO InstamartImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM InstamartKeywordRanking_daily WHERE cdate >= '2026-02-06' AND cdate <= '2026-02-10' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM InstamartMerchantKeyword_Impressions WHERE cdate >= '2026-02-06' AND cdate <= '2026-02-10' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 9 (2026-02-11 to 2026-02-15)
INSERT INTO InstamartImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM InstamartKeywordRanking_daily WHERE cdate >= '2026-02-11' AND cdate <= '2026-02-15' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM InstamartMerchantKeyword_Impressions WHERE cdate >= '2026-02-11' AND cdate <= '2026-02-15' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 10 (2026-02-16 to 2026-02-20)
INSERT INTO InstamartImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM InstamartKeywordRanking_daily WHERE cdate >= '2026-02-16' AND cdate <= '2026-02-20' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM InstamartMerchantKeyword_Impressions WHERE cdate >= '2026-02-16' AND cdate <= '2026-02-20' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 11 (2026-02-21 to 2026-02-25)
INSERT INTO InstamartImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM InstamartKeywordRanking_daily WHERE cdate >= '2026-02-21' AND cdate <= '2026-02-25' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM InstamartMerchantKeyword_Impressions WHERE cdate >= '2026-02-21' AND cdate <= '2026-02-25' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 12 (2026-02-26 to 2026-02-28)
INSERT INTO InstamartImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM InstamartKeywordRanking_daily WHERE cdate >= '2026-02-26' AND cdate <= '2026-02-28' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM InstamartMerchantKeyword_Impressions WHERE cdate >= '2026-02-26' AND cdate <= '2026-02-28' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 13 (2026-03-01 to 2026-03-05)
INSERT INTO InstamartImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM InstamartKeywordRanking_daily WHERE cdate >= '2026-03-01' AND cdate <= '2026-03-05' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM InstamartMerchantKeyword_Impressions WHERE cdate >= '2026-03-01' AND cdate <= '2026-03-05' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 14 (2026-03-06 to 2026-03-10)
INSERT INTO InstamartImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM InstamartKeywordRanking_daily WHERE cdate >= '2026-03-06' AND cdate <= '2026-03-10' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM InstamartMerchantKeyword_Impressions WHERE cdate >= '2026-03-06' AND cdate <= '2026-03-10' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 15 (2026-03-11 to 2026-03-15)
INSERT INTO InstamartImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM InstamartKeywordRanking_daily WHERE cdate >= '2026-03-11' AND cdate <= '2026-03-15' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM InstamartMerchantKeyword_Impressions WHERE cdate >= '2026-03-11' AND cdate <= '2026-03-15' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 16 (2026-03-16 to 2026-03-19)
INSERT INTO InstamartImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM InstamartKeywordRanking_daily WHERE cdate >= '2026-03-16' AND cdate <= '2026-03-19' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM InstamartMerchantKeyword_Impressions WHERE cdate >= '2026-03-16' AND cdate <= '2026-03-19' GROUP BY cdate, merchantid, keywordid)
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
FROM InstamartImpressions
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
