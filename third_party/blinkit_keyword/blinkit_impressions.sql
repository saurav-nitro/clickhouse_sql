-- ============================================================
-- Blinkit_Impressions
-- Per-(cdate, productid, merchantid, keywordid) estimated
-- overall, organic, and ad impressions, weighted by rank factor.
--
-- overall_impressions  = rankfactor(ov_rank)  * impressions
-- organic_impressions  = rankfactor(org_rank) * impressions
-- ad_impressions       = rankfactor(ad_rank)  * impressions
--
-- impressions is the merchant-keyword daily estimated impression
-- share (from BlinkitMerchantKeyword_Impressions).
-- rankfactor is a lookup table mapping integer rank → weight.
--
-- Source: BlinkitKeywordRanking_daily
--           × BlinkitMerchantKeyword_Impressions
--           × RankFactor (LEFT JOIN × 3)
-- Destination: BlinkitImpressions_daily
--
-- Dependencies (must be populated first):
--   BlinkitKeywordRanking_daily            (avgMerge → ov_rank, ad_rank, org_rank)
--   BlinkitMerchantKeyword_Impressions (sumMerge → impressions)
--   RankFactor                               (rank Int, rankFactor Float64)
--
-- To query final values use:
--   SELECT sumMerge(overall_impressions),
--          sumMerge(organic_impressions),
--          sumMerge(ad_impressions)
--   FROM BlinkitImpressions_daily GROUP BY ...
-- ============================================================


-- ============================================================
-- TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS BlinkitImpressions
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

CREATE MATERIALIZED VIEW IF NOT EXISTS BlinkitImpressions_MV
TO BlinkitImpressions
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
        FROM BlinkitKeywordRanking_daily
        GROUP BY cdate, productid, merchantid, keywordid
    ),
    mki AS (
        SELECT
            cdate,
            merchantid,
            keywordid,
            sumMerge(impressions) AS impressions
        FROM BlinkitMerchantKeyword_Impressions
        GROUP BY cdate, merchantid, keywordid
    )
SELECT
    kr.cdate                                                              AS cdate,
    kr.productid                                                          AS productid,
    kr.merchantid                                                         AS merchantid,
    kr.keywordid                                                          AS keywordid,

    anyLast(kr.brandid)                                                   AS brandid,
    anyLast(kr.categoryid)                                                AS categoryid,
    anyLast(kr.subcategoryid)                                             AS subcategoryid,
    anyLast(kr.cityname)                                                  AS cityname,

    sumState(toFloat64(coalesce(rf_ov.rankFactor  * mki.impressions, 0.0))) AS overall_impressions,
    sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions,
    sumState(toFloat64(coalesce(rf_ad.rankFactor  * mki.impressions, 0.0))) AS ad_impressions

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





-- Chunk 7 (2026-02-01)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-01' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-01' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 8 (2026-02-02)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-02' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-02' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 9 (2026-02-03)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-03' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-03' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 10 (2026-02-04)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-04' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-04' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 11 (2026-02-05)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-05' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-05' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 12 (2026-02-06)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-06' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-06' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 13 (2026-02-07)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-07' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-07' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 14 (2026-02-08)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-08' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-08' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 15 (2026-02-09)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-09' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-09' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 16 (2026-02-10)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-10' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-10' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 17 (2026-02-11)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-11' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-11' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 18 (2026-02-12)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-12' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-12' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 19 (2026-02-13)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-13' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-13' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 20 (2026-02-14)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-14' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-14' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 21 (2026-02-15)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-15' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-15' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 22 (2026-02-16)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-16' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-16' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 23 (2026-02-17)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-17' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-17' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 24 (2026-02-18)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-18' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-18' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 25 (2026-02-19)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-19' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-19' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 26 (2026-02-20)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-20' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-20' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 27 (2026-02-21)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-21' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-21' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 28 (2026-02-22)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-22' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-22' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 29 (2026-02-23)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-23' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-23' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 30 (2026-02-24)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-24' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-24' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 31 (2026-02-25)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-25' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-25' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 32 (2026-02-26)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-26' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-26' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 33 (2026-02-27)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-27' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-27' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 34 (2026-02-28)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-02-28' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-02-28' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 35 (2026-03-01)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-03-01' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-03-01' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 36 (2026-03-02)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-03-02' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-03-02' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 37 (2026-03-03)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-03-03' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-03-03' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 38 (2026-03-04)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-03-04' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-03-04' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 39 (2026-03-05)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-03-05' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-03-05' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 40 (2026-03-06)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-03-06' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-03-06' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 41 (2026-03-07)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-03-07' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-03-07' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 42 (2026-03-08)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-03-08' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-03-08' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 43 (2026-03-09)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-03-09' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-03-09' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 44 (2026-03-10)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-03-10' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-03-10' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 45 (2026-03-11)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-03-11' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-03-11' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 46 (2026-03-12)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-03-12' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-03-12' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 47 (2026-03-13)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-03-13' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-03-13' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 48 (2026-03-14)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-03-14' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-03-14' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 49 (2026-03-15)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-03-15' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-03-15' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 50 (2026-03-16)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-03-16' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-03-16' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 51 (2026-03-17)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-03-17' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-03-17' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;

-- Chunk 52 (2026-03-18)
INSERT INTO BlinkitImpressions
WITH kr AS (SELECT cdate, productid, merchantid, keywordid, anyLast(brandid) AS brandid, anyLast(categoryid) AS categoryid, anyLast(subcategoryid) AS subcategoryid, anyLast(cityname) AS cityname, avgMerge(ov_rank) AS ov_rank, avgMerge(ad_rank) AS ad_rank, avgMerge(org_rank) AS org_rank FROM BlinkitKeywordRanking_daily WHERE cdate = '2026-03-18' GROUP BY cdate, productid, merchantid, keywordid), mki AS (SELECT cdate, merchantid, keywordid, sumMerge(impressions) AS impressions FROM BlinkitMerchantKeyword_Impressions WHERE cdate = '2026-03-18' GROUP BY cdate, merchantid, keywordid)
SELECT kr.cdate, kr.productid, kr.merchantid, kr.keywordid, anyLast(kr.brandid) AS brandid, anyLast(kr.categoryid) AS categoryid, anyLast(kr.subcategoryid) AS subcategoryid, anyLast(kr.cityname) AS cityname, sumState(toFloat64(coalesce(rf_ov.rankFactor * mki.impressions, 0.0))) AS overall_impressions, sumState(toFloat64(coalesce(rf_org.rankFactor * mki.impressions, 0.0))) AS organic_impressions, sumState(toFloat64(coalesce(rf_ad.rankFactor * mki.impressions, 0.0))) AS ad_impressions
FROM kr JOIN mki ON kr.keywordid = mki.keywordid AND kr.merchantid = mki.merchantid AND kr.cdate = mki.cdate LEFT JOIN RankFactor AS rf_ov ON toInt64(round(kr.ov_rank)) = rf_ov.rank LEFT JOIN RankFactor AS rf_org ON toInt64(round(kr.org_rank)) = rf_org.rank LEFT JOIN RankFactor AS rf_ad ON toInt64(round(kr.ad_rank)) = rf_ad.rank
GROUP BY kr.cdate, kr.productid, kr.merchantid, kr.keywordid;
