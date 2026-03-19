-- ============================================================
-- Instamart_Keyword_Ranking
-- Per-(cdate, productid, merchantid, keywordid) average
-- overall, ad, and organic keyword ranks.
--
-- ov_rank  = AVG(rank)         across all snapshots in the day
-- ad_rank  = AVG(adRank)       across all snapshots in the day (nullable)
-- org_rank = AVG(organicRank)  across all snapshots in the day (nullable)
--
-- Source: InstamartKeywordRanking × InstamartProduct × InstamartMerchant
-- Destination: InstamartKeywordRanking_daily
--
-- Redshift join key: productgroupid = p.groupId (not p.id)
--
-- To query final averages use:
--   SELECT avgMerge(ov_rank), avgMerge(ad_rank), avgMerge(org_rank)
--   FROM InstamartKeywordRanking_daily GROUP BY ...
-- ============================================================


-- ============================================================
-- TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS InstamartKeywordRanking_daily
(
    cdate         Date,
    productid     String,
    merchantid    String,
    keywordid     String,

    brandid       String,
    categoryid    String,
    subcategoryid String,
    cityname      String,

    ov_rank       AggregateFunction(avg, Float64),
    ad_rank       AggregateFunction(avg, Float64),
    org_rank      AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(cdate)
ORDER BY (cdate, keywordid, merchantid, productid);


-- ============================================================
-- MATERIALIZED VIEW
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS InstamartKeywordRanking_daily_MV
TO InstamartKeywordRanking_daily
AS
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productGroupId)         AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,

    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,

    avgState(toFloat64(kr.rank))                     AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))         AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0)))    AS org_rank

FROM InstamartKeywordRanking AS kr
JOIN InstamartProduct  AS p ON kr.productGroupId = p.groupId
JOIN InstamartMerchant AS m ON kr.merchantId     = m.id

GROUP BY
    cdate,
    productid,
    merchantid,
    keywordid;


-- ============================================================
-- BACKFILL INSERT — 5-day chunks (2026-01-01 → 2026-03-19)
-- Run each block separately.
-- ============================================================

-- Chunk 1 (2026-01-01 to 2026-01-05)
INSERT INTO InstamartKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productGroupId)         AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM InstamartKeywordRanking AS kr
JOIN InstamartProduct  AS p ON kr.productGroupId = p.groupId
JOIN InstamartMerchant AS m ON kr.merchantId     = m.id
WHERE toDate(kr.createdAt) BETWEEN '2026-01-01' AND '2026-01-05'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 2 (2026-01-06 to 2026-01-10)
INSERT INTO InstamartKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productGroupId)         AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM InstamartKeywordRanking AS kr
JOIN InstamartProduct  AS p ON kr.productGroupId = p.groupId
JOIN InstamartMerchant AS m ON kr.merchantId     = m.id
WHERE toDate(kr.createdAt) BETWEEN '2026-01-06' AND '2026-01-10'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 3 (2026-01-11 to 2026-01-15)
INSERT INTO InstamartKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productGroupId)         AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM InstamartKeywordRanking AS kr
JOIN InstamartProduct  AS p ON kr.productGroupId = p.groupId
JOIN InstamartMerchant AS m ON kr.merchantId     = m.id
WHERE toDate(kr.createdAt) BETWEEN '2026-01-11' AND '2026-01-15'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 4 (2026-01-16 to 2026-01-20)
INSERT INTO InstamartKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productGroupId)         AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM InstamartKeywordRanking AS kr
JOIN InstamartProduct  AS p ON kr.productGroupId = p.groupId
JOIN InstamartMerchant AS m ON kr.merchantId     = m.id
WHERE toDate(kr.createdAt) BETWEEN '2026-01-16' AND '2026-01-20'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 5 (2026-01-21 to 2026-01-25)
INSERT INTO InstamartKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productGroupId)         AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM InstamartKeywordRanking AS kr
JOIN InstamartProduct  AS p ON kr.productGroupId = p.groupId
JOIN InstamartMerchant AS m ON kr.merchantId     = m.id
WHERE toDate(kr.createdAt) BETWEEN '2026-01-21' AND '2026-01-25'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 6 (2026-01-26 to 2026-01-31)
INSERT INTO InstamartKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productGroupId)         AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM InstamartKeywordRanking AS kr
JOIN InstamartProduct  AS p ON kr.productGroupId = p.groupId
JOIN InstamartMerchant AS m ON kr.merchantId     = m.id
WHERE toDate(kr.createdAt) BETWEEN '2026-01-26' AND '2026-01-31'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 7 (2026-02-01 to 2026-02-05)
INSERT INTO InstamartKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productGroupId)         AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM InstamartKeywordRanking AS kr
JOIN InstamartProduct  AS p ON kr.productGroupId = p.groupId
JOIN InstamartMerchant AS m ON kr.merchantId     = m.id
WHERE toDate(kr.createdAt) BETWEEN '2026-02-01' AND '2026-02-05'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 8 (2026-02-06 to 2026-02-10)
INSERT INTO InstamartKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productGroupId)         AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM InstamartKeywordRanking AS kr
JOIN InstamartProduct  AS p ON kr.productGroupId = p.groupId
JOIN InstamartMerchant AS m ON kr.merchantId     = m.id
WHERE toDate(kr.createdAt) BETWEEN '2026-02-06' AND '2026-02-10'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 9 (2026-02-11 to 2026-02-15)
INSERT INTO InstamartKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productGroupId)         AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM InstamartKeywordRanking AS kr
JOIN InstamartProduct  AS p ON kr.productGroupId = p.groupId
JOIN InstamartMerchant AS m ON kr.merchantId     = m.id
WHERE toDate(kr.createdAt) BETWEEN '2026-02-11' AND '2026-02-15'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 10 (2026-02-16 to 2026-02-20)
INSERT INTO InstamartKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productGroupId)         AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM InstamartKeywordRanking AS kr
JOIN InstamartProduct  AS p ON kr.productGroupId = p.groupId
JOIN InstamartMerchant AS m ON kr.merchantId     = m.id
WHERE toDate(kr.createdAt) BETWEEN '2026-02-16' AND '2026-02-20'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 11 (2026-02-21 to 2026-02-25)
INSERT INTO InstamartKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productGroupId)         AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM InstamartKeywordRanking AS kr
JOIN InstamartProduct  AS p ON kr.productGroupId = p.groupId
JOIN InstamartMerchant AS m ON kr.merchantId     = m.id
WHERE toDate(kr.createdAt) BETWEEN '2026-02-21' AND '2026-02-25'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 12 (2026-02-26 to 2026-02-28)
INSERT INTO InstamartKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productGroupId)         AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM InstamartKeywordRanking AS kr
JOIN InstamartProduct  AS p ON kr.productGroupId = p.groupId
JOIN InstamartMerchant AS m ON kr.merchantId     = m.id
WHERE toDate(kr.createdAt) BETWEEN '2026-02-26' AND '2026-02-28'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 13 (2026-03-01 to 2026-03-05)
INSERT INTO InstamartKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productGroupId)         AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM InstamartKeywordRanking AS kr
JOIN InstamartProduct  AS p ON kr.productGroupId = p.groupId
JOIN InstamartMerchant AS m ON kr.merchantId     = m.id
WHERE toDate(kr.createdAt) BETWEEN '2026-03-01' AND '2026-03-05'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 14 (2026-03-06 to 2026-03-10)
INSERT INTO InstamartKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productGroupId)         AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM InstamartKeywordRanking AS kr
JOIN InstamartProduct  AS p ON kr.productGroupId = p.groupId
JOIN InstamartMerchant AS m ON kr.merchantId     = m.id
WHERE toDate(kr.createdAt) BETWEEN '2026-03-06' AND '2026-03-10'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 15 (2026-03-11 to 2026-03-15)
INSERT INTO InstamartKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productGroupId)         AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM InstamartKeywordRanking AS kr
JOIN InstamartProduct  AS p ON kr.productGroupId = p.groupId
JOIN InstamartMerchant AS m ON kr.merchantId     = m.id
WHERE toDate(kr.createdAt) BETWEEN '2026-03-11' AND '2026-03-15'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 16 (2026-03-16 to 2026-03-19)
INSERT INTO InstamartKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productGroupId)         AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM InstamartKeywordRanking AS kr
JOIN InstamartProduct  AS p ON kr.productGroupId = p.groupId
JOIN InstamartMerchant AS m ON kr.merchantId     = m.id
WHERE toDate(kr.createdAt) BETWEEN '2026-03-16' AND '2026-03-19'
GROUP BY cdate, productid, merchantid, keywordid;


-- ============================================================
-- QUERY — read final merged averages
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
    avgMerge(ov_rank)  AS ov_rank,
    avgMerge(ad_rank)  AS ad_rank,
    avgMerge(org_rank) AS org_rank
FROM InstamartKeywordRanking_daily
where productid='185Y8XJWL4'
GROUP BY
    cdate,
    productid,
    merchantid,
    keywordid,
    brandid,
    categoryid,
    subcategoryid,
    cityname
ORDER BY cdate DESC limit 100;
