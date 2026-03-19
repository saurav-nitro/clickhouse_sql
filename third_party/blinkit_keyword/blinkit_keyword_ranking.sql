-- ============================================================
-- Blinkit_Keyword_Ranking
-- Per-(cdate, productid, merchantid, keywordid) average
-- overall, ad, and organic keyword ranks.
--
-- ov_rank  = AVG(rank)         across all snapshots in the day
-- ad_rank  = AVG(adRank)       across all snapshots in the day (nullable)
-- org_rank = AVG(organicRank)  across all snapshots in the day (nullable)
--
-- Source: BlinkitKeywordRanking × BlinkitProduct × BlinkitMerchant
-- Destination: BlinkitKeywordRanking_daily
-- Excludes combo products (isCombo = 'false').
--
-- Prisma model: BlinkitKeywordRanking
--   productId   Int    → toString()
--   merchantId  Int    → toString()
--   keywordId   String
--   rank        Int    → avgState(toFloat64())
--   adRank      Int?   → avgState(toFloat64(ifNull(adRank, 0)))
--   organicRank Int?   → avgState(toFloat64(ifNull(organicRank, 0)))
--
-- To query final averages use:
--   SELECT avgMerge(ov_rank), avgMerge(ad_rank), avgMerge(org_rank)
--   FROM BlinkitKeywordRanking_daily GROUP BY ...
-- ============================================================


-- ============================================================
-- TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS BlinkitKeywordRanking_daily
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

CREATE MATERIALIZED VIEW IF NOT EXISTS BlinkitKeywordRanking_daily_MV
TO BlinkitKeywordRanking_daily
AS
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productId)              AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,

    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,

    avgState(toFloat64(kr.rank))                     AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))         AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0)))    AS org_rank

FROM BlinkitKeywordRanking AS kr
JOIN BlinkitProduct  AS p ON kr.productId  = p.id
JOIN BlinkitMerchant AS m ON kr.merchantId = m.id

WHERE p.isCombo = 'false'

GROUP BY
    cdate,
    productid,
    merchantid,
    keywordid;


-- ============================================================
-- BACKFILL INSERT — 5-day chunks
-- Run each block separately, advancing the date range each time.
-- Start from your earliest data date, increment by 5 days.
-- ============================================================

-- Chunk 7
INSERT INTO BlinkitKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productId)              AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM BlinkitKeywordRanking AS kr
JOIN BlinkitProduct  AS p ON kr.productId  = p.id
JOIN BlinkitMerchant AS m ON kr.merchantId = m.id
WHERE p.isCombo = 'false'
  AND toDate(kr.createdAt) BETWEEN '2026-02-01' AND '2026-02-05'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 8
INSERT INTO BlinkitKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productId)              AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM BlinkitKeywordRanking AS kr
JOIN BlinkitProduct  AS p ON kr.productId  = p.id
JOIN BlinkitMerchant AS m ON kr.merchantId = m.id
WHERE p.isCombo = 'false'
  AND toDate(kr.createdAt) BETWEEN '2026-02-06' AND '2026-02-10'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 9
INSERT INTO BlinkitKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productId)              AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM BlinkitKeywordRanking AS kr
JOIN BlinkitProduct  AS p ON kr.productId  = p.id
JOIN BlinkitMerchant AS m ON kr.merchantId = m.id
WHERE p.isCombo = 'false'
  AND toDate(kr.createdAt) BETWEEN '2026-02-11' AND '2026-02-15'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 10
INSERT INTO BlinkitKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productId)              AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM BlinkitKeywordRanking AS kr
JOIN BlinkitProduct  AS p ON kr.productId  = p.id
JOIN BlinkitMerchant AS m ON kr.merchantId = m.id
WHERE p.isCombo = 'false'
  AND toDate(kr.createdAt) BETWEEN '2026-02-16' AND '2026-02-20'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 11
INSERT INTO BlinkitKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productId)              AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM BlinkitKeywordRanking AS kr
JOIN BlinkitProduct  AS p ON kr.productId  = p.id
JOIN BlinkitMerchant AS m ON kr.merchantId = m.id
WHERE p.isCombo = 'false'
  AND toDate(kr.createdAt) BETWEEN '2026-02-21' AND '2026-02-25'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 12
INSERT INTO BlinkitKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productId)              AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM BlinkitKeywordRanking AS kr
JOIN BlinkitProduct  AS p ON kr.productId  = p.id
JOIN BlinkitMerchant AS m ON kr.merchantId = m.id
WHERE p.isCombo = 'false'
  AND toDate(kr.createdAt) BETWEEN '2026-02-26' AND '2026-02-28'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 13
INSERT INTO BlinkitKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productId)              AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM BlinkitKeywordRanking AS kr
JOIN BlinkitProduct  AS p ON kr.productId  = p.id
JOIN BlinkitMerchant AS m ON kr.merchantId = m.id
WHERE p.isCombo = 'false'
  AND toDate(kr.createdAt) BETWEEN '2026-03-01' AND '2026-03-05'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 14
INSERT INTO BlinkitKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productId)              AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM BlinkitKeywordRanking AS kr
JOIN BlinkitProduct  AS p ON kr.productId  = p.id
JOIN BlinkitMerchant AS m ON kr.merchantId = m.id
WHERE p.isCombo = 'false'
  AND toDate(kr.createdAt) BETWEEN '2026-03-06' AND '2026-03-10'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 15
INSERT INTO BlinkitKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productId)              AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM BlinkitKeywordRanking AS kr
JOIN BlinkitProduct  AS p ON kr.productId  = p.id
JOIN BlinkitMerchant AS m ON kr.merchantId = m.id
WHERE p.isCombo = 'false'
  AND toDate(kr.createdAt) BETWEEN '2026-03-11' AND '2026-03-15'
GROUP BY cdate, productid, merchantid, keywordid;

-- Chunk 16
INSERT INTO BlinkitKeywordRanking_daily
SELECT
    toDate(kr.createdAt)                AS cdate,
    toString(kr.productId)              AS productid,
    toString(kr.merchantId)             AS merchantid,
    toString(kr.keywordId)              AS keywordid,
    anyIf(toString(p.brandId),       p.brandId       IS NOT NULL) AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL) AS cityname,
    avgState(toFloat64(kr.rank))                  AS ov_rank,
    avgState(toFloat64(ifNull(kr.adRank, 0)))      AS ad_rank,
    avgState(toFloat64(ifNull(kr.organicRank, 0))) AS org_rank
FROM BlinkitKeywordRanking AS kr
JOIN BlinkitProduct  AS p ON kr.productId  = p.id
JOIN BlinkitMerchant AS m ON kr.merchantId = m.id
WHERE p.isCombo = 'false'
  AND toDate(kr.createdAt) BETWEEN '2026-03-16' AND '2026-03-18'
GROUP BY cdate, productid, merchantid, keywordid;





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
FROM BlinkitKeywordRanking_daily
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
