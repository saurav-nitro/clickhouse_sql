-- ============================================================
-- Instamart_MerchantKeyword_Impressions
-- Per-(cdate, merchantid, keywordid) estimated impressions,
-- derived from each merchant's share of listings for a keyword
-- scaled by the keyword's monthly search volume.
--
-- impressions = (searchCount / 30.0) * (item_count / total_items_for_keyword_on_day)
--
-- item_count  = AVG(count) per (cdate, merchantid, keywordid)
-- total_items = SUM(item_count) across all merchants for that keyword on that day
--
-- NOTE: Instamart provides its own search counts via InstamartKeyword,
--       joined on keywordid WHERE matchType = 'EXACT'.
--
-- Source: InstamartMerchantKeyword × InstamartKeyword
-- Destination: InstamartMerchantKeyword_Impressions
--
-- To query final impressions use:
--   SELECT sumMerge(impressions)
--   FROM InstamartMerchantKeyword_Impressions GROUP BY ...
-- ============================================================


-- ============================================================
-- TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS InstamartMerchantKeyword_Impressions
(
    cdate       Date,
    merchantid  String,
    keywordid   String,

    impressions AggregateFunction(sum, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(cdate)
ORDER BY (cdate, keywordid, merchantid);


-- ============================================================
-- MATERIALIZED VIEW
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS InstamartMerchantKeyword_Impressions_MV
TO InstamartMerchantKeyword_Impressions
AS
WITH
    instamartmerchantkeyword_daily AS (
        SELECT
            toDate(mk.createdAt)             AS cdate,
            toString(mk.merchantId)          AS merchantid,
            mk.keywordId                     AS keywordid,
            avg(toFloat64(mk.count))         AS item_count
        FROM InstamartMerchantKeyword AS mk
        GROUP BY
            cdate,
            merchantid,
            keywordid
    ),
    total_items_per_keyword AS (
        SELECT
            keywordid,
            cdate,
            sum(item_count) AS total_items
        FROM instamartmerchantkeyword_daily
        GROUP BY
            keywordid,
            cdate
    )
SELECT
    mk.cdate                                                                  AS cdate,
    mk.merchantid                                                             AS merchantid,
    mk.keywordid                                                              AS keywordid,
    sumState(
        toFloat64(
            coalesce(
                (toFloat64(k.searchCount) / 30.0)
                    * (mk.item_count / nullIf(t.total_items, 0)),
                0.0
            )
        )
    )                                                                         AS impressions
FROM instamartmerchantkeyword_daily AS mk
JOIN InstamartKeyword AS k
    ON  mk.keywordid = k.keywordId
    AND k.matchType  = 'EXACT'
JOIN total_items_per_keyword AS t
    ON  mk.keywordid = t.keywordid
    AND mk.cdate     = t.cdate
GROUP BY
    mk.cdate,
    mk.merchantid,
    mk.keywordid;


-- ============================================================
-- BACKFILL INSERT — 15-day chunks (2026-01-01 → 2026-03-19)
-- Run each block separately.
-- ============================================================

-- Chunk 1 (2026-01-01 to 2026-01-15)
INSERT INTO InstamartMerchantKeyword_Impressions
WITH
    instamartmerchantkeyword_daily AS (
        SELECT
            toDate(mk.createdAt)      AS cdate,
            toString(mk.merchantId)   AS merchantid,
            mk.keywordId              AS keywordid,
            avg(toFloat64(mk.count))  AS item_count
        FROM InstamartMerchantKeyword AS mk
        WHERE toDate(mk.createdAt) BETWEEN '2026-01-01' AND '2026-01-15'
        GROUP BY cdate, merchantid, keywordid
    ),
    total_items_per_keyword AS (
        SELECT keywordid, cdate, sum(item_count) AS total_items
        FROM instamartmerchantkeyword_daily
        GROUP BY keywordid, cdate
    )
SELECT
    mk.cdate, mk.merchantid, mk.keywordid,
    sumState(toFloat64(coalesce((toFloat64(k.searchCount) / 30.0) * (mk.item_count / nullIf(t.total_items, 0)), 0.0))) AS impressions
FROM instamartmerchantkeyword_daily AS mk
JOIN InstamartKeyword AS k ON mk.keywordid = k.keywordId AND k.matchType = 'EXACT'
JOIN total_items_per_keyword AS t ON mk.keywordid = t.keywordid AND mk.cdate = t.cdate
GROUP BY mk.cdate, mk.merchantid, mk.keywordid;

-- Chunk 2 (2026-01-16 to 2026-01-31)
INSERT INTO InstamartMerchantKeyword_Impressions
WITH
    instamartmerchantkeyword_daily AS (
        SELECT
            toDate(mk.createdAt)      AS cdate,
            toString(mk.merchantId)   AS merchantid,
            mk.keywordId              AS keywordid,
            avg(toFloat64(mk.count))  AS item_count
        FROM InstamartMerchantKeyword AS mk
        WHERE toDate(mk.createdAt) BETWEEN '2026-01-16' AND '2026-01-31'
        GROUP BY cdate, merchantid, keywordid
    ),
    total_items_per_keyword AS (
        SELECT keywordid, cdate, sum(item_count) AS total_items
        FROM instamartmerchantkeyword_daily
        GROUP BY keywordid, cdate
    )
SELECT
    mk.cdate, mk.merchantid, mk.keywordid,
    sumState(toFloat64(coalesce((toFloat64(k.searchCount) / 30.0) * (mk.item_count / nullIf(t.total_items, 0)), 0.0))) AS impressions
FROM instamartmerchantkeyword_daily AS mk
JOIN InstamartKeyword AS k ON mk.keywordid = k.keywordId AND k.matchType = 'EXACT'
JOIN total_items_per_keyword AS t ON mk.keywordid = t.keywordid AND mk.cdate = t.cdate
GROUP BY mk.cdate, mk.merchantid, mk.keywordid;

-- Chunk 3 (2026-02-01 to 2026-02-15)
INSERT INTO InstamartMerchantKeyword_Impressions
WITH
    instamartmerchantkeyword_daily AS (
        SELECT
            toDate(mk.createdAt)      AS cdate,
            toString(mk.merchantId)   AS merchantid,
            mk.keywordId              AS keywordid,
            avg(toFloat64(mk.count))  AS item_count
        FROM InstamartMerchantKeyword AS mk
        WHERE toDate(mk.createdAt) BETWEEN '2026-02-01' AND '2026-02-15'
        GROUP BY cdate, merchantid, keywordid
    ),
    total_items_per_keyword AS (
        SELECT keywordid, cdate, sum(item_count) AS total_items
        FROM instamartmerchantkeyword_daily
        GROUP BY keywordid, cdate
    )
SELECT
    mk.cdate, mk.merchantid, mk.keywordid,
    sumState(toFloat64(coalesce((toFloat64(k.searchCount) / 30.0) * (mk.item_count / nullIf(t.total_items, 0)), 0.0))) AS impressions
FROM instamartmerchantkeyword_daily AS mk
JOIN InstamartKeyword AS k ON mk.keywordid = k.keywordId AND k.matchType = 'EXACT'
JOIN total_items_per_keyword AS t ON mk.keywordid = t.keywordid AND mk.cdate = t.cdate
GROUP BY mk.cdate, mk.merchantid, mk.keywordid;

-- Chunk 4 (2026-02-16 to 2026-02-28)
INSERT INTO InstamartMerchantKeyword_Impressions
WITH
    instamartmerchantkeyword_daily AS (
        SELECT
            toDate(mk.createdAt)      AS cdate,
            toString(mk.merchantId)   AS merchantid,
            mk.keywordId              AS keywordid,
            avg(toFloat64(mk.count))  AS item_count
        FROM InstamartMerchantKeyword AS mk
        WHERE toDate(mk.createdAt) BETWEEN '2026-02-16' AND '2026-02-28'
        GROUP BY cdate, merchantid, keywordid
    ),
    total_items_per_keyword AS (
        SELECT keywordid, cdate, sum(item_count) AS total_items
        FROM instamartmerchantkeyword_daily
        GROUP BY keywordid, cdate
    )
SELECT
    mk.cdate, mk.merchantid, mk.keywordid,
    sumState(toFloat64(coalesce((toFloat64(k.searchCount) / 30.0) * (mk.item_count / nullIf(t.total_items, 0)), 0.0))) AS impressions
FROM instamartmerchantkeyword_daily AS mk
JOIN InstamartKeyword AS k ON mk.keywordid = k.keywordId AND k.matchType = 'EXACT'
JOIN total_items_per_keyword AS t ON mk.keywordid = t.keywordid AND mk.cdate = t.cdate
GROUP BY mk.cdate, mk.merchantid, mk.keywordid;

-- Chunk 5 (2026-03-01 to 2026-03-15)
INSERT INTO InstamartMerchantKeyword_Impressions
WITH
    instamartmerchantkeyword_daily AS (
        SELECT
            toDate(mk.createdAt)      AS cdate,
            toString(mk.merchantId)   AS merchantid,
            mk.keywordId              AS keywordid,
            avg(toFloat64(mk.count))  AS item_count
        FROM InstamartMerchantKeyword AS mk
        WHERE toDate(mk.createdAt) BETWEEN '2026-03-01' AND '2026-03-15'
        GROUP BY cdate, merchantid, keywordid
    ),
    total_items_per_keyword AS (
        SELECT keywordid, cdate, sum(item_count) AS total_items
        FROM instamartmerchantkeyword_daily
        GROUP BY keywordid, cdate
    )
SELECT
    mk.cdate, mk.merchantid, mk.keywordid,
    sumState(toFloat64(coalesce((toFloat64(k.searchCount) / 30.0) * (mk.item_count / nullIf(t.total_items, 0)), 0.0))) AS impressions
FROM instamartmerchantkeyword_daily AS mk
JOIN InstamartKeyword AS k ON mk.keywordid = k.keywordId AND k.matchType = 'EXACT'
JOIN total_items_per_keyword AS t ON mk.keywordid = t.keywordid AND mk.cdate = t.cdate
GROUP BY mk.cdate, mk.merchantid, mk.keywordid;

-- Chunk 6 (2026-03-16 to 2026-03-19)
INSERT INTO InstamartMerchantKeyword_Impressions
WITH
    instamartmerchantkeyword_daily AS (
        SELECT
            toDate(mk.createdAt)      AS cdate,
            toString(mk.merchantId)   AS merchantid,
            mk.keywordId              AS keywordid,
            avg(toFloat64(mk.count))  AS item_count
        FROM InstamartMerchantKeyword AS mk
        WHERE toDate(mk.createdAt) BETWEEN '2026-03-16' AND '2026-03-19'
        GROUP BY cdate, merchantid, keywordid
    ),
    total_items_per_keyword AS (
        SELECT keywordid, cdate, sum(item_count) AS total_items
        FROM instamartmerchantkeyword_daily
        GROUP BY keywordid, cdate
    )
SELECT
    mk.cdate, mk.merchantid, mk.keywordid,
    sumState(toFloat64(coalesce((toFloat64(k.searchCount) / 30.0) * (mk.item_count / nullIf(t.total_items, 0)), 0.0))) AS impressions
FROM instamartmerchantkeyword_daily AS mk
JOIN InstamartKeyword AS k ON mk.keywordid = k.keywordId AND k.matchType = 'EXACT'
JOIN total_items_per_keyword AS t ON mk.keywordid = t.keywordid AND mk.cdate = t.cdate
GROUP BY mk.cdate, mk.merchantid, mk.keywordid;


-- ============================================================
-- QUERY HELPER — read final merged impressions
-- ============================================================

SELECT
    cdate,
    merchantid,
    keywordid,
    sumMerge(impressions) AS impressions
FROM InstamartMerchantKeyword_Impressions
GROUP BY
    cdate,
    merchantid,
    keywordid
ORDER BY cdate DESC;
