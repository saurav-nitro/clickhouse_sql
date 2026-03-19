-- ============================================================
-- Zepto_MerchantKeyword_Impressions
-- Per-(cdate, merchantid, keywordid) estimated impressions,
-- derived from each merchant's share of listings for a keyword
-- scaled by the keyword's monthly search volume.
--
-- impressions = (searchcount / 30.0) * (item_count / total_items_for_keyword_on_day)
--
-- item_count  = AVG(count) per (cdate, merchantid, keywordid)
-- total_items = SUM(item_count) across all merchants for that keyword on that day
--
-- NOTE: Zepto does not provide its own search counts, so searchCount
--       is sourced from BlinkitKeyword (joined on keywordid).
--
-- Source: ZeptoMerchantKeyword × BlinkitKeyword
-- Destination: ZeptoMerchantKeyword_Impressions
--
-- Prisma model: ZeptoMerchantKeyword
--   merchantId  Int    → toString()
--   keywordId   String
--   count       Int    → avg(toFloat64())
--
-- Prisma model: BlinkitKeyword
--   keywordId   String
--   searchCount Int
--
-- To query final impressions use:
--   SELECT sumMerge(impressions)
--   FROM ZeptoMerchantKeyword_Impressions GROUP BY ...
-- ============================================================


-- ============================================================
-- TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS ZeptoMerchantKeyword_Impressions
(
    cdate      Date,
    merchantid String,
    keywordid  String,

    impressions AggregateFunction(sum, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(cdate)
ORDER BY (cdate, keywordid, merchantid);


-- ============================================================
-- MATERIALIZED VIEW
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS ZeptoMerchantKeyword_Impressions_MV
TO ZeptoMerchantKeyword_Impressions
AS
WITH
    zeptomerchantkeyword_daily AS (
        SELECT
            toDate(mk.createdAt)                 AS cdate,
            toString(mk.merchantId)              AS merchantid,
            mk.keywordId                         AS keywordid,
            avg(toFloat64(mk.count))             AS item_count
        FROM ZeptoMerchantKeyword AS mk
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
        FROM zeptomerchantkeyword_daily
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
FROM zeptomerchantkeyword_daily AS mk
-- using BlinkitKeyword because Zepto doesn't provide search counts
JOIN BlinkitKeyword AS k ON mk.keywordid = k.keywordId
JOIN total_items_per_keyword AS t
    ON  mk.keywordid = t.keywordid
    AND mk.cdate     = t.cdate
GROUP BY
    mk.cdate,
    mk.merchantid,
    mk.keywordid;


-- ============================================================
-- BACKFILL INSERT — 15-day chunks
-- Run each block separately, advancing the date range each time.
-- Start from your earliest data date, increment by 15 days.
-- ============================================================


INSERT INTO ZeptoMerchantKeyword_Impressions
WITH
    zeptomerchantkeyword_daily AS (
        SELECT
            toDate(mk.createdAt)      AS cdate,
            toString(mk.merchantId)   AS merchantid,
            mk.keywordId              AS keywordid,
            avg(toFloat64(mk.count))  AS item_count
        FROM ZeptoMerchantKeyword AS mk
        WHERE toDate(mk.createdAt) BETWEEN '2026-01-01' AND '2026-01-15'
        GROUP BY cdate, merchantid, keywordid
    ),
    total_items_per_keyword AS (
        SELECT keywordid, cdate, sum(item_count) AS total_items
        FROM zeptomerchantkeyword_daily
        GROUP BY keywordid, cdate
    )
SELECT
    mk.cdate, mk.merchantid, mk.keywordid,
    sumState(toFloat64(coalesce((toFloat64(k.searchCount) / 30.0) * (mk.item_count / nullIf(t.total_items, 0)), 0.0))) AS impressions
FROM zeptomerchantkeyword_daily AS mk
JOIN BlinkitKeyword AS k ON mk.keywordid = k.keywordId
JOIN total_items_per_keyword AS t ON mk.keywordid = t.keywordid AND mk.cdate = t.cdate
GROUP BY mk.cdate, mk.merchantid, mk.keywordid;

-- Chunk 26
INSERT INTO ZeptoMerchantKeyword_Impressions
WITH
    zeptomerchantkeyword_daily AS (
        SELECT
            toDate(mk.createdAt)      AS cdate,
            toString(mk.merchantId)   AS merchantid,
            mk.keywordId              AS keywordid,
            avg(toFloat64(mk.count))  AS item_count
        FROM ZeptoMerchantKeyword AS mk
        WHERE toDate(mk.createdAt) BETWEEN '2026-01-16' AND '2026-01-31'
        GROUP BY cdate, merchantid, keywordid
    ),
    total_items_per_keyword AS (
        SELECT keywordid, cdate, sum(item_count) AS total_items
        FROM zeptomerchantkeyword_daily
        GROUP BY keywordid, cdate
    )
SELECT
    mk.cdate, mk.merchantid, mk.keywordid,
    sumState(toFloat64(coalesce((toFloat64(k.searchCount) / 30.0) * (mk.item_count / nullIf(t.total_items, 0)), 0.0))) AS impressions
FROM zeptomerchantkeyword_daily AS mk
JOIN BlinkitKeyword AS k ON mk.keywordid = k.keywordId
JOIN total_items_per_keyword AS t ON mk.keywordid = t.keywordid AND mk.cdate = t.cdate
GROUP BY mk.cdate, mk.merchantid, mk.keywordid;

-- Chunk 27
INSERT INTO ZeptoMerchantKeyword_Impressions
WITH
    zeptomerchantkeyword_daily AS (
        SELECT
            toDate(mk.createdAt)      AS cdate,
            toString(mk.merchantId)   AS merchantid,
            mk.keywordId              AS keywordid,
            avg(toFloat64(mk.count))  AS item_count
        FROM ZeptoMerchantKeyword AS mk
        WHERE toDate(mk.createdAt) BETWEEN '2026-02-01' AND '2026-02-15'
        GROUP BY cdate, merchantid, keywordid
    ),
    total_items_per_keyword AS (
        SELECT keywordid, cdate, sum(item_count) AS total_items
        FROM zeptomerchantkeyword_daily
        GROUP BY keywordid, cdate
    )
SELECT
    mk.cdate, mk.merchantid, mk.keywordid,
    sumState(toFloat64(coalesce((toFloat64(k.searchCount) / 30.0) * (mk.item_count / nullIf(t.total_items, 0)), 0.0))) AS impressions
FROM zeptomerchantkeyword_daily AS mk
JOIN BlinkitKeyword AS k ON mk.keywordid = k.keywordId
JOIN total_items_per_keyword AS t ON mk.keywordid = t.keywordid AND mk.cdate = t.cdate
GROUP BY mk.cdate, mk.merchantid, mk.keywordid;

-- Chunk 28
INSERT INTO ZeptoMerchantKeyword_Impressions
WITH
    zeptomerchantkeyword_daily AS (
        SELECT
            toDate(mk.createdAt)      AS cdate,
            toString(mk.merchantId)   AS merchantid,
            mk.keywordId              AS keywordid,
            avg(toFloat64(mk.count))  AS item_count
        FROM ZeptoMerchantKeyword AS mk
        WHERE toDate(mk.createdAt) BETWEEN '2026-02-16' AND '2026-02-28'
        GROUP BY cdate, merchantid, keywordid
    ),
    total_items_per_keyword AS (
        SELECT keywordid, cdate, sum(item_count) AS total_items
        FROM zeptomerchantkeyword_daily
        GROUP BY keywordid, cdate
    )
SELECT
    mk.cdate, mk.merchantid, mk.keywordid,
    sumState(toFloat64(coalesce((toFloat64(k.searchCount) / 30.0) * (mk.item_count / nullIf(t.total_items, 0)), 0.0))) AS impressions
FROM zeptomerchantkeyword_daily AS mk
JOIN BlinkitKeyword AS k ON mk.keywordid = k.keywordId
JOIN total_items_per_keyword AS t ON mk.keywordid = t.keywordid AND mk.cdate = t.cdate
GROUP BY mk.cdate, mk.merchantid, mk.keywordid;

-- Chunk 29
INSERT INTO ZeptoMerchantKeyword_Impressions
WITH
    zeptomerchantkeyword_daily AS (
        SELECT
            toDate(mk.createdAt)      AS cdate,
            toString(mk.merchantId)   AS merchantid,
            mk.keywordId              AS keywordid,
            avg(toFloat64(mk.count))  AS item_count
        FROM ZeptoMerchantKeyword AS mk
        WHERE toDate(mk.createdAt) BETWEEN '2026-03-01' AND '2026-03-15'
        GROUP BY cdate, merchantid, keywordid
    ),
    total_items_per_keyword AS (
        SELECT keywordid, cdate, sum(item_count) AS total_items
        FROM zeptomerchantkeyword_daily
        GROUP BY keywordid, cdate
    )
SELECT
    mk.cdate, mk.merchantid, mk.keywordid,
    sumState(toFloat64(coalesce((toFloat64(k.searchCount) / 30.0) * (mk.item_count / nullIf(t.total_items, 0)), 0.0))) AS impressions
FROM zeptomerchantkeyword_daily AS mk
JOIN BlinkitKeyword AS k ON mk.keywordid = k.keywordId
JOIN total_items_per_keyword AS t ON mk.keywordid = t.keywordid AND mk.cdate = t.cdate
GROUP BY mk.cdate, mk.merchantid, mk.keywordid;

-- Chunk 30
INSERT INTO ZeptoMerchantKeyword_Impressions
WITH
    zeptomerchantkeyword_daily AS (
        SELECT
            toDate(mk.createdAt)      AS cdate,
            toString(mk.merchantId)   AS merchantid,
            mk.keywordId              AS keywordid,
            avg(toFloat64(mk.count))  AS item_count
        FROM ZeptoMerchantKeyword AS mk
        WHERE toDate(mk.createdAt) BETWEEN '2026-03-16' AND '2026-03-19'
        GROUP BY cdate, merchantid, keywordid
    ),
    total_items_per_keyword AS (
        SELECT keywordid, cdate, sum(item_count) AS total_items
        FROM zeptomerchantkeyword_daily
        GROUP BY keywordid, cdate
    )
SELECT
    mk.cdate, mk.merchantid, mk.keywordid,
    sumState(toFloat64(coalesce((toFloat64(k.searchCount) / 30.0) * (mk.item_count / nullIf(t.total_items, 0)), 0.0))) AS impressions
FROM zeptomerchantkeyword_daily AS mk
JOIN BlinkitKeyword AS k ON mk.keywordid = k.keywordId
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
FROM ZeptoMerchantKeyword_Impressions
GROUP BY
    cdate,
    merchantid,
    keywordid
ORDER BY cdate DESC;
