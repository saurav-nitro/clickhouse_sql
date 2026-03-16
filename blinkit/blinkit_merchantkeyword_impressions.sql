CREATE TABLE blinkit_merchantkeyword_impressions
(
    cdate Date,
    merchantid UInt32,
    keywordid String,
    impressions Float64,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(cdate)
ORDER BY (cdate, keywordid, merchantid);




CREATE MATERIALIZED VIEW blinkit_merchantkeyword_impressions_mv
TO blinkit_merchantkeyword_impressions
AS
WITH last_processed AS (
    SELECT
        coalesce(max(cdate), toDate('2026-01-01')) AS max_date
    FROM blinkit_merchantkeyword_impressions
),

blinkitmerchantkeyword_daily AS
(
    SELECT
        toDate(createdAt) AS cdate,
        merchantId AS merchantid,
        keywordId AS keywordid,
        avg(toFloat64(count)) AS item_count
    FROM blinkitmerchantkeyword
    WHERE toDate(createdAt) > (SELECT max_date FROM last_processed)
    GROUP BY
        cdate,
        merchantid,
        keywordid
),

total_items_per_keyword AS
(
    SELECT
        keywordid,
        cdate,
        sum(item_count) AS total_items
    FROM blinkitmerchantkeyword_daily
    GROUP BY
        keywordid,
        cdate
)

SELECT
    mk.cdate,
    mk.merchantid,
    mk.keywordid,

    if(
        t.total_items = 0,
        0.0,
        any(k.searchcount) / 30.0
        * (mk.item_count / t.total_items)
    ) AS impressions,

    now() AS updated_at

FROM blinkitmerchantkeyword_daily mk

INNER JOIN blinkitkeyword k
    ON mk.keywordid = k.keywordid

INNER JOIN total_items_per_keyword t
    ON mk.keywordid = t.keywordid
    AND mk.cdate = t.cdate

GROUP BY
    mk.cdate,
    mk.merchantid,
    mk.keywordid;