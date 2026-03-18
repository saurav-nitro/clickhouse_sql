-- ============================================================
-- Blinkit_Availability
-- Per-(cdate, productid, brandid, cityname) listing &
-- availability metrics, weighted by each merchant's share of
-- category sales.
--
-- lst  = 100 * Σ merchant_weight              / city_category_weight
-- avl  = 100 * Σ (sign(inventory) * merchant_weight) / city_category_weight
--
-- Source: BlinkitProductMerchant_daily  ×  Blinkit_Merchant_Weight
-- ============================================================

CREATE TABLE IF NOT EXISTS Blinkit_Availability
(
    cdate                  Date,
    productid              String,
    brandid                String,
    cityname               String,

    categoryid             String,
    subcategoryid          String,

    city_category_weight   Float64,
    lst_count              Float64,
    avl_count              Float64,
    lst                    Float64,
    avl                    Float64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(cdate)
ORDER BY (cdate, productid, brandid, cityname);




CREATE MATERIALIZED VIEW IF NOT EXISTS Blinkit_Availability_MV
TO Blinkit_Availability
AS
WITH merchant_weights AS (
    -- Resolve AggregatingMergeTree states into plain Float64 values first.
    SELECT
        merchantid,
        categoryid,
        subcategoryid,
        cdate,
        coalesce(
            sumMerge(merchant_daily_sales) /
                nullIf(sumMerge(category_daily_sales), 0),
            0.0
        ) AS merchant_weight
    FROM Blinkit_Merchant_Weight
    GROUP BY
        merchantid,
        categoryid,
        subcategoryid,
        cdate
),
city_weights AS (
    -- Sum merchant weights per city by joining pm (which carries cityname) with merchant_weights.
    SELECT
        p.cityname,
        p.categoryid,
        p.subcategoryid,
        p.cdate,
        sum(mw.merchant_weight) AS city_category_weight
    FROM BlinkitProductMerchant_daily AS p
    LEFT JOIN merchant_weights AS mw
        ON  mw.merchantid    = p.merchantid
        AND mw.categoryid    = p.categoryid
        AND mw.subcategoryid = p.subcategoryid
        AND mw.cdate         = p.cdate
    GROUP BY
        p.cityname,
        p.categoryid,
        p.subcategoryid,
        p.cdate
)
SELECT
    pm.cdate                                                        AS cdate,
    pm.productid                                                    AS productid,
    pm.brandid                                                      AS brandid,
    pm.cityname                                                     AS cityname,
    min(pm.categoryid)                                              AS categoryid,
    min(pm.subcategoryid)                                           AS subcategoryid,
    toFloat64(any(cw.city_category_weight))                         AS city_category_weight,
    toFloat64(count())                                              AS lst_count,
    toFloat64(sum(sign(pm.inventory)))                              AS avl_count,
    toFloat64(sum(mw.merchant_weight))                              AS lst,
    toFloat64(sum(sign(pm.inventory) * mw.merchant_weight))         AS avl
FROM BlinkitProductMerchant_daily AS pm
LEFT JOIN merchant_weights AS mw
    ON  mw.merchantid    = pm.merchantid
    AND mw.categoryid    = pm.categoryid
    AND mw.subcategoryid = pm.subcategoryid
    AND mw.cdate         = pm.cdate
LEFT JOIN city_weights AS cw
    ON  cw.cityname      = pm.cityname
    AND cw.categoryid    = pm.categoryid
    AND cw.subcategoryid = pm.subcategoryid
    AND cw.cdate         = pm.cdate
GROUP BY
    pm.cdate,
    pm.productid,
    pm.brandid,
    pm.cityname;




-- ============================================================
-- QUERY HELPER
-- Read final values; SummingMergeTree auto-sums Float64 columns
-- on merge, so a GROUP BY + sum() returns the correct totals.
-- ============================================================

SELECT
    cdate,
    productid,
    brandid,
    cityname,
    categoryid,
    subcategoryid,
    city_category_weight,
    lst_count,
    avl_count,
    coalesce(100.0 * lst / nullIf(city_category_weight, 0), 0.0)     AS lst,
    coalesce(100.0 * avl / nullIf(city_category_weight, 0), 0.0)     AS avl
FROM (
    SELECT
        cdate,
        productid,
        brandid,
        cityname,
        min(categoryid)              AS categoryid,
        min(subcategoryid)           AS subcategoryid,
        sum(city_category_weight)    AS city_category_weight,
        sum(lst_count)               AS lst_count,
        sum(avl_count)               AS avl_count,
        sum(lst)                     AS lst,
        sum(avl)                     AS avl
    FROM Blinkit_Availability
    GROUP BY cdate, productid, brandid, cityname
);




-- ============================================================
-- INITIAL BACKFILL INSERT
-- Run ONCE after creating the table and MV to populate
-- Blinkit_Availability with all historical data from
-- BlinkitProductMerchant_daily. The MV will handle new
-- inserts going forward automatically.
-- ============================================================

INSERT INTO Blinkit_Availability
WITH merchant_weights AS (
    SELECT
        merchantid,
        categoryid,
        subcategoryid,
        cdate,
        coalesce(
            sumMerge(merchant_daily_sales) /
                nullIf(sumMerge(category_daily_sales), 0),
            0.0
        ) AS merchant_weight
    FROM Blinkit_Merchant_Weight
    GROUP BY
        merchantid,
        categoryid,
        subcategoryid,
        cdate
),
city_weights AS (
    SELECT
        p.cityname,
        p.categoryid,
        p.subcategoryid,
        p.cdate,
        sum(mw.merchant_weight) AS city_category_weight
    FROM BlinkitProductMerchant_daily AS p
    LEFT JOIN merchant_weights AS mw
        ON  mw.merchantid    = p.merchantid
        AND mw.categoryid    = p.categoryid
        AND mw.subcategoryid = p.subcategoryid
        AND mw.cdate         = p.cdate
    GROUP BY
        p.cityname,
        p.categoryid,
        p.subcategoryid,
        p.cdate
)
SELECT
    pm.cdate                                                        AS cdate,
    pm.productid                                                    AS productid,
    pm.brandid                                                      AS brandid,
    pm.cityname                                                     AS cityname,
    min(pm.categoryid)                                              AS categoryid,
    min(pm.subcategoryid)                                           AS subcategoryid,
    toFloat64(any(cw.city_category_weight))                         AS city_category_weight,
    toFloat64(count())                                              AS lst_count,
    toFloat64(sum(sign(pm.inventory)))                              AS avl_count,
    toFloat64(sum(mw.merchant_weight))                              AS lst,
    toFloat64(sum(sign(pm.inventory) * mw.merchant_weight))         AS avl
FROM BlinkitProductMerchant_daily AS pm
LEFT JOIN merchant_weights AS mw
    ON  mw.merchantid    = pm.merchantid
    AND mw.categoryid    = pm.categoryid
    AND mw.subcategoryid = pm.subcategoryid
    AND mw.cdate         = pm.cdate
LEFT JOIN city_weights AS cw
    ON  cw.cityname      = pm.cityname
    AND cw.categoryid    = pm.categoryid
    AND cw.subcategoryid = pm.subcategoryid
    AND cw.cdate         = pm.cdate
GROUP BY
    pm.cdate,
    pm.productid,
    pm.brandid,
    pm.cityname;
