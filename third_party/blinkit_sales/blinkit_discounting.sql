-- ============================================================
-- Blinkit_Discounting
-- Per-(cdate, productid, cityname) weighted price, discount,
-- and price-per-unit metrics, weighted by each merchant's
-- share of category sales.
--
-- weighted_discount = Σ(discount × merchant_weight) / Σ merchant_weight
-- weighted_ppu      = Σ(price/quantity × merchant_weight) / Σ merchant_weight
-- weighted_price    = Σ(price × merchant_weight) / Σ merchant_weight
--
-- Only rows where inventory > 0 are included (in-stock merchants only).
--
-- Source: BlinkitProductMerchant_daily × Blinkit_Merchant_Weight × UnitWeight
-- ============================================================


-- ============================================================
-- TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS Blinkit_Discounting
(
    cdate                  Date,
    productid              String,
    cityname               String,

    brandid                String,
    categoryid             String,
    subcategoryid          String,
    unit                   String,
    unit_base              String,

    mrp                    Float64,
    city_category_weight   Float64,
    weighted_discount      Float64,
    weighted_ppu           Float64,
    weighted_price         Float64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(cdate)
ORDER BY (cdate, productid, cityname);


-- ============================================================
-- MATERIALIZED VIEW
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS Blinkit_Discounting_MV
TO Blinkit_Discounting
AS
WITH merchant_weights AS (
    -- Resolve AggregatingMergeTree states into plain Float64 values.
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
)
SELECT
    pm.cdate                                                                    AS cdate,
    pm.productid                                                                AS productid,
    pm.cityname                                                                 AS cityname,
    min(pm.brandid)                                                             AS brandid,
    min(pm.categoryid)                                                          AS categoryid,
    min(pm.subcategoryid)                                                       AS subcategoryid,
    min(pm.unit)                                                                AS unit,
    min(uw.baseUnit)                                                            AS unit_base,
    toFloat64(min(pm.mrp))                                                      AS mrp,
    toFloat64(sum(mw.merchant_weight))                                          AS city_category_weight,
    toFloat64(
        coalesce(
            sum(pm.discount * mw.merchant_weight) /
                nullIf(sum(mw.merchant_weight), 0),
            0.0
        )
    )                                                                           AS weighted_discount,
    toFloat64(
        coalesce(
            sum(
                (pm.price / nullIf(uw.quantity, 0)) * mw.merchant_weight
            ) / nullIf(sum(mw.merchant_weight), 0),
            0.0
        )
    )                                                                           AS weighted_ppu,
    toFloat64(
        coalesce(
            sum(pm.price * mw.merchant_weight) /
                nullIf(sum(mw.merchant_weight), 0),
            0.0
        )
    )                                                                           AS weighted_price
FROM BlinkitProductMerchant_daily AS pm
LEFT JOIN UnitWeight AS uw
    ON  uw.unit = pm.unit
INNER JOIN merchant_weights AS mw
    ON  mw.merchantid    = pm.merchantid
    AND mw.categoryid    = pm.categoryid
    AND mw.subcategoryid = pm.subcategoryid
    AND mw.cdate         = pm.cdate
WHERE pm.inventory > 0
GROUP BY
    pm.cdate,
    pm.productid,
    pm.cityname;


-- ============================================================
-- QUERY HELPER
-- ============================================================

SELECT
    cdate,
    productid,
    cityname,
    categoryid,
    subcategoryid,
    brandid,
    unit,
    unit_base,
    mrp,
    city_category_weight,
    weighted_discount,
    weighted_ppu,
    weighted_price
FROM (
    SELECT
        cdate,
        productid,
        cityname,
        min(categoryid)              AS categoryid,
        min(subcategoryid)           AS subcategoryid,
        min(brandid)                 AS brandid,
        min(unit)                    AS unit,
        min(unit_base)               AS unit_base,
        min(mrp)                     AS mrp,
        sum(city_category_weight)    AS city_category_weight,
        sum(weighted_discount)       AS weighted_discount,
        sum(weighted_ppu)            AS weighted_ppu,
        sum(weighted_price)          AS weighted_price
    FROM Blinkit_Discounting
    GROUP BY cdate, productid, cityname
);


-- ============================================================
-- INITIAL BACKFILL INSERT
-- Run ONCE after creating the table and MV to populate
-- Blinkit_Discounting with all historical data.
-- The MV handles new inserts automatically going forward.
-- ============================================================

INSERT INTO Blinkit_Discounting
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
)
SELECT
    pm.cdate                                                                    AS cdate,
    pm.productid                                                                AS productid,
    pm.cityname                                                                 AS cityname,
    min(pm.brandid)                                                             AS brandid,
    min(pm.categoryid)                                                          AS categoryid,
    min(pm.subcategoryid)                                                       AS subcategoryid,
    min(pm.unit)                                                                AS unit,
    min(uw.baseUnit)                                                            AS unit_base,
    toFloat64(min(pm.mrp))                                                      AS mrp,
    toFloat64(sum(mw.merchant_weight))                                          AS city_category_weight,
    toFloat64(
        coalesce(
            sum(pm.discount * mw.merchant_weight) /
                nullIf(sum(mw.merchant_weight), 0),
            0.0
        )
    )                                                                           AS weighted_discount,
    toFloat64(
        coalesce(
            sum(
                (pm.price / nullIf(uw.quantity, 0)) * mw.merchant_weight
            ) / nullIf(sum(mw.merchant_weight), 0),
            0.0
        )
    )                                                                           AS weighted_ppu,
    toFloat64(
        coalesce(
            sum(pm.price * mw.merchant_weight) /
                nullIf(sum(mw.merchant_weight), 0),
            0.0
        )
    )                                                                           AS weighted_price
FROM BlinkitProductMerchant_daily AS pm
LEFT JOIN UnitWeight AS uw
    ON  uw.unit = pm.unit
INNER JOIN merchant_weights AS mw
    ON  mw.merchantid    = pm.merchantid
    AND mw.categoryid    = pm.categoryid
    AND mw.subcategoryid = pm.subcategoryid
    AND mw.cdate         = pm.cdate
WHERE pm.inventory > 0
GROUP BY
    pm.cdate,
    pm.productid,
    pm.cityname;
