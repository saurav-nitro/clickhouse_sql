-- ============================================================
-- Blinkit_Pricing_Band
-- Per-(cdate, keywordid, productid, cityname) pricing metrics
-- joined from BlinkitKeywordRanking_daily × Blinkit_Discounting.
--
-- mrp                 = MIN(mrp)                from Blinkit_Discounting
-- city_category_weight= MIN(city_category_weight)
-- weighted_price      = MIN(weighted_price)
-- weighted_discount   = MIN(weighted_discount)
--
-- Source: BlinkitKeywordRanking_daily × Blinkit_Discounting
-- Destination: Blinkit_Pricing_Band
-- ============================================================


-- ============================================================
-- TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS Blinkit_Pricing_Band
(
    cdate                  Date,
    keywordid              String,
    productid              String,
    cityname               String,

    brandid                String,
    categoryid             String,
    subcategoryid          String,

    mrp                    Float64,
    city_category_weight   Float64,
    weighted_price         Float64,
    weighted_discount      Float64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(cdate)
ORDER BY (cdate, keywordid, productid, cityname);


-- ============================================================
-- MATERIALIZED VIEW
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS Blinkit_Pricing_Band_MV
TO Blinkit_Pricing_Band
AS
SELECT
    kr.cdate                                                        AS cdate,
    kr.keywordid                                                    AS keywordid,
    kr.productid                                                    AS productid,
    kr.cityname                                                     AS cityname,

    min(kr.brandid)                                                 AS brandid,
    min(kr.categoryid)                                              AS categoryid,
    min(kr.subcategoryid)                                           AS subcategoryid,

    toFloat64(min(d.mrp))                                           AS mrp,
    toFloat64(min(d.city_category_weight))                          AS city_category_weight,
    toFloat64(min(d.weighted_price))                                AS weighted_price,
    toFloat64(min(d.weighted_discount))                             AS weighted_discount
FROM
(
    SELECT
        cdate,
        keywordid,
        productid,
        cityname,
        min(brandid)       AS brandid,
        min(categoryid)    AS categoryid,
        min(subcategoryid) AS subcategoryid
    FROM BlinkitKeywordRanking_daily
    GROUP BY cdate, keywordid, productid, cityname
) AS kr
JOIN
(
    SELECT
        cdate,
        productid,
        cityname,
        min(mrp)                  AS mrp,
        sum(city_category_weight) AS city_category_weight,
        sum(weighted_price)       AS weighted_price,
        sum(weighted_discount)    AS weighted_discount
    FROM Blinkit_Discounting
    GROUP BY cdate, productid, cityname
) AS d
ON  kr.productid = d.productid
AND kr.cityname  = d.cityname
AND kr.cdate     = d.cdate
GROUP BY
    kr.cdate,
    kr.keywordid,
    kr.productid,
    kr.cityname;


-- ============================================================
-- QUERY HELPER
-- ============================================================

SELECT
    cdate,
    keywordid,
    productid,
    cityname,
    brandid,
    categoryid,
    subcategoryid,
    mrp,
    city_category_weight,
    weighted_price,
    weighted_discount
FROM (
    SELECT
        cdate,
        keywordid,
        productid,
        cityname,
        min(brandid)                  AS brandid,
        min(categoryid)               AS categoryid,
        min(subcategoryid)            AS subcategoryid,
        min(mrp)                      AS mrp,
        sum(city_category_weight)     AS city_category_weight,
        sum(weighted_price)           AS weighted_price,
        sum(weighted_discount)        AS weighted_discount
    FROM Blinkit_Pricing_Band
    GROUP BY cdate, keywordid, productid, cityname
);


-- ============================================================
-- INITIAL BACKFILL INSERT
-- Run ONCE after creating the table and MV to populate
-- Blinkit_Pricing_Band with all historical data.
-- The MV handles new inserts automatically going forward.
-- ============================================================

INSERT INTO Blinkit_Pricing_Band
SELECT
    kr.cdate                                                        AS cdate,
    kr.keywordid                                                    AS keywordid,
    kr.productid                                                    AS productid,
    kr.cityname                                                     AS cityname,

    min(kr.brandid)                                                 AS brandid,
    min(kr.categoryid)                                              AS categoryid,
    min(kr.subcategoryid)                                           AS subcategoryid,

    toFloat64(min(d.mrp))                                           AS mrp,
    toFloat64(min(d.city_category_weight))                          AS city_category_weight,
    toFloat64(min(d.weighted_price))                                AS weighted_price,
    toFloat64(min(d.weighted_discount))                             AS weighted_discount
FROM
(
    SELECT
        cdate,
        keywordid,
        productid,
        cityname,
        min(brandid)       AS brandid,
        min(categoryid)    AS categoryid,
        min(subcategoryid) AS subcategoryid
    FROM BlinkitKeywordRanking_daily
    GROUP BY cdate, keywordid, productid, cityname
) AS kr
JOIN
(
    SELECT
        cdate,
        productid,
        cityname,
        min(mrp)                  AS mrp,
        sum(city_category_weight) AS city_category_weight,
        sum(weighted_price)       AS weighted_price,
        sum(weighted_discount)    AS weighted_discount
    FROM Blinkit_Discounting
    GROUP BY cdate, productid, cityname
) AS d
ON  kr.productid = d.productid
AND kr.cityname  = d.cityname
AND kr.cdate     = d.cdate
GROUP BY
    kr.cdate,
    kr.keywordid,
    kr.productid,
    kr.cityname;
