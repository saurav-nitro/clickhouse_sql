CREATE TABLE blinkit_pricing_band
(
    keywordid String,
    cdate Date,
    cityname String,
    productid UInt32,
    brandid String,
    categoryid Int32,
    subcategoryid Int32,
    mrp Float64,
    city_category_weight Float64,
    weighted_price Float64,
    weighted_discount Float64
)
ENGINE = MergeTree
ORDER BY (cdate, productid, cityname, keywordid);


CREATE MATERIALIZED VIEW blinkit_pricing_band_mv
TO blinkit_pricing_band
AS
SELECT
    kr.keywordid,
    kr.cdate,
    kr.cityname,
    kr.productid,

    any(kr.brandid) AS brandid,
    any(kr.categoryid) AS categoryid,
    any(kr.subcategoryid) AS subcategoryid,

    any(d.mrp) AS mrp,
    any(d.city_category_weight) AS city_category_weight,
    any(d.weighted_price) AS weighted_price,
    any(d.weighted_discount) AS weighted_discount

FROM blinkitkeywordranking_daily kr
INNER JOIN blinkit_discounting d
    ON kr.productid = d.productid
    AND kr.cityname = d.cityname
    AND kr.cdate = d.cdate

GROUP BY
    kr.keywordid,
    kr.cdate,
    kr.cityname,
    kr.productid;