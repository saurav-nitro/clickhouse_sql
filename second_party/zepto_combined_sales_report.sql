CREATE TABLE Zepto_Combined_Sales_Report
(
    itemid                  String,
    brands_choice           LowCardinality(String),
    reportdate              Date,
    category                String,
    subcategory             String,
    organizationid          String,

    productid               AggregateFunction(any, String),
    itemname                AggregateFunction(any, String),
    brand_internal_sku_code AggregateFunction(any, String),

    cogs                    AggregateFunction(any, Float64),
    margin                  AggregateFunction(any, Float64),
    tax_rate                AggregateFunction(any, Float64),
    active                  AggregateFunction(any, Boolean),
    brand                   AggregateFunction(any, String),

    qty                     AggregateFunction(sum, Float64),
    mrp                     AggregateFunction(any, Float64),
    gmv                     AggregateFunction(sum, Float64),
    net                     AggregateFunction(sum, Float64),
    taxes_paid              AggregateFunction(sum, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(reportdate)
ORDER BY (organizationid, reportdate, brands_choice, category, subcategory, itemid);



CREATE MATERIALIZED VIEW Zepto_Combined_Sales_Report_MV
TO Zepto_Combined_Sales_Report
AS
SELECT
    lower(toString(sr.skuNumber))                           AS itemid,

    ifNull(scm.Brand_s_Choice, 'Unknown')                   AS brands_choice,

    toDate(sr.reportDate)                                   AS reportdate,

    toString(sr.organizationId)                             AS organizationid,

    anyState(ifNull(scd.Zepto_SKU_Code, 'Unknown'))         AS productid,

    anyState(ifNull(sr.skuName, 'Unknown'))                  AS itemname,

    anyState(coalesce(scd.Category, sr.skuCategory, 'Unknown'))        AS category,

    anyState(coalesce(scd.Sub__Category, sr.skuSubCategory, 'Unknown')) AS subcategory,

    anyState(ifNull(scd.Brand_Internal_SKU_Code, 'Unknown')) AS brand_internal_sku_code,

    anyState(assumeNotNull(toFloat64OrZero(scd.COGS)))      AS cogs,

    anyState(ifNull(scd.zepto_margin, 0.0))                 AS margin,

    anyState(assumeNotNull(toFloat64OrZero(scd.TAXES)))     AS tax_rate,

    anyState(ifNull(toUInt8(scd.zepto_active), 0))          AS active,

    anyState(coalesce(scd.brand, sr.brandName, 'Unknown'))  AS brand,

    sumState(toFloat64(ifNull(sr.salesQuantity, 0)))        AS qty,

    anyState(toFloat64(ifNull(sr.mrp, 0)))                  AS mrp,

    sumState(toFloat64(ifNull(sr.grossMerchandiseValue, 0))) AS gmv,

    sumState(
        toFloat64(ifNull(sr.grossMerchandiseValue, 0)) *
        (1 - ifNull(scd.zepto_margin, 0) / 100.0) /
        (1 + assumeNotNull(toFloat64OrZero(scd.TAXES)) / 100.0)
    )                                                       AS net,

    sumState(
        toFloat64(ifNull(sr.grossMerchandiseValue, 0)) *
        (1 - ifNull(scd.zepto_margin, 0) / 100.0) /
        (1 + assumeNotNull(toFloat64OrZero(scd.TAXES)) / 100.0) *
        (assumeNotNull(toFloat64OrZero(scd.TAXES)) / 100.0)
    )                                                       AS taxes_paid

FROM ZeptoReportSales AS sr

LEFT JOIN Static_City_Mapping AS scm
    ON sr.city          = scm.Zepto
   AND sr.organizationId = scm.organizationid

LEFT JOIN Static_Combined_Data AS scd
    ON lower(toString(sr.skuNumber)) = lower(toString(scd.Zepto_SKU_Code))
   AND sr.organizationId              = scd.organizationid

GROUP BY
    itemid,
    reportdate,
    brands_choice,
    organizationid;



INSERT INTO Zepto_Combined_Sales_Report
(
    itemid,
    brands_choice,
    reportdate,
    category,
    subcategory,
    organizationid,
    productid,
    itemname,
    brand_internal_sku_code,
    cogs,
    margin,
    tax_rate,
    active,
    brand,
    qty,
    mrp,
    gmv,
    net,
    taxes_paid
)

SELECT
    lower(toString(sr.skuNumber))                           AS itemid,

    ifNull(scm.Brand_s_Choice, 'Unknown')                   AS brands_choice,

    toDate(sr.reportDate)                                   AS reportdate,

    anyState(coalesce(scd.Category, sr.skuCategory, 'Unknown'))        AS category,

    anyState(coalesce(scd.Sub__Category, sr.skuSubCategory, 'Unknown')) AS subcategory,

    toString(sr.organizationId)                             AS organizationid,

    anyState(ifNull(scd.Zepto_SKU_Code, 'Unknown'))         AS productid,

    anyState(ifNull(sr.skuName, 'Unknown'))                  AS itemname,

    anyState(ifNull(scd.Brand_Internal_SKU_Code, 'Unknown')) AS brand_internal_sku_code,

    anyState(assumeNotNull(toFloat64OrZero(scd.COGS)))      AS cogs,

    anyState(ifNull(scd.zepto_margin, 0.0))                 AS margin,

    anyState(assumeNotNull(toFloat64OrZero(scd.TAXES)))     AS tax_rate,

    anyState(ifNull(toUInt8(scd.zepto_active), 0))          AS active,

    anyState(coalesce(scd.brand, sr.brandName, 'Unknown'))  AS brand,

    sumState(toFloat64(ifNull(sr.salesQuantity, 0)))        AS qty,

    anyState(toFloat64(ifNull(sr.mrp, 0)))                  AS mrp,

    sumState(toFloat64(ifNull(sr.grossMerchandiseValue, 0))) AS gmv,

    sumState(
        toFloat64(ifNull(sr.grossMerchandiseValue, 0)) *
        (1 - ifNull(scd.zepto_margin, 0) / 100.0) /
        (1 + assumeNotNull(toFloat64OrZero(scd.TAXES)) / 100.0)
    )                                                       AS net,

    sumState(
        toFloat64(ifNull(sr.grossMerchandiseValue, 0)) *
        (1 - ifNull(scd.zepto_margin, 0) / 100.0) /
        (1 + assumeNotNull(toFloat64OrZero(scd.TAXES)) / 100.0) *
        (assumeNotNull(toFloat64OrZero(scd.TAXES)) / 100.0)
    )                                                       AS taxes_paid

FROM ZeptoReportSales AS sr

LEFT JOIN Static_City_Mapping AS scm
    ON sr.city          = scm.Zepto
   AND sr.organizationId = scm.organizationid

LEFT JOIN Static_Combined_Data AS scd
    ON lower(toString(sr.skuNumber)) = lower(toString(scd.Zepto_SKU_Code))
   AND sr.organizationId              = scd.organizationid

GROUP BY
    itemid,
    brands_choice,
    reportdate,
    organizationid;
