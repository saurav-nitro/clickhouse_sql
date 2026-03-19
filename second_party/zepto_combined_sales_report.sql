-- Table
CREATE TABLE Zepto_Combined_Sales_Report
(
    organizationid          String,
    reportdate              Date,
    itemid                  String,

    brands_choice           LowCardinality(String),
    category                LowCardinality(String),
    subcategory             LowCardinality(String),
    brand                   LowCardinality(String),

    productid               AggregateFunction(any, String),
    itemname                AggregateFunction(any, String),
    brand_internal_sku_code AggregateFunction(any, String),

    cogs                    AggregateFunction(any, Float64),
    margin                  AggregateFunction(any, Float64),
    tax_rate                AggregateFunction(any, Float64),
    active                  AggregateFunction(any, Boolean),

    qty                     AggregateFunction(sum, Float64),
    mrp                     AggregateFunction(any, Float64),
    gmv                     AggregateFunction(sum, Float64),
    net                     AggregateFunction(sum, Float64),
    taxes_paid              AggregateFunction(sum, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(reportdate)
ORDER BY (organizationid, reportdate, itemid);


-- Materialized View
CREATE MATERIALIZED VIEW Zepto_Combined_Sales_Report_MV
TO Zepto_Combined_Sales_Report
AS
SELECT
    lower(toString(sr.skuNumber)) AS itemid,
    coalesce(scm.Brand_s_Choice, 'Unknown') AS brands_choice,
    toDate(sr.reportDate) AS reportdate,
    coalesce(sr.organizationId, 'Unknown') AS organizationid,

    anyState(coalesce(scd.Zepto_SKU_Code, 'Unknown')) AS productid,
    anyState(coalesce(sr.skuName, 'Unknown')) AS itemname,
    anyState(coalesce(scd.Category, sr.skuCategory, 'Unknown')) AS category,
    anyState(coalesce(scd.Sub__Category, sr.skuSubCategory, 'Unknown')) AS subcategory,
    anyState(coalesce(scd.Brand_Internal_SKU_Code, 'Unknown')) AS brand_internal_sku_code,
    anyState(ifNull(toFloat64(scd.COGS), 0)) AS cogs,
    anyState(ifNull(scd.zepto_margin, 0)) AS margin,
    anyState(ifNull(toFloat64(scd.TAXES), 0)) AS tax_rate,
    anyState(ifNull(scd.zepto_active, 0) != 0) AS active,
    anyState(coalesce(scd.brand, sr.brandName, 'Unknown')) AS brand,

    sumState(toFloat64(ifNull(sr.salesQuantity, 0))) AS qty,
    anyState(toFloat64(ifNull(sr.mrp, 0))) AS mrp,
    sumState(toFloat64(ifNull(sr.grossMerchandiseValue, 0))) AS gmv,
    sumState(
        toFloat64(ifNull(sr.grossMerchandiseValue, 0)) *
        (1 - ifNull(scd.zepto_margin, 0)/100) /
        (1 + ifNull(toFloat64(scd.TAXES), 0)/100)
    ) AS net,
    sumState(
        toFloat64(ifNull(sr.grossMerchandiseValue, 0)) *
        (1 - ifNull(scd.zepto_margin, 0)/100) /
        (1 + ifNull(toFloat64(scd.TAXES), 0)/100) *
        (ifNull(toFloat64(scd.TAXES), 0)/100)
    ) AS taxes_paid

FROM ZeptoReportSales AS sr
LEFT JOIN Static_City_Mapping AS scm
    ON sr.city = scm.Zepto
   AND sr.organizationId = scm.organizationid
LEFT JOIN Static_Combined_Data AS scd
    ON lower(toString(sr.skuNumber)) = lower(toString(scd.Zepto_SKU_Code))
   AND sr.organizationId = scd.organizationid

GROUP BY
    itemid,
    reportdate,
    brands_choice,
    organizationid;


-- Insert (for backfill)
INSERT INTO Zepto_Combined_Sales_Report
(
    organizationid,
    reportdate,
    itemid,
    brands_choice,
    category,
    subcategory,
    brand,
    productid,
    itemname,
    brand_internal_sku_code,
    cogs,
    margin,
    tax_rate,
    active,
    qty,
    mrp,
    gmv,
    net,
    taxes_paid
)
SELECT
    coalesce(sr.organizationId, 'Unknown') AS organizationid,
    toDate(sr.reportDate) AS reportdate,
    lower(toString(sr.skuNumber)) AS itemid,

    coalesce(scm.Brand_s_Choice, 'Unknown') AS brands_choice,
    anyState(coalesce(scd.Category, sr.skuCategory, 'Unknown')) AS category,
    anyState(coalesce(scd.Sub__Category, sr.skuSubCategory, 'Unknown')) AS subcategory,
    anyState(coalesce(scd.brand, sr.brandName, 'Unknown')) AS brand,

    anyState(coalesce(scd.Zepto_SKU_Code, 'Unknown')) AS productid,
    anyState(coalesce(sr.skuName, 'Unknown')) AS itemname,
    anyState(coalesce(scd.Brand_Internal_SKU_Code, 'Unknown')) AS brand_internal_sku_code,
    anyState(ifNull(toFloat64(scd.COGS), 0)) AS cogs,
    anyState(ifNull(scd.zepto_margin, 0)) AS margin,
    anyState(ifNull(toFloat64(scd.TAXES), 0)) AS tax_rate,
    anyState(ifNull(scd.zepto_active, 0) != 0) AS active,

    sumState(toFloat64(ifNull(sr.salesQuantity, 0))) AS qty,
    anyState(toFloat64(ifNull(sr.mrp, 0))) AS mrp,
    sumState(toFloat64(ifNull(sr.grossMerchandiseValue, 0))) AS gmv,
    sumState(
        toFloat64(ifNull(sr.grossMerchandiseValue, 0)) *
        (1 - ifNull(scd.zepto_margin, 0)/100) /
        (1 + ifNull(toFloat64(scd.TAXES), 0)/100)
    ) AS net,
    sumState(
        toFloat64(ifNull(sr.grossMerchandiseValue, 0)) *
        (1 - ifNull(scd.zepto_margin, 0)/100) /
        (1 + ifNull(toFloat64(scd.TAXES), 0)/100) *
        (ifNull(toFloat64(scd.TAXES), 0)/100)
    ) AS taxes_paid

FROM ZeptoReportSales AS sr
LEFT JOIN Static_City_Mapping AS scm
    ON sr.city = scm.Zepto
   AND sr.organizationId = scm.organizationid
LEFT JOIN Static_Combined_Data AS scd
    ON lower(toString(sr.skuNumber)) = lower(toString(scd.Zepto_SKU_Code))
   AND sr.organizationId = scd.organizationid

GROUP BY
    itemid,
    reportdate,
    brands_choice,
    organizationid;