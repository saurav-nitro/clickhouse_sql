CREATE TABLE Instamart_Combined_Sales_Report (
    itemid String,
    brands_choice LowCardinality(String),
    reportdate Date,
    category String,
    subcategory String,
    organizationid String,

    productid AggregateFunction(any, String),
    itemname AggregateFunction(any, String),
    brand_internal_sku_code AggregateFunction(any, String),

    cogs AggregateFunction(any, Float64),
    margin AggregateFunction(any, Float64),
    tax_rate AggregateFunction(any, Float64),
    active AggregateFunction(any, Boolean),
    brand AggregateFunction(any, String),

    qty AggregateFunction(sum, Float64),
    mrp AggregateFunction(any, Float64),
    gmv AggregateFunction(sum, Float64),
    net AggregateFunction(sum, Float64),
    taxes_paid AggregateFunction(sum, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(reportdate)
ORDER BY (organizationid, reportdate, brands_choice, category, subcategory, itemid);



CREATE MATERIALIZED VIEW Instamart_Combined_Sales_Report_MV
TO Instamart_Combined_Sales_Report AS
SELECT
    sr.itemCode AS itemid,
    coalesce(scm.Brand_s_Choice,'Unknown') AS brands_choice,
    toDate(sr.reportDate) AS reportdate,
    sr.organizationId AS organizationid,

    anyState(scd.Instamart_Item_Code) AS productid,
    anyState(sr.productName) AS itemname,
    anyState(coalesce(scd.Category,'Unknown')) AS category,
    anyState(coalesce(scd.Sub__Category,'Unknown')) AS subcategory,
    anyState(coalesce(scd.Brand_Internal_SKU_Code,'Unknown')) AS brand_internal_sku_code,

    anyState(toFloat64(ifNull(scd.COGS,0))) AS cogs,
    anyState(ifNull(scd.instamart_margin,0)) AS margin,
    anyState(toFloat64(ifNull(scd.TAXES,0))) AS tax_rate,
    anyState(toBoolean(ifNull(scd.instamart_active,0))) AS active,
    anyState(coalesce(scd.brand,'Unknown')) AS brand,

    sumState(ifNull(sr.finalQtyYesterday,0)) AS qty,
    anyState(toFloat64(ifNull(sr.finalMrp,0))) AS mrp,
    sumState(toFloat64(ifNull(sr.finalGmvYesterday,0))) AS gmv,
    sumState(
        toFloat64(ifNull(sr.finalGmvYesterday,0)) *
        (1 - ifNull(scd.instamart_margin,0)/100.0) /
        (1 + toFloat64(ifNull(scd.TAXES,0))/100.0)
    ) AS net,
    sumState(
        toFloat64(ifNull(sr.finalGmvYesterday,0)) *
        (1 - ifNull(scd.instamart_margin,0)/100.0) /
        (1 + toFloat64(ifNull(scd.TAXES,0))/100.0) *
        (toFloat64(ifNull(scd.TAXES,0))/100.0)
    ) AS taxes_paid
FROM InstamartReportSales sr
LEFT JOIN Static_City_Mapping scm
    ON sr.city = scm.Instamart
    AND sr.organizationId = scm.organizationid
LEFT JOIN Static_Combined_Data scd
    ON sr.itemCode = scd.Instamart_SKU_Code
    AND sr.organizationId = scd.organizationid
GROUP BY
    itemid,
    reportdate,
    brands_choice,
    organizationid;


INSERT INTO Instamart_Combined_Sales_Report
SELECT
    sr.itemCode AS itemid,
    coalesce(scm.Brand_s_Choice,'Unknown') AS brands_choice,
    toDate(sr.reportDate) AS reportdate,
    sr.organizationId AS organizationid,

    anyState(scd.Instamart_Item_Code) AS productid,
    anyState(sr.productName) AS itemname,
    anyState(coalesce(scd.Category,'Unknown')) AS category,
    anyState(coalesce(scd.Sub__Category,'Unknown')) AS subcategory,
    anyState(coalesce(scd.Brand_Internal_SKU_Code,'Unknown')) AS brand_internal_sku_code,

    anyState(toFloat64(ifNull(scd.COGS,0))) AS cogs,
    anyState(ifNull(scd.instamart_margin,0)) AS margin,
    anyState(toFloat64(ifNull(scd.TAXES,0))) AS tax_rate,
    anyState(toBoolean(ifNull(scd.instamart_active,0))) AS active,
    anyState(coalesce(scd.brand,'Unknown')) AS brand,

    sumState(ifNull(sr.finalQtyYesterday,0)) AS qty,
    anyState(toFloat64(ifNull(sr.finalMrp,0))) AS mrp,
    sumState(toFloat64(ifNull(sr.finalGmvYesterday,0))) AS gmv,
    sumState(
        toFloat64(ifNull(sr.finalGmvYesterday,0)) *
        (1 - ifNull(scd.instamart_margin,0)/100.0) /
        (1 + toFloat64(ifNull(scd.TAXES,0))/100.0)
    ) AS net,
    sumState(
        toFloat64(ifNull(sr.finalGmvYesterday,0)) *
        (1 - ifNull(scd.instamart_margin,0)/100.0) /
        (1 + toFloat64(ifNull(scd.TAXES,0))/100.0) *
        (toFloat64(ifNull(scd.TAXES,0))/100.0)
    ) AS taxes_paid
FROM InstamartReportSales sr
LEFT JOIN Static_City_Mapping scm
    ON sr.city = scm.Instamart
    AND sr.organizationId = scm.organizationid
LEFT JOIN Static_Combined_Data scd
    ON sr.itemCode = scd.Instamart_SKU_Code
    AND sr.organizationId = scd.organizationid
GROUP BY
    itemid,
    reportdate,
    brands_choice,
    organizationid;    