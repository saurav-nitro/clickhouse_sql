CREATE TABLE Blinkit_Combined_Sales_Report
(
    organizationid String,
    reportdate Date,
    itemid String,

    brands_choice LowCardinality(String),
    category LowCardinality(String),
    subcategory LowCardinality(String),
    brand LowCardinality(String),

    productid AggregateFunction(any, String),
    itemname AggregateFunction(any, String),
    brand_internal_sku_code AggregateFunction(any, String),
    cogs AggregateFunction(any, Float64),
    margin AggregateFunction(any, Float64),
    tax_rate AggregateFunction(any, Float64),
    active AggregateFunction(any, Boolean),

    qty AggregateFunction(sum, Float64),
    gmv AggregateFunction(sum, Float64),
    net AggregateFunction(sum, Float64),
    taxes_paid AggregateFunction(sum, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(reportdate)
ORDER BY (organizationid, reportdate, itemid);



CREATE MATERIALIZED VIEW Blinkit_Combined_Sales_Report_MV
TO Blinkit_Combined_Sales_Report
AS
SELECT
    sr.itemId AS itemid,
    coalesce(scm.Brand_s_Choice, 'Unknown') AS brands_choice,
    toDate(sr.reportDate) AS reportdate,
    coalesce(sr.organizationId, 'Unknown') AS organizationid,
    anyState(coalesce(scd.Category, 'Unknown')) AS category,
    anyState(coalesce(scd.Sub__Category, 'Unknown')) AS subcategory,
    anyState(coalesce(scd.Blinkit_PRID, 'Unknown')) AS productid,
    anyState(coalesce(sr.itemName, 'Unknown')) AS itemname,
    anyState(coalesce(scd.Brand_Internal_SKU_Code, 'Unknown')) AS brand_internal_sku_code,
    anyState(ifNull(toFloat64(scd.COGS), 0)) AS cogs,
    anyState(ifNull(scd.blinkit_margin, 0)) AS margin,
    anyState(ifNull(toFloat64(scd.TAXES), 0)) AS tax_rate,
    anyState(ifNull(scd.blinkit_active, 0) != 0) AS active,
    anyState(coalesce(scd.brand, 'Unknown')) AS brand,
    sumState(toFloat64(ifNull(sr.qtySold, 0))) AS qty,
    sumState(toFloat64(ifNull(sr.mrp, 0))) AS gmv,
    sumState(toFloat64(ifNull(sr.mrp, 0)) * (1 - ifNull(scd.blinkit_margin, 0)/100) / (1 + ifNull(toFloat64(scd.TAXES),0)/100)) AS net,
    sumState(toFloat64(ifNull(sr.mrp, 0)) * (1 - ifNull(scd.blinkit_margin, 0)/100) / (1 + ifNull(toFloat64(scd.TAXES),0)/100) * (ifNull(toFloat64(scd.TAXES),0)/100)) AS taxes_paid
FROM BlinkitReportSales sr
LEFT JOIN Static_City_Mapping scm
    ON sr.cityName = scm.Blinkit
   AND sr.organizationId = scm.organizationid
LEFT JOIN Static_Combined_Data scd
    ON sr.itemId = scd.Blinkit_SKU_Code
   AND sr.organizationId = scd.organizationid
GROUP BY
    itemid,
    reportdate,
    brands_choice,
    organizationid;




INSERT INTO Blinkit_Combined_Sales_Report
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
    gmv,
    net,
    taxes_paid
)
SELECT
    ifNull(sr.organizationId, '') AS organizationid,
    toDate(sr.reportDate) AS reportdate,
    sr.itemId AS itemid,

    coalesce(scm.Brand_s_Choice, 'Unknown') AS brands_choice,
    anyState(coalesce(scd.Category, 'Unknown')) AS category,
    anyState(coalesce(scd.Sub__Category, 'Unknown')) AS subcategory,
    anyState(coalesce(scd.brand, 'Unknown')) AS brand,

    anyState(coalesce(scd.Blinkit_PRID, 'Unknown')) AS productid,
    anyState(coalesce(sr.itemName, 'Unknown')) AS itemname,
    anyState(coalesce(scd.Brand_Internal_SKU_Code, 'Unknown')) AS brand_internal_sku_code,
    anyState(ifNull(toFloat64(scd.COGS), 0)) AS cogs,
    anyState(ifNull(scd.blinkit_margin, 0)) AS margin,
    anyState(ifNull(toFloat64(scd.TAXES), 0)) AS tax_rate,
    anyState(ifNull(scd.blinkit_active, 0) != 0) AS active,

    sumState(toFloat64(ifNull(sr.qtySold, 0))) AS qty,
    sumState(toFloat64(ifNull(sr.mrp, 0))) AS gmv,
    sumState(
        (toFloat64(ifNull(sr.mrp, 0)) * (1 - ifNull(scd.blinkit_margin, 0)/100)) /
        (1 + ifNull(toFloat64(scd.TAXES), 0)/100)
    ) AS net,
    sumState(
        ((toFloat64(ifNull(sr.mrp, 0)) * (1 - ifNull(scd.blinkit_margin, 0)/100)) /
        (1 + ifNull(toFloat64(scd.TAXES), 0)/100)) *
        (ifNull(toFloat64(scd.TAXES), 0)/100)
    ) AS taxes_paid

FROM BlinkitReportSales sr
LEFT JOIN Static_City_Mapping scm
    ON sr.cityName = scm.Blinkit
   AND sr.organizationId = scm.organizationid
LEFT JOIN Static_Combined_Data scd
    ON sr.itemId = scd.Blinkit_SKU_Code
   AND sr.organizationId = scd.organizationid

GROUP BY
    sr.itemId,
    sr.reportDate,
    sr.organizationId,
    brands_choice;