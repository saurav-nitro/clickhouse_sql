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
TO Instamart_Combined_Sales_Report
AS
SELECT
    toString(sr.itemCode) AS itemid,

    ifNull(scm.Brand_s_Choice,'Unknown') AS brands_choice,

    toDate(sr.reportDate) AS reportdate,

    toString(sr.organizationId) AS organizationid,

    anyState(ifNull(scd.Instamart_Item_Code,'Unknown')) AS productid,

    anyState(ifNull(sr.productName,'Unknown')) AS itemname,

    anyState(ifNull(scd.Category,'Unknown')) AS category,

    anyState(ifNull(scd.Sub__Category,'Unknown')) AS subcategory,

    anyState(ifNull(scd.Brand_Internal_SKU_Code,'Unknown')) AS brand_internal_sku_code,

    anyState(assumeNotNull(toFloat64OrZero(scd.COGS))) AS cogs,

    anyState(ifNull(scd.instamart_margin,0.0)) AS margin,

    anyState(assumeNotNull(toFloat64OrZero(scd.TAXES))) AS tax_rate,

    anyState(ifNull(toUInt8(scd.instamart_active),0)) AS active,

    anyState(ifNull(scd.brand,'Unknown')) AS brand,

    sumState(toFloat64(ifNull(sr.finalQtyYesterday,0))) AS qty,

    anyState(toFloat64(ifNull(sr.finalMrp,0))) AS mrp,

    sumState(toFloat64(ifNull(sr.finalGmvYesterday,0))) AS gmv,

    sumState(
        toFloat64(ifNull(sr.finalGmvYesterday,0)) *
        (1 - ifNull(scd.instamart_margin,0)/100.0) /
        (1 + assumeNotNull(toFloat64OrZero(scd.TAXES))/100.0)
    ) AS net,

    sumState(
        toFloat64(ifNull(sr.finalGmvYesterday,0)) *
        (1 - ifNull(scd.instamart_margin,0)/100.0) /
        (1 + assumeNotNull(toFloat64OrZero(scd.TAXES))/100.0) *
        (assumeNotNull(toFloat64OrZero(scd.TAXES))/100.0)
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
toString(sr.itemCode) AS itemid,

ifNull(scm.Brand_s_Choice,'Unknown') AS brands_choice,

toDate(sr.reportDate) AS reportdate,

anyState(ifNull(scd.Category,'Unknown')) AS category,

anyState(ifNull(scd.Sub__Category,'Unknown')) AS subcategory,

toString(sr.organizationId) AS organizationid,

anyState(ifNull(scd.Instamart_Item_Code,'Unknown')) AS productid,

anyState(ifNull(sr.productName,'Unknown')) AS itemname,

anyState(ifNull(scd.Brand_Internal_SKU_Code,'Unknown')) AS brand_internal_sku_code,

anyState(assumeNotNull(toFloat64OrZero(scd.COGS))) AS cogs,

anyState(ifNull(scd.instamart_margin,0.0)) AS margin,

anyState(assumeNotNull(toFloat64OrZero(scd.TAXES))) AS tax_rate,

anyState(ifNull(toUInt8(scd.instamart_active),0)) AS active,

anyState(ifNull(scd.brand,'Unknown')) AS brand,

sumState(toFloat64(ifNull(sr.finalQtyYesterday,0))) AS qty,

anyState(toFloat64(ifNull(sr.finalMrp,0))) AS mrp,

sumState(toFloat64(ifNull(sr.finalGmvYesterday,0))) AS gmv,

sumState(
toFloat64(ifNull(sr.finalGmvYesterday,0))
*(1-ifNull(scd.instamart_margin,0)/100)
/
(1+assumeNotNull(toFloat64OrZero(scd.TAXES))/100)
) AS net,

sumState(
toFloat64(ifNull(sr.finalGmvYesterday,0))
*(1-ifNull(scd.instamart_margin,0)/100)
/
(1+assumeNotNull(toFloat64OrZero(scd.TAXES))/100)
*(assumeNotNull(toFloat64OrZero(scd.TAXES))/100)
) AS taxes_paid

FROM InstamartReportSales sr

LEFT JOIN Static_City_Mapping scm
ON sr.city=scm.Instamart
AND sr.organizationId=scm.organizationid

LEFT JOIN Static_Combined_Data scd
ON sr.itemCode=scd.Instamart_SKU_Code
AND sr.organizationId=scd.organizationid

GROUP BY
itemid,
brands_choice,
reportdate,
organizationid;