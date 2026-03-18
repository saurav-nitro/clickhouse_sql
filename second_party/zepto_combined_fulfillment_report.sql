CREATE TABLE Zepto_Combined_Fulfillment_Report
(
    itemid                  String,
    ponumber                String,
    po_status               String,
    brand_internal_sku_code String,
    brands_choice           String,
    organizationid          String,
    orderdate               Date,

    itemname                AggregateFunction(any, String),
    productid               AggregateFunction(any, String),
    mrp                     AggregateFunction(any, Float64),
    appointmentdate         AggregateFunction(any, Date),
    expirydate              AggregateFunction(any, Date),
    backendfacilityname     AggregateFunction(any, String),
    quantity                AggregateFunction(any, Float64),
    grn_quantity            AggregateFunction(any, Float64),
    total_amount            AggregateFunction(any, Float64),
    fill_rate_sku           AggregateFunction(any, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(orderdate)
ORDER BY (organizationid, orderdate, brands_choice, ponumber, po_status, brand_internal_sku_code, itemid);



CREATE MATERIALIZED VIEW Zepto_Combined_Fulfillment_Report_MV
TO Zepto_Combined_Fulfillment_Report
AS
WITH daily_mrp AS (
    SELECT
        lower(skuNumber)  AS sku,
        toDate(reportDate) AS orderdate,
        min(mrp)           AS mrp
    FROM ZeptoReportSales
    GROUP BY sku, orderdate
)
SELECT
    lower(toString(rf.skuId))                          AS itemid,
    rf.poCode                                          AS ponumber,
    'Unknown'                                          AS po_status,
    coalesce(scd.Brand_Internal_SKU_Code, 'Unknown')   AS brand_internal_sku_code,
    coalesce(scm.Brand_s_Choice, 'Unknown')            AS brands_choice,
    toString(rf.organizationId)                        AS organizationid,
    toDate(rf.poDate)                                  AS orderdate,

    anyState(ifNull(rf.skuName, 'Unknown'))             AS itemname,

    anyState(ifNull(toString(scd.Zepto_SKU_Code), 'Unknown')) AS productid,

    anyState(toFloat64(ifNull(sales.mrp, 0)))           AS mrp,

    anyState(ifNull(toDate(rf.grnDate), toDate('1970-01-01'))) AS appointmentdate,

    anyState(toDate('1970-01-01'))                      AS expirydate,

    anyState(ifNull(rf.warehouseName, 'Unknown'))       AS backendfacilityname,

    anyState(toFloat64(ifNull(rf.poQuantity, 0)))       AS quantity,

    anyState(toFloat64(ifNull(rf.grnQuantity, 0)))      AS grn_quantity,

    anyState(
        toFloat64(ifNull(sales.mrp, 0)) * toFloat64(ifNull(rf.poQuantity, 0))
    )                                                   AS total_amount,

    anyState(
        ifNull(
            toFloat64(ifNull(rf.skuLevelFillRate, 0)),
            0.0
        )
    )                                                   AS fill_rate_sku

FROM ZeptoReportFulfillment AS rf

LEFT JOIN Static_City_Mapping AS scm
    ON rf.city          = scm.Zepto
   AND rf.organizationId = scm.organizationid

LEFT JOIN Static_Combined_Data AS scd
    ON lower(toString(rf.skuId)) = lower(toString(scd.Zepto_SKU_Code))
   AND rf.organizationId          = scd.organizationid

LEFT JOIN daily_mrp AS sales
    ON lower(toString(rf.skuId)) = sales.sku
   AND toDate(rf.poDate)          = sales.orderdate

GROUP BY
    itemid,
    ponumber,
    po_status,
    brand_internal_sku_code,
    brands_choice,
    organizationid,
    orderdate;



INSERT INTO Zepto_Combined_Fulfillment_Report
WITH daily_mrp AS (
    SELECT
        lower(skuNumber)   AS sku,
        toDate(reportDate) AS orderdate,
        min(mrp)           AS mrp
    FROM ZeptoReportSales
    GROUP BY sku, orderdate
)
SELECT
    lower(toString(rf.skuId))                          AS itemid,
    rf.poCode                                          AS ponumber,
    'Unknown'                                          AS po_status,
    coalesce(scd.Brand_Internal_SKU_Code, 'Unknown')   AS brand_internal_sku_code,
    coalesce(scm.Brand_s_Choice, 'Unknown')            AS brands_choice,
    toString(rf.organizationId)                        AS organizationid,
    toDate(rf.poDate)                                  AS orderdate,

    anyState(ifNull(rf.skuName, 'Unknown'))             AS itemname,

    anyState(ifNull(toString(scd.Zepto_SKU_Code), 'Unknown')) AS productid,

    anyState(toFloat64(ifNull(sales.mrp, 0)))           AS mrp,

    anyState(ifNull(toDate(rf.grnDate), toDate('1970-01-01'))) AS appointmentdate,

    anyState(toDate('1970-01-01'))                      AS expirydate,

    anyState(ifNull(rf.warehouseName, 'Unknown'))       AS backendfacilityname,

    anyState(toFloat64(ifNull(rf.poQuantity, 0)))       AS quantity,

    anyState(toFloat64(ifNull(rf.grnQuantity, 0)))      AS grn_quantity,

    anyState(
        toFloat64(ifNull(sales.mrp, 0)) * toFloat64(ifNull(rf.poQuantity, 0))
    )                                                   AS total_amount,

    anyState(
        ifNull(
            toFloat64(ifNull(rf.skuLevelFillRate, 0)),
            0.0
        )
    )                                                   AS fill_rate_sku

FROM ZeptoReportFulfillment AS rf

LEFT JOIN Static_City_Mapping AS scm
    ON rf.city          = scm.Zepto
   AND rf.organizationId = scm.organizationid

LEFT JOIN Static_Combined_Data AS scd
    ON lower(toString(rf.skuId)) = lower(toString(scd.Zepto_SKU_Code))
   AND rf.organizationId          = scd.organizationid

LEFT JOIN daily_mrp AS sales
    ON lower(toString(rf.skuId)) = sales.sku
   AND toDate(rf.poDate)          = sales.orderdate

GROUP BY
    itemid,
    ponumber,
    po_status,
    brand_internal_sku_code,
    brands_choice,
    organizationid,
    orderdate;
