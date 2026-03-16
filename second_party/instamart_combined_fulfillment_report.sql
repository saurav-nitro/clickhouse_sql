CREATE TABLE Instamart_Combined_Fulfillment_Report
(
    itemid String,
    ponumber String,
    po_status String,
    brand_internal_sku_code String,
    brands_choice String,
    organizationid String,
    orderdate Date,

    itemname AggregateFunction(any, String),
    productid AggregateFunction(any, String),
    mrp AggregateFunction(any, Float64),
    appointmentdate AggregateFunction(any, Date),
    expirydate AggregateFunction(any, Date),
    backendfacilityname AggregateFunction(any, String),
    quantity AggregateFunction(any, Float64),
    grn_quantity AggregateFunction(any, Float64),
    total_amount AggregateFunction(any, Float64),
    fill_rate_sku AggregateFunction(any, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(orderdate)
ORDER BY (organizationid, orderdate, brands_choice, ponumber, po_status, brand_internal_sku_code, itemid);



CREATE MATERIALIZED VIEW Instamart_Combined_Fulfillment_Report_MV
TO Instamart_Combined_Fulfillment_Report
AS
SELECT
    toString(rf.itemId) AS itemid,
    rf.poNumber AS ponumber,
    coalesce(scd.Brand_Internal_SKU_Code,'Unknown') AS brand_internal_sku_code,
    coalesce(scm.Brand_s_Choice,'Unknown') AS brands_choice,
    toString(rf.organizationId) AS organizationid,
    coalesce(rf.status,'Unknown') AS po_status,
    toDate(rf.poDate) AS orderdate,

    anyState(ifNull(rf.name,'Unknown')) AS itemname,
    anyState(toString(scd.Instamart_Item_Code)) AS productid,
    anyState(toFloat64(ifNull(rf.mrp,0))) AS mrp,
    anyState(toDate(rf.appointmentDate)) AS appointmentdate,
    anyState(toDate(rf.expiryDate)) AS expirydate,
    anyState(coalesce(rf.facilityName,'Unknown')) AS backendfacilityname,
    anyState(coalesce(rf.status,'Unknown')) AS po_status,
    anyState(toFloat64(ifNull(rf.unitsOrdered,0))) AS quantity,
    anyState(toFloat64(ifNull(rf.unitsOrdered,0) - ifNull(rf.remainingQuantity,0)) ) AS grn_quantity,
    anyState(toFloat64(ifNull(rf.totalAmount,0))) AS total_amount,
    anyState( 1.0 - toFloat64(ifNull(rf.remainingQuantity,0)) / nullIf(toFloat64(ifNull(rf.unitsOrdered,0)),0) ) AS fill_rate_sku

FROM InstamartReportFulfillment rf

LEFT JOIN Static_City_Mapping scm
    ON rf.facilityName = scm.Instamart
   AND rf.organizationId = scm.organizationid

LEFT JOIN Static_Combined_Data scd
    ON rf.itemId = scd.Instamart_SKU_Code
   AND rf.organizationId = scd.organizationid

GROUP BY
    itemid,
    ponumber,
    brand_internal_sku_code,
    brands_choice,
    organizationid,
    po_status,
    orderdate;