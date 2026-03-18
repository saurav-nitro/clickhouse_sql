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
    coalesce(rf.status,'Unknown') AS po_status,
    coalesce(scd.Brand_Internal_SKU_Code,'Unknown') AS brand_internal_sku_code,
    coalesce(scm.Brand_s_Choice,'Unknown') AS brands_choice,
    toString(rf.organizationId) AS organizationid,
    toDate(rf.poDate) AS orderdate,

    anyState(ifNull(rf.name,'Unknown')) AS itemname,

    anyState(ifNull(toString(scd.Instamart_Item_Code),'Unknown')) AS productid,

    anyState(toFloat64(ifNull(rf.mrp,0))) AS mrp,

    anyState(ifNull(toDate(rf.appointmentDate),toDate('1970-01-01'))) AS appointmentdate,

    anyState(ifNull(toDate(rf.expiryDate),toDate('1970-01-01'))) AS expirydate,

    anyState(ifNull(rf.facilityName,'Unknown')) AS backendfacilityname,

    anyState(toFloat64(ifNull(rf.unitsOrdered,0))) AS quantity,

    anyState(
        toFloat64(ifNull(rf.unitsOrdered,0) - ifNull(rf.remainingQuantity,0))
    ) AS grn_quantity,

    anyState(toFloat64(ifNull(rf.totalAmount,0))) AS total_amount,

    anyState(
        ifNull(
            1 - (
                toFloat64(ifNull(rf.remainingQuantity,0)) /
                nullIf(toFloat64(ifNull(rf.unitsOrdered,0)),0)
            ),
            0
        )
    ) AS fill_rate_sku

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
    po_status,
    brand_internal_sku_code,
    brands_choice,
    organizationid,
    orderdate;




INSERT INTO Instamart_Combined_Fulfillment_Report
(
    itemid,
    ponumber,
    po_status,
    brand_internal_sku_code,
    brands_choice,
    organizationid,
    orderdate,
    itemname,
    productid,
    mrp,
    appointmentdate,
    expirydate,
    backendfacilityname,
    quantity,
    grn_quantity,
    total_amount,
    fill_rate_sku
)

SELECT
    toString(rf.itemId),
    rf.poNumber,
    coalesce(rf.status,'Unknown'),
    coalesce(scd.Brand_Internal_SKU_Code,'Unknown'),
    coalesce(scm.Brand_s_Choice,'Unknown'),
    toString(rf.organizationId),
    toDate(rf.poDate),

    anyState(ifNull(rf.name,'Unknown')),
    anyState(ifNull(toString(scd.Instamart_Item_Code),'Unknown')),
    anyState(toFloat64(ifNull(rf.mrp,0))),

    anyState(ifNull(toDate(rf.appointmentDate),toDate('1970-01-01'))),
    anyState(ifNull(toDate(rf.expiryDate),toDate('1970-01-01'))),

    anyState(ifNull(rf.facilityName,'Unknown')),

    anyState(toFloat64(ifNull(rf.unitsOrdered,0))),

    anyState(
        toFloat64(ifNull(rf.unitsOrdered,0) - ifNull(rf.remainingQuantity,0))
    ),

    anyState(toFloat64(ifNull(rf.totalAmount,0))),

    anyState(
        ifNull(
            1 - (
                toFloat64(ifNull(rf.remainingQuantity,0)) /
                nullIf(toFloat64(ifNull(rf.unitsOrdered,0)),0)
            ),
            0
        )
    )

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
    po_status,
    brand_internal_sku_code,
    brands_choice,
    organizationid,
    orderdate;

