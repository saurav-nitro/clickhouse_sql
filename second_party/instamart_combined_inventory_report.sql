CREATE TABLE Instamart_Combined_Inventory_Report
(
    itemid String,
    backendfacilityname String,
    reportdate Date,

    itemname String,
    productid String,
    category String,
    subcategory String,
    brand String,
    organizationid String,
    brand_internal_sku_code String,
    brands_choice LowCardinality(String),
    active Boolean,

    total_quantity Float64,
    frontendinvqty Float64,
    backendinvqty Float64,

    run_rate_backend Float64,
    run_rate_total Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(reportdate)
ORDER BY (organizationid, reportdate, active, backendfacilityname, itemid)
PRIMARY KEY (organizationid, reportdate, active, backendfacilityname, itemid);


CREATE MATERIALIZED VIEW Instamart_Combined_Inventory_Report_MV
TO Instamart_Combined_Inventory_Report
AS

WITH inventory_data AS
(
    SELECT
        toString(irb.itemCode) AS itemid,

        coalesce(irb.productName,'Unknown') AS itemname,

        toString(scd.Instamart_Item_Code) AS productid,

        coalesce(scd.Category,'Unknown') AS category,
        coalesce(scd.Sub__Category,'Unknown') AS subcategory,

        coalesce(scm.Brand_s_Choice,'Unknown') AS brands_choice,

        toString(irb.organizationId) AS organizationid,

        coalesce(scd.brand,'Unknown') AS brand,

        toUInt8(ifNull(scd.instamart_active,0)) AS active,

        coalesce(scd.Brand_Internal_SKU_Code,'Unknown') AS brand_internal_sku_code,

        coalesce(irb.warehouseName,'Unknown') AS backendfacilityname,

        0.0 AS frontendinvqty,

        toFloat64(ifNull(irb.warehouseSOH,0)) AS backendinvqty,

        toFloat64(ifNull(irb.warehouseSOH,0)) AS total_quantity,

        toDate(irb.reportDate) AS reportdate

    FROM InstamartReportInventory irb

    LEFT JOIN Static_City_Mapping scm
        ON irb.warehouseName = scm.Instamart
       AND irb.organizationId = scm.organizationid

    LEFT JOIN Static_Combined_Data scd
        ON irb.itemCode = scd.Instamart_SKU_Code
       AND irb.organizationId = scd.organizationid
)

SELECT
    itemid,
    backendfacilityname,
    reportdate,

    any(itemname) AS itemname,
    any(productid) AS productid,
    any(category) AS category,
    any(subcategory) AS subcategory,
    any(brand) AS brand,
    any(organizationid) AS organizationid,
    any(brand_internal_sku_code) AS brand_internal_sku_code,
    any(brands_choice) AS brands_choice,
    any(active) AS active,

    sum(total_quantity) AS total_quantity,
    sum(frontendinvqty) AS frontendinvqty,
    sum(backendinvqty) AS backendinvqty,

    greatest(
        lagInFrame(sum(backendinvqty))
        OVER (PARTITION BY itemid, backendfacilityname ORDER BY reportdate)
        - sum(backendinvqty),
        0
    ) AS run_rate_backend,

    greatest(
        lagInFrame(sum(total_quantity))
        OVER (PARTITION BY itemid, backendfacilityname ORDER BY reportdate)
        - sum(total_quantity),
        0
    ) AS run_rate_total

FROM inventory_data

GROUP BY
    itemid,
    backendfacilityname,
    reportdate;