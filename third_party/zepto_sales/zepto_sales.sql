-- ============================================================
-- zepto_sales
-- Stores per-day sales & volume derived from inventory changes
-- Source: ZeptoProductMerchant_daily (1 row per cdate/productid/merchantid)
-- ============================================================

CREATE TABLE IF NOT EXISTS Zepto_Sales
(
    cdate         Date,
    productid     String,
    merchantid    String,
    categoryid    String,
    subcategoryid String,
    brandid       String,
    inventory     Float64,
    price         Float64,
    sales         Float64,
    volume        Float64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(cdate)
ORDER BY (cdate, merchantid, productid);


-- ============================================================
-- DAILY INCREMENTAL INSERT
-- Reads max(cdate) already in Zepto_Sales, inserts only new days.
-- Schedule this to run every day after ZeptoProductMerchant_daily
-- has been refreshed.
-- ============================================================

INSERT INTO Zepto_Sales
WITH

-- Step 1: find the latest date already loaded
latest AS (
    SELECT coalesce(max(cdate), toDate('2025-01-01')) AS latest_date
    FROM Zepto_Sales
),

-- Step 2: compute prev_inventory (previous day) via lagInFrame on cdate
inventory_changes AS (
    SELECT
        d.cdate,
        d.productid,
        d.merchantid,
        d.categoryid,
        d.subcategoryid,
        d.brandid,
        d.inventory,
        d.price,
        lagInFrame(d.inventory, 1, 0) OVER (
            PARTITION BY d.productid, d.merchantid
            ORDER BY d.cdate
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS prev_inventory
    FROM ZeptoProductMerchant_daily AS d
    CROSS JOIN latest
    WHERE d.cdate > latest.latest_date
)

-- Step 3: compute sales & volume
SELECT
    cdate,
    productid,
    merchantid,
    categoryid,
    subcategoryid,
    brandid,
    inventory,
    price,
    greatest(0, (prev_inventory - inventory) * price) AS sales,
    inventory * price                                  AS volume
FROM inventory_changes;

