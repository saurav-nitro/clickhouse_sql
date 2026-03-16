CREATE TABLE blinkit_sales
(
    merchantid UInt32,
    productid UInt32,
    batchid String,
    categoryid Int32,
    subcategoryid Int32,
    brandid String,
    inventory Float64,
    price Float64,
    createdat DateTime64(3),
    cdate Date,
    sales Float64,
    volume Float64
)
ENGINE = ReplacingMergeTree(createdat)
ORDER BY (productid, merchantid, cdate, batchid);


CREATE MATERIALIZED VIEW blinkit_sales_mv
TO blinkit_sales
AS
WITH inventory_changes AS (
    SELECT
        pm.merchantSalesId AS merchantid,
        pm.productId AS productid,
        pm.batchId AS batchid,
        pm.inventory,
        pm.price,
        pm.createdAt AS createdat,
        p.categoryId AS categoryid,
        p.subCategoryId AS subcategoryid,
        p.brandId AS brandid,
        toDate(pm.createdAt) AS cdate,
        lagInFrame(pm.inventory) OVER (
            PARTITION BY pm.merchantSalesId, pm.productId, pm.batchId
            ORDER BY pm.createdAt
        ) AS prev_inventory
    FROM blinkitproductmerchant pm
    INNER JOIN blinkitproduct p ON pm.productId = p.id
    WHERE p.isCombo = 0
      AND pm.createdAt >= (SELECT coalesce(max(cdate), toDate('2026-01-01')) FROM blinkit_sales)
)
SELECT
    merchantid,
    productid,
    batchid,
    any(categoryid) AS categoryid,
    any(subcategoryid) AS subcategoryid,
    any(brandid) AS brandid,
    any(inventory) AS inventory,
    any(price) AS price,
    max(createdat) AS createdat,
    any(cdate) AS cdate,
    sum(greatest(0, (prev_inventory - inventory) * price)) AS sales,
    sum(inventory * price) AS volume
FROM inventory_changes
WHERE createdat >= (SELECT coalesce(max(createdat), toDateTime64('2026-01-01 00:00:00', 3)) FROM blinkit_sales)
GROUP BY
    merchantid,
    productid,
    batchid;



SELECT
    pm.merchantSalesId AS merchantid,
    pm.productId AS productid,
    pm.batchId AS batchid,

    p.categoryId AS categoryid,
    p.subCategoryId AS subcategoryid,
    p.brandId AS brandid,

    pm.inventory,
    pm.price,
    pm.createdAt AS createdat,
    toDate(pm.createdAt) AS cdate,

    greatest(
        0,
        (lagInFrame(pm.inventory)
            OVER (
                PARTITION BY pm.merchantSalesId, pm.productId, pm.batchId
                ORDER BY pm.createdAt
            ) - pm.inventory
        ) * pm.price
    ) AS sales,

    pm.inventory * pm.price AS volume

FROM BlinkitProductMerchant pm
INNER JOIN BlinkitProduct p
    ON pm.productId = p.id
WHERE p.isCombo = 0;    

