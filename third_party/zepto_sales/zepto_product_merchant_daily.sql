-- ============================================================
-- ZeptoProductMerchant_daily
-- Stores one aggregated row per (cdate, productid, merchantid)
-- Table is truncated and reloaded every day with fresh data
-- ============================================================

CREATE TABLE ZeptoProductMerchant_daily
(
    cdate         Date,
    productid     String,
    merchantid    String,

    brandid       String,
    categoryid    String,
    subcategoryid String,
    cityname      String,
    iscombo       String,
    unit          String,

    mrp           Float64,
    inventory     Float64,
    discount      Float64,
    price         Float64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(cdate)
ORDER BY (cdate, productid, merchantid);


-- ============================================================
-- STEP 1 — Truncate the table (run this every day before load)
-- ============================================================

TRUNCATE TABLE ZeptoProductMerchant_daily;


-- ============================================================
-- STEP 2 — Load today's data  (run after the truncate above)
-- ============================================================

INSERT INTO ZeptoProductMerchant_daily
SELECT
    toDate(pm.createdAt)                                               AS cdate,
    toString(pm.productId)                                             AS productid,
    toString(pm.merchantId)                                            AS merchantid,

    anyIf(toString(p.brandName),     p.brandName     IS NOT NULL)     AS brandid,
    anyIf(toString(p.categoryId),    p.categoryId    IS NOT NULL)     AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL)     AS subcategoryid,
    anyIf(m.cityName,                m.cityName      IS NOT NULL)     AS cityname,
    anyIf(toString(p.isCombo),       p.isCombo       IS NOT NULL)     AS iscombo,
    anyIf(toString(p.unit),          p.unit          IS NOT NULL)     AS unit,

    max(toFloat64(p.mrp))            AS mrp,
    avg(toFloat64(pm.inventory))     AS inventory,
    avg(toFloat64(pm.discount))      AS discount,
    avg(toFloat64(pm.price))         AS price

FROM ZeptoProductMerchant AS pm
JOIN ZeptoProduct  AS p ON pm.productId  = p.id
JOIN ZeptoMerchant AS m ON pm.merchantId = m.id

WHERE p.isCombo = 'false'
  AND toDate(pm.createdAt) = today()   -- always loads only today's data

GROUP BY
    cdate,
    productid,
    merchantid;


-- ============================================================
-- ONE-TIME BACK-FILL — last 15 days in batch (1 day at a time)
-- Run each INSERT separately to avoid memory issues
-- ============================================================

-- Day 1
INSERT INTO ZeptoProductMerchant_daily
SELECT toDate(pm.createdAt) AS cdate, toString(pm.productId) AS productid, toString(pm.merchantId) AS merchantid,
    anyIf(toString(p.brandName), p.brandName IS NOT NULL) AS brandid, anyIf(toString(p.categoryId), p.categoryId IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid, anyIf(m.cityName, m.cityName IS NOT NULL) AS cityname,
    anyIf(toString(p.isCombo), p.isCombo IS NOT NULL) AS iscombo, anyIf(toString(p.unit), p.unit IS NOT NULL) AS unit,
    max(toFloat64(p.mrp)) AS mrp, avg(toFloat64(pm.inventory)) AS inventory, avg(toFloat64(pm.discount)) AS discount, avg(toFloat64(pm.price)) AS price
FROM ZeptoProductMerchant AS pm JOIN ZeptoProduct AS p ON pm.productId = p.id JOIN ZeptoMerchant AS m ON pm.merchantId = m.id
WHERE p.isCombo = 'false' AND toDate(pm.createdAt) = today() - 1
GROUP BY cdate, productid, merchantid;

-- Day 2
INSERT INTO ZeptoProductMerchant_daily
SELECT toDate(pm.createdAt) AS cdate, toString(pm.productId) AS productid, toString(pm.merchantId) AS merchantid,
    anyIf(toString(p.brandName), p.brandName IS NOT NULL) AS brandid, anyIf(toString(p.categoryId), p.categoryId IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid, anyIf(m.cityName, m.cityName IS NOT NULL) AS cityname,
    anyIf(toString(p.isCombo), p.isCombo IS NOT NULL) AS iscombo, anyIf(toString(p.unit), p.unit IS NOT NULL) AS unit,
    max(toFloat64(p.mrp)) AS mrp, avg(toFloat64(pm.inventory)) AS inventory, avg(toFloat64(pm.discount)) AS discount, avg(toFloat64(pm.price)) AS price
FROM ZeptoProductMerchant AS pm JOIN ZeptoProduct AS p ON pm.productId = p.id JOIN ZeptoMerchant AS m ON pm.merchantId = m.id
WHERE p.isCombo = 'false' AND toDate(pm.createdAt) = today() - 2
GROUP BY cdate, productid, merchantid;

-- Day 3
INSERT INTO ZeptoProductMerchant_daily
SELECT toDate(pm.createdAt) AS cdate, toString(pm.productId) AS productid, toString(pm.merchantId) AS merchantid,
    anyIf(toString(p.brandName), p.brandName IS NOT NULL) AS brandid, anyIf(toString(p.categoryId), p.categoryId IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid, anyIf(m.cityName, m.cityName IS NOT NULL) AS cityname,
    anyIf(toString(p.isCombo), p.isCombo IS NOT NULL) AS iscombo, anyIf(toString(p.unit), p.unit IS NOT NULL) AS unit,
    max(toFloat64(p.mrp)) AS mrp, avg(toFloat64(pm.inventory)) AS inventory, avg(toFloat64(pm.discount)) AS discount, avg(toFloat64(pm.price)) AS price
FROM ZeptoProductMerchant AS pm JOIN ZeptoProduct AS p ON pm.productId = p.id JOIN ZeptoMerchant AS m ON pm.merchantId = m.id
WHERE p.isCombo = 'false' AND toDate(pm.createdAt) = today() - 3
GROUP BY cdate, productid, merchantid;

-- Day 4
INSERT INTO ZeptoProductMerchant_daily
SELECT toDate(pm.createdAt) AS cdate, toString(pm.productId) AS productid, toString(pm.merchantId) AS merchantid,
    anyIf(toString(p.brandName), p.brandName IS NOT NULL) AS brandid, anyIf(toString(p.categoryId), p.categoryId IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid, anyIf(m.cityName, m.cityName IS NOT NULL) AS cityname,
    anyIf(toString(p.isCombo), p.isCombo IS NOT NULL) AS iscombo, anyIf(toString(p.unit), p.unit IS NOT NULL) AS unit,
    max(toFloat64(p.mrp)) AS mrp, avg(toFloat64(pm.inventory)) AS inventory, avg(toFloat64(pm.discount)) AS discount, avg(toFloat64(pm.price)) AS price
FROM ZeptoProductMerchant AS pm JOIN ZeptoProduct AS p ON pm.productId = p.id JOIN ZeptoMerchant AS m ON pm.merchantId = m.id
WHERE p.isCombo = 'false' AND toDate(pm.createdAt) = today() - 4
GROUP BY cdate, productid, merchantid;

-- Day 5
INSERT INTO ZeptoProductMerchant_daily
SELECT toDate(pm.createdAt) AS cdate, toString(pm.productId) AS productid, toString(pm.merchantId) AS merchantid,
    anyIf(toString(p.brandName), p.brandName IS NOT NULL) AS brandid, anyIf(toString(p.categoryId), p.categoryId IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid, anyIf(m.cityName, m.cityName IS NOT NULL) AS cityname,
    anyIf(toString(p.isCombo), p.isCombo IS NOT NULL) AS iscombo, anyIf(toString(p.unit), p.unit IS NOT NULL) AS unit,
    max(toFloat64(p.mrp)) AS mrp, avg(toFloat64(pm.inventory)) AS inventory, avg(toFloat64(pm.discount)) AS discount, avg(toFloat64(pm.price)) AS price
FROM ZeptoProductMerchant AS pm JOIN ZeptoProduct AS p ON pm.productId = p.id JOIN ZeptoMerchant AS m ON pm.merchantId = m.id
WHERE p.isCombo = 'false' AND toDate(pm.createdAt) = today() - 5
GROUP BY cdate, productid, merchantid;

-- Day 6
INSERT INTO ZeptoProductMerchant_daily
SELECT toDate(pm.createdAt) AS cdate, toString(pm.productId) AS productid, toString(pm.merchantId) AS merchantid,
    anyIf(toString(p.brandName), p.brandName IS NOT NULL) AS brandid, anyIf(toString(p.categoryId), p.categoryId IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid, anyIf(m.cityName, m.cityName IS NOT NULL) AS cityname,
    anyIf(toString(p.isCombo), p.isCombo IS NOT NULL) AS iscombo, anyIf(toString(p.unit), p.unit IS NOT NULL) AS unit,
    max(toFloat64(p.mrp)) AS mrp, avg(toFloat64(pm.inventory)) AS inventory, avg(toFloat64(pm.discount)) AS discount, avg(toFloat64(pm.price)) AS price
FROM ZeptoProductMerchant AS pm JOIN ZeptoProduct AS p ON pm.productId = p.id JOIN ZeptoMerchant AS m ON pm.merchantId = m.id
WHERE p.isCombo = 'false' AND toDate(pm.createdAt) = today() - 6
GROUP BY cdate, productid, merchantid;

-- Day 7
INSERT INTO ZeptoProductMerchant_daily
SELECT toDate(pm.createdAt) AS cdate, toString(pm.productId) AS productid, toString(pm.merchantId) AS merchantid,
    anyIf(toString(p.brandName), p.brandName IS NOT NULL) AS brandid, anyIf(toString(p.categoryId), p.categoryId IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid, anyIf(m.cityName, m.cityName IS NOT NULL) AS cityname,
    anyIf(toString(p.isCombo), p.isCombo IS NOT NULL) AS iscombo, anyIf(toString(p.unit), p.unit IS NOT NULL) AS unit,
    max(toFloat64(p.mrp)) AS mrp, avg(toFloat64(pm.inventory)) AS inventory, avg(toFloat64(pm.discount)) AS discount, avg(toFloat64(pm.price)) AS price
FROM ZeptoProductMerchant AS pm JOIN ZeptoProduct AS p ON pm.productId = p.id JOIN ZeptoMerchant AS m ON pm.merchantId = m.id
WHERE p.isCombo = 'false' AND toDate(pm.createdAt) = today() - 7
GROUP BY cdate, productid, merchantid;

-- Day 8
INSERT INTO ZeptoProductMerchant_daily
SELECT toDate(pm.createdAt) AS cdate, toString(pm.productId) AS productid, toString(pm.merchantId) AS merchantid,
    anyIf(toString(p.brandName), p.brandName IS NOT NULL) AS brandid, anyIf(toString(p.categoryId), p.categoryId IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid, anyIf(m.cityName, m.cityName IS NOT NULL) AS cityname,
    anyIf(toString(p.isCombo), p.isCombo IS NOT NULL) AS iscombo, anyIf(toString(p.unit), p.unit IS NOT NULL) AS unit,
    max(toFloat64(p.mrp)) AS mrp, avg(toFloat64(pm.inventory)) AS inventory, avg(toFloat64(pm.discount)) AS discount, avg(toFloat64(pm.price)) AS price
FROM ZeptoProductMerchant AS pm JOIN ZeptoProduct AS p ON pm.productId = p.id JOIN ZeptoMerchant AS m ON pm.merchantId = m.id
WHERE p.isCombo = 'false' AND toDate(pm.createdAt) = today() - 8
GROUP BY cdate, productid, merchantid;

-- Day 9
INSERT INTO ZeptoProductMerchant_daily
SELECT toDate(pm.createdAt) AS cdate, toString(pm.productId) AS productid, toString(pm.merchantId) AS merchantid,
    anyIf(toString(p.brandName), p.brandName IS NOT NULL) AS brandid, anyIf(toString(p.categoryId), p.categoryId IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid, anyIf(m.cityName, m.cityName IS NOT NULL) AS cityname,
    anyIf(toString(p.isCombo), p.isCombo IS NOT NULL) AS iscombo, anyIf(toString(p.unit), p.unit IS NOT NULL) AS unit,
    max(toFloat64(p.mrp)) AS mrp, avg(toFloat64(pm.inventory)) AS inventory, avg(toFloat64(pm.discount)) AS discount, avg(toFloat64(pm.price)) AS price
FROM ZeptoProductMerchant AS pm JOIN ZeptoProduct AS p ON pm.productId = p.id JOIN ZeptoMerchant AS m ON pm.merchantId = m.id
WHERE p.isCombo = 'false' AND toDate(pm.createdAt) = today() - 9
GROUP BY cdate, productid, merchantid;

-- Day 10
INSERT INTO ZeptoProductMerchant_daily
SELECT toDate(pm.createdAt) AS cdate, toString(pm.productId) AS productid, toString(pm.merchantId) AS merchantid,
    anyIf(toString(p.brandName), p.brandName IS NOT NULL) AS brandid, anyIf(toString(p.categoryId), p.categoryId IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid, anyIf(m.cityName, m.cityName IS NOT NULL) AS cityname,
    anyIf(toString(p.isCombo), p.isCombo IS NOT NULL) AS iscombo, anyIf(toString(p.unit), p.unit IS NOT NULL) AS unit,
    max(toFloat64(p.mrp)) AS mrp, avg(toFloat64(pm.inventory)) AS inventory, avg(toFloat64(pm.discount)) AS discount, avg(toFloat64(pm.price)) AS price
FROM ZeptoProductMerchant AS pm JOIN ZeptoProduct AS p ON pm.productId = p.id JOIN ZeptoMerchant AS m ON pm.merchantId = m.id
WHERE p.isCombo = 'false' AND toDate(pm.createdAt) = today() - 10
GROUP BY cdate, productid, merchantid;

-- Day 11
INSERT INTO ZeptoProductMerchant_daily
SELECT toDate(pm.createdAt) AS cdate, toString(pm.productId) AS productid, toString(pm.merchantId) AS merchantid,
    anyIf(toString(p.brandName), p.brandName IS NOT NULL) AS brandid, anyIf(toString(p.categoryId), p.categoryId IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid, anyIf(m.cityName, m.cityName IS NOT NULL) AS cityname,
    anyIf(toString(p.isCombo), p.isCombo IS NOT NULL) AS iscombo, anyIf(toString(p.unit), p.unit IS NOT NULL) AS unit,
    max(toFloat64(p.mrp)) AS mrp, avg(toFloat64(pm.inventory)) AS inventory, avg(toFloat64(pm.discount)) AS discount, avg(toFloat64(pm.price)) AS price
FROM ZeptoProductMerchant AS pm JOIN ZeptoProduct AS p ON pm.productId = p.id JOIN ZeptoMerchant AS m ON pm.merchantId = m.id
WHERE p.isCombo = 'false' AND toDate(pm.createdAt) = today() - 11
GROUP BY cdate, productid, merchantid;

-- Day 12
INSERT INTO ZeptoProductMerchant_daily
SELECT toDate(pm.createdAt) AS cdate, toString(pm.productId) AS productid, toString(pm.merchantId) AS merchantid,
    anyIf(toString(p.brandName), p.brandName IS NOT NULL) AS brandid, anyIf(toString(p.categoryId), p.categoryId IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid, anyIf(m.cityName, m.cityName IS NOT NULL) AS cityname,
    anyIf(toString(p.isCombo), p.isCombo IS NOT NULL) AS iscombo, anyIf(toString(p.unit), p.unit IS NOT NULL) AS unit,
    max(toFloat64(p.mrp)) AS mrp, avg(toFloat64(pm.inventory)) AS inventory, avg(toFloat64(pm.discount)) AS discount, avg(toFloat64(pm.price)) AS price
FROM ZeptoProductMerchant AS pm JOIN ZeptoProduct AS p ON pm.productId = p.id JOIN ZeptoMerchant AS m ON pm.merchantId = m.id
WHERE p.isCombo = 'false' AND toDate(pm.createdAt) = today() - 12
GROUP BY cdate, productid, merchantid;

-- Day 13
INSERT INTO ZeptoProductMerchant_daily
SELECT toDate(pm.createdAt) AS cdate, toString(pm.productId) AS productid, toString(pm.merchantId) AS merchantid,
    anyIf(toString(p.brandName), p.brandName IS NOT NULL) AS brandid, anyIf(toString(p.categoryId), p.categoryId IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid, anyIf(m.cityName, m.cityName IS NOT NULL) AS cityname,
    anyIf(toString(p.isCombo), p.isCombo IS NOT NULL) AS iscombo, anyIf(toString(p.unit), p.unit IS NOT NULL) AS unit,
    max(toFloat64(p.mrp)) AS mrp, avg(toFloat64(pm.inventory)) AS inventory, avg(toFloat64(pm.discount)) AS discount, avg(toFloat64(pm.price)) AS price
FROM ZeptoProductMerchant AS pm JOIN ZeptoProduct AS p ON pm.productId = p.id JOIN ZeptoMerchant AS m ON pm.merchantId = m.id
WHERE p.isCombo = 'false' AND toDate(pm.createdAt) = today() - 13
GROUP BY cdate, productid, merchantid;

-- Day 14
INSERT INTO ZeptoProductMerchant_daily
SELECT toDate(pm.createdAt) AS cdate, toString(pm.productId) AS productid, toString(pm.merchantId) AS merchantid,
    anyIf(toString(p.brandName), p.brandName IS NOT NULL) AS brandid, anyIf(toString(p.categoryId), p.categoryId IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid, anyIf(m.cityName, m.cityName IS NOT NULL) AS cityname,
    anyIf(toString(p.isCombo), p.isCombo IS NOT NULL) AS iscombo, anyIf(toString(p.unit), p.unit IS NOT NULL) AS unit,
    max(toFloat64(p.mrp)) AS mrp, avg(toFloat64(pm.inventory)) AS inventory, avg(toFloat64(pm.discount)) AS discount, avg(toFloat64(pm.price)) AS price
FROM ZeptoProductMerchant AS pm JOIN ZeptoProduct AS p ON pm.productId = p.id JOIN ZeptoMerchant AS m ON pm.merchantId = m.id
WHERE p.isCombo = 'false' AND toDate(pm.createdAt) = today() - 14
GROUP BY cdate, productid, merchantid;

-- Day 15
INSERT INTO ZeptoProductMerchant_daily
SELECT toDate(pm.createdAt) AS cdate, toString(pm.productId) AS productid, toString(pm.merchantId) AS merchantid,
    anyIf(toString(p.brandName), p.brandName IS NOT NULL) AS brandid, anyIf(toString(p.categoryId), p.categoryId IS NOT NULL) AS categoryid,
    anyIf(toString(p.subCategoryId), p.subCategoryId IS NOT NULL) AS subcategoryid, anyIf(m.cityName, m.cityName IS NOT NULL) AS cityname,
    anyIf(toString(p.isCombo), p.isCombo IS NOT NULL) AS iscombo, anyIf(toString(p.unit), p.unit IS NOT NULL) AS unit,
    max(toFloat64(p.mrp)) AS mrp, avg(toFloat64(pm.inventory)) AS inventory, avg(toFloat64(pm.discount)) AS discount, avg(toFloat64(pm.price)) AS price
FROM ZeptoProductMerchant AS pm JOIN ZeptoProduct AS p ON pm.productId = p.id JOIN ZeptoMerchant AS m ON pm.merchantId = m.id
WHERE p.isCombo = 'false' AND toDate(pm.createdAt) = today() - 15
GROUP BY cdate, productid, merchantid;
