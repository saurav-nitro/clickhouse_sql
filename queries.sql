SELECT
    COALESCE(brand, 'Unknown') AS brand,
    reportdate AS date,
    COALESCE(SUM(gmv), 0) AS total_sales
FROM Blinkit_Combined_Sales_Report
WHERE organizationid = '19098698-bbea-4314-9077-f5c846973d73'
AND reportdate BETWEEN '2026-02-02' AND '2026-02-31'
GROUP BY brand, reportdate
ORDER BY reportdate;


SELECT DISTINCT
    sub__category AS subcategory
FROM static_combined_data
WHERE organizationid = '19098698-bbea-4314-9077-f5c846973d73';


SELECT
    itemid,
    COALESCE(MIN(productid), 'productid') AS productid,
    COALESCE(MIN(category), 'Uncategorised') AS category,
    COALESCE(MIN(subcategory), 'Uncategorised') AS subcategory,
    COALESCE(MIN(itemname), 'Unknown') AS itemname,
    COALESCE(SUM(gmv), 0) AS mrp,
    COALESCE(SUM(qty), 0) AS qty,
    COALESCE(SUM(taxes_paid), 0) AS taxes,
    COALESCE(SUM(net), 0) AS net_sales,
    COALESCE(SUM(gmv * COALESCE(margin,0) / 100.0),0) AS platform_margin
FROM Blinkit_Combined_Sales_Report
WHERE organizationid = '19098698-bbea-4314-9077-f5c846973d73'
AND reportdate BETWEEN '2026-02-02' AND '2026-02-31'
GROUP BY itemid
ORDER BY category, subcategory, itemname;


SELECT
    itemid,
    COALESCE(MIN(productid), 'productid') AS productid,
    COALESCE(MIN(category), 'Uncategorised') AS category,
    COALESCE(MIN(subcategory), 'Uncategorised') AS subcategory,
    COALESCE(MIN(itemname), 'Unknown') AS itemname,
    COALESCE(SUM(gmv), 0) AS mrp,
    COALESCE(SUM(qty), 0) AS qty,
    COALESCE(SUM(taxes_paid), 0) AS taxes,
    COALESCE(SUM(net), 0) AS net_sales,
    COALESCE(SUM(gmv * COALESCE(margin,0) / 100.0),0) AS platform_margin
FROM Blinkit_Combined_Sales_Report
WHERE organizationid = '19098698-bbea-4314-9077-f5c846973d73'
AND reportdate BETWEEN addMonths('2026-02-02',-1) AND addMonths('2026-02-31',-1)
GROUP BY itemid
ORDER BY category, subcategory, itemname;


SELECT
    MIN(brands_choice) AS city
FROM Blinkit_Combined_Sales_Report
WHERE organizationid = '19098698-bbea-4314-9077-f5c846973d73'
AND brands_choice IS NOT NULL
GROUP BY brands_choice
ORDER BY city;


SELECT
    COALESCE(brands_choice,'Unknown') AS city,
    COALESCE(category,'Unknown') AS category,
    COALESCE(SUM(gmv),0) AS total_mrp,
    COALESCE(SUM(qty),0) AS total_qty
FROM Blinkit_Combined_Sales_Report
WHERE organizationid = '19098698-bbea-4314-9077-f5c846973d73'
AND reportdate BETWEEN '2026-02-02' AND '2026-02-31'
GROUP BY brands_choice, category
ORDER BY brands_choice, category;


SELECT
    COALESCE(brands_choice,'Unknown') AS city,
    COALESCE(category,'Unknown') AS category,
    reportdate,
    COALESCE(SUM(gmv),0) AS total_mrp
FROM Blinkit_Combined_Sales_Report
WHERE organizationid = '19098698-bbea-4314-9077-f5c846973d73'
AND reportdate BETWEEN '2026-02-02' AND '2026-02-31'
GROUP BY brands_choice, category, reportdate
ORDER BY brands_choice, category, reportdate;


SELECT
    COALESCE(brands_choice,'Unknown') AS city,
    COALESCE(SUM(gmv),0) AS sales
FROM Blinkit_Combined_Sales_Report
WHERE organizationid = '19098698-bbea-4314-9077-f5c846973d73'
AND reportdate BETWEEN '2026-02-02' AND '2026-02-20'
GROUP BY brands_choice;


SELECT
    COALESCE(brands_choice,'Unknown') AS city,
    COALESCE(SUM(gmv),0) AS sales
FROM Blinkit_Combined_Sales_Report
WHERE organizationid = '19098698-bbea-4314-9077-f5c846973d73'
AND reportdate BETWEEN '2024-12-02' AND '2024-12-20'
GROUP BY brands_choice;


SELECT
    itemid,
    MIN(productid) AS productid,
    COALESCE(MIN(itemname),'Unknown') AS itemname,
    COALESCE(SUM(gmv),0) AS sales
FROM Blinkit_Combined_Sales_Report
WHERE organizationid = '19098698-bbea-4314-9077-f5c846973d73'
AND reportdate BETWEEN '2026-02-02' AND '2026-02-20'
GROUP BY itemid
ORDER BY itemid;


SELECT
    reportdate,
    COALESCE(category,'Uncategorised') AS category,
    COALESCE(SUM(gmv),0) AS total_mrp,
    COALESCE(SUM(qty),0) AS total_qty,
    COALESCE(SUM(net),0) AS net_sales
FROM Blinkit_Combined_Sales_Report
WHERE organizationid = '19098698-bbea-4314-9077-f5c846973d73'
AND reportdate BETWEEN '2026-02-02' AND '2026-02-31'
GROUP BY reportdate, category
ORDER BY reportdate, category;


SELECT
    reportdate,
    COALESCE(category,'Uncategorised') AS category,
    COALESCE(SUM(gmv),0) AS total_mrp,
    COALESCE(SUM(qty),0) AS total_qty,
    COALESCE(SUM(net),0) AS net_sales
FROM Blinkit_Combined_Sales_Report
WHERE organizationid = '19098698-bbea-4314-9077-f5c846973d73'
AND reportdate BETWEEN addMonths('2026-02-02',-1) AND addMonths('2026-02-31',-1)
GROUP BY reportdate, category
ORDER BY reportdate, category;



SELECT
    DATE(date) AS date,
    MIN(totaltarget) AS target
FROM orgrevenuetarget
WHERE organizationid = '19098698-bbea-4314-9077-f5c846973d73'
AND platformid = 'blinkit'
AND date BETWEEN '2026-02-02' AND '2026-02-31'
GROUP BY date;



CREATE MATERIALIZED VIEW
  Blinkit_Combined_Sales_Report DISTKEY (itemid) SORTKEY (itemid, reportdate) AS
SELECT
  sr.itemid,
  scm.brand_s_choice                         AS brands_choice,
  sr.reportdate,
  MIN(scd.blinkit_prid)                      AS productid,
  MIN(sr.itemname)                           AS itemname,
  MIN(scd.category)                          AS category,
  MIN(scd.sub__category)                     AS subcategory,
  MIN(sr.organizationid)                     AS organizationid,
  MIN(scd.brand_internal_sku_code)           AS brand_internal_sku_code,
  MIN(scd.cogs)                              AS cogs,
  MIN(scd.blinkit_margin)                    AS margin,
  MIN(scd.taxes)                             AS tax_rate,
  MIN(scd.blinkit_active)                    AS active,
  MIN(scd.brand)                             AS brand,
  SUM(sr.qtySold::DOUBLE PRECISION)          AS qty,
  MIN(sr.mrp::DOUBLE PRECISION / sr.qtySold) AS mrp,
  SUM(sr.mrp)                                AS gmv,
  SUM(
    sr.mrp * (1 - scd.blinkit_margin / 100.0) / (1 + scd.taxes / 100.0)
  ) AS net,
  SUM(
    sr.mrp * (1 - scd.blinkit_margin / 100.0) / (1 + scd.taxes / 100.0) * (scd.taxes / 100.0)
  ) AS taxes_paid
FROM
  blinkitreportsales AS sr
  LEFT JOIN static_city_mapping AS scm ON (
    sr.cityname = scm.blinkit
    AND sr.organizationid = scm.organizationid
  )
  LEFT JOIN static_combined_data AS scd ON (
    sr.itemid = scd.blinkit_sku_code
    AND sr.organizationid = scd.organizationid
  )
GROUP BY
  itemid,
  brands_choice,
  reportdate;



SELECT
    sr.itemId,
    scm.Brand_s_Choice AS brands_choice,
    sr.reportDate,
    sr.organizationId,

    anyState(scd.category) AS category,
    anyState(scd.sub__category) AS subcategory,
    anyState(sr.itemname) AS itemname,
    anyState(scd.blinkit_prid) AS productid,
    anyState(scd.brand_internal_sku_code) AS brand_internal_sku_code,
    anyState(scd.brand) AS brand,

    anyState(scd.blinkit_margin) AS margin,
    anyState(scd.taxes) AS tax_rate,
    anyState(scd.cogs) AS cogs,
    anyState(scd.blinkit_active) AS active,

    sumState(toFloat64(sr.qtySold)) AS qty,
    anyState(toFloat64(sr.mrp) / sr.qtySold) AS mrp,
    sumState(sr.mrp) AS gmv,

    sumState(
        sr.mrp * (1 - scd.blinkit_margin / 100) / (1 + scd.taxes / 100)
    ) AS net,

    sumState(
        sr.mrp * (1 - scd.blinkit_margin / 100) / (1 + scd.taxes / 100)
        * (scd.taxes / 100)
    ) AS taxes_paid

FROM BlinkitReportSales sr
LEFT JOIN Static_City_Mapping scm
    ON sr.cityname = scm.blinkit
    AND sr.organizationid = scm.organizationid

LEFT JOIN Static_Combined_Data scd
    ON sr.itemid = scd.blinkit_sku_code
    AND sr.organizationid = scd.organizationid

GROUP BY
    sr.itemid,
    brands_choice,
    sr.reportdate,
    sr.organizationid;