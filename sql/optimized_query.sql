-- First, create materialized CTEs for frequently used subqueries
WITH MATERIALIZED_PERIODS AS (
    SELECT DISTINCT ap.ending::date AS period_end_date, ap.accounting_period_id
    FROM ods.ns_accounting_periods ap
    WHERE ap.ending::date > '2024-01-01'
),

MATERIALIZED_TRANSACTIONS AS (
    SELECT 
        tr.INCREMENT_ID,
        tr.ORDER_ID,
        tr.MAGENTO_LOCATION_ID,
        tr.TRAN_TYPE,
        tr.TRAN_SUB_TYPE,
        tr.SKU,
        tr.TRAN_GL_DATE,
        tr.TRAN_QTY,
        tr.TRAN_AMT,
        tr.JE_MAP_ID,
        tr.LOCATION_ID,
        tr.GROUP_ID,
        tr.TRAN_ID,
        tr.TRAN_SUB_TYPE_ID,
        tr.ORDER_LINE_ID,
        tr.PARENT_ITEM_ID,
        COALESCE(loc.FC_NAME, maxl.maxfc) AS locname,
        CASE 
            WHEN mp.DEBIT_ACCOUNT_NUMBER = '22705' THEN mp.CREDIT_ACCOUNT_NUMBER 
            ELSE mp.DEBIT_ACCOUNT_NUMBER 
        END AS acctnumber,
        admp.actnum,
        CASE 
            WHEN tr.tran_sub_type = 'Product' THEN tr.tran_qty 
            ELSE 0 
        END AS quantity,
        CASE
            WHEN tr.tran_sub_type = 'Product' AND right(tr.sku,2) = '-G' THEN tr.tran_qty
            ELSE 0
        END AS gwpquantity,
        CASE
            WHEN tr.tran_sub_type = 'Product' AND right(tr.sku,2) != '-G' AND tr.tran_amt = 0 THEN tr.tran_qty
            ELSE 0
        END AS mgquantity,
        CASE
            WHEN tr.tran_sub_type = 'Product' AND right(tr.sku,2) != '-G' AND tr.tran_amt != 0 THEN tr.tran_qty
            ELSE 0
        END AS paidquantity,
        CASE WHEN tr.tran_sub_type = 'Product' AND pg.category_name = 'Dry' THEN tr.tran_qty ELSE 0 END AS dry_qty,
        CASE WHEN tr.tran_sub_type = 'Product' AND pg.category_name = 'Frozen' THEN tr.tran_qty ELSE 0 END AS frozen_qty,
        CASE WHEN tr.tran_sub_type = 'Product' AND pg.category_name = 'Fresh' THEN tr.tran_qty ELSE 0 END AS fresh_qty,
        CASE WHEN tr.tran_sub_type = 'Product' AND pg.category_name = 'Wine' THEN tr.tran_qty ELSE 0 END AS wine_qty,
        tr.tran_amt,
        tr.tran_cogs_amt
    FROM ods."TRANSACTIONS" tr
    LEFT JOIN (
        SELECT tr.INCREMENT_ID AS maxinc, max(ns.FC_NAME) AS maxfc  
        FROM ods."TRANSACTIONS" tr
        LEFT JOIN ods.NS_FC_XREF ns ON ns.FC_ID = tr.MAGENTO_LOCATION_ID 
        WHERE tr.TRAN_TYPE = 300 AND tr.TRAN_SUB_TYPE = 'Product' 
        GROUP BY tr.INCREMENT_ID
    ) maxl ON maxl.maxinc = tr.INCREMENT_ID 
    LEFT JOIN ods.V_GL_MAP_DETAIL mp ON mp.MAP_ID = tr.JE_MAP_ID 
    LEFT JOIN (
        SELECT mp.map_id, ns.accountnumber AS actnum 
        FROM ods.ADJUSTMENT_TYPE aa
        LEFT JOIN ods.GL_MAP_HEADER mp ON mp.ADJUSTMENT_ID = aa.ADJUSTMENT_ID 
        LEFT JOIN ods.NETSUITE_ACCOUNTS ns ON ns.ACCOUNT_ID = aa.ACCOUNT_ID 
        WHERE mp.ADJUSTMENT_ID IS NOT NULL
    ) admp ON admp.map_id = tr.je_map_id
    LEFT JOIN (
        SELECT DISTINCT tr.MAGENTO_LOCATION_ID AS locationid, ns.FC_NAME  
        FROM ods."TRANSACTIONS" tr
        LEFT JOIN ods.NS_FC_XREF ns ON ns.FC_ID = tr.MAGENTO_LOCATION_ID 
        WHERE tr.TRAN_TYPE = 300 AND tr.TRAN_SUB_TYPE = 'Product'
    ) loc ON loc.locationid = tr.MAGENTO_LOCATION_ID
    LEFT JOIN ods.gl_product_group_xref pg ON pg.location_id = tr.location_id AND pg.group_id = tr.group_id
    WHERE tr.TRAN_TYPE IN (300,390)
    AND EXISTS (
        SELECT 1 FROM MATERIALIZED_PERIODS mp 
        WHERE mp.period_end_date = tr.TRAN_GL_DATE::date
    )
),

MATERIALIZED_ORDER_METRICS AS (
    SELECT 
        period_end_date,
        increment_id,
        order_id,
        locname,
        CASE WHEN SUM(tran_amt) = 0 THEN 'Y' ELSE 'N' END AS zero_charge_order,
        SUM(quantity) AS "Units Shipped",
        SUM(gwpquantity) AS "GWP Units Shipped",
        SUM(mgquantity) AS "Non GWP Units Shipped",
        SUM(paidquantity) AS "Paid Units Shipped",
        SUM(dry_qty) AS "Dry Units Shipped",
        SUM(frozen_qty) AS "Frozen Units Shipped",
        SUM(fresh_qty) AS "Fresh Units Shipped",
        SUM(wine_qty) AS "Wine Units Shipped",
        SUM(CASE WHEN acctnumber = '40100' THEN tran_amt ELSE 0 END) AS "40100",
        SUM(CASE WHEN acctnumber = '40101' THEN tran_amt ELSE 0 END) AS "40101",
        SUM(CASE WHEN acctnumber = '40102' THEN tran_amt ELSE 0 END) AS "40102",
        SUM(CASE WHEN acctnumber = '41101' THEN -tran_amt ELSE 0 END) AS "41101",
        SUM(CASE WHEN acctnumber = '41106' THEN -tran_amt ELSE 0 END) AS "41106",
        SUM(CASE WHEN acctnumber = '41118' THEN -tran_amt ELSE 0 END) AS "41118",
        SUM(CASE WHEN acctnumber = '41102' THEN -tran_amt ELSE 0 END) AS "41102",
        SUM(CASE WHEN acctnumber = '41104' THEN -tran_amt ELSE 0 END) AS "41104",
        SUM(CASE WHEN acctnumber = '41107' THEN -tran_amt ELSE 0 END) AS "41107",
        SUM(CASE WHEN acctnumber = '41109' THEN -tran_amt ELSE 0 END) AS "41109",
        SUM(CASE WHEN acctnumber = '41112' THEN -tran_amt ELSE 0 END) AS "41112",
        SUM(CASE WHEN acctnumber = '41113' AND tr.TRAN_TYPE = 300 THEN -tran_amt
                 WHEN acctnumber = '41113' AND tr.TRAN_TYPE = 390 THEN tran_amt
                 ELSE 0 END) AS "41113",
        SUM(CASE WHEN acctnumber = '41116' THEN -tran_amt ELSE 0 END) AS "41116",
        SUM(CASE WHEN acctnumber = '41119' THEN -tran_amt ELSE 0 END) AS "41119",
        SUM(CASE WHEN acctnumber = '41227' THEN -tran_amt ELSE 0 END) AS "41227",
        SUM(CASE WHEN acctnumber = '41103' THEN -tran_amt ELSE 0 END) AS "41103",
        SUM(CASE WHEN acctnumber = '41108' THEN -tran_amt ELSE 0 END) AS "41108",
        SUM(CASE WHEN acctnumber = '41117' THEN -tran_amt ELSE 0 END) AS "41117",
        SUM(CASE WHEN acctnumber = '40115' THEN tran_amt ELSE 0 END) AS "40115",
        SUM(CASE WHEN acctnumber = '40116' THEN tran_amt ELSE 0 END) AS "40116",
        SUM(CASE WHEN actnum = '50100' THEN tran_cogs_amt ELSE 0 END) AS "50100",
        SUM(CASE WHEN actnum = '50101' THEN tran_cogs_amt ELSE 0 END) AS "50101",
        SUM(CASE WHEN actnum = '51039' THEN tran_cogs_amt ELSE 0 END) AS "51039",
        SUM(CASE WHEN actnum = '51040' THEN tran_cogs_amt ELSE 0 END) AS "51040",
        SUM(CASE WHEN actnum = '50104' THEN tran_cogs_amt ELSE 0 END) AS "50104",
        SUM(CASE WHEN acctnumber = '40107' THEN tran_amt ELSE 0 END) AS "40107",
        SUM(CASE WHEN acctnumber = '40108' THEN tran_amt ELSE 0 END) AS "40108",
        SUM(CASE WHEN acctnumber = '41101A ' THEN tran_amt ELSE 0 END) AS "41101A ",
        SUM(CASE WHEN acctnumber = '41121' THEN tran_amt ELSE 0 END) AS "41121",
        SUM(CASE WHEN acctnumber = '41122' THEN tran_amt ELSE 0 END) AS "41122",
        SUM(CASE WHEN acctnumber = '41123' THEN tran_amt ELSE 0 END) AS "41123",
        SUM(CASE WHEN acctnumber = '41126' THEN tran_amt ELSE 0 END) AS "41126",
        SUM(CASE WHEN acctnumber = '41222' THEN tran_amt ELSE 0 END) AS "41222",
        SUM(CASE WHEN acctnumber = '41224' THEN tran_amt ELSE 0 END) AS "41224",
        SUM(CASE WHEN acctnumber = '41225' THEN tran_amt ELSE 0 END) AS "41225",
        SUM(CASE WHEN acctnumber = '41228' THEN tran_amt ELSE 0 END) AS "41228",
        SUM(CASE WHEN acctnumber = '41230' THEN tran_amt ELSE 0 END) AS "41230"
    FROM MATERIALIZED_TRANSACTIONS tr
    GROUP BY period_end_date, increment_id, order_id, locname
),

MATERIALIZED_ACCOUNT_ACTIVITY AS (
    SELECT 
        mp.period_end_date,
        'Account Activity' AS INCREMENT_ID,
        NULL AS ORDER_ID,
        NULL AS LOCNAME,
        NULL AS zero_charge_order,
        0 AS "Units Shipped",
        0 AS "GWP Units Shipped",
        0 AS "Non GWP Units Shipped",
        0 AS "Paid Units Shipped",
        0 AS "Dry Units Shipped",
        0 AS "Frozen Units Shipped",
        0 AS "Fresh Units Shipped",
        0 AS "Wine Units Shipped",
        SUM(CASE WHEN ns.accountnumber = '40100' THEN -(COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "40100",
        SUM(CASE WHEN ns.accountnumber = '40101' THEN -(COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "40101",
        SUM(CASE WHEN ns.accountnumber = '40102' THEN -(COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "40102",
        SUM(CASE WHEN ns.accountnumber = '41101' THEN -(COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41101",
        SUM(CASE WHEN ns.accountnumber = '41106' THEN -(COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41106",
        SUM(CASE WHEN ns.accountnumber = '41118' THEN -(COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41118",
        SUM(CASE WHEN ns.accountnumber = '41102' THEN -(COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41102",
        SUM(CASE WHEN ns.accountnumber = '41104' THEN -(COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41104",
        SUM(CASE WHEN ns.accountnumber = '41107' THEN -(COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41107",
        SUM(CASE WHEN ns.accountnumber = '41109' THEN -(COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41109",
        SUM(CASE WHEN ns.accountnumber = '41112' THEN -(COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41112",
        SUM(CASE WHEN ns.accountnumber = '41113' THEN -(COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41113",
        SUM(CASE WHEN ns.accountnumber = '41116' THEN -(COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41116",
        SUM(CASE WHEN ns.accountnumber = '41119' THEN -(COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41119",
        SUM(CASE WHEN ns.accountnumber = '41227' THEN -(COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41227",
        SUM(CASE WHEN ns.accountnumber = '41103' THEN -(COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41103",
        SUM(CASE WHEN ns.accountnumber = '41108' THEN -(COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41108",
        SUM(CASE WHEN ns.accountnumber = '41117' THEN -(COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41117",
        SUM(CASE WHEN ns.accountnumber = '40115' THEN -(COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "40115",
        SUM(CASE WHEN ns.accountnumber = '40116' THEN -(COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "40116",
        SUM(CASE WHEN ns.accountnumber = '50100' THEN (COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "50100",
        SUM(CASE WHEN ns.accountnumber = '50101' THEN (COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "50101",
        SUM(CASE WHEN ns.accountnumber = '51039' THEN (COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "51039",
        SUM(CASE WHEN ns.accountnumber = '51040' THEN (COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "51040",
        SUM(CASE WHEN ns.accountnumber = '50104' THEN (COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "50104",
        SUM(CASE WHEN ns.accountnumber = '40107' THEN (COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "40107",
        SUM(CASE WHEN ns.accountnumber = '40108' THEN (COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "40108",
        SUM(CASE WHEN ns.accountnumber = '41101A ' THEN (COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41101A ",
        SUM(CASE WHEN ns.accountnumber = '41121' THEN (COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41121",
        SUM(CASE WHEN ns.accountnumber = '41122' THEN (COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41122",
        SUM(CASE WHEN ns.accountnumber = '41123' THEN (COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41123",
        SUM(CASE WHEN ns.accountnumber = '41126' THEN (COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41126",
        SUM(CASE WHEN ns.accountnumber = '41222' THEN (COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41222",
        SUM(CASE WHEN ns.accountnumber = '41224' THEN (COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41224",
        SUM(CASE WHEN ns.accountnumber = '41225' THEN (COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41225",
        SUM(CASE WHEN ns.accountnumber = '41228' THEN (COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41228",
        SUM(CASE WHEN ns.accountnumber = '41230' THEN (COALESCE(jed.debit_amount,0) - COALESCE(jed.credit_amount,0)) ELSE 0 END) AS "41230"
    FROM MATERIALIZED_PERIODS mp
    LEFT JOIN ods.NS_JE_HEADER jeh ON jeh.accounting_period_id = mp.accounting_period_id
    LEFT JOIN ods.NS_JE_DETAIL jed ON jed.TRANSACTION_ID = jeh.TRANSACTION_ID 
    LEFT JOIN ods.NETSUITE_ACCOUNTS ns ON jed.ACCOUNT_ID = ns.ACCOUNT_ID 
    WHERE jeh.batch_id IS NULL
    GROUP BY mp.period_end_date
),

MATERIALIZED_COST_ADJUSTMENTS AS (
    SELECT 
        mp.period_end_date,
        'Account Activity' AS INCREMENT_ID,
        NULL AS ORDER_ID,
        NULL AS LOCNAME,
        NULL AS zero_charge_order,
        0 AS "Units Shipped",
        0 AS "GWP Units Shipped",
        0 AS "Non GWP Units Shipped",
        0 AS "Paid Units Shipped",
        0 AS "Dry Units Shipped",
        0 AS "Frozen Units Shipped",
        0 AS "Fresh Units Shipped",
        0 AS "Wine Units Shipped",
        0 AS "40100",
        0 AS "40101",
        0 AS "40102",
        0 AS "41101",
        0 AS "41106",
        0 AS "41118",
        0 AS "41102",
        0 AS "41104",
        0 AS "41107",
        0 AS "41109",
        0 AS "41112",
        0 AS "41113",
        0 AS "41116",
        0 AS "41119",
        0 AS "41227",
        0 AS "41103",
        0 AS "41108",
        0 AS "41117",
        0 AS "40115",
        0 AS "40116",
        SUM(CASE WHEN na.accountnumber = '50100' THEN cad.amount ELSE 0 END) AS "50100",
        SUM(CASE WHEN na.accountnumber = '50101' THEN cad.amount ELSE 0 END) AS "50101",
        0 AS "51039",
        0 AS "51040",
        0 AS "50104",
        0 AS "40107",
        0 AS "40108",
        0 AS "41101A ",
        0 AS "41121",
        0 AS "41122",
        0 AS "41123",
        0 AS "41126",
        0 AS "41222",
        0 AS "41224",
        0 AS "41225",
        0 AS "41228",
        0 AS "41230"
    FROM MATERIALIZED_PERIODS mp
    INNER JOIN ods.NS_COSTADJ_HEADER cah ON cah.accounting_period_id = mp.accounting_period_id
    INNER JOIN ods.NS_COSTADJ_DETAIL cad ON cad.TRANSACTION_ID = cah.TRANSACTION_ID 
    LEFT JOIN ods.NETSUITE_ACCOUNTS na ON na.ACCOUNT_ID = cad.ACCOUNT_ID 
    WHERE na.accountnumber IN ('50100','50101')
    GROUP BY mp.period_end_date
)

-- Main query combining all materialized CTEs
SELECT 
    *,
    CASE WHEN "Dry Units Shipped" > 0 THEN 1 ELSE 0 END AS dry_box,
    CASE WHEN "Frozen Units Shipped" > 0 THEN 1 ELSE 0 END AS frozen_box,
    CASE WHEN "Fresh Units Shipped" > 0 THEN 1 ELSE 0 END AS fresh_box,
    CASE WHEN "Wine Units Shipped" > 0 THEN 1 ELSE 0 END AS wine_box
FROM (
    SELECT * FROM MATERIALIZED_ORDER_METRICS
    UNION ALL
    SELECT * FROM MATERIALIZED_ACCOUNT_ACTIVITY
    UNION ALL
    SELECT * FROM MATERIALIZED_COST_ADJUSTMENTS
) combined_data
ORDER BY increment_id ASC; 