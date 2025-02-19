SELECT "t38693"."ca_county", "t38693"."d_year", "t38705"."web_sales" / "t38702"."web_sales" AS "web_q1_q2_increase", "t38696"."store_sales" / "t38693"."store_sales" AS "store_q1_q2_increase", "t38708"."web_sales" / "t38705"."web_sales" AS "web_q2_q3_increase", "t38699"."store_sales" / "t38696"."store_sales" AS "store_q2_q3_increase" FROM (SELECT "customer_address1069"."ca_county", "date_dim6106"."d_qoy", "date_dim6106"."d_year", SUM("store_sales4096"."ss_ext_sales_price") AS "store_sales" FROM "store_sales" AS "store_sales4096", "date_dim" AS "date_dim6106", "customer_address" AS "customer_address1069" WHERE "store_sales4096"."ss_sold_date_sk" = "date_dim6106"."d_date_sk" AND "store_sales4096"."ss_addr_sk" = "customer_address1069"."ca_address_sk" GROUP BY "customer_address1069"."ca_county", "date_dim6106"."d_qoy", "date_dim6106"."d_year") AS "t38693", (SELECT "customer_address1070"."ca_county", "date_dim6107"."d_qoy", "date_dim6107"."d_year", SUM("store_sales4097"."ss_ext_sales_price") AS "store_sales" FROM "store_sales" AS "store_sales4097", "date_dim" AS "date_dim6107", "customer_address" AS "customer_address1070" WHERE "store_sales4097"."ss_sold_date_sk" = "date_dim6107"."d_date_sk" AND "store_sales4097"."ss_addr_sk" = "customer_address1070"."ca_address_sk" GROUP BY "customer_address1070"."ca_county", "date_dim6107"."d_qoy", "date_dim6107"."d_year") AS "t38696", (SELECT "customer_address1071"."ca_county", "date_dim6108"."d_qoy", "date_dim6108"."d_year", SUM("store_sales4098"."ss_ext_sales_price") AS "store_sales" FROM "store_sales" AS "store_sales4098", "date_dim" AS "date_dim6108", "customer_address" AS "customer_address1071" WHERE "store_sales4098"."ss_sold_date_sk" = "date_dim6108"."d_date_sk" AND "store_sales4098"."ss_addr_sk" = "customer_address1071"."ca_address_sk" GROUP BY "customer_address1071"."ca_county", "date_dim6108"."d_qoy", "date_dim6108"."d_year") AS "t38699", (SELECT "customer_address1072"."ca_county", "date_dim6109"."d_qoy", "date_dim6109"."d_year", SUM("web_sales1619"."ws_ext_sales_price") AS "web_sales" FROM "web_sales" AS "web_sales1619", "date_dim" AS "date_dim6109", "customer_address" AS "customer_address1072" WHERE "web_sales1619"."ws_sold_date_sk" = "date_dim6109"."d_date_sk" AND "web_sales1619"."ws_bill_addr_sk" = "customer_address1072"."ca_address_sk" GROUP BY "customer_address1072"."ca_county", "date_dim6109"."d_qoy", "date_dim6109"."d_year") AS "t38702", (SELECT "customer_address1073"."ca_county", "date_dim6110"."d_qoy", "date_dim6110"."d_year", SUM("web_sales1620"."ws_ext_sales_price") AS "web_sales" FROM "web_sales" AS "web_sales1620", "date_dim" AS "date_dim6110", "customer_address" AS "customer_address1073" WHERE "web_sales1620"."ws_sold_date_sk" = "date_dim6110"."d_date_sk" AND "web_sales1620"."ws_bill_addr_sk" = "customer_address1073"."ca_address_sk" GROUP BY "customer_address1073"."ca_county", "date_dim6110"."d_qoy", "date_dim6110"."d_year") AS "t38705", (SELECT "customer_address1074"."ca_county", "date_dim6111"."d_qoy", "date_dim6111"."d_year", SUM("web_sales1621"."ws_ext_sales_price") AS "web_sales" FROM "web_sales" AS "web_sales1621", "date_dim" AS "date_dim6111", "customer_address" AS "customer_address1074" WHERE "web_sales1621"."ws_sold_date_sk" = "date_dim6111"."d_date_sk" AND "web_sales1621"."ws_bill_addr_sk" = "customer_address1074"."ca_address_sk" GROUP BY "customer_address1074"."ca_county", "date_dim6111"."d_qoy", "date_dim6111"."d_year") AS "t38708" WHERE "t38693"."d_qoy" = 1 AND "t38693"."d_year" = 1999 AND ("t38693"."ca_county" = "t38696"."ca_county" AND "t38696"."d_qoy" = 2) AND ("t38696"."d_year" = 1999 AND "t38696"."ca_county" = "t38699"."ca_county" AND ("t38699"."d_qoy" = 3 AND ("t38699"."d_year" = 1999 AND "t38693"."ca_county" = "t38702"."ca_county"))) AND ("t38702"."d_qoy" = 1 AND "t38702"."d_year" = 1999 AND ("t38702"."ca_county" = "t38705"."ca_county" AND ("t38705"."d_qoy" = 2 AND "t38705"."d_year" = 1999)) AND ("t38702"."ca_county" = "t38708"."ca_county" AND "t38708"."d_qoy" = 3 AND ("t38708"."d_year" = 1999 AND (CASE WHEN "t38702"."web_sales" > 0 THEN CAST("t38705"."web_sales" / "t38702"."web_sales" AS DECIMAL(19, 0)) ELSE NULL END > CASE WHEN "t38693"."store_sales" > 0 THEN CAST("t38696"."store_sales" / "t38693"."store_sales" AS DECIMAL(19, 0)) ELSE NULL END AND CASE WHEN "t38705"."web_sales" > 0 THEN CAST("t38708"."web_sales" / "t38705"."web_sales" AS DECIMAL(19, 0)) ELSE NULL END > CASE WHEN "t38696"."store_sales" > 0 THEN CAST("t38699"."store_sales" / "t38696"."store_sales" AS DECIMAL(19, 0)) ELSE NULL END)))) ORDER BY "t38699"."store_sales" / "t38696"."store_sales"