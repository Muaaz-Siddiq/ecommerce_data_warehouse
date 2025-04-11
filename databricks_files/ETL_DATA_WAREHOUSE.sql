-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Stagging and Initial Load

-- COMMAND ----------

-- DROP DATABASE sale_ecommerce_dwh CASCADE
CREATE DATABASE IF NOT EXISTS sale_ecommerce_dwh

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sale_ecommerce_dwh.incremental_value_storage
( 
  id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  start_date DATE,
  incremental_date DATE
)
-- DROP TABLE sale_ecommerce_dwh.incremental_value_storage

-- COMMAND ----------

CREATE OR REPLACE TABLE sale_ecommerce_dwh.stagging_table
AS
SELECT * FROM big_sales_ecoms_cleaned
WHERE created_at BETWEEN (
  SELECT 
    CASE
      WHEN COUNT(*) > 0 THEN (SELECT start_date FROM sale_ecommerce_dwh.incremental_value_storage ORDER BY id DESC LIMIT 1)
      ELSE '2016-01-01'
    END AS str_date

  FROM sale_ecommerce_dwh.incremental_value_storage
) 

AND

(
  SELECT 
    CASE
      WHEN COUNT(*) > 0 THEN (SELECT incremental_date FROM sale_ecommerce_dwh.incremental_value_storage ORDER BY id DESC LIMIT 1)
      ELSE '2016-12-31'
    END AS last_date

  FROM sale_ecommerce_dwh.incremental_value_storage
)


-- COMMAND ----------

SELECT * FROM sale_ecommerce_dwh.stagging_table ORDER BY created_at DESC LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### - Creating Dimension Tables
-- MAGIC #### - Extract Incremental Data from stagging table
-- MAGIC #### - View to Transform Data in dimension and their dimenstion (surrogate key)
-- MAGIC #### - Load Data into Dimenstion Table
-- MAGIC #### - Managing Slowly Changing Dimension

-- COMMAND ----------

CREATE TABLE sale_ecommerce_dwh.DimProducts
(
  item_id INT,
  sku STRING,
  category STRING,
  DimProductKey INT
)

-- COMMAND ----------

CREATE OR REPLACE VIEW sale_ecommerce_dwh.view_DimProducts
AS
SELECT T.*, row_number() over(ORDER BY T.item_id) as DimProductKey FROM
(
  SELECT
   DISTINCT(item_id_new) as item_id,
   sku,
   category
   FROM sale_ecommerce_dwh.stagging_table
) AS T

-- COMMAND ----------

-- INSERT INTO sale_ecommerce_dwh.DimProducts
-- SELECT * FROM sale_ecommerce_dwh.view_DimProducts

--  Managing slowly changing dimension

MERGE INTO sale_ecommerce_dwh.DimProducts AS trg
USING sale_ecommerce_dwh.view_DimProducts AS src
ON trg.item_id = src.item_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

CREATE TABLE sale_ecommerce_dwh.DimDate
(
  created_at DATE,
  year INT,
  month INT,
  fy STRING,
  DimDateKey INT
)

-- COMMAND ----------

CREATE OR REPLACE VIEW sale_ecommerce_dwh.view_DimDates
AS
SELECT T.*, row_number() over(ORDER BY T.created_at) as DimDateKey FROM
(
  SELECT
  DISTINCT created_at AS created_at,
  year,
  month,
  fy

  FROM sale_ecommerce_dwh.stagging_table
) AS T

-- COMMAND ----------

-- INSERT INTO sale_ecommerce_dwh.DimDate
-- SELECT * FROM sale_ecommerce_dwh.view_DimDates

MERGE INTO sale_ecommerce_dwh.dimdate as trg
USING sale_ecommerce_dwh.view_dimdates as src
ON trg.created_at = src.created_at
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

CREATE TABLE sale_ecommerce_dwh.DimStatus ( status STRING, DimStatusKey INT)

-- COMMAND ----------

CREATE OR REPLACE VIEW sale_ecommerce_dwh.view_DimStatus
AS
SELECT T.* , row_number() OVER (ORDER BY T.status) AS DimStatusKey FROM
(
  SELECT DISTINCT status AS status
  FROM sale_ecommerce_dwh.stagging_table
) AS T

-- COMMAND ----------

-- INSERT INTO sale_ecommerce_dwh.DimStatus
-- SELECT * FROM sale_ecommerce_dwh.view_DimStatus

MERGE INTO sale_ecommerce_dwh.dimstatus AS trg
USING sale_ecommerce_dwh.view_dimstatus AS src
ON trg.status = src.status
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

CREATE TABLE sale_ecommerce_dwh.DimPaymentMethod
(
  payment_method STRING,
  DimMethodKey INT
)

-- COMMAND ----------

CREATE OR REPLACE VIEW sale_ecommerce_dwh.view_DimPaymentMethod
AS
SELECT T.* , row_number() OVER (ORDER BY T.payment_method) AS DimMethodKey FROM
(
  SELECT DISTINCT payment_method AS payment_method
  FROM sale_ecommerce_dwh.stagging_table
) AS T

-- COMMAND ----------

-- INSERT INTO sale_ecommerce_dwh.DimPaymentMethod SELECT * FROM sale_ecommerce_dwh.view_DimPaymentMethod

MERGE INTO sale_ecommerce_dwh.DimPaymentMethod AS trg
USING sale_ecommerce_dwh.view_DimPaymentMethod AS src
ON trg.payment_method = src.payment_method
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

CREATE TABLE sale_ecommerce_dwh.DimBiStatus (bi_status STRING, DimBiStatusKey INT)

-- COMMAND ----------

CREATE OR REPLACE VIEW sale_ecommerce_dwh.view_DimBiStatus
AS
SELECT T.* , row_number() OVER (ORDER BY T.bi_status) AS DimBiStatusKey FROM
( SELECT DISTINCT bi_status AS bi_status
  FROM sale_ecommerce_dwh.stagging_table
) AS T

-- COMMAND ----------

-- INSERT INTO sale_ecommerce_dwh.DimBiStatus
-- SELECT * FROM sale_ecommerce_dwh.view_DimBiStatus

MERGE INTO sale_ecommerce_dwh.DimBiStatus AS trg
USING sale_ecommerce_dwh.view_DimBiStatus AS src
ON trg.bi_status = src.bi_status
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

CREATE TABLE sale_ecommerce_dwh.DimCustomer
( 
  customer_id INT,
  customer_since STRING,
  DimCustomerKey INT
)

-- COMMAND ----------

CREATE OR REPLACE VIEW sale_ecommerce_dwh.view_DimCustomer
AS
SELECT T.*, row_number() OVER ( ORDER BY T.customer_id) AS DimCustomerKey
FROM (
  SELECT DISTINCT customer_id AS customer_id, customer_since
  FROM sale_ecommerce_dwh.stagging_table
) AS T

-- COMMAND ----------

-- INSERT INTO sale_ecommerce_dwh.dimcustomer
-- SELECT * FROM sale_ecommerce_dwh.view_DimCustomer

MERGE INTO sale_ecommerce_dwh.DimCustomer AS trg
USING sale_ecommerce_dwh.view_DimCustomer AS src
ON trg.customer_id = src.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##CREATING AND FILLING FACT TABLE

-- COMMAND ----------

CREATE TABLE sale_ecommerce_dwh.FactSalesTable
(
  price DECIMAL (10,2),
  qty_ordered INT,
  grand_total DECIMAL (10,2),
  discount_amount DECIMAL (10,2),
  mv DECIMAL (10,2),
  DimProductKey INT,
  DimDateKey INT,
  DimStatusKey INT,
  DimMethodKey INT,
  DimBiStatusKey INT,
  DimCustomerKey INT
)

-- COMMAND ----------

CREATE OR REPLACE VIEW view_fact_table
AS
SELECT 
  f.price, f.qty_ordered, f.grand_total, f.discount_amount, f.mv, p.DimProductKey, dt.DimDateKey, s.DimStatusKey, m.DimMethodKey, bs.DimBiStatusKey, c.DimCustomerKey
FROM sale_ecommerce_dwh.stagging_table AS F

LEFT JOIN
sale_ecommerce_dwh.dimproducts AS p
ON f.item_id_new = p.item_id

LEFT JOIN
sale_ecommerce_dwh.dimdate AS dt
ON f.created_at = dt.created_at

LEFT JOIN
sale_ecommerce_dwh.dimstatus AS s
ON f.status = s.status

LEFT JOIN
sale_ecommerce_dwh.dimpaymentmethod AS m
ON f.payment_method = m.payment_method

LEFT JOIN
sale_ecommerce_dwh.dimbistatus AS bs
ON f.bi_status = bs.bi_status

LEFT JOIN
sale_ecommerce_dwh.dimcustomer AS c
ON f.customer_id = c.customer_id

-- COMMAND ----------

INSERT INTO sale_ecommerce_dwh.FactSalesTable
SELECT * FROM view_fact_table

-- COMMAND ----------

INSERT INTO sale_ecommerce_dwh.incremental_value_storage ( start_date, incremental_date)
SELECT
  CASE 
    WHEN COUNT(*) > 0 THEN (SELECT MAX(incremental_date) FROM sale_ecommerce_dwh.incremental_value_storage)
    ELSE date('2017-01-01')
  END,

  CASE 
    WHEN COUNT(*) > 0 THEN (SELECT DATE_ADD(MAX(incremental_date), 365) FROM sale_ecommerce_dwh.incremental_value_storage)
    ELSE date('2017-12-31')
  END

FROM sale_ecommerce_dwh.incremental_value_storage

-- COMMAND ----------

SELECT * FROM sale_ecommerce_dwh.FactSalesTable as f LEFT JOIN
sale_ecommerce_dwh.DimDate as dd
ON f.DimDateKey = dd.DimDateKey

ORDER BY dd.created_at DESC